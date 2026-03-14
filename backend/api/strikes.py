# backend/api/strikes.py

import time
from datetime import timezone, timedelta
from fastapi import APIRouter, Request
from pydantic import BaseModel
from backend.services.websocket_handler import kite1
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN

router = APIRouter()

_strikes_cache = {"data": None, "ts": 0}
STRIKES_CACHE_TTL = 60  # seconds


async def _query_strikes(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT symbol, strike, option_type, expiry_date
            FROM gap_ticks
            ORDER BY expiry_date DESC, strike
        """)
    return [
        {
            "symbol": r["symbol"],
            "display": f'{r["expiry_date"]} | {int(r["strike"])} {r["option_type"]}'
        }
        for r in rows
    ]


async def prewarm_strikes_cache(pool):
    result = await _query_strikes(pool)
    _strikes_cache["data"] = result
    _strikes_cache["ts"] = time.monotonic()
    # Prewarm DB plan cache for history queries by running one history fetch
    if result:
        first_symbol = result[0]["symbol"]
        async with pool.acquire() as conn:
            data = await _query_history(conn, first_symbol)
        _history_cache[first_symbol] = {"data": data, "ts": time.monotonic()}


@router.get("/strikes")
async def get_strikes(request: Request):

    now = time.monotonic()
    if _strikes_cache["data"] is not None and (now - _strikes_cache["ts"]) < STRIKES_CACHE_TTL:
        return _strikes_cache["data"]

    result = await _query_strikes(request.app.state.pool)
    _strikes_cache["data"] = result
    _strikes_cache["ts"] = now
    return result


@router.get("/resolve-symbol")
async def resolve_symbol(display: str, request: Request):

    try:
        # Example display:
        # "2026-03-02 | 24600 CE"

        parts = display.split("|")
        expiry = parts[0].strip()
        strike_part = parts[1].strip()

        strike, option_type = strike_part.split()

        pool = request.app.state.pool

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT symbol
                FROM gap_ticks
                WHERE strike = $1
                AND option_type = $2
                AND expiry_date = $3
                LIMIT 1
                """,
                float(strike),
                option_type,
                expiry
            )

        if row:
            return {"symbol": row["symbol"]}

        return {"symbol": None}

    except Exception as e:
        return {"symbol": None}


_IST = timezone(timedelta(hours=5, minutes=30))
_history_cache = {}   # symbol → {"day": date, "data": [...]}


async def _query_history(conn, symbol: str):
    # Step 1: O(1) index lookup — find the latest timestamp for this symbol
    max_ts = await conn.fetchval(
        "SELECT MAX(timestamp) FROM gap_ticks WHERE symbol = $1", symbol
    )
    if not max_ts:
        return []

    # Step 2: compute IST day boundaries (asyncpg returns naive UTC datetimes)
    max_ist = max_ts.replace(tzinfo=timezone.utc).astimezone(_IST)
    day_start_ist = max_ist.replace(hour=0, minute=0, second=0, microsecond=0)
    # Strip back to naive UTC for asyncpg
    day_start = day_start_ist.astimezone(timezone.utc).replace(tzinfo=None)
    day_end   = (day_start_ist + timedelta(days=1)).astimezone(timezone.utc).replace(tzinfo=None)

    # Step 3: range query — uses covering index, zero heap reads
    rows = await conn.fetch("""
        SELECT
            bucket,
            (ARRAY_AGG(curr_price ORDER BY timestamp))[1]      AS open,
            MAX(curr_price)                                     AS high,
            MIN(curr_price)                                     AS low,
            (ARRAY_AGG(curr_price ORDER BY timestamp DESC))[1]  AS close
        FROM (
            SELECT
                FLOOR(EXTRACT(EPOCH FROM timestamp)/5)*5 AS bucket,
                curr_price, timestamp
            FROM gap_ticks
            WHERE symbol = $1
              AND timestamp >= $2
              AND timestamp <  $3
        ) t
        GROUP BY bucket
        ORDER BY bucket ASC
    """, symbol, day_start, day_end)

    return [
        {
            "time":  int(row["bucket"]),
            "open":  float(row["open"]),
            "high":  float(row["high"]),
            "low":   float(row["low"]),
            "close": float(row["close"]),
        }
        for row in rows
    ]


@router.get("/history/{symbol}")
async def get_history(symbol: str, request: Request):
    pool = request.app.state.pool

    cached = _history_cache.get(symbol)
    if cached:
        if time.monotonic() - cached["ts"] < 10:
            return cached["data"]

    async with pool.acquire() as conn:
        data = await _query_history(conn, symbol)

    _history_cache[symbol] = {"data": data, "ts": time.monotonic()}
    return data


_gaps_cache = {}   # symbol → {"data": [...], "ts": float}


@router.get("/gaps/{symbol}")
async def get_gaps(symbol: str, request: Request):
    pool = request.app.state.pool

    cached = _gaps_cache.get(symbol)
    if cached and time.monotonic() - cached["ts"] < 10:
        return cached["data"]

    async with pool.acquire() as conn:
        max_ts = await conn.fetchval(
            "SELECT MAX(timestamp) FROM gap_ticks WHERE symbol = $1", symbol
        )
        if not max_ts:
            return []

        max_ist      = max_ts.replace(tzinfo=timezone.utc).astimezone(_IST)
        day_start_ist = max_ist.replace(hour=0, minute=0, second=0, microsecond=0)
        day_start    = day_start_ist.astimezone(timezone.utc).replace(tzinfo=None)
        day_end      = (day_start_ist + timedelta(days=1)).astimezone(timezone.utc).replace(tzinfo=None)

        # Check if this symbol has any is_gap rows (NIFTY) or needs fallback (SENSEX)
        is_sensex = symbol.startswith("SENSEX")

        if is_sensex:
            # SENSEX historical data was collected before per-symbol gap detection.
            # Detect gaps on-the-fly using SENSEX-specific thresholds (3x NIFTY):
            #   price_jump >= 3.0, time_diff = 0.0, spread_pct <= 0.75
            rows = await conn.fetch("""
                SELECT DISTINCT ON (bucket)
                    FLOOR(EXTRACT(EPOCH FROM timestamp)/5)*5 AS bucket,
                    direction,
                    prev_price,
                    curr_price,
                    vol_change
                FROM gap_ticks
                WHERE symbol    = $1
                  AND timestamp >= $2
                  AND timestamp <  $3
                  AND ABS(price_jump) >= 3.0
                  AND time_diff  = 0.0
                  AND spread_pct <= 0.75
                ORDER BY bucket, timestamp ASC
            """, symbol, day_start, day_end)
        else:
            # NIFTY/BANKNIFTY: use the is_gap flag set by the gap processor
            rows = await conn.fetch("""
                SELECT DISTINCT ON (bucket)
                    FLOOR(EXTRACT(EPOCH FROM timestamp)/5)*5 AS bucket,
                    direction,
                    prev_price,
                    curr_price,
                    vol_change
                FROM gap_ticks
                WHERE symbol   = $1
                  AND is_gap    = true
                  AND timestamp >= $2
                  AND timestamp <  $3
                ORDER BY bucket, timestamp ASC
            """, symbol, day_start, day_end)

    data = [
        {
            "time":       int(row["bucket"]),
            "direction":  row["direction"],
            "prev_price": float(row["prev_price"]),
            "curr_price": float(row["curr_price"]),
            "vol_change": int(row["vol_change"]) if row["vol_change"] else 0,
        }
        for row in rows
    ]
    _gaps_cache[symbol] = {"data": data, "ts": time.monotonic()}
    return data

class SLOrder(BaseModel):
    symbol: str
    price: float
    side: str   # BUY or SELL

@router.post("/place-sl-order")
async def place_sl_order(order: SLOrder):

    try:
        print(f"Order request received: {order.symbol} {order.side} {order.price}")
        def round_to_tick(price, tick_size=0.05):
            return round(round(price / tick_size) * tick_size, 2)

        trigger_buffer = 0.10
        price = round_to_tick(order.price)

        if order.side == "BUY":

            trigger = round_to_tick(price - trigger_buffer)

            await kite1.hard_code_regular_buy_order(
                exchange="NFO",
                trade_symbol=order.symbol,
                qty=65,
                price=price,
                trig_price=trigger,
                api_key=KITE_API_KEY,
                access_token=KITE_ACCESS_TOKEN
            )

        elif order.side == "SELL":

            trigger = round_to_tick(price + trigger_buffer)

            await kite1.hard_code_regular_sell_order(
                exchange="NFO",
                trade_symbol=order.symbol,
                qty=65,
                stop_loss_price=price,
                trig_price=trigger,
                api_key=KITE_API_KEY,
                access_token=KITE_ACCESS_TOKEN
            )

        return {
            "status": "success",
            "price": price,
            "trigger": trigger
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}