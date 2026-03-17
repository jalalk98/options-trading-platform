# backend/api/strikes.py

import time
import random
import asyncio
from datetime import timezone, timedelta
from fastapi import APIRouter, Request
from pydantic import BaseModel
from kiteconnect import KiteConnect
from backend.services.websocket_handler import kite1
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN

_kite = KiteConnect(api_key=KITE_API_KEY)
_kite.set_access_token(KITE_ACCESS_TOKEN)

router = APIRouter()

_strikes_cache = {"data": None, "ts": 0}
STRIKES_CACHE_TTL = 300  # seconds — strikes don't change during the trading day

_atm_cache = {}          # keyed by index, e.g. "NIFTY" / "SENSEX"
ATM_CACHE_TTL = 300      # 5 minutes

_ltp_cache = {"data": None, "ts": 0}
LTP_CACHE_TTL = 10       # 10 seconds — keep near real-time


async def _query_strikes(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT symbol, strike, option_type, expiry_date
            FROM tracked_symbols
            WHERE expiry_date >= CURRENT_DATE
            ORDER BY expiry_date ASC, strike
        """)
    return [
        {
            "symbol": r["symbol"],
            "display": f'{r["expiry_date"]} | {int(r["strike"])} {r["option_type"]}'
        }
        for r in rows
    ]


async def prewarm_strikes_cache(pool):
    async with pool.acquire() as conn:
        result = await _query_strikes(pool)
        _strikes_cache["data"] = result
        _strikes_cache["ts"] = time.monotonic()

        # Prewarm ATM cache for NIFTY
        atm_row = await _query_atm_symbol(conn, "NIFTY")
        if atm_row:
            atm_result = {"symbol": atm_row["symbol"], "strike": float(atm_row["strike"])}
            _atm_cache["NIFTY"] = {"data": atm_result, "ts": time.monotonic()}
            atm_symbol = atm_row["symbol"]
        else:
            atm_symbol = result[0]["symbol"] if result else None

        # Prewarm history cache for the ATM symbol
        if atm_symbol:
            data = await _query_history(conn, atm_symbol)
            _history_cache[atm_symbol] = {"data": data, "ts": time.monotonic()}


@router.get("/strikes")
async def get_strikes(request: Request):

    now = time.monotonic()
    if _strikes_cache["data"] is not None and (now - _strikes_cache["ts"]) < STRIKES_CACHE_TTL:
        return _strikes_cache["data"]

    result = await _query_strikes(request.app.state.pool)
    _strikes_cache["data"] = result
    _strikes_cache["ts"] = now
    return result


async def _query_atm_symbol(conn, index: str = "NIFTY"):
    """
    Returns the ATM CE symbol for the nearest expiry.
    ATM = strike where |CE_price - PE_price| is minimum (put-call parity).
    Uses tracked_symbols for the expiry/strike list (fast),
    then fetches the latest price per symbol via the covering index.
    """
    if index == "SENSEX":
        sym_filter = "symbol LIKE 'SENSEX%'"
    else:
        sym_filter = "symbol NOT LIKE 'SENSEX%' AND symbol NOT LIKE 'BANKNIFTY%'"

    # Step 1: get symbols for nearest expiry from tracked_symbols (fast)
    symbols = await conn.fetch(f"""
        SELECT symbol, strike, option_type
        FROM tracked_symbols
        WHERE expiry_date = (
            SELECT MIN(expiry_date) FROM tracked_symbols
            WHERE expiry_date >= CURRENT_DATE
              AND {sym_filter}
        )
        AND {sym_filter}
    """)

    if not symbols:
        return None

    # Step 2: get latest price for all symbols in one batch query
    symbol_list = [s["symbol"] for s in symbols]
    symbol_meta = {s["symbol"]: {"strike": s["strike"], "option_type": s["option_type"]} for s in symbols}
    price_rows = await conn.fetch("""
        SELECT DISTINCT ON (symbol) symbol, curr_price
        FROM gap_ticks
        WHERE symbol = ANY($1)
        ORDER BY symbol, timestamp DESC
    """, symbol_list)
    prices = {
        r["symbol"]: {**symbol_meta[r["symbol"]], "price": r["curr_price"]}
        for r in price_rows if r["symbol"] in symbol_meta
    }

    # Step 3: find strike where |CE_price - PE_price| is minimum
    strikes = {}
    for sym, info in prices.items():
        k = info["strike"]
        if k not in strikes:
            strikes[k] = {}
        strikes[k][info["option_type"]] = {"symbol": sym, "price": info["price"]}

    best_strike = None
    best_diff = float("inf")
    for strike, opts in strikes.items():
        if "CE" in opts and "PE" in opts:
            diff = abs(opts["CE"]["price"] - opts["PE"]["price"])
            if diff < best_diff:
                best_diff = diff
                best_strike = strike

    if best_strike and "CE" in strikes[best_strike]:
        ce = strikes[best_strike]["CE"]
        # return as asyncpg-compatible Record-like dict
        return {"symbol": ce["symbol"], "strike": best_strike}
    return None


@router.get("/atm-symbol")
async def get_atm_symbol(request: Request, index: str = "NIFTY"):
    index = index.upper()
    now = time.monotonic()
    cached = _atm_cache.get(index)
    if cached and (now - cached["ts"]) < ATM_CACHE_TTL:
        return cached["data"]
    try:
        async with request.app.state.pool.acquire() as conn:
            row = await _query_atm_symbol(conn, index)
        result = {"symbol": row["symbol"], "strike": float(row["strike"])} if row else {"symbol": None, "strike": None}
        _atm_cache[index] = {"data": result, "ts": now}
        return result
    except Exception:
        return {"symbol": None, "strike": None}


@router.get("/index-ltp")
async def get_index_ltp():
    """Returns live LTP for NIFTY and SENSEX indices from Kite API."""
    now = time.monotonic()
    if _ltp_cache["data"] and (now - _ltp_cache["ts"]) < LTP_CACHE_TTL:
        return _ltp_cache["data"]
    try:
        data = _kite.ltp(["NSE:NIFTY 50", "BSE:SENSEX"])
        result = {
            "NIFTY":  data.get("NSE:NIFTY 50",  {}).get("last_price"),
            "SENSEX": data.get("BSE:SENSEX",     {}).get("last_price"),
        }
        _ltp_cache["data"] = result
        _ltp_cache["ts"] = now
        return result
    except Exception:
        return {"NIFTY": None, "SENSEX": None}


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
    # Single query: compute IST day bounds inside SQL, then aggregate OHLC.
    # Uses idx_gap_ticks_history_cover (symbol, timestamp) INCLUDE (curr_price).
    rows = await conn.fetch("""
        WITH bounds AS (
            SELECT
                DATE_TRUNC('day',
                    (MAX(timestamp) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Kolkata'
                ) AT TIME ZONE 'Asia/Kolkata' AS day_start_ist
            FROM gap_ticks
            WHERE symbol = $1
        ),
        ticks AS (
            SELECT
                FLOOR(EXTRACT(EPOCH FROM g.timestamp)/5)*5 AS bucket,
                g.curr_price,
                g.timestamp
            FROM gap_ticks g, bounds
            WHERE g.symbol = $1
              AND g.timestamp >= (bounds.day_start_ist AT TIME ZONE 'UTC')
              AND g.timestamp <  ((bounds.day_start_ist + INTERVAL '1 day') AT TIME ZONE 'UTC')
        )
        SELECT
            bucket,
            (ARRAY_AGG(curr_price ORDER BY timestamp))[1]      AS open,
            MAX(curr_price)                                     AS high,
            MIN(curr_price)                                     AS low,
            (ARRAY_AGG(curr_price ORDER BY timestamp DESC))[1]  AS close
        FROM ticks
        GROUP BY bucket
        ORDER BY bucket ASC
    """, symbol)

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
async def get_history(symbol: str, request: Request, nocache: bool = False):
    pool = request.app.state.pool

    if not nocache:
        cached = _history_cache.get(symbol)
        if cached and time.monotonic() - cached["ts"] < cached.get("ttl", 300):
            return cached["data"]

    async with pool.acquire() as conn:
        data = await _query_history(conn, symbol)

    _history_cache[symbol] = {"data": data, "ts": time.monotonic(), "ttl": 300 + random.randint(0, 60)}
    return data


_gaps_cache = {}   # symbol → {"data": [...], "ts": float}


async def _query_gaps(conn, symbol: str):
    is_sensex = symbol.startswith("SENSEX")
    if is_sensex:
        rows = await conn.fetch("""
            WITH bounds AS (
                SELECT
                    DATE_TRUNC('day',
                        (MAX(timestamp) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Kolkata'
                    ) AT TIME ZONE 'Asia/Kolkata' AS day_start_ist
                FROM gap_ticks WHERE symbol = $1
            )
            SELECT DISTINCT ON (bucket)
                FLOOR(EXTRACT(EPOCH FROM g.timestamp)/5)*5 AS bucket,
                g.direction, g.prev_price, g.curr_price, g.vol_change
            FROM gap_ticks g, bounds
            WHERE g.symbol    = $1
              AND g.timestamp >= (bounds.day_start_ist AT TIME ZONE 'UTC')
              AND g.timestamp <  ((bounds.day_start_ist + INTERVAL '1 day') AT TIME ZONE 'UTC')
              AND ABS(g.price_jump) >= 3.0
              AND g.time_diff  = 0.0
              AND g.spread_pct <= 0.75
            ORDER BY bucket, g.timestamp ASC
        """, symbol)
    else:
        rows = await conn.fetch("""
            WITH bounds AS (
                SELECT
                    DATE_TRUNC('day',
                        (MAX(timestamp) AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Kolkata'
                    ) AT TIME ZONE 'Asia/Kolkata' AS day_start_ist
                FROM gap_ticks WHERE symbol = $1
            )
            SELECT DISTINCT ON (bucket)
                FLOOR(EXTRACT(EPOCH FROM g.timestamp)/5)*5 AS bucket,
                g.direction, g.prev_price, g.curr_price, g.vol_change
            FROM gap_ticks g, bounds
            WHERE g.symbol   = $1
              AND g.is_gap    = true
              AND g.timestamp >= (bounds.day_start_ist AT TIME ZONE 'UTC')
              AND g.timestamp <  ((bounds.day_start_ist + INTERVAL '1 day') AT TIME ZONE 'UTC')
            ORDER BY bucket, g.timestamp ASC
        """, symbol)
    return [
        {
            "time":       int(row["bucket"]),
            "direction":  row["direction"],
            "prev_price": float(row["prev_price"]),
            "curr_price": float(row["curr_price"]),
            "vol_change": int(row["vol_change"]) if row["vol_change"] else 0,
        }
        for row in rows
    ]


@router.get("/gaps/{symbol}")
async def get_gaps(symbol: str, request: Request, nocache: bool = False):
    pool = request.app.state.pool

    if not nocache:
        cached = _gaps_cache.get(symbol)
        if cached and time.monotonic() - cached["ts"] < cached.get("ttl", 300):
            return cached["data"]

    async with pool.acquire() as conn:
        data = await _query_gaps(conn, symbol)

    _gaps_cache[symbol] = {"data": data, "ts": time.monotonic(), "ttl": 300 + random.randint(0, 60)}
    return data


class BatchRequest(BaseModel):
    symbols: list[str]
    nocache: bool = False


@router.post("/batch")
async def get_batch(body: BatchRequest, request: Request):
    """Fetch history + gaps for multiple symbols in parallel. Used by auto-populate."""
    pool = request.app.state.pool
    symbols = list(dict.fromkeys(body.symbols))  # deduplicate, preserve order

    async def fetch_one(symbol):
        # Serve from cache if fresh and not bypassed
        if not body.nocache:
            h_cached = _history_cache.get(symbol)
            g_cached  = _gaps_cache.get(symbol)
            now = time.monotonic()
            if (h_cached and now - h_cached["ts"] < h_cached.get("ttl", 300)) and (g_cached and now - g_cached["ts"] < g_cached.get("ttl", 300)):
                return symbol, h_cached["data"], g_cached["data"]

        # Each query needs its own connection (asyncpg doesn't allow concurrent queries on one conn)
        async def do_history():
            async with pool.acquire() as conn:
                return await _query_history(conn, symbol)

        async def do_gaps():
            async with pool.acquire() as conn:
                return await _query_gaps(conn, symbol)

        history, gaps = await asyncio.gather(do_history(), do_gaps())
        ttl = 300 + random.randint(0, 60)
        _history_cache[symbol] = {"data": history, "ts": time.monotonic(), "ttl": ttl}
        _gaps_cache[symbol]    = {"data": gaps,    "ts": time.monotonic(), "ttl": ttl}
        return symbol, history, gaps

    results = await asyncio.gather(*[fetch_one(s) for s in symbols])
    return {sym: {"history": h, "gaps": g} for sym, h, g in results}

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