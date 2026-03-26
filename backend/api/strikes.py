# backend/api/strikes.py

import time
import random
import asyncio
import logging
import orjson
from fastapi import Response

logger = logging.getLogger(__name__)
from datetime import timezone, timedelta, date as PyDate
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
LTP_CACHE_TTL = 2        # 2 seconds — live index prices


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
    try:
        async with pool.acquire() as conn:
            await conn.execute("SET statement_timeout = '60000'")  # 60s for prewarm only
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

            # Prewarm history + gaps cache for the ATM symbol
            if atm_symbol:
                data = await _query_history(conn, atm_symbol)
                _history_cache[atm_symbol] = {"data": data, "ts": time.monotonic()}
                gaps_data = await _query_gaps(conn, atm_symbol)
                _gaps_cache[atm_symbol] = {"data": gaps_data, "ts": time.monotonic()}
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"prewarm_strikes_cache failed (non-fatal): {e}")


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
    # Try today first; fall back to last 7 days if no data (e.g. weekend/holiday)
    price_rows = await conn.fetch("""
        SELECT DISTINCT ON (symbol) symbol, curr_price
        FROM gap_ticks
        WHERE symbol = ANY($1)
          AND timestamp >= CURRENT_DATE
        ORDER BY symbol, timestamp DESC
    """, symbol_list)
    if not price_rows:
        price_rows = await conn.fetch("""
            SELECT DISTINCT ON (symbol) symbol, curr_price
            FROM gap_ticks
            WHERE symbol = ANY($1)
              AND timestamp >= NOW() - INTERVAL '7 days'
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


def _idx_entry(d):
    ltp  = d.get("last_price")
    prev = d.get("ohlc", {}).get("close")
    if ltp is None:
        return {"ltp": None, "change": None, "change_pct": None}
    change     = round(ltp - prev, 2) if prev else None
    change_pct = round(change * 100 / prev, 2) if prev and prev != 0 else None
    return {"ltp": ltp, "change": change, "change_pct": change_pct}

@router.get("/index-ltp")
async def get_index_ltp(request: Request):
    """Returns live LTP + change from prev close for all tracked indices."""
    now = time.monotonic()
    if _ltp_cache["data"] and (now - _ltp_cache["ts"]) < LTP_CACHE_TTL:
        return _ltp_cache["data"]
    try:
        data = _kite.quote(["NSE:NIFTY 50", "BSE:SENSEX", "NSE:NIFTY BANK", "NSE:NIFTY FIN SERVICE", "NSE:NIFTY MID SELECT"])
        result = {
            "NIFTY":      _idx_entry(data.get("NSE:NIFTY 50",          {})),
            "SENSEX":     _idx_entry(data.get("BSE:SENSEX",             {})),
            "BANKNIFTY":  _idx_entry(data.get("NSE:NIFTY BANK",         {})),
            "FINNIFTY":   _idx_entry(data.get("NSE:NIFTY FIN SERVICE",  {})),
            "MIDCPNIFTY": _idx_entry(data.get("NSE:NIFTY MID SELECT",   {})),
        }
        if any(v["ltp"] is not None for v in result.values()):
            _ltp_cache["data"] = result
            _ltp_cache["ts"] = now
            return result
    except Exception:
        pass

    # Fallback: read latest prices from gap_ticks DB
    try:
        pool = request.app.state.pool
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (symbol) symbol, curr_price
                FROM gap_ticks
                WHERE symbol IN ('NIFTY', 'SENSEX', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY')
                  AND timestamp >= NOW() - INTERVAL '7 days'
                ORDER BY symbol, timestamp DESC
            """)
        db_prices = {r["symbol"]: r["curr_price"] for r in rows}
        empty = {"ltp": None, "change": None, "change_pct": None}
        result = {
            "NIFTY":      {"ltp": db_prices.get("NIFTY"),      "change": None, "change_pct": None},
            "SENSEX":     {"ltp": db_prices.get("SENSEX"),     "change": None, "change_pct": None},
            "BANKNIFTY":  {"ltp": db_prices.get("BANKNIFTY"),  "change": None, "change_pct": None},
            "FINNIFTY":   {"ltp": db_prices.get("FINNIFTY"),   "change": None, "change_pct": None},
            "MIDCPNIFTY": {"ltp": db_prices.get("MIDCPNIFTY"), "change": None, "change_pct": None},
        }
        _ltp_cache["data"] = result
        _ltp_cache["ts"] = now
        return result
    except Exception:
        empty = {"ltp": None, "change": None, "change_pct": None}
        return {"NIFTY": empty, "SENSEX": empty, "BANKNIFTY": empty, "FINNIFTY": empty, "MIDCPNIFTY": empty}


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


async def _query_history(conn, symbol: str, date: str = None):
    # Single query: compute IST day bounds inside SQL, then aggregate OHLC.
    # Uses idx_gap_ticks_history_cover (symbol, timestamp) INCLUDE (curr_price).
    # When date (YYYY-MM-DD) is provided, fetch that specific IST day.
    if date:
        date_obj = PyDate.fromisoformat(date)
        rows = await conn.fetch("""
            WITH ticks AS (
                SELECT
                    FLOOR(EXTRACT(EPOCH FROM g.timestamp)/5)*5 AS bucket,
                    g.curr_price,
                    g.timestamp
                FROM gap_ticks g
                WHERE g.symbol = $1
                  AND g.timestamp >= $2::date + TIME '09:15:00'
                  AND g.timestamp <= $2::date + TIME '16:00:00'
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
        """, symbol, date_obj)
    else:
        _HIST_Q = """
            WITH ticks AS (
                SELECT
                    FLOOR(EXTRACT(EPOCH FROM g.timestamp)/5)*5 AS bucket,
                    g.curr_price,
                    g.timestamp
                FROM gap_ticks g
                WHERE g.symbol = $1
                  AND g.timestamp >= $2::date + TIME '09:15:00'
                  AND g.timestamp <= $2::date + TIME '16:00:00'
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
        """
        today = PyDate.today()
        rows = await conn.fetch(_HIST_Q, symbol, today)
        if not rows:
            # Weekend/holiday/late-start — fall back to last date with any tick data
            last_date = await conn.fetchval("""
                SELECT DATE(timestamp) FROM gap_ticks
                WHERE symbol = $1
                ORDER BY timestamp DESC LIMIT 1
            """, symbol)
            if last_date:
                rows = await conn.fetch(_HIST_Q, symbol, last_date)

    # Compact array format [time, open, high, low, close] — 54% smaller than dict format
    return [
        [int(row["bucket"]), float(row["open"]), float(row["high"]), float(row["low"]), float(row["close"])]
        for row in rows
    ]


@router.get("/history/{symbol}")
async def get_history(symbol: str, request: Request, nocache: bool = False, date: str = None):
    pool = request.app.state.pool
    cache_key = f"HIST:{symbol}:{date}" if date else symbol

    if not nocache:
        cached = _history_cache.get(cache_key)
        if cached and time.monotonic() - cached["ts"] < cached.get("ttl", 300):
            return cached["data"]

    async with pool.acquire() as conn:
        data = await _query_history(conn, symbol, date)

    ttl = 3600 if date else 300 + random.randint(0, 60)  # historical data cached longer
    _history_cache[cache_key] = {"data": data, "ts": time.monotonic(), "ttl": ttl}
    return data


_gaps_cache = {}   # symbol → {"data": [...], "ts": float}


async def _query_gaps(conn, symbol: str, date: str = None):
    # Use SENSEX price-jump filter only for the index itself, not for SENSEX options
    is_sensex = symbol == "SENSEX"

    async def _run_gaps_query(date_obj):
        ts_filter = "g.timestamp >= $2::date + TIME '09:15:00' AND g.timestamp <= $2::date + TIME '16:00:00'"
        if is_sensex:
            return await conn.fetch(f"""
                SELECT DISTINCT ON (bucket)
                    FLOOR(EXTRACT(EPOCH FROM g.timestamp)/5)*5 AS bucket,
                    g.direction, g.prev_price, g.curr_price, g.vol_change
                FROM gap_ticks g
                WHERE g.symbol    = $1
                  AND {ts_filter}
                  AND ABS(g.price_jump) >= 3.0
                  AND g.time_diff  = 0.0
                  AND g.spread_pct <= 0.75
                ORDER BY bucket, g.timestamp ASC
            """, symbol, date_obj)
        else:
            return await conn.fetch(f"""
                SELECT DISTINCT ON (bucket)
                    FLOOR(EXTRACT(EPOCH FROM g.timestamp)/5)*5 AS bucket,
                    g.direction, g.prev_price, g.curr_price, g.vol_change
                FROM gap_ticks g
                WHERE g.symbol   = $1
                  AND g.is_gap    = true
                  AND {ts_filter}
                ORDER BY bucket, g.timestamp ASC
            """, symbol, date_obj)

    if date:
        rows = await _run_gaps_query(PyDate.fromisoformat(date))
    else:
        today = PyDate.today()
        rows = await _run_gaps_query(today)
        if not rows:
            # Weekend/holiday/late-start — fall back to last date with any tick data
            last_date = await conn.fetchval("""
                SELECT DATE(timestamp) FROM gap_ticks
                WHERE symbol = $1
                ORDER BY timestamp DESC LIMIT 1
            """, symbol)
            if last_date:
                rows = await _run_gaps_query(last_date)
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
async def get_gaps(symbol: str, request: Request, nocache: bool = False, date: str = None):
    pool = request.app.state.pool
    cache_key = f"HIST:{symbol}:{date}" if date else symbol

    if not nocache:
        cached = _gaps_cache.get(cache_key)
        if cached and time.monotonic() - cached["ts"] < cached.get("ttl", 300):
            return cached["data"]

    async with pool.acquire() as conn:
        data = await _query_gaps(conn, symbol, date)

    ttl = 3600 if date else 300 + random.randint(0, 60)
    _gaps_cache[cache_key] = {"data": data, "ts": time.monotonic(), "ttl": ttl}
    return data


def _compute_gap_fills(history, gaps):
    """Mark each gap is_filled=True if any later candle's high/low crosses the prev_price level.
    Runs server-side in Python so the browser's main thread is never blocked by this work.
    Filled gaps are removed from the working set so they are never re-checked — O(N × unfilled_G)
    instead of O(N × G), which matters a lot by mid-day when most gaps are already filled."""
    for g in gaps:
        g["is_filled"] = False
    unfilled = list(gaps)
    for candle in history:
        if not unfilled:
            break  # all gaps filled — no point scanning remaining candles
        bucket, _o, high, low, _c = candle
        still_unfilled = []
        for g in unfilled:
            if bucket <= g["time"]:
                still_unfilled.append(g)
                continue
            if g["direction"] == "UP" and low <= g["prev_price"]:
                g["is_filled"] = True
            elif g["direction"] != "UP" and high >= g["prev_price"]:
                g["is_filled"] = True
            else:
                still_unfilled.append(g)
        unfilled = still_unfilled
    return gaps


_chart_cache = {}   # symbol → {"data": {...}, "ts": float, "ttl": int}


@router.get("/chart/{symbol}")
async def get_chart(symbol: str, request: Request, nocache: bool = False, date: str = None):
    """Combined history + gaps endpoint with server-side gap-fill computation.
    Replaces two separate /history and /gaps fetches on every panel load."""
    pool = request.app.state.pool
    cache_key = f"CHART:{symbol}:{date}" if date else f"CHART:{symbol}"

    if not nocache:
        cached = _chart_cache.get(cache_key)
        if cached and time.monotonic() - cached["ts"] < cached.get("ttl", 300):
            return Response(content=orjson.dumps(cached["data"]), media_type="application/json")

    async def _fetch_history():
        async with pool.acquire() as conn:
            return await _query_history(conn, symbol, date)

    async def _fetch_gaps():
        async with pool.acquire() as conn:
            return await _query_gaps(conn, symbol, date)

    history, gaps = await asyncio.gather(_fetch_history(), _fetch_gaps())
    _compute_gap_fills(history, gaps)

    data = {"history": history, "gaps": gaps}
    ttl = 3600 if date else 300 + random.randint(0, 60)
    _chart_cache[cache_key] = {"data": data, "ts": time.monotonic(), "ttl": ttl}
    return Response(content=orjson.dumps(data), media_type="application/json")


_hist_symbols_cache: dict = {}  # date_str → list of {symbol, display}

@router.get("/hist-symbols")
async def get_hist_symbols(date: str, request: Request):
    """Returns all distinct symbols that have tick data for a given IST date (YYYY-MM-DD).
    Historical data never changes so results are cached indefinitely after first load."""
    if date in _hist_symbols_cache:
        return _hist_symbols_cache[date]

    pool = request.app.state.pool
    date_obj = PyDate.fromisoformat(date)
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT symbol, strike, option_type, expiry_date
            FROM gap_ticks
            WHERE timestamp >= $1::date + TIME '09:15:00'
              AND timestamp <= $1::date + TIME '15:35:00'
            ORDER BY expiry_date, strike, option_type
        """, date_obj)
    result = [
        {
            "symbol": r["symbol"],
            "display": f'{int(r["strike"])} {r["option_type"]}  [exp {r["expiry_date"].strftime("%d %b")}]'
        }
        for r in rows
    ]
    _hist_symbols_cache[date] = result
    return result


class BatchRequest(BaseModel):
    symbols: list[str]
    nocache: bool = False


@router.post("/batch")
async def get_batch(body: BatchRequest, request: Request):
    """Fetch history + gaps for multiple symbols in parallel. Used by auto-populate."""
    pool = request.app.state.pool
    symbols = list(dict.fromkeys(body.symbols))  # deduplicate, preserve order
    sem = asyncio.Semaphore(20)  # limit concurrent DB pairs to pool size

    async def fetch_one(symbol):
        # Serve from cache if fresh and not bypassed
        if not body.nocache:
            h_cached = _history_cache.get(symbol)
            g_cached  = _gaps_cache.get(symbol)
            now = time.monotonic()
            if (h_cached and now - h_cached["ts"] < h_cached.get("ttl", 300)) and (g_cached and now - g_cached["ts"] < g_cached.get("ttl", 300)):
                return symbol, h_cached["data"], g_cached["data"]

        async def do_history():
            async with pool.acquire() as conn:
                return await _query_history(conn, symbol)

        async def do_gaps():
            async with pool.acquire() as conn:
                return await _query_gaps(conn, symbol)

        async with sem:
            try:
                history, gaps = await asyncio.gather(do_history(), do_gaps())
            except Exception as e:
                logger.warning(f"batch fetch_one failed for {symbol}: {e}")
                return symbol, None, None
        if history is None:
            return symbol, None, None
        _compute_gap_fills(history, gaps)
        ttl = 300 + random.randint(0, 60)
        _history_cache[symbol] = {"data": history, "ts": time.monotonic(), "ttl": ttl}
        _gaps_cache[symbol]    = {"data": gaps,    "ts": time.monotonic(), "ttl": ttl}
        return symbol, history, gaps

    results = await asyncio.gather(*[fetch_one(s) for s in symbols])
    payload = {sym: {"history": h, "gaps": g} for sym, h, g in results if h is not None}
    return Response(content=orjson.dumps(payload), media_type="application/json")

class SLOrder(BaseModel):
    symbol: str
    price: float
    side: str   # BUY or SELL

@router.post("/place-sl-order")
async def place_sl_order(order: SLOrder):

    def round_to_tick(price, tick_size=0.05):
        return round(round(price / tick_size) * tick_size, 2)

    trigger_buffer = 0.10
    price = round_to_tick(order.price)

    if order.side == "BUY":
        trigger = round_to_tick(price - trigger_buffer)
        tx = "BUY"
    else:
        trigger = round_to_tick(price + trigger_buffer)
        tx = "SELL"

    headers = {
        "X-Kite-Version": "3",
        "User-Agent": "Kiteconnect-python/5.0.1",
        "Authorization": f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}",
    }
    data = {
        "variety":          "regular",
        "exchange":         "NFO",
        "tradingsymbol":    order.symbol,
        "transaction_type": tx,
        "quantity":         "65",
        "product":          "NRML",
        "order_type":       "SL",
        "validity":         "DAY",
        "trigger_price":    str(trigger),
        "price":            str(price),
    }

    try:
        r = await kite1.reqsession.post(
            "https://api.kite.trade/orders/regular",
            data=data, headers=headers, timeout=7
        )
        result = r.json()
        if r.status_code != 200:
            msg = result.get("message") or result.get("error") or f"HTTP {r.status_code}"
            logger.error(f"place-sl-order broker error ({tx}): {msg}")
            return {"status": "error", "message": msg}
        order_id = (result.get("data") or {}).get("order_id")
        logger.info(f"SL {tx} {order.symbol} price={price} trigger={trigger} → order_id={order_id}")
        return {"status": "success", "price": price, "trigger": trigger, "order_id": order_id}
    except Exception as e:
        logger.error(f"place-sl-order error: {e}")
        return {"status": "error", "message": str(e)}