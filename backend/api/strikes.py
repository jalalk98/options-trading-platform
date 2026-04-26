# backend/api/strikes.py

import re
import time
import random
import asyncio
import bisect
import logging
import orjson
import subprocess
import shutil
from fastapi import Response

logger = logging.getLogger(__name__)
import json
from datetime import datetime, timezone, timedelta, date as PyDate
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
LTP_CACHE_TTL  = 2       # 2 seconds — live index prices
_ltp_refreshing: bool = False
_ltp_inflight: "asyncio.Task | None" = None


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

    # Step 2: get latest price for all symbols from candles_5s (fast index scan, no heap fetches)
    symbol_list = [s["symbol"] for s in symbols]
    symbol_meta = {s["symbol"]: {"strike": s["strike"], "option_type": s["option_type"]} for s in symbols}
    price_rows = await conn.fetch("""
        SELECT c.symbol, c.close AS curr_price
        FROM candles_5s c
        JOIN (
            SELECT symbol, MAX(bucket) AS max_bucket
            FROM candles_5s
            WHERE symbol = ANY($1)
              AND bucket >= EXTRACT(EPOCH FROM NOW() - INTERVAL '5 days')::BIGINT
            GROUP BY symbol
        ) latest ON c.symbol = latest.symbol AND c.bucket = latest.max_bucket
    """, symbol_list)
    if not price_rows:
        # Widen search if no recent candles (e.g. long holiday break)
        price_rows = await conn.fetch("""
            SELECT c.symbol, c.close AS curr_price
            FROM candles_5s c
            JOIN (
                SELECT symbol, MAX(bucket) AS max_bucket
                FROM candles_5s
                WHERE symbol = ANY($1)
                GROUP BY symbol
            ) latest ON c.symbol = latest.symbol AND c.bucket = latest.max_bucket
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

async def _compute_ltp(pool) -> dict:
    """Fetch live index LTPs: Kite first (2s timeout), DB fallback.
    Sets _ltp_cache['ttl'] as a side-effect (2s live, 60s DB fallback)."""
    # Try Kite with 2-second timeout
    try:
        loop = asyncio.get_event_loop()
        data = await asyncio.wait_for(
            loop.run_in_executor(None, lambda: _kite.quote(
                ["NSE:NIFTY 50", "BSE:SENSEX", "NSE:NIFTY BANK",
                 "NSE:NIFTY FIN SERVICE", "NSE:NIFTY MID SELECT"]
            )),
            timeout=2.0,
        )
        result = {
            "NIFTY":      _idx_entry(data.get("NSE:NIFTY 50",          {})),
            "SENSEX":     _idx_entry(data.get("BSE:SENSEX",             {})),
            "BANKNIFTY":  _idx_entry(data.get("NSE:NIFTY BANK",         {})),
            "FINNIFTY":   _idx_entry(data.get("NSE:NIFTY FIN SERVICE",  {})),
            "MIDCPNIFTY": _idx_entry(data.get("NSE:NIFTY MID SELECT",   {})),
        }
        if any(v["ltp"] is not None for v in result.values()):
            _ltp_cache["ttl"] = LTP_CACHE_TTL  # 2s — live data
            return result
    except Exception:
        pass

    # DB fallback: LATERAL forces per-symbol index lookup via
    # idx_candles_5s_symbol_bucket_desc → 5 index seeks, 1 row each, ~7ms total
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT s.symbol, c.close AS curr_price
            FROM (VALUES ('NIFTY'), ('SENSEX'), ('BANKNIFTY'), ('FINNIFTY'), ('MIDCPNIFTY')) AS s(symbol)
            CROSS JOIN LATERAL (
                SELECT close
                FROM candles_5s
                WHERE symbol = s.symbol
                  AND bucket >= EXTRACT(EPOCH FROM NOW() - INTERVAL '5 days')::BIGINT
                ORDER BY symbol, bucket DESC
                LIMIT 1
            ) c
        """)
    db_prices = {r["symbol"]: r["curr_price"] for r in rows}
    _ltp_cache["ttl"] = 60  # 60s — DB fallback (market closed)
    return {
        "NIFTY":      {"ltp": db_prices.get("NIFTY"),      "change": None, "change_pct": None},
        "SENSEX":     {"ltp": db_prices.get("SENSEX"),     "change": None, "change_pct": None},
        "BANKNIFTY":  {"ltp": db_prices.get("BANKNIFTY"),  "change": None, "change_pct": None},
        "FINNIFTY":   {"ltp": db_prices.get("FINNIFTY"),   "change": None, "change_pct": None},
        "MIDCPNIFTY": {"ltp": db_prices.get("MIDCPNIFTY"), "change": None, "change_pct": None},
    }


async def _refresh_ltp_background(pool) -> None:
    """Refresh LTP cache in background (stale-while-revalidate helper)."""
    global _ltp_refreshing
    try:
        result = await _compute_ltp(pool)
        _ltp_cache["data"] = result
        _ltp_cache["ts"]   = time.monotonic()
    except Exception as e:
        print(f"[LTP] background refresh failed: {e}", flush=True)
    finally:
        _ltp_refreshing = False


@router.get("/index-ltp")
async def get_index_ltp(request: Request):
    """Returns live LTP + change from prev close for all tracked indices.

    Stale-while-revalidate: any cached data (even stale) is returned instantly
    while a background task silently refreshes.  On cold start (no cache) the
    first caller computes; concurrent callers wait on the same in-flight Task
    instead of each spawning their own DB query.
    """
    global _ltp_refreshing, _ltp_inflight

    max_age = _ltp_cache.get("ttl", 60)

    # ── Stale-while-revalidate ─────────────────────────────────────────────
    if _ltp_cache.get("data"):
        age = time.monotonic() - _ltp_cache.get("ts", 0)
        if age >= max_age and not _ltp_refreshing:
            # Stale: trigger background refresh but return immediately
            _ltp_refreshing = True
            asyncio.create_task(_refresh_ltp_background(request.app.state.pool))
        return _ltp_cache["data"]   # always instant once cache is populated

    # ── Cold start: no cache yet — compute once, deduplicate concurrent callers
    if _ltp_inflight and not _ltp_inflight.done():
        # Another coroutine is already computing — wait for it instead of a
        # duplicate DB query
        try:
            await _ltp_inflight
            if _ltp_cache.get("data"):
                return _ltp_cache["data"]
        except Exception:
            pass

    # We are the first caller — create task so latecomers can join it
    pool = request.app.state.pool
    _ltp_inflight = asyncio.create_task(_compute_ltp(pool))
    try:
        result = await _ltp_inflight
        _ltp_cache["data"] = result
        _ltp_cache["ts"]   = time.monotonic()
        return result
    except Exception:
        empty = {"ltp": None, "change": None, "change_pct": None}
        return {"NIFTY": empty, "SENSEX": empty, "BANKNIFTY": empty,
                "FINNIFTY": empty, "MIDCPNIFTY": empty}
    finally:
        _ltp_inflight = None


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
                FROM tracked_symbols
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

# ── Fast-table flag ────────────────────────────────────────────────────────
# Set False to revert to scanning gap_ticks (old behaviour, zero risk).
_USE_FAST_TABLES = True


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

_HIST_INCR_Q = """
    WITH ticks AS (
        SELECT
            FLOOR(EXTRACT(EPOCH FROM g.timestamp)/5)*5 AS bucket,
            g.curr_price,
            g.timestamp
        FROM gap_ticks g
        WHERE g.symbol = $1
          AND g.timestamp >= TO_TIMESTAMP($2) - INTERVAL '10 seconds'
          AND g.timestamp >= $3::date + TIME '09:15:00'
          AND g.timestamp <= $3::date + TIME '16:00:00'
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


def _rows_to_candles(rows):
    return [
        [int(r["bucket"]), float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])]
        for r in rows
    ]


# ── Fast queries (candles_5s / gap_events) ────────────────────────────────

_CANDLES_Q = """
    SELECT bucket, open, high, low, close
    FROM candles_5s
    WHERE symbol = $1
      AND bucket >= EXTRACT(EPOCH FROM $2::date + TIME '09:15:00')::BIGINT
      AND bucket <= EXTRACT(EPOCH FROM $2::date + TIME '16:00:00')::BIGINT
    ORDER BY bucket ASC
"""

_CANDLES_INCR_Q = """
    SELECT bucket, open, high, low, close
    FROM candles_5s
    WHERE symbol = $1
      AND bucket >= $2 - 10
      AND bucket >= EXTRACT(EPOCH FROM $3::date + TIME '09:15:00')::BIGINT
      AND bucket <= EXTRACT(EPOCH FROM $3::date + TIME '16:00:00')::BIGINT
    ORDER BY bucket ASC
"""

_GAP_EVENTS_Q = """
    SELECT bucket, direction, prev_price, curr_price, vol_change
    FROM gap_events
    WHERE symbol = $1
      AND bucket >= EXTRACT(EPOCH FROM $2::date + TIME '09:15:00')::BIGINT
      AND bucket <= EXTRACT(EPOCH FROM $2::date + TIME '16:00:00')::BIGINT
    ORDER BY bucket ASC
"""


def _rows_to_candles_fast(rows):
    return [
        [int(r["bucket"]), float(r["open"]), float(r["high"]), float(r["low"]), float(r["close"])]
        for r in rows
    ]


async def _get_last_trading_date(conn, symbol: str) -> PyDate | None:
    """Find the most recent trading date for a symbol using candles_5s (0.1ms index scan)."""
    last_bucket = await conn.fetchval(
        "SELECT MAX(bucket) FROM candles_5s WHERE symbol=$1", symbol)
    return PyDate.fromtimestamp(last_bucket) if last_bucket else None


async def _query_history_fast(conn, symbol: str, date: str = None, since_bucket: int = None):
    """Read OHLC from candles_5s — scans ~375 rows vs ~30k in gap_ticks."""
    if date:
        rows = await conn.fetch(_CANDLES_Q, symbol, PyDate.fromisoformat(date))
        return _rows_to_candles_fast(rows)

    today = PyDate.today()
    if since_bucket:
        rows = await conn.fetch(_CANDLES_INCR_Q, symbol, since_bucket, today)
        return _rows_to_candles_fast(rows)

    rows = await conn.fetch(_CANDLES_Q, symbol, today)
    if not rows:
        last_date = await _get_last_trading_date(conn, symbol)
        if last_date:
            rows = await conn.fetch(_CANDLES_Q, symbol, last_date)
    return _rows_to_candles_fast(rows)


async def _query_gaps_fast(conn, symbol: str, date: str = None, since_bucket: int = None):
    """Read gap events from gap_events — scans ~10-30 rows vs ~30k in gap_ticks.
    SENSEX index uses the old gap_ticks path (non-standard filter, one symbol)."""
    if symbol == "SENSEX":
        return None  # caller falls back to original _query_gaps

    def _to_gap_list(rows):
        return [
            {
                "time":       int(r["bucket"]),
                "direction":  r["direction"],
                "prev_price": float(r["prev_price"]),
                "curr_price": float(r["curr_price"]),
                "vol_change": int(r["vol_change"]) if r["vol_change"] else 0,
            }
            for r in rows
        ]

    if date:
        rows = await conn.fetch(_GAP_EVENTS_Q, symbol, PyDate.fromisoformat(date))
        return _to_gap_list(rows)

    today = PyDate.today()
    if since_bucket:
        incr_q = """
            SELECT bucket, direction, prev_price, curr_price, vol_change
            FROM gap_events
            WHERE symbol = $1
              AND bucket >= $2 - 10
              AND bucket >= EXTRACT(EPOCH FROM $3::date + TIME '09:15:00')::BIGINT
              AND bucket <= EXTRACT(EPOCH FROM $3::date + TIME '16:00:00')::BIGINT
            ORDER BY bucket ASC
        """
        rows = await conn.fetch(incr_q, symbol, since_bucket, today)
        return _to_gap_list(rows)

    rows = await conn.fetch(_GAP_EVENTS_Q, symbol, today)
    if not rows:
        last_date = await _get_last_trading_date(conn, symbol)
        if last_date:
            rows = await conn.fetch(_GAP_EVENTS_Q, symbol, last_date)
    return _to_gap_list(rows)


async def _query_history(conn, symbol: str, date: str = None, since_bucket: int = None):
    """Full or incremental OHLC history query.
    since_bucket: if set, only fetch candles at or after that epoch second (incremental update).
    """
    if _USE_FAST_TABLES:
        return await _query_history_fast(conn, symbol, date, since_bucket)

    if date:
        rows = await conn.fetch(_HIST_Q, symbol, PyDate.fromisoformat(date))
        return _rows_to_candles(rows)

    today = PyDate.today()
    if since_bucket:
        rows = await conn.fetch(_HIST_INCR_Q, symbol, since_bucket, today)
        return _rows_to_candles(rows)

    rows = await conn.fetch(_HIST_Q, symbol, today)
    if not rows:
        last_date = await conn.fetchval(
            "SELECT DATE(timestamp) FROM gap_ticks WHERE symbol=$1 ORDER BY timestamp DESC LIMIT 1",
            symbol)
        if last_date:
            rows = await conn.fetch(_HIST_Q, symbol, last_date)
    return _rows_to_candles(rows)


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

    ttl = 3600 if date else 600 + random.randint(0, 60)
    _history_cache[cache_key] = {"data": data, "ts": time.monotonic(), "ttl": ttl}
    return data


_gaps_cache = {}   # symbol → {"data": [...], "ts": float}


async def _query_gaps(conn, symbol: str, date: str = None, since_bucket: int = None):
    if _USE_FAST_TABLES:
        result = await _query_gaps_fast(conn, symbol, date, since_bucket)
        if result is not None:  # None means SENSEX — fall through to old path
            return result

    # Use SENSEX price-jump filter only for the index itself, not for SENSEX options
    is_sensex = symbol == "SENSEX"

    async def _run_gaps_query(date_obj, since_epoch=None):
        ts_filter = "g.timestamp >= $2::date + TIME '09:15:00' AND g.timestamp <= $2::date + TIME '16:00:00'"
        params = [symbol, date_obj]
        if since_epoch is not None:
            ts_filter += " AND g.timestamp >= TO_TIMESTAMP($3) - INTERVAL '10 seconds'"
            params.append(float(since_epoch))
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
            """, *params)
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
            """, *params)

    if date:
        rows = await _run_gaps_query(PyDate.fromisoformat(date))
    else:
        today = PyDate.today()
        rows = await _run_gaps_query(today, since_epoch=since_bucket)
        if not rows and since_bucket is None:
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

    ttl = 3600 if date else 600 + random.randint(0, 60)
    _gaps_cache[cache_key] = {"data": data, "ts": time.monotonic(), "ttl": ttl}
    return data


def _compute_gap_fills(history, gaps):
    """Mark each gap is_filled=True if any later candle's high/low crosses the prev_price level.

    Uses suffix min(low) / suffix max(high) arrays so each gap needs only a binary search
    + O(1) array lookup — O(N + G log N) total vs the old O(N × G) scan."""
    if not gaps:
        return gaps

    n = len(history)
    if n == 0:
        for g in gaps:
            g["is_filled"] = False
        return gaps

    buckets = [c[0] for c in history]   # sorted list of candle bucket times

    # Build suffix max(high) and suffix min(low) — O(N)
    INF = float("inf")
    suffix_max_high = [0.0] * (n + 1)
    suffix_min_low  = [INF]  * (n + 1)
    for i in range(n - 1, -1, -1):
        h = history[i][2]
        l = history[i][3]
        suffix_max_high[i] = h if h > suffix_max_high[i + 1] else suffix_max_high[i + 1]
        suffix_min_low[i]  = l if l < suffix_min_low[i + 1]  else suffix_min_low[i + 1]

    # For each gap, binary-search the first candle after the gap, then check suffix — O(G log N)
    for g in gaps:
        idx = bisect.bisect_right(buckets, g["time"])   # first candle strictly after gap
        if idx >= n:
            g["is_filled"] = False
        elif g["direction"] == "UP":
            g["is_filled"] = suffix_min_low[idx] <= g["prev_price"]
        else:
            g["is_filled"] = suffix_max_high[idx] >= g["prev_price"]

    return gaps


_chart_cache = {}   # symbol → {"data": {...}, "ts": float, "ttl": int}
_jump_cache:  dict = {}  # f"{symbol}_{date}" → {"data": [...], "ts": float}


# ── Conviction scorer ──────────────────────────────────────────────────────
_CONVICTION_Q = """
    SELECT
        symbol,
        to_timestamp(bucket) AT TIME ZONE 'UTC' AS candle_time,
        bucket,
        close,
        gap_ref_price,
        gap_ref_bucket,
        dist_from_gap,
        dist_from_gap_pct,
        closed_above_gap,
        candles_above_gap,
        seconds_above_gap,
        delta,
        cum_delta,
        oi_close,
        oi_change,
        depth_imbalance
    FROM candles_5s
    WHERE symbol = $1
      AND bucket >= EXTRACT(EPOCH FROM CURRENT_DATE
                   + TIME '09:15:00')::BIGINT
      AND candles_above_gap >= 3
    ORDER BY bucket DESC
    LIMIT 1
"""

_CONVICTION_SAVE_Q = """
    INSERT INTO conviction_signals (
        symbol, bucket, signal_time, score,
        gap_ref_price, candles_above, seconds_above,
        dist_from_gap_pct, checks
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (symbol, bucket) DO NOTHING
    RETURNING id
"""

_CONVICTION_HISTORY_Q = """
    SELECT
        bucket,
        score,
        gap_ref_price,
        candles_above,
        seconds_above,
        dist_from_gap_pct,
        checks
    FROM conviction_signals
    WHERE symbol = $1
      AND bucket >= EXTRACT(EPOCH FROM ($2::date
                   + TIME '09:15:00'))::BIGINT
      AND bucket <= EXTRACT(EPOCH FROM ($2::date
                   + TIME '16:00:00'))::BIGINT
    ORDER BY bucket ASC
"""

@router.get("/conviction/history/{symbol}")
async def get_conviction_history(symbol: str, request: Request, date: str = None):
    """
    Returns all conviction signals for a symbol on a given date (defaults to today).
    Used by frontend to render historical ▲ markers.
    """
    try:
        target_date = (
            datetime.strptime(date, "%Y-%m-%d").date()
            if date
            else PyDate.today()
        )
        pool = request.app.state.pool
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                _CONVICTION_HISTORY_Q,
                symbol,
                target_date,
            )
        signals = [
            {
                "bucket"           : row["bucket"],
                "score"            : row["score"],
                "gap_ref_price"    : row["gap_ref_price"],
                "candles_above"    : row["candles_above"],
                "seconds_above"    : row["seconds_above"],
                "dist_from_gap_pct": row["dist_from_gap_pct"],
                "checks"           : json.loads(row["checks"]) if isinstance(row["checks"], str) else (row["checks"] or {}),
            }
            for row in rows
        ]
        return {"symbol": symbol, "signals": signals}
    except Exception as e:
        return {"symbol": symbol, "signals": [], "error": str(e)}


@router.get("/conviction/{symbol}")
async def get_conviction(symbol: str, request: Request):
    """
    Returns go-long conviction score for a symbol.
    Only scores when candles_above_gap >= 3.
    Score >= 4 out of 6 = go_long signal.
    Saves signal to conviction_signals on first fire (candles_above == 3).
    """
    try:
        pool = request.app.state.pool
        async with pool.acquire() as conn:
            row = await conn.fetchrow(_CONVICTION_Q, symbol)

            if row is None:
                return {
                    "symbol"        : symbol,
                    "go_long"       : False,
                    "score"         : 0,
                    "reason"        : "no_gap_sustaining",
                    "checks"        : {},
                    "candles_above" : 0,
                }

            # ── Run the 6 conviction checks ───────────────────────
            checks = {
                "candles_held"      : (
                    row["candles_above_gap"] is not None
                    and row["candles_above_gap"] >= 3
                ),
                "safe_buffer"       : (
                    row["dist_from_gap_pct"] is not None
                    and row["dist_from_gap_pct"] > 0.3
                ),
                "delta_positive"    : (
                    row["delta"] is not None
                    and row["delta"] > 0
                ),
                "cum_delta_positive": (
                    row["cum_delta"] is not None
                    and row["cum_delta"] > 0
                ),
                "cum_delta_strong"  : (
                    row["cum_delta"] is not None
                    and row["cum_delta"] > 500
                ),
                "bids_stacking"     : (
                    row["depth_imbalance"] is not None
                    and row["depth_imbalance"] > 0.55
                ),
            }

            score   = sum(checks.values())
            go_long = score >= 3

            # ── Save signal on first fire (candles_above == 3) ───
            if go_long and row["candles_above_gap"] == 3:
                try:
                    await conn.fetchval(
                        _CONVICTION_SAVE_Q,
                        symbol,
                        row["bucket"],
                        row["candle_time"],
                        score,
                        row["gap_ref_price"],
                        row["candles_above_gap"],
                        row["seconds_above_gap"],
                        row["dist_from_gap_pct"],
                        json.dumps(checks),
                    )
                except Exception as e:
                    print(f"Conviction save failed (non-critical): {e}")

        return {
            "symbol"            : symbol,
            "candle_time"       : row["candle_time"].isoformat()
                                  if row["candle_time"] else None,
            "bucket"            : row["bucket"],
            "gap_ref_price"     : row["gap_ref_price"],
            "candles_above"     : row["candles_above_gap"],
            "seconds_above"     : row["seconds_above_gap"],
            "dist_from_gap_pct" : row["dist_from_gap_pct"],
            "checks"            : checks,
            "score"             : score,
            "go_long"           : go_long,
        }

    except Exception as e:
        return {
            "symbol"  : symbol,
            "go_long" : False,
            "score"   : 0,
            "error"   : str(e),
            "checks"  : {},
        }


@router.get("/jumps/{symbol}")
async def get_jump_history(symbol: str, request: Request, date: str = None):
    """Returns all intraday price jumps above per-index threshold for a symbol on a given date.
    Uses two targeted SQL queries + O(N) single-pass fill detection + TTL cache (30s live / 86400s historical).
    Query 1 hits idx_gap_ticks_jump_lookup (partial, price_jump+curr_price as key cols) for <1s cold compute.
    """
    try:
        import math as _math

        def _price_filter(sym: str) -> float:
            if sym.startswith("SENSEX"):       return 600.0
            elif sym.startswith("BANKNIFTY"):  return 400.0
            elif sym.startswith("MIDCPNIFTY"): return 120.0
            elif sym.startswith("FINNIFTY"):   return 300.0
            return 300.0  # NIFTY and default

        def _jump_threshold(sym: str) -> float:
            if sym.startswith("SENSEX"):      return 15.0
            if sym.startswith("BANKNIFTY"):   return 10.0
            if sym.startswith("MIDCPNIFTY"):  return 3.0
            if sym.startswith("FINNIFTY"):    return 3.0
            return 5.0

        query_date = PyDate.fromisoformat(date) if date else PyDate.today()
        today_str  = str(PyDate.today())
        is_today   = (str(query_date) == today_str)
        cache_key  = f"{symbol}_{query_date}"
        cache_ttl  = 60 if is_today else 86400

        # ── Cache check ───────────────────────────────────────────────
        cached = _jump_cache.get(cache_key)
        if cached and (time.monotonic() - cached["ts"]) < cache_ttl:
            return {"jumps": cached["data"], "count": len(cached["data"]), "cached": True}

        price_filter = _price_filter(symbol)
        threshold    = _jump_threshold(symbol)

        pool = request.app.state.pool
        async with pool.acquire() as conn:
            await conn.execute("SET statement_timeout = 0")

            # ── Query 1: jump candidates only (tiny result set) ───────
            # Range predicates on timestamp are sargable with idx_gap_ticks_jump_lookup
            # (symbol, timestamp INCLUDE curr_price, prev_price, price_jump
            #  WHERE curr_price > 0 AND ABS(price_jump) > 3).
            # Partial predicate means only big-jump rows are in the index — tiny scan.
            # Lower bound moved to 09:15:03 to skip only first 2s of opening noise.
            raw_jumps = await conn.fetch("""
                SELECT timestamp, curr_price, prev_price, price_jump
                FROM gap_ticks
                WHERE symbol        = $1
                  AND timestamp    >= $2::date + TIME '09:15:03'
                  AND timestamp    <= $2::date + TIME '15:35:00'
                  AND curr_price    > 0
                  AND curr_price    < $3
                  AND ABS(price_jump) > $4
                ORDER BY timestamp ASC
            """, symbol, query_date, price_filter, threshold)

        if not raw_jumps:
            _jump_cache[cache_key] = {"data": [], "ts": time.monotonic()}
            return {"jumps": [], "count": 0, "cached": False}

        # Build jump list from Query 1 — no fill info yet
        jumps    = []
        is_first = True
        for row in raw_jumps:
            ts   = row["timestamp"]
            pj   = float(row["price_jump"] or 0)
            curr = float(row["curr_price"])
            prev = float(row["prev_price"] or 0)
            epoch  = int(ts.timestamp()) + 19800
            bucket = _math.floor(epoch / 5) * 5
            jumps.append({
                "bucket"       : bucket,
                "timestamp"    : str(ts),
                "direction"    : "UP" if pj > 0 else "DOWN",
                "pre_price"    : prev,
                "post_price"   : curr,
                "jump_pts"     : round(pj, 2),
                "is_first"     : is_first,
                "filled"       : False,
                "filled_bucket": None,
            })
            is_first = False

        # ── Deduplicate by 5-second bucket (keep largest abs jump) ───
        # Multiple ticks in the same 5s window produce the same chartTime
        # and stack as duplicate circles on the same candle.
        seen_buckets: dict = {}
        for j in jumps:
            b = j["bucket"]
            if b not in seen_buckets or abs(j["jump_pts"]) > abs(seen_buckets[b]["jump_pts"]):
                seen_buckets[b] = j
        jumps = list(seen_buckets.values())
        for i, j in enumerate(jumps):
            j["is_first"] = (i == 0)

        # ── Query 2: fill scan — all ticks from first jump onward ─────
        # No price_filter here so fill ticks above price_filter are visible
        # (e.g. SENSEX DOWN jump with pre_price=453 fills at 457).
        # Only fetches ticks from the first jump timestamp forward — not the
        # whole day — keeping the result set manageable.
        first_jump_ts = raw_jumps[0]["timestamp"]
        async with pool.acquire() as conn:
            await conn.execute("SET statement_timeout = 0")
            fill_ticks = await conn.fetch("""
                SELECT timestamp, curr_price
                FROM gap_ticks
                WHERE symbol      = $1
                  AND timestamp  >= $2
                  AND timestamp  <= $3::date + TIME '15:35:00'
                  AND curr_price  > 0
                ORDER BY timestamp ASC
            """, symbol, first_jump_ts, query_date)

        # ── O(N) single-pass fill detection over fill_ticks ──────────
        # pending_fills holds jump dicts (shared refs) awaiting fill.
        # Updating pf in-place propagates to the jumps list automatically.
        pending_fills = list(jumps)   # start with all jumps pending fill

        for ftick in fill_ticks:
            if not pending_fills:
                break   # all fills found early — skip remaining ticks
            ts          = ftick["timestamp"]
            curr        = float(ftick["curr_price"])
            fill_epoch  = int(ts.timestamp()) + 19800
            fill_bucket = _math.floor(fill_epoch / 5) * 5
            still_pending = []
            for pf in pending_fills:
                # Fill tick must be strictly after the jump's own candle —
                # a tick before or at the jump time cannot retroactively fill it
                if fill_bucket <= pf["bucket"]:
                    still_pending.append(pf)
                    continue
                if pf["direction"] == "UP" and curr <= pf["pre_price"]:
                    pf["filled"]        = True
                    pf["filled_bucket"] = fill_bucket
                elif pf["direction"] == "DOWN" and curr >= pf["pre_price"]:
                    pf["filled"]        = True
                    pf["filled_bucket"] = fill_bucket
                else:
                    still_pending.append(pf)
            pending_fills = still_pending

        # ── Write cache ───────────────────────────────────────────────
        _jump_cache[cache_key] = {"data": jumps, "ts": time.monotonic()}

        # Evict entries older than 25 hours (covers max TTL of 86400s with margin)
        cutoff = time.monotonic() - 90000
        for k in [k for k, v in _jump_cache.items() if v["ts"] < cutoff]:
            _jump_cache.pop(k, None)

        return {"jumps": jumps, "count": len(jumps), "cached": False}

    except Exception as e:
        return {"jumps": [], "count": 0, "error": str(e)}


class PrefetchPayload(BaseModel):
    symbols: list = []
    date: str = None

@router.post("/jumps/prefetch")
async def prefetch_jumps(payload: PrefetchPayload, request: Request):
    """Pre-warm jump cache for multiple symbols.
    Semaphore(2) limits concurrent DB computations to prevent CPU/DB overload.
    """
    try:
        import datetime, asyncio

        date_str   = payload.date
        query_date = (
            datetime.date.fromisoformat(date_str)
            if date_str
            else datetime.date.today()
        )

        symbols = list(dict.fromkeys(payload.symbols))[:12]
        if not symbols:
            return {"prefetched": 0}

        # Semaphore limits to 2 concurrent DB queries at a time
        sem = asyncio.Semaphore(2)

        async def compute_one(sym):
            # Check cache first — no DB query needed on hit
            cache_key = f"{sym}_{query_date}"
            cached    = _jump_cache.get(cache_key)
            if cached:
                today_str = str(datetime.date.today())
                ttl = 30 if str(query_date) == today_str else 86400
                if (time.monotonic() - cached["ts"]) < ttl:
                    return  # already warm

            async with sem:
                try:
                    await get_jump_history(
                        sym,
                        request,
                        str(query_date) if date_str else None
                    )
                except Exception:
                    pass

        await asyncio.gather(*[compute_one(s) for s in symbols])
        return {"prefetched": len(symbols)}

    except Exception as e:
        return {"prefetched": 0, "error": str(e)}


_IST_TZ = timezone(timedelta(hours=5, minutes=30))

def _ist_hms() -> str:
    return datetime.now(_IST_TZ).strftime("%H:%M:%S")


@router.get("/chart/{symbol}")
async def get_chart(symbol: str, request: Request, nocache: bool = False, date: str = None):
    """Combined history + gaps endpoint with server-side gap-fill computation.
    For today's data: uses incremental DB scan when cache is stale (only fetches
    new candles since last_bucket, merges with cached data) to avoid O(N) full scan."""
    _t0 = time.monotonic()
    pool = request.app.state.pool
    cache_key = f"CHART:{symbol}:{date}" if date else f"CHART:{symbol}"

    if not nocache:
        cached = _chart_cache.get(cache_key)
        if cached:
            age = time.monotonic() - cached["ts"]
            if age < cached.get("ttl", 600):
                ms = (time.monotonic() - _t0) * 1000
                rows = len(cached["data"].get("history", []))
                logger.info("[CHART_TIMING] ts=%s sym=%s rows=%d ms=%.1f path=cache", _ist_hms(), symbol, rows, ms)
                return Response(content=orjson.dumps(cached["data"]), media_type="application/json")
            # Stale today-cache with a known last_bucket → incremental update
            if not date and cached.get("last_bucket"):
                last_bucket = cached["last_bucket"]

                async with pool.acquire() as conn:
                    new_history = await _query_history(conn, symbol, since_bucket=last_bucket)
                    new_gaps    = await _query_gaps(conn, symbol, since_bucket=last_bucket)

                # Merge candles: new candles overwrite cached ones (last_bucket candle may have grown)
                hist_dict = {c[0]: c for c in cached["data"]["history"]}
                for c in new_history:
                    hist_dict[c[0]] = c
                merged_history = sorted(hist_dict.values(), key=lambda c: c[0])

                # Merge gaps: new gaps overwrite cached ones (fill status can change)
                gaps_dict = {g["time"]: g for g in cached["data"]["gaps"]}
                for g in new_gaps:
                    gaps_dict[g["time"]] = g
                merged_gaps = sorted(gaps_dict.values(), key=lambda g: g["time"])

                _compute_gap_fills(merged_history, merged_gaps)

                data = {"history": merged_history, "gaps": merged_gaps}
                new_last_bucket = merged_history[-1][0] if merged_history else last_bucket
                ttl = 600 + random.randint(0, 60)
                _chart_cache[cache_key] = {"data": data, "ts": time.monotonic(), "ttl": ttl, "last_bucket": new_last_bucket}
                ms = (time.monotonic() - _t0) * 1000
                logger.info("[CHART_TIMING] ts=%s sym=%s rows=%d ms=%.1f path=incr", _ist_hms(), symbol, len(merged_history), ms)
                return Response(content=orjson.dumps(data), media_type="application/json")

    # Full query path: first load, nocache=true, or historical date
    # Also evict jump cache for this symbol so next jumps call recomputes fresh.
    if nocache:
        today_str = str(PyDate.today())
        _jump_cache.pop(f"{symbol}_{today_str}", None)
    # Single connection per chart keeps pool usage to 1 per panel (8 vs 16 for 8 panels).
    async with pool.acquire() as conn:
        history = await _query_history(conn, symbol, date)
        gaps    = await _query_gaps(conn, symbol, date)
    _compute_gap_fills(history, gaps)

    data = {"history": history, "gaps": gaps}
    last_bucket = history[-1][0] if history else None
    ttl = 3600 if date else 600 + random.randint(0, 60)
    _chart_cache[cache_key] = {"data": data, "ts": time.monotonic(), "ttl": ttl, "last_bucket": last_bucket}
    ms = (time.monotonic() - _t0) * 1000
    logger.info("[CHART_TIMING] ts=%s sym=%s rows=%d ms=%.1f path=full", _ist_hms(), symbol, len(history), ms)
    return Response(content=orjson.dumps(data), media_type="application/json")


_hist_symbols_cache: dict = {}  # date_str → list of {symbol, display}

_MONTH_MAP = {m: i for i, m in enumerate(
    ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'], 1
)}
_OPT_RE = re.compile(
    r'^(NIFTY|BANKNIFTY|FINNIFTY|MIDCPNIFTY|SENSEX)'
    r'(\d{2})([A-Z]{3}|\d{1}\d{2})'  # YY + (MON3 monthly | M+DD weekly)
    r'(\d+)'                           # strike
    r'(CE|PE)$'
)

def _parse_symbol_display(sym: str) -> tuple:
    """Parse NSE option symbol into (expiry_date, strike, opt_type, display).
    Returns (PyDate.max, 0, '', sym) for index/unrecognised symbols."""
    m = _OPT_RE.match(sym)
    if not m:
        return (PyDate.max, 0, '', sym)
    _, yy, date_code, strike_s, opt_type = m.groups()
    year = 2000 + int(yy)
    strike = float(strike_s)
    if date_code.isalpha():                          # monthly e.g. MAR
        month = _MONTH_MAP.get(date_code, 1)
        expiry = PyDate(year, month, 28)             # approximate — only used for sort
        exp_label = date_code
    else:                                            # weekly e.g. 407 = Apr 07
        month, day = int(date_code[0]), int(date_code[1:])
        expiry = PyDate(year, month, day)
        exp_label = expiry.strftime("%d %b")
    display = f'{int(strike)} {opt_type}  [exp {exp_label}]'
    return (expiry, strike, opt_type, display)


@router.get("/hist-symbols")
async def get_hist_symbols(date: str, request: Request):
    """Returns all distinct symbols that have candle data for a given IST date (YYYY-MM-DD).
    Uses candles_5s (511 MB) instead of gap_ticks (14 GB) to avoid a full-table-scan timeout.
    Display strings are derived by parsing the NSE symbol name — no gap_ticks query needed.
    Historical data never changes so results are cached indefinitely after first load.
    """
    _t0 = time.monotonic()
    if date in _hist_symbols_cache:
        ms = (time.monotonic() - _t0) * 1000
        logger.info("[HIST_TIMING] ts=%s date=%s rows=%d ms=%.1f path=cache", _ist_hms(), date, len(_hist_symbols_cache[date]), ms)
        return _hist_symbols_cache[date]

    pool = request.app.state.pool
    date_obj = PyDate.fromisoformat(date)
    async with pool.acquire() as conn:
        # Index Only Scan: checks candles_5s_pkey (symbol, bucket) for each tracked_symbols row.
        # Heap Fetches: 0 — purely index-resident. 8× faster than the prior DISTINCT seq scan.
        sym_rows = await conn.fetch("""
            SELECT ts.symbol
            FROM tracked_symbols ts
            WHERE EXISTS (
                SELECT 1 FROM candles_5s c
                WHERE c.symbol = ts.symbol
                  AND c.bucket >= EXTRACT(EPOCH FROM $1::date + TIME '09:15:00')::BIGINT
                  AND c.bucket <= EXTRACT(EPOCH FROM $1::date + TIME '15:35:00')::BIGINT
                LIMIT 1
            )
        """, date_obj)

    entries = []
    for r in sym_rows:
        expiry, strike, opt_type, display = _parse_symbol_display(r["symbol"])
        entries.append((expiry, strike, opt_type, r["symbol"], display))
    entries.sort(key=lambda x: (x[0], x[1], x[2]))
    result = [{"symbol": e[3], "display": e[4]} for e in entries]
    _hist_symbols_cache[date] = result
    ms = (time.monotonic() - _t0) * 1000
    logger.info("[HIST_TIMING] ts=%s date=%s rows=%d ms=%.1f path=full", _ist_hms(), date, len(result), ms)
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
            if (h_cached and now - h_cached["ts"] < h_cached.get("ttl", 600)) and (g_cached and now - g_cached["ts"] < g_cached.get("ttl", 600)):
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
        ttl = 600 + random.randint(0, 60)
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


# ═══════════════════════════════════════════════════════════════
# ARCHIVE MANAGEMENT ENDPOINTS
# ═══════════════════════════════════════════════════════════════

# ── GET /api/archive/config ──────────────────────────────────
@router.get("/archive/config")
async def get_archive_config(request: Request):
    """Get current retention config for all symbol types."""
    try:
        pool = request.app.state.pool
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT symbol_type, retain_days, description,
                       updated_at
                FROM retention_config
                ORDER BY retain_days DESC, symbol_type ASC
            """)
        return {"config": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e), "config": []}


# ── POST /api/archive/config ─────────────────────────────────
@router.post("/archive/config")
async def update_archive_config(request: Request, payload: dict):
    """
    Update retention days for a symbol type.
    Body: {"symbol_type": "NIFTY_OPTIONS", "retain_days": 21}
    """
    try:
        symbol_type = payload.get("symbol_type")
        retain_days = payload.get("retain_days")

        if not symbol_type or not retain_days:
            return {"error": "symbol_type and retain_days required"}

        if not (1 <= int(retain_days) <= 90):
            return {"error": "retain_days must be between 1 and 90"}

        pool = request.app.state.pool
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE retention_config
                SET retain_days = $2,
                    updated_at  = NOW()
                WHERE symbol_type = $1
            """, symbol_type, int(retain_days))

        return {
            "success"    : True,
            "symbol_type": symbol_type,
            "retain_days": retain_days,
        }
    except Exception as e:
        return {"error": str(e)}


# ── GET /api/archive/status ──────────────────────────────────
@router.get("/archive/status")
async def get_archive_status(request: Request):
    """
    Returns storage stats — PostgreSQL sizes,
    disk usage, archive log summary.
    """
    try:
        pool = request.app.state.pool
        async with pool.acquire() as conn:
            await conn.execute("SET statement_timeout = 0")

            sizes = await conn.fetch("""
                SELECT
                    tablename AS table_name,
                    pg_size_pretty(pg_total_relation_size(
                        'public.'||tablename)) AS size,
                    pg_total_relation_size(
                        'public.'||tablename) AS size_bytes
                FROM pg_tables
                WHERE schemaname = 'public'
                  AND tablename IN (
                    'gap_ticks','candles_5s',
                    'gap_events','conviction_signals'
                  )
                ORDER BY size_bytes DESC
            """)

            # Use index-friendly queries instead of full-table COUNT/MIN/MAX
            date_range = await conn.fetchrow("""
                SELECT
                    (SELECT timestamp FROM gap_ticks
                     ORDER BY timestamp ASC  LIMIT 1)::date AS oldest_date,
                    (SELECT timestamp FROM gap_ticks
                     ORDER BY timestamp DESC LIMIT 1)::date AS newest_date,
                    (SELECT reltuples::bigint FROM pg_class
                     WHERE relname = 'gap_ticks')           AS total_rows
            """)

            archive_summary = await conn.fetch("""
                SELECT
                    archive_date,
                    COUNT(*)             AS tables_archived,
                    SUM(rows_archived)   AS total_rows,
                    SUM(file_size_bytes) AS total_bytes,
                    MAX(completed_at)    AS completed_at
                FROM archive_log
                WHERE status = 'completed'
                GROUP BY archive_date
                ORDER BY archive_date DESC
                LIMIT 30
            """)

            dead_tuples = await conn.fetch("""
                SELECT relname, n_dead_tup
                FROM pg_stat_user_tables
                WHERE relname IN (
                    'gap_ticks','candles_5s','gap_events'
                )
                ORDER BY n_dead_tup DESC
            """)

        disk       = shutil.disk_usage('/')
        disk_pct   = round(disk.used / disk.total * 100, 1)
        disk_free  = round(disk.free  / 1024**3, 1)
        disk_total = round(disk.total / 1024**3, 1)
        disk_used  = round(disk.used  / 1024**3, 1)

        return {
            "postgresql": {
                "tables"      : [dict(r) for r in sizes],
                "oldest_date" : str(date_range["oldest_date"])
                                if date_range["oldest_date"] else None,
                "newest_date" : str(date_range["newest_date"])
                                if date_range["newest_date"] else None,
                "total_rows"  : date_range["total_rows"],
                "dead_tuples" : [dict(r) for r in dead_tuples],
            },
            "disk": {
                "used_gb" : disk_used,
                "free_gb" : disk_free,
                "total_gb": disk_total,
                "pct_used": disk_pct,
            },
            "archive_log": [
                {
                    "date"        : str(r["archive_date"]),
                    "tables"      : r["tables_archived"],
                    "rows"        : r["total_rows"],
                    "size_mb"     : round(
                        (r["total_bytes"] or 0) / 1024 / 1024, 1
                    ),
                    "completed_at": str(r["completed_at"]),
                }
                for r in archive_summary
            ],
        }
    except Exception as e:
        return {"error": str(e)}


# In-memory cache for B2 manifest (avoids 2-min S3 round-trip on every open)
# In-memory cache for B2 manifest (avoids 2-min S3 round-trip on every open)
_b2_cache: dict = {"data": None, "ts": 0.0}
_B2_CACHE_TTL = 300  # seconds (5 minutes)


async def refresh_b2_cache(pool):
    """Fetch B2 manifest + per-date existence check; store in _b2_cache.
    Called by background task in chart_server.py every 5 minutes."""
    import time as _time, boto3
    from botocore.client import Config
    from pathlib import Path
    from datetime import date as _date, timedelta
    global _b2_cache

    secrets = {}
    for line in (Path.home() / '.kite_secrets').read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            k, _, v = line.partition('=')
            secrets[k.strip()] = v.strip()

    # Blocking boto3 call (fast: manifest.json is small)
    s3 = boto3.client(
        's3',
        endpoint_url          = f"https://{secrets['BACKBLAZE_ENDPOINT']}",
        aws_access_key_id     = secrets['BACKBLAZE_KEY_ID'],
        aws_secret_access_key = secrets['BACKBLAZE_APP_KEY'],
        config                = Config(signature_version='s3v4', connect_timeout=15, read_timeout=30),
    )
    obj      = s3.get_object(Bucket=secrets['BACKBLAZE_BUCKET'], Key='manifest.json')
    manifest = json.loads(obj['Body'].read())

    # Per-date existence check via Index Only Scan (ORDER BY ts LIMIT 1 = instant)
    dates_in_b2 = sorted(manifest.keys())
    db_exists: dict = {}
    if dates_in_b2:
        async with pool.acquire() as conn:
            await conn.execute("SET statement_timeout = 0")
            for d_str in dates_in_b2:
                d     = _date.fromisoformat(d_str)
                d_end = d + timedelta(days=1)
                row   = await conn.fetchrow(
                    "SELECT timestamp FROM gap_ticks"
                    " WHERE timestamp >= $1::timestamp AND timestamp < $2::timestamp"
                    " ORDER BY timestamp LIMIT 1",
                    d, d_end)
                db_exists[d_str] = row is not None

    dates = []
    for d in sorted(manifest.keys(), reverse=True):
        tables      = manifest[d]
        total_rows  = sum(t.get('rows', 0) for t in tables.values())
        total_size  = sum(t.get('size_bytes', 0) for t in tables.values())
        in_db       = db_exists.get(d, False)
        # Latest archived_at across all tables for this date
        archived_at = max(
            (t.get('archived_at', '') for t in tables.values()),
            default=''
        )
        dates.append({
            "date"        : d,
            "tables"      : list(tables.keys()),
            "total_rows"  : total_rows,
            "size_mb"     : round(total_size / 1024 / 1024, 1),
            "in_db_rows"  : total_rows if in_db else 0,
            "in_db"       : in_db,
            "archived_at" : archived_at,
        })

    result = {"dates": dates}
    _b2_cache["data"] = result
    _b2_cache["ts"]   = _time.monotonic()
    print("[B2 cache] refreshed: " + str(len(dates)) + " dates", flush=True)

# ── GET /api/archive/b2 ─────────────────
@router.get("/archive/b2")
async def get_b2_manifest(request: Request):
    """Returns cached B2 manifest. Cache is populated by background task every 5 min."""
    if _b2_cache["data"] is not None:
        return _b2_cache["data"]
    return {"dates": [], "loading": True}


# ── GET /api/archive/log ──────────────────────────────────────
@router.get("/archive/log")
async def get_archive_log(request: Request, date: str = None):
    """Returns archive_log entries for live polling.
    Used by UI to show archive progress."""
    try:
        from collections import defaultdict
        from datetime import date as _date
        pool = request.app.state.pool
        async with pool.acquire() as conn:
            if date:
                rows = await conn.fetch("""
                    SELECT
                        archive_date,
                        table_name,
                        rows_archived,
                        file_size_bytes,
                        status,
                        error_message,
                        started_at,
                        completed_at
                    FROM archive_log
                    WHERE archive_date = $1
                    ORDER BY table_name ASC
                """, _date.fromisoformat(date))
            else:
                rows = await conn.fetch("""
                    SELECT
                        archive_date,
                        table_name,
                        rows_archived,
                        file_size_bytes,
                        status,
                        error_message,
                        started_at,
                        completed_at
                    FROM archive_log
                    ORDER BY started_at DESC
                    LIMIT 30
                """)

        by_date = defaultdict(list)
        for r in rows:
            by_date[str(r["archive_date"])].append({
                "table"       : r["table_name"],
                "rows"        : r["rows_archived"] or 0,
                "size_mb"     : round((r["file_size_bytes"] or 0) / 1024 / 1024, 1),
                "status"      : r["status"],
                "error"       : r["error_message"],
                "started_at"  : str(r["started_at"]) if r["started_at"] else None,
                "completed_at": str(r["completed_at"]) if r["completed_at"] else None,
            })

        archives = []
        for d, entries in sorted(by_date.items(), reverse=True):
            total_rows = sum(e["rows"] for e in entries)
            total_mb   = sum(e["size_mb"] for e in entries)
            completed  = sum(1 for e in entries if e["status"] == "completed")
            failed     = sum(1 for e in entries if e["status"] == "failed")
            running    = sum(1 for e in entries if e["status"] in ("pending", "running"))
            skipped    = sum(1 for e in entries if e["status"] == "skipped")
            overall    = (
                "failed"    if failed   > 0 else
                "running"   if running  > 0 else
                "completed" if completed > 0 else
                "skipped"
            )
            archives.append({
                "date"           : d,
                "total_rows"     : total_rows,
                "total_mb"       : round(total_mb, 1),
                "tables_done"    : completed,
                "tables_failed"  : failed,
                "tables_running" : running,
                "tables_skipped" : skipped,
                "tables_total"   : len(entries),
                "status"         : overall,
                "entries"        : entries,
            })

        return {"archives": archives}
    except Exception as e:
        return {"archives": [], "error": str(e)}


@router.post("/archive/run")
async def run_archive_now(payload: dict = {}):
    """
    Trigger archive job manually. Runs as background process.
    Body: {"date": "2026-03-15"} (optional)
    """
    try:
        import sys
        from pathlib import Path

        project_dir = Path(__file__).parent.parent.parent
        script      = project_dir / 'scripts' / 'archive_to_b2.py'
        python      = project_dir / 'venv' / 'bin' / 'python3'

        if not python.exists():
            python = Path(sys.executable)

        cmd = [str(python), str(script)]

        # Accept either a list of dates {"dates": [...]} or a single
        # date {"date": "..."} for backward compat. Always launch ONE
        # process so VACUUM runs once at the end, not N times in parallel.
        dates = payload.get("dates") or (
            [payload["date"]] if payload.get("date") else []
        )
        if dates:
            cmd += ['--dates'] + dates
        if payload.get("force"):
            cmd += ['--force']

        subprocess.Popen(
            cmd,
            cwd    = str(project_dir),
            stdout = open(Path.home() / 'archive_b2.log', 'a'),
            stderr = subprocess.STDOUT,
        )

        return {
            "success" : True,
            "message" : "Archive started in background",
            "log_file": "~/archive_b2.log",
            "dates"   : dates or ["auto"],
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── POST /api/restore ────────────────────────────────────────
@router.post("/restore")
async def restore_from_b2(payload: dict):
    """
    Restore archived data from B2 to test tables.
    Body: {"date": "2026-03-16", "permanent": false}
    """
    try:
        import sys
        from pathlib import Path

        restore_date = payload.get("date")
        permanent    = payload.get("permanent", False)

        if not restore_date:
            return {"error": "date is required"}

        project_dir = Path(__file__).parent.parent.parent
        script      = project_dir / 'scripts' / 'restore_from_b2.py'
        python      = project_dir / 'venv' / 'bin' / 'python3'

        if not python.exists():
            python = Path(sys.executable)

        cmd = [str(python), str(script), '--date', restore_date]
        if permanent:
            cmd.append('--permanent')

        subprocess.Popen(
            cmd,
            cwd    = str(project_dir),
            stdout = open(Path.home() / 'restore_b2.log', 'a'),
            stderr = subprocess.STDOUT,
        )

        # Invalidate B2 cache so DB Rows column updates after restore completes
        global _b2_cache
        _b2_cache["data"] = None
        _b2_cache["ts"]   = 0.0

        return {
            "success" : True,
            "message" : "Restore started in background",
            "date"    : restore_date,
            "mode"    : "permanent" if permanent else "test",
            "log_file": "~/restore_b2.log",
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── GET /api/restore/status ──────────────────────────────────
@router.get("/restore/status")
async def get_restore_status(request: Request):
    """Returns list of currently restored test tables."""
    try:
        pool = request.app.state.pool
        async with pool.acquire() as conn:

            test_tables = await conn.fetch("""
                SELECT
                    table_name,
                    pg_size_pretty(pg_total_relation_size(
                        'public.'||table_name)) AS size
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name LIKE '%_test_%'
                ORDER BY table_name
            """)

            restore_log = await conn.fetch("""
                SELECT
                    restore_date,
                    table_name,
                    restored_table,
                    rows_restored,
                    status,
                    completed_at
                FROM restore_log
                ORDER BY started_at DESC
                LIMIT 20
            """)

        return {
            "test_tables": [dict(r) for r in test_tables],
            "restore_log": [
                {
                    "date"        : str(r["restore_date"]),
                    "table"       : r["table_name"],
                    "restored_as" : r["restored_table"],
                    "rows"        : r["rows_restored"],
                    "status"      : r["status"],
                    "completed_at": str(r["completed_at"]),
                }
                for r in restore_log
            ],
        }
    except Exception as e:
        return {"error": str(e)}


# ── GET /api/restore/log ─────────────────────────────────────
@router.get("/restore/log")
async def get_restore_log(date: str = None, request: Request = None):
    """
    Returns restore_log entries for a specific date or all recent entries.
    Used by UI to poll restore progress.
    """
    try:
        from collections import defaultdict
        from datetime import date as _date
        pool = request.app.state.pool
        async with pool.acquire() as conn:
            if date:
                rows = await conn.fetch("""
                    SELECT restore_date, table_name, restored_table,
                           rows_restored, status, error_message,
                           session_id, started_at, completed_at
                    FROM restore_log
                    WHERE restore_date = $1
                    ORDER BY started_at DESC
                """, _date.fromisoformat(date))
            else:
                rows = await conn.fetch("""
                    SELECT restore_date, table_name, restored_table,
                           rows_restored, status, error_message,
                           session_id, started_at, completed_at
                    FROM restore_log
                    ORDER BY started_at DESC
                    LIMIT 30
                """)

        entries = []
        for r in rows:
            entries.append({
                "date"        : str(r["restore_date"]),
                "table"       : r["table_name"],
                "restored_as" : r["restored_table"],
                "rows"        : r["rows_restored"] or 0,
                "status"      : r["status"],
                "error"       : r["error_message"],
                "session_id"  : r["session_id"] if "session_id" in r.keys() else None,
                "started_at"  : r["started_at"].isoformat() if r["started_at"] else None,
                "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
            })

        # Group by date + session_id so multiple restores of same date show separately
        by_session = defaultdict(list)
        for e in entries:
            key = f"{e['date']}_{e.get('session_id') or ''}"
            by_session[key].append(e)

        summary = []
        for key, session_entries in sorted(by_session.items(), reverse=True):
            d          = session_entries[0]["date"]
            session_id = session_entries[0].get("session_id", "")
            total_rows = sum(e["rows"] for e in session_entries)
            completed  = sum(1 for e in session_entries if e["status"] == "completed")
            failed     = sum(1 for e in session_entries if e["status"] == "failed")
            running    = sum(1 for e in session_entries if e["status"] in ("pending", "running"))
            overall    = (
                "failed"    if failed   > 0
                else "running"   if running  > 0
                else "completed" if completed > 0
                else "pending"
            )
            summary.append({
                "date"          : d,
                "session_id"    : session_id,
                "total_rows"    : total_rows,
                "tables_done"   : completed,
                "tables_failed" : failed,
                "tables_running": running,
                "tables_total"  : len(session_entries),
                "status"        : overall,
                "entries"       : session_entries,
            })

        return {"restores": summary}
    except Exception as e:
        return {"restores": [], "error": str(e)}


# ── DELETE /api/restore/{date} ───────────────────────────────
@router.delete("/restore/{date}")
async def cleanup_restore(date: str):
    """Remove test tables for a specific date."""
    try:
        import sys
        from pathlib import Path

        project_dir = Path(__file__).parent.parent.parent
        script      = project_dir / 'scripts' / 'restore_from_b2.py'
        python      = project_dir / 'venv' / 'bin' / 'python3'

        if not python.exists():
            python = Path(sys.executable)

        result = subprocess.run(
            [str(python), str(script), '--cleanup', date],
            cwd            = str(project_dir),
            capture_output = True,
            text           = True,
        )

        return {
            "success": result.returncode == 0,
            "date"   : date,
            "output" : result.stdout,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── GET /api/archive/eligible ────────────────────────────────
@router.get("/archive/eligible")
async def get_eligible_dates(request: Request):
    """
    Returns dates currently in PostgreSQL that are past their
    retention period and have NOT been archived yet.
    """
    try:
        from datetime import date as _date, timedelta
        pool = request.app.state.pool
        async with pool.acquire() as conn:
            await conn.execute("SET statement_timeout = 0")

            config_rows = await conn.fetch(
                "SELECT symbol_type, retain_days FROM retention_config"
            )
            config = {r['symbol_type']: r['retain_days'] for r in config_rows}
            min_retain = min(config.values())
            earliest   = _date.today() - timedelta(days=min_retain)

            # Fast approach: use index ORDER BY LIMIT 1 to find date bounds,
            # then enumerate dates and count per-day with range scans.
            # Avoids scanning millions of rows when old data has been restored.
            oldest_ts = await conn.fetchval(
                "SELECT timestamp FROM gap_ticks WHERE timestamp < $1::timestamp ORDER BY timestamp ASC LIMIT 1",
                earliest
            )
            if not oldest_ts:
                return {"eligible": []}

            newest_ts = await conn.fetchval(
                "SELECT timestamp FROM gap_ticks WHERE timestamp < $1::timestamp ORDER BY timestamp DESC LIMIT 1",
                earliest
            )

            # Enumerate all dates in range — show ALL dates with data
            # past retention, regardless of archive_log status.
            # This ensures restored dates reappear as eligible.
            d     = oldest_ts.date()
            end_d = newest_ts.date()
            eligible_dates = []
            while d <= end_d:
                eligible_dates.append(d)
                d += timedelta(days=1)

            # Use ORDER BY timestamp LIMIT 1 — forces index scan (<1ms).
            # Plain EXISTS/COUNT without ORDER BY picks seq scan on 19GB.
            rows = []
            for d in eligible_dates:
                hit = await conn.fetchval(
                    "SELECT 1 FROM gap_ticks WHERE timestamp >= $1::timestamp AND timestamp < $2::timestamp ORDER BY timestamp ASC LIMIT 1",
                    d, d + timedelta(days=1)
                )
                if hit is not None:
                    rows.append({'tick_date': d, 'row_count': None})

        return {
            "eligible": [
                {"date": str(r["tick_date"]), "rows": r["row_count"]}
                for r in rows
            ]
        }
    except Exception as e:
        return {"eligible": [], "error": str(e)}