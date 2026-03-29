# backend/api/strikes.py

import time
import random
import asyncio
import bisect
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

@router.get("/index-ltp")
async def get_index_ltp(request: Request):
    """Returns live LTP + change from prev close for all tracked indices."""
    now = time.monotonic()
    cached = _ltp_cache.get("data")
    if cached and (now - _ltp_cache["ts"]) < _ltp_cache.get("ttl", LTP_CACHE_TTL):
        return cached

    # Run _kite.quote() in a thread so it never blocks the asyncio event loop.
    # 2-second timeout: if Kite is unreachable (weekend/market closed) we fail
    # fast instead of stalling for 10+ seconds.
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
            _ltp_cache["data"] = result
            _ltp_cache["ts"]   = now
            _ltp_cache["ttl"]  = LTP_CACHE_TTL   # live data: 2s TTL
            return result
    except Exception:
        pass

    # Fallback: read latest prices from candles_5s (fast index scan, no heap fetches)
    try:
        pool = request.app.state.pool
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT c.symbol, c.close AS curr_price
                FROM candles_5s c
                JOIN (
                    SELECT symbol, MAX(bucket) AS max_bucket
                    FROM candles_5s
                    WHERE symbol IN ('NIFTY', 'SENSEX', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY')
                      AND bucket >= EXTRACT(EPOCH FROM NOW() - INTERVAL '5 days')::BIGINT
                    GROUP BY symbol
                ) latest ON c.symbol = latest.symbol AND c.bucket = latest.max_bucket
            """)
        db_prices = {r["symbol"]: r["curr_price"] for r in rows}
        result = {
            "NIFTY":      {"ltp": db_prices.get("NIFTY"),      "change": None, "change_pct": None},
            "SENSEX":     {"ltp": db_prices.get("SENSEX"),     "change": None, "change_pct": None},
            "BANKNIFTY":  {"ltp": db_prices.get("BANKNIFTY"),  "change": None, "change_pct": None},
            "FINNIFTY":   {"ltp": db_prices.get("FINNIFTY"),   "change": None, "change_pct": None},
            "MIDCPNIFTY": {"ltp": db_prices.get("MIDCPNIFTY"), "change": None, "change_pct": None},
        }
        _ltp_cache["data"] = result
        _ltp_cache["ts"]   = now
        _ltp_cache["ttl"]  = 60   # DB fallback (market closed): cache for 60s
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


@router.get("/chart/{symbol}")
async def get_chart(symbol: str, request: Request, nocache: bool = False, date: str = None):
    """Combined history + gaps endpoint with server-side gap-fill computation.
    For today's data: uses incremental DB scan when cache is stale (only fetches
    new candles since last_bucket, merges with cached data) to avoid O(N) full scan."""
    pool = request.app.state.pool
    cache_key = f"CHART:{symbol}:{date}" if date else f"CHART:{symbol}"

    if not nocache:
        cached = _chart_cache.get(cache_key)
        if cached:
            age = time.monotonic() - cached["ts"]
            if age < cached.get("ttl", 600):
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
                return Response(content=orjson.dumps(data), media_type="application/json")

    # Full query path: first load, nocache=true, or historical date
    # Single connection per chart keeps pool usage to 1 per panel (8 vs 16 for 8 panels).
    async with pool.acquire() as conn:
        history = await _query_history(conn, symbol, date)
        gaps    = await _query_gaps(conn, symbol, date)
    _compute_gap_fills(history, gaps)

    data = {"history": history, "gaps": gaps}
    last_bucket = history[-1][0] if history else None
    ttl = 3600 if date else 600 + random.randint(0, 60)
    _chart_cache[cache_key] = {"data": data, "ts": time.monotonic(), "ttl": ttl, "last_bucket": last_bucket}
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