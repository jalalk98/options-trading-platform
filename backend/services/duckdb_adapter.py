"""
DuckDB-based historical data adapter.

Reads parquet files from LOCAL_PARQUET_PATH instead of PostgreSQL.
Used on the laptop dev environment (DATA_SOURCE=duckdb in .env).

Return shapes are BYTE-IDENTICAL to the PostgreSQL asyncpg versions
in backend/api/strikes.py so no caller changes are needed.

Supports:
  - query_history       -> candles (5s OHLC)
  - query_gaps          -> gap events (text D/U markers)
  - query_jumps         -> jump markers with fill detection (circles)
  - query_hist_symbols  -> distinct symbol list for a date
  - last_trading_date   -> discover most recent date on disk
"""
import asyncio
import calendar
import threading
from datetime import date as PyDate, datetime, timedelta
from pathlib import Path
from typing import Optional

import duckdb

from config.credentials import LOCAL_PARQUET_PATH


# Paths
_PARQUET_ROOT = Path(LOCAL_PARQUET_PATH)
_CANDLES_ROOT = _PARQUET_ROOT / 'candles_5s'
_GAPS_ROOT    = _PARQUET_ROOT / 'gap_events'
_TICKS_ROOT   = _PARQUET_ROOT / 'raw_ticks'
_JOURNAL_PATH = _PARQUET_ROOT / 'trade_journal' / 'latest.parquet'


# Thread-local DuckDB connections: each thread gets its own connection so that
# concurrent asyncio.to_thread calls (parallel panel loads) don't contend on a
# single connection and return empty results.
_thread_local = threading.local()

def _get_db() -> duckdb.DuckDBPyConnection:
    if not hasattr(_thread_local, 'db'):
        _thread_local.db = duckdb.connect(':memory:')
    return _thread_local.db


# Jump detection thresholds (match production constants)
_JUMP_CONFIG = {
    'SENSEX':     {'price_filter': 600.0, 'threshold': 15.0},
    'BANKNIFTY':  {'price_filter': 400.0, 'threshold': 10.0},
    'MIDCPNIFTY': {'price_filter': 120.0, 'threshold': 3.0},
    'FINNIFTY':   {'price_filter': 300.0, 'threshold': 3.0},
    'NIFTY':      {'price_filter': 300.0, 'threshold': 5.0},
}

_IST_OFFSET = 5 * 3600 + 30 * 60  # +19800 seconds


# Helpers
def _date_path(root: Path, d: PyDate) -> Path:
    return root / f'year={d.year}' / f'month={d.month:02d}' / f'day={d.day:02d}' / 'data.parquet'


def _raw_ticks_path(symbol_group: str, d: PyDate) -> Path:
    return (_TICKS_ROOT / f'symbol_group={symbol_group}'
            / f'year={d.year}' / f'month={d.month:02d}' / f'day={d.day:02d}'
            / 'data.parquet')


def _symbol_to_group(symbol: str) -> str:
    if symbol.startswith('BANKNIFTY'):  return 'BANKNIFTY'
    if symbol.startswith('MIDCPNIFTY'): return 'MIDCPNIFTY'
    if symbol.startswith('FINNIFTY'):   return 'FINNIFTY'
    if symbol.startswith('NIFTY'):      return 'NIFTY'
    if symbol.startswith('SENSEX'):     return 'SENSEX'
    return 'OTHER'


def _jump_config_for(symbol: str) -> dict:
    if symbol.startswith('SENSEX'):     return _JUMP_CONFIG['SENSEX']
    if symbol.startswith('BANKNIFTY'):  return _JUMP_CONFIG['BANKNIFTY']
    if symbol.startswith('MIDCPNIFTY'): return _JUMP_CONFIG['MIDCPNIFTY']
    if symbol.startswith('FINNIFTY'):   return _JUMP_CONFIG['FINNIFTY']
    return _JUMP_CONFIG['NIFTY']


def _day_epoch_range(d: PyDate) -> tuple[int, int]:
    """
    Candles window 09:15:00 to 16:00:00 as naive epoch.
    Matches Postgres EXTRACT(EPOCH FROM $2::date + TIME ...).
    """
    start_dt = datetime(d.year, d.month, d.day, 9, 15, 0)
    end_dt = datetime(d.year, d.month, d.day, 16, 0, 0)
    return calendar.timegm(start_dt.timetuple()), calendar.timegm(end_dt.timetuple())


def _jumps_time_range(d: PyDate) -> tuple[datetime, datetime]:
    """Jumps window 09:15:03 to 15:35:00 (matches production)."""
    return (datetime(d.year, d.month, d.day, 9, 15, 3),
            datetime(d.year, d.month, d.day, 15, 35, 0))


# Candles (sync)
def _sync_query_candles(symbol: str, d: PyDate) -> list:
    path = _date_path(_CANDLES_ROOT, d)
    if not path.exists():
        return []

    start_ep, end_ep = _day_epoch_range(d)
    result = _get_db().execute("""
        SELECT bucket, open, high, low, close
        FROM read_parquet(?)
        WHERE symbol = ?
          AND bucket >= ?
          AND bucket <= ?
        ORDER BY bucket ASC
    """, [str(path), symbol, start_ep, end_ep]).fetchall()

    return [[int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4])]
            for r in result]


# Gap events (sync)
def _sync_query_gaps(symbol: str, d: PyDate) -> Optional[list]:
    if symbol == "SENSEX":
        return None

    path = _date_path(_GAPS_ROOT, d)
    if not path.exists():
        return []

    start_ep, end_ep = _day_epoch_range(d)
    result = _get_db().execute("""
        SELECT bucket, direction, prev_price, curr_price, vol_change
        FROM read_parquet(?)
        WHERE symbol = ?
          AND bucket >= ?
          AND bucket <= ?
        ORDER BY bucket ASC
    """, [str(path), symbol, start_ep, end_ep]).fetchall()

    return [{
        "time": int(r[0]),
        "direction": r[1],
        "prev_price": float(r[2]) if r[2] is not None else 0.0,
        "curr_price": float(r[3]) if r[3] is not None else 0.0,
        "vol_change": int(r[4]) if r[4] is not None else 0,
    } for r in result]


# Jumps -- computed from raw_ticks parquet
def _sync_query_jumps(symbol: str, d: PyDate) -> list:
    """
    Compute jump markers with fill detection from raw_ticks parquet.
    Returns same shape as production /api/jumps endpoint.
    """
    group = _symbol_to_group(symbol)
    path = _raw_ticks_path(group, d)
    if not path.exists():
        return []

    cfg = _jump_config_for(symbol)
    price_filter = cfg['price_filter']
    threshold = cfg['threshold']
    time_start, time_end = _jumps_time_range(d)

    # Query 1: jump candidates via LAG() window function
    jumps_raw = _get_db().execute("""
        WITH ticks_with_prev AS (
            SELECT
                timestamp,
                curr_price,
                LAG(curr_price) OVER (
                    PARTITION BY symbol ORDER BY timestamp
                ) AS prev_price
            FROM read_parquet(?)
            WHERE symbol = ?
              AND timestamp >= ?
              AND timestamp <= ?
        )
        SELECT timestamp, curr_price, prev_price
        FROM ticks_with_prev
        WHERE prev_price IS NOT NULL
          AND curr_price > 0
          AND curr_price < ?
          AND ABS(curr_price - prev_price) > ?
        ORDER BY timestamp ASC
    """, [str(path), symbol, time_start, time_end,
          price_filter, threshold]).fetchall()

    if not jumps_raw:
        return []

    # Build jump records; bucket = floor(ist_naive_epoch / 5) * 5  (matches production Postgres)
    jumps = []
    for ts, curr, prev in jumps_raw:
        epoch = calendar.timegm(ts.timetuple())
        bucket = (epoch // 5) * 5  # parquet ts is already IST-naive
        jumps.append({
            'bucket': int(bucket),
            'timestamp': ts.isoformat(sep=' '),
            'direction': 'UP' if curr > prev else 'DOWN',
            'pre_price': float(prev),
            'post_price': float(curr),
            'jump_pts': round(float(curr - prev), 2),
        })

    # Deduplicate by bucket -- keep largest abs(jump_pts) per 5s window
    by_bucket: dict = {}
    for j in jumps:
        b = j['bucket']
        if b not in by_bucket or abs(j['jump_pts']) > abs(by_bucket[b]['jump_pts']):
            by_bucket[b] = j
    dedup_jumps = sorted(by_bucket.values(), key=lambda x: x['bucket'])

    # Mark is_first (first jump overall, matching production behaviour)
    for i, j in enumerate(dedup_jumps):
        j['is_first'] = (i == 0)

    # Fill detection via candles_5s (reliable even on days where raw_ticks is sparse).
    # candles_5s high/low capture intra-candle extremes; raw_ticks on Apr 21/22/24
    # was missing ~65-80% of ticks, causing fills to be silently missed.
    candles_path = _date_path(_CANDLES_ROOT, d)
    end_bucket = calendar.timegm(time_end.timetuple())

    for j in dedup_jumps:
        j['filled'] = False
        j['filled_bucket'] = None

    if candles_path.exists():
        for j in dedup_jumps:
            if j['direction'] == 'UP':
                # UP filled when any subsequent candle LOW <= pre_price
                row = _get_db().execute("""
                    SELECT bucket FROM read_parquet(?)
                    WHERE symbol = ?
                      AND bucket > ?
                      AND bucket <= ?
                      AND low <= ?
                    ORDER BY bucket ASC
                    LIMIT 1
                """, [str(candles_path), symbol,
                      j['bucket'], end_bucket, j['pre_price']]).fetchone()
            else:
                # DOWN filled when any subsequent candle HIGH >= pre_price
                row = _get_db().execute("""
                    SELECT bucket FROM read_parquet(?)
                    WHERE symbol = ?
                      AND bucket > ?
                      AND bucket <= ?
                      AND high >= ?
                    ORDER BY bucket ASC
                    LIMIT 1
                """, [str(candles_path), symbol,
                      j['bucket'], end_bucket, j['pre_price']]).fetchone()
            if row:
                j['filled'] = True
                j['filled_bucket'] = int(row[0])

    return dedup_jumps


# List distinct symbols in candles_5s for a date
def _sync_query_hist_symbols(d: PyDate) -> list:
    """List DISTINCT symbols in candles_5s for a given date."""
    path = _date_path(_CANDLES_ROOT, d)
    if not path.exists():
        return []
    # Production uses 09:15:00 to 15:35:00 for hist-symbols window
    start_ep = calendar.timegm(datetime(d.year, d.month, d.day, 9, 15, 0).timetuple())
    end_ep = calendar.timegm(datetime(d.year, d.month, d.day, 15, 35, 0).timetuple())
    result = _get_db().execute("""
        SELECT DISTINCT symbol FROM read_parquet(?)
        WHERE bucket >= ? AND bucket <= ?
    """, [str(path), start_ep, end_ep]).fetchall()
    return [r[0] for r in result]


# Last available trading date
def _sync_last_trading_date(symbol: str) -> Optional[PyDate]:
    if not _CANDLES_ROOT.exists():
        return None

    dates = []
    for year_dir in _CANDLES_ROOT.iterdir():
        if not year_dir.name.startswith('year='):
            continue
        year = int(year_dir.name.split('=')[1])
        for month_dir in year_dir.iterdir():
            if not month_dir.name.startswith('month='):
                continue
            month = int(month_dir.name.split('=')[1])
            for day_dir in month_dir.iterdir():
                if not day_dir.name.startswith('day='):
                    continue
                day = int(day_dir.name.split('=')[1])
                dates.append(PyDate(year, month, day))

    if not dates:
        return None

    for d in sorted(dates, reverse=True):
        path = _date_path(_CANDLES_ROOT, d)
        if not path.exists():
            continue
        row = _get_db().execute("""
            SELECT COUNT(*) FROM read_parquet(?) WHERE symbol = ? LIMIT 1
        """, [str(path), symbol]).fetchone()
        if row and row[0] > 0:
            return d
    return None


# Public async APIs
async def query_history(symbol: str,
                        date_str: Optional[str] = None,
                        since_bucket: Optional[int] = None) -> list:
    """Drop-in DuckDB replacement for _query_history_fast."""
    if date_str:
        return await asyncio.to_thread(_sync_query_candles, symbol, PyDate.fromisoformat(date_str))

    today = PyDate.today()
    rows = await asyncio.to_thread(_sync_query_candles, symbol, today)
    if rows:
        return rows

    last = await asyncio.to_thread(_sync_last_trading_date, symbol)
    if last:
        return await asyncio.to_thread(_sync_query_candles, symbol, last)
    return []


async def query_gaps(symbol: str,
                     date_str: Optional[str] = None,
                     since_bucket: Optional[int] = None):
    """Drop-in DuckDB replacement for _query_gaps_fast."""
    if symbol == "SENSEX":
        return None

    if date_str:
        return await asyncio.to_thread(_sync_query_gaps, symbol, PyDate.fromisoformat(date_str))

    today = PyDate.today()
    rows = await asyncio.to_thread(_sync_query_gaps, symbol, today)
    if rows is None:
        return None
    if rows:
        return rows

    last = await asyncio.to_thread(_sync_last_trading_date, symbol)
    if last:
        return await asyncio.to_thread(_sync_query_gaps, symbol, last)
    return []


async def query_jumps(symbol: str, date_str: Optional[str] = None) -> list:
    """Drop-in DuckDB replacement for production /api/jumps endpoint."""
    if date_str:
        return await asyncio.to_thread(_sync_query_jumps, symbol, PyDate.fromisoformat(date_str))

    today = PyDate.today()
    jumps = await asyncio.to_thread(_sync_query_jumps, symbol, today)
    if jumps:
        return jumps

    last = await asyncio.to_thread(_sync_last_trading_date, symbol)
    if last:
        return await asyncio.to_thread(_sync_query_jumps, symbol, last)
    return []


async def query_hist_symbols(date_str: str) -> list:
    """Drop-in DuckDB replacement for get_hist_symbols -- returns distinct symbol list for a date."""
    d = PyDate.fromisoformat(date_str)
    return await asyncio.to_thread(_sync_query_hist_symbols, d)


async def last_trading_date(symbol: str) -> Optional[PyDate]:
    """Drop-in DuckDB replacement for _get_last_trading_date."""
    return await asyncio.to_thread(_sync_last_trading_date, symbol)


# Raw ticks for tick-level replay
def _sync_query_ticks(symbol: str, d: PyDate) -> list:
    """Return all raw ticks for symbol/date as [{t: epoch_sec, p: price}]."""
    group = _symbol_to_group(symbol)
    path = _raw_ticks_path(group, d)
    if not path.exists():
        return []

    time_start = datetime(d.year, d.month, d.day, 9, 15, 0)   # full open, not 09:15:03
    _, time_end = _jumps_time_range(d)

    rows = _get_db().execute("""
        SELECT timestamp, curr_price
        FROM read_parquet(?)
        WHERE symbol = ?
          AND timestamp >= ?
          AND timestamp <= ?
          AND curr_price > 0
        ORDER BY timestamp ASC
    """, [str(path), symbol, time_start, time_end]).fetchall()

    return [{"t": calendar.timegm(r[0].timetuple()), "p": float(r[1])} for r in rows]


async def query_ticks(symbol: str, date_str: str) -> list:
    """Return all raw ticks for tick-level replay."""
    d = PyDate.fromisoformat(date_str)
    return await asyncio.to_thread(_sync_query_ticks, symbol, d)


# ── Trade journal ─────────────────────────────────────────────────────────────

def _sync_query_trade_journal(date_str: str, symbol: Optional[str] = None) -> dict:
    """
    Read trade_journal/latest.parquet and return the same JSON shape as
    the production GET /api/journal endpoint — so the frontend works unchanged.
    """
    if not _JOURNAL_PATH.exists():
        return {"date": date_str, "trades": [], "summary": {"total": 0, "wins": 0, "losses": 0, "pnl": 0.0}}

    where = "WHERE CAST(trade_date AS VARCHAR) = ?"
    params = [date_str]
    if symbol:
        where += " AND symbol = ?"
        params.append(symbol)

    rows = _get_db().execute(
        f"""
        SELECT id, trade_date, symbol, underlying, strike, option_type, expiry_date,
               direction, quantity, entry_price, exit_price, entry_time, exit_time,
               underlying_price_at_entry, underlying_price_at_exit,
               pnl, setup, notes, outcome, order_id
        FROM read_parquet(?)
        {where}
        ORDER BY entry_time ASC NULLS LAST
        """,
        [str(_JOURNAL_PATH)] + params,
    ).fetchall()

    cols = [
        'id', 'trade_date', 'symbol', 'underlying', 'strike', 'option_type',
        'expiry_date', 'direction', 'quantity', 'entry_price', 'exit_price',
        'entry_time', 'exit_time', 'underlying_price_at_entry',
        'underlying_price_at_exit', 'pnl', 'setup', 'notes', 'outcome', 'order_id',
    ]

    result = []
    for row in rows:
        r = dict(zip(cols, row))
        result.append({
            "id":                        int(r['id']) if r['id'] else None,
            "trade_date":                str(r['trade_date']),
            "symbol":                    r['symbol'],
            "underlying":                r['underlying'],
            "strike":                    float(r['strike']) if r['strike'] else None,
            "option_type":               r['option_type'],
            "expiry_date":               str(r['expiry_date']) if r['expiry_date'] else None,
            "direction":                 r['direction'],
            "quantity":                  int(r['quantity']) if r['quantity'] else 0,
            "entry_price":               float(r['entry_price']) if r['entry_price'] else None,
            "exit_price":                float(r['exit_price']) if r['exit_price'] else None,
            "entry_time":                r['entry_time'].isoformat() if r['entry_time'] else None,
            "exit_time":                 r['exit_time'].isoformat() if r['exit_time'] else None,
            "underlying_price_at_entry": float(r['underlying_price_at_entry']) if r['underlying_price_at_entry'] else None,
            "underlying_price_at_exit":  float(r['underlying_price_at_exit']) if r['underlying_price_at_exit'] else None,
            "pnl":                       None,   # recomputed below
            "setup":                     r['setup'],
            "notes":                     r['notes'],
            "outcome":                   r['outcome'],
            "order_id":                  r['order_id'],
            "trade_no":                  None,   # assigned below
        })

    # Compute trade_no + P&L (same algorithm as production journal.py)
    _global_no: int = 0
    _sym_qty: dict  = {}
    _sym_tno: dict  = {}
    _sym_pnl: dict  = {}

    for trade in result:
        sym   = trade["symbol"]
        qty   = trade["quantity"] or 0
        price = trade["entry_price"] or 0.0
        prev  = _sym_qty.get(sym, 0)

        if prev == 0:
            _global_no   += 1
            _sym_tno[sym] = _global_no
            _sym_pnl[sym] = 0.0

        trade["trade_no"] = _sym_tno[sym]
        new_qty = prev + (qty if trade["direction"] == "BUY" else -qty)
        _sym_qty[sym]  = new_qty
        _sym_pnl[sym] += price * qty * (1 if trade["direction"] == "SELL" else -1)

        if new_qty == 0:
            trade["pnl"] = round(_sym_pnl[sym], 2)
            del _sym_tno[sym]
            del _sym_pnl[sym]

    closed = [t for t in result if t["pnl"] is not None]
    total_pnl = sum(t["pnl"] for t in closed)
    return {
        "date":    date_str,
        "trades":  result,
        "summary": {
            "total":  len(closed),
            "wins":   sum(1 for t in closed if t["pnl"] > 0),
            "losses": sum(1 for t in closed if t["pnl"] < 0),
            "pnl":    round(total_pnl, 2),
        },
    }


async def query_trade_journal(date_str: str, symbol: Optional[str] = None) -> dict:
    """DuckDB replacement for GET /api/journal — reads from trade_journal/latest.parquet."""
    return await asyncio.to_thread(_sync_query_trade_journal, date_str, symbol)


# ── Index opening gap ──────────────────────────────────────────────────────────

_INDICES = ['NIFTY', 'BANKNIFTY', 'FINNIFTY', 'MIDCPNIFTY', 'SENSEX']


def _sync_query_index_gap(d: PyDate) -> dict:
    """Return opening gap (today's 9:15 open vs prev trading day's close) for each index."""
    today_path = _date_path(_CANDLES_ROOT, d)
    if not today_path.exists():
        return {}

    # Walk back up to 7 calendar days to find the previous trading day's Parquet.
    prev_d = None
    for offset in range(1, 8):
        candidate = d - timedelta(days=offset)
        if _date_path(_CANDLES_ROOT, candidate).exists():
            prev_d = candidate
            break
    if prev_d is None:
        return {}

    prev_path = _date_path(_CANDLES_ROOT, prev_d)
    today_open_ep = calendar.timegm(datetime(d.year, d.month, d.day, 9, 15, 0).timetuple())
    prev_end_ep   = calendar.timegm(datetime(prev_d.year, prev_d.month, prev_d.day, 16, 0, 0).timetuple())

    result = {}
    db = _get_db()
    nifty_prev_close = None

    for idx in _INDICES:
        try:
            open_row = db.execute("""
                SELECT open FROM read_parquet(?)
                WHERE symbol = ? AND bucket >= ? AND bucket <= ?
                ORDER BY bucket ASC LIMIT 1
            """, [str(today_path), idx, today_open_ep, today_open_ep + 5]).fetchone()
            if open_row is None:
                continue
            today_open = float(open_row[0])

            close_row = db.execute("""
                SELECT close FROM read_parquet(?)
                WHERE symbol = ? AND bucket <= ?
                ORDER BY bucket DESC LIMIT 1
            """, [str(prev_path), idx, prev_end_ep]).fetchone()
            if close_row is None or close_row[0] == 0:
                continue
            prev_close = float(close_row[0])

            if idx == 'NIFTY':
                nifty_prev_close = prev_close

            gap_pts = round(today_open - prev_close, 2)
            gap_pct = round((gap_pts / prev_close) * 100, 3)
            result[idx] = {
                'open':       today_open,
                'prev_close': prev_close,
                'gap_pts':    gap_pts,
                'gap_pct':    gap_pct,
                'direction':  'UP' if gap_pts > 0 else ('DOWN' if gap_pts < 0 else 'FLAT'),
            }
        except Exception:
            continue

    # GIFT Nifty: 9:10 AM price vs NIFTY's previous day close.
    # Uses open of the 9:10:00 bucket (pre-market indicator of expected NIFTY open).
    if nifty_prev_close:
        try:
            gift_ep = calendar.timegm(datetime(d.year, d.month, d.day, 9, 10, 0).timetuple())
            gift_row = db.execute("""
                SELECT open FROM read_parquet(?)
                WHERE symbol = 'GIFTNIFTY' AND bucket >= ? AND bucket <= ?
                ORDER BY bucket ASC LIMIT 1
            """, [str(today_path), gift_ep, gift_ep + 5]).fetchone()
            if gift_row:
                gift_price = float(gift_row[0])
                gap_pts = round(gift_price - nifty_prev_close, 2)
                gap_pct = round((gap_pts / nifty_prev_close) * 100, 3)
                result['GIFTNIFTY'] = {
                    'open':       gift_price,
                    'prev_close': nifty_prev_close,
                    'gap_pts':    gap_pts,
                    'gap_pct':    gap_pct,
                    'direction':  'UP' if gap_pts > 0 else ('DOWN' if gap_pts < 0 else 'FLAT'),
                    'snapshot_time': '9:10',
                }
        except Exception:
            pass

    return result


async def query_index_gap(date_str: str) -> dict:
    """DuckDB replacement for GET /api/hist-index-gap."""
    d = PyDate.fromisoformat(date_str)
    return await asyncio.to_thread(_sync_query_index_gap, d)
