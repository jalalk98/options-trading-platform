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
from datetime import date as PyDate, datetime
from pathlib import Path
from typing import Optional

import duckdb

from config.credentials import LOCAL_PARQUET_PATH


# Paths
_PARQUET_ROOT = Path(LOCAL_PARQUET_PATH)
_CANDLES_ROOT = _PARQUET_ROOT / 'candles_5s'
_GAPS_ROOT = _PARQUET_ROOT / 'gap_events'
_TICKS_ROOT = _PARQUET_ROOT / 'raw_ticks'


# Single long-lived DuckDB connection (thread-safe for reads)
_db = duckdb.connect(':memory:')


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
    result = _db.execute("""
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
    result = _db.execute("""
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
    jumps_raw = _db.execute("""
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
                row = _db.execute("""
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
                row = _db.execute("""
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
    result = _db.execute("""
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
        row = _db.execute("""
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
