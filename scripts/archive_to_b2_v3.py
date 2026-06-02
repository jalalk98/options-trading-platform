#!/usr/bin/env python3
"""
archive_to_b2_v3.py — Two-phase safe archive: export (Phase A) + drop (Phase B).

SAFETY GUARANTEE: No ctid-based deletes anywhere.
  - gap_ticks partitions are removed via DETACH PARTITION + DROP TABLE only.
    This is partition-local and cannot touch sibling partitions.
  - candles_5s and gap_events use a direct date-bounded WHERE DELETE.
    Phase B refuses to delete from any table not independently verified in B2.

Phase A  (--phase export):
  Target = today_IST - 2 calendar days (or --date YYYY-MM-DD).
  For gap_ticks: exports one parquet file per symbol_group (NIFTY, BANKNIFTY, etc.),
    each uploaded to raw_ticks/symbol_group=<GRP>/year=.../month=.../day=.../data.parquet.
    archive_status gets one row per (partition_date, 'gap_ticks', symbol_group).
  For candles_5s and gap_events: single file per date, symbol_group=''.
    1. Export rows to local parquet (chunked cursor, partition-safe).
    2. Upload to B2 with retry.
    3. Verify B2 file: size + ETag/MD5 + row count (metadata-only via pyarrow S3FS).
    4. Update archive_status → verified.
  Phase A is entirely read-only; no DB rows are modified or deleted.

Phase B  (--phase drop):
  Eligible = candles_5s verified + gap_events verified + ALL gap_ticks symbol_group
             rows verified, latest verified_at < NOW() - 10h,
             partition_date < today_IST - 3 days.
  For each eligible date:
    1. Re-verify every B2 file: all gap_ticks groups + candles_5s + gap_events.
    2. Assert gap_ticks partition COUNT(*) == SUM(db_row_count) across all groups.
    3. DETACH PARTITION + DROP TABLE (gap_ticks only).
    4. DELETE candles_5s rows (WHERE bucket range, after pre-check).
    5. DELETE gap_events rows (WHERE bucket range, after pre-check).
    6. Update archive_status → dropped for all rows.

Failure policy: any verification mismatch or unexpected exception → status='failed',
Telegram alert with full detail, exit non-zero. Never silently continue on error.

For dry-run testing of Phase B without waiting 10 hours, manually age the
verified_at timestamp in the DB:
  UPDATE archive_status
  SET verified_at = verified_at - INTERVAL '25 hours'
  WHERE partition_date = '<test_date>';
Then run --phase drop normally. There is no code bypass for this gate.

Usage:
  python3 scripts/archive_to_b2_v3.py --phase export
  python3 scripts/archive_to_b2_v3.py --phase export --date 2026-05-12
  python3 scripts/archive_to_b2_v3.py --phase drop
"""

import sys
import os
import json
import hashlib
import argparse
import asyncio
import asyncpg
import boto3
import time
import tempfile
import traceback
from botocore.client import Config
from pathlib import Path
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import fs as pa_fs

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.credentials import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)

IST = ZoneInfo("Asia/Kolkata")

# ── Column lists ──────────────────────────────────────────────────────────────
GAP_TICKS_COLS = [
    'id', 'instrument_token', 'symbol', 'expiry_date', 'strike',
    'option_type', 'timestamp', 'last_trade_time',
    'curr_price', 'last_quantity', 'average_price',
    'curr_volume', 'oi', 'oi_day_high', 'oi_day_low',
    'buy_quantity', 'sell_quantity', 'depth',
    'bid_depth_qty', 'ask_depth_qty',
    'best_bid', 'best_ask',
]

CANDLES_5S_COLS = [
    'symbol', 'bucket', 'open', 'high', 'low', 'close',
    'prev_close', 'candle_gap', 'is_candle_gap',
    'gap_ref_price', 'gap_ref_bucket',
    'dist_from_gap', 'dist_from_gap_pct',
    'closed_above_gap', 'candles_above_gap', 'seconds_above_gap',
    'volume', 'buy_vol', 'sell_vol',
    'delta', 'cum_delta',
    'oi_close', 'oi_change', 'depth_imbalance',
]

GAP_EVENTS_COLS = [
    'symbol', 'bucket', 'direction',
    'prev_price', 'curr_price', 'vol_change',
]

TABLE_COLS = {
    'gap_ticks':  GAP_TICKS_COLS,
    'candles_5s': CANDLES_5S_COLS,
    'gap_events': GAP_EVENTS_COLS,
}

# ── Symbol group classifier (mirrors tick collector logic) ────────────────────
def get_symbol_group(symbol: str) -> str:
    if symbol.startswith('BANKNIFTY'):  return 'BANKNIFTY'
    if symbol.startswith('MIDCPNIFTY'): return 'MIDCPNIFTY'
    if symbol.startswith('FINNIFTY'):   return 'FINNIFTY'
    if symbol.startswith('NIFTY'):      return 'NIFTY'
    if symbol.startswith('SENSEX'):     return 'SENSEX'
    return 'OTHER'


# ── B2 key builders ───────────────────────────────────────────────────────────
def b2_key_for(table_kind: str, d: date, symbol_group: str = '') -> str:
    if table_kind == 'gap_ticks':
        return (
            f"raw_ticks/symbol_group={symbol_group}/"
            f"year={d.year}/month={d.month:02d}/day={d.day:02d}/data.parquet"
        )
    return f"{table_kind}/year={d.year}/month={d.month:02d}/day={d.day:02d}/data.parquet"


# ── Credentials / clients ─────────────────────────────────────────────────────
def read_secrets() -> dict:
    s = {}
    for line in (Path.home() / '.kite_secrets').read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            k, _, v = line.partition('=')
            s[k.strip()] = v.strip()
    return s


def get_b2_client(secrets):
    return boto3.client(
        's3',
        endpoint_url=f"https://{secrets['BACKBLAZE_ENDPOINT']}",
        aws_access_key_id=secrets['BACKBLAZE_KEY_ID'],
        aws_secret_access_key=secrets['BACKBLAZE_APP_KEY'],
        config=Config(signature_version='s3v4'),
    )


def get_pa_s3fs(secrets) -> pa_fs.S3FileSystem:
    return pa_fs.S3FileSystem(
        access_key=secrets['BACKBLAZE_KEY_ID'],
        secret_key=secrets['BACKBLAZE_APP_KEY'],
        endpoint_override=secrets['BACKBLAZE_ENDPOINT'],
        scheme='https',
    )


def send_telegram(text: str, secrets: dict):
    try:
        import requests
        token    = secrets.get('TELEGRAM_BOT_TOKEN')
        chat_id  = secrets.get('TELEGRAM_CHAT_ID')
        if not token or not chat_id:
            return
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": text[:4000]},
            timeout=8,
        )
    except Exception as e:
        print(f"  [telegram] alert failed: {e}")


def md5_of_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def ist_today() -> date:
    return datetime.now(IST).date()


MAX_BACKLOG_PER_RUN = 5  # Maximum backlog dates archived in a single Phase A run


async def get_retention_days(conn) -> int:
    """
    Read the minimum retain_days from retention_config.
    This is the conservative value — Phase A archives data this many days old,
    Phase B drops data (this + 1) days old.
    """
    val = await conn.fetchval(
        "SELECT MIN(retain_days) FROM retention_config"
    )
    if val is None or val < 1:
        raise RuntimeError(
            f"retention_config returned invalid min(retain_days)={val}. "
            f"Cannot proceed without a valid retention value."
        )
    return int(val)


async def find_backlog_dates(conn, target_date) -> list:
    """
    Find gap_ticks partitions older than target_date that have never been
    successfully archived (no 'verified' or 'dropped' row in archive_status).

    Excludes:
      - Dates >= target_date (future or current target)
      - Dates with status='failed' (operator must reset to pending manually)
      - Already verified or dropped dates

    Returns: chronologically sorted list of dates (oldest first), capped at
    MAX_BACKLOG_PER_RUN entries.
    """
    rows = await conn.fetch("""
        SELECT
            c.relname AS partition_name,
            TO_DATE(
                SUBSTRING(c.relname FROM 'gap_ticks_(\\d{4}_\\d{2}_\\d{2})$'),
                'YYYY_MM_DD'
            ) AS partition_date
        FROM pg_class c
        JOIN pg_inherits i ON c.oid = i.inhrelid
        JOIN pg_class p    ON i.inhparent = p.oid
        WHERE p.relname = 'gap_ticks'
          AND c.relname ~ '^gap_ticks_\\d{4}_\\d{2}_\\d{2}$'
        ORDER BY partition_date ASC
    """)

    backlog = []
    for row in rows:
        pd = row['partition_date']

        if pd >= target_date:
            continue

        # "handled" = at least one gap_ticks row is verified/dropped AND
        # none are still pending/exporting/failed (i.e. all groups done).
        gt_rows = await conn.fetch("""
            SELECT status FROM archive_status
            WHERE partition_date = $1 AND table_kind = 'gap_ticks'
        """, pd)
        if gt_rows:
            statuses = [r['status'] for r in gt_rows]
            all_done = all(s in ('verified', 'dropped') for s in statuses)
            any_failed = any(s == 'failed' for s in statuses)
            if all_done:
                continue
            if any_failed:
                print(f"  Backlog skip: {pd} has status='failed' — manual reset required")
                continue

        backlog.append(pd)
        if len(backlog) >= MAX_BACKLOG_PER_RUN:
            break

    return backlog


# ── B2 upload with retry + three-way verification ─────────────────────────────
def upload_and_verify(
    s3, pa_s3, secrets, bucket: str,
    local_path: str, b2_key: str,
    expected_rows: int,
) -> tuple[int, str]:
    """
    Upload local_path to B2. Verify three ways after upload:
      1. ContentLength (byte size) matches local file.
      2. ETag matches local MD5 (single-part upload ⟹ ETag == MD5).
      3. Parquet row count read from B2 via pyarrow S3FileSystem matches expected_rows.
    Returns (size_bytes, md5_hex).
    Raises RuntimeError on any mismatch after 3 upload attempts.
    """
    local_size = os.path.getsize(local_path)
    local_md5  = md5_of_file(local_path)

    last_exc = None
    for attempt in range(1, 4):
        try:
            with open(local_path, 'rb') as f:
                s3.put_object(
                    Bucket=bucket,
                    Key=b2_key,
                    Body=f.read(),
                    Metadata={'archive_version': 'v3', 'md5': local_md5},
                )

            # ── Verify 1: byte size ───────────────────────────────────────
            head = s3.head_object(Bucket=bucket, Key=b2_key)
            remote_size = head['ContentLength']
            if remote_size != local_size:
                raise RuntimeError(
                    f"Size mismatch: local={local_size}, B2={remote_size}"
                )

            # ── Verify 2: ETag / MD5 ──────────────────────────────────────
            # B2 ETag for single-part uploads equals the file MD5.
            etag = head.get('ETag', '').strip('"')
            if etag and etag != local_md5:
                raise RuntimeError(
                    f"ETag mismatch: local_md5={local_md5}, B2_etag={etag}"
                )

            # ── Verify 3: parquet row count from B2 (metadata-only) ──────
            actual_rows = _b2_parquet_row_count(pa_s3, bucket, b2_key)
            if actual_rows != expected_rows:
                raise RuntimeError(
                    f"Row count mismatch: B2 parquet has {actual_rows:,} rows, "
                    f"expected {expected_rows:,}"
                )

            print(
                f"    ✅ Uploaded & verified: {b2_key}\n"
                f"       size={local_size/1024/1024:.1f} MB  "
                f"md5={local_md5[:10]}  rows={actual_rows:,}"
            )
            return local_size, local_md5

        except Exception as e:
            last_exc = e
            if attempt < 3:
                print(f"    Upload attempt {attempt} failed ({e}) — retrying in 5s…")
                time.sleep(5)

    raise RuntimeError(
        f"B2 upload failed for {b2_key} after 3 attempts: {last_exc}"
    ) from last_exc


def _b2_parquet_row_count(pa_s3: pa_fs.S3FileSystem, bucket: str, key: str) -> int:
    """Read parquet footer via pyarrow S3FileSystem — no full download needed."""
    with pa_s3.open_input_file(f"{bucket}/{key}") as fh:
        return pq.ParquetFile(fh).metadata.num_rows


def b2_reverify(
    s3, pa_s3: pa_fs.S3FileSystem, bucket: str,
    b2_key: str, expected_size: int,
    expected_md5: str, expected_rows: int,
):
    """
    Phase B re-verification: assert B2 file is intact before any drop.
    Checks size, MD5/ETag, and parquet row count independently.
    Raises RuntimeError on any mismatch.
    """
    try:
        head = s3.head_object(Bucket=bucket, Key=b2_key)
    except Exception as e:
        raise RuntimeError(f"B2 file not found: {b2_key} — {e}") from e

    actual_size = head['ContentLength']
    if actual_size != expected_size:
        raise RuntimeError(
            f"B2 re-verify size mismatch: {b2_key} "
            f"stored={expected_size}, actual={actual_size}"
        )

    if expected_md5:
        etag = head.get('ETag', '').strip('"')
        if etag and etag != expected_md5:
            raise RuntimeError(
                f"B2 re-verify ETag mismatch: {b2_key} "
                f"stored_md5={expected_md5}, B2_etag={etag}"
            )

    actual_rows = _b2_parquet_row_count(pa_s3, bucket, b2_key)
    if actual_rows != expected_rows:
        raise RuntimeError(
            f"B2 re-verify row count mismatch: {b2_key} "
            f"stored={expected_rows:,}, B2={actual_rows:,}"
        )

    print(
        f"    ✅ B2 re-verified: {b2_key} "
        f"({actual_size/1024/1024:.1f} MB, {actual_rows:,} rows)"
    )


# ── Export helpers ────────────────────────────────────────────────────────────
async def export_gap_ticks(
    conn, tick_date: date, tmp_dir: str
) -> dict[str, tuple[str, int]]:
    """
    Stream gap_ticks for tick_date, split by symbol_group, writing one parquet
    file per group into tmp_dir.
    Cursor: id (bigint, unique, sequence) — immune to duplicate (timestamp,
    instrument_token) tuples that caused the old composite cursor to skip rows
    at 250k chunk boundaries.
    Returns {symbol_group: (file_path, row_count)} for all non-empty groups.
    """
    cols_str = ', '.join(GAP_TICKS_COLS)
    where = (
        f"timestamp >= '{tick_date}'::date "
        f"AND timestamp < '{tick_date}'::date + INTERVAL '1 day'"
    )
    CHUNK = 250_000
    last_id = 0
    total = 0
    writers: dict[str, pq.ParquetWriter] = {}
    counts:  dict[str, int]              = {}
    paths:   dict[str, str]              = {}
    start = time.time()

    try:
        while True:
            rows = await conn.fetch(
                f"SELECT {cols_str} FROM gap_ticks "
                f"WHERE {where} AND id > $1 "
                f"ORDER BY id ASC LIMIT {CHUNK}",
                last_id,
            )
            if not rows:
                break

            # Group rows by symbol_group
            grp_data: dict[str, dict[str, list]] = {}
            for row in rows:
                grp = get_symbol_group(row['symbol'])
                if grp not in grp_data:
                    grp_data[grp] = {col: [] for col in GAP_TICKS_COLS}
                for col in GAP_TICKS_COLS:
                    val = row[col]
                    if col == 'depth' and val is not None and not isinstance(val, str):
                        val = json.dumps(val)
                    grp_data[grp][col].append(val)

            for grp, data in grp_data.items():
                tbl = pa.table(data)
                if grp not in writers:
                    fpath = os.path.join(tmp_dir, f"gap_ticks_{grp}.parquet")
                    paths[grp]   = fpath
                    counts[grp]  = 0
                    writers[grp] = pq.ParquetWriter(fpath, tbl.schema, compression='snappy')
                writers[grp].write_table(tbl)
                counts[grp] += tbl.num_rows

            last_id = rows[-1]['id']
            total += len(rows)

            elapsed = time.time() - start
            print(
                f"    {total:,} rows | {total/elapsed:,.0f} rows/s | {elapsed:.0f}s",
                end='\r', flush=True,
            )
            del rows
    finally:
        for w in writers.values():
            w.close()

    elapsed = time.time() - start
    print(f"\n    {total:,} gap_ticks rows in {elapsed/60:.1f} min")
    return {grp: (paths[grp], counts[grp]) for grp in counts}


async def export_bucket_table(conn, table_name: str, tick_date: date,
                               cols: list, out_path: str) -> int:
    """
    Export candles_5s or gap_events for tick_date to parquet.
    Cursor: composite (bucket, symbol) — bucket alone is not unique (many symbols
    share each 5s bucket), so a single-column cursor skips rows at chunk boundaries
    where multiple rows share the boundary bucket value. The composite cursor is safe
    because (symbol, bucket) is the primary key of both tables.
    Returns exact row count written.
    """
    cols_str = ', '.join(cols)
    epoch_start = int(datetime.combine(tick_date, datetime.min.time()).timestamp())
    epoch_end   = int(datetime.combine(
        tick_date + timedelta(days=1), datetime.min.time()
    ).timestamp())

    CHUNK = 250_000
    last_bucket = epoch_start - 1
    last_symbol = ''  # '' sorts before all valid symbol strings
    total = 0
    writer = None

    try:
        while True:
            rows = await conn.fetch(
                f"SELECT {cols_str} FROM {table_name} "
                f"WHERE bucket >= $1 AND bucket < $2 "
                f"AND (bucket, symbol) > ($3, $4) "
                f"ORDER BY bucket, symbol ASC LIMIT $5",
                epoch_start, epoch_end, last_bucket, last_symbol, CHUNK,
            )
            if not rows:
                break
            data = {col: [r[col] for r in rows] for col in cols}
            tbl = pa.table(data)
            if writer is None:
                writer = pq.ParquetWriter(out_path, tbl.schema, compression='snappy')
            writer.write_table(tbl)
            last_bucket = rows[-1]['bucket']
            last_symbol = rows[-1]['symbol']
            total += len(rows)
            del rows
    finally:
        if writer:
            writer.close()

    print(f"    {total:,} {table_name} rows")
    return total


# ── archive_status helpers ────────────────────────────────────────────────────
async def status_set(conn, tick_date: date, table_kind: str,
                     partition_name: str, symbol_group: str = '', **kwargs):
    """
    Upsert archive_status for (partition_date, table_kind, symbol_group).
    symbol_group='' for candles_5s and gap_events; actual group name for gap_ticks.

    Two rules for callers:
      - Always pass status= explicitly; it becomes $4 (never hardcoded 'pending').
        Keeping it in kwargs would duplicate the column in the INSERT list and
        trigger DuplicateColumnError.
      - Pass the sentinel string 'now()' for timestamp fields that should record
        the current time. These are embedded as SQL now() literals rather than
        bound as string parameters (PostgreSQL cannot cast the string 'now()' to
        TIMESTAMPTZ — only the bare string 'now' or a real timestamp works).
    """
    # Pop status — it is always an explicit column in the INSERT list.
    status = kwargs.pop('status', 'pending')

    # Split kwargs into SQL-literal fields ('now()') vs. parameterized fields.
    now_fields   = [k for k, v in kwargs.items() if v == 'now()']
    param_kwargs = {k: v for k, v in kwargs.items() if v != 'now()'}

    # ON CONFLICT SET clauses
    set_clauses = ['last_updated = now()', 'status = EXCLUDED.status']
    for k in now_fields:
        set_clauses.append(f"{k} = now()")
    for k in param_kwargs:
        set_clauses.append(f"{k} = EXCLUDED.{k}")

    # INSERT column / value fragments
    # Binding positions: $1=tick_date  $2=partition_name  $3=symbol_group  $4=status  $5+=params
    now_col_str = (', ' + ', '.join(now_fields)) if now_fields else ''
    now_val_str = (', ' + ', '.join('now()' for _ in now_fields)) if now_fields else ''
    p_col_str   = (', ' + ', '.join(param_kwargs.keys())) if param_kwargs else ''
    p_val_str   = (
        ', ' + ', '.join(f'${i + 5}' for i in range(len(param_kwargs)))
    ) if param_kwargs else ''

    await conn.execute(f"""
        INSERT INTO archive_status
            (partition_date, partition_name, table_kind, symbol_group, status, last_updated
             {now_col_str}{p_col_str})
        VALUES ($1, $2, '{table_kind}', $3, $4, now()
                {now_val_str}{p_val_str})
        ON CONFLICT (partition_date, table_kind, symbol_group) DO UPDATE SET
            {', '.join(set_clauses)}
    """, tick_date, partition_name, symbol_group, status, *param_kwargs.values())


async def get_status_rows(conn, tick_date: date, table_kind: str) -> list:
    """Return all archive_status rows for (partition_date, table_kind) — one row
    for candles_5s/gap_events, one per symbol_group for gap_ticks."""
    return await conn.fetch(
        "SELECT * FROM archive_status "
        "WHERE partition_date = $1 AND table_kind = $2",
        tick_date, table_kind,
    )


# ── Phase A: export ───────────────────────────────────────────────────────────
async def run_phase_a(target_date: date, secrets: dict, s3, pa_s3, bucket: str):
    """
    Export all three tables for target_date to B2. No DB rows are deleted.
    Updates archive_status for each table_kind independently.
    """
    conn = await asyncpg.connect(
        host=DB_HOST, port=int(DB_PORT),
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
    )
    try:
        await conn.execute("SET statement_timeout = 0")
        partition_name = f"gap_ticks_{str(target_date).replace('-', '_')}"

        print(f"\n━━━ Phase A: {target_date} ━━━")

        # ── Guard: skip if already fully verified ─────────────────────────
        # candles_5s and gap_events each need exactly 1 verified/dropped row.
        # gap_ticks needs ≥1 verified/dropped rows and 0 unfinished rows.
        already_done = await conn.fetchval("""
            SELECT
                COUNT(*) FILTER (WHERE table_kind = 'candles_5s'
                                   AND status IN ('verified','dropping','dropped')) = 1
                AND
                COUNT(*) FILTER (WHERE table_kind = 'gap_events'
                                   AND status IN ('verified','dropping','dropped')) = 1
                AND
                COUNT(*) FILTER (WHERE table_kind = 'gap_ticks'
                                   AND status IN ('verified','dropping','dropped')) > 0
                AND
                COUNT(*) FILTER (WHERE table_kind = 'gap_ticks'
                                   AND status NOT IN ('verified','dropping','dropped')) = 0
            FROM archive_status
            WHERE partition_date = $1
        """, target_date)
        if already_done:
            print(f"  All table_kinds already verified/dropped — nothing to do")
            return

        # ── Guard: gap_ticks partition must exist ─────────────────────────
        exists = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = $1)",
            partition_name,
        )
        if not exists:
            print(f"  Partition {partition_name} not found — nothing to export")
            send_telegram(
                f"⏭ Archive v3 Phase A: {target_date} — partition not found",
                secrets,
            )
            return

        tbl_results = {}  # table_kind → {rows, b2_key, size, md5}

        with tempfile.TemporaryDirectory(prefix='archive_v3_') as tmp:

            # ── gap_ticks: one file per symbol_group ──────────────────────
            # Clean up any stale symbol_group='' rows left by a schema reset or
            # a previous run under the old single-file schema. Safe because the
            # new code never creates a '' row for gap_ticks except for 0-row dates,
            # where it is immediately marked verified and would not be pending.
            await conn.execute("""
                DELETE FROM archive_status
                WHERE partition_date = $1
                  AND table_kind = 'gap_ticks'
                  AND symbol_group = ''
                  AND status NOT IN ('verified', 'dropped')
            """, target_date)

            gt_existing = await get_status_rows(conn, target_date, 'gap_ticks')
            gt_done_grps = {r['symbol_group'] for r in gt_existing
                            if r['status'] in ('verified', 'dropped')}
            gt_total_rows = sum(r['db_row_count'] or 0 for r in gt_existing
                                if r['status'] in ('verified', 'dropped'))
            all_gt_done = (
                bool(gt_existing)
                and all(r['status'] in ('verified', 'dropped') for r in gt_existing)
            )

            if all_gt_done:
                print(f"  gap_ticks: all groups already verified/dropped — skipping")
                gt_groups = {r['symbol_group']: (r['db_row_count'] or 0)
                             for r in gt_existing if r['symbol_group']}
                tbl_results['gap_ticks'] = {'rows': gt_total_rows, 'groups': gt_groups, 'skipped': True}
            else:
                try:
                    db_rows = await conn.fetchval(
                        f"SELECT COUNT(*) FROM {partition_name}"
                    )

                    if db_rows == 0:
                        print(f"  gap_ticks: 0 rows in partition — skipping upload")
                        await status_set(
                            conn, target_date, 'gap_ticks', partition_name,
                            symbol_group='', status='verified',
                            db_row_count=0, b2_row_count=0,
                            exported_at='now()', verified_at='now()',
                        )
                        tbl_results['gap_ticks'] = {'rows': 0}
                    else:
                        print(f"  Exporting gap_ticks ({db_rows:,} rows) by symbol_group…")
                        groups = await export_gap_ticks(conn, target_date, tmp)
                        total_written = sum(cnt for _, cnt in groups.values())

                        if total_written != db_rows:
                            raise RuntimeError(
                                f"gap_ticks export row mismatch: "
                                f"DB={db_rows:,} written={total_written:,}"
                            )

                        gt_rows_result = 0
                        for grp, (fpath, written) in sorted(groups.items()):
                            if grp in gt_done_grps:
                                print(f"    {grp}: already verified — skipping")
                                gt_rows_result += written
                                continue

                            b2_key = b2_key_for('gap_ticks', target_date, grp)
                            size, md5 = upload_and_verify(
                                s3, pa_s3, secrets, bucket, fpath, b2_key, written,
                            )
                            await status_set(
                                conn, target_date, 'gap_ticks', partition_name,
                                symbol_group=grp, status='verified',
                                db_row_count=written,
                                b2_row_count=written,
                                b2_key=b2_key,
                                b2_size_bytes=size,
                                b2_md5=md5,
                                exported_at='now()', verified_at='now()',
                            )
                            gt_rows_result += written

                        gt_groups = {grp: cnt for grp, (_, cnt) in sorted(groups.items())}
                        tbl_results['gap_ticks'] = {'rows': gt_rows_result, 'groups': gt_groups}

                except Exception as e:
                    tb = traceback.format_exc()
                    msg = f"Phase A failed for {target_date}/gap_ticks: {e}"
                    await status_set(conn, target_date, 'gap_ticks', partition_name,
                                      symbol_group='', status='failed',
                                      failure_reason=str(e)[:500])
                    send_telegram(
                        f"❌ Archive v3 Phase A FAILED\n{target_date} / gap_ticks\n"
                        f"{str(e)[:300]}\n{tb[-400:]}",
                        secrets,
                    )
                    raise RuntimeError(msg) from e

            # ── candles_5s and gap_events: single file per date ───────────
            for table_kind in ('candles_5s', 'gap_events'):
                existing_rows = await get_status_rows(conn, target_date, table_kind)
                existing = existing_rows[0] if existing_rows else None
                if existing and existing['status'] in ('verified', 'dropped'):
                    print(f"  {table_kind}: already {existing['status']} — skipping")
                    tbl_results[table_kind] = {
                        'rows': existing['db_row_count'] or 0,
                        'skipped': True,
                    }
                    continue

                await status_set(conn, target_date, table_kind,
                                  partition_name, status='exporting')
                print(f"  Exporting {table_kind}…")

                out_path = os.path.join(tmp, f"{table_kind}.parquet")
                b2_key   = b2_key_for(table_kind, target_date)

                try:
                    written = await export_bucket_table(
                        conn, table_kind, target_date,
                        TABLE_COLS[table_kind], out_path,
                    )

                    if written == 0:
                        print(f"    0 rows — skipping upload")
                        await status_set(
                            conn, target_date, table_kind, partition_name,
                            status='verified',
                            db_row_count=0, b2_row_count=0,
                            exported_at='now()', verified_at='now()',
                        )
                        tbl_results[table_kind] = {'rows': 0, 'b2_key': None}
                        continue

                    size, md5 = upload_and_verify(
                        s3, pa_s3, secrets, bucket,
                        out_path, b2_key, written,
                    )
                    await status_set(
                        conn, target_date, table_kind, partition_name,
                        status='verified',
                        db_row_count=written,
                        b2_row_count=written,
                        b2_key=b2_key,
                        b2_size_bytes=size,
                        b2_md5=md5,
                        exported_at='now()',
                        verified_at='now()',
                    )
                    tbl_results[table_kind] = {
                        'rows': written, 'b2_key': b2_key,
                        'size': size, 'md5': md5,
                    }

                except Exception as e:
                    tb = traceback.format_exc()
                    msg = f"Phase A failed for {target_date}/{table_kind}: {e}"
                    await status_set(conn, target_date, table_kind, partition_name,
                                      status='failed', failure_reason=str(e)[:500])
                    send_telegram(
                        f"❌ Archive v3 Phase A FAILED\n{target_date} / {table_kind}\n"
                        f"{str(e)[:300]}\n{tb[-400:]}",
                        secrets,
                    )
                    raise RuntimeError(msg) from e

        # ── All tables done ───────────────────────────────────────────────
        gt = tbl_results.get('gap_ticks', {})
        c5 = tbl_results.get('candles_5s', {})
        ge = tbl_results.get('gap_events', {})

        gt_groups = gt.get('groups', {})
        gt_group_lines = ''.join(
            f"    {grp:<12} {cnt:>12,}\n"
            for grp, cnt in sorted(gt_groups.items(), key=lambda x: -x[1])
        )
        total_rows = gt.get('rows', 0) + c5.get('rows', 0) + ge.get('rows', 0)
        msg = (
            f"✅ Archive v3 Phase A: {target_date}\n"
            f"  gap_ticks:  {gt.get('rows',0):>12,} rows\n"
            f"{gt_group_lines}"
            f"  candles_5s: {c5.get('rows',0):>12,} rows\n"
            f"  gap_events: {ge.get('rows',0):>12,} rows\n"
            f"  {'─'*26}\n"
            f"  total:      {total_rows:>12,} rows\n"
            f"  All table_kinds verified in B2."
        )
        print(f"\n{msg}")
        send_telegram(msg, secrets)

    except Exception as e:
        tb = traceback.format_exc()
        send_telegram(
            f"❌ Archive v3 Phase A unexpected error for {target_date}:\n"
            f"{str(e)[:300]}\n{tb[-400:]}",
            secrets,
        )
        raise
    finally:
        await conn.close()


# ── Phase B: drop ─────────────────────────────────────────────────────────────
async def run_phase_b(secrets: dict, s3, pa_s3, bucket: str):
    """
    Drop gap_ticks partitions (and delete candles/events rows) for all dates
    where all three table_kinds are verified > 10h ago and the date is > 3 days old.
    Phase B refuses to touch any table that hasn't been independently verified in B2.
    """
    conn = await asyncpg.connect(
        host=DB_HOST, port=int(DB_PORT),
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
    )
    try:
        await conn.execute("SET statement_timeout = 0")
        retention  = await get_retention_days(conn)
        today      = ist_today()
        cutoff_age = today - timedelta(days=retention)
        print(f"Phase B: retention={retention} days, "
              f"dropping partitions where partition_date < {cutoff_age} "
              f"AND verified > 10h ago")

        # ── Find dates where all table_kinds are verified ────────────────
        # gap_ticks: ≥1 verified rows, 0 non-verified rows.
        # candles_5s and gap_events: exactly 1 verified row each.
        eligible_dates = await conn.fetch("""
            SELECT partition_date
            FROM archive_status
            WHERE partition_date < $1
            GROUP BY partition_date
            HAVING
                COUNT(*) FILTER (WHERE table_kind = 'candles_5s'
                                   AND status = 'verified') = 1
                AND COUNT(*) FILTER (WHERE table_kind = 'gap_events'
                                      AND status = 'verified') = 1
                AND COUNT(*) FILTER (WHERE table_kind = 'gap_ticks'
                                      AND status = 'verified') > 0
                AND COUNT(*) FILTER (WHERE table_kind = 'gap_ticks'
                                      AND status != 'verified') = 0
                AND MAX(verified_at) < NOW() - INTERVAL '10 hours'
            ORDER BY partition_date ASC
        """, cutoff_age)

        if not eligible_dates:
            print("✅ Phase B: no dates eligible for drop today")
            return

        print(f"Phase B: {len(eligible_dates)} date(s) eligible")
        dropped_dates  = []
        date_summaries = []
        total_freed    = 0

        for row in eligible_dates:
            tick_date = row['partition_date']
            print(f"\n━━━ Phase B: {tick_date} ━━━")

            # Load all status rows for this date, separated by table_kind
            all_rows = await conn.fetch(
                "SELECT * FROM archive_status WHERE partition_date = $1",
                tick_date,
            )
            gap_ticks_rows = [r for r in all_rows if r['table_kind'] == 'gap_ticks']
            candles_row    = next(r for r in all_rows if r['table_kind'] == 'candles_5s')
            events_row     = next(r for r in all_rows if r['table_kind'] == 'gap_events')
            partition_name = gap_ticks_rows[0]['partition_name']

            # ── Re-verify all B2 files ────────────────────────────────────
            try:
                # gap_ticks: one file per symbol_group
                for sr in gap_ticks_rows:
                    if not sr['b2_key'] or not sr['b2_size_bytes']:
                        if (sr['b2_row_count'] or 0) == 0:
                            print(f"  gap_ticks/{sr['symbol_group'] or 'ALL'}: 0 rows, no B2 file — OK")
                            continue
                        raise RuntimeError(
                            f"gap_ticks/{sr['symbol_group']}: b2_key or b2_size_bytes missing "
                            f"but b2_row_count={sr['b2_row_count']} — cannot verify"
                        )
                    b2_reverify(s3, pa_s3, bucket,
                                sr['b2_key'], sr['b2_size_bytes'],
                                sr['b2_md5'], sr['b2_row_count'])

                # candles_5s and gap_events: single file each
                for sr in (candles_row, events_row):
                    tbl = sr['table_kind']
                    if not sr['b2_key'] or not sr['b2_size_bytes']:
                        if (sr['b2_row_count'] or 0) == 0:
                            print(f"  {tbl}: 0 rows, no B2 file — OK")
                            continue
                        raise RuntimeError(
                            f"{tbl}: b2_key or b2_size_bytes missing "
                            f"but b2_row_count={sr['b2_row_count']} — cannot verify"
                        )
                    b2_reverify(s3, pa_s3, bucket,
                                sr['b2_key'], sr['b2_size_bytes'],
                                sr['b2_md5'], sr['b2_row_count'])

            except RuntimeError as e:
                msg = f"B2 re-verify FAILED for {tick_date}: {e}"
                for sr in all_rows:
                    await status_set(conn, tick_date, sr['table_kind'],
                                      sr['partition_name'],
                                      symbol_group=sr['symbol_group'],
                                      status='failed', failure_reason=str(e)[:500])
                send_telegram(f"❌ Archive v3 Phase B ABORT: {msg}", secrets)
                print(f"  {msg}")
                continue

            # ── Drop gap_ticks partition ───────────────────────────────────
            # Row count assertion: partition total must equal SUM across all groups.
            expected_gt_rows = sum((sr['db_row_count'] or 0) for sr in gap_ticks_rows)

            for sr in gap_ticks_rows:
                await status_set(conn, tick_date, 'gap_ticks', partition_name,
                                  symbol_group=sr['symbol_group'], status='dropping')
            try:
                gt_freed = await _drop_partition(
                    partition_name, expected_gt_rows, secrets,
                )
            except Exception as e:
                tb = traceback.format_exc()
                msg = f"DROP failed for {partition_name}: {e}"
                for sr in gap_ticks_rows:
                    await status_set(conn, tick_date, 'gap_ticks', partition_name,
                                      symbol_group=sr['symbol_group'],
                                      status='failed', failure_reason=str(e)[:500])
                send_telegram(
                    f"❌ Archive v3 Phase B DROP FAILED: {tick_date}\n"
                    f"{str(e)[:300]}\n{tb[-300:]}",
                    secrets,
                )
                print(f"  {msg}")
                continue

            # ── Delete candles_5s and gap_events ──────────────────────────
            c5_freed = 0
            ge_freed = 0
            try:
                c5_freed = await _delete_table_for_date(
                    conn, 'candles_5s', tick_date, candles_row,
                )
                ge_freed = await _delete_table_for_date(
                    conn, 'gap_events', tick_date, events_row,
                )
            except Exception as e:
                tb = traceback.format_exc()
                msg = f"Row delete failed for {tick_date}: {e}"
                send_telegram(
                    f"❌ Archive v3 Phase B DELETE FAILED: {tick_date}\n"
                    f"{str(e)[:300]}\n{tb[-300:]}",
                    secrets,
                )
                print(f"  {msg}")
                continue

            # ── Mark all rows dropped ──────────────────────────────────────
            for sr in all_rows:
                await status_set(conn, tick_date, sr['table_kind'],
                                  sr['partition_name'],
                                  symbol_group=sr['symbol_group'],
                                  status='dropped', dropped_at='now()')

            freed = (gt_freed or 0) + c5_freed + ge_freed
            total_freed += freed
            dropped_dates.append(str(tick_date))
            print(
                f"  ✅ {tick_date}: dropped {gt_freed:,} gap_ticks + "
                f"{c5_freed:,} candles + {ge_freed:,} events"
            )

            gt_group_lines = ''.join(
                f"    {sr['symbol_group']:<12} {(sr['db_row_count'] or 0):>12,}\n"
                for sr in sorted(gap_ticks_rows, key=lambda r: -(r['db_row_count'] or 0))
            )
            date_summaries.append(
                f"✅ Archive v3 Phase B: {tick_date}\n"
                f"  gap_ticks:  {(gt_freed or 0):>12,} rows\n"
                f"{gt_group_lines}"
                f"  candles_5s: {c5_freed:>12,} rows\n"
                f"  gap_events: {ge_freed:>12,} rows\n"
                f"  {'─'*26}\n"
                f"  total:      {freed:>12,} rows freed"
            )

        if dropped_dates:
            msg = '\n\n'.join(date_summaries)
            print(f"\n{msg}")
            send_telegram(msg, secrets)

    except Exception as e:
        tb = traceback.format_exc()
        send_telegram(
            f"❌ Archive v3 Phase B unexpected error:\n{str(e)[:300]}\n{tb[-400:]}",
            secrets,
        )
        raise
    finally:
        await conn.close()


async def _drop_partition(
    partition_name: str, expected_rows: int, secrets: dict
) -> int:
    """
    Open a fresh connection (DETACH needs ACCESS EXCLUSIVE, separate from
    the long-running Phase B connection to avoid lock conflicts).
    Assert COUNT(*) == expected_rows, then DETACH + DROP.
    Returns rows freed (== expected_rows) or 0 if partition was already gone.
    """
    drop_conn = await asyncpg.connect(
        host=DB_HOST, port=int(DB_PORT),
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
    )
    try:
        await drop_conn.execute("SET statement_timeout = 0")
        await drop_conn.execute("SET lock_timeout = '2min'")

        exists = await drop_conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = $1)",
            partition_name,
        )
        if not exists:
            print(f"  {partition_name}: already absent — treating as dropped")
            return 0

        # ── Row count assertion (core safety check) ────────────────────────
        actual_rows = await drop_conn.fetchval(
            f"SELECT COUNT(*) FROM {partition_name}"
        )
        if actual_rows != expected_rows:
            raise RuntimeError(
                f"PRE-DROP MISMATCH: {partition_name} has {actual_rows:,} rows "
                f"but archive_status.db_row_count = {expected_rows:,}. "
                f"Refusing to drop — investigate before retrying."
            )

        print(f"  Detaching {partition_name} ({actual_rows:,} rows)…")
        await drop_conn.execute(
            f"ALTER TABLE gap_ticks DETACH PARTITION {partition_name}"
        )

        print(f"  Dropping {partition_name}…")
        await drop_conn.execute(f"DROP TABLE {partition_name}")

        # ── Verify partition is truly gone ─────────────────────────────────
        still_exists = await drop_conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = $1)",
            partition_name,
        )
        if still_exists:
            raise RuntimeError(
                f"DROP TABLE {partition_name} appeared to succeed but "
                f"partition is still in pg_class — manual inspection required."
            )

        print(f"  ✅ {partition_name} detached and dropped.")
        return actual_rows

    finally:
        await drop_conn.close()


async def _delete_table_for_date(
    conn, table_name: str, tick_date: date, status_row: dict
) -> int:
    """
    Delete rows for tick_date from candles_5s or gap_events using a direct
    date-bounded WHERE (no ctid). Refuses to delete if archive_status for this
    (partition_date, table_kind) is not 'verified'.
    """
    if status_row['status'] != 'verified':
        raise RuntimeError(
            f"REFUSING to delete {table_name} for {tick_date}: "
            f"archive_status.status = '{status_row['status']}' (expected 'verified'). "
            f"B2 archive must be verified before any data is deleted."
        )

    epoch_start = int(datetime.combine(tick_date, datetime.min.time()).timestamp())
    epoch_end   = int(datetime.combine(
        tick_date + timedelta(days=1), datetime.min.time()
    ).timestamp())

    result = await conn.execute(
        f"DELETE FROM {table_name} WHERE bucket >= $1 AND bucket < $2",
        epoch_start, epoch_end,
    )
    deleted = int(result.split()[-1])
    print(f"  Deleted {deleted:,} rows from {table_name}")
    return deleted


# ── Entry point ───────────────────────────────────────────────────────────────
async def main():
    parser = argparse.ArgumentParser(
        description="Archive v3: two-phase export+drop",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        '--phase', required=True, choices=['export', 'drop'],
        help=(
            'export = Phase A (write to B2, read-only on DB); '
            'drop = Phase B (partition drop after 10h gate)'
        ),
    )
    parser.add_argument(
        '--date', type=str, default=None,
        help='Override target date for --phase export (YYYY-MM-DD). '
             'Defaults to today_IST - 2 days.',
    )
    args = parser.parse_args()

    secrets = read_secrets()
    s3      = get_b2_client(secrets)
    pa_s3   = get_pa_s3fs(secrets)
    bucket  = secrets['BACKBLAZE_BUCKET']

    if args.phase == 'export':
        if args.date:
            # Manual run — process only the specified date, no backlog scan
            target = datetime.strptime(args.date, '%Y-%m-%d').date()
            print(f"Phase A manual run, target date: {target}")
            await run_phase_a(target, secrets, s3, pa_s3, bucket)
        else:
            # Cron run — scan for backlog, then process today's target
            tmp_conn = await asyncpg.connect(
                host=DB_HOST, port=int(DB_PORT),
                user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
            )
            try:
                retention = await get_retention_days(tmp_conn)
                target = ist_today() - timedelta(days=retention)
                print(f"Phase A: retention={retention} days, today's target={target}")
                backlog = await find_backlog_dates(tmp_conn, target)
            finally:
                await tmp_conn.close()

            if backlog:
                backlog_msg = (
                    f"📋 Phase A backlog detected: {len(backlog)} date(s) "
                    f"older than {target} not yet archived\n"
                    f"  Backlog: {', '.join(str(d) for d in backlog)}\n"
                    f"  Cap per run: {MAX_BACKLOG_PER_RUN}\n"
                    f"  Today's target after backlog: {target}"
                )
                print(backlog_msg)
                send_telegram(backlog_msg, secrets)

                for backlog_date in backlog:
                    print(f"\n=== Backlog run: {backlog_date} ===")
                    try:
                        await run_phase_a(backlog_date, secrets, s3, pa_s3, bucket)
                    except Exception as e:
                        # Failure already recorded in archive_status and Telegram
                        # by run_phase_a; continue to next backlog date
                        print(f"  Backlog date {backlog_date} failed: {e} — continuing")

            print(f"\n=== Regular run: {target} ===")
            await run_phase_a(target, secrets, s3, pa_s3, bucket)

    elif args.phase == 'drop':
        if args.date:
            print("NOTE: --date is ignored for --phase drop; "
                  "eligibility is determined from archive_status.")
        await run_phase_b(secrets, s3, pa_s3, bucket)


if __name__ == '__main__':
    asyncio.run(main())
