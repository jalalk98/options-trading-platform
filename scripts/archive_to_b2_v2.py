#!/usr/bin/env python3
"""
Archive v2: PostgreSQL → Parquet → Backblaze B2
- Raw columns only (no calculated columns)
- Hive-partitioned folder structure
- symbol_group partitioning for gap_ticks
- Cursor-based streaming export

Usage:
  python3 scripts/archive_to_b2_v2.py --date 2026-04-06
  python3 scripts/archive_to_b2_v2.py --dates 2026-04-06 2026-04-07
  python3 scripts/archive_to_b2_v2.py --date 2026-04-06 --force
  python3 scripts/archive_to_b2_v2.py --date 2026-04-06 --dry-run
"""

import sys
import os
import json
import hashlib
import argparse
import asyncio
import asyncpg
import boto3
import tempfile
import time
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.client import Config
from pathlib import Path
from datetime import datetime, date, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.credentials import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)

# ── Raw columns to keep for gap_ticks ─────────────────────────
GAP_TICKS_RAW_COLS = [
    'instrument_token', 'symbol', 'expiry_date', 'strike',
    'option_type', 'timestamp', 'last_trade_time',
    'curr_price', 'last_quantity', 'average_price',
    'curr_volume', 'oi', 'oi_day_high', 'oi_day_low',
    'buy_quantity', 'sell_quantity', 'depth',
    'bid_depth_qty', 'ask_depth_qty',
    'best_bid', 'best_ask',
]

# candles_5s and gap_events keep all columns
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

# ── Symbol grouping (mirrors tick collector logic) ────────────
def get_symbol_group(symbol):
    """Classify symbol into partition group."""
    if symbol.startswith('BANKNIFTY'):
        return 'BANKNIFTY'
    if symbol.startswith('MIDCPNIFTY'):
        return 'MIDCPNIFTY'
    if symbol.startswith('FINNIFTY'):
        return 'FINNIFTY'
    if symbol.startswith('NIFTY'):
        return 'NIFTY'
    if symbol.startswith('SENSEX'):
        return 'SENSEX'
    return 'OTHER'


# ── Read Backblaze credentials ────────────────────────────────
def read_secrets():
    secrets = {}
    path = Path.home() / '.kite_secrets'
    for line in path.read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            k, _, v = line.partition('=')
            secrets[k.strip()] = v.strip()
    return secrets


def get_b2_client(secrets):
    return boto3.client(
        's3',
        endpoint_url=f"https://{secrets['BACKBLAZE_ENDPOINT']}",
        aws_access_key_id=secrets['BACKBLAZE_KEY_ID'],
        aws_secret_access_key=secrets['BACKBLAZE_APP_KEY'],
        config=Config(signature_version='s3v4'),
    )


def send_telegram(text, secrets):
    try:
        import requests
        token = secrets.get('TELEGRAM_BOT_TOKEN')
        chat_id = secrets.get('TELEGRAM_CHAT_ID')
        if not token or not chat_id:
            return
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": text},
            timeout=5,
        )
    except Exception as e:
        print(f"Telegram alert failed: {e}")


# ── Export gap_ticks: stream to per-symbol-group parquet files ─
async def export_gap_ticks(conn, tick_date, tmp_dir):
    """
    Export gap_ticks for tick_date, split by symbol_group.
    Writes one parquet file per symbol_group in tmp_dir.
    Returns dict: {symbol_group: (file_path, row_count, size_bytes)}
    """
    cols = ', '.join(GAP_TICKS_RAW_COLS)
    where = (
        f"timestamp >= '{tick_date}'::date "
        f"AND timestamp < '{tick_date}'::date + INTERVAL '1 day'"
    )

    CHUNK = 500_000
    last_cursor = None
    total_done = 0
    start = time.time()

    # Per-group writers — created lazily on first row for that group
    writers = {}   # symbol_group -> pyarrow.parquet.ParquetWriter
    counts  = {}   # symbol_group -> row count
    schema  = None

    try:
        while True:
            if last_cursor is None:
                cursor_where = where
            else:
                cursor_where = (
                    f"{where} AND timestamp > "
                    f"'{last_cursor}'::timestamp"
                )

            chunk_rows = await conn.fetch(
                f"SELECT {cols} FROM gap_ticks "
                f"WHERE {cursor_where} "
                f"ORDER BY timestamp ASC "
                f"LIMIT {CHUNK}"
            )

            if not chunk_rows:
                break

            # Convert rows to dict of column arrays
            data = {col: [] for col in GAP_TICKS_RAW_COLS}
            groups = []
            for row in chunk_rows:
                for col in GAP_TICKS_RAW_COLS:
                    val = row[col]
                    # Convert JSONB (depth) to string for parquet
                    if col == 'depth' and val is not None:
                        val = json.dumps(val) if not isinstance(val, str) else val
                    data[col].append(val)
                groups.append(get_symbol_group(row['symbol']))

            # Build pyarrow table
            table = pa.table(data)

            # Split by symbol_group and write
            for grp in set(groups):
                mask = [g == grp for g in groups]
                grp_table = table.filter(pa.array(mask))

                if grp not in writers:
                    file_path = Path(tmp_dir) / f"{grp}.parquet"
                    writers[grp] = pq.ParquetWriter(
                        file_path, grp_table.schema,
                        compression='snappy',
                    )
                    counts[grp] = 0

                writers[grp].write_table(grp_table)
                counts[grp] += grp_table.num_rows

            last_cursor = chunk_rows[-1]['timestamp']
            total_done += len(chunk_rows)

            elapsed = time.time() - start
            rate = total_done / elapsed if elapsed > 0 else 0
            print(
                f"    {total_done:,} rows | {rate:,.0f} rows/s "
                f"| {elapsed:.0f}s elapsed     ",
                end='\r', flush=True
            )

            del chunk_rows

    finally:
        # Close all writers
        for w in writers.values():
            w.close()

    print(f"\n    {total_done:,} gap_ticks rows exported "
          f"in {(time.time()-start)/60:.1f} min")

    # Build result dict
    result = {}
    for grp, cnt in counts.items():
        file_path = Path(tmp_dir) / f"{grp}.parquet"
        if file_path.exists() and cnt > 0:
            result[grp] = (
                str(file_path),
                cnt,
                file_path.stat().st_size,
            )
    return result


# ── Export candles_5s or gap_events to single parquet ────────
async def export_simple_table(conn, table_name, tick_date, tmp_dir, cols):
    """Export candles_5s or gap_events to a single parquet file."""
    cols_str = ', '.join(cols)
    where = (
        f"bucket >= EXTRACT(EPOCH FROM '{tick_date}'::date)::BIGINT "
        f"AND bucket < EXTRACT(EPOCH FROM '{tick_date}'::date "
        f"             + INTERVAL '1 day')::BIGINT"
    )

    CHUNK = 500_000
    last_cursor = 0
    total_done = 0
    start = time.time()
    writer = None
    file_path = Path(tmp_dir) / f"{table_name}.parquet"

    try:
        while True:
            cursor_where = f"{where} AND bucket > {last_cursor}"
            chunk_rows = await conn.fetch(
                f"SELECT {cols_str} FROM {table_name} "
                f"WHERE {cursor_where} "
                f"ORDER BY bucket ASC "
                f"LIMIT {CHUNK}"
            )
            if not chunk_rows:
                break

            data = {col: [row[col] for row in chunk_rows] for col in cols}
            table = pa.table(data)

            if writer is None:
                writer = pq.ParquetWriter(
                    file_path, table.schema, compression='snappy'
                )

            writer.write_table(table)

            last_cursor = chunk_rows[-1]['bucket']
            total_done += len(chunk_rows)

            elapsed = time.time() - start
            rate = total_done / elapsed if elapsed > 0 else 0
            print(
                f"    {total_done:,} rows | {rate:,.0f} rows/s     ",
                end='\r', flush=True
            )
            del chunk_rows
    finally:
        if writer:
            writer.close()

    print(f"\n    {total_done:,} {table_name} rows exported "
          f"in {(time.time()-start)/60:.1f} min")

    if total_done == 0 or not file_path.exists():
        return None, 0, 0
    return str(file_path), total_done, file_path.stat().st_size


# ── Batched DELETE ────────────────────────────────────────────
async def delete_rows_batched(conn, table_name, tick_date):
    """Delete rows for tick_date in batches of 50k."""
    print(f"  Deleting {table_name} rows for {tick_date}...")

    if table_name == 'gap_ticks':
        where = (
            f"timestamp >= '{tick_date}'::date "
            f"AND timestamp < '{tick_date}'::date + INTERVAL '1 day'"
        )
    else:
        where = (
            f"bucket >= EXTRACT(EPOCH FROM '{tick_date}'::date)::BIGINT "
            f"AND bucket < EXTRACT(EPOCH FROM '{tick_date}'::date "
            f"             + INTERVAL '1 day')::BIGINT"
        )

    BATCH = 50_000
    total = 0
    start = time.time()

    while True:
        result = await conn.execute(
            f"DELETE FROM {table_name} "
            f"WHERE ctid IN ("
            f"  SELECT ctid FROM {table_name} "
            f"  WHERE {where} LIMIT {BATCH}"
            f")"
        )
        deleted = int(result.split()[-1])
        total += deleted
        if deleted == 0:
            break
        elapsed = time.time() - start
        rate = total / elapsed if elapsed > 0 else 0
        print(
            f"    Deleted {total:,} | {rate:,.0f} rows/s     ",
            end='\r', flush=True
        )

    print(f"\n    Deleted {total:,} rows from {table_name} "
          f"in {(time.time()-start)/60:.1f} min")
    return total


# ── Upload helpers ───────────────────────────────────────────
def upload_file_to_b2(s3, bucket, local_path, b2_key):
    with open(local_path, 'rb') as f:
        s3.put_object(Bucket=bucket, Key=b2_key, Body=f.read())
    print(f"    Uploaded: {b2_key}")


def md5_of_file(path):
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


# ── Build B2 key paths (Hive-partitioned) ────────────────────
def b2_key_gap_ticks(symbol_group, d):
    return (
        f"raw_ticks/"
        f"symbol_group={symbol_group}/"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        f"data.parquet"
    )


def b2_key_simple(table, d):
    return (
        f"{table}/"
        f"year={d.year}/month={d.month:02d}/day={d.day:02d}/"
        f"data.parquet"
    )


# ── Update manifest ───────────────────────────────────────────
def update_manifest(s3, bucket, date_str, table_name, entries):
    """
    entries: list of dicts with keys
             {b2_key, rows, size_bytes, md5, symbol_group?}
    """
    manifest = {}
    try:
        obj = s3.get_object(Bucket=bucket, Key='manifest_v2.json')
        manifest = json.loads(obj['Body'].read())
    except Exception:
        pass

    manifest.setdefault(date_str, {})[table_name] = {
        'files': entries,
        'archived_at': datetime.now().isoformat(),
        'version': 'v2',
    }

    s3.put_object(
        Bucket=bucket, Key='manifest_v2.json',
        Body=json.dumps(manifest, indent=2).encode(),
    )


# ── Retention config + auto date discovery ───────────────────
async def get_retention_config(conn):
    rows = await conn.fetch(
        "SELECT symbol_type, retain_days FROM retention_config"
    )
    return {r['symbol_type']: r['retain_days'] for r in rows}


async def get_dates_to_archive(conn, config, force=False):
    """Return dates that exceed retention and haven't been archived yet."""
    min_retain = min(config.values())
    earliest_possible = date.today() - timedelta(days=min_retain)

    if force:
        # Force mode: get all dates older than retention, including
        # ones already in archive_log. Used to re-archive after
        # retention config change or format migration.
        rows = await conn.fetch("""
            SELECT DISTINCT timestamp::date AS tick_date
            FROM gap_ticks
            WHERE timestamp::date < $1
            ORDER BY tick_date ASC
        """, earliest_possible)
    else:
        rows = await conn.fetch("""
            SELECT DISTINCT timestamp::date AS tick_date
            FROM gap_ticks
            WHERE timestamp::date < $1
              AND timestamp::date NOT IN (
                  SELECT archive_date FROM archive_log
                  WHERE status = 'completed'
                    AND table_name = 'gap_ticks'
              )
            ORDER BY tick_date ASC
        """, earliest_possible)

    return [r['tick_date'] for r in rows]


async def drop_archived_partition(tick_date, secrets):
    """
    Detach and DROP a fully-archived gap_ticks partition.
    Opens a fresh connection because DETACH PARTITION acquires
    ACCESS EXCLUSIVE lock incompatible with the archiving connection.

    Returns True on success, False if partition missing, has rows, or DROP fails.
    """
    partition_name = f"gap_ticks_{str(tick_date).replace('-', '_')}"

    drop_conn = await asyncpg.connect(
        host=DB_HOST, port=int(DB_PORT),
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
    )
    try:
        exists = await drop_conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_class WHERE relname = $1)",
            partition_name,
        )
        if not exists:
            print(f"  drop_partition: {partition_name} not found, skipping")
            return False

        row_count = await drop_conn.fetchval(
            f"SELECT COUNT(*) FROM {partition_name}"
        )
        if row_count > 0:
            msg = (
                f"REFUSING to DROP {partition_name}: "
                f"{row_count} rows still present — DELETE may have failed"
            )
            print(f"  drop_partition: {msg}")
            send_telegram(f"⚠️ Archive WARN: {msg}", secrets)
            return False

        await drop_conn.execute(
            f"ALTER TABLE gap_ticks DETACH PARTITION {partition_name}"
        )
        await drop_conn.execute(f"DROP TABLE {partition_name}")
        print(f"  Dropped partition {partition_name} — disk reclaimed")
        return True

    except Exception as e:
        print(f"  drop_partition: FAILED for {partition_name}: {e}")
        send_telegram(f"❌ Archive ERROR: DROP {partition_name} failed: {e}", secrets)
        return False
    finally:
        await drop_conn.close()


# ── Main archive runner ──────────────────────────────────────
async def run_archive(dates=None, dry_run=False, force=False):
    secrets = read_secrets()
    s3 = get_b2_client(secrets)
    bucket = secrets['BACKBLAZE_BUCKET']

    conn = await asyncpg.connect(
        host=DB_HOST, port=int(DB_PORT),
        user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME,
    )

    try:
        await conn.execute("SET statement_timeout = 0")

        if dates is None:
            config = await get_retention_config(conn)
            dates = await get_dates_to_archive(conn, config, force=force)
            if not dates:
                print("✅ Nothing to archive — all data within retention")
                return
            print(f"Auto-detected dates to archive: {[str(d) for d in dates]}")

        if dry_run:
            print(f"DRY RUN — would archive: {[str(d) for d in dates]}")
            return

        total_rows = 0
        total_size = 0
        archived_dates = []
        table_totals = {
            'gap_ticks':  {'rows': 0, 'size': 0},
            'candles_5s': {'rows': 0, 'size': 0},
            'gap_events': {'rows': 0, 'size': 0},
        }

        for tick_date in dates:
            print(f"\n━━━ Archiving {tick_date} ━━━")
            date_rows = 0
            date_size = 0

            with tempfile.TemporaryDirectory(prefix='archive_') as tmp_dir:
                # ── gap_ticks (split by symbol_group) ─────────
                try:
                    await conn.execute("""
                        INSERT INTO archive_log
                            (archive_date, table_name, status)
                        VALUES ($1, 'gap_ticks', 'running')
                        ON CONFLICT (archive_date, table_name)
                        DO UPDATE SET
                            status='running',
                            started_at=NOW(),
                            error_message=NULL,
                            completed_at=NULL
                    """, tick_date)

                    print(f"  Exporting gap_ticks...")
                    groups = await export_gap_ticks(conn, tick_date, tmp_dir)

                    if not groups:
                        print(f"    No gap_ticks for {tick_date}")
                        await conn.execute("""
                            UPDATE archive_log
                            SET status='skipped', completed_at=NOW()
                            WHERE archive_date=$1 AND table_name='gap_ticks'
                        """, tick_date)
                    else:
                        entries = []
                        for grp, (fpath, rows, size) in groups.items():
                            b2_key = b2_key_gap_ticks(grp, tick_date)
                            md5 = md5_of_file(fpath)
                            upload_file_to_b2(s3, bucket, fpath, b2_key)
                            entries.append({
                                'b2_key': b2_key,
                                'symbol_group': grp,
                                'rows': rows,
                                'size_bytes': size,
                                'md5': md5,
                            })
                            date_rows += rows
                            date_size += size

                        update_manifest(s3, bucket, str(tick_date),
                                        'gap_ticks', entries)

                        total_g_rows = sum(e['rows'] for e in entries)
                        total_g_size = sum(e['size_bytes'] for e in entries)
                        table_totals['gap_ticks']['rows'] += total_g_rows
                        table_totals['gap_ticks']['size'] += total_g_size

                        await delete_rows_batched(conn, 'gap_ticks', tick_date)

                        await conn.execute("""
                            UPDATE archive_log SET
                                status='completed',
                                rows_archived=$2,
                                file_size_bytes=$3,
                                completed_at=NOW()
                            WHERE archive_date=$1 AND table_name='gap_ticks'
                        """, tick_date, total_g_rows, total_g_size)

                        # Drop the now-empty partition to reclaim disk space
                        await drop_archived_partition(tick_date, secrets)

                except Exception as e:
                    print(f"  ❌ gap_ticks failed: {e}")
                    await conn.execute("""
                        UPDATE archive_log SET
                            status='failed',
                            error_message=$2,
                            completed_at=NOW()
                        WHERE archive_date=$1 AND table_name='gap_ticks'
                    """, tick_date, str(e))

                # ── candles_5s and gap_events ─────────────────
                for tbl, cols in [
                    ('candles_5s', CANDLES_5S_COLS),
                    ('gap_events', GAP_EVENTS_COLS),
                ]:
                    try:
                        await conn.execute("""
                            INSERT INTO archive_log
                                (archive_date, table_name, status)
                            VALUES ($1, $2, 'running')
                            ON CONFLICT (archive_date, table_name)
                            DO UPDATE SET
                                status='running',
                                started_at=NOW(),
                                error_message=NULL,
                                completed_at=NULL
                        """, tick_date, tbl)

                        print(f"  Exporting {tbl}...")
                        fpath, rows, size = await export_simple_table(
                            conn, tbl, tick_date, tmp_dir, cols
                        )

                        if fpath is None:
                            print(f"    No {tbl} for {tick_date}")
                            await conn.execute("""
                                UPDATE archive_log
                                SET status='skipped', completed_at=NOW()
                                WHERE archive_date=$1 AND table_name=$2
                            """, tick_date, tbl)
                            continue

                        b2_key = b2_key_simple(tbl, tick_date)
                        md5 = md5_of_file(fpath)
                        upload_file_to_b2(s3, bucket, fpath, b2_key)

                        update_manifest(s3, bucket, str(tick_date), tbl, [{
                            'b2_key': b2_key,
                            'rows': rows,
                            'size_bytes': size,
                            'md5': md5,
                        }])

                        date_rows += rows
                        date_size += size
                        table_totals[tbl]['rows'] += rows
                        table_totals[tbl]['size'] += size

                        await delete_rows_batched(conn, tbl, tick_date)

                        await conn.execute("""
                            UPDATE archive_log SET
                                status='completed',
                                rows_archived=$3,
                                file_size_bytes=$4,
                                b2_file_key=$5,
                                checksum_md5=$6,
                                completed_at=NOW()
                            WHERE archive_date=$1 AND table_name=$2
                        """, tick_date, tbl, rows, size, b2_key, md5)

                    except Exception as e:
                        print(f"  ❌ {tbl} failed: {e}")
                        await conn.execute("""
                            UPDATE archive_log SET
                                status='failed',
                                error_message=$3,
                                completed_at=NOW()
                            WHERE archive_date=$1 AND table_name=$2
                        """, tick_date, tbl, str(e))

            archived_dates.append(str(tick_date))
            total_rows += date_rows
            total_size += date_size
            print(f"  ✅ {tick_date}: {date_rows:,} rows, "
                  f"{date_size/1024/1024:.1f} MB")

        if archived_dates:
            tbl_lines = []
            tbl_labels = {
                'gap_ticks':  'gap_ticks ',
                'candles_5s': 'candles_5s',
                'gap_events': 'gap_events',
            }
            for tbl_name, stats in table_totals.items():
                if stats['rows'] > 0:
                    tbl_lines.append(
                        f"  {tbl_labels[tbl_name]}: "
                        f"{stats['rows']:,} rows  "
                        f"({stats['size']/1024/1024:.1f} MB)"
                    )
            send_telegram(
                f"✅ Archive v2 completed\n"
                f"Dates: {', '.join(archived_dates)}\n"
                + '\n'.join(tbl_lines) + '\n'
                f"Total: {total_rows:,} rows  "
                f"({total_size/1024/1024:.1f} MB)",
                secrets,
            )

        print("\nRunning VACUUM ANALYZE...")
        try:
            vac_conn = await asyncpg.connect(
                host=DB_HOST, port=int(DB_PORT),
                user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
            )
            await vac_conn.execute("SET statement_timeout = 0")
            await vac_conn.execute("VACUUM ANALYZE gap_ticks")
            await vac_conn.execute("VACUUM ANALYZE candles_5s")
            await vac_conn.execute("VACUUM ANALYZE gap_events")
            await vac_conn.close()
            print("✅ VACUUM done")
        except Exception as e:
            print(f"⚠️ VACUUM failed: {e}")

    finally:
        await conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--date', type=str, default=None)
    parser.add_argument('--dates', type=str, nargs='+', default=None)
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()

    if args.dates:
        dates = [datetime.strptime(d, '%Y-%m-%d').date() for d in args.dates]
    elif args.date:
        dates = [datetime.strptime(args.date, '%Y-%m-%d').date()]
    else:
        dates = None  # auto-discover from retention config

    asyncio.run(run_archive(dates, dry_run=args.dry_run, force=args.force))
