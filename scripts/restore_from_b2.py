#!/usr/bin/env python3
"""
Restore archived tick data from Backblaze B2
back into PostgreSQL for testing/analysis.

Usage:
  # Restore to test tables (safe)
  python3 scripts/restore_from_b2.py --date 2026-03-16

  # List what is available in B2
  python3 scripts/restore_from_b2.py --list

  # Clean up test tables
  python3 scripts/restore_from_b2.py --cleanup 2026-03-16

  # Restore to main tables (permanent)
  python3 scripts/restore_from_b2.py --date 2026-03-16 --permanent
"""

import sys
import os
import gzip
import csv
import json
import argparse
import asyncio
import asyncpg
import boto3
from botocore.client import Config
from pathlib import Path
from datetime import datetime, date
from io import StringIO

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.credentials import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)

# ── Read credentials ──────────────────────────────────────────
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
        endpoint_url          = f"https://{secrets['BACKBLAZE_ENDPOINT']}",
        aws_access_key_id     = secrets['BACKBLAZE_KEY_ID'],
        aws_secret_access_key = secrets['BACKBLAZE_APP_KEY'],
        config                = Config(signature_version='s3v4'),
    )

# ── List available archives in B2 ────────────────────────────
def list_available(s3, bucket):
    try:
        obj      = s3.get_object(Bucket=bucket, Key='manifest.json')
        manifest = json.loads(obj['Body'].read())
        print(f"\n{'Date':<15} {'Table':<15} {'Rows':>12} {'Size':>10}")
        print("─" * 55)
        total_rows = 0
        total_size = 0
        for d in sorted(manifest.keys()):
            for table, info in manifest[d].items():
                rows = info.get('rows', 0)
                size = info.get('size_bytes', 0)
                print(f"{d:<15} {table:<15} "
                      f"{rows:>12,} "
                      f"{size/1024/1024:>9.1f}MB")
                total_rows += rows
                total_size += size
        print("─" * 55)
        print(f"{'TOTAL':<15} {'':<15} "
              f"{total_rows:>12,} "
              f"{total_size/1024/1024:>9.1f}MB")
        return manifest
    except Exception as e:
        print(f"❌ Could not read manifest: {e}")
        return {}

# ── Download and decompress from B2 ──────────────────────────
def download_from_b2(s3, bucket, b2_key):
    print(f"  Downloading {b2_key}...")
    try:
        obj      = s3.get_object(Bucket=bucket, Key=b2_key)
        gz_bytes = obj['Body'].read()
        size_mb  = len(gz_bytes) / 1024 / 1024
        csv_bytes = gzip.decompress(gz_bytes)
        print(f"  Downloaded: {size_mb:.1f} MB compressed"
              f" → {len(csv_bytes)/1024/1024:.1f} MB uncompressed")
        del gz_bytes  # free compressed bytes immediately
        return csv_bytes.decode('utf-8')
    except Exception as e:
        if 'NoSuchKey' in str(e) or '404' in str(e):
            print(f"  ⚠️  Not found in B2: {b2_key}")
            return None
        raise

# ── Get columns for a table ───────────────────────────────────
async def get_table_columns(conn, table_name):
    rows = await conn.fetch("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name   = $1
        ORDER BY ordinal_position
    """, table_name)
    return [(r['column_name'], r['data_type']) for r in rows]

# ── Create test table mirroring main table ────────────────────
async def create_test_table(conn, table_name, test_table):
    # Drop if exists
    await conn.execute(
        f"DROP TABLE IF EXISTS {test_table}"
    )
    # Create as copy of main table structure
    await conn.execute(f"""
        CREATE TABLE {test_table}
        (LIKE {table_name} INCLUDING ALL)
    """)
    print(f"  Created test table: {test_table}")

# ── Load CSV into table using streaming batches ───────────────
async def load_csv_to_table(conn, table_name, csv_text, columns):
    """
    Load CSV into table in streaming batches.
    Never holds full dataset in memory — RAM stays flat.
    """
    import io
    col_names = [c[0] for c in columns if c[0] != 'id']

    col_str      = ', '.join(col_names)
    placeholders = ', '.join(
        f'${i+1}' for i in range(len(col_names))
    )
    insert_q = (
        f"INSERT INTO {table_name} "
        f"({col_str}) VALUES ({placeholders})"
        f" ON CONFLICT DO NOTHING"
    )

    reader        = csv.DictReader(io.StringIO(csv_text))
    batch_records = []
    total         = 0
    skipped       = 0
    BATCH         = 500  # small batch = flat RAM

    for row in reader:
        try:
            record = []
            for col_name, col_type in columns:
                if col_name == 'id':
                    continue
                val = row.get(col_name, '')
                if val == '' or val == 'None' or val is None:
                    record.append(None)
                elif 'timestamp' in col_type or col_type == 'date':
                    if val:
                        try:
                            record.append(datetime.fromisoformat(val))
                        except Exception:
                            record.append(None)
                    else:
                        record.append(None)
                elif col_type in ('bigint', 'integer'):
                    record.append(int(float(val)) if val else None)
                elif col_type in ('double precision', 'real', 'numeric'):
                    record.append(float(val) if val else None)
                elif col_type == 'boolean':
                    record.append(
                        val.lower() in ('true', 't', '1') if val else None
                    )
                elif col_type == 'jsonb':
                    record.append(val if val else None)
                else:
                    record.append(val if val else None)
            batch_records.append(record)
        except Exception:
            skipped += 1
            continue

        if len(batch_records) >= BATCH:
            await conn.executemany(insert_q, batch_records)
            total += len(batch_records)
            batch_records = []
            if total % 50000 == 0:
                print(f"  Loaded {total:,} rows...", end='\r')

    # Flush remaining
    if batch_records:
        await conn.executemany(insert_q, batch_records)
        total += len(batch_records)

    print(f"  ✅ Loaded {total:,} rows (skipped {skipped})")
    if skipped > 0:
        print(f"  ⚠️  {skipped} rows skipped due to errors")
    return total

# ── Log restore to DB ─────────────────────────────────────────
async def log_restore(conn, restore_date, table_name,
                      restored_table, rows, b2_key,
                      status, session_id=None, error=None):
    await conn.execute("""
        INSERT INTO restore_log (
            restore_date, table_name, restored_table,
            rows_restored, b2_file_key, status,
            error_message, session_id, completed_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
    """, restore_date, table_name, restored_table,
        rows, b2_key, status, error, session_id)

# ── Cleanup test tables ───────────────────────────────────────
async def cleanup_test_tables(conn, restore_date):
    date_str = str(restore_date).replace('-', '')
    tables   = ['gap_ticks', 'candles_5s', 'gap_events']
    for table in tables:
        test_table = f"{table}_test_{date_str}"
        result = await conn.execute(
            f"DROP TABLE IF EXISTS {test_table}"
        )
        print(f"  Dropped: {test_table}")
        await conn.execute("""
            DELETE FROM restore_log
            WHERE restore_date = $1
              AND table_name   = $2
        """, restore_date, table)
    print(f"✅ Cleanup complete for {restore_date}")

# ── Main restore function ─────────────────────────────────────
async def run_restore(restore_date, permanent=False):
    secrets = read_secrets()
    s3      = get_b2_client(secrets)
    bucket  = secrets['BACKBLAZE_BUCKET']

    conn = await asyncpg.connect(
        host     = DB_HOST,
        port     = int(DB_PORT),
        user     = DB_USER,
        password = DB_PASSWORD,
        database = DB_NAME,
    )

    try:
        await conn.execute("SET statement_timeout = 0")
        import uuid
        session_id = str(uuid.uuid4())[:8]
        print(f"Restore session: {session_id}")
        date_str  = str(restore_date).replace('-', '')
        tables    = ['gap_ticks', 'candles_5s', 'gap_events']

        print(f"\nRestoring data for {restore_date}")
        print(f"Mode: {'PERMANENT' if permanent else 'TEST'}")
        if not permanent:
            print(f"Test tables will be created with "
                  f"_test_{date_str} suffix")
        print()

        total_rows = 0

        for table in tables:
            b2_key = f"{table}/{restore_date}.csv.gz"
            print(f"Processing {table}...")

            # Download from B2
            csv_text = download_from_b2(s3, bucket, b2_key)
            if csv_text is None:
                await log_restore(
                    conn, restore_date, table,
                    'N/A', 0, b2_key, 'not_found',
                    session_id=session_id
                )
                continue

            # Determine target table
            if permanent:
                target_table = table
            else:
                target_table = f"{table}_test_{date_str}"
                await create_test_table(
                    conn, table, target_table
                )

            # Get columns
            columns = await get_table_columns(conn, table)

            # Load data
            try:
                rows = await load_csv_to_table(
                    conn, target_table, csv_text, columns
                )
                await log_restore(
                    conn, restore_date, table,
                    target_table, rows, b2_key, 'completed',
                    session_id=session_id
                )
                total_rows += rows
                print(f"  ✅ {table} → {target_table}: "
                      f"{rows:,} rows")

            except Exception as e:
                print(f"  ❌ {table} failed: {e}")
                await log_restore(
                    conn, restore_date, table,
                    target_table, 0, b2_key,
                    'failed', session_id=session_id,
                    error=str(e)
                )

        # Permanent restore: clear archive_log so the date shows
        # as eligible to archive again in the Archive Manager
        if permanent and total_rows > 0:
            await conn.execute("""
                DELETE FROM archive_log
                WHERE archive_date = $1
            """, restore_date)
            print(f"  archive_log cleared for {restore_date} — "
                  f"date will appear as eligible to archive")

        print(f"\n{'━'*50}")
        print(f"✅ RESTORE COMPLETE")
        print(f"   Date       : {restore_date}")
        print(f"   Total rows : {total_rows:,}")
        print(f"   Mode       : "
              f"{'PERMANENT' if permanent else 'TEST'}")
        if not permanent:
            print(f"\nTest tables created:")
            for t in tables:
                print(f"  {t}_test_{date_str}")
            print(f"\nTo query test data:")
            print(f"  SELECT * FROM "
                  f"gap_ticks_test_{date_str} LIMIT 10;")
            print(f"\nTo clean up when done:")
            print(f"  python3 scripts/restore_from_b2.py "
                  f"--cleanup {restore_date}")

    finally:
        await conn.close()

# ── Entry point ───────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Restore archived data from Backblaze B2'
    )
    parser.add_argument(
        '--date', type=str,
        help='Date to restore (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--list', action='store_true',
        help='List available archives in B2'
    )
    parser.add_argument(
        '--cleanup', type=str,
        help='Remove test tables for a date (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--permanent', action='store_true',
        help='Restore to main tables instead of test tables'
    )
    args = parser.parse_args()

    secrets = read_secrets()
    s3      = get_b2_client(secrets)
    bucket  = secrets['BACKBLAZE_BUCKET']

    if args.list:
        list_available(s3, bucket)

    elif args.cleanup:
        cleanup_date = datetime.strptime(
            args.cleanup, '%Y-%m-%d'
        ).date()
        async def do_cleanup():
            conn = await asyncpg.connect(
                host=DB_HOST, port=int(DB_PORT),
                user=DB_USER, password=DB_PASSWORD,
                database=DB_NAME,
            )
            await cleanup_test_tables(conn, cleanup_date)
            await conn.close()
        asyncio.run(do_cleanup())

    elif args.date:
        restore_date = datetime.strptime(
            args.date, '%Y-%m-%d'
        ).date()
        asyncio.run(run_restore(
            restore_date,
            permanent=args.permanent
        ))

    else:
        parser.print_help()
