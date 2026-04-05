#!/usr/bin/env python3
"""
Archive old tick data to Backblaze B2.

Usage:
  python3 scripts/archive_to_b2.py           # auto mode
  python3 scripts/archive_to_b2.py --dry-run # show what would be archived
  python3 scripts/archive_to_b2.py --date 2026-03-01  # archive specific date
"""

import sys
import os
import gzip
import csv
import json
import hashlib
import argparse
import asyncio
import asyncpg
import boto3
from botocore.client import Config
from pathlib import Path
from datetime import datetime, date, timedelta
from io import StringIO

sys.path.insert(0, str(Path(__file__).parent.parent))
from config.credentials import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)


# ── Read Backblaze credentials ────────────────────────────────
def read_secrets():
    secrets = {}
    path = Path.home() / '.kite_secrets'
    for line in path.read_text().splitlines():
        if '=' in line and not line.startswith('#'):
            k, _, v = line.partition('=')
            secrets[k.strip()] = v.strip()
    return secrets


# ── Backblaze B2 client ───────────────────────────────────────
def get_b2_client(secrets):
    return boto3.client(
        's3',
        endpoint_url          = f"https://{secrets['BACKBLAZE_ENDPOINT']}",
        aws_access_key_id     = secrets['BACKBLAZE_KEY_ID'],
        aws_secret_access_key = secrets['BACKBLAZE_APP_KEY'],
        config                = Config(signature_version='s3v4'),
    )


# ── Telegram alert ────────────────────────────────────────────
def send_telegram(text, secrets):
    try:
        import requests
        token   = secrets.get('TELEGRAM_BOT_TOKEN')
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


# ── Symbol classification ─────────────────────────────────────
def get_symbol_type(symbol):
    """Classify symbol into retention config category."""
    has_option = 'CE' in symbol or 'PE' in symbol

    if symbol.startswith('NIFTY'):
        return 'NIFTY_OPTIONS' if has_option else 'NIFTY_INDEX'
    elif symbol.startswith('SENSEX'):
        return 'SENSEX_OPTIONS' if has_option else 'SENSEX_INDEX'
    elif symbol.startswith('BANKNIFTY'):
        return 'BANKNIFTY_OPTIONS' if has_option else 'BANKNIFTY_INDEX'
    elif symbol.startswith('MIDCPNIFTY'):
        return 'MIDCPNIFTY_OPTIONS' if has_option else 'MIDCPNIFTY_INDEX'
    elif symbol.startswith('FINNIFTY'):
        return 'FINNIFTY_OPTIONS' if has_option else 'FINNIFTY_INDEX'
    return 'DEFAULT'


# ── Retention helpers ─────────────────────────────────────────
async def get_retention_config(conn):
    rows = await conn.fetch(
        "SELECT symbol_type, retain_days FROM retention_config"
    )
    return {r['symbol_type']: r['retain_days'] for r in rows}


async def get_dates_to_archive(conn, config, specific_date=None, force=False):
    """
    Find dates that should be archived based on retention config.
    Returns list of dates to archive.
    """
    if specific_date:
        if force:
            return [specific_date]
        return [specific_date]

    min_retain = min(config.values())
    max_retain = max(config.values())
    # Use min_retain so we catch all dates that need archiving
    # for shorter-retain types (e.g. BANKNIFTY 14d vs NIFTY 16d)
    earliest_possible = date.today() - timedelta(days=min_retain)

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


# ── Build per-symbol WHERE conditions ────────────────────────
def _symbol_conditions(config, tick_date):
    """
    Return list of SQL conditions for symbols whose retention
    period has expired as of tick_date.
    """
    conditions = []
    for sym_type, days in config.items():
        cutoff = date.today() - timedelta(days=days)
        if tick_date >= cutoff:
            continue  # still within retention
        if sym_type == 'NIFTY_OPTIONS':
            conditions.append(
                "(symbol LIKE 'NIFTY%' AND "
                "(symbol LIKE '%CE%' OR symbol LIKE '%PE%'))"
            )
        elif sym_type == 'SENSEX_OPTIONS':
            conditions.append(
                "(symbol LIKE 'SENSEX%' AND "
                "(symbol LIKE '%CE%' OR symbol LIKE '%PE%'))"
            )
        elif sym_type == 'BANKNIFTY_OPTIONS':
            conditions.append(
                "(symbol LIKE 'BANKNIFTY%' AND "
                "(symbol LIKE '%CE%' OR symbol LIKE '%PE%'))"
            )
        elif sym_type == 'MIDCPNIFTY_OPTIONS':
            conditions.append(
                "(symbol LIKE 'MIDCPNIFTY%' AND "
                "(symbol LIKE '%CE%' OR symbol LIKE '%PE%'))"
            )
        elif sym_type == 'FINNIFTY_OPTIONS':
            conditions.append(
                "(symbol LIKE 'FINNIFTY%' AND "
                "(symbol LIKE '%CE%' OR symbol LIKE '%PE%'))"
            )
        elif sym_type == 'NIFTY_INDEX':
            conditions.append(
                "(symbol LIKE 'NIFTY%' AND "
                "symbol NOT LIKE '%CE%' AND symbol NOT LIKE '%PE%')"
            )
        elif sym_type == 'SENSEX_INDEX':
            conditions.append(
                "(symbol LIKE 'SENSEX%' AND "
                "symbol NOT LIKE '%CE%' AND symbol NOT LIKE '%PE%')"
            )
        elif sym_type == 'BANKNIFTY_INDEX':
            conditions.append(
                "(symbol LIKE 'BANKNIFTY%' AND "
                "symbol NOT LIKE '%CE%' AND symbol NOT LIKE '%PE%')"
            )
        elif sym_type == 'MIDCPNIFTY_INDEX':
            conditions.append(
                "(symbol LIKE 'MIDCPNIFTY%' AND "
                "symbol NOT LIKE '%CE%' AND symbol NOT LIKE '%PE%')"
            )
        elif sym_type == 'FINNIFTY_INDEX':
            conditions.append(
                "(symbol LIKE 'FINNIFTY%' AND "
                "symbol NOT LIKE '%CE%' AND symbol NOT LIKE '%PE%')"
            )
        elif sym_type == 'DEFAULT':
            conditions.append("(1=1)")
    return conditions


# ── Export one table for one date to CSV.GZ ──────────────────
async def export_table_date(conn, table_name, tick_date, config, force=False):
    """
    Export all rows for tick_date from table_name.
    Returns (csv_gz_bytes, row_count, md5_checksum)
    or (None, 0, None) if nothing to export.
    """
    print(f"  Exporting {table_name} for {tick_date}...")

    if table_name == 'gap_ticks':
        if force:
            where = f"timestamp::date = '{tick_date}'"
        else:
            conditions = _symbol_conditions(config, tick_date)
            if not conditions:
                print(f"    Skipping {tick_date} — within retention period")
                return None, 0, None
            where = (
                f"timestamp::date = '{tick_date}' AND "
                f"({' OR '.join(conditions)})"
            )
        query = f"SELECT * FROM gap_ticks WHERE {where} ORDER BY timestamp ASC"

    elif table_name == 'candles_5s':
        query = f"""
            SELECT * FROM candles_5s
            WHERE bucket >= EXTRACT(EPOCH FROM '{tick_date}'::date)::BIGINT
              AND bucket <  EXTRACT(EPOCH FROM '{tick_date}'::date
                            + INTERVAL '1 day')::BIGINT
            ORDER BY bucket ASC
        """
    elif table_name == 'gap_events':
        query = f"""
            SELECT * FROM gap_events
            WHERE bucket >= EXTRACT(EPOCH FROM '{tick_date}'::date)::BIGINT
              AND bucket <  EXTRACT(EPOCH FROM '{tick_date}'::date
                            + INTERVAL '1 day')::BIGINT
            ORDER BY bucket ASC
        """
    else:
        return None, 0, None

    # Use COPY for streaming — avoids materialising millions of Python objects
    from io import BytesIO
    buf = BytesIO()
    await conn.copy_from_query(query, output=buf, format='csv', header=True)
    csv_bytes = buf.getvalue()
    buf.close()

    # Header-only means no data rows
    first_nl = csv_bytes.find(b'\n')
    if first_nl == -1 or first_nl == len(csv_bytes) - 1:
        print(f"    No rows found for {table_name} {tick_date}")
        return None, 0, None

    # Count rows without loading into memory (count newlines minus header)
    row_count = csv_bytes.count(b'\n') - 1  # subtract header line
    gz_bytes  = gzip.compress(csv_bytes, compresslevel=9)
    md5       = hashlib.md5(gz_bytes).hexdigest()

    print(f"    {row_count:,} rows → {len(gz_bytes)/1024/1024:.1f} MB compressed")
    return gz_bytes, row_count, md5


# ── Upload to Backblaze B2 ────────────────────────────────────
def upload_to_b2(s3_client, bucket, key, data_bytes):
    s3_client.put_object(Bucket=bucket, Key=key, Body=data_bytes)
    print(f"    Uploaded to B2: {key}")


# ── Update manifest.json in B2 ────────────────────────────────
def update_manifest(s3_client, bucket, date_str, table_name,
                    row_count, size_bytes, md5):
    manifest = {}
    try:
        obj = s3_client.get_object(Bucket=bucket, Key='manifest.json')
        manifest = json.loads(obj['Body'].read())
    except Exception:
        pass

    manifest.setdefault(date_str, {})[table_name] = {
        'rows'       : row_count,
        'size_bytes' : size_bytes,
        'md5'        : md5,
        'archived_at': datetime.now().isoformat(),
    }

    s3_client.put_object(
        Bucket=bucket,
        Key   ='manifest.json',
        Body  =json.dumps(manifest, indent=2).encode(),
    )


# ── Delete archived rows from PostgreSQL ──────────────────────
async def delete_archived_rows(conn, table_name, tick_date, config):
    print(f"  Deleting {table_name} rows for {tick_date}...")

    if table_name == 'gap_ticks':
        conditions = _symbol_conditions(config, tick_date)
        if not conditions:
            return 0
        where  = (
            f"timestamp::date = '{tick_date}' AND "
            f"({' OR '.join(conditions)})"
        )
        result = await conn.execute(
            f"DELETE FROM gap_ticks WHERE {where}"
        )

    elif table_name == 'candles_5s':
        result = await conn.execute("""
            DELETE FROM candles_5s
            WHERE bucket >= EXTRACT(EPOCH FROM $1::date)::BIGINT
              AND bucket <  EXTRACT(EPOCH FROM $1::date
                            + INTERVAL '1 day')::BIGINT
        """, tick_date)

    elif table_name == 'gap_events':
        result = await conn.execute("""
            DELETE FROM gap_events
            WHERE bucket >= EXTRACT(EPOCH FROM $1::date)::BIGINT
              AND bucket <  EXTRACT(EPOCH FROM $1::date
                            + INTERVAL '1 day')::BIGINT
        """, tick_date)

    deleted = int(result.split()[-1])
    print(f"    Deleted {deleted:,} rows from {table_name}")
    return deleted


# ── Main archive function ─────────────────────────────────────
async def run_archive(dry_run=False, specific_date=None,
                      specific_dates=None, force=False):
    secrets = read_secrets()
    s3      = get_b2_client(secrets)
    bucket  = secrets['BACKBLAZE_BUCKET']

    conn = await asyncpg.connect(
        host=DB_HOST, port=int(DB_PORT),
        user=DB_USER, password=DB_PASSWORD,
        database=DB_NAME,
    )

    try:
        await conn.execute("SET statement_timeout = 0")
        config = await get_retention_config(conn)

        # specific_dates (list) takes priority over specific_date (single)
        if specific_dates:
            dates = specific_dates
        else:
            dates = await get_dates_to_archive(
                conn, config, specific_date, force=force
            )

        if not dates:
            print("✅ Nothing to archive — all data within retention")
            return

        print(f"Dates to archive: {[str(d) for d in dates]}")
        if dry_run:
            print("DRY RUN — no changes will be made")
            return

        # Load manifest once so we can skip uploads where row count matches
        try:
            obj      = s3.get_object(Bucket=bucket, Key='manifest.json')
            manifest = json.loads(obj['Body'].read())
        except Exception:
            manifest = {}

        tables         = ['gap_ticks', 'candles_5s', 'gap_events']
        total_rows     = 0
        total_size     = 0
        archived_dates = []

        for tick_date in dates:
            print(f"\nArchiving {tick_date}...")
            date_rows = 0
            date_size = 0
            success   = True

            for table in tables:
                try:
                    await conn.execute("""
                        INSERT INTO archive_log
                            (archive_date, table_name, status)
                        VALUES ($1, $2, 'running')
                        ON CONFLICT (archive_date, table_name)
                        DO UPDATE SET status='running', started_at=NOW()
                    """, tick_date, table)

                    gz_bytes, row_count, md5 = await export_table_date(
                        conn, table, tick_date, config, force=force
                    )

                    if gz_bytes is None:
                        await conn.execute("""
                            UPDATE archive_log
                            SET status='skipped', completed_at=NOW()
                            WHERE archive_date=$1 AND table_name=$2
                        """, tick_date, table)
                        continue

                    b2_key = f"{table}/{tick_date}.csv.gz"

                    # Skip upload if already in B2 with same row count
                    existing_rows = (
                        manifest
                        .get(str(tick_date), {})
                        .get(table, {})
                        .get('rows', -1)
                    )

                    if not force and existing_rows == row_count:
                        print(f"    Skipping upload — already in B2 "
                              f"with {existing_rows:,} rows")
                    else:
                        if existing_rows >= 0 and existing_rows != row_count:
                            print(f"    Overwriting B2 — row count changed: "
                                  f"{existing_rows:,} → {row_count:,}")
                        upload_to_b2(s3, bucket, b2_key, gz_bytes)
                    update_manifest(s3, bucket, str(tick_date),
                                    table, row_count, len(gz_bytes), md5)

                    await delete_archived_rows(
                        conn, table, tick_date, config
                    )

                    await conn.execute("""
                        UPDATE archive_log SET
                            status          = 'completed',
                            rows_archived   = $3,
                            file_size_bytes = $4,
                            b2_file_key     = $5,
                            checksum_md5    = $6,
                            completed_at    = NOW()
                        WHERE archive_date=$1 AND table_name=$2
                    """, tick_date, table,
                        row_count, len(gz_bytes), b2_key, md5)

                    date_rows += row_count
                    date_size += len(gz_bytes)

                except Exception as e:
                    success = False
                    print(f"  ❌ {table} failed: {e}")
                    await conn.execute("""
                        UPDATE archive_log SET
                            status='failed', error_message=$3,
                            completed_at=NOW()
                        WHERE archive_date=$1 AND table_name=$2
                    """, tick_date, table, str(e))

            if success:
                archived_dates.append(str(tick_date))
                total_rows += date_rows
                total_size += date_size
                print(f"  ✅ {tick_date}: "
                      f"{date_rows:,} rows, "
                      f"{date_size/1024/1024:.1f} MB")

        if archived_dates:
            send_telegram(
                f"✅ Archive completed\n"
                f"Dates: {', '.join(archived_dates)}\n"
                f"Total rows: {total_rows:,}\n"
                f"Total size: {total_size/1024/1024:.1f} MB\n"
                f"Bucket: {bucket}",
                secrets,
            )
        else:
            send_telegram(
                "⚠️ Archive ran but nothing archived\n"
                "Check logs for details",
                secrets,
            )

        # ── VACUUM ANALYZE after archive ──────────────────
        print("\nRunning VACUUM ANALYZE on archived tables...")
        try:
            vac_conn = await asyncpg.connect(
                host     = DB_HOST,
                port     = int(DB_PORT),
                user     = DB_USER,
                password = DB_PASSWORD,
                database = DB_NAME,
            )
            # Disable timeout — VACUUM can take minutes
            await vac_conn.execute(
                "SET statement_timeout = 0"
            )
            # VACUUM cannot run inside a transaction
            # asyncpg requires isolation='autocommit'
            await vac_conn.execute(
                "VACUUM ANALYZE gap_ticks"
            )
            await vac_conn.execute(
                "VACUUM ANALYZE candles_5s"
            )
            await vac_conn.execute(
                "VACUUM ANALYZE gap_events"
            )
            await vac_conn.close()
            print("✅ VACUUM ANALYZE complete")
        except Exception as e:
            print(f"⚠️ VACUUM ANALYZE failed "
                  f"(non-critical): {e}")

    finally:
        await conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Archive old tick data to Backblaze B2"
    )
    parser.add_argument('--dry-run', action='store_true',
                        help="Show what would be archived without making changes")
    parser.add_argument('--date', type=str, default=None,
                        help="Archive a specific date (YYYY-MM-DD)")
    parser.add_argument('--dates', type=str, nargs='+', default=None,
                        help="Archive multiple dates sequentially (YYYY-MM-DD ...)")
    parser.add_argument('--force', action='store_true',
                        help="Force archive regardless of retention config")
    args = parser.parse_args()

    if args.dates:
        # Multiple dates in one process — sequential, single VACUUM at end.
        # This is the safe path: avoids N parallel VACUUMs on gap_ticks.
        specific_dates = [
            datetime.strptime(d, '%Y-%m-%d').date() for d in args.dates
        ]
        asyncio.run(run_archive(
            dry_run        = args.dry_run,
            specific_dates = specific_dates,
            force          = args.force,
        ))
    else:
        specific_date = None
        if args.date:
            specific_date = datetime.strptime(args.date, '%Y-%m-%d').date()
        asyncio.run(run_archive(
            dry_run       = args.dry_run,
            specific_date = specific_date,
            force         = args.force,
        ))
