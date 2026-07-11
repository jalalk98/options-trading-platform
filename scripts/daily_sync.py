"""
Daily B2 -> laptop sync wrapper.

Designed to run via Windows Task Scheduler at 3:20 PM.
- Detects what dates are missing locally vs what exists in B2
- Limits to 3 days per run (respects 1 GB/day B2 download budget)
- Sends Telegram notification on success/failure
- Logs to D:/tickdata/sync.log

Usage:
  python scripts/daily_sync.py           # run normally
  python scripts/daily_sync.py --force   # ignore last-run timestamp
  python scripts/daily_sync.py --dry-run # show what would sync
"""
import argparse
import sys
import os
import json
import logging
import re
from pathlib import Path
from datetime import datetime, date, timedelta
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
from botocore.client import Config
from config.credentials import (
    BACKBLAZE_KEY_ID, BACKBLAZE_APP_KEY,
    BACKBLAZE_ENDPOINT, BACKBLAZE_BUCKET,
    LOCAL_PARQUET_PATH,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
)

# ----- Config -----
MAX_DATES_PER_RUN = 2       # Stay under 1 GB daily budget (~400 MB/date)
DAILY_BUDGET_MB = 950       # Hard ceiling per run

# ----- Setup logging -----
LOG_DIR = Path('D:/tickdata')
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / 'sync.log'

logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
)
log = logging.getLogger('daily_sync')


def send_telegram(text: str):
    """Send Telegram notification, silent failure if not configured."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.info("Telegram not configured, skipping notification")
        return
    try:
        import requests
        resp = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=10,
        )
        if resp.ok:
            log.info("Telegram notification sent")
        else:
            log.warning(f"Telegram failed: {resp.status_code}")
    except Exception as e:
        log.warning(f"Telegram error: {e}")


def get_b2_client():
    return boto3.client(
        's3',
        endpoint_url=f'https://{BACKBLAZE_ENDPOINT}',
        aws_access_key_id=BACKBLAZE_KEY_ID,
        aws_secret_access_key=BACKBLAZE_APP_KEY,
        config=Config(signature_version='s3v4', connect_timeout=15, read_timeout=120),
    )


def list_b2_dates(s3) -> set:
    """Return set of YYYY-MM-DD dates that exist in B2 (any of the 3 tables)."""
    dates = set()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BACKBLAZE_BUCKET):
        for obj in page.get('Contents', []):
            m = re.search(r'year=(\d{4})/month=(\d{2})/day=(\d{2})', obj['Key'])
            if m:
                dates.add(f"{m.group(1)}-{m.group(2)}-{m.group(3)}")
    return dates


def list_local_dates() -> set:
    """
    Return set of YYYY-MM-DD dates that are fully synced to laptop.

    A date counts only if candles_5s AND at least one raw_ticks symbol_group
    both have a non-empty parquet file. This prevents a partially-failed
    download (which leaves behind a handful of small raw_ticks files) from
    being counted as complete and silently skipped on the next run.
    """
    root = Path(LOCAL_PARQUET_PATH)
    if not root.exists():
        return set()

    candles_root = root / 'candles_5s'
    raw_ticks_root = root / 'raw_ticks'

    # Gather dates that have a non-empty candles_5s file
    candles_dates = set()
    if candles_root.exists():
        for f in candles_root.rglob('*.parquet'):
            if f.is_file() and f.stat().st_size > 0:
                m = re.search(r'year=(\d{4})/month=(\d{2})/day=(\d{2})', f.as_posix())
                if m:
                    candles_dates.add(f"{m.group(1)}-{m.group(2)}-{m.group(3)}")

    # Gather dates that have at least one non-empty raw_ticks file
    raw_dates = set()
    if raw_ticks_root.exists():
        for f in raw_ticks_root.rglob('*.parquet'):
            if f.is_file() and f.stat().st_size > 0:
                m = re.search(r'year=(\d{4})/month=(\d{2})/day=(\d{2})', f.as_posix())
                if m:
                    raw_dates.add(f"{m.group(1)}-{m.group(2)}-{m.group(3)}")

    # Only dates present in both are considered fully synced
    return candles_dates & raw_dates


def get_table_stats(dates: list) -> dict:
    """Read parquet metadata for synced dates.

    Returns {date_str: {table: {rows, bytes, groups?}}} where
    raw_ticks also includes a 'groups' dict keyed by symbol_group name.
    Uses pyarrow metadata-only reads (fast — no data loaded).
    """
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return {}

    local_root = Path(LOCAL_PARQUET_PATH)
    result: dict = {}

    for d_str in dates:
        d = datetime.strptime(d_str, '%Y-%m-%d').date()
        year, month, day = d.year, f'{d.month:02d}', f'{d.day:02d}'
        date_stats: dict = {}

        # raw_ticks — partitioned by symbol_group
        ticks_day_root = local_root / 'raw_ticks'
        if ticks_day_root.exists():
            ticks_entry: dict = {'rows': 0, 'bytes': 0, 'groups': {}}
            for sg_dir in sorted(ticks_day_root.iterdir()):
                if not sg_dir.name.startswith('symbol_group='):
                    continue
                group_name = sg_dir.name.split('=', 1)[1]
                pf = sg_dir / f'year={year}' / f'month={month}' / f'day={day}' / 'data.parquet'
                if not pf.is_file() or pf.stat().st_size == 0:
                    continue
                try:
                    rows = pq.read_metadata(str(pf)).num_rows
                except Exception:
                    rows = 0
                ticks_entry['rows'] += rows
                ticks_entry['bytes'] += pf.stat().st_size
                ticks_entry['groups'][group_name] = rows
            if ticks_entry['rows'] > 0:
                date_stats['raw_ticks'] = ticks_entry

        # candles_5s and gap_events
        for table in ('candles_5s', 'gap_events'):
            pf = local_root / table / f'year={year}' / f'month={month}' / f'day={day}' / 'data.parquet'
            if not pf.is_file() or pf.stat().st_size == 0:
                continue
            try:
                rows = pq.read_metadata(str(pf)).num_rows
            except Exception:
                rows = 0
            if rows > 0:
                date_stats[table] = {'rows': rows, 'bytes': pf.stat().st_size}

        if date_stats:
            result[d_str] = date_stats

    return result


def format_date_stats(d_str: str, date_stats: dict) -> str:
    """Format per-date stats as the Telegram message block."""
    lines = [f"Syncing: {d_str}"]
    total_rows = 0
    all_tables = {'raw_ticks', 'candles_5s', 'gap_events'}
    found_tables = set(date_stats.keys())

    # gap_ticks (raw_ticks)
    if 'raw_ticks' in date_stats:
        s = date_stats['raw_ticks']
        lines.append(f"  gap_ticks:  {s['rows']:>12,} rows")
        for group, rows in sorted(date_stats['raw_ticks']['groups'].items(),
                                  key=lambda x: -x[1]):
            lines.append(f"    {group:<16} {rows:>10,}")
        total_rows += s['rows']

    for table in ('candles_5s', 'gap_events'):
        if table in date_stats:
            rows = date_stats[table]['rows']
            lines.append(f"  {table}:  {rows:>12,} rows")
            total_rows += rows

    lines.append("  " + "-" * 26)
    lines.append(f"  total:      {total_rows:>12,} rows")

    missing = all_tables - found_tables
    if missing:
        lines.append(f"  WARNING: missing tables: {', '.join(sorted(missing))}")
    else:
        lines.append("  All table_kinds verified in B2.")

    return "\n".join(lines)


def estimate_size_mb(s3, dates: list) -> float:
    """Estimate total MB needed for given dates."""
    total = 0
    dates_set = set(dates)
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BACKBLAZE_BUCKET):
        for obj in page.get('Contents', []):
            m = re.search(r'year=(\d{4})/month=(\d{2})/day=(\d{2})', obj['Key'])
            if m:
                obj_date = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
                if obj_date in dates_set:
                    total += obj['Size']
    return total / 1024 / 1024


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--force', action='store_true')
    args = parser.parse_args()

    log.info("=" * 60)
    log.info(f"Daily sync started (dry_run={args.dry_run}, force={args.force})")

    try:
        s3 = get_b2_client()

        # Discover what's missing
        log.info("Scanning B2 for available dates...")
        b2_dates = list_b2_dates(s3)
        log.info(f"B2 has {len(b2_dates)} dates")

        local_dates = list_local_dates()
        log.info(f"Laptop has {len(local_dates)} dates")

        missing = sorted(b2_dates - local_dates)
        log.info(f"Missing locally: {len(missing)} dates")

        if not missing:
            msg = "Daily sync: nothing to download (laptop is up to date)"
            log.info(msg)
            print(msg)
            send_telegram(msg)
            return 0

        # Limit to MAX_DATES_PER_RUN, prioritizing oldest first
        # so backlog catches up over multiple runs
        to_sync = missing[:MAX_DATES_PER_RUN]
        log.info(f"This run will sync: {to_sync}")

        if args.dry_run:
            est_mb = estimate_size_mb(s3, to_sync)
            log.info(f"DRY RUN: would download {est_mb:.1f} MB")
            print(f"DRY RUN: would download {est_mb:.1f} MB across {len(to_sync)} dates")
            for d in to_sync:
                print(f"  {d}")
            remaining = len(missing) - len(to_sync)
            if remaining > 0:
                print(f"  ({remaining} more dates pending after this batch)")
            return 0

        # Delegate to sync_from_b2.py
        import subprocess
        cmd = [
            sys.executable,
            str(Path(__file__).parent / 'sync_from_b2.py'),
            '--dates', *to_sync,
        ]
        log.info(f"Running: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            cwd=str(Path(__file__).parent.parent),
            capture_output=True,
            text=True,
            timeout=7200,  # 2 hours max
            stdin=subprocess.DEVNULL,
        )

        if result.returncode == 0:
            log.info("sync_from_b2.py completed successfully")
            log.info(f"stdout (last 500 chars):\n{result.stdout[-500:]}")

            # Verify the dates actually landed locally — sync_from_b2.py exits 0
            # even when it finds no files (e.g. wrong B2 prefix), so a clean exit
            # code alone is not sufficient proof that anything was downloaded.
            local_after = list_local_dates()
            actually_synced = [d for d in to_sync if d in local_after]
            still_missing = [d for d in to_sync if d not in local_after]

            if still_missing:
                log.error(
                    f"sync_from_b2.py exited 0 but these dates are still missing "
                    f"locally: {still_missing}"
                )
                log.error(f"stdout:\n{result.stdout[-500:]}")
                send_telegram(
                    f"Daily sync FAILED — no files downloaded\n"
                    f"Dates attempted: {', '.join(to_sync)}\n"
                    f"Still missing: {', '.join(still_missing)}\n"
                    f"B2 files may be under an unexpected prefix. Check sync.log."
                )
                return 1

            remaining = len(missing) - len(to_sync)
            all_stats = get_table_stats(actually_synced)
            msg_parts = []
            for d_str in actually_synced:
                date_stats = all_stats.get(d_str, {})
                msg_parts.append(format_date_stats(d_str, date_stats))
            if remaining > 0:
                msg_parts.append(f"{remaining} more date(s) pending (will sync next runs)")
            send_telegram("\n\n".join(msg_parts))
            return 0
        else:
            log.error(f"sync_from_b2.py failed with code {result.returncode}")
            log.error(f"stderr:\n{result.stderr[-500:]}")
            send_telegram(
                f"Daily sync FAILED (code {result.returncode})\n"
                f"Check sync.log for details"
            )
            return 1

    except Exception as e:
        log.exception("Daily sync crashed")
        send_telegram(f"Daily sync CRASHED: {e}")
        return 2


if __name__ == '__main__':
    sys.exit(main())
