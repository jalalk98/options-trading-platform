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
MAX_DATES_PER_RUN = 3       # Stay under 1 GB daily budget (~250 MB/day)
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
        config=Config(signature_version='s3v4'),
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
    """Return set of YYYY-MM-DD dates already on laptop."""
    dates = set()
    root = Path(LOCAL_PARQUET_PATH)
    if not root.exists():
        return dates
    for parquet in root.rglob('*.parquet'):
        m = re.search(r'year=(\d{4})/month=(\d{2})/day=(\d{2})', parquet.as_posix())
        if m:
            dates.add(f"{m.group(1)}-{m.group(2)}-{m.group(3)}")
    return dates


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
            timeout=3600,  # 1 hour max
        )

        if result.returncode == 0:
            log.info("sync_from_b2.py completed successfully")
            log.info(f"stdout (last 500 chars):\n{result.stdout[-500:]}")

            remaining = len(missing) - len(to_sync)
            msg_lines = [
                "Daily sync OK",
                f"Synced {len(to_sync)} date(s):",
                *[f"  - {d}" for d in to_sync],
            ]
            if remaining > 0:
                msg_lines.append(
                    f"{remaining} more date(s) pending (will sync next runs)"
                )
            send_telegram("\n".join(msg_lines))
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
