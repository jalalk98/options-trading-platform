#!/usr/bin/env python3
"""
Sync parquet files from B2 to local laptop cache.

Usage:
  python scripts/sync_from_b2.py --dates 2026-03-16 2026-03-17
  python scripts/sync_from_b2.py --from 2026-03-16 --to 2026-03-20
  python scripts/sync_from_b2.py --dates 2026-03-16 --tables candles_5s gap_events
  python scripts/sync_from_b2.py --from 2026-03-16 --to 2026-04-17 --dry-run
  python scripts/sync_from_b2.py --list-local
"""
import argparse
import sys
import os
import hashlib
from pathlib import Path
from datetime import datetime, date, timedelta
from collections import defaultdict

# Allow running from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

import boto3
from botocore.client import Config
from config.credentials import (
    BACKBLAZE_KEY_ID, BACKBLAZE_APP_KEY,
    BACKBLAZE_ENDPOINT, BACKBLAZE_BUCKET,
    LOCAL_PARQUET_PATH,
)

# Daily download budget warning threshold (respect B2 1 GB/day limit)
DAILY_WARN_MB = 900


def get_b2_client():
    if not BACKBLAZE_KEY_ID or not BACKBLAZE_APP_KEY:
        sys.exit("BACKBLAZE_KEY_ID / BACKBLAZE_APP_KEY not set in .env")
    return boto3.client(
        's3',
        endpoint_url=f'https://{BACKBLAZE_ENDPOINT}',
        aws_access_key_id=BACKBLAZE_KEY_ID,
        aws_secret_access_key=BACKBLAZE_APP_KEY,
        config=Config(signature_version='s3v4'),
    )


def date_range(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def local_path_for(key: str) -> Path:
    """Convert B2 key to local path."""
    return Path(LOCAL_PARQUET_PATH) / key


def file_md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def list_b2_files_for_date(s3, d: date, tables):
    """List all parquet files in B2 for given date, filtered by tables."""
    year = d.year
    month_str = f'{d.month:02d}'
    day_str = f'{d.day:02d}'

    prefixes = []
    if 'raw_ticks' in tables:
        prefixes.append('raw_ticks/')
    if 'candles_5s' in tables:
        prefixes.append(f'candles_5s/year={year}/month={month_str}/day={day_str}/')
    if 'gap_events' in tables:
        prefixes.append(f'gap_events/year={year}/month={month_str}/day={day_str}/')

    files = []
    paginator = s3.get_paginator('list_objects_v2')
    for prefix in prefixes:
        for page in paginator.paginate(Bucket=BACKBLAZE_BUCKET, Prefix=prefix):
            for obj in page.get('Contents', []):
                k = obj['Key']
                if k.startswith('raw_ticks/'):
                    if f'year={year}/month={month_str}/day={day_str}' not in k:
                        continue
                files.append({
                    'key': k,
                    'size': obj['Size'],
                    'etag': obj.get('ETag', '').strip('"'),
                })
    return files


def download_file(s3, b2_key: str, local_path: Path, expected_md5=None):
    """Download one file, verify MD5, return (success, size_bytes, status)."""
    local_path.parent.mkdir(parents=True, exist_ok=True)

    if local_path.exists():
        return True, 0, 'already-present'

    try:
        s3.download_file(BACKBLAZE_BUCKET, b2_key, str(local_path))
        size = local_path.stat().st_size

        if expected_md5 and len(expected_md5) == 32:
            actual_md5 = file_md5(local_path)
            if actual_md5 != expected_md5:
                local_path.unlink()
                return False, size, f'md5 mismatch: {actual_md5} != {expected_md5}'

        return True, size, 'downloaded'
    except Exception as e:
        if local_path.exists():
            local_path.unlink()
        return False, 0, str(e)


def sync_dates(dates, tables, dry_run=False):
    print(f'Target: {len(dates)} date(s), tables: {tables}')
    print(f'Local path: {LOCAL_PARQUET_PATH}')
    print('-' * 70)

    s3 = get_b2_client()

    plan = []
    for d in dates:
        files = list_b2_files_for_date(s3, d, tables)
        for f in files:
            local = local_path_for(f['key'])
            plan.append({
                'date': d,
                'key': f['key'],
                'size': f['size'],
                'etag': f['etag'],
                'local': local,
                'exists': local.exists(),
            })

    if not plan:
        print('No files found on B2 for given dates.')
        return

    to_download = [p for p in plan if not p['exists']]
    already = [p for p in plan if p['exists']]
    total_dl_mb = sum(p['size'] for p in to_download) / 1024 / 1024
    total_already_mb = sum(p['size'] for p in already) / 1024 / 1024

    print(f'Files already on laptop: {len(already)} ({total_already_mb:.1f} MB)')
    print(f'Files to download:       {len(to_download)} ({total_dl_mb:.1f} MB)')
    print()

    if total_dl_mb > DAILY_WARN_MB:
        print(f'Warning: Download {total_dl_mb:.0f} MB exceeds daily warn '
              f'threshold {DAILY_WARN_MB} MB')
        print(f'   B2 free tier allows ~1 GB/day downloads.')
        if not dry_run:
            resp = input('   Continue anyway? (yes/no): ')
            if resp.lower() != 'yes':
                print('Aborted.')
                return

    if dry_run:
        print('--- DRY RUN -- would download: ---')
        by_date = defaultdict(lambda: {'count': 0, 'size': 0})
        for p in to_download:
            by_date[p['date']]['count'] += 1
            by_date[p['date']]['size'] += p['size']
        for d in sorted(by_date.keys()):
            v = by_date[d]
            print(f'  {d}: {v["count"]} files, {v["size"]/1024/1024:.1f} MB')
        return

    print('--- Downloading ---')
    downloaded = 0
    skipped = 0
    failed = 0
    total_bytes = 0

    for i, p in enumerate(to_download, 1):
        print(f'[{i}/{len(to_download)}] {p["key"]}... ', end='', flush=True)
        ok, size, status = download_file(
            s3, p['key'], p['local'], expected_md5=p['etag']
        )
        if ok:
            if status == 'already-present':
                skipped += 1
                print('skipped (already present)')
            else:
                downloaded += 1
                total_bytes += size
                print(f'OK {size/1024/1024:.2f} MB')
        else:
            failed += 1
            print(f'FAIL {status}')

    print()
    print('--- Summary ---')
    print(f'Downloaded: {downloaded} files, {total_bytes/1024/1024:.1f} MB')
    print(f'Skipped:    {skipped}')
    print(f'Failed:     {failed}')


def list_local():
    """Show what's currently on the laptop."""
    local_root = Path(LOCAL_PARQUET_PATH)
    if not local_root.exists():
        print(f'Local path does not exist: {local_root}')
        return

    total_size = 0
    files_by_date = defaultdict(lambda: {'count': 0, 'size': 0, 'tables': set()})

    for parquet in local_root.rglob('*.parquet'):
        size = parquet.stat().st_size
        total_size += size

        parts = parquet.parts
        year = month = day = None
        table = None
        for p in parts:
            if p.startswith('year='):
                year = p.split('=')[1]
            elif p.startswith('month='):
                month = p.split('=')[1]
            elif p.startswith('day='):
                day = p.split('=')[1]

        if 'raw_ticks' in parts:
            table = 'raw_ticks'
        elif 'candles_5s' in parts:
            table = 'candles_5s'
        elif 'gap_events' in parts:
            table = 'gap_events'

        if year and month and day:
            key = f'{year}-{month}-{day}'
            files_by_date[key]['count'] += 1
            files_by_date[key]['size'] += size
            if table:
                files_by_date[key]['tables'].add(table)

    print(f'Local path: {local_root}')
    print(f'Total: {sum(v["count"] for v in files_by_date.values())} files, '
          f'{total_size/1024/1024:.1f} MB')
    print()
    if not files_by_date:
        print('(no parquet files yet)')
        return

    print('By date:')
    print('-' * 70)
    for d in sorted(files_by_date.keys()):
        v = files_by_date[d]
        tables_str = ', '.join(sorted(v['tables']))
        print(f'  {d}: {v["count"]:3d} files, {v["size"]/1024/1024:7.1f} MB  [{tables_str}]')
    print('-' * 70)


def main():
    p = argparse.ArgumentParser(description='Sync parquet files from B2 to laptop.')
    p.add_argument('--dates', nargs='+', help='Specific dates (YYYY-MM-DD)')
    p.add_argument('--from', dest='from_date', help='Start date (YYYY-MM-DD)')
    p.add_argument('--to', dest='to_date', help='End date (YYYY-MM-DD)')
    p.add_argument('--tables', nargs='+',
                   default=['candles_5s', 'gap_events', 'raw_ticks'],
                   choices=['raw_ticks', 'candles_5s', 'gap_events'],
                   help='Tables to sync (default: all)')
    p.add_argument('--dry-run', action='store_true')
    p.add_argument('--list-local', action='store_true',
                   help='Show what parquet files are already on laptop')
    args = p.parse_args()

    if args.list_local:
        list_local()
        return

    dates = []
    if args.dates:
        dates = [datetime.strptime(d, '%Y-%m-%d').date() for d in args.dates]
    elif args.from_date and args.to_date:
        start = datetime.strptime(args.from_date, '%Y-%m-%d').date()
        end = datetime.strptime(args.to_date, '%Y-%m-%d').date()
        dates = list(date_range(start, end))
    else:
        p.error('Must provide --dates or --from/--to (or --list-local)')

    sync_dates(dates, args.tables, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
