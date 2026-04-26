#!/usr/bin/env python3
"""
Re-archive old CSV.GZ files in B2 to new Parquet format.

Flow per date:
  1. Download gap_ticks/{date}.csv.gz from B2
  2. Stream-decompress and parse CSV in chunks
  3. Drop 13 calculated columns, keep only 21 raw columns
  4. Split by symbol_group, write per-group Parquet files
  5. Upload new Parquet files to B2
  6. Verify row counts match (old CSV -> new Parquet)
  7. Delete old CSV.GZ from B2
  8. Same for candles_5s and gap_events (no column drop, no split)

Usage:
  python3 scripts/rearchive_b2_to_parquet.py --date 2026-03-16
  python3 scripts/rearchive_b2_to_parquet.py --dates 2026-03-16 2026-03-17
  python3 scripts/rearchive_b2_to_parquet.py --date 2026-03-16 --dry-run
  python3 scripts/rearchive_b2_to_parquet.py --date 2026-03-16 --no-delete
"""

import sys
import os
import gzip
import csv
import json
import argparse
import tempfile
import time
import hashlib
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config
from pathlib import Path
from datetime import datetime, date
from io import TextIOWrapper

sys.path.insert(0, str(Path(__file__).parent.parent))

# Raw columns to keep (same as archive_to_b2_v2.py)
GAP_TICKS_RAW_COLS = [
    'instrument_token', 'symbol', 'expiry_date', 'strike',
    'option_type', 'timestamp', 'last_trade_time',
    'curr_price', 'last_quantity', 'average_price',
    'curr_volume', 'oi', 'oi_day_high', 'oi_day_low',
    'buy_quantity', 'sell_quantity', 'depth',
    'bid_depth_qty', 'ask_depth_qty',
    'best_bid', 'best_ask',
]

# Columns to drop (calculated -- can re-derive on restore)
GAP_TICKS_DROP_COLS = {
    'id', 'depth_imbalance', 'prev_price', 'prev_volume',
    'price_jump', 'direction', 'vol_change', 'time_diff',
    'spread', 'spread_pct', 'only_gap', 'gap_with_spread',
    'is_gap',
}


# ── Read Backblaze credentials ───────────────────────────────
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


# ── Symbol grouping (mirrors archive_to_b2_v2.py) ────────────
def get_symbol_group(symbol):
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


# ── Parse types for gap_ticks columns ─────────────────────────
def parse_gap_ticks_row(row):
    """Convert CSV strings to proper Python types for parquet."""
    def to_int(v):
        return int(v) if v not in ('', None, '\\N', 'NULL') else None

    def to_float(v):
        return float(v) if v not in ('', None, '\\N', 'NULL') else None

    def to_date(v):
        if v in ('', None, '\\N', 'NULL'):
            return None
        return datetime.strptime(v, '%Y-%m-%d').date()

    def to_ts(v):
        if v in ('', None, '\\N', 'NULL'):
            return None
        for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S'):
            try:
                return datetime.strptime(v, fmt)
            except ValueError:
                continue
        return None

    def to_str_or_none(v):
        return v if v not in ('', None, '\\N', 'NULL') else None

    return {
        'instrument_token': to_int(row.get('instrument_token')),
        'symbol': row.get('symbol'),
        'expiry_date': to_date(row.get('expiry_date')),
        'strike': to_float(row.get('strike')),
        'option_type': row.get('option_type'),
        'timestamp': to_ts(row.get('timestamp')),
        'last_trade_time': to_ts(row.get('last_trade_time')),
        'curr_price': to_float(row.get('curr_price')),
        'last_quantity': to_float(row.get('last_quantity')),
        'average_price': to_float(row.get('average_price')),
        'curr_volume': to_int(row.get('curr_volume')),
        'oi': to_int(row.get('oi')),
        'oi_day_high': to_int(row.get('oi_day_high')),
        'oi_day_low': to_int(row.get('oi_day_low')),
        'buy_quantity': to_int(row.get('buy_quantity')),
        'sell_quantity': to_int(row.get('sell_quantity')),
        'depth': to_str_or_none(row.get('depth')),
        'bid_depth_qty': to_int(row.get('bid_depth_qty')),
        'ask_depth_qty': to_int(row.get('ask_depth_qty')),
        'best_bid': to_float(row.get('best_bid')),
        'best_ask': to_float(row.get('best_ask')),
    }


# ── B2 key builders ───────────────────────────────────────────
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


def md5_of_file(path):
    h = hashlib.md5()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


# ── Stream-process gap_ticks CSV.GZ ───────────────────────────
def process_gap_ticks(s3, bucket, tick_date, tmp_dir):
    """
    Download gap_ticks CSV.GZ, stream-parse in chunks,
    split by symbol_group, write per-group parquet files.
    """
    old_key = f"gap_ticks/{tick_date}.csv.gz"
    download_path = Path(tmp_dir) / f"gap_ticks_{tick_date}.csv.gz"

    print(f"  Downloading {old_key}...")
    t_start = time.time()
    s3.download_file(bucket, old_key, str(download_path))
    dl_size = download_path.stat().st_size / 1024 / 1024
    print(f"    Downloaded {dl_size:.1f} MB in {time.time()-t_start:.1f}s")

    CHUNK_ROWS = 100_000
    writers = {}
    counts = {}
    buffers = {}
    total_rows = 0
    start = time.time()

    try:
        with gzip.open(download_path, 'rt', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)

            for row in reader:
                parsed = parse_gap_ticks_row(row)
                grp = get_symbol_group(parsed['symbol'] or 'OTHER')

                if grp not in buffers:
                    buffers[grp] = []
                    counts[grp] = 0

                buffers[grp].append(parsed)
                counts[grp] += 1
                total_rows += 1

                if len(buffers[grp]) >= CHUNK_ROWS:
                    write_chunk_to_parquet(buffers[grp], grp, tmp_dir, writers)
                    buffers[grp] = []

                if total_rows % 500_000 == 0:
                    elapsed = time.time() - start
                    rate = total_rows / elapsed if elapsed > 0 else 0
                    print(
                        f"    Processed {total_rows:,} rows | {rate:,.0f} rows/s    ",
                        end='\r', flush=True
                    )

            for grp, buf in buffers.items():
                if buf:
                    write_chunk_to_parquet(buf, grp, tmp_dir, writers)

    finally:
        for w in writers.values():
            w.close()
        download_path.unlink(missing_ok=True)

    print(f"\n    {total_rows:,} rows processed in {(time.time()-start)/60:.1f} min")

    result = {}
    for grp, cnt in counts.items():
        fpath = Path(tmp_dir) / f"gap_ticks_{grp}.parquet"
        if fpath.exists() and cnt > 0:
            result[grp] = (str(fpath), cnt, fpath.stat().st_size)

    return result, total_rows


def write_chunk_to_parquet(buffer, grp, tmp_dir, writers):
    if not buffer:
        return
    data = {col: [] for col in GAP_TICKS_RAW_COLS}
    for row in buffer:
        for col in GAP_TICKS_RAW_COLS:
            data[col].append(row.get(col))

    table = pa.table(data)

    if grp not in writers:
        file_path = Path(tmp_dir) / f"gap_ticks_{grp}.parquet"
        writers[grp] = pq.ParquetWriter(
            file_path, table.schema, compression='snappy'
        )
    writers[grp].write_table(table)


# ── Stream-process candles_5s / gap_events CSV.GZ ─────────────
def process_simple_table(s3, bucket, table, tick_date, tmp_dir):
    old_key = f"{table}/{tick_date}.csv.gz"
    download_path = Path(tmp_dir) / f"{table}_{tick_date}.csv.gz"

    print(f"  Downloading {old_key}...")
    t_start = time.time()
    try:
        s3.download_file(bucket, old_key, str(download_path))
    except Exception as e:
        print(f"    No {old_key} in B2 ({e})")
        return None, 0
    dl_size = download_path.stat().st_size / 1024 / 1024
    print(f"    Downloaded {dl_size:.1f} MB in {time.time()-t_start:.1f}s")

    CHUNK_ROWS = 100_000
    total_rows = 0
    start = time.time()
    out_path = Path(tmp_dir) / f"{table}.parquet"

    try:
        with gzip.open(download_path, 'rt', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            if not fieldnames:
                return None, 0

            buffer = []
            for row in reader:
                clean = {k: (v if v not in ('', '\\N', 'NULL') else None)
                         for k, v in row.items()}
                buffer.append(clean)
                total_rows += 1

                if len(buffer) >= CHUNK_ROWS:
                    write_simple_chunk(buffer, fieldnames, out_path, table)
                    buffer = []

            if buffer:
                write_simple_chunk(buffer, fieldnames, out_path, table)

    finally:
        download_path.unlink(missing_ok=True)

    print(f"    {total_rows:,} {table} rows processed in {(time.time()-start)/60:.1f} min")

    if total_rows == 0 or not out_path.exists():
        return None, 0
    return str(out_path), total_rows


def write_simple_chunk(buffer, fieldnames, out_path, table_name):
    if not buffer:
        return
    data = {col: [] for col in fieldnames}
    for row in buffer:
        for col in fieldnames:
            data[col].append(row.get(col))

    pa_cols = {}
    for col in fieldnames:
        pa_cols[col] = infer_and_convert(data[col], col, table_name)

    t = pa.table(pa_cols)

    if out_path.exists():
        existing = pq.read_table(str(out_path))
        combined = pa.concat_tables([existing, t])
        pq.write_table(combined, str(out_path), compression='snappy')
    else:
        pq.write_table(t, str(out_path), compression='snappy')


def infer_and_convert(vals, col, table_name):
    candles_int_cols = {'bucket', 'volume', 'oi_close', 'oi_change',
                        'gap_ref_bucket', 'candles_above_gap', 'seconds_above_gap'}
    candles_float_cols = {'open', 'high', 'low', 'close', 'prev_close',
                          'candle_gap', 'gap_ref_price', 'dist_from_gap',
                          'dist_from_gap_pct', 'buy_vol', 'sell_vol',
                          'delta', 'cum_delta', 'depth_imbalance'}
    candles_bool_cols = {'is_candle_gap', 'closed_above_gap'}
    ge_int_cols = {'bucket', 'vol_change'}
    ge_float_cols = {'prev_price', 'curr_price'}

    if table_name == 'candles_5s':
        if col in candles_int_cols:
            return pa.array([int(v) if v else None for v in vals], type=pa.int64())
        if col in candles_float_cols:
            return pa.array([float(v) if v else None for v in vals], type=pa.float64())
        if col in candles_bool_cols:
            return pa.array([v.lower() == 't' if v else None for v in vals], type=pa.bool_())
    elif table_name == 'gap_events':
        if col in ge_int_cols:
            return pa.array([int(v) if v else None for v in vals], type=pa.int64())
        if col in ge_float_cols:
            return pa.array([float(v) if v else None for v in vals], type=pa.float64())
    return pa.array(vals, type=pa.string())


# ── Verify row count ──────────────────────────────────────────
def verify_rows(old_total, new_files):
    new_total = sum(c for _, c, _ in new_files)
    if old_total == new_total:
        print(f"    ✅ Row count match: {old_total:,} = {new_total:,}")
        return True
    print(f"    ❌ MISMATCH: old={old_total:,}, new={new_total:,}")
    return False


# ── Upload / delete helpers ──────────────────────────────────
def upload_file(s3, bucket, local, key):
    with open(local, 'rb') as f:
        s3.put_object(Bucket=bucket, Key=key, Body=f.read())
    print(f"    ↑ {key}")


def delete_key(s3, bucket, key):
    s3.delete_object(Bucket=bucket, Key=key)
    print(f"    🗑  {key}")


# ── Update manifest_v2.json ──────────────────────────────────
def update_manifest(s3, bucket, date_str, table_name, entries):
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
        'source': 'rearchive_from_csvgz',
    }

    s3.put_object(
        Bucket=bucket, Key='manifest_v2.json',
        Body=json.dumps(manifest, indent=2).encode(),
    )


# ── Main re-archive function ─────────────────────────────────
def rearchive_date(s3, bucket, tick_date, dry_run=False, no_delete=False):
    print(f"\n━━━ Re-archiving {tick_date} ━━━")

    if dry_run:
        print(f"  [DRY RUN] Would re-archive {tick_date}")
        return

    with tempfile.TemporaryDirectory(prefix='rearchive_') as tmp_dir:
        # ── gap_ticks ─────────────────────────────────────────
        try:
            print(f"  Processing gap_ticks...")
            groups, total_gt = process_gap_ticks(s3, bucket, tick_date, tmp_dir)

            if not groups:
                print(f"    No gap_ticks rows found in old archive")
            else:
                entries = []
                for grp, (fpath, rows, size) in groups.items():
                    new_key = b2_key_gap_ticks(grp, tick_date)
                    md5 = md5_of_file(fpath)
                    upload_file(s3, bucket, fpath, new_key)
                    entries.append({
                        'b2_key': new_key,
                        'symbol_group': grp,
                        'rows': rows,
                        'size_bytes': size,
                        'md5': md5,
                    })

                new_files = [(e['b2_key'], e['rows'], e['size_bytes']) for e in entries]
                verified = verify_rows(total_gt, new_files)

                if verified:
                    update_manifest(s3, bucket, str(tick_date), 'gap_ticks', entries)
                    if not no_delete:
                        delete_key(s3, bucket, f"gap_ticks/{tick_date}.csv.gz")
                    total_mb = sum(e['size_bytes'] for e in entries) / 1024 / 1024
                    print(f"    ✅ gap_ticks: {total_gt:,} rows, "
                          f"{total_mb:.1f} MB across {len(entries)} groups")
                else:
                    print(f"    ⚠️ Skipping deletion of old CSV.GZ due to row mismatch")

        except Exception as e:
            print(f"  ❌ gap_ticks failed: {e}")
            import traceback
            traceback.print_exc()

        # ── candles_5s and gap_events ─────────────────────────
        for table in ('candles_5s', 'gap_events'):
            try:
                print(f"  Processing {table}...")
                fpath, total = process_simple_table(s3, bucket, table, tick_date, tmp_dir)
                if fpath is None:
                    print(f"    No {table} data for {tick_date}")
                    continue

                new_key = b2_key_simple(table, tick_date)
                md5 = md5_of_file(fpath)
                size = Path(fpath).stat().st_size
                upload_file(s3, bucket, fpath, new_key)

                entries = [{'b2_key': new_key, 'rows': total, 'size_bytes': size, 'md5': md5}]
                update_manifest(s3, bucket, str(tick_date), table, entries)

                if not no_delete:
                    delete_key(s3, bucket, f"{table}/{tick_date}.csv.gz")
                print(f"    ✅ {table}: {total:,} rows, {size/1024/1024:.1f} MB")

            except Exception as e:
                print(f"  ❌ {table} failed: {e}")
                import traceback
                traceback.print_exc()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--date', type=str)
    parser.add_argument('--dates', type=str, nargs='+')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--no-delete', action='store_true',
                        help="Don't delete old CSV.GZ after new parquet uploaded")
    args = parser.parse_args()

    if args.dates:
        dates = [datetime.strptime(d, '%Y-%m-%d').date() for d in args.dates]
    elif args.date:
        dates = [datetime.strptime(args.date, '%Y-%m-%d').date()]
    else:
        print("Must provide --date or --dates")
        sys.exit(1)

    secrets = read_secrets()
    bucket = secrets['BACKBLAZE_BUCKET']

    for d in dates:
        try:
            s3 = get_b2_client(secrets)  # fresh client per date avoids auth issues after long ops
            rearchive_date(s3, bucket, d, dry_run=args.dry_run, no_delete=args.no_delete)
        except Exception as e:
            print(f"❌ {d} failed at top level: {e}")

    print("\n✅ Re-archive complete")


if __name__ == '__main__':
    main()
