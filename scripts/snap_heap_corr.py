#!/usr/bin/env python3
"""Snapshot candles_5s heap correlation and emit to perf_reports/corr_snapshots.log.
Run three times per trading day: 09:30, 12:00, 15:25 IST.
The daily_perf_report.py reads this log to show intraday correlation decay."""

import sys
import datetime
import subprocess
from pathlib import Path

_IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

def main():
    now_ist = datetime.datetime.now(_IST)
    ts = now_ist.strftime("%Y-%m-%d %H:%M:%S")

    sql = """
        SELECT attname, correlation
        FROM pg_stats
        WHERE tablename = 'candles_5s'
          AND attname IN ('symbol', 'bucket')
        ORDER BY attname;
    """
    result = subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-d", "tickdata", "-t", "-A", "-F", "\t", "-c", sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"snap_heap_corr failed: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    corr = {}
    for line in result.stdout.strip().splitlines():
        parts = line.strip().split("\t")
        if len(parts) == 2:
            corr[parts[0]] = parts[1]

    log_path = Path.home() / "perf_reports" / "corr_snapshots.log"
    log_path.parent.mkdir(exist_ok=True)
    with open(log_path, "a") as f:
        f.write(
            f"{ts}\tsymbol={corr.get('symbol', 'N/A')}\tbucket={corr.get('bucket', 'N/A')}\n"
        )
    print(f"[corr_snap] {ts} symbol={corr.get('symbol')} bucket={corr.get('bucket')}")

if __name__ == "__main__":
    main()
