#!/usr/bin/env python3
"""Snapshot candles_5s statistics freshness and emit to perf_reports/stats_freshness.log.
Run three times per trading day: 09:30, 12:00, 15:25 IST.
The daily_perf_report.py reads this log to show intraday stats staleness."""

import sys
import datetime
import subprocess
from pathlib import Path

_IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def main():
    now_ist = datetime.datetime.now(_IST)
    ts = now_ist.strftime("%Y-%m-%d %H:%M:%S")

    sql = """
        SELECT
            n_mod_since_analyze,
            n_live_tup,
            n_dead_tup,
            GREATEST(last_autovacuum, last_vacuum)   AS last_any_vacuum,
            GREATEST(last_autoanalyze, last_analyze) AS last_any_analyze,
            EXTRACT(EPOCH FROM (NOW() - GREATEST(last_autovacuum, last_vacuum)))   / 3600.0 AS hrs_since_autovac,
            EXTRACT(EPOCH FROM (NOW() - GREATEST(last_autoanalyze, last_analyze))) / 3600.0 AS hrs_since_autoanalyze
        FROM pg_stat_user_tables
        WHERE relname = 'candles_5s';
    """
    result = subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-d", "tickdata", "-t", "-A", "-F", "\t", "-c", sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"snap_stats_freshness failed: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    parts = result.stdout.strip().split("\t")
    if len(parts) < 7:
        print(f"snap_stats_freshness: unexpected output: {result.stdout!r}", file=sys.stderr)
        sys.exit(1)

    n_mod        = parts[0]
    n_live       = parts[1]
    n_dead       = parts[2]
    hrs_autovac  = parts[5]
    hrs_analyze  = parts[6]

    try:
        n_mod_pct = f"{int(n_mod) / max(int(n_live), 1) * 100:.2f}"
    except (ValueError, ZeroDivisionError):
        n_mod_pct = "N/A"

    log_path = Path.home() / "perf_reports" / "stats_freshness.log"
    log_path.parent.mkdir(exist_ok=True)
    with open(log_path, "a") as f:
        f.write(
            f"{ts}\tn_mod={n_mod}\tn_mod_pct={n_mod_pct}\tn_live={n_live}\tn_dead={n_dead}"
            f"\thrs_autovac={hrs_autovac}\thrs_analyze={hrs_analyze}\n"
        )
    print(
        f"[stats_fresh] {ts} n_mod={n_mod} n_mod_pct={n_mod_pct}% n_live={n_live} n_dead={n_dead}"
        f" hrs_autovac={hrs_autovac} hrs_analyze={hrs_analyze}"
    )


if __name__ == "__main__":
    main()
