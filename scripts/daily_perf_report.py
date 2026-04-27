#!/usr/bin/env python3
"""Daily performance report — runs at 15:35 IST via cron.

Sections:
  1. Chart load latency (p50/p95/p99) bucketed by IST time
  2. hist-symbols latency across the day
  3. _pending_fills size curve and max
  4. [FILL_AGE] distribution — determines TTL for Phase 2
  5. pg_stat_statements top 10
  6. pg_stat_user_tables snapshot for candles_5s
  7. Heap correlation intraday curve

Output: ~/perf_reports/YYYY-MM-DD.md
"""

import sys
import re
import statistics
import subprocess
import datetime
from pathlib import Path
from collections import defaultdict

# ── Config ────────────────────────────────────────────────────────────────────
LOG_FILE     = Path("/var/log/trading-api.log")
CORR_LOG     = Path.home() / "perf_reports" / "corr_snapshots.log"
REPORT_DIR   = Path.home() / "perf_reports"
DB_NAME      = "tickdata"
_IST         = datetime.timezone(datetime.timedelta(hours=5, minutes=30))

# IST time buckets for latency percentile grouping
TIME_BUCKETS = ["09:30", "11:00", "13:00", "14:00", "14:30", "15:00", "15:25"]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pct(data, p):
    if not data:
        return None
    s = sorted(data)
    idx = max(0, int(len(s) * p / 100) - 1)
    return s[idx]

def _fmt(v, unit="ms"):
    return f"{v:.1f}{unit}" if v is not None else "—"

def _bucket_label(hms: str) -> str:
    """Return the TIME_BUCKETS label for a HH:MM:SS string (assign to nearest preceding bucket)."""
    h, m, _ = (int(x) for x in hms.split(":"))
    t = h * 60 + m
    best = None
    for b in TIME_BUCKETS:
        bh, bm = (int(x) for x in b.split(":"))
        bt = bh * 60 + bm
        if bt <= t:
            best = b
    return best  # None = before 09:30

def _run_sql(sql: str) -> str:
    result = subprocess.run(
        ["sudo", "-u", "postgres", "psql", "-d", DB_NAME,
         "-t", "-A", "-F", "\t", "-c", sql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        return f"ERROR: {result.stderr.strip()}"
    return result.stdout.strip()

def _today_str() -> str:
    return datetime.datetime.now(_IST).strftime("%Y-%m-%d")

def _read_log_lines_today() -> list[str]:
    """Read /var/log/trading-api.log; return lines from today's IST date.
    The log has NO timestamps on most lines so we return ALL lines and rely
    on embedded ts= fields in CHART_TIMING / HIST_TIMING / FILL_AGE lines.
    We read the full file because the log is rotated daily by the service.
    """
    try:
        return LOG_FILE.read_text(errors="replace").splitlines()
    except Exception as e:
        return [f"# could not read {LOG_FILE}: {e}"]


# ── Section 1: Chart load latency ─────────────────────────────────────────────

def section_chart_latency(lines: list[str]) -> str:
    """Parse [CHART_TIMING] lines and bucket by IST time."""
    pat = re.compile(
        r"\[CHART_TIMING\] ts=(\S+) sym=(\S+) rows=(\d+) ms=([\d.]+) path=(\S+)"
    )
    by_bucket  = defaultdict(list)   # label → [ms, ...]
    by_path    = defaultdict(list)   # path  → [ms, ...]
    all_full   = []

    for line in lines:
        m = pat.search(line)
        if not m:
            continue
        ts, sym, rows, ms, path = m.group(1), m.group(2), int(m.group(3)), float(m.group(4)), m.group(5)
        label = _bucket_label(ts)
        if label:
            by_bucket[label].append(ms)
        by_path[path].append(ms)
        if path == "full":
            all_full.append(ms)

    lines_out = ["## 1. Chart Load Latency (`/api/chart/{symbol}`)\n"]

    # Totals by path
    for path in ("full", "incr", "cache"):
        d = by_path[path]
        if d:
            lines_out.append(
                f"**{path}** — calls={len(d)} p50={_fmt(_pct(d,50))} "
                f"p95={_fmt(_pct(d,95))} p99={_fmt(_pct(d,99))} max={_fmt(max(d))}"
            )
    lines_out.append("")

    if not by_bucket:
        lines_out.append("_No chart timing data found — check that [CHART_TIMING] lines are present in the log._")
        return "\n".join(lines_out)

    # Time-of-day table (full-query path only — cache/incr don't reflect DB load)
    lines_out.append("**Full-query path — p50 / p95 / p99 by IST time window**\n")
    lines_out.append("| IST window | calls | p50 | p95 | p99 | max |")
    lines_out.append("|-----------|-------|-----|-----|-----|-----|")
    for label in TIME_BUCKETS:
        # Filter full-path calls in this bucket
        # Re-parse to get per-bucket full-path data
        pass

    # Re-bucket full-path only
    full_by_bucket = defaultdict(list)
    for line in lines:
        m = pat.search(line)
        if not m:
            continue
        ts, ms, path = m.group(1), float(m.group(4)), m.group(5)
        if path != "full":
            continue
        label = _bucket_label(ts)
        if label:
            full_by_bucket[label].append(ms)

    for label in TIME_BUCKETS:
        d = full_by_bucket.get(label, [])
        if d:
            lines_out.append(
                f"| ≥{label} | {len(d)} | {_fmt(_pct(d,50))} | {_fmt(_pct(d,95))} "
                f"| {_fmt(_pct(d,99))} | {_fmt(max(d))} |"
            )
        else:
            lines_out.append(f"| ≥{label} | 0 | — | — | — | — |")

    return "\n".join(lines_out)


# ── Section 2: hist-symbols latency ──────────────────────────────────────────

def section_hist_latency(lines: list[str]) -> str:
    pat = re.compile(r"\[HIST_TIMING\] ts=(\S+) date=(\S+) rows=(\d+) ms=([\d.]+) path=(\S+)")
    full_ms  = []
    cache_ms = []
    for line in lines:
        m = pat.search(line)
        if not m:
            continue
        ms, path = float(m.group(4)), m.group(5)
        if path == "full":
            full_ms.append(ms)
        else:
            cache_ms.append(ms)

    out = ["## 2. hist-symbols Latency (`/api/hist-symbols`)\n"]
    if full_ms:
        out.append(
            f"**DB queries** — calls={len(full_ms)} p50={_fmt(_pct(full_ms,50))} "
            f"p95={_fmt(_pct(full_ms,95))} max={_fmt(max(full_ms))}"
        )
    else:
        out.append("_No full-query hits (all served from cache, or no calls today)_")
    if cache_ms:
        out.append(f"**Cache hits** — calls={len(cache_ms)} p50={_fmt(_pct(cache_ms,50))} max={_fmt(max(cache_ms))}")
    return "\n".join(out)


# ── Section 3: _pending_fills size ───────────────────────────────────────────

def section_pending_fills(lines: list[str]) -> str:
    pat = re.compile(r"\[PENDING_FILLS\] total=(\d+) symbols=(\d+)")
    # Match timestamp from uvicorn log format or embedded ts= (not present here)
    # PENDING_FILLS uses logger.info which may not have timestamps.
    # We log them from chart_server; they appear in the log without wall-clock ts.
    # Approximation: track sequence order and note max/last values.
    totals = []
    for line in lines:
        m = pat.search(line)
        if m:
            totals.append(int(m.group(1)))

    out = ["## 3. `_pending_fills` Size\n"]
    if not totals:
        out.append("_No [PENDING_FILLS] lines found — monitor task may not have fired yet._")
        return "\n".join(out)

    out.append(f"- **Samples collected**: {len(totals)}")
    out.append(f"- **Max across day**: {max(totals)}")
    out.append(f"- **EOD value (last sample)**: {totals[-1]}")
    out.append(f"- **Min**: {min(totals)}")

    # Time-of-day curve: report every ~10th sample as a rough progression
    step = max(1, len(totals) // 10)
    curve = [str(totals[i]) for i in range(0, len(totals), step)]
    out.append(f"- **Curve (sampled)**: {' → '.join(curve)}")
    return "\n".join(out)


# ── Section 4: FILL_AGE distribution ─────────────────────────────────────────

def section_fill_age(lines: list[str]) -> str:
    pat = re.compile(r"\[FILL_AGE\] symbol=(\S+) dir=(\S+) age_secs=([\d.]+) bucket_diff=(\d+)")
    ages = []
    by_dir = defaultdict(list)
    for line in lines:
        m = pat.search(line)
        if m:
            age = float(m.group(3))
            ages.append(age)
            by_dir[m.group(2)].append(age)

    out = ["## 4. Fill-Age Distribution (TTL calibration)\n"]
    if not ages:
        out.append("_No [FILL_AGE] lines yet — fills not observed today, or no jumps occurred._")
        out.append("_(This is expected on a weekend dry-run; will populate on a trading day.)_")
        return "\n".join(out)

    out.append(f"- **Total fills observed**: {len(ages)}")
    out.append(f"- **p50**: {_fmt(_pct(ages, 50), 's')}")
    out.append(f"- **p95**: {_fmt(_pct(ages, 95), 's')}")
    out.append(f"- **p99**: {_fmt(_pct(ages, 99), 's')}")
    out.append(f"- **max**: {_fmt(max(ages), 's')}")
    out.append(f"- **mean**: {_fmt(statistics.mean(ages), 's')}")
    for d, d_ages in sorted(by_dir.items()):
        out.append(
            f"- **{d}** fills={len(d_ages)} p95={_fmt(_pct(d_ages, 95), 's')} max={_fmt(max(d_ages), 's')}"
        )
    out.append("")
    out.append(
        "> **TTL recommendation**: set `TTL_SECONDS` to p99 + 20% safety margin. "
        "If p99 ≤ 1800s (30min), use 2700s (45min). If p99 > 3600s, investigate before evicting."
    )
    return "\n".join(out)


# ── Section 5a: Conviction query stats (from pg_stat_statements) ─────────────

def section_conviction_stats() -> str:
    sql = """
        SELECT
            calls,
            round(mean_exec_time::numeric, 1)           AS mean_ms,
            round(total_exec_time::numeric, 0)          AS total_ms,
            round((total_exec_time / NULLIF(
                (SELECT SUM(total_exec_time) FROM pg_stat_statements), 0) * 100)::numeric, 1
            )                                           AS pct_total
        FROM pg_stat_statements
        WHERE query ILIKE '%candles_above_gap >= 3%'
          AND query ILIKE '%candles_5s%'
        ORDER BY total_exec_time DESC
        LIMIT 1;
    """
    raw = _run_sql(sql)
    out = ["## 5a. Conviction Query Performance\n"]
    out.append("_Target post-Fix-1: mean_ms < 2ms, pct_total < 2%_\n")
    if raw.startswith("ERROR") or not raw.strip():
        out.append("_No data — pg_stat_statements may not have captured this query yet._")
        return "\n".join(out)
    parts = raw.strip().split("\t")
    if len(parts) < 4:
        out.append(f"_Unexpected result: {raw}_")
        return "\n".join(out)
    out.append(f"- **calls**: {parts[0]}")
    out.append(f"- **mean_ms**: {parts[1]} ms")
    out.append(f"- **total_ms**: {parts[2]} ms")
    out.append(f"- **% of total DB time**: {parts[3]}%")
    explain_before = Path.home() / "perf_reports" / "conviction_explain_before.txt"
    explain_after  = Path.home() / "perf_reports" / "conviction_explain_after.txt"
    out.append("")
    out.append(f"_EXPLAIN baseline (before index): `{explain_before}`_")
    if explain_after.exists():
        out.append(f"_EXPLAIN after index: `{explain_after}`_")
    return "\n".join(out)


# ── Section 5b: Pool acquire-wait distribution ────────────────────────────────

def section_pool_wait(lines: list[str]) -> str:
    pat = re.compile(r"\[POOL_WAIT\] ts=(\S+) sym=\S+ wait_ms=([\d.]+) query_ms=([\d.]+)")
    all_wait  = []
    all_query = []
    wait_1500  = []
    wait_1525  = []

    for line in lines:
        m = pat.search(line)
        if not m:
            continue
        ts, wait_ms, query_ms = m.group(1), float(m.group(2)), float(m.group(3))
        all_wait.append(wait_ms)
        all_query.append(query_ms)
        label = _bucket_label(ts)
        if label == "15:25":
            wait_1525.append(wait_ms)
        elif label == "15:00":
            wait_1500.append(wait_ms)

    out = ["## 5b. Pool Acquire-Wait (`/api/conviction/{symbol}`)\n"]
    out.append("_Pool contention target post-Fix-1: p99 wait_ms < 5ms at all windows_\n")
    if not all_wait:
        out.append("_No [POOL_WAIT] lines found — instrumentation active from next restart._")
        return "\n".join(out)

    out.append(f"**Full day** — samples={len(all_wait)}")
    out.append(
        f"  wait: p50={_fmt(_pct(all_wait,50))} p95={_fmt(_pct(all_wait,95))} "
        f"p99={_fmt(_pct(all_wait,99))} max={_fmt(max(all_wait))}"
    )
    out.append(
        f"  query: p50={_fmt(_pct(all_query,50))} p95={_fmt(_pct(all_query,95))} "
        f"p99={_fmt(_pct(all_query,99))} max={_fmt(max(all_query))}"
    )
    if wait_1500:
        out.append(f"\n**≥15:00 window** — samples={len(wait_1500)}")
        out.append(
            f"  wait: p50={_fmt(_pct(wait_1500,50))} p95={_fmt(_pct(wait_1500,95))} "
            f"p99={_fmt(_pct(wait_1500,99))} max={_fmt(max(wait_1500))}"
        )
    if wait_1525:
        out.append(f"\n**≥15:25 window** — samples={len(wait_1525)}")
        out.append(
            f"  wait: p50={_fmt(_pct(wait_1525,50))} p95={_fmt(_pct(wait_1525,95))} "
            f"p99={_fmt(_pct(wait_1525,99))} max={_fmt(max(wait_1525))}"
        )
    return "\n".join(out)


# ── Section 5: pg_stat_statements top 10 ────────────────────────────────────

def section_pg_stat_statements() -> str:
    sql = """
        SELECT
            LEFT(query, 120)                            AS query_snippet,
            calls,
            round(total_exec_time::numeric, 0)          AS total_ms,
            round(mean_exec_time::numeric, 1)           AS mean_ms,
            round(stddev_exec_time::numeric, 1)         AS stddev_ms,
            round((total_exec_time / NULLIF(
                (SELECT SUM(total_exec_time) FROM pg_stat_statements), 0) * 100)::numeric, 1
            )                                           AS pct_total
        FROM pg_stat_statements
        ORDER BY total_exec_time DESC
        LIMIT 10;
    """
    raw = _run_sql(sql)
    out = ["## 5. Top 10 Queries — `pg_stat_statements`\n"]
    if raw.startswith("ERROR"):
        out.append(f"_{raw}_")
        return "\n".join(out)

    out.append("| # | Query (120 chars) | calls | total_ms | mean_ms | stddev_ms | % total |")
    out.append("|---|-------------------|-------|----------|---------|-----------|---------|")
    for i, line in enumerate(raw.splitlines(), 1):
        parts = line.split("\t")
        if len(parts) < 6:
            continue
        q = parts[0].replace("|", "∣").strip()
        out.append(f"| {i} | `{q}` | {parts[1]} | {parts[2]} | {parts[3]} | {parts[4]} | {parts[5]}% |")
    return "\n".join(out)


# ── Section 6: pg_stat_user_tables for candles_5s ────────────────────────────

def section_table_stats() -> str:
    sql = """
        SELECT
            n_live_tup,
            n_dead_tup,
            round(n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0) * 100, 2) AS dead_pct,
            n_tup_upd,
            n_tup_hot_upd,
            round(n_tup_hot_upd::numeric / NULLIF(n_tup_upd, 0) * 100, 2) AS hot_pct,
            last_autovacuum,
            last_autoanalyze,
            autovacuum_count,
            autoanalyze_count
        FROM pg_stat_user_tables
        WHERE relname = 'candles_5s';
    """
    raw = _run_sql(sql)
    out = ["## 6. `candles_5s` Table Stats (EOD snapshot)\n"]
    if raw.startswith("ERROR") or not raw.strip():
        out.append("_No data._")
        return "\n".join(out)

    parts = raw.strip().split("\t")
    labels = [
        "live_tuples", "dead_tuples", "dead_pct%",
        "n_upd", "n_hot_upd", "hot_upd_pct%",
        "last_autovacuum", "last_autoanalyze",
        "autovacuum_count", "autoanalyze_count"
    ]
    for label, val in zip(labels, parts):
        out.append(f"- **{label}**: {val.strip()}")
    return "\n".join(out)


# ── Section 7: Heap correlation intraday ────────────────────────────────────

def section_heap_correlation() -> str:
    out = ["## 7. `candles_5s` Heap Correlation — Intraday Decay\n"]

    # Read today's snapshots from corr_snapshots.log
    today = _today_str()
    snapshots = []
    if CORR_LOG.exists():
        for line in CORR_LOG.read_text().splitlines():
            if line.startswith(today):
                parts = line.split("\t")
                if len(parts) == 3:
                    ts_str = parts[0].split(" ")[1]   # HH:MM:SS
                    sym_c  = parts[1].replace("symbol=", "")
                    bkt_c  = parts[2].replace("bucket=", "").strip()
                    snapshots.append((ts_str, sym_c, bkt_c))

    if not snapshots:
        out.append("_No correlation snapshots for today — snap_heap_corr.py cron has not run yet._")
        out.append("_(Expected on a weekend dry-run; will populate on a trading day.)_")
    else:
        out.append("| Time (IST) | symbol corr | bucket corr |")
        out.append("|------------|-------------|-------------|")
        for ts_str, sym_c, bkt_c in snapshots:
            out.append(f"| {ts_str} | {sym_c} | {bkt_c} |")
        out.append("")
        out.append(
            "> **Interpretation**: correlation near ±1 = rows physically ordered by this column "
            "(fast index scans). Near 0 = scattered (slow). CLUSTER after last trading day "
            "restores correlation to ~0.95 at open; watch how fast it decays."
        )

    # Also show current EOD correlation
    sql = """
        SELECT attname, round(correlation::numeric, 4)
        FROM pg_stats
        WHERE tablename = 'candles_5s' AND attname IN ('symbol','bucket')
        ORDER BY attname;
    """
    raw = _run_sql(sql)
    out.append("\n**Current (EOD) correlation from pg_stats:**")
    if not raw.startswith("ERROR"):
        for line in raw.splitlines():
            parts = line.split("\t")
            if len(parts) == 2:
                out.append(f"- {parts[0]}: {parts[1]}")
    else:
        out.append(f"_{raw}_")

    return "\n".join(out)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    today    = _today_str()
    now_ist  = datetime.datetime.now(_IST).strftime("%Y-%m-%d %H:%M IST")
    lines    = _read_log_lines_today()

    sections = [
        f"# Daily Performance Report — {today}\n",
        f"Generated: {now_ist}  |  Log lines scanned: {len(lines)}\n",
        "---\n",
        section_conviction_stats(),
        "",
        section_pool_wait(lines),
        "",
        section_chart_latency(lines),
        "",
        section_hist_latency(lines),
        "",
        section_pending_fills(lines),
        "",
        section_fill_age(lines),
        "",
        section_pg_stat_statements(),
        "",
        section_table_stats(),
        "",
        section_heap_correlation(),
        "",
        "---",
        f"_Report complete. Fix-1 measurement gate:_",
        f"- [ ] Conviction mean_ms < 2ms (was 11.3ms)?",
        f"- [ ] Conviction pct_total < 2% (was 41.3%)?",
        f"- [ ] Chart p99 at ≥15:25 < 500ms (was 5,792ms)?",
        f"- [ ] Pool wait p99 at ≥15:25 < 5ms?",
        f"",
        f"_Phase 2 authorization checklist:_",
        f"- [ ] Chart p99 at 14:30+ acceptable (target <400ms)?",
        f"- [ ] FILL_AGE p99 known → TTL_SECONDS can be set?",
        f"- [ ] Dead tuple ratio stable (autovacuum keeping up)?",
        f"- [ ] Correlation still above 0.5 at 15:00 (CLUSTER helping)?",
    ]

    report = "\n".join(sections)
    REPORT_DIR.mkdir(exist_ok=True)
    out_path = REPORT_DIR / f"{today}.md"
    out_path.write_text(report)
    print(f"Report saved: {out_path}")
    print()
    print(report)


if __name__ == "__main__":
    main()
