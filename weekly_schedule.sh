#!/bin/bash
# weekly_schedule.sh — Runs every Sunday at 10:30 PM.
# Dynamically generates the schedule from the live crontab, then sends
# a second message with inline buttons to run any job on demand.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEXT_MONDAY=$(date -d "next monday" '+%d %b %Y')

python3 - "$NEXT_MONDAY" <<'PYEOF'
import subprocess, re, sys, json, os, requests

next_monday = sys.argv[1]

# ── Credentials ───────────────────────────────────────────────────────────────
secrets = {}
with open(os.path.expanduser("~/.kite_secrets")) as f:
    for line in f:
        line = line.strip()
        if "=" in line and not line.startswith("#"):
            k, _, v = line.partition("=")
            secrets[k.strip()] = v.strip()

BOT_TOKEN = secrets["TELEGRAM_BOT_TOKEN"]
CHAT_ID   = secrets["TELEGRAM_CHAT_ID"]


def tg_send(text, reply_markup=None):
    data = {"chat_id": CHAT_ID, "text": text}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        data=data, timeout=10,
    )


# ── Schedule parser ───────────────────────────────────────────────────────────

def to_12h(h, m):
    period = "AM" if h < 12 else "PM"
    return f"{h % 12 or 12}:{m:02d} {period}"

def parse_time(min_f, hour_f):
    if min_f.isdigit() and hour_f.isdigit():
        h, m = int(hour_f), int(min_f)
        return h * 60 + m, to_12h(h, m)
    return None, None

def get_group(dow_f):
    if dow_f == "1-5": return "weekday"
    if dow_f == "0":   return "sunday"
    return "daily"

def extract_label(comment, cmd):
    if comment:
        part = re.split(r"\s+[—–]\s+", comment)[0].strip()
        if part and not re.match(r"^(ENABLE|DISABLE|TODO|NOTE|SEE|CHECK|FIRST|──)", part, re.I):
            return part
    if "snap_stats_freshness"      in cmd: return "Stats freshness snapshot"
    if "vacuum_today_partition"    in cmd: return "Partition VACUUM (post-market)"
    if "daily_perf_report"         in cmd: return "Daily performance report"
    if "chart_investigation_alert" in cmd: return "Chart investigation alert"
    if "archive_to_b2" in cmd and "--phase export" in cmd: return "Archive — export to B2"
    if "archive_to_b2" in cmd and "--phase drop"   in cmd: return "Archive — drop old partitions"
    if "create_daily_partition"    in cmd: return "Create daily partitions"
    if "remind_resume"             in cmd: return "Resume reminder"
    if "VACUUM ANALYZE" in cmd and "gap_ticks" in cmd: return "VACUUM ANALYZE gap_ticks"
    if "VACUUM ANALYZE"            in cmd: return "VACUUM ANALYZE (all tables)"
    if "ANALYZE candles_5s"        in cmd: return "ANALYZE candles_5s"
    if "pg_stat_statements_reset"  in cmd: return "pg_stat_statements reset"
    m = re.search(r"([\w_-]+)\.(sh|py)\b", cmd)
    if m: return m.group(1).replace("_", " ").title()
    return cmd[:45]

raw  = subprocess.run(["crontab", "-l"], capture_output=True, text=True).stdout.splitlines()
entries      = []
prev_comment = None
seen         = {"tick_rate": False, "tick_collector": False}

for line in raw:
    s = line.strip()
    if not s:
        prev_comment = None
        continue
    if s.startswith("#DISABLED") or s.startswith("#ENABLE"):
        prev_comment = None
        continue
    if s.startswith("#"):
        prev_comment = s.lstrip("#").strip()
        continue
    parts = s.split(None, 5)
    if len(parts) < 6:
        prev_comment = None
        continue
    min_f, hour_f, dom_f, month_f, dow_f, cmd = parts

    if "check_tick_rate" in cmd:
        if not seen["tick_rate"]:
            seen["tick_rate"] = True
            entries.append({"sort": 9*60+17, "time": "9:17 AM – 3:29 PM",
                            "label": "Tick rate watchdog (every 5 min)", "group": "weekday"})
        prev_comment = None
        continue

    if "watchdog_tick_collector" in cmd:
        if not seen["tick_collector"]:
            seen["tick_collector"] = True
            entries.append({"sort": 9*60+15, "time": "9:15 AM – 3:29 PM",
                            "label": "Tick collector watchdog (every 5 min)", "group": "weekday"})
        prev_comment = None
        continue

    sort_key, time_str = parse_time(min_f, hour_f)
    if sort_key is None:
        prev_comment = None
        continue

    entries.append({"sort": sort_key, "time": time_str,
                    "label": extract_label(prev_comment, cmd),
                    "group": get_group(dow_f)})
    prev_comment = None

weekday = sorted([e for e in entries if e["group"] == "weekday"], key=lambda x: x["sort"])
daily   = sorted([e for e in entries if e["group"] == "daily"],   key=lambda x: x["sort"])
sunday  = sorted([e for e in entries if e["group"] == "sunday"],  key=lambda x: x["sort"])

# ── Message 1: schedule text ──────────────────────────────────────────────────
out = [f"📅 Weekly Cron Schedule — Week of {next_monday}", ""]
if weekday:
    out.append("Every weekday (Mon–Fri):")
    for e in weekday:
        out.append(f"  {e['time']}  —  {e['label']}")
if daily:
    out.append("")
    out.append("Every day (incl. weekends):")
    for e in daily:
        out.append(f"  {e['time']}  —  {e['label']}")
if sunday:
    out.append("")
    out.append("Every Sunday:")
    for e in sunday:
        out.append(f"  {e['time']}  —  {e['label']}")

total = len(weekday) + len(daily) + len(sunday)
out.append(f"\n{total} jobs total — generated live from crontab")
tg_send("\n".join(out))

# ── Message 2: inline buttons (one per row, with timing, matching schedule order) ──
#
# Map from the exact label the parser produces → (job_key, display_label).
# Labels not in this map get no button (monitoring-only or text-command jobs).
LABEL_TO_JOB = {
    "Token refresh":                  ("token_refresh",       "🔑 Token Refresh"),
    "Instrument update":              ("instrument_update",   "📋 Instrument Update"),
    "Daily VACUUM ANALYZE":           ("vacuum_all",          "🧹 VACUUM ANALYZE (all tables)"),
    "pg_stat_statements reset":       ("pg_stat_reset",       "🔄 pg_stat Reset"),
    "ANALYZE candles_5s mid-session": ("analyze_candles",     "📈 ANALYZE candles_5s"),
    "Start trading session":          ("start_trading",       "▶️ Start Trading ⚠️"),
    "VACUUM ANALYZE gap_ticks":       ("vacuum_gap_ticks",    "🧹 VACUUM gap_ticks"),
    "Stats freshness snapshot":       ("stats_snapshot",      "📸 Stats Snapshot"),
    "Expiry alert":                   ("expiry_alert",        "🗓 Expiry Alert"),
    "ANALYZE candles_5s":             ("analyze_candles",     "📈 ANALYZE candles_5s"),
    "Stop trading session":           ("stop_trading",        "⏹ Stop Trading ⚠️"),
    "Check Tomorrow Holiday":         ("check_holiday",       "🗓 Check Tomorrow Holiday"),
    "Daily performance report":       ("perf_report",         "📈 Performance Report"),
    "Partition vacuum":               ("partition_vacuum",    "🧹 Partition VACUUM"),
    "Chart investigation":            ("chart_investigation", "🔍 Chart Investigation"),
    "Archive — export to B2":         ("archive_export",      "📦 Archive Export ⚠️"),
    "Archive — drop old partitions":  ("archive_drop",        "🗑 Archive Drop ⚠️"),
    "Daily summary":                  ("daily_summary",       "📊 Daily Summary"),
    "Create daily partitions":        ("create_partitions",   "💾 Create Partitions"),
    "Weekly disk cleanup":            ("disk_cleanup",        "🗑 Disk Cleanup ⚠️"),
    "Weekly VACUUM ANALYZE":          ("vacuum_weekly",       "🧹 Weekly VACUUM ANALYZE ⚠️"),
}

def section_header(title):
    return [{"text": title, "callback_data": "noop"}]

def build_group_rows(group_entries, seen):
    rows = []
    for e in group_entries:
        mapping = LABEL_TO_JOB.get(e["label"])
        if not mapping:
            continue
        job_key, display = mapping
        if job_key in seen:
            continue
        seen.add(job_key)
        rows.append([{"text": f"{e['time']}  —  {display}", "callback_data": f"run:{job_key}"}])
    return rows

seen_jobs = set()
keyboard_rows = []

wd_rows = build_group_rows(weekday, seen_jobs)
if wd_rows:
    keyboard_rows.append(section_header("── Every weekday (Mon–Fri) ──"))
    keyboard_rows.extend(wd_rows)

dl_rows = build_group_rows(daily, seen_jobs)
if dl_rows:
    keyboard_rows.append(section_header("── Every day ──"))
    keyboard_rows.extend(dl_rows)

su_rows = build_group_rows(sunday, seen_jobs)
if su_rows:
    keyboard_rows.append(section_header("── Every Sunday ──"))
    keyboard_rows.extend(su_rows)

keyboard_rows.append(section_header("── On demand ──"))
keyboard_rows.extend([
    [{"text": "🔍 Trading Status",      "callback_data": "cmd:status"}],
    [{"text": "⏸ Holiday Mode ON ⚠️",  "callback_data": "cmd:holiday"}],
    [{"text": "▶️ Resume Trading ⚠️",   "callback_data": "cmd:resume"}],
])

tg_send(
    "⚡ Run any job on demand — tap a button below\n"
    "(⚠️ = requires confirmation before running)",
    reply_markup={"inline_keyboard": keyboard_rows},
)
PYEOF
