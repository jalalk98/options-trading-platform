#!/bin/bash
# daily_summary.sh — Send end-of-day health + data summary to Telegram.
# Runs at 3:40 PM Mon–Fri after trading session stops.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECRETS_FILE="$HOME/.kite_secrets"
TODAY=$(date +%Y-%m-%d)
DATE_LABEL=$(date '+%d %b %Y')

# ── Telegram helper ───────────────────────────────────────────────────────────
_tg() {
    local BOT_TOKEN CHAT_ID
    BOT_TOKEN=$(grep '^TELEGRAM_BOT_TOKEN=' "$SECRETS_FILE" 2>/dev/null | cut -d'=' -f2-)
    CHAT_ID=$(grep '^TELEGRAM_CHAT_ID='    "$SECRETS_FILE" 2>/dev/null | cut -d'=' -f2-)
    [ -n "$BOT_TOKEN" ] && [ -n "$CHAT_ID" ] && \
        curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
            -d chat_id="$CHAT_ID" \
            --data-urlencode "text=$1" > /dev/null
}

trap '_tg "❌ daily_summary.sh crashed on '"$TODAY"'. Check the server."' ERR

# ── Early disk warning (sent separately before full summary) ─────────────────
_check_disk_warning() {
    local pct
    pct=$(df / | awk 'NR==2 {print $5}' | tr -d '%')
    if [ "${pct:-0}" -ge 70 ]; then
        _tg "⚠️ DISK WARNING — ${pct}% used on ${TODAY}. Free up space before it hits 100%."
    fi
}
_check_disk_warning

# ── DB helpers ────────────────────────────────────────────────────────────────
_pg()  { PGPASSWORD='MustafaHasnain@123' psql -h localhost -U postgres -d tickdata -tAq -c "$1" 2>/dev/null | tr -d ' \n'; }
_pgs() { PGPASSWORD='MustafaHasnain@123' psql -h localhost -U postgres -d tickdata -tAq -c "$1" 2>/dev/null | tr -d '\n' | sed 's/^ //;s/ $//'; }

# ── DB health — only count partitions that actually have data ─────────────────
# Empty future pre-created partitions (n_live_tup = 0) are excluded.
TABLE_SIZE=$(_pgs "SELECT pg_size_pretty(COALESCE(SUM(pg_total_relation_size(relid)), 0)) FROM pg_stat_user_tables WHERE relname LIKE 'gap_ticks_%' AND n_live_tup > 0;")
TABLE_SIZE_BYTES=$(_pg  "SELECT COALESCE(SUM(pg_total_relation_size(relid)), 0) FROM pg_stat_user_tables WHERE relname LIKE 'gap_ticks_%' AND n_live_tup > 0;")
TOTAL_ROWS=$(_pg  "SELECT COALESCE(SUM(n_live_tup), 0)::bigint FROM pg_stat_user_tables WHERE relname LIKE 'gap_ticks_%' AND n_live_tup > 0;")
DEAD_TUPLES=$(_pg  "SELECT COALESCE(SUM(n_dead_tup), 0) FROM pg_stat_user_tables WHERE relname LIKE 'gap_ticks_%';")
LAST_ANALYZE_DATE=$(_pg  "SELECT COALESCE(DATE(MAX(GREATEST(last_analyze, last_autoanalyze)))::text, '1970-01-01') FROM pg_stat_user_tables WHERE relname LIKE 'gap_ticks_%' AND n_live_tup > 0;")

# ── Today's tick data (computed early — used in growth comparison below) ──────
SYMBOLS_TODAY=$(_pg "SELECT COUNT(DISTINCT symbol) FROM gap_ticks WHERE timestamp >= '${TODAY} 03:30:00'::timestamp;")
TICKS_TODAY=$(_pg   "SELECT COUNT(*) FROM gap_ticks WHERE timestamp >= '${TODAY} 03:30:00'::timestamp;")
CANDLES_TODAY=$(_pg "SELECT COUNT(*) FROM candles_5s WHERE bucket >= EXTRACT(EPOCH FROM '${TODAY} 03:30:00'::timestamp)::bigint;")
EVENTS_TODAY=$(_pg  "SELECT COUNT(*) FROM gap_events WHERE bucket >= EXTRACT(EPOCH FROM '${TODAY} 03:30:00'::timestamp)::bigint;")

TOTAL_TICKS=$(( ${TICKS_TODAY:-0} + ${CANDLES_TODAY:-0} + ${EVENTS_TODAY:-0} ))
TOTAL_TICKS_FMT=$(printf "%'d" "${TOTAL_TICKS:-0}"   2>/dev/null || echo "${TOTAL_TICKS:-0}")
TICKS_FMT=$(printf   "%'d" "${TICKS_TODAY:-0}"        2>/dev/null || echo "${TICKS_TODAY:-0}")
CANDLES_FMT=$(printf "%'d" "${CANDLES_TODAY:-0}"      2>/dev/null || echo "${CANDLES_TODAY:-0}")
EVENTS_FMT=$(printf  "%'d" "${EVENTS_TODAY:-0}"       2>/dev/null || echo "${EVENTS_TODAY:-0}")

# ── Growth trend: today's ticks vs last active trading day ────────────────────
# Stats file format: DATE,TICKS_TODAY,TABLE_SIZE_BYTES
#   TICKS_TODAY  = exact gap_ticks count for that day — unaffected by archiving
#   TABLE_SIZE_BYTES = total bytes of non-empty partitions — disk capacity trend
STATS_FILE="/var/log/trading-gap-ticks-stats.csv"
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)

TREND_LINE=""
if [ "${TICKS_TODAY:-0}" -eq 0 ]; then
    : # No trading today — skip growth comparison (would just show 0 vs prev day's count)
elif [ -f "$STATS_FILE" ]; then
    # Find last entry with ticks > 0 that isn't today (skips weekends/holidays automatically)
    PREV=$(grep -v "^${TODAY}," "$STATS_FILE" | awk -F',' '$2 > 0' | tail -1)
    if [ -n "$PREV" ]; then
        PREV_DATE=$(echo "$PREV" | cut -d',' -f1)
        PREV_TICKS=$(echo "$PREV" | cut -d',' -f2)
        PREV_BYTES=$(echo "$PREV" | cut -d',' -f3)
        DELTA_TICKS=$(( ${TICKS_TODAY:-0} - ${PREV_TICKS:-0} ))
        DELTA_BYTES=$(( ${TABLE_SIZE_BYTES:-0} - ${PREV_BYTES:-0} ))
        DELTA_TICKS_FMT=$(printf "%'+d" "$DELTA_TICKS" 2>/dev/null || echo "$DELTA_TICKS")
        DELTA_MB=$(awk "BEGIN {printf \"%.0f\", $DELTA_BYTES/1048576}")
        if [ "$DELTA_MB" -ge 0 ] 2>/dev/null; then
            DELTA_SIZE_FMT="+${DELTA_MB} MB"
        else
            DELTA_SIZE_FMT="${DELTA_MB} MB"
        fi
        if [ "$PREV_DATE" = "$YESTERDAY" ]; then
            CMP_LABEL="yesterday"
        else
            CMP_LABEL="$PREV_DATE"
        fi
        TREND_LINE="  Growth vs ${CMP_LABEL}: ${DELTA_TICKS_FMT} ticks | ${DELTA_SIZE_FMT}"$'\n'
    fi
fi

# ── Last analyzed status ──────────────────────────────────────────────────────
# On non-trading days autoanalyze doesn't run (nothing changed) — suppress the
# warning unless the stats are genuinely stale (>3 days).
DAYS_AGO=0
if [ "$LAST_ANALYZE_DATE" != "$TODAY" ]; then
    DAYS_AGO=$(( ( $(date +%s) - $(date -d "${LAST_ANALYZE_DATE:-1970-01-01}" +%s 2>/dev/null || echo $(date +%s)) ) / 86400 ))
fi

if [ "$LAST_ANALYZE_DATE" = "$TODAY" ]; then
    VACUUM_STATUS="today ✅"
elif [ "${TICKS_TODAY:-0}" -eq 0 ] && [ "$DAYS_AGO" -le 3 ]; then
    VACUUM_STATUS="${DAYS_AGO}d ago ✅"
else
    VACUUM_STATUS="${DAYS_AGO}d ago ⚠️"
fi

# ── Disk & memory ─────────────────────────────────────────────────────────────
DISK_USED=$(df -h / | awk 'NR==2 {print $3}')
DISK_TOTAL=$(df -h / | awk 'NR==2 {print $2}')
DISK_PCT=$(df -h / | awk 'NR==2 {print $5}')

DISK_PCT_NUM=${DISK_PCT//%/}
if [ "${DISK_PCT_NUM:-0}" -ge 70 ]; then
    DISK_ICON="⚠️"
else
    DISK_ICON="✅"
fi

# Top-3 PostgreSQL tables by total size — direct psql to preserve newlines
TOP_TABLES=$(PGPASSWORD='MustafaHasnain@123' psql -h localhost -U postgres -d tickdata -tAq \
    -c "SELECT string_agg(relname || ':  ' || pg_size_pretty(pg_total_relation_size(relid)), E'\n' ORDER BY pg_total_relation_size(relid) DESC) FROM (SELECT relid, relname FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 3) t;" 2>/dev/null)

TRADING_LOG_SIZE=$(du -h /var/log/trading-api.log 2>/dev/null | cut -f1 || echo "N/A")
SYSLOG_SIZE=$(du -h /var/log/syslog 2>/dev/null | cut -f1 || echo "N/A")

MEM_TOTAL=$(free -h | awk '/^Mem/ {print $2}')
MEM_USED=$(free -h  | awk '/^Mem/ {print $3}')
MEM_FREE=$(free -h  | awk '/^Mem/ {print $7}')

# ── Service health ────────────────────────────────────────────────────────────
if systemctl is-active --quiet trading-bot.service; then
    BOT_STATUS="✅ running"
else
    BOT_STATUS="❌ stopped"
fi

if systemctl is-active --quiet trading-api && curl -s --max-time 3 http://localhost:8000/api/strikes > /dev/null 2>&1; then
    API_STATUS="✅ running (systemd)"
else
    API_STATUS="❌ not responding"
fi

# Reboot-required check
if [ -f /var/run/reboot-required ]; then
    REBOOT_LINE="
⚠️ Kernel update pending — reboot required"
else
    REBOOT_LINE=""
fi

# ── Compose message ───────────────────────────────────────────────────────────
MESSAGE="📊 Daily Summary — ${DATE_LABEL}

🗄 Database
  Size: ${TABLE_SIZE:-N/A} | Rows: ${TOTAL_ROWS:-N/A}
${TREND_LINE}  Last analyzed: ${VACUUM_STATUS}
  Dead tuples: ${DEAD_TUPLES:-0}
  Top tables:
$(echo "${TOP_TABLES:-N/A}" | sed 's/^/    /')

💾 Disk: ${DISK_USED} used / ${DISK_TOTAL} (${DISK_PCT}) ${DISK_ICON}
  Logs: trading-api=${TRADING_LOG_SIZE} | syslog=${SYSLOG_SIZE}
🧠 RAM: ${MEM_FREE} free / ${MEM_TOTAL} (used ${MEM_USED})

⚙️ Services
  Telegram bot: ${BOT_STATUS}
  API server:   ${API_STATUS}

📈 Today's data
  Symbols tracked: ${SYMBOLS_TODAY:-0}
  Ticks collected: ${TOTAL_TICKS_FMT}
    gap_ticks:  ${TICKS_FMT}
    candles_5s: ${CANDLES_FMT}
    gap_events: ${EVENTS_FMT}${REBOOT_LINE}"

_tg "$MESSAGE"
echo "$(date): Daily summary sent."

# ── Persist today's stats for tomorrow's trend comparison ────────────────────
# Format: DATE,TICKS_TODAY,TABLE_SIZE_BYTES
touch "$STATS_FILE"
_TMP=$(mktemp)
{ grep -v "^${TODAY}," "$STATS_FILE" 2>/dev/null || true; } > "$_TMP"
cat "$_TMP" > "$STATS_FILE"; rm -f "$_TMP"
echo "${TODAY},${TICKS_TODAY:-0},${TABLE_SIZE_BYTES:-0}" >> "$STATS_FILE"
# Keep only last 30 days of history
_TMP=$(mktemp)
tail -30 "$STATS_FILE" > "$_TMP"
cat "$_TMP" > "$STATS_FILE"; rm -f "$_TMP"
