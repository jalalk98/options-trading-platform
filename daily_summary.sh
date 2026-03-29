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

# ── DB health — query each value separately to avoid space-in-value parsing issues
_pg() { PGPASSWORD='MustafaHasnain@123' psql -h localhost -U postgres -d tickdata -tAq -c "$1" 2>/dev/null | tr -d ' \n'; }

TABLE_SIZE=$(_pg "SELECT pg_size_pretty(pg_total_relation_size('gap_ticks'));")
TABLE_SIZE_BYTES=$(_pg "SELECT pg_total_relation_size('gap_ticks');")
TOTAL_ROWS=$(_pg "SELECT COUNT(*) FROM gap_ticks;")
DEAD_TUPLES=$(_pg "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname='gap_ticks';")
LAST_ANALYZE_DATE=$(_pg "SELECT COALESCE(DATE(GREATEST(last_analyze, last_autoanalyze))::text, '1970-01-01') FROM pg_stat_user_tables WHERE relname='gap_ticks';")

# ── gap_ticks growth trend (compare to yesterday) ────────────────────────────
STATS_FILE="/var/log/trading-gap-ticks-stats.csv"
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)

TREND_LINE=""
if [ -f "$STATS_FILE" ]; then
    PREV=$(grep "^${YESTERDAY}," "$STATS_FILE" 2>/dev/null | tail -1)
    if [ -n "$PREV" ]; then
        PREV_ROWS=$(echo "$PREV" | cut -d',' -f2)
        PREV_BYTES=$(echo "$PREV" | cut -d',' -f3)
        DELTA_ROWS=$(( ${TOTAL_ROWS:-0} - ${PREV_ROWS:-0} ))
        DELTA_BYTES=$(( ${TABLE_SIZE_BYTES:-0} - ${PREV_BYTES:-0} ))
        DELTA_ROWS_FMT=$(printf "%'+d" "$DELTA_ROWS" 2>/dev/null || echo "+$DELTA_ROWS")
        # Convert bytes delta to human-readable
        DELTA_MB=$(awk "BEGIN {printf \"%.0f\", $DELTA_BYTES/1048576}")
        if [ "$DELTA_MB" -ge 0 ] 2>/dev/null; then
            DELTA_SIZE_FMT="+${DELTA_MB} MB"
        else
            DELTA_SIZE_FMT="${DELTA_MB} MB"
        fi
        TREND_LINE="  Growth vs yesterday: ${DELTA_ROWS_FMT} rows | ${DELTA_SIZE_FMT}"
    fi
fi

if [ "$LAST_ANALYZE_DATE" = "$TODAY" ]; then
    VACUUM_STATUS="today ✅"
else
    DAYS_AGO=$(( ( $(date +%s) - $(date -d "${LAST_ANALYZE_DATE:-1970-01-01}" +%s 2>/dev/null || echo $(date +%s)) ) / 86400 ))
    VACUUM_STATUS="${DAYS_AGO}d ago ⚠️"
fi

# ── Today's tick data ─────────────────────────────────────────────────────────
SYMBOLS_TODAY=$(_pg "SELECT COUNT(DISTINCT symbol) FROM gap_ticks WHERE timestamp >= '${TODAY} 03:30:00'::timestamp;")
TICKS_TODAY=$(_pg "SELECT COUNT(*) FROM gap_ticks WHERE timestamp >= '${TODAY} 03:30:00'::timestamp;")

# Format ticks with thousands separator
TICKS_FMT=$(printf "%'d" "${TICKS_TODAY:-0}" 2>/dev/null || echo "${TICKS_TODAY:-0}")

# ── Disk & memory ─────────────────────────────────────────────────────────────
DISK_USED=$(df -h / | awk 'NR==2 {print $3}')
DISK_TOTAL=$(df -h / | awk 'NR==2 {print $2}')
DISK_PCT=$(df -h / | awk 'NR==2 {print $5}')

# Warn if disk > 70%
DISK_PCT_NUM=${DISK_PCT//%/}
if [ "${DISK_PCT_NUM:-0}" -ge 70 ]; then
    DISK_ICON="⚠️"
else
    DISK_ICON="✅"
fi

# Top-3 PostgreSQL tables by total size
TOP_TABLES=$(_pg "SELECT string_agg(relname || ': ' || pg_size_pretty(pg_total_relation_size(relid)), E'\n' ORDER BY pg_total_relation_size(relid) DESC) FROM (SELECT relid, relname FROM pg_stat_user_tables ORDER BY pg_total_relation_size(relid) DESC LIMIT 3) t;")

# Log file sizes
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
  Ticks collected: ${TICKS_FMT}${REBOOT_LINE}"

_tg "$MESSAGE"
echo "$(date): Daily summary sent."

# ── Persist today's stats for tomorrow's trend comparison ────────────────────
touch "$STATS_FILE"
# Remove any existing entry for today before appending
grep -v "^${TODAY}," "$STATS_FILE" > "${STATS_FILE}.tmp" && mv "${STATS_FILE}.tmp" "$STATS_FILE"
echo "${TODAY},${TOTAL_ROWS:-0},${TABLE_SIZE_BYTES:-0}" >> "$STATS_FILE"
# Keep only last 30 days of history
tail -30 "$STATS_FILE" > "${STATS_FILE}.tmp" && mv "${STATS_FILE}.tmp" "$STATS_FILE"
