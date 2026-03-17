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

# ── DB health — query each value separately to avoid space-in-value parsing issues
PG="PGPASSWORD='MustafaHasnain@123' psql -h localhost -U postgres -d tickdata -tAq"

TABLE_SIZE=$(eval $PG -c "SELECT pg_size_pretty(pg_total_relation_size('gap_ticks'));" 2>/dev/null | tr -d ' ')
TOTAL_ROWS=$(eval $PG -c "SELECT COUNT(*) FROM gap_ticks;" 2>/dev/null | tr -d ' ')
DEAD_TUPLES=$(eval $PG -c "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname='gap_ticks';" 2>/dev/null | tr -d ' ')
LAST_ANALYZE_DATE=$(eval $PG -c "SELECT COALESCE(DATE(last_analyze)::text, '1970-01-01') FROM pg_stat_user_tables WHERE relname='gap_ticks';" 2>/dev/null | tr -d ' ')

if [ "$LAST_ANALYZE_DATE" = "$TODAY" ]; then
    VACUUM_STATUS="today ✅"
else
    DAYS_AGO=$(( ( $(date +%s) - $(date -d "${LAST_ANALYZE_DATE:-1970-01-01}" +%s 2>/dev/null || echo $(date +%s)) ) / 86400 ))
    VACUUM_STATUS="${DAYS_AGO}d ago ⚠️"
fi

# ── Today's tick data ─────────────────────────────────────────────────────────
SYMBOLS_TODAY=$(eval $PG -c "SELECT COUNT(DISTINCT symbol) FROM gap_ticks WHERE timestamp >= '${TODAY} 03:30:00'::timestamp;" 2>/dev/null | tr -d ' ')
TICKS_TODAY=$(eval $PG -c "SELECT COUNT(*) FROM gap_ticks WHERE timestamp >= '${TODAY} 03:30:00'::timestamp;" 2>/dev/null | tr -d ' ')

# Format ticks with thousands separator
TICKS_FMT=$(printf "%'d" "${TICKS_TODAY:-0}" 2>/dev/null || echo "${TICKS_TODAY:-0}")

# ── Disk & memory ─────────────────────────────────────────────────────────────
DISK_USED=$(df -h / | awk 'NR==2 {print $3}')
DISK_TOTAL=$(df -h / | awk 'NR==2 {print $2}')
DISK_PCT=$(df -h / | awk 'NR==2 {print $5}')

# Warn if disk > 80%
DISK_PCT_NUM=${DISK_PCT//%/}
if [ "${DISK_PCT_NUM:-0}" -ge 80 ]; then
    DISK_ICON="⚠️"
else
    DISK_ICON="✅"
fi

MEM_TOTAL=$(free -h | awk '/^Mem/ {print $2}')
MEM_USED=$(free -h  | awk '/^Mem/ {print $3}')
MEM_FREE=$(free -h  | awk '/^Mem/ {print $7}')

# ── Service health ────────────────────────────────────────────────────────────
if systemctl is-active --quiet trading-bot.service; then
    BOT_STATUS="✅ running"
else
    BOT_STATUS="❌ stopped"
fi

if curl -s --max-time 3 http://localhost:8000/api/strikes > /dev/null 2>&1; then
    API_STATUS="✅ running (port 8000)"
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
  Last analyzed: ${VACUUM_STATUS}
  Dead tuples: ${DEAD_TUPLES:-0}

💾 Disk: ${DISK_USED} used / ${DISK_TOTAL} (${DISK_PCT}) ${DISK_ICON}
🧠 RAM: ${MEM_FREE} free / ${MEM_TOTAL} (used ${MEM_USED})

⚙️ Services
  Telegram bot: ${BOT_STATUS}
  API server:   ${API_STATUS}

📈 Today's data
  Symbols tracked: ${SYMBOLS_TODAY:-0}
  Ticks collected: ${TICKS_FMT}${REBOOT_LINE}"

_tg "$MESSAGE"
echo "$(date): Daily summary sent."
