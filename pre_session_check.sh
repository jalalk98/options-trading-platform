#!/bin/bash
# pre_session_check.sh — Pre-market health check at 09:09 IST Mon–Fri.
# Verifies services, WebSocket, Redis, PostgreSQL are all live before market open.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECRETS_FILE="$HOME/.kite_secrets"
TODAY=$(date +%Y-%m-%d)
DATE_LABEL=$(date '+%d %b %Y')

# ── Telegram helper ────────────────────────────────────────────────────────────
_tg() {
    local BOT_TOKEN CHAT_ID
    BOT_TOKEN=$(grep '^TELEGRAM_BOT_TOKEN=' "$SECRETS_FILE" 2>/dev/null | cut -d'=' -f2-)
    CHAT_ID=$(grep '^TELEGRAM_CHAT_ID='    "$SECRETS_FILE" 2>/dev/null | cut -d'=' -f2-)
    [ -n "$BOT_TOKEN" ] && [ -n "$CHAT_ID" ] && \
        curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
            -d chat_id="$CHAT_ID" \
            --data-urlencode "text=$1" > /dev/null
}

# ── Skip on holidays / paused ──────────────────────────────────────────────────
if [ -f "$HOME/.trading_paused" ]; then
    echo "$(date): Trading paused — skipping pre-session check."
    exit 0
fi
HOLIDAY_NAME=$(grep "^$TODAY " "$HOME/.trading_holidays" 2>/dev/null | cut -d' ' -f2-)
if [ -n "$HOLIDAY_NAME" ]; then
    echo "$(date): NSE holiday (${HOLIDAY_NAME}) — skipping pre-session check."
    exit 0
fi

# ── Individual checks ──────────────────────────────────────────────────────────

# 0. Restart trading-api to clear overnight swap accumulation
sudo systemctl restart trading-api.service
sleep 4
if systemctl is-active --quiet trading-api.service; then
    API_STATUS="✅ restarted (swap cleared)"
else
    API_STATUS="❌ FAILED TO RESTART"
fi

# 1. tick-collector service
if systemctl is-active --quiet tick-collector.service; then
    # Verify it started today (not a zombie from yesterday)
    START_TIME=$(systemctl show -p ActiveEnterTimestamp --value tick-collector.service 2>/dev/null)
    START_DATE=$(date -d "$START_TIME" +%Y-%m-%d 2>/dev/null)
    if [ "$START_DATE" = "$TODAY" ]; then
        TC_STATUS="✅ running (started today)"
    else
        TC_STATUS="⚠️ running but started ${START_DATE:-unknown}"
    fi
else
    TC_STATUS="❌ NOT RUNNING"
fi

# 2. db-writer service
if systemctl is-active --quiet db-writer.service; then
    DBW_STATUS="✅ running"
else
    DBW_STATUS="❌ NOT RUNNING"
fi

# 3. Redis
if redis-cli ping 2>/dev/null | grep -q PONG; then
    REDIS_STATUS="✅ up"
else
    REDIS_STATUS="❌ DOWN"
fi

# 4. PostgreSQL
if pg_isready -q 2>/dev/null; then
    PG_STATUS="✅ up"
else
    PG_STATUS="❌ DOWN"
fi

# 5. WebSocket — check logs for successful connection today
WS_CONNECTED=$(journalctl -u tick-collector.service --since "${TODAY} 00:00:00" --no-pager 2>/dev/null \
    | grep "Connected to WebSocket" | tail -1)
WS_ERROR=$(journalctl -u tick-collector.service --since "${TODAY} 00:00:00" --no-pager 2>/dev/null \
    | grep -c "Error during WebSocket connection" 2>/dev/null; true)

if [ -n "$WS_CONNECTED" ]; then
    WS_TIME=$(echo "$WS_CONNECTED" | awk '{print $3}')
    if [ "${WS_ERROR:-0}" -gt 0 ]; then
        WS_STATUS="✅ connected (${WS_TIME}) [${WS_ERROR} earlier error(s)]"
    else
        WS_STATUS="✅ connected (${WS_TIME})"
    fi
else
    WS_STATUS="❌ NOT connected (${WS_ERROR:-0} error(s) in logs)"
fi

# 6. Live ticks — most recent tick in Redis stream
LAST_TICK_RAW=$(redis-cli XREVRANGE ticks_stream + - COUNT 1 2>/dev/null | grep '"timestamp"' | grep -o '"timestamp": "[^"]*"' | cut -d'"' -f4)
if [ -n "$LAST_TICK_RAW" ]; then
    LAST_TICK_EPOCH=$(date -d "$LAST_TICK_RAW" +%s 2>/dev/null)
    NOW_EPOCH=$(date +%s)
    AGE=$(( NOW_EPOCH - ${LAST_TICK_EPOCH:-0} ))
    if [ "$AGE" -le 120 ]; then
        TICK_STATUS="✅ flowing (last: ${LAST_TICK_RAW#* })"
    elif [ "$AGE" -le 300 ]; then
        TICK_STATUS="⚠️ stale ${AGE}s ago (last: ${LAST_TICK_RAW#* })"
    else
        TICK_STATUS="❌ NO TICKS for ${AGE}s"
    fi
else
    TICK_STATUS="❌ no ticks in Redis stream"
fi

# 7. Token count subscribed
TOKEN_COUNT=$(journalctl -u tick-collector.service --since "${TODAY} 00:00:00" --no-pager 2>/dev/null \
    | grep "Total unique tokens to subscribe" | tail -1 | grep -o '[0-9]*$')
TOKEN_LINE="  Tokens subscribed: ${TOKEN_COUNT:-unknown}"

# 8. tick_rate.log freshness
TICK_RATE_LOG="$HOME/tick_rate.log"
if [ -f "$TICK_RATE_LOG" ]; then
    LOG_AGE=$(( $(date +%s) - $(stat -c %Y "$TICK_RATE_LOG") ))
    LOG_LAST=$(tail -1 "$TICK_RATE_LOG" 2>/dev/null)
    if [ "$LOG_AGE" -le 120 ]; then
        TICK_RATE_STATUS="✅ fresh (${LOG_AGE}s ago)"
    else
        # Pre-open: tick_metrics starts on WS connect; within a minute of session start is fine
        TICK_RATE_STATUS="⚠️ ${LOG_AGE}s old (thread may not have written yet)"
    fi
else
    TICK_RATE_STATUS="⚠️ log not found"
fi

# 9. Order API test — fetch SBIN LTP, place 1-qty regular limit-buy at 5% below LTP, cancel immediately
AMO_RESULT=$(cd "$SCRIPT_DIR" && ./venv/bin/python scripts/test_amo_order.py 2>/dev/null)
AMO_EXIT=$?
if [ $AMO_EXIT -eq 0 ]; then
    AMO_ORDER_ID=$(echo "$AMO_RESULT" | grep -o '"order_id": "[^"]*"' | cut -d'"' -f4)
    AMO_STATUS="✅ placed + cancelled (order ${AMO_ORDER_ID})"
else
    AMO_ERROR=$(echo "$AMO_RESULT" | grep -o '"error": "[^"]*"' | cut -d'"' -f4)
    AMO_STATUS="❌ FAILED — ${AMO_ERROR:-unknown error}"
fi

# ── Overall result ─────────────────────────────────────────────────────────────
ALL_OK=true
for STATUS in "$API_STATUS" "$TC_STATUS" "$DBW_STATUS" "$REDIS_STATUS" "$PG_STATUS" "$WS_STATUS" "$TICK_STATUS" "$AMO_STATUS"; do
    if echo "$STATUS" | grep -q "❌"; then
        ALL_OK=false
        break
    fi
done

if $ALL_OK; then
    HEADER="🟢 Pre-Session Check — ${DATE_LABEL}"
    FOOTER="All systems green. Market opens at 09:15."
else
    HEADER="🔴 Pre-Session Check — ${DATE_LABEL}"
    FOOTER="⚠️ Issues detected — check immediately before 09:15."
fi

# ── Compose and send ──────────────────────────────────────────────────────────
MESSAGE="${HEADER}

⚙️ Services
  trading-api:    ${API_STATUS}
  tick-collector: ${TC_STATUS}
  db-writer:      ${DBW_STATUS}

🔌 WebSocket
  ${WS_STATUS}
${TOKEN_LINE}

📡 Data
  Redis:      ${REDIS_STATUS}
  Ticks:      ${TICK_STATUS}
  tick_rate:  ${TICK_RATE_STATUS}

🗄 Database
  PostgreSQL: ${PG_STATUS}

📋 Order API (AMO test)
  ${AMO_STATUS}

${FOOTER}"

_tg "$MESSAGE"
echo "$(date): Pre-session check sent. All OK: ${ALL_OK}"
