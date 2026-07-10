#!/bin/bash
# watchdog_fyers_freshness.sh — Checks Fyers ticks are actually being STORED
# during trading hours (data freshness, not just service status — a service can
# be "active" while writing nothing, as with the Zerodha reconnect bug and the
# 2026-07-10 fyers_db_writer parser bug).
# Runs every 5 minutes Mon–Fri between 9:20am and 3:30pm.
# If fyers_ticks max(timestamp) is older than MAX_AGE_SEC, restarts both Fyers
# services and sends a Telegram alert.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MAX_AGE_SEC=120

# Skip if holiday mode is ON
if [ -f "$HOME/.trading_paused" ]; then
    exit 0
fi

# Skip if today is an NSE holiday
TODAY=$(date +%Y-%m-%d)
HOLIDAY_NAME=$(grep "^$TODAY " "$HOME/.trading_holidays" 2>/dev/null | cut -d' ' -f2-)
if [ -n "$HOLIDAY_NAME" ]; then
    exit 0
fi

# Skip if current time is at or past market close (15:30)
CURRENT_TIME=$(date +%H%M)
if [ "$CURRENT_TIME" -ge "1530" ]; then
    exit 0
fi

# Age (seconds) of the newest stored Fyers tick; 999999 if table is empty.
AGE=$(PGPASSWORD='MustafaHasnain@123' psql -h localhost -U postgres -d tickdata -t -A -c \
    "SELECT COALESCE(EXTRACT(EPOCH FROM (now() AT TIME ZONE 'Asia/Kolkata' - max(timestamp)))::int, 999999) FROM fyers_ticks;" 2>/dev/null)

if [ -z "$AGE" ]; then
    "$SCRIPT_DIR/notify.sh" "❌ Fyers watchdog could not query fyers_ticks — check PostgreSQL."
    echo "$(date): fyers watchdog DB query failed."
    exit 1
fi

if [ "$AGE" -le "$MAX_AGE_SEC" ]; then
    exit 0
fi

# Stale — gather context, alert, restart both Fyers services
COLLECTOR_STATE=$(systemctl is-active fyers-collector)
WRITER_STATE=$(systemctl is-active fyers-db-writer)
STREAM_LAG=$(redis-cli XINFO GROUPS fyers_ticks_stream 2>/dev/null | grep -A1 '^lag' | tail -1)

"$SCRIPT_DIR/notify.sh" "⚠️ Fyers ticks STALE: last stored tick ${AGE}s ago (collector=$COLLECTOR_STATE writer=$WRITER_STATE lag=${STREAM_LAG:-?}). Restarting Fyers services. If this repeats, the daily token may be expired — run the manual token refresh."
echo "$(date): fyers ticks stale (${AGE}s) — restarting fyers services."

sudo systemctl restart fyers-db-writer fyers-collector
sleep 30

AGE2=$(PGPASSWORD='MustafaHasnain@123' psql -h localhost -U postgres -d tickdata -t -A -c \
    "SELECT COALESCE(EXTRACT(EPOCH FROM (now() AT TIME ZONE 'Asia/Kolkata' - max(timestamp)))::int, 999999) FROM fyers_ticks;" 2>/dev/null)

if [ -n "$AGE2" ] && [ "$AGE2" -le "$MAX_AGE_SEC" ]; then
    "$SCRIPT_DIR/notify.sh" "✅ Fyers tick flow recovered after restart (last tick ${AGE2}s ago)."
    echo "$(date): fyers tick flow recovered (${AGE2}s)."
else
    "$SCRIPT_DIR/notify.sh" "❌ Fyers ticks STILL STALE after restart (${AGE2:-?}s). Likely an expired access token — run get_token_manual, or check journalctl -u fyers-collector."
    echo "$(date): fyers still stale after restart (${AGE2:-?}s)."
fi
