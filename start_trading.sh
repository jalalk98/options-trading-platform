#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$HOME/trading_start.txt"
LOCK_FILE="$HOME/.start_trading.lock"

# Holiday check — manual flag
if [ -f "$HOME/.trading_paused" ]; then
    "$SCRIPT_DIR/notify.sh" "⏸ Trading session start skipped — holiday mode is ON."
    echo "Holiday mode active — skipping session start."
    exit 0
fi

# Holiday check — NSE holiday list
TODAY=$(date +%Y-%m-%d)
HOLIDAY_NAME=$(grep "^$TODAY " "$HOME/.trading_holidays" 2>/dev/null | cut -d' ' -f2-)
if [ -n "$HOLIDAY_NAME" ]; then
    "$SCRIPT_DIR/notify.sh" "⏸ Trading session start skipped — today is a market holiday: $HOLIDAY_NAME."
    echo "NSE holiday ($HOLIDAY_NAME) — skipping session start."
    exit 0
fi

# Prevent two simultaneous runs of this script
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo "start_trading.sh already running — exiting."
    exit 0
fi

echo "Clearing old Redis ticks..."
redis-cli DEL ticks_stream

echo "Current Redis stream size:"
redis-cli XLEN ticks_stream

# Restart both services cleanly
sudo systemctl restart db-writer.service
sleep 2
sudo systemctl restart tick-collector.service
sleep 5

# Verify both services are running
DB_OK=$(systemctl is-active --quiet db-writer.service && echo "yes" || echo "no")
TC_OK=$(systemctl is-active --quiet tick-collector.service && echo "yes" || echo "no")
TC_PID=$(systemctl show -p MainPID --value tick-collector.service 2>/dev/null)

if [ "$DB_OK" != "yes" ] || [ "$TC_OK" != "yes" ]; then
    "$SCRIPT_DIR/notify.sh" "❌ Trading session start FAILED — db_writer: $DB_OK, tick_collector: $TC_OK." "$LOG_FILE"
    echo "Trading session start failed."
    exit 1
fi

# Verify Kite token is valid
ENV_FILE="$SCRIPT_DIR/.env"
API_KEY=$(grep '^KITE_API_KEY'      "$ENV_FILE" | cut -d'=' -f2- | tr -d "' ")
TOKEN=$(grep   '^KITE_ACCESS_TOKEN' "$ENV_FILE" | cut -d'=' -f2- | tr -d "' ")

HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" \
    -H "X-Kite-Version: 3" \
    -H "Authorization: token ${API_KEY}:${TOKEN}" \
    "https://api.kite.trade/user/profile")

if [ "$HTTP_STATUS" = "200" ]; then
    "$SCRIPT_DIR/notify.sh" "✅ Trading session started successfully.
- db_writer      running
- tick_collector running (PID ${TC_PID:-unknown})
- api_server     running via systemd (always on)
- Kite token     valid ✅" "$LOG_FILE"
    echo "Trading session started. Kite token verified."
else
    "$SCRIPT_DIR/notify.sh" "⚠️ Trading session started but Kite token is INVALID (HTTP $HTTP_STATUS).
- db_writer      running
- tick_collector running (but token expired ❌)
- Run refresh_token.py then restart the session." "$LOG_FILE"
    echo "WARNING: Kite token invalid (HTTP $HTTP_STATUS). Re-run refresh_token.py."
fi
