#!/bin/bash
# watchdog_tick_collector.sh — Checks if tick_collector is alive during trading hours.
# Runs every 5 minutes Mon–Fri between 9:15am and 3:30pm.
# If the service is not running, restarts it and sends a Telegram alert.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE="tick-collector.service"

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
    echo "$(date): Past market close (15:30) — not restarting tick_collector."
    exit 0
fi

# Check if tick-collector service is active
if systemctl is-active --quiet "$SERVICE"; then
    exit 0
fi

# Service is down — alert and restart
PID=$(systemctl show -p MainPID --value "$SERVICE" 2>/dev/null || echo "?")
"$SCRIPT_DIR/notify.sh" "⚠️ tick_collector (PID $PID) has died — restarting now."
echo "$(date): tick_collector service down — restarting."

sudo systemctl restart "$SERVICE"
sleep 5

if systemctl is-active --quiet "$SERVICE"; then
    NEW_PID=$(systemctl show -p MainPID --value "$SERVICE" 2>/dev/null)
    "$SCRIPT_DIR/notify.sh" "✅ tick_collector restarted successfully (PID $NEW_PID)."
    echo "$(date): tick_collector restarted with PID $NEW_PID."
else
    "$SCRIPT_DIR/notify.sh" "❌ tick_collector restart FAILED — please check the server manually."
    echo "$(date): tick_collector restart failed."
fi
