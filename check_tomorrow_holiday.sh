#!/bin/bash
# Runs at 3:32pm Mon-Fri.
# If tomorrow is an NSE holiday, auto-sets pause flag and sends Telegram notification.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOLIDAYS_FILE="$HOME/.trading_holidays"
PAUSE_FLAG="$HOME/.trading_paused"

trap '"$SCRIPT_DIR/notify.sh" "❌ check_tomorrow_holiday.sh failed unexpectedly on $(date +%Y-%m-%d). Please check the script."' ERR

TOMORROW=$(date -d "tomorrow" +%Y-%m-%d)
TOMORROW_DOW=$(date -d "tomorrow" +%u)  # 6=Saturday, 7=Sunday

# Weekend — no flag needed, scripts handle weekends natively
if [ "$TOMORROW_DOW" -ge 6 ]; then
    "$SCRIPT_DIR/notify.sh" "🗓 Tomorrow is Saturday — market closed for the weekend. See you Monday!"
    exit 0
fi

HOLIDAY_NAME=$(grep "^$TOMORROW " "$HOLIDAYS_FILE" 2>/dev/null | cut -d' ' -f2-)

if [ -n "$HOLIDAY_NAME" ]; then
    touch "$PAUSE_FLAG"
    "$SCRIPT_DIR/notify.sh" "🗓 Tomorrow ($TOMORROW) is a market holiday: $HOLIDAY_NAME.

Holiday mode has been turned ON automatically.
Token refresh and trading session will be skipped tomorrow.
Send 'resume' if you want to override."
else
    # If flag was set by a previous auto-holiday and today is NOT a holiday, remind user it's active
    if [ -f "$PAUSE_FLAG" ]; then
        "$SCRIPT_DIR/notify.sh" "✅ Tomorrow ($TOMORROW) is a normal trading day.
Note: Holiday mode is still ON (set manually). Send 'resume' to re-enable trading."
    else
        "$SCRIPT_DIR/notify.sh" "✅ Tomorrow ($TOMORROW) is a normal trading day. See you tomorrow!"
    fi
fi
