#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$HOME/trading_stop.log"

# Holiday check — manual flag
if [ -f "$HOME/.trading_paused" ]; then
    "$SCRIPT_DIR/notify.sh" "⏸ Trading session stop skipped — holiday mode is ON."
    echo "Holiday mode active — skipping session stop."
    exit 0
fi

# Holiday check — NSE holiday list
TODAY=$(date +%Y-%m-%d)
HOLIDAY_NAME=$(grep "^$TODAY " "$HOME/.trading_holidays" 2>/dev/null | cut -d' ' -f2-)
if [ -n "$HOLIDAY_NAME" ]; then
    "$SCRIPT_DIR/notify.sh" "⏸ Trading session stop skipped — today is a market holiday: $HOLIDAY_NAME."
    echo "NSE holiday ($HOLIDAY_NAME) — skipping session stop."
    exit 0
fi

echo "Stopping trading session..."

tmux kill-session -t trading

if [ $? -eq 0 ]; then
    "$SCRIPT_DIR/notify.sh" "🛑 Trading session stopped successfully at $(date '+%H:%M IST')." "$LOG_FILE"
else
    "$SCRIPT_DIR/notify.sh" "⚠️ Trading session stop — tmux session was not running." "$LOG_FILE"
fi
