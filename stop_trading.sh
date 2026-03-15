#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$HOME/trading_stop.log"

# Holiday check
if [ -f "$HOME/.trading_paused" ]; then
    "$SCRIPT_DIR/notify.sh" "⏸ Trading session stop skipped — holiday mode is ON."
    echo "Holiday mode active — skipping session stop."
    exit 0
fi

echo "Stopping trading session..."

tmux kill-session -t trading

if [ $? -eq 0 ]; then
    "$SCRIPT_DIR/notify.sh" "🛑 Trading session stopped successfully at $(date '+%H:%M IST')." "$LOG_FILE"
else
    "$SCRIPT_DIR/notify.sh" "⚠️ Trading session stop — tmux session was not running." "$LOG_FILE"
fi
