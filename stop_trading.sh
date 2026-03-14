#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$HOME/trading_stop.log"

echo "Stopping trading session..."

tmux kill-session -t trading

if [ $? -eq 0 ]; then
    "$SCRIPT_DIR/notify.sh" "🛑 Trading session stopped successfully at $(date '+%H:%M IST')." "$LOG_FILE"
else
    "$SCRIPT_DIR/notify.sh" "⚠️ Trading session stop — tmux session was not running." "$LOG_FILE"
fi
