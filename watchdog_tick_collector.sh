#!/bin/bash
# watchdog_tick_collector.sh — Checks if tick_collector is alive during trading hours.
# Runs every 5 minutes Mon–Fri between 9:15am and 3:30pm.
# If the process is dead, restarts it and sends a Telegram alert.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$HOME/.tick_collector.pid"
SESSION="trading"

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

# Skip if no PID file — session was never started
if [ ! -f "$PID_FILE" ]; then
    exit 0
fi

PID=$(cat "$PID_FILE")

# Check if tick_collector is still alive
if kill -0 "$PID" 2>/dev/null && ps -p "$PID" -o comm= 2>/dev/null | grep -q "python"; then
    # All good — process is running
    exit 0
fi

# Process is dead — alert and restart
"$SCRIPT_DIR/notify.sh" "⚠️ tick_collector (PID $PID) has died — restarting now."
echo "$(date): tick_collector died — restarting."

# Remove stale PID file
rm -f "$PID_FILE"

# Restart tick_collector in the existing tmux session
if tmux has-session -t "$SESSION" 2>/dev/null; then
    tmux send-keys -t "$SESSION:1" "" ""
    tmux send-keys -t "$SESSION:1" "cd ~/projects/options-trading-platform && ~/projects/options-trading-platform/venv/bin/python -m backend.processors.tick_collector" C-m
    sleep 5
    NEW_PID=$(pgrep -f "tick_collector" | head -1)
    if [ -n "$NEW_PID" ]; then
        echo "$NEW_PID" > "$PID_FILE"
        "$SCRIPT_DIR/notify.sh" "✅ tick_collector restarted successfully (PID $NEW_PID)."
        echo "$(date): tick_collector restarted with PID $NEW_PID."
    else
        "$SCRIPT_DIR/notify.sh" "❌ tick_collector restart FAILED — please check the server manually."
        echo "$(date): tick_collector restart failed."
    fi
else
    "$SCRIPT_DIR/notify.sh" "❌ tmux session '$SESSION' not found — cannot restart tick_collector. Please start the trading session manually."
    echo "$(date): tmux session not found — cannot restart."
fi
