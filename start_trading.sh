#!/bin/bash

SESSION="trading"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$HOME/trading_start.txt"

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

tmux kill-session -t $SESSION 2>/dev/null

echo "Clearing old Redis ticks..."
redis-cli DEL ticks_stream

echo "Current Redis stream size:"
redis-cli XLEN ticks_stream

tmux new-session -d -s $SESSION -n db_writer

tmux send-keys -t $SESSION:0 "cd ~/projects/options-trading-platform && ~/projects/options-trading-platform/venv/bin/python -m backend.processors.db_writer_runner" C-m

sleep 2

tmux new-window -t $SESSION -n tick_collector

tmux send-keys -t $SESSION:1 "cd ~/projects/options-trading-platform && ~/projects/options-trading-platform/venv/bin/python -m backend.processors.tick_collector" C-m

# Wait for processes to settle then check health
sleep 5

WINDOWS=$(tmux list-windows -t $SESSION 2>/dev/null | wc -l)

if [ "$WINDOWS" -eq 2 ]; then
    "$SCRIPT_DIR/notify.sh" "✅ Trading session started successfully.
- db_writer      running
- tick_collector running
- api_server     running via systemd (always on)" "$LOG_FILE"
    echo "Trading session started."
else
    "$SCRIPT_DIR/notify.sh" "❌ Trading session start FAILED — only $WINDOWS/2 tmux windows are running." "$LOG_FILE"
    echo "Trading session start may have failed."
fi