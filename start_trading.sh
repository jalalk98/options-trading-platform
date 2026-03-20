#!/bin/bash

SESSION="trading"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_FILE="$HOME/trading_start.txt"
PID_FILE="$HOME/.tick_collector.pid"
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

# Prevent two simultaneous runs of this script (race condition guard)
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
    echo "start_trading.sh already running — exiting."
    exit 0
fi

# Check if tick_collector is already running via PID file
if [ -f "$PID_FILE" ]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null && ps -p "$OLD_PID" -o comm= 2>/dev/null | grep -q "python"; then
        "$SCRIPT_DIR/notify.sh" "⚠️ tick_collector already running (PID $OLD_PID) — start skipped to prevent duplicate subscriptions."
        echo "tick_collector already running (PID $OLD_PID) — aborting."
        exit 0
    else
        echo "Stale PID file found (PID $OLD_PID not running) — cleaning up."
        rm -f "$PID_FILE"
    fi
fi

# Also catch orphan processes not tracked by PID file
ORPHAN_PID=$(pgrep -f "tick_collector" | head -1)
if [ -n "$ORPHAN_PID" ]; then
    echo "Orphan tick_collector found (PID $ORPHAN_PID) — killing before restart."
    kill "$ORPHAN_PID" 2>/dev/null
    sleep 2
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

# Wait for processes to settle then check tmux windows
sleep 5

WINDOWS=$(tmux list-windows -t $SESSION 2>/dev/null | wc -l)

if [ "$WINDOWS" -ne 2 ]; then
    "$SCRIPT_DIR/notify.sh" "❌ Trading session start FAILED — only $WINDOWS/2 tmux windows are running." "$LOG_FILE"
    echo "Trading session start may have failed."
    exit 1
fi

# Write tick_collector PID to file for watchdog and duplicate-run detection
TC_PID=$(pgrep -f "tick_collector" | head -1)
if [ -n "$TC_PID" ]; then
    echo "$TC_PID" > "$PID_FILE"
    echo "tick_collector started with PID $TC_PID"
fi

# Verify Kite token is valid by calling the profile endpoint
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
