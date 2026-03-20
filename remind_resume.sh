#!/bin/bash
# remind_resume.sh — Runs every day at 9:30pm.
# If holiday mode is ON, sends a Telegram reminder to send 'resume'.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PAUSE_FLAG="$HOME/.trading_paused"

if [ -f "$PAUSE_FLAG" ]; then
    "$SCRIPT_DIR/notify.sh" "⏸ Reminder: Holiday mode is still ON. Send 'resume' to re-enable trading for tomorrow."
fi
