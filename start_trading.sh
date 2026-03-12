#!/bin/bash

SESSION="trading"

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

tmux new-window -t $SESSION -n api_server

tmux send-keys -t $SESSION:2 "cd ~/projects/options-trading-platform && ~/projects/options-trading-platform/venv/bin/uvicorn backend.api.chart_server:app --host 0.0.0.0 --port 8000" C-m