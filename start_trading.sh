#!/bin/bash



SESSION="trading"



tmux new-session -d -s $SESSION



tmux rename-window -t $SESSION:0 "tick_collector"

tmux send-keys -t $SESSION "cd ~/projects/options-trading-platform && source venv/bin/activate && python -m backend.processors.tick_collector" C-m



tmux new-window -t $SESSION -n "db_writer"

tmux send-keys -t $SESSION "cd ~/projects/options-trading-platform && source venv/bin/activate && python -m backend.processors.db_writer_runner" C-m



tmux new-window -t $SESSION -n "api_server"

tmux send-keys -t $SESSION "cd ~/projects/options-trading-platform && source venv/bin/activate && uvicorn backend.api.chart_server:app --host 0.0.0.0 --port 8000" C-m



tmux attach -t $SESSION
