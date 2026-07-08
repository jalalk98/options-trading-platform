#!/bin/bash
# sync_trade_journal.sh — Auto-sync today's trades from Zerodha into trade_journal.
# Called by cron at 3:31 PM IST on weekdays.

NOTIFY="/home/ubuntu/projects/options-trading-platform/notify.sh"
API="http://localhost:8000/api/journal/sync"

response=$(curl -s -X POST "$API" --max-time 30)
exit_code=$?

if [ $exit_code -ne 0 ]; then
    "$NOTIFY" "❌ Trade Journal auto-sync FAILED: curl error (exit $exit_code). Check ~/sync_trade_journal.txt"
    exit 1
fi

# Parse JSON fields: {"synced": N, "total_orders_from_zerodha": M}
synced=$(echo "$response" | grep -o '"synced":[0-9]*' | grep -o '[0-9]*')
total=$(echo "$response" | grep -o '"total_orders_from_zerodha":[0-9]*' | grep -o '[0-9]*')

if [ -z "$synced" ]; then
    "$NOTIFY" "❌ Trade Journal auto-sync: unexpected response — $response"
    exit 1
fi

if [ "$synced" -gt 0 ]; then
    "$NOTIFY" "📒 Trade Journal synced: $synced new trade(s) added ($total orders from Zerodha)"
else
    "$NOTIFY" "📒 Trade Journal synced: no new trades ($total orders from Zerodha, all already recorded)"
fi
