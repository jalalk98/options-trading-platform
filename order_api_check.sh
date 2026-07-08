#!/bin/bash
# order_api_check.sh — Order API connectivity test at 09:02 IST Mon–Fri.
# Pre-open order collection is open (09:00–09:08), so a real order can be placed + cancelled.
# Sends a Telegram message on both success and failure.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SECRETS_FILE="$HOME/.kite_secrets"
DATE_LABEL=$(date '+%d %b %Y')

_tg() {
    local BOT_TOKEN CHAT_ID
    BOT_TOKEN=$(grep '^TELEGRAM_BOT_TOKEN=' "$SECRETS_FILE" 2>/dev/null | cut -d'=' -f2-)
    CHAT_ID=$(grep '^TELEGRAM_CHAT_ID='    "$SECRETS_FILE" 2>/dev/null | cut -d'=' -f2-)
    [ -n "$BOT_TOKEN" ] && [ -n "$CHAT_ID" ] && \
        curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
            -d chat_id="$CHAT_ID" \
            --data-urlencode "text=$1" > /dev/null
}

if [ -f "$HOME/.trading_paused" ]; then
    echo "$(date): Trading paused — skipping order API check."
    exit 0
fi
TODAY=$(date +%Y-%m-%d)
HOLIDAY_NAME=$(grep "^$TODAY " "$HOME/.trading_holidays" 2>/dev/null | cut -d' ' -f2-)
if [ -n "$HOLIDAY_NAME" ]; then
    echo "$(date): NSE holiday (${HOLIDAY_NAME}) — skipping order API check."
    exit 0
fi

AMO_RESULT=$(cd "$SCRIPT_DIR" && ./venv/bin/python scripts/test_amo_order.py 2>/dev/null)
AMO_EXIT=$?

if [ $AMO_EXIT -eq 0 ]; then
    AMO_ORDER_ID=$(echo "$AMO_RESULT" | grep -o '"order_id": "[^"]*"' | cut -d'"' -f4)
    NIFTY_LTP=$(echo "$AMO_RESULT" | grep -o '"nifty_ltp": [^,}]*' | awk '{print $2}')
    SYMBOL=$(echo "$AMO_RESULT" | grep -o '"symbol": "[^"]*"' | cut -d'"' -f4)
    EXPIRY=$(echo "$AMO_RESULT" | grep -o '"expiry": "[^"]*"' | cut -d'"' -f4)
    OPT_LTP=$(echo "$AMO_RESULT" | grep -o '"option_ltp": [^,}]*' | awk '{print $2}')
    TEST_PRICE=$(echo "$AMO_RESULT" | grep -o '"test_price": [^,}]*' | awk '{print $2}')
    MSG="✅ Order API — ${DATE_LABEL}
Placed + cancelled Nifty option test order OK
  Symbol:     ${SYMBOL}
  Expiry:     ${EXPIRY}
  Nifty LTP:  ₹${NIFTY_LTP}
  Option LTP: ₹${OPT_LTP}  →  test price ₹${TEST_PRICE}
  Order ID:   ${AMO_ORDER_ID}"
else
    AMO_ERROR=$(echo "$AMO_RESULT" | grep -o '"error": "[^"]*"' | cut -d'"' -f4)
    MSG="❌ Order API — ${DATE_LABEL}
FAILED — Nifty option order placement test unsuccessful
  Error: ${AMO_ERROR:-unknown error}
⚠️ Fix before 09:15 or orders will fail in live market."
fi

_tg "$MSG"
echo "$(date): Order API check done. Exit: ${AMO_EXIT}"
