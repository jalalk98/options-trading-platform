#!/bin/bash
# notify.sh — Send a Telegram message, optionally attaching a log file.
# Usage: notify.sh "message text" [/path/to/logfile]

MESSAGE="$1"
LOGFILE="$2"

SECRETS_FILE="$HOME/.kite_secrets"

BOT_TOKEN=$(grep '^TELEGRAM_BOT_TOKEN=' "$SECRETS_FILE" | cut -d'=' -f2-)
CHAT_ID=$(grep '^TELEGRAM_CHAT_ID='    "$SECRETS_FILE" | cut -d'=' -f2-)

if [ -z "$BOT_TOKEN" ] || [ -z "$CHAT_ID" ]; then
    echo "notify.sh: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing from $SECRETS_FILE"
    exit 1
fi

if [ -n "$LOGFILE" ] && [ -f "$LOGFILE" ]; then
    curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendDocument" \
        -F "chat_id=${CHAT_ID}" \
        -F "caption=${MESSAGE}" \
        -F "document=@${LOGFILE}" \
        > /dev/null
else
    curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
        -d "chat_id=${CHAT_ID}" \
        --data-urlencode "text=${MESSAGE}" \
        > /dev/null
fi
