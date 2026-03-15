#!/usr/bin/env python3
"""
Lightweight Telegram bot listener for trading session control.
Polls Telegram every 30s and responds to commands:
  holiday / /holiday  — pause all trading scripts for today
  resume  / /resume   — clear the pause flag
  status  / /status   — show current state
"""

import os
import time
import logging
import datetime
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-5s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

SECRETS_FILE  = os.path.expanduser("~/.kite_secrets")
PAUSE_FLAG    = os.path.expanduser("~/.trading_paused")
HOLIDAYS_FILE = os.path.expanduser("~/.trading_holidays")
POLL_INTERVAL = 30  # seconds


def get_holiday(date_str: str) -> str | None:
    """Return holiday name for given YYYY-MM-DD date, or None."""
    try:
        with open(HOLIDAYS_FILE) as f:
            for line in f:
                line = line.strip()
                if line.startswith(date_str):
                    return line[len(date_str):].strip() or "NSE Holiday"
    except FileNotFoundError:
        pass
    return None


def load_secrets():
    secrets = {}
    with open(SECRETS_FILE) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                key, _, val = line.partition("=")
                secrets[key.strip()] = val.strip()
    return secrets


def send_message(bot_token, chat_id, text):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    requests.post(url, data={"chat_id": chat_id, "text": text}, timeout=10)


def get_updates(bot_token, offset):
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    resp = requests.get(url, params={"offset": offset, "timeout": 25}, timeout=35)
    resp.raise_for_status()
    return resp.json().get("result", [])


def handle_command(cmd, bot_token, chat_id):
    cmd = cmd.strip().lstrip("/").lower()

    if cmd == "holiday":
        send_message(bot_token, chat_id, "⏳ Processing...")
        time.sleep(1)
        with open(PAUSE_FLAG, "w") as f:
            f.write("")
        log.info("Holiday flag set.")
        send_message(bot_token, chat_id,
            "⏸ Holiday mode ON.\n"
            "Token refresh, session start and stop will all be skipped.\n"
            "Send 'resume' to re-enable.")

    elif cmd == "resume":
        send_message(bot_token, chat_id, "⏳ Processing...")
        time.sleep(1)
        if os.path.exists(PAUSE_FLAG):
            os.remove(PAUSE_FLAG)
            log.info("Holiday flag cleared.")
            send_message(bot_token, chat_id,
                "▶️ Holiday mode OFF.\n"
                "Trading scripts will run as normal from next trigger.")
        else:
            send_message(bot_token, chat_id,
                "ℹ️ Already active — no holiday flag was set.")

    elif cmd == "status":
        today    = datetime.date.today().isoformat()
        tomorrow = (datetime.date.today() + datetime.timedelta(days=1)).isoformat()
        today_holiday    = get_holiday(today)
        tomorrow_holiday = get_holiday(tomorrow)

        lines = []
        if os.path.exists(PAUSE_FLAG):
            lines.append("⏸ Status: PAUSED (holiday mode is ON manually)")
            lines.append("Send 'resume' to re-enable.")
        elif today_holiday:
            lines.append(f"⏸ Status: PAUSED — today is a market holiday: {today_holiday}")
        else:
            lines.append("✅ Status: ACTIVE (trading scripts will run normally)")

        if tomorrow_holiday:
            lines.append(f"\n🗓 Tomorrow is a market holiday: {tomorrow_holiday}")
        send_message(bot_token, chat_id, "\n".join(lines))

    else:
        send_message(bot_token, chat_id,
            f"❓ Unknown command: '{cmd}'\n"
            "Available commands:\n"
            "  holiday — pause all trading scripts\n"
            "  resume  — re-enable trading scripts\n"
            "  status  — show current state")


def main():
    secrets   = load_secrets()
    bot_token = secrets["TELEGRAM_BOT_TOKEN"]
    chat_id   = secrets["TELEGRAM_CHAT_ID"]

    log.info("Telegram bot listener started. Polling every %ds.", POLL_INTERVAL)
    send_message(bot_token, chat_id, "🤖 Trading bot listener started.\nCommands: holiday | resume | status")

    offset = 0
    while True:
        try:
            updates = get_updates(bot_token, offset)
            for update in updates:
                offset = update["update_id"] + 1
                msg = update.get("message", {})
                text = msg.get("text", "").strip()
                from_id = str(msg.get("chat", {}).get("id", ""))

                # Only respond to the authorised chat
                if from_id != chat_id:
                    log.warning("Ignored message from unknown chat_id: %s", from_id)
                    continue

                if text:
                    log.info("Received: %s", text)
                    handle_command(text, bot_token, chat_id)

        except Exception as e:
            log.error("Polling error: %s", e)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
