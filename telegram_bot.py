#!/usr/bin/env python3
"""
Telegram bot for trading session control and adhoc job execution.

Text commands:
  holiday / /holiday  — pause all trading scripts
  resume  / /resume   — clear the pause flag
  status  / /status   — show current state

Inline buttons (sent via weekly_schedule.sh):
  Tap any job button to run it on demand.
  Jobs marked ⚠️ require a confirmation tap before executing.
"""

import os
import json
import time
import logging
import datetime
import subprocess
import threading
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
SCRIPT_DIR    = os.path.expanduser("~/projects/options-trading-platform")
VENV          = os.path.join(SCRIPT_DIR, "venv", "bin")
OFFSET_FILE   = os.path.expanduser("~/.telegram_bot_offset")
POLL_INTERVAL = 30  # seconds

# ── Job registry ──────────────────────────────────────────────────────────────
# confirm=True  → shows an "Are you sure?" step before executing
# env           → extra environment variables (merged on top of os.environ)
# cwd           → working directory (defaults to SCRIPT_DIR)
JOBS = {
    "token_refresh": {
        "label":   "🔑 Token Refresh",
        "cmd":     ["/bin/bash", os.path.expanduser("~/run_refresh_token.sh")],
        "timeout": 180,
        "confirm": False,
    },
    "instrument_update": {
        "label":   "📋 Instrument Update",
        "cmd":     [f"{VENV}/python", "run_instrument_update.py"],
        "timeout": 120,
        "confirm": False,
        "cwd":     SCRIPT_DIR,
    },
    "vacuum_all": {
        "label":   "🧹 VACUUM ANALYZE (all tables)",
        "cmd":     ["psql", "-h", "localhost", "-U", "postgres", "-d", "tickdata",
                    "-c", "VACUUM ANALYZE;"],
        "timeout": 600,
        "confirm": False,
        "env":     {"PGPASSWORD": "MustafaHasnain@123", "PGOPTIONS": "-c statement_timeout=0"},
    },
    "vacuum_gap_ticks": {
        "label":   "🧹 VACUUM ANALYZE gap_ticks",
        "cmd":     ["psql", "-h", "localhost", "-U", "postgres", "-d", "tickdata",
                    "-c", "VACUUM ANALYZE gap_ticks;"],
        "timeout": 300,
        "confirm": False,
        "env":     {"PGPASSWORD": "MustafaHasnain@123", "PGOPTIONS": "-c statement_timeout=0"},
    },
    "daily_summary": {
        "label":   "📊 Daily Summary",
        "cmd":     ["/bin/bash", os.path.join(SCRIPT_DIR, "daily_summary.sh")],
        "timeout": 60,
        "confirm": False,
    },
    "perf_report": {
        "label":   "📈 Performance Report",
        "cmd":     [f"{VENV}/python3", "scripts/daily_perf_report.py"],
        "timeout": 120,
        "confirm": False,
        "cwd":     SCRIPT_DIR,
    },
    "expiry_alert": {
        "label":   "🗓 Expiry Alert",
        "cmd":     [f"{VENV}/python", "expiry_alert.py"],
        "timeout": 60,
        "confirm": False,
        "cwd":     SCRIPT_DIR,
    },
    "stats_snapshot": {
        "label":   "📸 Stats Snapshot",
        "cmd":     [f"{VENV}/python3", "scripts/snap_stats_freshness.py"],
        "timeout": 30,
        "confirm": False,
        "cwd":     SCRIPT_DIR,
    },
    "create_partitions": {
        "label":   "💾 Create Daily Partitions",
        "cmd":     [f"{VENV}/python3", "scripts/create_daily_partition.py"],
        "timeout": 60,
        "confirm": False,
        "cwd":     SCRIPT_DIR,
    },
    "start_trading": {
        "label":   "▶️ Start Trading Session",
        "cmd":     ["/bin/bash", os.path.join(SCRIPT_DIR, "start_trading.sh")],
        "timeout": 60,
        "confirm": True,
    },
    "stop_trading": {
        "label":   "⏹ Stop Trading Session",
        "cmd":     ["/bin/bash", os.path.join(SCRIPT_DIR, "stop_trading.sh")],
        "timeout": 60,
        "confirm": True,
    },
    "archive_export": {
        "label":   "📦 Archive — Export to B2",
        "cmd":     [f"{VENV}/python3", "scripts/archive_to_b2_v3.py", "--phase", "export"],
        "timeout": None,
        "confirm": True,
        "cwd":     SCRIPT_DIR,
    },
    "archive_drop": {
        "label":   "🗑 Archive — Drop Old Partitions",
        "cmd":     [f"{VENV}/python3", "scripts/archive_to_b2_v3.py", "--phase", "drop"],
        "timeout": 120,
        "confirm": True,
        "cwd":     SCRIPT_DIR,
    },
    "pg_stat_reset": {
        "label":   "🔄 pg_stat Reset",
        "cmd":     ["sudo", "-u", "postgres", "psql", "-d", "tickdata",
                    "-c", "SELECT pg_stat_statements_reset();"],
        "timeout": 30,
        "confirm": False,
    },
    "analyze_candles": {
        "label":   "📈 ANALYZE candles_5s",
        "cmd":     ["psql", "-h", "localhost", "-U", "postgres", "-d", "tickdata",
                    "-c", "ANALYZE candles_5s;"],
        "timeout": 60,
        "confirm": False,
        "env":     {"PGPASSWORD": "MustafaHasnain@123", "PGOPTIONS": "-c statement_timeout=0"},
    },
    "check_holiday": {
        "label":   "🗓 Check Tomorrow Holiday",
        "cmd":     ["/bin/bash", os.path.join(SCRIPT_DIR, "check_tomorrow_holiday.sh")],
        "timeout": 30,
        "confirm": False,
    },
    "partition_vacuum": {
        "label":   "🧹 Partition VACUUM",
        "cmd":     ["/bin/bash", os.path.join(SCRIPT_DIR, "scripts", "vacuum_today_partition.sh")],
        "timeout": 120,
        "confirm": False,
    },
    "chart_investigation": {
        "label":   "🔍 Chart Investigation",
        "cmd":     ["/bin/bash", os.path.expanduser("~/scripts/chart_investigation_alert.sh")],
        "timeout": 60,
        "confirm": False,
    },
    "disk_cleanup": {
        "label":   "🗑 Disk Cleanup",
        "cmd":     ["/bin/bash", "/usr/local/bin/trading-cleanup.sh"],
        "timeout": 120,
        "confirm": True,
    },
    "vacuum_weekly": {
        "label":   "🧹 Weekly VACUUM ANALYZE",
        "cmd":     ["psql", "-h", "localhost", "-U", "postgres", "-d", "tickdata",
                    "-c", "VACUUM ANALYZE;"],
        "timeout": 1800,
        "confirm": True,
        "env":     {"PGPASSWORD": "MustafaHasnain@123", "PGOPTIONS": "-c statement_timeout=0"},
    },
}

# Pending confirmations: chat_id (str) -> job_key (str)
_pending_confirm: dict[str, str] = {}
_confirm_lock = threading.Lock()

# Currently running jobs — prevents duplicate launches
_running_jobs: set[str] = set()
_running_lock = threading.Lock()


# ── Telegram API helpers ──────────────────────────────────────────────────────

def _post(bot_token: str, method: str, **kwargs) -> dict:
    url = f"https://api.telegram.org/bot{bot_token}/{method}"
    try:
        resp = requests.post(url, timeout=10, **kwargs)
        return resp.json()
    except Exception as e:
        log.error("Telegram API error (%s): %s", method, e)
        return {}


def send_message(bot_token: str, chat_id: str, text: str, reply_markup=None) -> None:
    data: dict = {"chat_id": chat_id, "text": text}
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup)
    _post(bot_token, "sendMessage", data=data)


def answer_callback(bot_token: str, callback_query_id: str, text: str = "") -> None:
    """Must be called within 30 s of receiving a callback or Telegram shows a spinner error."""
    _post(bot_token, "answerCallbackQuery",
          data={"callback_query_id": callback_query_id, "text": text})


def get_updates(bot_token: str, offset: int) -> list:
    url  = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    resp = requests.get(url, params={"offset": offset, "timeout": 25}, timeout=35)
    resp.raise_for_status()
    return resp.json().get("result", [])


# ── Job execution ─────────────────────────────────────────────────────────────

def _run_in_thread(job_key: str, bot_token: str, chat_id: str) -> None:
    job     = JOBS[job_key]
    label   = job["label"]
    env     = {**os.environ, **(job.get("env") or {})}
    cwd     = job.get("cwd") or SCRIPT_DIR
    t0      = time.time()

    log.info("Adhoc job started: %s", job_key)
    try:
        with _running_lock:
            _running_jobs.add(job_key)
        result  = subprocess.run(
            job["cmd"],
            capture_output=True,
            text=True,
            timeout=job["timeout"],
            env=env,
            cwd=cwd,
        )
        elapsed = int(time.time() - t0)
        output  = (result.stdout + result.stderr).strip()
        # Last 3 lines of output for context, capped at 300 chars
        snippet = "\n".join(output.splitlines()[-3:])[:300] if output else ""

        if result.returncode == 0:
            msg = f"✅ {label} completed in {elapsed}s"
        else:
            msg = f"❌ {label} failed (exit {result.returncode}) in {elapsed}s"
        if snippet:
            msg += f"\n\n{snippet}"

    except subprocess.TimeoutExpired:
        elapsed = int(time.time() - t0)
        msg = f"⏱ {label} timed out after {elapsed}s"
    except Exception as e:
        msg = f"❌ {label} crashed: {e}"

    finally:
        with _running_lock:
            _running_jobs.discard(job_key)

    log.info("Adhoc job done: %s — %s", job_key, msg[:100])
    send_message(bot_token, chat_id, msg)


def launch_job(job_key: str, bot_token: str, chat_id: str) -> None:
    with _running_lock:
        if job_key in _running_jobs:
            send_message(bot_token, chat_id,
                         f"⚠️ {JOBS[job_key]['label']} is already running — ignoring duplicate request.")
            return
    send_message(bot_token, chat_id, f"⏳ Starting {JOBS[job_key]['label']}…")
    t = threading.Thread(
        target=_run_in_thread,
        args=(job_key, bot_token, chat_id),
        daemon=True,
        name=f"job-{job_key}",
    )
    t.start()


# ── Callback query handler ────────────────────────────────────────────────────

def handle_callback(cq: dict, bot_token: str, chat_id: str) -> None:
    cq_id = cq["id"]
    data  = cq.get("data", "")

    # Always acknowledge immediately to dismiss the button spinner
    answer_callback(bot_token, cq_id)

    if data.startswith("run:"):
        job_key = data[4:]
        if job_key not in JOBS:
            send_message(bot_token, chat_id, f"❓ Unknown job: {job_key}")
            return

        if JOBS[job_key]["confirm"]:
            with _confirm_lock:
                _pending_confirm[chat_id] = job_key
            keyboard = {"inline_keyboard": [[
                {"text": f"✅ Yes, run it",  "callback_data": f"confirm:{job_key}"},
                {"text": "❌ Cancel",         "callback_data": "cancel"},
            ]]}
            send_message(
                bot_token, chat_id,
                f"⚠️ Are you sure you want to run:\n{JOBS[job_key]['label']}?",
                reply_markup=keyboard,
            )
        else:
            launch_job(job_key, bot_token, chat_id)

    elif data.startswith("confirm:"):
        job_key = data[8:]
        with _confirm_lock:
            expected = _pending_confirm.pop(chat_id, None)
        if expected == job_key and job_key in JOBS:
            launch_job(job_key, bot_token, chat_id)
        else:
            send_message(bot_token, chat_id,
                         "⚠️ Confirmation mismatch — please tap the button again.")

    elif data == "cancel":
        with _confirm_lock:
            _pending_confirm.pop(chat_id, None)
        send_message(bot_token, chat_id, "❌ Cancelled.")

    elif data == "noop":
        pass  # section header button — tapping it does nothing

    else:
        send_message(bot_token, chat_id, f"❓ Unknown action: {data}")


# ── Text command handler ──────────────────────────────────────────────────────

def get_holiday(date_str: str) -> str | None:
    try:
        with open(HOLIDAYS_FILE) as f:
            for line in f:
                line = line.strip()
                if line.startswith(date_str):
                    return line[len(date_str):].strip() or "NSE Holiday"
    except FileNotFoundError:
        pass
    return None


def handle_command(cmd: str, bot_token: str, chat_id: str) -> None:
    cmd = cmd.strip().lstrip("/").lower()

    if cmd == "holiday":
        send_message(bot_token, chat_id, "⏳ Processing…")
        time.sleep(1)
        with open(PAUSE_FLAG, "w") as f:
            f.write("")
        log.info("Holiday flag set.")
        send_message(bot_token, chat_id,
                     "⏸ Holiday mode ON.\n"
                     "Token refresh, session start/stop will all be skipped.\n"
                     "Send 'resume' to re-enable.")

    elif cmd == "resume":
        send_message(bot_token, chat_id, "⏳ Processing…")
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
        lines    = []
        if os.path.exists(PAUSE_FLAG):
            lines.append("⏸ Status: PAUSED (holiday mode is ON manually)")
            lines.append("Send 'resume' to re-enable.")
        elif (h := get_holiday(today)):
            lines.append(f"⏸ Status: PAUSED — today is a market holiday: {h}")
        else:
            lines.append("✅ Status: ACTIVE (trading scripts will run normally)")
        if (h := get_holiday(tomorrow)):
            lines.append(f"\n🗓 Tomorrow is a market holiday: {h}")
        send_message(bot_token, chat_id, "\n".join(lines))

    else:
        send_message(bot_token, chat_id,
                     f"❓ Unknown command: '{cmd}'\n"
                     "Available commands:\n"
                     "  holiday — pause all trading scripts\n"
                     "  resume  — re-enable trading scripts\n"
                     "  status  — show current state")


# ── Main loop ─────────────────────────────────────────────────────────────────

def load_secrets() -> dict:
    secrets: dict = {}
    with open(SECRETS_FILE) as f:
        for line in f:
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                key, _, val = line.partition("=")
                secrets[key.strip()] = val.strip()
    return secrets


def load_offset() -> int:
    try:
        return int(open(OFFSET_FILE).read().strip())
    except Exception:
        return 0


def save_offset(offset: int) -> None:
    try:
        with open(OFFSET_FILE, "w") as f:
            f.write(str(offset))
    except Exception as e:
        log.warning("Could not save offset: %s", e)


def main() -> None:
    secrets   = load_secrets()
    bot_token = secrets["TELEGRAM_BOT_TOKEN"]
    chat_id   = secrets["TELEGRAM_CHAT_ID"]

    log.info("Telegram bot listener started. Polling every %ds.", POLL_INTERVAL)
    send_message(bot_token, chat_id,
                 "🤖 Trading bot listener started.\n"
                 "Commands: holiday | resume | status\n"
                 "Tap any button in the weekly schedule to run a job on demand.")

    offset = load_offset()
    log.info("Starting from update offset %d", offset)
    while True:
        try:
            updates = get_updates(bot_token, offset)
            for update in updates:
                offset = update["update_id"] + 1
                save_offset(offset)

                # ── Inline button tap ──────────────────────────────────────
                if "callback_query" in update:
                    cq      = update["callback_query"]
                    from_id = str(
                        cq.get("message", {}).get("chat", {}).get("id", "")
                        or cq.get("from", {}).get("id", "")
                    )
                    if from_id != chat_id:
                        log.warning("Ignored callback from unknown chat_id: %s", from_id)
                        continue
                    log.info("Callback: %s", cq.get("data"))
                    handle_callback(cq, bot_token, chat_id)
                    continue

                # ── Text message ───────────────────────────────────────────
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip()
                from_id = str(msg.get("chat", {}).get("id", ""))

                if from_id != chat_id:
                    if from_id:
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
