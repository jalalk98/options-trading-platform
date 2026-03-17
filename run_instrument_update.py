import logging
import subprocess
import sys
from datetime import date
from pathlib import Path

from backend.services.instrument_registry import extract_instrument_file

BASE_DIR = Path(__file__).resolve().parent
NOTIFY = str(BASE_DIR / "notify.sh")
PAUSE_FLAG = Path.home() / ".trading_paused"
HOLIDAYS_FILE = Path.home() / ".trading_holidays"

logging.basicConfig(level=logging.INFO)

def notify(msg):
    subprocess.run(["/bin/bash", NOTIFY, msg], check=False)

# Holiday check — skip if token refresh was skipped (token would be expired)
today = date.today().strftime("%Y-%m-%d")

if PAUSE_FLAG.exists():
    print("Holiday mode active — skipping instrument update.")
    notify("⏸ Instrument update skipped — holiday mode is ON.")
    sys.exit(0)

holiday_name = None
if HOLIDAYS_FILE.exists():
    for line in HOLIDAYS_FILE.read_text().splitlines():
        if line.startswith(today):
            holiday_name = line.split(" ", 1)[1] if " " in line else "Holiday"
            break

if holiday_name:
    print(f"NSE holiday ({holiday_name}) — skipping instrument update.")
    notify(f"⏸ Instrument update skipped — today is a market holiday: {holiday_name}.")
    sys.exit(0)

print("Starting weekly instrument update...")

try:
    extract_instrument_file()
    csv_path = BASE_DIR / "data" / "Combined_Instruments.csv"
    row_count = sum(1 for _ in open(csv_path)) - 1  # exclude header
    print("Instrument update completed.")
    notify(f"✅ Instrument CSV updated successfully.\n{row_count:,} instruments loaded into data/Combined_Instruments.csv.")
except Exception as e:
    print(f"Instrument update FAILED: {e}")
    notify(f"❌ Instrument CSV update FAILED: {e}\nStrikes may be stale — check run_instrument_update.py manually.")
    sys.exit(1)
