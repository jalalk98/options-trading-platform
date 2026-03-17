import logging
import subprocess
import sys
from pathlib import Path

from backend.services.instrument_registry import extract_instrument_file

BASE_DIR = Path(__file__).resolve().parent
NOTIFY = str(BASE_DIR / "notify.sh")

logging.basicConfig(level=logging.INFO)

def notify(msg):
    subprocess.run(["/bin/bash", NOTIFY, msg], check=False)

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
