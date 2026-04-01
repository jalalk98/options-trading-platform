#!/usr/bin/env python3
"""
expiry_alert.py — Send a Telegram alert if any index has an expiry tomorrow.

Special case: if expiry falls on a Monday, the alert is sent on Friday
(since the cron does not run on weekends).

Run via cron at 10:00 AM IST Mon–Fri.
"""

import sys
import requests
from pathlib import Path
from datetime import date, timedelta

import pandas as pd

BASE_DIR  = Path(__file__).resolve().parent
DATA_DIR  = BASE_DIR / "data"
CSV_PATH  = DATA_DIR / "Combined_Instruments.csv"
SECRETS   = Path.home() / ".kite_secrets"

# Strike counts mirror tick_collector.py subscriptions
STRIKE_CONFIG = {
    "NIFTY":      {"gap": 50,  "count": 10},
    "SENSEX":     {"gap": 100, "count": 10},
    "BANKNIFTY":  {"gap": 100, "count": 8},
    "FINNIFTY":   {"gap": 50,  "count": 6},
    "MIDCPNIFTY": {"gap": 25,  "count": 6},
}


def _load_secrets():
    secrets = {}
    try:
        for line in SECRETS.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                secrets[k.strip()] = v.strip()
    except Exception as e:
        print(f"Could not read secrets: {e}", file=sys.stderr)
    return secrets


def _send_telegram(text: str) -> None:
    s = _load_secrets()
    token   = s.get("TELEGRAM_BOT_TOKEN")
    chat_id = s.get("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("Telegram credentials missing in ~/.kite_secrets", file=sys.stderr)
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": text},
            timeout=10,
        )
    except Exception as e:
        print(f"Telegram send failed: {e}", file=sys.stderr)


def _nearest_expiry(df, index_name, from_date: date):
    """Return the nearest expiry date for index_name on or after from_date."""
    cutoff = pd.Timestamp(from_date)
    mask   = (df["name"] == index_name) & (df["expiry"] >= cutoff)
    series = df.loc[mask, "expiry"]
    if series.empty:
        return None
    return series.min().date()


def _strikes_tracked(index_name: str) -> int:
    cfg   = STRIKE_CONFIG[index_name]
    count = cfg["count"]
    return (2 * count + 1) * 2   # CE + PE for each strike


def main():
    today    = date.today()
    weekday  = today.weekday()   # Mon=0 … Fri=4, Sat=5, Sun=6

    # Determine which future date to check for expiry
    # Friday (4) → check Monday (+3 calendar days)
    # Otherwise  → check tomorrow (+1 calendar day)
    if weekday == 4:
        check_date = today + timedelta(days=3)   # Monday
    else:
        check_date = today + timedelta(days=1)   # Tomorrow

    # Load instruments
    try:
        df = pd.read_csv(CSV_PATH)
        df["expiry"] = pd.to_datetime(df["expiry"], errors="coerce")
    except Exception as e:
        print(f"Could not load instruments CSV: {e}", file=sys.stderr)
        sys.exit(1)

    # Find indices expiring on check_date
    expiring = []
    for index_name in STRIKE_CONFIG:
        expiry = _nearest_expiry(df, index_name, today)
        if expiry == check_date:
            expiring.append((index_name, expiry))

    if not expiring:
        print(f"No expiry on {check_date} — no alert sent.")
        return

    # Build message
    day_label = check_date.strftime("%A %d %b %Y")
    if weekday == 4:
        header = f"📅 Expiry Alert — Monday {check_date.strftime('%d %b %Y')}"
    else:
        header = f"📅 Expiry Alert — Tomorrow {day_label}"

    lines = [header, ""]
    for index_name, expiry in expiring:
        strikes = _strikes_tracked(index_name)
        lines.append(f"  • {index_name:12} {expiry.strftime('%d %b %Y')}  ({strikes} strikes)")

    lines.append("")
    lines.append("Tick monitoring will be active for these indices.")

    message = "\n".join(lines)
    print(message)
    _send_telegram(message)


if __name__ == "__main__":
    main()
