"""
symbol_resolver.py
------------------
Resolves the 8 nearest ATM NIFTY strikes (4 CE + 4 PE) to subscribe on Fyers,
using the symbols already present in Zerodha's tracked_symbols table.

Cross-process strategy (no modification to tick_collector):
  Query tracked_symbols (PostgreSQL) for NIFTY CE/PE nearest weekly expiry.
  Fetch current NIFTY LTP from candles_5s.
  Sort by distance from ATM, pick FYERS_NUM_STRIKES // 2 of each type.

Fyers symbol format: "NSE:" + zerodha_symbol  (confirmed by Fyers v3 docs)
Display symbol     : "FY:"  + zerodha_symbol  (routing key in the UI)
"""

import asyncpg
import logging
import re
from datetime import date, datetime, timedelta

from config.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from backend.services.fyers_collector.config import FYERS_NUM_STRIKES

logger = logging.getLogger(__name__)

# Regex for weekly (M+DD) and monthly (MON3) Zerodha option symbols
_MONTH_MAP = {m: i for i, m in enumerate(
    ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"], 1
)}
_OPT_RE = re.compile(
    r"^(NIFTY|BANKNIFTY|FINNIFTY|MIDCPNIFTY|SENSEX)"
    r"(\d{2})"                    # YY
    r"([A-Z]{3}|\d{1,2}\d{2})"   # MON3 monthly  OR  M/MM + DD weekly
    r"(\d+)"                       # strike
    r"(CE|PE)$"
)


def parse_zerodha_symbol(symbol: str) -> dict | None:
    """
    Parse a Zerodha-format option symbol into metadata dict.
    Returns None if the symbol doesn't match the expected pattern.

    Examples:
      NIFTY2651224500CE  → year=2026, month=5,  day=12, strike=24500, type=CE
      NIFTY26MAY24500CE  → year=2026, month=5,  day=None (monthly), strike=24500, type=CE
    """
    m = _OPT_RE.match(symbol)
    if not m:
        return None
    index, yy, date_code, strike_s, opt_type = m.groups()
    year   = 2000 + int(yy)
    strike = float(strike_s)

    if date_code.isalpha():
        # Monthly expiry: MON3
        month = _MONTH_MAP.get(date_code.upper(), 1)
        # Approximate expiry date (last Thursday of month) — good enough for sorting
        expiry = date(year, month, 28)
    else:
        # Weekly expiry: M+DD or MM+DD (e.g. "512" → month=5, day=12; "1015" → month=10, day=15)
        if len(date_code) == 3:
            month, day = int(date_code[0]), int(date_code[1:])
        else:
            month, day = int(date_code[:2]), int(date_code[2:])
        expiry = date(year, month, day)

    return {
        "index":       index,
        "strike":      strike,
        "option_type": opt_type,
        "expiry_date": expiry,
    }


async def resolve_symbols(num_strikes: int = FYERS_NUM_STRIKES) -> list[dict]:
    """
    Connect to the PostgreSQL DB and resolve the nearest ATM NIFTY strikes.
    Returns a list of dicts:
        {
          "zerodha_symbol": "NIFTY2651224500CE",
          "fyers_symbol":   "NSE:NIFTY2651224500CE",
          "display_symbol": "FY:NIFTY2651224500CE",
          "strike":         24500.0,
          "option_type":    "CE",
          "expiry_date":    date(2026, 5, 12),
        }
    """
    pool = await asyncpg.create_pool(
        host=DB_HOST, port=int(DB_PORT),
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
        min_size=1, max_size=2,
    )
    try:
        async with pool.acquire() as conn:
            # ── Step 1: nearest NIFTY weekly expiry from tracked_symbols ────
            rows = await conn.fetch("""
                SELECT symbol, strike, option_type, expiry_date
                FROM tracked_symbols
                WHERE symbol LIKE 'NIFTY%'
                  AND option_type IN ('CE', 'PE')
                  AND expiry_date = (
                      SELECT MIN(expiry_date)
                      FROM tracked_symbols
                      WHERE symbol LIKE 'NIFTY%'
                        AND option_type IN ('CE', 'PE')
                        AND expiry_date >= CURRENT_DATE
                  )
                ORDER BY strike
            """)

            if not rows:
                raise RuntimeError(
                    "No NIFTY options found in tracked_symbols. "
                    "Is the Zerodha collector running and receiving ticks?"
                )

            # ── Step 2: current NIFTY LTP from candles_5s ───────────────────
            ltp_row = await conn.fetchrow(
                "SELECT close FROM candles_5s WHERE symbol = 'NIFTY' "
                "ORDER BY bucket DESC LIMIT 1"
            )

    finally:
        await pool.close()

    if ltp_row is None:
        raise RuntimeError(
            "Cannot determine NIFTY LTP from candles_5s. "
            "Make sure at least one NIFTY candle exists."
        )

    nifty_ltp = float(ltp_row["close"])
    atm        = round(nifty_ltp / 50) * 50

    logger.info("NIFTY LTP=%.2f  ATM=%d", nifty_ltp, atm)

    # ── Step 3: split into CE / PE lists, sort by ATM distance ─────────────
    ce_rows = sorted(
        [r for r in rows if r["option_type"] == "CE"],
        key=lambda r: abs(r["strike"] - atm)
    )
    pe_rows = sorted(
        [r for r in rows if r["option_type"] == "PE"],
        key=lambda r: abs(r["strike"] - atm)
    )

    n = num_strikes // 2
    selected = ce_rows[:n] + pe_rows[:n]

    # ── Step 4: build output ─────────────────────────────────────────────────
    result = []
    for r in selected:
        z_sym = r["symbol"]
        result.append({
            "zerodha_symbol": z_sym,
            "fyers_symbol":   f"NSE:{z_sym}",
            "display_symbol": f"FY:{z_sym}",
            "strike":         float(r["strike"]),
            "option_type":    r["option_type"],
            "expiry_date":    r["expiry_date"],
        })

    # Sort output: CE first, then PE, both sorted by strike asc
    result.sort(key=lambda x: (x["option_type"], x["strike"]))
    return result


def print_resolved(symbols: list[dict]) -> None:
    """Pretty-print the resolved symbol list for visual verification."""
    print("\n" + "=" * 60)
    print("Fyers subscription list (dry run):")
    print("=" * 60)
    for s in symbols:
        print(
            f"  Zerodha: {s['zerodha_symbol']:25s}  "
            f"Fyers: {s['fyers_symbol']:28s}  "
            f"Display: {s['display_symbol']:28s}  "
            f"Strike: {int(s['strike']):>6}  "
            f"Type: {s['option_type']}  "
            f"Expiry: {s['expiry_date']}"
        )
    print("=" * 60 + "\n")
