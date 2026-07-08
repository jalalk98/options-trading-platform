# backend/api/journal.py
# Trade journal endpoints:
#   POST /api/journal/sync  — fetch today's trades from Zerodha, insert new ones
#   GET  /api/journal       — list entries by date / symbol
#   PATCH /api/journal/{id} — update notes / setup / outcome
#   DELETE /api/journal/{id}

import re
import logging
from datetime import date, datetime, timezone, timedelta
from typing import Optional

import httpx
from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel

from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN

logger = logging.getLogger(__name__)
router = APIRouter()

# ── IST offset ────────────────────────────────────────────────────────────────
_IST = timedelta(hours=5, minutes=30)

def _now_ist() -> datetime:
    return datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=5, minutes=30)))

# ── Symbol parsing (reused from strikes.py conventions) ───────────────────────
_MONTH_MAP = {m: i for i, m in enumerate(
    ['JAN','FEB','MAR','APR','MAY','JUN','JUL','AUG','SEP','OCT','NOV','DEC'], 1
)}
_OPT_RE = re.compile(
    r'^(NIFTY|BANKNIFTY|FINNIFTY|MIDCPNIFTY|SENSEX)'
    r'(\d{2})([A-Z]{3}|\d{1}\d{2})'  # YY + (MON3 monthly | M+DD weekly)
    r'(\d+)'                           # strike
    r'(CE|PE)$'
)

# Maps underlying name → symbol used in gap_ticks
_UNDERLYING_TICK_SYMBOL = {
    "NIFTY":      "NIFTY",
    "BANKNIFTY":  "BANKNIFTY",
    "FINNIFTY":   "FINNIFTY",
    "MIDCPNIFTY": "MIDCPNIFTY",
    "SENSEX":     "SENSEX",
}

def _parse_symbol(sym: str) -> dict:
    """Return underlying/strike/option_type/expiry_date from an NSE option symbol.
    Returns None values for unrecognised symbols."""
    m = _OPT_RE.match(sym)
    if not m:
        return {"underlying": sym, "strike": None, "option_type": None, "expiry_date": None}
    underlying, yy, date_code, strike_s, opt_type = m.groups()
    year = 2000 + int(yy)
    strike = float(strike_s)
    if date_code.isalpha():                          # monthly e.g. JUN
        month = _MONTH_MAP.get(date_code, 1)
        # Last Thursday of month — approximate as 28th for storage
        expiry = date(year, month, 28)
    else:                                            # weekly e.g. 612 = Jun 12
        month, day = int(date_code[0]), int(date_code[1:])
        expiry = date(year, month, day)
    return {
        "underlying":  underlying,
        "strike":      strike,
        "option_type": opt_type,
        "expiry_date": expiry,
    }


async def _fetch_underlying_price(pool, underlying: str, at: datetime) -> Optional[float]:
    """Nearest gap_ticks price for the underlying index at the given timestamp.
    gap_ticks stores IST timestamps as timestamp without time zone, so we convert
    the trade time (UTC-aware) to a naive IST datetime before querying."""
    tick_sym = _UNDERLYING_TICK_SYMBOL.get(underlying)
    if not tick_sym:
        return None
    # Convert UTC-aware timestamp → naive IST (gap_ticks has no tz info)
    ist_naive = at.astimezone(timezone(_IST)).replace(tzinfo=None)
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT curr_price
                FROM gap_ticks
                WHERE symbol = $1
                  AND timestamp BETWEEN $2 - INTERVAL '2 minutes' AND $2 + INTERVAL '2 minutes'
                ORDER BY ABS(EXTRACT(EPOCH FROM (timestamp - $2)))
                LIMIT 1
                """,
                tick_sym, ist_naive,
            )
        return float(row["curr_price"]) if row else None
    except Exception as e:
        logger.warning("underlying price lookup failed for %s at %s: %s", underlying, at, e)
        return None


# ── POST /api/journal/sync ────────────────────────────────────────────────────
@router.post("/journal/sync")
async def sync_journal(request: Request):
    """Fetch today's executed trades from Zerodha and insert any not yet stored."""
    pool = request.app.state.pool
    headers = {
        "X-Kite-Version":  "3",
        "User-Agent":      "Kiteconnect-python/5.0.1",
        "Authorization":   f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}",
    }
    try:
        async with httpx.AsyncClient(verify=False) as client:
            r = await client.get("https://api.kite.trade/trades", headers=headers, timeout=10)
            r.raise_for_status()
            data = r.json()
    except Exception as e:
        logger.error("Zerodha /trades fetch failed: %s", e)
        raise HTTPException(status_code=502, detail=f"Zerodha API error: {e}")

    raw_trades = data.get("data") or []
    today      = _now_ist().date()
    inserted   = 0

    # ── Group fills by order_id ───────────────────────────────────────────────
    # Zerodha returns one row per fill — a single order can have multiple fills
    # (e.g. 500 qty filled as 260 + 20 + 40). We consolidate all fills for an
    # order into one journal entry: total qty, weighted-average price, earliest
    # fill timestamp.
    from collections import defaultdict
    orders: dict = defaultdict(list)
    for t in raw_trades:
        oid = str(t.get("order_id", ""))
        if oid and t.get("exchange", "") in ("NFO", "BFO"):
            orders[oid].append(t)

    async with pool.acquire() as conn:
        # Existing order_ids — used for dedup across all dates (order_id is globally unique)
        existing = {
            row["order_id"]
            for row in await conn.fetch("SELECT order_id FROM trade_journal")
        }

        for order_id, fills in orders.items():
            if order_id in existing:
                continue

            # Use first fill for metadata (symbol, direction, exchange are same across fills)
            first  = fills[0]
            sym    = first.get("tradingsymbol", "")
            parsed = _parse_symbol(sym)
            direction = first.get("transaction_type", "BUY")

            # Aggregate across all fills: total qty, weighted-average price
            total_qty   = sum(int(f.get("quantity") or 0) for f in fills)
            total_value = sum(
                int(f.get("quantity") or 0) * float(f.get("average_price") or 0)
                for f in fills
            )
            avg_price = round(total_value / total_qty, 2) if total_qty else 0.0

            # Use the earliest fill timestamp as the order time
            def _parse_ts(f):
                raw = f.get("exchange_timestamp") or f.get("fill_timestamp") or f.get("order_timestamp")
                try:
                    return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(
                        tzinfo=timezone(_IST)
                    )
                except Exception:
                    return _now_ist()

            trade_time = min((_parse_ts(f) for f in fills), default=_now_ist())
            trade_date = trade_time.date()   # use actual trade date, not sync date

            underlying_price = await _fetch_underlying_price(
                pool, parsed["underlying"], trade_time
            )

            await conn.execute(
                """
                INSERT INTO trade_journal (
                    trade_date, symbol, underlying, strike, option_type, expiry_date,
                    direction, quantity, entry_price, entry_time,
                    underlying_price_at_entry, order_id, outcome
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,'open')
                ON CONFLICT (order_id) DO NOTHING
                """,
                trade_date, sym, parsed["underlying"], parsed["strike"],
                parsed["option_type"], parsed["expiry_date"],
                direction, total_qty, avg_price, trade_time,
                underlying_price, order_id,
            )
            existing.add(order_id)
            inserted += 1

    return {"synced": inserted, "total_orders_from_zerodha": len(orders)}


# ── GET /api/journal ──────────────────────────────────────────────────────────
@router.get("/journal")
async def get_journal(
    request: Request,
    date:    Optional[str] = None,
    symbol:  Optional[str] = None,
):
    """Return journal entries. Filter by date (YYYY-MM-DD) and/or symbol."""
    pool = request.app.state.pool
    from datetime import date as _date
    query_date_str = date or _now_ist().strftime("%Y-%m-%d")
    query_date_obj = _date.fromisoformat(query_date_str)

    conditions = ["trade_date = $1"]
    params: list = [query_date_obj]

    if symbol:
        conditions.append(f"symbol = ${len(params)+1}")
        params.append(symbol)

    where = " AND ".join(conditions)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT id, trade_date, symbol, underlying, strike, option_type, expiry_date,
                   direction, quantity, entry_price, exit_price, entry_time, exit_time,
                   underlying_price_at_entry, underlying_price_at_exit,
                   pnl, setup, notes, outcome, order_id, created_at
            FROM trade_journal
            WHERE {where}
            ORDER BY entry_time ASC NULLS LAST
            """,
            *params,
        )

    result = []
    for r in rows:
        result.append({
            "id":                         r["id"],
            "trade_date":                 str(r["trade_date"]),
            "symbol":                     r["symbol"],
            "underlying":                 r["underlying"],
            "strike":                     float(r["strike"]) if r["strike"] else None,
            "option_type":                r["option_type"],
            "expiry_date":                str(r["expiry_date"]) if r["expiry_date"] else None,
            "direction":                  r["direction"],
            "quantity":                   r["quantity"],
            "entry_price":                float(r["entry_price"]) if r["entry_price"] else None,
            "exit_price":                 float(r["exit_price"]) if r["exit_price"] else None,
            "entry_time":                 r["entry_time"].isoformat() if r["entry_time"] else None,
            "exit_time":                  r["exit_time"].isoformat() if r["exit_time"] else None,
            "underlying_price_at_entry":  float(r["underlying_price_at_entry"]) if r["underlying_price_at_entry"] else None,
            "underlying_price_at_exit":   float(r["underlying_price_at_exit"]) if r["underlying_price_at_exit"] else None,
            "pnl":                        float(r["pnl"]) if r["pnl"] else None,
            "setup":                      r["setup"],
            "notes":                      r["notes"],
            "outcome":                    r["outcome"],
            "order_id":                   r["order_id"],
            "trade_no":                   None,  # filled in below
        })

    # ── Compute trade_no + P&L: global sequential, ordered by position-open time
    # result is already sorted by entry_time ASC.
    # Each time a symbol's net position goes from 0 → non-zero, a new global
    # trade number is issued. All legs share that number. P&L = SELL proceeds
    # minus BUY cost, assigned to the closing leg when qty returns to 0.
    _global_no: int = 0
    _sym_qty: dict  = {}   # symbol → current net qty
    _sym_tno: dict  = {}   # symbol → trade_no of the open position
    _sym_pnl: dict  = {}   # symbol → running P&L accumulator

    for trade in result:
        sym   = trade["symbol"]
        qty   = trade["quantity"] or 0
        price = trade["entry_price"] or 0.0
        prev  = _sym_qty.get(sym, 0)

        if prev == 0:
            _global_no    += 1
            _sym_tno[sym]  = _global_no
            _sym_pnl[sym]  = 0.0

        trade["trade_no"] = _sym_tno[sym]
        trade["pnl"]      = None  # only set on closing leg

        new_qty = prev + (qty if trade["direction"] == "BUY" else -qty)
        _sym_qty[sym]  = new_qty
        _sym_pnl[sym] += price * qty * (1 if trade["direction"] == "SELL" else -1)

        if new_qty == 0:
            trade["pnl"] = round(_sym_pnl[sym], 2)
            del _sym_tno[sym]
            del _sym_pnl[sym]

    # Summary: count complete trades (those with a computed P&L on closing leg)
    closed_trades = [e for e in result if e["pnl"] is not None]
    total_pnl     = sum(e["pnl"] for e in closed_trades)
    wins          = sum(1 for e in closed_trades if e["pnl"] > 0)
    losses        = sum(1 for e in closed_trades if e["pnl"] < 0)

    return {
        "date":    query_date_str,
        "trades":  result,
        "summary": {
            "total":  len(closed_trades),
            "wins":   wins,
            "losses": losses,
            "pnl":    round(total_pnl, 2),
        },
    }


# ── PATCH /api/journal/{id} ───────────────────────────────────────────────────
class UpdateJournalRequest(BaseModel):
    setup:   Optional[str] = None
    notes:   Optional[str] = None
    outcome: Optional[str] = None  # win / loss / be / open

@router.patch("/journal/{entry_id}")
async def update_journal(entry_id: int, req: UpdateJournalRequest, request: Request):
    pool = request.app.state.pool
    fields, params = [], [entry_id]

    if req.setup is not None:
        params.append(req.setup)
        fields.append(f"setup = ${len(params)}")
    if req.notes is not None:
        params.append(req.notes)
        fields.append(f"notes = ${len(params)}")
    if req.outcome is not None:
        if req.outcome not in ("win", "loss", "be", "open"):
            raise HTTPException(status_code=400, detail="outcome must be win/loss/be/open")
        params.append(req.outcome)
        fields.append(f"outcome = ${len(params)}")

    if not fields:
        raise HTTPException(status_code=400, detail="Nothing to update")

    async with pool.acquire() as conn:
        result = await conn.execute(
            f"UPDATE trade_journal SET {', '.join(fields)} WHERE id = $1",
            *params,
        )
    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail="Entry not found")
    return {"updated": entry_id}


# ── DELETE /api/journal/{id} ──────────────────────────────────────────────────
@router.delete("/journal/{entry_id}")
async def delete_journal(entry_id: int, request: Request):
    pool = request.app.state.pool
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM trade_journal WHERE id = $1", entry_id
        )
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="Entry not found")
    return {"deleted": entry_id}
