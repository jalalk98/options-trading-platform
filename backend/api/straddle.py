# backend/api/straddle.py
# Straddle popup endpoints:
#   GET  /straddle/quotes   — strike ladder (CE/PE/combined LTPs) around ATM
#   GET  /straddle/history  — today's combined-premium 5s series (sparkline)
#   POST /straddle/enter    — place BOTH legs (auto-chase) or a single leg
#   POST /straddle/sl       — move combined SL (straddle) / Kite SL orders (leg)
#   POST /straddle/exit     — flatten a position
#   GET  /straddle/status   — all of today's positions (popup re-hydration)
# Orchestration/monitoring lives in backend/services/straddle_monitor.py.

import asyncio
import datetime
import logging
import re
import time

from fastapi import APIRouter, Request
from pydantic import BaseModel
from typing import Optional

import backend.state as state
from backend.services import straddle_monitor
from backend.services.websocket_handler import kite1
from backend.api.strikes import get_atm_symbol
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN

logger = logging.getLogger(__name__)
router = APIRouter()

STRIKE_STEP = {"NIFTY": 50, "SENSEX": 100, "BANKNIFTY": 100, "MIDCPNIFTY": 25, "FINNIFTY": 50}

# Longest-prefix-first so NIFTY doesn't swallow BANKNIFTY etc.
_INDEX_ORDER = ["BANKNIFTY", "MIDCPNIFTY", "FINNIFTY", "SENSEX", "NIFTY"]
_OPT_TAIL_RE = re.compile(r"^(.{5})(\d+)(CE|PE)$")  # expiry is always 5 chars


def parse_option_symbol(sym: str) -> Optional[dict]:
    for idx in _INDEX_ORDER:
        if sym.startswith(idx):
            m = _OPT_TAIL_RE.match(sym[len(idx):])
            if not m:
                return None
            return {"index": idx, "expiry": m.group(1),
                    "strike": int(m.group(2)), "type": m.group(3)}
    return None


def _exchange_for(index: str) -> str:
    return "BFO" if index == "SENSEX" else "NFO"


def _round05(price: float) -> float:
    return round(round(price / 0.05) * 0.05, 2)


# ── LTP resolution: live tick cache first, DB latest-close fallback ──────────

_db_ltp_cache: dict = {"key": None, "data": {}, "ts": 0.0}


async def _resolve_ltps(pool, symbols: list) -> dict:
    out = {sym: state.latest_ltp.get(sym) for sym in symbols}
    missing = [s for s, v in out.items() if v is None]
    if not missing:
        return out
    # DB fallback (market closed / symbol not ticking) — cached 10s
    key = tuple(sorted(missing))
    now = time.monotonic()
    if _db_ltp_cache["key"] == key and now - _db_ltp_cache["ts"] < 10:
        out.update(_db_ltp_cache["data"])
        return out
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT c.symbol, c.close AS price
                FROM candles_5s c
                JOIN (
                    SELECT symbol, MAX(bucket) AS max_bucket
                    FROM candles_5s
                    WHERE symbol = ANY($1)
                      AND bucket >= EXTRACT(EPOCH FROM NOW() - INTERVAL '5 days')::BIGINT
                    GROUP BY symbol
                ) latest ON c.symbol = latest.symbol AND c.bucket = latest.max_bucket
            """, missing)
        found = {r["symbol"]: float(r["price"]) for r in rows}
        _db_ltp_cache.update(key=key, data=found, ts=now)
        out.update(found)
    except Exception as e:
        logger.error("[STRDL] DB LTP fallback failed: %s", e)
    return out


# ─────────────────────────────────────────────
# GET /api/straddle/quotes
# ─────────────────────────────────────────────
@router.get("/straddle/quotes")
async def straddle_quotes(request: Request, index: str = "NIFTY",
                          center: Optional[int] = None, rows: int = 7):
    index = index.upper()
    step = STRIKE_STEP.get(index)
    if not step:
        return {"status": "error", "message": f"unsupported index {index}"}
    rows = max(2, min(rows, 15))

    atm = await get_atm_symbol(request, index)
    parsed = parse_option_symbol(atm.get("symbol") or "")
    if not parsed:
        return {"status": "error", "message": f"ATM symbol unavailable for {index}"}
    atm_strike = parsed["strike"]
    prefix = f"{index}{parsed['expiry']}"

    c = center if center else atm_strike
    half = rows // 2
    strikes = [c + (i - half) * step for i in range(rows)]

    syms = []
    for k in strikes:
        syms += [f"{prefix}{k}CE", f"{prefix}{k}PE"]
    ltps = await _resolve_ltps(request.app.state.pool, syms)

    out_rows = []
    for k in strikes:
        ce_sym, pe_sym = f"{prefix}{k}CE", f"{prefix}{k}PE"
        ce, pe = ltps.get(ce_sym), ltps.get(pe_sym)
        out_rows.append({
            "strike": k, "ce_symbol": ce_sym, "pe_symbol": pe_sym,
            "ce": ce, "pe": pe,
            "combined": round(ce + pe, 2) if ce is not None and pe is not None else None,
        })
    return {"status": "ok", "index": index, "expiry": parsed["expiry"], "step": step,
            "atm_strike": atm_strike, "center": c, "rows": out_rows}


# ─────────────────────────────────────────────
# GET /api/straddle/history — combined 5s closes for today (sparkline)
# ─────────────────────────────────────────────
_hist_cache: dict = {}  # (ce, pe) → {"ts": monotonic, "data": [...]}


@router.get("/straddle/history")
async def straddle_history(request: Request, ce: str, pe: str):
    key = (ce, pe)
    now = time.monotonic()
    cached = _hist_cache.get(key)
    if cached and now - cached["ts"] < 20:
        return {"status": "ok", "points": cached["data"]}

    # Bucket convention: IST wall-clock treated as UTC epoch (matches candles_5s)
    ist = straddle_monitor._ist_now()
    start_bucket = int(datetime.datetime(
        ist.year, ist.month, ist.day, 9, 15,
        tzinfo=datetime.timezone.utc).timestamp())
    try:
        async with request.app.state.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT symbol, bucket, close FROM candles_5s
                WHERE symbol = ANY($1) AND bucket >= $2
                ORDER BY bucket
            """, [ce, pe], start_bucket)
    except Exception as e:
        logger.error("[STRDL] history query failed: %s", e)
        return {"status": "error", "message": str(e), "points": []}

    # Merge the two legs: carry each leg's last close forward, emit CE+PE
    # only once both legs have printed at least once.
    by_bucket: dict = {}
    for r in rows:
        by_bucket.setdefault(r["bucket"], {})[r["symbol"]] = float(r["close"])
    points, last_ce, last_pe = [], None, None
    for bucket in sorted(by_bucket):
        vals = by_bucket[bucket]
        last_ce = vals.get(ce, last_ce)
        last_pe = vals.get(pe, last_pe)
        if last_ce is not None and last_pe is not None:
            points.append([bucket, round(last_ce + last_pe, 2)])
    _hist_cache[key] = {"ts": now, "data": points}
    return {"status": "ok", "points": points}


# ─────────────────────────────────────────────
# POST /api/straddle/enter
# ─────────────────────────────────────────────
class EnterRequest(BaseModel):
    index:        str
    strike:       int
    expiry:       str            # 5-char expiry code from /straddle/quotes
    legs:         str            # "BOTH" | "CE" | "PE"
    qty:          int            # per leg
    buffer:       float = 0.85   # limit price = LTP + buffer
    chase_secs:   float = 4.0
    disaster_pct: float = 0.60   # per-leg disaster SL trigger = pct × fill price


@router.post("/straddle/enter")
async def straddle_enter(req: EnterRequest, request: Request):
    from backend.api.sl import place_limit_order, LimitOrderRequest

    index = req.index.upper()
    legs_arg = req.legs.upper()
    if legs_arg not in ("BOTH", "CE", "PE"):
        return {"status": "error", "message": f"invalid legs {req.legs}"}
    if index not in STRIKE_STEP:
        return {"status": "error", "message": f"unsupported index {index}"}
    if req.qty <= 0:
        return {"status": "error", "message": "qty must be > 0"}
    if not (0 < req.disaster_pct < 1):
        return {"status": "error", "message": "disaster_pct must be between 0 and 1"}

    leg_types = ["CE", "PE"] if legs_arg == "BOTH" else [legs_arg]
    prefix = f"{index}{req.expiry}"
    symbols = {lt: f"{prefix}{req.strike}{lt}" for lt in leg_types}
    exchange = _exchange_for(index)

    ltps = await _resolve_ltps(request.app.state.pool, list(symbols.values()))
    for lt, sym in symbols.items():
        if ltps.get(sym) is None:
            return {"status": "error", "message": f"no LTP available for {sym}"}

    ist = straddle_monitor._ist_now()
    pos_id = f"{index}{req.strike}-{legs_arg}-{ist.strftime('%H%M%S')}"
    if pos_id in state.straddle_positions:
        pos_id += f"-{int(time.monotonic() * 1000) % 1000}"

    pos = {
        "id": pos_id,
        "kind": "straddle" if legs_arg == "BOTH" else "leg",
        "index": index, "strike": req.strike, "expiry": req.expiry,
        "date": ist.strftime("%Y-%m-%d"), "created": ist.isoformat(),
        "status": "entering", "close_reason": None,
        "qty": req.qty, "buffer": req.buffer,
        "chase_secs": req.chase_secs, "disaster_pct": req.disaster_pct,
        "combined_entry": None, "combined_sl": None,
        "legs": {}, "logs": [],
    }
    for lt, sym in symbols.items():
        pos["legs"][lt] = {
            "symbol": sym, "qty": req.qty, "entry_orders": [],
            "filled_qty": 0, "avg_price": None,
            "sl_orders": [], "sl_level": None,
            "exit_orders": [], "closed": False, "chased": False,
        }
    state.straddle_positions[pos_id] = pos

    # Place all legs (limit @ LTP + buffer); placement errors are absorbed by
    # the auto-chase in finalize_entry, which markets any unfilled remainder.
    results = await asyncio.gather(*[
        place_limit_order(LimitOrderRequest(
            symbol=symbols[lt], price=_round05(ltps[symbols[lt]] + req.buffer),
            side="BUY", qty=req.qty, exchange=exchange, order_type="L"))
        for lt in leg_types
    ])
    any_placed = False
    for lt, res in zip(leg_types, results):
        order_ids = [oid for oid in (res.get("order_ids") or []) if oid]
        pos["legs"][lt]["entry_orders"] = order_ids
        if res.get("status") == "success":
            any_placed = True
            straddle_monitor._log(pos, f"{lt} entry: BUY {req.qty} @ {res.get('price')} → {order_ids}")
        else:
            straddle_monitor._log(pos, f"{lt} entry error: {res.get('message')} (placed: {order_ids})")
            any_placed = any_placed or bool(order_ids)

    if not any_placed:
        pos["status"], pos["close_reason"] = "failed", "entry_failed"
        straddle_monitor.save_positions()
        return {"status": "error", "message": "no orders placed — see logs", "id": pos_id}

    straddle_monitor.save_positions()
    asyncio.create_task(straddle_monitor.finalize_entry(pos_id))
    return {"status": "ok", "id": pos_id}


# ─────────────────────────────────────────────
# POST /api/straddle/sl
# ─────────────────────────────────────────────
class SlUpdateRequest(BaseModel):
    id: str
    sl: float


@router.post("/straddle/sl")
async def straddle_sl(req: SlUpdateRequest):
    pos = state.straddle_positions.get(req.id)
    if not pos:
        return {"status": "error", "message": f"unknown position {req.id}"}
    if pos["status"] not in ("open", "entering"):
        return {"status": "error", "message": f"position is {pos['status']}"}
    sl = _round05(req.sl)
    if sl <= 0:
        return {"status": "error", "message": "sl must be > 0"}

    if pos["kind"] == "straddle":
        pos["combined_sl"] = sl
        straddle_monitor._log(pos, f"combined SL moved to {sl}")
        straddle_monitor.save_positions()
        return {"status": "ok", "sl": sl, "enforced_by": "monitor"}

    # Single leg: move the real Kite SL orders
    leg = next(iter(pos["legs"].values()))
    if not leg.get("sl_orders"):
        return {"status": "error", "message": "SL order not placed yet — wait for entry to complete"}
    limit = _round05(sl - straddle_monitor._trigger_buffer(leg["symbol"]))
    errors = []
    for o in leg.get("sl_orders", []):
        try:
            res = await kite1.hard_code_regular_modify_order(
                order_id=o["order_id"], price=limit, trig_price=sl,
                access_token=KITE_ACCESS_TOKEN, api_key=KITE_API_KEY)
            if not (res and res.get("status") == "success"):
                errors.append(f"{o['order_id']}: {(res or {}).get('message', 'modify failed')}")
        except Exception as e:
            errors.append(f"{o['order_id']}: {e}")
    if errors:
        straddle_monitor._log(pos, f"SL modify errors: {'; '.join(errors)}")
        return {"status": "error", "message": "; ".join(errors)}
    leg["sl_level"] = sl
    straddle_monitor._log(pos, f"leg SL moved to {sl}")
    straddle_monitor.save_positions()
    return {"status": "ok", "sl": sl, "enforced_by": "exchange"}


# ─────────────────────────────────────────────
# POST /api/straddle/exit
# ─────────────────────────────────────────────
class ExitRequest(BaseModel):
    id: str


@router.post("/straddle/exit")
async def straddle_exit(req: ExitRequest):
    return await straddle_monitor.execute_exit(req.id, "manual")


# ─────────────────────────────────────────────
# GET /api/straddle/status
# ─────────────────────────────────────────────
@router.get("/straddle/status")
async def straddle_status():
    positions = []
    for pid, pos in state.straddle_positions.items():
        p = dict(pos)
        p["logs"] = pos.get("logs", [])[-8:]
        p["monitor_active"] = straddle_monitor.is_monitor_active(pid)
        # Live leg LTPs for P&L (popup already polls quotes, but position
        # strikes may sit outside the current ladder window)
        legs = {}
        for lt, leg in pos["legs"].items():
            legs[lt] = {**leg, "ltp": state.latest_ltp.get(leg["symbol"])}
        p["legs"] = legs
        positions.append(p)
    positions.sort(key=lambda p: p.get("created", ""), reverse=True)
    return {"status": "ok", "positions": positions}
