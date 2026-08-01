# backend/services/straddle_monitor.py
# Straddle popup orchestration (same task pattern as reverse_snipper):
#   finalize_entry()  — polls entry-order fills, auto-chases any unfilled leg to
#                       MARKET after chase_secs, places per-leg disaster SL
#                       orders (trigger at disaster_pct of fill), then starts
#                       the combined-premium monitor for straddle positions.
#   _monitor_loop()   — watches CE+PE via state.latest_ltp; when the combined
#                       premium drops to combined_sl, exits both legs.
#   execute_exit()    — cancels resting SL orders, then flattens this
#                       position's qty (capped by live net position) in
#                       freeze-qty MARKET chunks.
#
# Position record (state.straddle_positions[pos_id]):
# {
#   "id": str, "kind": "straddle"|"leg", "index": "NIFTY", "strike": 24450,
#   "expiry": "26710", "date": "YYYY-MM-DD", "created": iso,
#   "status": "entering"|"open"|"closing"|"closed"|"failed",
#   "close_reason": None|"manual"|"combined_sl"|"entry_failed",
#   "qty": int (per leg), "buffer": float,
#   "chase_secs": float, "disaster_pct": float,
#   "combined_entry": float|None, "combined_sl": float|None,
#   "combined_ltp": float|None (memory-only, refreshed by monitor),
#   "legs": { "CE"|"PE": {
#       "symbol": str, "qty": int, "entry_orders": [order_id,...],
#       "filled_qty": int, "avg_price": float|None,
#       "sl_orders": [{"order_id": str, "qty": int}], "sl_level": float|None,
#       "exit_orders": [order_id,...], "closed": bool, "chased": bool } },
#   "logs": [str],
# }

import asyncio
import datetime
import json
import logging
import time
from pathlib import Path
from typing import Optional

import backend.state as state
from backend.services.websocket_handler import kite1
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN

logger = logging.getLogger(__name__)

# pos_id → asyncio.Task (combined-premium monitor loops)
_active_monitors: dict = {}

_STATE_FILE = Path(__file__).parents[2] / "data" / "straddle_positions.json"

# Kite order statuses that mean "still working" (cancellable)
_OPEN_STATUSES = {
    "OPEN", "TRIGGER PENDING", "OPEN PENDING", "VALIDATION PENDING",
    "PUT ORDER REQ RECEIVED", "MODIFY PENDING", "AMO REQ RECEIVED",
}


def _ist_now() -> datetime.datetime:
    IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    return datetime.datetime.now(IST)


def _is_market_open() -> bool:
    now = _ist_now()
    if now.weekday() >= 5:
        return False
    hhmm = now.hour * 100 + now.minute
    return 915 <= hhmm < 1530


def _exchange(symbol: str) -> str:
    return "BFO" if symbol.startswith("SENSEX") else "NFO"


def _trigger_buffer(symbol: str) -> float:
    return 1.75 if symbol.startswith("SENSEX") else 0.85


def _round05(price: float) -> float:
    return round(round(price / 0.05) * 0.05, 2)


def _log(pos: dict, msg: str) -> None:
    line = f"{_ist_now().strftime('%H:%M:%S')} {msg}"
    pos.setdefault("logs", []).append(line)
    logger.info("[STRDL] %s %s", pos.get("id"), msg)


# ── Persistence ───────────────────────────────────────────────────────────────

def save_positions() -> None:
    try:
        # combined_ltp is a live memory-only field — don't persist stale prices
        snapshot = {
            pid: {k: v for k, v in pos.items() if k != "combined_ltp"}
            for pid, pos in state.straddle_positions.items()
        }
        _STATE_FILE.write_text(json.dumps(snapshot, indent=1))
    except Exception as e:
        logger.error("[STRDL] state save failed: %s", e)


def load_positions() -> None:
    """Called once at chart_server startup. Only today's positions are
    restored — DAY-validity SL orders from earlier days are gone and the
    monitor must not act on stale entries."""
    try:
        data = json.loads(_STATE_FILE.read_text())
    except FileNotFoundError:
        return
    except Exception as e:
        logger.error("[STRDL] state load failed: %s", e)
        return
    today = _ist_now().strftime("%Y-%m-%d")
    kept = {pid: p for pid, p in data.items() if p.get("date") == today}
    state.straddle_positions.update(kept)
    for pid, pos in kept.items():
        if pos.get("kind") == "straddle" and pos.get("status") == "open":
            start_monitor(pid)
        elif pos.get("status") == "entering":
            # Entry task died with the old process — resume finalization
            asyncio.get_event_loop().create_task(finalize_entry(pid))
    if kept:
        logger.info("[STRDL] restored %d position(s) from disk", len(kept))


# ── Order helpers ─────────────────────────────────────────────────────────────

async def _order_info(order_id: str) -> dict:
    try:
        return await kite1.get_order_status(order_id, KITE_API_KEY, KITE_ACCESS_TOKEN) or {}
    except Exception as e:
        logger.error("[STRDL] order status fetch failed %s: %s", order_id, e)
        return {}


async def _leg_fill_state(leg: dict) -> tuple:
    """Returns (filled_qty, open_order_ids, avg_price) across all entry orders."""
    filled, notional, open_ids = 0, 0.0, []
    for oid in leg.get("entry_orders", []):
        info = await _order_info(oid)
        fq = int(info.get("filled_quantity") or 0)
        ap = float(info.get("average_price") or 0)
        filled += fq
        if fq and ap:
            notional += fq * ap
        if info.get("status", "") in _OPEN_STATUSES:
            open_ids.append(oid)
    avg = round(notional / filled, 2) if filled else None
    return filled, open_ids, avg


async def _cancel_order(order_id: str, pos: dict, reason: str) -> None:
    try:
        await kite1.hard_code_regular_cancel_order(order_id, KITE_ACCESS_TOKEN, KITE_API_KEY)
        _log(pos, f"cancelled {order_id} ({reason})")
    except Exception as e:
        _log(pos, f"cancel {order_id} failed ({reason}): {e}")


# ── Entry finalization ────────────────────────────────────────────────────────

async def finalize_entry(pos_id: str) -> None:
    pos = state.straddle_positions.get(pos_id)
    if not pos:
        return
    legs = pos["legs"]
    try:
        # Phase 1: poll fills until all legs complete or chase deadline
        deadline = time.monotonic() + float(pos.get("chase_secs", 4.0))
        while time.monotonic() < deadline:
            all_filled = True
            for leg in legs.values():
                filled, _, avg = await _leg_fill_state(leg)
                leg["filled_qty"], leg["avg_price"] = filled, avg
                if filled < leg["qty"]:
                    all_filled = False
            if all_filled:
                break
            await asyncio.sleep(0.7)

        # Phase 2: auto-chase — cancel unfilled entry orders, MARKET the remainder
        from backend.api.sl import _place_market_order_chunked, _ChunkedOrderError
        for lt, leg in legs.items():
            filled, open_ids, avg = await _leg_fill_state(leg)
            leg["filled_qty"], leg["avg_price"] = filled, avg
            if filled >= leg["qty"]:
                continue
            for oid in open_ids:
                await _cancel_order(oid, pos, "auto-chase")
            # Cancel can race a fill — re-check before chasing
            filled, _, avg = await _leg_fill_state(leg)
            leg["filled_qty"], leg["avg_price"] = filled, avg
            remainder = leg["qty"] - filled
            if remainder <= 0:
                continue
            _log(pos, f"auto-chase {lt}: {remainder} unfilled → MARKET")
            leg["chased"] = True
            try:
                placed = await _place_market_order_chunked(
                    _exchange(leg["symbol"]), leg["symbol"], "BUY", remainder)
                leg["entry_orders"].extend(p["order_id"] for p in placed)
            except _ChunkedOrderError as e:
                _log(pos, f"auto-chase {lt} MARKET failed: {e}")
        save_positions()

        # Phase 3: wait briefly for chase fills, then take final fill state
        for _ in range(8):
            all_filled = True
            for leg in legs.values():
                filled, _, avg = await _leg_fill_state(leg)
                leg["filled_qty"], leg["avg_price"] = filled, avg
                if filled < leg["qty"]:
                    all_filled = False
            if all_filled:
                break
            await asyncio.sleep(0.7)

        if all(leg["filled_qty"] <= 0 for leg in legs.values()):
            pos["status"], pos["close_reason"] = "failed", "entry_failed"
            _log(pos, "entry failed — nothing filled")
            save_positions()
            return

        # Phase 4: disaster SL per filled leg (SELL SL at disaster_pct of fill)
        from backend.api.sl import place_sl_exact, PlaceSlExactReq, _freeze_qty
        disaster_pct = float(pos.get("disaster_pct", 0.60))
        for lt, leg in legs.items():
            if leg["filled_qty"] <= 0 or not leg["avg_price"]:
                continue
            trigger = _round05(leg["avg_price"] * disaster_pct)
            limit = _round05(trigger - _trigger_buffer(leg["symbol"]))
            freeze = _freeze_qty(leg["symbol"])
            remaining = leg["filled_qty"]
            while remaining > 0:
                chunk = min(remaining, freeze)
                res = await place_sl_exact(PlaceSlExactReq(
                    symbol=leg["symbol"], side="SELL", qty=chunk,
                    trigger=trigger, limit=limit, exchange=_exchange(leg["symbol"])))
                if res.get("status") == "success" and res.get("order_id"):
                    leg["sl_orders"].append({"order_id": res["order_id"], "qty": chunk})
                else:
                    _log(pos, f"disaster SL {lt} failed: {res.get('message')}")
                    break
                remaining -= chunk
            leg["sl_level"] = trigger
            _log(pos, f"{lt} filled {leg['filled_qty']} @ {leg['avg_price']} · disaster SL {trigger}")

        # Phase 5: open the position; straddles get the combined-premium monitor
        if pos["kind"] == "straddle":
            avgs = [leg["avg_price"] for leg in legs.values() if leg["avg_price"]]
            if len(avgs) == 2:
                pos["combined_entry"] = round(sum(avgs), 2)
                if pos.get("combined_sl") is None:
                    pos["combined_sl"] = _round05(pos["combined_entry"] * disaster_pct)
        pos["status"] = "open"
        save_positions()
        if pos["kind"] == "straddle":
            start_monitor(pos_id)
    except Exception as e:
        logger.error("[STRDL] finalize_entry %s crashed: %s", pos_id, e, exc_info=True)
        _log(pos, f"entry finalization error: {e}")
        pos["status"] = "open" if any(l.get("filled_qty") for l in legs.values()) else "failed"
        save_positions()


# ── Combined-premium monitor ──────────────────────────────────────────────────

async def _monitor_loop(pos_id: str) -> None:
    logger.info("[STRDL] monitor start %s", pos_id)
    try:
        while True:
            pos = state.straddle_positions.get(pos_id)
            if not pos or pos["status"] != "open":
                break
            if not _is_market_open():
                _log(pos, "market closed — monitor stopped (disaster SLs remain)")
                break
            ce = pos["legs"].get("CE", {})
            pe = pos["legs"].get("PE", {})
            ltp_ce = state.latest_ltp.get(ce.get("symbol"))
            ltp_pe = state.latest_ltp.get(pe.get("symbol"))
            if ltp_ce is not None and ltp_pe is not None:
                combined = round(ltp_ce + ltp_pe, 2)
                pos["combined_ltp"] = combined
                sl = pos.get("combined_sl")
                if sl and combined <= sl:
                    _log(pos, f"combined SL hit: {combined} <= {sl} — exiting both legs")
                    await execute_exit(pos_id, "combined_sl")
                    break
            await asyncio.sleep(0.25)
    except Exception as e:
        logger.error("[STRDL] monitor %s crashed: %s", pos_id, e, exc_info=True)
    finally:
        _active_monitors.pop(pos_id, None)
        logger.info("[STRDL] monitor end %s", pos_id)


def start_monitor(pos_id: str) -> bool:
    if pos_id in _active_monitors:
        return False
    _active_monitors[pos_id] = asyncio.get_event_loop().create_task(_monitor_loop(pos_id))
    return True


def stop_monitor(pos_id: str) -> bool:
    task = _active_monitors.pop(pos_id, None)
    if task:
        task.cancel()
        return True
    return False


def is_monitor_active(pos_id: str) -> bool:
    return pos_id in _active_monitors


# ── Exit ──────────────────────────────────────────────────────────────────────

async def execute_exit(pos_id: str, reason: str) -> dict:
    pos = state.straddle_positions.get(pos_id)
    if not pos:
        return {"status": "error", "message": f"unknown position {pos_id}"}
    if pos["status"] in ("closing", "closed"):
        return {"status": "ok", "message": f"already {pos['status']}"}
    pos["status"] = "closing"
    save_positions()
    if reason != "combined_sl":  # SL path is already inside the monitor loop
        stop_monitor(pos_id)

    from backend.api.sl import _place_market_order_chunked, _ChunkedOrderError, _fetch_net_position
    for lt, leg in pos["legs"].items():
        if leg.get("closed"):
            continue
        symbol, exch = leg["symbol"], _exchange(leg["symbol"])
        # 1. Cancel resting disaster SLs (a "filled" response means the SL
        #    already exited that qty — the net-position cap below absorbs it)
        for o in leg.get("sl_orders", []):
            await _cancel_order(o["order_id"], pos, f"exit-{reason}")
        # Prune our order ids from the sl_state cache so close-all never acts
        # on orders we just cancelled
        cached = state.sl_state.get(symbol)
        if cached and cached.get("sl_orders"):
            ours = {o["order_id"] for o in leg.get("sl_orders", [])}
            cached["sl_orders"] = [o for o in cached["sl_orders"] if o["order_id"] not in ours]
        # 2. Flatten our qty, capped by the live net position (never oversell
        #    if part already exited via SL, never touch other strategies' qty
        #    beyond what we own)
        exit_qty = int(leg.get("filled_qty") or 0)
        try:
            net = await _fetch_net_position(symbol, exch)
            exit_qty = min(exit_qty, max(net, 0))
        except Exception as e:
            _log(pos, f"net-position fetch failed for {symbol} (using own qty): {e}")
        if exit_qty > 0:
            try:
                placed = await _place_market_order_chunked(exch, symbol, "SELL", exit_qty)
                leg["exit_orders"] = [p["order_id"] for p in placed]
                _log(pos, f"{lt} exit: SELL {exit_qty} @ MARKET ({len(placed)} order(s))")
            except _ChunkedOrderError as e:
                leg["exit_orders"] = [p["order_id"] for p in e.placed]
                _log(pos, f"{lt} exit partially failed: {e}")
        else:
            _log(pos, f"{lt} exit: nothing to flatten")
        leg["closed"] = True

    pos["status"] = "closed"
    pos["close_reason"] = reason
    pos["closed_at"] = _ist_now().isoformat()
    save_positions()
    return {"status": "ok"}
