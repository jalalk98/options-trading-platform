# backend/api/sl.py
# Stop-loss management endpoints.

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional, List
from backend.state import sl_state, app_config
from backend.services.websocket_handler import kite1
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN
import asyncio
import logging
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)
router = APIRouter()

def _round(price: float, tick: float = 0.05) -> float:
    return round(round(price / tick) * tick, 2)


# ─────────────────────────────────────────────
# GET  /api/sl/{symbol}
# Returns current SL state for the symbol.
# ─────────────────────────────────────────────
@router.get("/sl/{symbol}")
async def get_sl(symbol: str):
    state = sl_state.get(symbol)
    if not state:
        return {"symbol": symbol, "state": "none"}
    return {"symbol": symbol, **state}


# ─────────────────────────────────────────────
# POST /api/sl/set
# Called when user drags the SL line.
# Only updates price; does not place an order.
# ─────────────────────────────────────────────
class SetSLRequest(BaseModel):
    symbol:          str
    price:           Optional[float]     = None   # if None, keep existing price (state-only update)
    trigger_buffer:  Optional[float]     = None   # if None, keep existing or default 0.20
    # Fields set by tick_collector after placing SL on Kite
    order_id:        Optional[str]       = None   # single order ID to append to list
    side:            Optional[str]       = None
    qty:             Optional[int]       = None
    exchange:        Optional[str]       = None
    state:           Optional[str]       = None

@router.post("/sl/set")
async def set_sl(req: SetSLRequest):
    existing   = sl_state.get(req.symbol, {})
    new_buffer = req.trigger_buffer if req.trigger_buffer is not None else existing.get("trigger_buffer", 0.20)

    # Clearing state — reset everything
    if req.state == "none":
        sl_state[req.symbol] = {
            "price":          None,
            "trigger_buffer": new_buffer,
            "order_id":       None,
            "order_ids":      [],
            "sl_orders":      [],
            "side":           None,
            "qty":            None,
            "exchange":       None,
            "state":          "none",
        }
        return {"status": "ok", "symbol": req.symbol, "price": None}

    new_price = _round(req.price) if req.price is not None else existing.get("price")

    # Build accumulated order_ids list
    existing_ids = existing.get("order_ids") or ([existing["order_id"]] if existing.get("order_id") else [])
    if req.order_id and req.order_id not in existing_ids:
        new_ids = existing_ids + [req.order_id]
    else:
        new_ids = existing_ids

    # Accumulate qty across sliced chunks
    if req.order_id and req.qty is not None:
        new_qty = (existing.get("qty") or 0) + req.qty
    else:
        new_qty = req.qty if req.qty is not None else existing.get("qty")

    # Build sl_orders list: {order_id, qty} per placed SL order
    existing_sl_orders = existing.get("sl_orders", [])
    existing_sl_ids    = {o["order_id"] for o in existing_sl_orders}
    if req.order_id and req.qty is not None and req.order_id not in existing_sl_ids:
        new_sl_orders = existing_sl_orders + [{"order_id": req.order_id, "qty": req.qty}]
    else:
        new_sl_orders = existing_sl_orders

    sl_state[req.symbol] = {
        "price":          new_price,
        "trigger_buffer": new_buffer,
        "order_id":       new_ids[0] if new_ids else None,   # backward compat
        "order_ids":      new_ids,
        "sl_orders":      new_sl_orders,
        "side":           req.side     if req.side     is not None else existing.get("side"),
        "qty":            new_qty,
        "exchange":       req.exchange if req.exchange is not None else existing.get("exchange"),
        "state":          req.state    if req.state    is not None else existing.get("state", "pending"),
    }

    # If dragging (state already "placed" and price changed), modify ALL orders on Kite
    modified_count = 0
    trigger_price  = None
    if existing.get("state") == "placed" and new_ids and req.price is not None and not req.order_id:
        side          = existing.get("side")
        trigger_price = _round(new_price + new_buffer) if side == "SELL" else _round(new_price - new_buffer)

        async def _mod(oid):
            try:
                r = await kite1.hard_code_regular_modify_order(
                    order_id=oid, price=new_price, trig_price=trigger_price,
                    access_token=KITE_ACCESS_TOKEN, api_key=KITE_API_KEY
                )
                logger.info(f"SL order {oid} modified to {new_price} (trigger {trigger_price}) for {req.symbol}: {r}")
                return True
            except Exception as e:
                logger.error(f"Failed to modify SL order {oid}: {e}")
                return False

        for i in range(0, len(new_ids), 9):
            batch         = new_ids[i:i + 9]
            batch_results = await asyncio.gather(*[_mod(oid) for oid in batch])
            modified_count += sum(batch_results)
            if i + 9 < len(new_ids):
                await asyncio.sleep(1.0)

        sl_state[req.symbol]["state"] = "placed"

    return {"status": "ok", "symbol": req.symbol, "price": new_price, "trigger_price": trigger_price, "modified_count": modified_count}


# ─────────────────────────────────────────────
# POST /api/sl/convert-to-limit   (L key)
# Converts the active SL order to a LIMIT order.
# Both trigger and limit price = crosshair price.
# ─────────────────────────────────────────────
class ConvertRequest(BaseModel):
    symbol: str
    price: float          # crosshair price

@router.post("/sl/convert-to-limit")
async def convert_to_limit(req: ConvertRequest):
    state = sl_state.get(req.symbol)
    if not state:
        return {"status": "error", "message": "No active SL order for this symbol"}
    order_ids = [o["order_id"] for o in state.get("sl_orders", [])]
    if not order_ids:
        return {"status": "error", "message": "No active SL order for this symbol"}

    price = _round(req.price)

    async def _modify_one(oid):
        try:
            result = await kite1.hard_code_modify_limit_type(
                order_id=oid, price=price, trig_price=price,
                access_token=KITE_ACCESS_TOKEN, api_key=KITE_API_KEY,
                type="LIMIT",
            )
            logger.info(f"SL→LIMIT for {req.symbol} order {oid} @ {price}: {result}")
            return result
        except Exception as e:
            logger.error(f"convert-to-limit error for {oid}: {e}")
            return {"error": str(e)}

    results = await asyncio.gather(*[_modify_one(oid) for oid in order_ids])
    sl_state[req.symbol]["price"] = price
    sl_state[req.symbol]["state"] = "placed"
    return {"status": "ok", "price": price, "results": list(results)}


# ─────────────────────────────────────────────
# POST /api/sl/convert-to-sl   (S key)
# Converts the active LIMIT order back to an SL order at a new price.
# ─────────────────────────────────────────────
class ConvertToSLRequest(BaseModel):
    symbol:  str
    price:   float   # SL limit price
    trigger: float   # SL trigger price

@router.post("/sl/convert-to-sl")
async def convert_to_sl(req: ConvertToSLRequest):
    state = sl_state.get(req.symbol)
    if not state:
        return {"status": "error", "message": "No active SL order for this symbol"}
    order_ids = [o["order_id"] for o in state.get("sl_orders", [])]
    if not order_ids:
        return {"status": "error", "message": "No active SL order for this symbol"}

    price   = _round(req.price)
    trigger = _round(req.trigger)

    async def _modify_one(oid):
        try:
            result = await kite1.hard_code_modify_limit_type(
                order_id=oid, price=price, trig_price=trigger,
                access_token=KITE_ACCESS_TOKEN, api_key=KITE_API_KEY,
                type="SL",
            )
            logger.info(f"LIMIT→SL for {req.symbol} order {oid} @ trigger={trigger} limit={price}: {result}")
            return result
        except Exception as e:
            logger.error(f"convert-to-sl error for {oid}: {e}")
            return {"error": str(e)}

    results = await asyncio.gather(*[_modify_one(oid) for oid in order_ids])
    sl_state[req.symbol]["price"]  = price
    sl_state[req.symbol]["state"]  = "placed"
    return {"status": "ok", "price": price, "trigger": trigger, "results": list(results)}


# POST /api/sl/convert-to-market  (M key)
# Converts the active SL order to a MARKET order.
# ─────────────────────────────────────────────
class SymbolRequest(BaseModel):
    symbol: str

@router.post("/sl/convert-to-market")
async def convert_to_market(req: SymbolRequest):
    state = sl_state.get(req.symbol)
    if not state:
        return {"status": "error", "message": "No active SL order for this symbol"}
    order_ids = [o["order_id"] for o in state.get("sl_orders", [])]
    if not order_ids:
        return {"status": "error", "message": "No active SL order for this symbol"}

    results = []
    for oid in order_ids:
        try:
            result = await kite1.hard_code_modify_limit_type(
                order_id=oid,
                price=0,
                trig_price=0,
                access_token=KITE_ACCESS_TOKEN,
                api_key=KITE_API_KEY,
                type="MARKET"
            )
            logger.info(f"SL→MARKET for {req.symbol} order {oid}: {result}")
            results.append(result)
        except Exception as e:
            logger.error(f"convert-to-market error for {oid}: {e}")
            results.append({"error": str(e)})
    return {"status": "ok", "results": results}


# Freeze qty limits per exchange/index
FREEZE_QTY = {
    "BANKNIFTY":   600,
    "MIDCPNIFTY": 2800,
    "FINNIFTY":   1200,
    "SENSEX":     1000,
    "NIFTY":      1755,
}

def _freeze_qty(symbol: str) -> int:
    for prefix, qty in FREEZE_QTY.items():
        if symbol.startswith(prefix):
            return qty
    return FREEZE_QTY["NIFTY"]


async def _fetch_net_position(symbol: str, exchange: str) -> int:
    """Fetch live net open qty for a symbol from Kite portfolio/positions."""
    headers = {
        "X-Kite-Version": "3",
        "Authorization": f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}",
    }
    try:
        r = await kite1.reqsession.get(
            "https://api.kite.trade/portfolio/positions",
            headers=headers,
            timeout=7
        )
        r.raise_for_status()
        data = r.json()
        for p in data.get("data", {}).get("net", []):
            if p.get("tradingsymbol") == symbol:
                return int(p.get("quantity", 0))
    except Exception as e:
        logger.error(f"_fetch_net_position error: {e}")
    return 0


# ─────────────────────────────────────────────
# GET /api/net-position?symbol=XXX&exchange=NFO
# Returns live net open qty for a symbol.
# ─────────────────────────────────────────────
@router.get("/net-position")
async def get_net_position(symbol: str, exchange: str = "NFO"):
    qty = await _fetch_net_position(symbol, exchange)
    return {"symbol": symbol, "net_qty": qty}


async def _place_market_order(exchange: str, symbol: str, tx: str, qty: int) -> dict:
    """Place a plain MARKET order (not SL) for the given qty."""
    headers = {
        "X-Kite-Version": "3",
        "User-Agent": "Kiteconnect-python/5.0.1",
        "Authorization": f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}",
    }
    data = {
        "variety":            "regular",
        "exchange":           exchange,
        "tradingsymbol":      symbol,
        "transaction_type":   tx,
        "quantity":           str(qty),
        "product":            "NRML",
        "order_type":         "MARKET",
        "validity":           "DAY",
        "market_protection":  "-1",
    }
    r = await kite1.reqsession.post(
        "https://api.kite.trade/orders/regular",
        data=data,
        headers=headers,
        timeout=7
    )
    body = r.json()
    if r.status_code != 200:
        msg = body.get("message") or body.get("error") or f"HTTP {r.status_code}"
        raise ValueError(msg)
    return body


class _ChunkedOrderError(Exception):
    """Raised when a mid-chunk market order placement fails.
    `.placed` holds the orders successfully placed before the failure,
    so the caller can unwind them if needed.
    """
    def __init__(self, message: str, placed: list):
        super().__init__(message)
        self.placed: list = placed  # [{"order_id": str, "qty": int}, ...]


async def _place_market_order_chunked(exchange: str, symbol: str, tx: str, qty: int) -> list:
    """Place MARKET orders in freeze-qty slices.

    Returns [{"order_id": str, "qty": int}, ...] covering the full qty on success.
    Raises _ChunkedOrderError on any chunk failure; .placed carries the already-placed
    slices so the caller can attempt an unwind of the partial fill.
    """
    freeze     = _freeze_qty(symbol)
    remaining  = qty
    placed: list = []
    while remaining > 0:
        chunk = min(remaining, freeze)
        try:
            result = await _place_market_order(exchange, symbol, tx, chunk)
        except Exception as exc:
            raise _ChunkedOrderError(
                f"MARKET {tx} chunk {chunk}/{qty} {symbol} network error: {exc}", placed
            ) from exc
        order_id = (result.get("data") or {}).get("order_id")
        if not order_id:
            msg = result.get("message") or result.get("error") or "no order_id returned"
            raise _ChunkedOrderError(
                f"MARKET {tx} chunk {chunk}/{qty} {symbol} rejected: {msg}", placed
            )
        placed.append({"order_id": order_id, "qty": chunk})
        remaining -= chunk
    return placed


# ─────────────────────────────────────────────
# POST /api/sl/close-position     (Shift+M)
# 1. Convert all TRIGGER PENDING SL orders for symbol → MARKET
# 2. Fetch live net position from Kite
# 3. If any qty remains, place pure MARKET orders in freeze-qty chunks
# ─────────────────────────────────────────────
@router.post("/sl/close-position")
async def close_position(req: SymbolRequest):
    symbol   = req.symbol
    exchange = sl_state.get(symbol, {}).get("exchange", "NFO")
    logs     = []

    # ── Step 1: Convert all TRIGGER PENDING SL orders → MARKET ──────────
    try:
        orders     = await kite1.hardcode_orders(KITE_API_KEY, KITE_ACCESS_TOKEN)
        all_orders = orders.get("data", []) if isinstance(orders, dict) else []
        sl_orders  = [
            o for o in all_orders
            if o.get("tradingsymbol") == symbol
            and o.get("order_type") == "SL"
            and o.get("status") == "TRIGGER PENDING"
        ]
        for o in sl_orders:
            oid = o["order_id"]
            res = await kite1.hard_code_modify_limit_type(
                order_id=oid, price=0, trig_price=0,
                access_token=KITE_ACCESS_TOKEN, api_key=KITE_API_KEY,
                type="MARKET"
            )
            logs.append(f"SL {oid} → MARKET: {res}")
            logger.info(logs[-1])
        logs.append(f"Step 1 done — converted {len(sl_orders)} SL order(s) to MARKET")
    except Exception as e:
        logger.error(f"close-position step-1 error: {e}")
        logs.append(f"Step-1 error: {e}")

    # ── Step 2: Fetch live position ──────────────────────────────────────
    try:
        net = await _fetch_net_position(symbol, exchange)
        logs.append(f"Step 2 — live net position: {net}")
        logger.info(logs[-1])
    except Exception as e:
        logger.error(f"close-position step-2 error: {e}")
        logs.append(f"Step-2 error: {e}")
        net = 0

    # ── Step 3: Flatten remainder in freeze-qty chunks ───────────────────
    if net != 0:
        tx         = "SELL" if net > 0 else "BUY"
        remaining  = abs(net)
        freeze     = _freeze_qty(symbol)
        order_num  = 0
        try:
            while remaining > 0:
                chunk = min(remaining, freeze)
                res   = await _place_market_order(exchange, symbol, tx, chunk)
                order_num += 1
                logs.append(f"Step 3 order {order_num}: {tx} {chunk} @ MARKET → {res}")
                logger.info(logs[-1])
                remaining -= chunk
            logs.append(f"Step 3 done — placed {order_num} MARKET order(s) to flatten {abs(net)} units")
        except Exception as e:
            logger.error(f"close-position step-3 error: {e}")
            logs.append(f"Step-3 error: {e}")
    else:
        logs.append("Step 3 skipped — position already flat after SL conversion")

    # Clear SL state
    sl_state.pop(symbol, None)

    return {"status": "ok", "logs": logs}


# ─────────────────────────────────────────────
# Helper: fetch all symbols with non-zero net qty
# ─────────────────────────────────────────────
async def _fetch_all_open_positions() -> list:
    headers = {
        "X-Kite-Version": "3",
        "Authorization": f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}",
    }
    r = await kite1.reqsession.get(
        "https://api.kite.trade/portfolio/positions",
        headers=headers,
        timeout=7,
    )
    r.raise_for_status()
    data = r.json()
    return [
        {
            "symbol":     p["tradingsymbol"],
            "qty":        int(p.get("quantity", 0)),
            "exchange":   p.get("exchange", "NFO"),
            "last_price": float(p.get("last_price", 0) or 0),
        }
        for p in data.get("data", {}).get("net", [])
        if int(p.get("quantity", 0)) != 0
    ]


# ─────────────────────────────────────────────
# POST /api/sl/close-all
# Close every open position on the account:
#   1. Fetch all positions + orders once
#   2. Per symbol: convert matched SL → MARKET,
#      then place MARKET exit for any remainder
#   3. Wait ~3 s for fills
#   4–5. Re-fetch + retry up to MAX_RETRIES times
#   6. Verify net == 0 and no orphan pending orders
# ─────────────────────────────────────────────
MAX_RETRIES = 3

@router.post("/sl/close-all")
async def close_all_positions():
    logs    = []
    results = {}   # symbol → {"closed": bool}

    # ── Step 1: Fetch open positions from Kite ───────────────────────────
    # SL order IDs come from the sl_orders cache (populated at placement time).
    # The Kite orders list is only fetched lazily as a fallback if the cache is empty.
    try:
        positions = await _fetch_all_open_positions()
    except Exception as e:
        return {"status": "error", "message": f"Failed to fetch positions: {e}", "results": {}, "logs": []}

    if not positions:
        return {"status": "ok", "message": "No open positions found", "results": {}, "logs": []}

    symbols_found = [p["symbol"] for p in positions]
    logs.append(f"Found {len(positions)} open position(s): {symbols_found}")
    for p in positions:
        results[p["symbol"]] = {"closed": False}

    # Lazy fallback: only fetch all Kite orders if a symbol has no cached SL orders
    _kite_orders_cache = None
    async def _get_kite_orders():
        nonlocal _kite_orders_cache
        if _kite_orders_cache is None:
            orders_res       = await kite1.hardcode_orders(KITE_API_KEY, KITE_ACCESS_TOKEN)
            _kite_orders_cache = orders_res.get("data", []) if isinstance(orders_res, dict) else []
            logs.append("Fetched orders from Kite (cache miss — sl_orders was empty)")
        return _kite_orders_cache

    # ── Step 2: Per symbol — SL convert + MARKET exit ───────────────────
    async def _exit_symbol(symbol, net_qty, exchange):
        sym_logs = []
        tx        = "SELL" if net_qty > 0 else "BUY"
        abs_qty   = abs(net_qty)

        # Fast path: use sl_orders populated at SL placement time
        cached_sl = sl_state.get(symbol, {}).get("sl_orders", [])

        if cached_sl:
            sl_total = sum(o["qty"] for o in cached_sl)
            sym_logs.append(f"Using {len(cached_sl)} cached SL order(s) (total qty {sl_total})")
            for o in cached_sl:
                try:
                    res = await kite1.hard_code_modify_limit_type(
                        order_id=o["order_id"], price=0, trig_price=0,
                        access_token=KITE_ACCESS_TOKEN, api_key=KITE_API_KEY,
                        type="MARKET",
                    )
                    sym_logs.append(f"[cache] SL {o['order_id']} ({o['qty']} qty) → MARKET: {res}")
                except Exception as e:
                    sym_logs.append(f"[cache] SL {o['order_id']} convert error: {e}")
        else:
            # Fallback: fetch live orders from Kite (e.g. after server restart)
            sym_logs.append("No cached SL orders — falling back to Kite orders fetch")
            all_orders = await _get_kite_orders()
            kite_sl    = [
                o for o in all_orders
                if o.get("tradingsymbol") == symbol
                and o.get("order_type") == "SL"
                and o.get("status") == "TRIGGER PENDING"
            ]
            sl_total = sum(int(o.get("quantity", 0)) for o in kite_sl)
            for o in kite_sl:
                try:
                    res = await kite1.hard_code_modify_limit_type(
                        order_id=o["order_id"], price=0, trig_price=0,
                        access_token=KITE_ACCESS_TOKEN, api_key=KITE_API_KEY,
                        type="MARKET",
                    )
                    sym_logs.append(f"[kite] SL {o['order_id']} ({o.get('quantity')} qty) → MARKET: {res}")
                except Exception as e:
                    sym_logs.append(f"[kite] SL {o['order_id']} convert error: {e}")

        remainder = abs_qty - sl_total
        if remainder <= 0:
            sym_logs.append(f"SL qty {sl_total} covers full position {abs_qty} — no extra MARKET order needed")
            return sym_logs

        if sl_total > 0:
            sym_logs.append(f"SL qty {sl_total} partial — placing MARKET {tx} for remaining {remainder}")
        else:
            sym_logs.append(f"No SL orders — placing MARKET {tx} for {remainder}")

        # Place MARKET exit in freeze-qty chunks for the remainder
        freeze    = _freeze_qty(symbol)
        order_num = 0
        try:
            while remainder > 0:
                chunk     = min(remainder, freeze)
                res       = await _place_market_order(exchange, symbol, tx, chunk)
                order_num += 1
                sym_logs.append(f"MARKET order {order_num}: {tx} {chunk} → {res}")
                remainder -= chunk
            sym_logs.append(f"Placed {order_num} MARKET order(s) to exit {abs_qty} units")
        except Exception as e:
            sym_logs.append(f"MARKET order error: {e}")

        return sym_logs

    for pos in positions:
        sym_logs = await _exit_symbol(pos["symbol"], pos["qty"], pos["exchange"])
        for line in sym_logs:
            logs.append(f"[{pos['symbol']}] {line}")

    # ── Step 3: Wait for MARKET fills (~1–2 s on Zerodha) ───────────────
    await asyncio.sleep(3)

    # ── Steps 4–5: Re-fetch + retry remaining ───────────────────────────
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            remaining_pos = await _fetch_all_open_positions()
        except Exception as e:
            logs.append(f"Re-fetch attempt {attempt} error: {e}")
            break

        still_open = {p["symbol"]: p for p in remaining_pos if p["symbol"] in results}
        if not still_open:
            break

        logs.append(f"Retry {attempt}: {len(still_open)} symbol(s) still open — {list(still_open.keys())}")
        for symbol, pos in still_open.items():
            sym_logs = await _exit_symbol(symbol, pos["qty"], pos["exchange"])
            for line in sym_logs:
                logs.append(f"[{symbol}] Retry {attempt}: {line}")

        if attempt < MAX_RETRIES:
            await asyncio.sleep(3)

    # ── Step 6: Final verification ───────────────────────────────────────
    symbols_remaining = []
    orphan_orders     = []
    try:
        final_pos = await _fetch_all_open_positions()
        final_map = {p["symbol"]: p["qty"] for p in final_pos}

        for symbol in results:
            if symbol not in final_map:
                results[symbol]["closed"] = True
                sl_state.pop(symbol, None)
            else:
                results[symbol]["closed"] = False
                symbols_remaining.append({"symbol": symbol, "qty": final_map[symbol]})

        # Check for orphan pending orders on any of the original symbols
        try:
            orders_res2 = await kite1.hardcode_orders(KITE_API_KEY, KITE_ACCESS_TOKEN)
            all_orders2 = orders_res2.get("data", []) if isinstance(orders_res2, dict) else []
            orphan_orders = [
                {"symbol": o["tradingsymbol"], "order_id": o["order_id"], "status": o["status"]}
                for o in all_orders2
                if o.get("tradingsymbol") in results
                and o.get("status") in ("TRIGGER PENDING", "OPEN")
            ]
        except Exception as e:
            logs.append(f"Orphan-order check error: {e}")

        if symbols_remaining:
            logs.append(f"⚠️ {len(symbols_remaining)} position(s) still open after retries: "
                        f"{[(r['symbol'], r['qty']) for r in symbols_remaining]}")
        else:
            logs.append("✅ All positions confirmed closed (net == 0)")

        if orphan_orders:
            logs.append(f"⚠️ {len(orphan_orders)} orphan pending order(s): "
                        f"{[(o['symbol'], o['order_id']) for o in orphan_orders]}")
        else:
            logs.append("✅ No orphan pending orders")

    except Exception as e:
        logs.append(f"Verification error: {e}")

    all_closed = not symbols_remaining
    return {
        "status":            "ok" if all_closed else "partial",
        "message":           "All positions closed" if all_closed else f"{len(symbols_remaining)} position(s) still open",
        "symbols_closed":    [s for s, r in results.items() if r["closed"]],
        "symbols_remaining": symbols_remaining,
        "orphan_orders":     orphan_orders,
        "logs":              logs,
    }


# ─────────────────────────────────────────────
# POST /api/place-limit-order
# Places a regular LIMIT order (Buy/Sell buttons).
# ─────────────────────────────────────────────
class LimitOrderRequest(BaseModel):
    symbol:      str
    price:       float
    side:        str        # BUY or SELL
    qty:         int
    exchange:    str   = "NFO"
    order_type:  str   = "L"    # 'L' (LIMIT) | 'M' (MARKET) | 'SL' (SL-LIMIT)
    sl_buffer:   float = 0.20   # gap between trigger and limit price
    sl_distance: float = 5.0    # points from LTP for SL trigger price

@router.post("/place-limit-order")
async def place_limit_order(req: LimitOrderRequest):
    price = _round(req.price)

    # Map frontend type codes → Kite order_type
    kite_type_map = {"L": "LIMIT", "M": "MARKET", "SL": "SL"}
    kite_order_type = kite_type_map.get(req.order_type.upper(), "LIMIT")

    headers = {
        "X-Kite-Version": "3",
        "User-Agent":      "Kiteconnect-python/5.0.1",
        "Authorization":   f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}",
    }
    # For SL orders compute trigger and limit from distance + buffer
    if kite_order_type == "SL":
        if req.side.upper() == "SELL":
            # Closing a BUY position: SL trigger below LTP
            trigger = _round(price - req.sl_distance)
            limit   = _round(trigger - req.sl_buffer)
        else:
            # Closing a SELL position: SL trigger above LTP
            trigger = _round(price + req.sl_distance)
            limit   = _round(trigger + req.sl_buffer)
    else:
        trigger = None
        limit   = price

    data = {
        "variety":          "regular",
        "exchange":         req.exchange,
        "tradingsymbol":    req.symbol,
        "transaction_type": req.side,
        "quantity":         str(req.qty),
        "product":          "NRML",
        "order_type":       kite_order_type,
        "validity":         "DAY",
    }
    if kite_order_type in ("LIMIT", "SL"):
        data["price"] = str(limit)
    if kite_order_type == "SL":
        data["trigger_price"] = str(trigger)

    freeze    = _freeze_qty(req.symbol)
    remaining = req.qty
    order_ids = []

    try:
        while remaining > 0:
            chunk = min(remaining, freeze)
            data["quantity"] = str(chunk)
            r = await kite1.reqsession.post(
                "https://api.kite.trade/orders/regular",
                data=data, headers=headers, timeout=7
            )
            result = r.json()
            if r.status_code != 200:
                msg = result.get("message") or result.get("error") or f"HTTP {r.status_code}"
                logger.error(f"place-limit-order broker error: {msg}")
                return {"status": "error", "message": msg}
            order_id = (result.get("data") or {}).get("order_id")
            order_ids.append(order_id)
            remaining -= chunk

        sliced = len(order_ids) > 1
        if kite_order_type == "SL":
            logger.info(f"SL {req.side} {req.qty} {req.symbol} trigger={trigger} limit={limit} → {order_ids}")
        else:
            logger.info(f"{kite_order_type} {req.side} {req.qty} {req.symbol} @ {limit} → {order_ids}" + (" [sliced]" if sliced else ""))
        return {"status": "success", "order_id": order_ids[0], "order_ids": order_ids, "price": limit, "qty": req.qty, "side": req.side, "order_type": kite_order_type, "sliced": sliced}
    except Exception as e:
        logger.error(f"place-limit-order error: {e}")
        return {"status": "error", "message": str(e)}


# ─────────────────────────────────────────────
# POST /api/cancel-order
# Cancels an order by order_id. Returns
# {status: "cancelled"} or {status: "filled"}.
# ─────────────────────────────────────────────
class CancelOrderReq(BaseModel):
    order_id: str

@router.post("/cancel-order")
async def cancel_order(req: CancelOrderReq):
    try:
        result = await kite1.hard_code_regular_cancel_order(
            order_id=req.order_id,
            access_token=KITE_ACCESS_TOKEN,
            api_key=KITE_API_KEY,
        )
        if result and result.get("status") == "success":
            logger.info(f"cancel-order {req.order_id}: cancelled")
            return {"status": "cancelled"}
        msg = (result or {}).get("message", "unknown")
        logger.info(f"cancel-order {req.order_id}: assumed filled — {msg}")
        return {"status": "filled", "message": msg}
    except Exception as e:
        logger.error(f"cancel-order error: {e}")
        return {"status": "filled", "message": str(e)}


# ─────────────────────────────────────────────
# POST /api/place-sl-exact
# Places an SL order with caller-supplied
# trigger_price and limit price (no server-side
# distance/buffer calculation).
# ─────────────────────────────────────────────
class PlaceSlExactReq(BaseModel):
    symbol:   str
    side:     str    # BUY or SELL
    qty:      int
    trigger:  float
    limit:    float
    exchange: str = "NFO"

@router.post("/place-sl-exact")
async def place_sl_exact(req: PlaceSlExactReq):
    trigger = _round(req.trigger)
    limit   = _round(req.limit)
    headers = {
        "X-Kite-Version": "3",
        "User-Agent":      "Kiteconnect-python/5.0.1",
        "Authorization":   f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}",
    }
    data = {
        "variety":          "regular",
        "exchange":         req.exchange,
        "tradingsymbol":    req.symbol,
        "transaction_type": req.side.upper(),
        "quantity":         str(req.qty),
        "product":          "NRML",
        "order_type":       "SL",
        "validity":         "DAY",
        "trigger_price":    str(trigger),
        "price":            str(limit),
    }
    try:
        r = await kite1.reqsession.post(
            "https://api.kite.trade/orders/regular",
            data=data, headers=headers, timeout=7
        )
        result = r.json()
        if r.status_code != 200:
            msg = result.get("message") or result.get("error") or f"HTTP {r.status_code}"
            logger.error(f"place-sl-exact broker error: {msg}")
            return {"status": "error", "message": msg}
        order_id = (result.get("data") or {}).get("order_id")
        logger.info(f"place-sl-exact {req.side} {req.qty} {req.symbol} trigger={trigger} limit={limit} → {order_id}")
        # Cache the order in sl_orders so close-all can use it without a Kite fetch
        if order_id:
            existing      = sl_state.get(req.symbol, {})
            existing_sl   = existing.get("sl_orders", [])
            existing_ids  = {o["order_id"] for o in existing_sl}
            if order_id not in existing_ids:
                sl_state[req.symbol] = {
                    **existing,
                    "sl_orders": existing_sl + [{"order_id": order_id, "qty": req.qty}],
                }
        return {"status": "success", "order_id": order_id}
    except Exception as e:
        logger.error(f"place-sl-exact error: {e}")
        return {"status": "error", "message": str(e)}


# ─────────────────────────────────────────────
# POST /api/sl/check-sl-orders
# Reconciles cached sl_orders against live Kite
# positions. Places any missing SL orders and
# updates the sl_orders cache.
# Price priority: cached SL price → SL line →
#                 default_sl_dist from LTP.
# ─────────────────────────────────────────────
@router.post("/sl/check-sl-orders")
async def check_sl_orders():
    try:
        positions = await _fetch_all_open_positions()
    except Exception as e:
        return {"status": "error", "message": f"Failed to fetch positions: {e}", "results": []}

    if not positions:
        return {"status": "ok", "message": "No open positions found", "results": []}

    # Fetch all live orders once — used to filter out cancelled/triggered cached SL IDs
    try:
        live_orders_resp = await kite1.hardcode_orders(KITE_API_KEY, KITE_ACCESS_TOKEN)
        live_orders      = live_orders_resp.get("data", []) if isinstance(live_orders_resp, dict) else []
        active_order_ids = {
            o["order_id"]
            for o in live_orders
            if o.get("status") in ("OPEN", "TRIGGER PENDING")
        }
    except Exception as e:
        logger.warning(f"check-sl-orders: could not fetch live orders, skipping cache filter: {e}")
        active_order_ids = None  # fall back to trusting cache as-is

    default_sl_dist = app_config.get("default_sl_dist", 10.0)
    used_fallback   = False
    results         = []

    headers = {
        "X-Kite-Version": "3",
        "User-Agent":      "Kiteconnect-python/5.0.1",
        "Authorization":   f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}",
    }

    for pos in positions:
        symbol   = pos["symbol"]
        net_qty  = pos["qty"]
        exchange = pos["exchange"]
        ltp      = pos["last_price"]
        abs_qty  = abs(net_qty)
        side     = "SELL" if net_qty > 0 else "BUY"

        sym_state = sl_state.get(symbol, {})
        cached_sl = sym_state.get("sl_orders", [])

        # Filter cached orders to only those still alive on Zerodha
        if active_order_ids is not None:
            active_sl = [o for o in cached_sl if o["order_id"] in active_order_ids]
            # Clean up stale IDs from sl_state so they don't accumulate
            if len(active_sl) != len(cached_sl):
                dead_ids = {o["order_id"] for o in cached_sl} - {o["order_id"] for o in active_sl}
                logger.info(f"check-sl-orders: pruning stale order IDs for {symbol}: {dead_ids}")
                new_oids = [oid for oid in (sym_state.get("order_ids") or []) if oid not in dead_ids]
                sl_state[symbol] = {
                    **sym_state,
                    "sl_orders": active_sl,
                    "order_ids": new_oids,
                    "order_id":  new_oids[0] if new_oids else None,
                }
                sym_state = sl_state[symbol]
        else:
            active_sl = cached_sl

        cached_qty  = sum(o["qty"] for o in active_sl)
        missing_qty = abs_qty - cached_qty

        if missing_qty <= 0:
            results.append({
                "symbol":       symbol,
                "status":       "covered",
                "position_qty": abs_qty,
                "cached_qty":   cached_qty,
                "placed_qty":   0,
                "order_count":  len(active_sl),
                "price_source": None,
            })
            continue

        # SL price priority: active SL price → SL line price → default dist from LTP
        sl_price       = sym_state.get("price")
        trigger_buffer = sym_state.get("trigger_buffer", 0.20)

        if sl_price is not None and active_sl:
            price_source = "cache"
        elif sl_price is not None:
            price_source = "SL line"
        else:
            sl_price     = _round(ltp - default_sl_dist) if side == "SELL" else _round(ltp + default_sl_dist)
            price_source = "default dist"
            used_fallback = True

        if side == "SELL":
            trigger = _round(sl_price + trigger_buffer)
            limit   = sl_price
        else:
            trigger = _round(sl_price - trigger_buffer)
            limit   = sl_price

        freeze     = _freeze_qty(symbol)
        remaining  = missing_qty
        placed_ids = []
        errors     = []

        while remaining > 0:
            chunk = min(remaining, freeze)
            data  = {
                "variety":          "regular",
                "exchange":         exchange,
                "tradingsymbol":    symbol,
                "transaction_type": side,
                "quantity":         str(chunk),
                "product":          "NRML",
                "order_type":       "SL",
                "validity":         "DAY",
                "trigger_price":    str(trigger),
                "price":            str(limit),
            }

            placed_this_chunk = False
            for attempt in range(3):
                try:
                    r      = await kite1.reqsession.post(
                        "https://api.kite.trade/orders/regular",
                        data=data, headers=headers, timeout=7,
                    )
                    result = r.json()
                    if r.status_code == 200:
                        order_id = (result.get("data") or {}).get("order_id")
                        if order_id:
                            placed_ids.append({"order_id": order_id, "qty": chunk})
                            existing      = sl_state.get(symbol, {})
                            existing_sl   = existing.get("sl_orders", [])
                            existing_oids = existing.get("order_ids") or ([existing["order_id"]] if existing.get("order_id") else [])
                            existing_sl_ids = {o["order_id"] for o in existing_sl}
                            new_sl   = existing_sl   + ([{"order_id": order_id, "qty": chunk}] if order_id not in existing_sl_ids else [])
                            new_oids = existing_oids + ([order_id] if order_id not in existing_oids else [])
                            sl_state[symbol] = {
                                **existing,
                                "sl_orders":      new_sl,
                                "order_ids":      new_oids,
                                "order_id":       new_oids[0] if new_oids else None,
                                "price":          existing.get("price") or sl_price,
                                "side":           existing.get("side") or side,
                                "exchange":       existing.get("exchange") or exchange,
                                "trigger_buffer": trigger_buffer,
                                "state":          "placed",
                            }
                            placed_this_chunk = True
                            logger.info(f"check-sl-orders: placed {side} {chunk} SL for {symbol} @ {trigger}/{limit} → {order_id}")
                        break
                    else:
                        msg = result.get("message") or f"HTTP {r.status_code}"
                        is_rate_limit = any(kw in msg.lower() for kw in ("exceeded", "too many", "rate"))
                        logger.error(f"check-sl-orders broker error for {symbol}: {msg}")
                        if is_rate_limit and attempt < 2:
                            await asyncio.sleep(1.0)
                            continue
                        errors.append(f"chunk {chunk}: {msg}")
                        break
                except Exception as e:
                    msg = str(e)
                    is_rate_limit = any(kw in msg.lower() for kw in ("429", "too many", "exceeded"))
                    logger.error(f"check-sl-orders error for {symbol}: {e}")
                    if is_rate_limit and attempt < 2:
                        await asyncio.sleep(1.0)
                        continue
                    errors.append(f"chunk {chunk}: {msg}")
                    break

            remaining -= chunk
            if placed_this_chunk:
                await asyncio.sleep(0.10)

        placed_qty = sum(o["qty"] for o in placed_ids)
        results.append({
            "symbol":       symbol,
            "status":       "placed" if placed_ids else "error",
            "position_qty": abs_qty,
            "cached_qty":   cached_qty,
            "placed_qty":   placed_qty,
            "order_count":  len(cached_sl) + len(placed_ids),
            "price":        sl_price,
            "price_source": price_source,
            "errors":       errors,
        })

    return {
        "status":       "ok",
        "used_fallback": used_fallback,
        "results":      results,
    }


# ─────────────────────────────────────────────
# POST /api/modify-sl-order
# Modifies trigger and limit price of an open
# SL order. Used by the Auto SL Increment loop.
# ─────────────────────────────────────────────
class ModifySlReq(BaseModel):
    order_id: str
    trigger:  float
    limit:    float

@router.post("/modify-sl-order")
async def modify_sl_order(req: ModifySlReq):
    trigger = _round(req.trigger)
    limit   = _round(req.limit)
    try:
        result = await kite1.hard_code_regular_modify_order(
            order_id=req.order_id,
            price=limit,
            trig_price=trigger,
            access_token=KITE_ACCESS_TOKEN,
            api_key=KITE_API_KEY,
        )
        if result and result.get("status") == "success":
            logger.info(f"modify-sl-order {req.order_id} trigger={trigger} limit={limit}")
            return {"status": "success"}
        msg = (result or {}).get("message", "modify failed")
        return {"status": "error", "message": msg}
    except Exception as e:
        logger.error(f"modify-sl-order error: {e}")
        return {"status": "error", "message": str(e)}


# ─────────────────────────────────────────────
# POST /api/set-default-sl
# Updates the in-memory default SL distance used
# by handle_order_update for L/M order fills.
# ─────────────────────────────────────────────
class DefaultSlReq(BaseModel):
    distance: float

@router.post("/set-default-sl")
async def set_default_sl(req: DefaultSlReq):
    if req.distance <= 0:
        return {"status": "error", "message": "distance must be > 0"}
    app_config["default_sl_dist"] = req.distance
    logger.info(f"default_sl_dist updated to {req.distance}")
    return {"status": "ok", "default_sl_dist": req.distance}


@router.post("/positions/reset")
async def reset_positions():
    """Signal tick_collector to clear its active_positions cache."""
    import pathlib
    pathlib.Path("/tmp/reset_positions").touch()
    logger.info("reset_positions flag set — tick_collector will clear active_positions on next order update")
    return {"status": "ok"}


@router.post("/refresh-kite-token")
async def refresh_kite_token():
    """
    Re-reads KITE_ACCESS_TOKEN from .env and updates every in-memory reference:
      - kite1 object (used by websocket_handler for order placement)
      - module-level KITE_ACCESS_TOKEN in sl.py (used by direct HTTP calls)
      - module-level KITE_ACCESS_TOKEN in websocket_handler.py
    Call this after generating a new Zerodha token each morning — no server restart needed.
    """
    import sys
    load_dotenv(override=True)
    new_token = os.getenv("KITE_ACCESS_TOKEN", "")
    if not new_token:
        return {"status": "error", "detail": "KITE_ACCESS_TOKEN not found in .env"}

    # 1. Update kite1 object
    kite1.set_access_token(new_token)

    # 2. Update module-level variable in this module (sl.py) — used in direct HTTP calls
    global KITE_ACCESS_TOKEN
    KITE_ACCESS_TOKEN = new_token

    # 3. Update module-level variable in websocket_handler — used in order placement calls
    ws_mod = sys.modules.get("backend.services.websocket_handler")
    if ws_mod:
        ws_mod.KITE_ACCESS_TOKEN = new_token

    logger.info("Kite access token refreshed in-process — kite1 object + all module globals updated")
    return {"status": "ok", "token_suffix": new_token[-6:]}
