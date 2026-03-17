# backend/api/sl.py
# Stop-loss management endpoints.

from fastapi import APIRouter
from pydantic import BaseModel
from typing import Optional
from backend.state import sl_state
from backend.services.websocket_handler import kite1
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN
import logging

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
    price:           float
    trigger_buffer:  Optional[float] = None   # if None, keep existing or default 0.20

@router.post("/sl/set")
async def set_sl(req: SetSLRequest):
    existing       = sl_state.get(req.symbol, {})
    new_price      = _round(req.price)
    new_buffer     = req.trigger_buffer if req.trigger_buffer is not None else existing.get("trigger_buffer", 0.20)

    sl_state[req.symbol] = {
        "price":          new_price,
        "trigger_buffer": new_buffer,
        "order_id":       existing.get("order_id"),
        "side":           existing.get("side"),
        "qty":            existing.get("qty"),
        "exchange":       existing.get("exchange"),
        "state":          existing.get("state", "pending"),
    }

    # If an order is already placed, modify it on Kite too
    order_id = existing.get("order_id")
    if order_id and existing.get("state") == "placed":
        try:
            trigger = _round(new_price + new_buffer) if existing.get("side") == "SELL" else _round(new_price - new_buffer)
            result = await kite1.hard_code_regular_modify_order(
                order_id=order_id,
                price=new_price,
                trig_price=trigger,
                access_token=KITE_ACCESS_TOKEN,
                api_key=KITE_API_KEY
            )
            logger.info(f"SL order {order_id} modified to {new_price} for {req.symbol}: {result}")
            sl_state[req.symbol]["state"] = "placed"
        except Exception as e:
            logger.error(f"Failed to modify SL order {order_id}: {e}")

    return {"status": "ok", "symbol": req.symbol, "price": new_price}


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
    if not state or not state.get("order_id"):
        return {"status": "error", "message": "No active SL order for this symbol"}

    price = _round(req.price)
    try:
        result = await kite1.hard_code_modify_limit_type(
            order_id=state["order_id"],
            price=price,
            trig_price=price,
            access_token=KITE_ACCESS_TOKEN,
            api_key=KITE_API_KEY,
            type="LIMIT"
        )
        logger.info(f"SL→LIMIT for {req.symbol} @ {price}: {result}")
        sl_state[req.symbol]["price"] = price
        sl_state[req.symbol]["state"] = "placed"
        return {"status": "ok", "price": price, "result": result}
    except Exception as e:
        logger.error(f"convert-to-limit error: {e}")
        return {"status": "error", "message": str(e)}


# ─────────────────────────────────────────────
# POST /api/sl/convert-to-market  (M key)
# Converts the active SL order to a MARKET order.
# ─────────────────────────────────────────────
class SymbolRequest(BaseModel):
    symbol: str

@router.post("/sl/convert-to-market")
async def convert_to_market(req: SymbolRequest):
    state = sl_state.get(req.symbol)
    if not state or not state.get("order_id"):
        return {"status": "error", "message": "No active SL order for this symbol"}

    try:
        result = await kite1.hard_code_modify_limit_type(
            order_id=state["order_id"],
            price=0,
            trig_price=0,
            access_token=KITE_ACCESS_TOKEN,
            api_key=KITE_API_KEY,
            type="MARKET"
        )
        logger.info(f"SL→MARKET for {req.symbol}: {result}")
        return {"status": "ok", "result": result}
    except Exception as e:
        logger.error(f"convert-to-market error: {e}")
        return {"status": "error", "message": str(e)}


# Freeze qty limits per exchange/index
FREEZE_QTY = {
    "NIFTY":  1755,
    "SENSEX": 1000,
}

def _freeze_qty(symbol: str) -> int:
    if symbol.startswith("SENSEX"):
        return FREEZE_QTY["SENSEX"]
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


async def _place_market_order(exchange: str, symbol: str, tx: str, qty: int) -> dict:
    """Place a plain MARKET order (not SL) for the given qty."""
    headers = {
        "X-Kite-Version": "3",
        "User-Agent": "Kiteconnect-python/5.0.1",
        "Authorization": f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}",
    }
    data = {
        "variety":          "regular",
        "exchange":         exchange,
        "tradingsymbol":    symbol,
        "transaction_type": tx,
        "quantity":         str(qty),
        "product":          "NRML",
        "order_type":       "MARKET",
        "validity":         "DAY",
    }
    r = await kite1.reqsession.post(
        "https://api.kite.trade/orders/regular",
        data=data,
        headers=headers,
        timeout=7
    )
    r.raise_for_status()
    return r.json()


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

    try:
        r = await kite1.reqsession.post(
            "https://api.kite.trade/orders/regular",
            data=data, headers=headers, timeout=7
        )
        r.raise_for_status()
        result = r.json()
        order_id = (result.get("data") or {}).get("order_id")
        if kite_order_type == "SL":
            logger.info(f"SL {req.side} {req.qty} {req.symbol} trigger={trigger} limit={limit} → order_id={order_id}")
        else:
            logger.info(f"{kite_order_type} {req.side} {req.qty} {req.symbol} @ {limit} → order_id={order_id}")
        return {"status": "success", "order_id": order_id, "price": limit, "qty": req.qty, "side": req.side, "order_type": kite_order_type}
    except Exception as e:
        logger.error(f"place-limit-order error: {e}")
        return {"status": "error", "message": str(e)}
