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
    symbol: str
    price: float

@router.post("/sl/set")
async def set_sl(req: SetSLRequest):
    existing  = sl_state.get(req.symbol, {})
    new_price = _round(req.price)

    sl_state[req.symbol] = {
        "price":    new_price,
        "order_id": existing.get("order_id"),
        "side":     existing.get("side"),
        "qty":      existing.get("qty"),
        "exchange": existing.get("exchange"),
        "state":    existing.get("state", "pending"),
    }

    # If an order is already placed, modify it on Kite too
    order_id = existing.get("order_id")
    if order_id and existing.get("state") == "placed":
        try:
            trigger = _round(new_price + 0.20) if existing.get("side") == "SELL" else _round(new_price - 0.20)
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


# ─────────────────────────────────────────────
# POST /api/sl/close-position     (Shift+M)
# 1. Convert all TRIGGER PENDING SL orders for symbol → MARKET
# 2. Check net open position
# 3. If units remain, place opposite MARKET order to flatten
# ─────────────────────────────────────────────
@router.post("/sl/close-position")
async def close_position(req: SymbolRequest):
    symbol = req.symbol
    state  = sl_state.get(symbol, {})
    logs   = []

    # Step 1 — convert any pending SL orders to market
    try:
        orders = await kite1.hardcode_orders(KITE_API_KEY, KITE_ACCESS_TOKEN)
        all_orders = orders.get("data", []) if isinstance(orders, dict) else []
        sl_orders = [
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
            logs.append(f"Converted SL {oid} → MARKET: {res}")
            logger.info(logs[-1])
    except Exception as e:
        logger.error(f"close-position step-1 error: {e}")
        logs.append(f"Step-1 error: {e}")

    # Step 2 — check net position and flatten remainder
    try:
        exchange = state.get("exchange", "NFO")
        qty      = state.get("qty")

        if qty:
            # Net position from sl_state (positive=long, negative=short)
            net = sl_state.get(symbol, {}).get("qty", 0)
            if isinstance(net, int) and net != 0:
                # Flatten: opposite direction at market
                tx = "SELL" if net > 0 else "BUY"
                close_qty = abs(net)

                if tx == "SELL":
                    res = await kite1.hard_code_regular_sell_order(
                        exchange=exchange,
                        trade_symbol=symbol,
                        qty=close_qty,
                        stop_loss_price=0,
                        trig_price=0,
                        api_key=KITE_API_KEY,
                        access_token=KITE_ACCESS_TOKEN
                    )
                else:
                    res = await kite1.hard_code_regular_buy_order(
                        exchange=exchange,
                        trade_symbol=symbol,
                        qty=close_qty,
                        price=0,
                        trig_price=0,
                        api_key=KITE_API_KEY,
                        access_token=KITE_ACCESS_TOKEN
                    )
                logs.append(f"Flattened {close_qty} units {tx} @ MARKET: {res}")
                logger.info(logs[-1])

        # Clear SL state for symbol
        sl_state.pop(symbol, None)

    except Exception as e:
        logger.error(f"close-position step-2 error: {e}")
        logs.append(f"Step-2 error: {e}")

    return {"status": "ok", "logs": logs}
