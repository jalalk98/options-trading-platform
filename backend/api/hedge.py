# backend/api/hedge.py
# Hedged sell endpoint: place a protective BUY, confirm fill, then execute the sell.
# If the sell fails, auto-unwind the hedge at market.

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, Request
from pydantic import BaseModel, field_validator

from backend.api.sl import (
    _ChunkedOrderError,
    _freeze_qty,
    _place_market_order_chunked,
    _round,
)
from backend.services.websocket_handler import kite1
from backend.state import sl_state
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN

logger = logging.getLogger(__name__)
router = APIRouter()


# ─────────────────────────────────────────────
# Request / Response schemas
# ─────────────────────────────────────────────

class HedgedSellRequest(BaseModel):
    # ── Hedge BUY leg ──────────────────────────────────────────────────────
    hedge_symbol:   str
    hedge_exchange: str = "NFO"
    # Number of individual contracts (not lots).
    # Example: NIFTY lot = 75 contracts. To hedge 1 lot, pass hedge_qty=75.
    hedge_qty: int

    # ── Sell leg ───────────────────────────────────────────────────────────
    sell_symbol:   str
    sell_exchange: str = "NFO"
    # Number of individual contracts (not lots). Must match hedge_qty scale.
    sell_qty:        int
    # LTP at click time, already adjusted by the frontend:
    #   LIMIT orders: price ± limitBuffer
    #   Square-off:   price ± sqBuf (2 pts NIFTY, 5 pts SENSEX)
    #   MARKET orders: raw LTP (unused for execution, included for audit logging)
    sell_price:      float
    sell_order_type: str   = "L"    # 'L' (LIMIT) | 'M' (MARKET) | 'SL' (SL-LIMIT)
    sl_buffer:       float = 0.20   # distance between SL trigger and limit price
    sl_distance:     float = 5.0    # distance from LTP to SL trigger price

    # ── SL-convert path (square-off when an active SL order exists) ────────
    # When True, the sell leg calls hard_code_modify_limit_type on the SL orders
    # already in sl_state[sell_symbol] — exactly mirroring /api/sl/convert-to-market
    # and /api/sl/convert-to-limit (both read sl_state server-side, never from the
    # request body).
    is_sl_convert:   bool = False
    sl_convert_type: str  = "M"    # 'M' → MARKET | 'L' → LIMIT

    @field_validator("hedge_qty", "sell_qty")
    @classmethod
    def must_be_positive(cls, v: int, info) -> int:
        if v <= 0:
            raise ValueError(f"{info.field_name} must be > 0 (got {v})")
        return v


# ─────────────────────────────────────────────
# DB helpers — best-effort; a failure never
# blocks the order flow (orders = real money)
# ─────────────────────────────────────────────

async def _db_insert_hedge_pair(
    pool, hedge_symbol: str, sell_symbol: str, hedge_qty: int
) -> Optional[int]:
    """INSERT initial row, return id or None on failure."""
    try:
        async with pool.acquire() as conn:
            return await conn.fetchval(
                """
                INSERT INTO hedge_pairs
                    (hedge_symbol, sell_symbol, hedge_qty, hedge_order_ids,
                     unwind_order_ids, status)
                VALUES ($1, $2, $3, '{}', '{}', 'hedge_placed')
                RETURNING id
                """,
                hedge_symbol, sell_symbol, hedge_qty,
            )
    except Exception as exc:
        logger.error(f"hedge_pairs INSERT failed: {exc}")
        return None


async def _db_update(pool, row_id: Optional[int], **kwargs) -> None:
    """Best-effort UPDATE of hedge_pairs by primary key. Never raises."""
    if row_id is None or not kwargs:
        return
    cols = list(kwargs)
    sets = ", ".join(f"{col} = ${i + 2}" for i, col in enumerate(cols))
    vals = [kwargs[c] for c in cols]
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                f"UPDATE hedge_pairs SET {sets} WHERE id = $1",
                row_id, *vals,
            )
    except Exception as exc:
        logger.error(f"hedge_pairs UPDATE id={row_id} cols={cols} failed: {exc}")


# ─────────────────────────────────────────────
# Fill-poll helper
# ─────────────────────────────────────────────

_TERMINAL_BAD = {"REJECTED", "CANCELLED"}


async def _poll_until_complete(
    order_ids:    list[str],
    max_attempts: int   = 10,
    interval:     float = 1.0,
) -> tuple[str, dict]:
    """Poll get_order_status for every order until all reach a terminal state.

    Attempt budget: only iterations that receive at least one non-ERROR
    response count against max_attempts. A separate hard cap (max_attempts * 3
    total iterations) prevents an infinite loop if the broker status endpoint is
    persistently unreachable.

    Returns:
        ("complete",  filled_map)  — all orders COMPLETE
        ("rejected",  filled_map)  — at least one REJECTED / CANCELLED
        ("timeout",   filled_map)  — budget exhausted before all complete
    filled_map: {order_id: {"qty": int, "avg_price": float}} for confirmed fills.
    """
    filled_map:   dict = {}
    real_attempts = 0
    total_iters   = 0
    hard_cap      = max_attempts * 3

    while real_attempts < max_attempts and total_iters < hard_cap:
        await asyncio.sleep(interval)
        total_iters += 1

        got_real = False   # True if ≥1 non-ERROR response received this iteration
        any_bad  = False

        for oid in order_ids:
            if oid in filled_map:
                continue   # already confirmed complete

            snap   = await kite1.get_order_status(oid, KITE_API_KEY, KITE_ACCESS_TOKEN)
            status = snap.get("status", "")

            if status == "ERROR":
                continue   # transient network blip — don't count against budget

            got_real = True

            if status == "COMPLETE":
                filled_map[oid] = {
                    "qty":       int(snap.get("filled_quantity", 0)),
                    "avg_price": float(snap.get("average_price", 0)),
                }
            elif status in _TERMINAL_BAD:
                any_bad = True

        if got_real:
            real_attempts += 1

        if any_bad:
            return "rejected", filled_map

        if all(oid in filled_map for oid in order_ids):
            return "complete", filled_map

    return "timeout", filled_map


# ─────────────────────────────────────────────
# Unwind helpers
# ─────────────────────────────────────────────

def _weighted_avg(placed: list, filled_map: dict) -> Optional[float]:
    """Quantity-weighted average fill price across confirmed slices."""
    total_val = 0.0
    total_qty = 0
    for chunk in placed:
        info = filled_map.get(chunk["order_id"])
        if info:
            total_val += info["qty"] * info["avg_price"]
            total_qty += info["qty"]
    return round(total_val / total_qty, 2) if total_qty else None


async def _unwind_filled(
    placed:     list,
    filled_map: dict,
    exchange:   str,
    symbol:     str,
    pool,
    row_id:     Optional[int],
) -> list[str]:
    """Market-sell only the confirmed-filled quantity of a hedge BUY.

    Handles all edge cases cleanly:
    - empty placed (first chunk failed before any order was sent): filled_qty=0, returns []
    - empty filled_map (no fills confirmed yet): filled_qty=0, returns []
    - partial filled_map (some chunks filled, others didn't): unwinds only filled qty
    In all cases the DB row still gets its status written by the caller; this
    function only writes unwind_order_ids.
    """
    filled_qty = sum(
        filled_map[chunk["order_id"]]["qty"]
        for chunk in placed
        if chunk["order_id"] in filled_map
    )
    if filled_qty <= 0:
        logger.info(f"[hedge:{row_id}] Nothing confirmed-filled to unwind for {symbol}")
        await _db_update(pool, row_id, unwind_order_ids=[])
        return []

    logger.info(f"[hedge:{row_id}] Unwinding {filled_qty} {symbol} at MARKET")
    unwind_ids: list[str] = []
    try:
        unwind_placed = await _place_market_order_chunked(exchange, symbol, "SELL", filled_qty)
        unwind_ids    = [o["order_id"] for o in unwind_placed]
    except _ChunkedOrderError as exc:
        unwind_ids = [o["order_id"] for o in exc.placed]
        logger.error(f"[hedge:{row_id}] Partial unwind failure ({symbol} qty={filled_qty}): {exc}")
    except Exception as exc:
        logger.error(f"[hedge:{row_id}] Unwind error ({symbol} qty={filled_qty}): {exc}")

    # Write immediately so a subsequent failure still leaves an audit trail
    await _db_update(pool, row_id, unwind_order_ids=unwind_ids)
    return unwind_ids


# ─────────────────────────────────────────────
# Sell-leg helper
# Mirrors /api/sl/convert-to-market and
# /api/sl/convert-to-limit (server-side sl_state
# lookup) and /api/place-limit-order (new orders).
# ─────────────────────────────────────────────

async def _place_sell(req: HedgedSellRequest) -> dict:
    """Execute the sell leg.

    Returns {"status": "success", "order_id": ..., "order_ids": [...], ...}
         or {"status": "error",   "message": ...}.
    """
    # ── SL-convert path ───────────────────────────────────────────────────
    # Read order IDs server-side from sl_state — exactly the same as
    # /api/sl/convert-to-market and /api/sl/convert-to-limit.
    if req.is_sl_convert:
        state     = sl_state.get(req.sell_symbol)
        order_ids = [o["order_id"] for o in (state or {}).get("sl_orders", [])]
        if not order_ids:
            return {
                "status":  "error",
                "message": f"No active SL orders in sl_state for {req.sell_symbol}",
            }

        convert_type = req.sl_convert_type.upper()
        kite_type    = "MARKET" if convert_type == "M" else "LIMIT"
        limit_price  = _round(req.sell_price) if convert_type == "L" else 0

        results = []
        for oid in order_ids:
            try:
                res = await kite1.hard_code_modify_limit_type(
                    order_id=oid,
                    price=limit_price,
                    trig_price=limit_price,
                    access_token=KITE_ACCESS_TOKEN,
                    api_key=KITE_API_KEY,
                    type=kite_type,
                )
                results.append(res)
            except Exception as exc:
                return {"status": "error", "message": f"SL→{kite_type} failed for {oid}: {exc}"}

        resp: dict = {
            "status":    "success",
            "order_id":  order_ids[0],
            "order_ids": order_ids,
        }
        if convert_type == "L":
            resp["price"] = limit_price
        return resp

    # ── New order path ────────────────────────────────────────────────────
    kite_type_map   = {"L": "LIMIT", "M": "MARKET", "SL": "SL"}
    kite_order_type = kite_type_map.get(req.sell_order_type.upper(), "LIMIT")

    # MARKET: use chunked helper for consistent freeze-qty handling
    if kite_order_type == "MARKET":
        try:
            placed = await _place_market_order_chunked(
                req.sell_exchange, req.sell_symbol, "SELL", req.sell_qty
            )
            return {
                "status":     "success",
                "order_id":   placed[0]["order_id"],
                "order_ids":  [o["order_id"] for o in placed],
                "qty":        req.sell_qty,
                "order_type": "MARKET",
            }
        except _ChunkedOrderError as exc:
            return {"status": "error", "message": str(exc)}

    # LIMIT / SL: build Kite payload, slice by freeze qty
    price = _round(req.sell_price)
    if kite_order_type == "SL":
        trigger = _round(price - req.sl_distance)
        limit   = _round(trigger - req.sl_buffer)
    else:
        trigger = None
        limit   = price

    headers = {
        "X-Kite-Version": "3",
        "User-Agent":      "Kiteconnect-python/5.0.1",
        "Authorization":   f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}",
    }
    base_data: dict = {
        "variety":          "regular",
        "exchange":         req.sell_exchange,
        "tradingsymbol":    req.sell_symbol,
        "transaction_type": "SELL",
        "product":          "NRML",
        "order_type":       kite_order_type,
        "validity":         "DAY",
        "price":            str(limit),
    }
    if kite_order_type == "SL":
        base_data["trigger_price"] = str(trigger)

    freeze    = _freeze_qty(req.sell_symbol)
    remaining = req.sell_qty
    order_ids_out: list[str] = []

    try:
        while remaining > 0:
            chunk = min(remaining, freeze)
            r = await kite1.reqsession.post(
                "https://api.kite.trade/orders/regular",
                data={**base_data, "quantity": str(chunk)},
                headers=headers,
                timeout=7,
            )
            result = r.json()
            if r.status_code != 200:
                msg = result.get("message") or result.get("error") or f"HTTP {r.status_code}"
                logger.error(f"_place_sell broker error: {msg}")
                return {"status": "error", "message": msg}
            order_id = (result.get("data") or {}).get("order_id")
            order_ids_out.append(order_id)
            remaining -= chunk

        return {
            "status":     "success",
            "order_id":   order_ids_out[0],
            "order_ids":  order_ids_out,
            "price":      limit,
            "qty":        req.sell_qty,
            "order_type": kite_order_type,
        }
    except Exception as exc:
        logger.error(f"_place_sell error: {exc}")
        return {"status": "error", "message": str(exc)}


# ─────────────────────────────────────────────
# POST /api/hedge/place-hedged-sell
# ─────────────────────────────────────────────

@router.post("/hedge/place-hedged-sell")
async def place_hedged_sell(req: HedgedSellRequest, request: Request):
    """Sequential hedge gate: BUY → confirm fill → SELL → unwind on sell failure.

    Every outcome returns a structured JSON response. No exception escapes.
    All hedge_pairs DB writes are best-effort — a write failure logs and
    continues so that real-money orders are never blocked by audit machinery.

    Response status values:
      "success"                     — hedge filled + sell placed
      "hedge_rejected"              — hedge BUY rejected or mid-chunk failure; sell blocked
      "hedge_timeout"               — fill poll exhausted; sell blocked
      "sell_rejected_hedge_unwound" — sell failed after hedge filled; unwind placed
      "error"                       — unexpected internal error (safety net)

    All responses include hedge_pair_id (correlates with hedge_pairs table) and
    hedge_order_ids (even on failure, for user-facing debugging).
    """
    pool   = request.app.state.pool
    row_id: Optional[int] = None

    try:
        # ── 1. Insert audit row ───────────────────────────────────────────
        row_id = await _db_insert_hedge_pair(
            pool, req.hedge_symbol, req.sell_symbol, req.hedge_qty
        )
        logger.info(
            f"[hedge:{row_id}] Start: BUY {req.hedge_qty} {req.hedge_symbol} "
            f"→ SELL {req.sell_qty} {req.sell_symbol}"
        )

        # ── 2. Place hedge BUY (chunked) ──────────────────────────────────
        try:
            hedge_placed = await _place_market_order_chunked(
                req.hedge_exchange, req.hedge_symbol, "BUY", req.hedge_qty
            )
        except _ChunkedOrderError as exc:
            logger.error(f"[hedge:{row_id}] Hedge BUY mid-chunk failure: {exc}")
            partial_ids = [o["order_id"] for o in exc.placed]
            # Poll the already-placed chunks to see what actually filled before unwinding.
            # If exc.placed is empty (failed on very first chunk), the poll is a no-op
            # and unwind_ids will be [] — DB still gets status='hedge_rejected'.
            if partial_ids:
                _, filled_map = await _poll_until_complete(partial_ids, max_attempts=5)
                unwind_ids    = await _unwind_filled(
                    exc.placed, filled_map, req.hedge_exchange, req.hedge_symbol, pool, row_id
                )
            else:
                unwind_ids = []
                await _db_update(pool, row_id, unwind_order_ids=[])
            await _db_update(
                pool, row_id,
                status          = "hedge_rejected",
                hedge_order_ids = partial_ids,
                notes           = str(exc),
            )
            return {
                "status":          "hedge_rejected",
                "message":         str(exc),
                "hedge_pair_id":   row_id,
                "hedge_order_ids": partial_ids,
            }

        hedge_order_ids = [o["order_id"] for o in hedge_placed]
        logger.info(f"[hedge:{row_id}] Hedge BUY placed: {hedge_order_ids}")

        # ── 3. Persist order IDs ──────────────────────────────────────────
        await _db_update(pool, row_id, hedge_order_ids=hedge_order_ids)

        # ── 4. Poll for fill ──────────────────────────────────────────────
        fill_outcome, filled_map = await _poll_until_complete(
            hedge_order_ids, max_attempts=10
        )
        logger.info(
            f"[hedge:{row_id}] Poll outcome={fill_outcome} "
            f"confirmed={list(filled_map.keys())}"
        )

        if fill_outcome in ("rejected", "timeout"):
            unwind_ids = await _unwind_filled(
                hedge_placed, filled_map,
                req.hedge_exchange, req.hedge_symbol, pool, row_id,
            )
            db_status      = "hedge_rejected" if fill_outcome == "rejected" else "hedge_timeout"
            response_status = db_status
            await _db_update(
                pool, row_id,
                status = db_status,
                notes  = f"poll outcome: {fill_outcome}",
            )
            return {
                "status":          response_status,
                "message":         f"Hedge fill {fill_outcome} — sell blocked",
                "hedge_pair_id":   row_id,
                "hedge_order_ids": hedge_order_ids,
            }

        # ── 5. Weighted average fill price ────────────────────────────────
        avg_hedge_price = _weighted_avg(hedge_placed, filled_map)
        await _db_update(pool, row_id, avg_hedge_price=avg_hedge_price)
        logger.info(f"[hedge:{row_id}] Hedge confirmed @ avg {avg_hedge_price}")

        # ── 6. Place the sell ─────────────────────────────────────────────
        sell_result = await _place_sell(req)
        logger.info(
            f"[hedge:{row_id}] Sell: status={sell_result.get('status')} "
            f"order_id={sell_result.get('order_id')}"
        )

        if sell_result.get("status") != "success":
            # ── 7. Sell rejected → unwind full hedge qty ──────────────────
            sell_err = sell_result.get("message", "sell rejected")
            logger.error(f"[hedge:{row_id}] Sell failed ({sell_err}) — unwinding hedge")

            unwind_ids: list[str] = []
            try:
                unwind_placed = await _place_market_order_chunked(
                    req.hedge_exchange, req.hedge_symbol, "SELL", req.hedge_qty
                )
                unwind_ids = [o["order_id"] for o in unwind_placed]
            except _ChunkedOrderError as exc:
                unwind_ids = [o["order_id"] for o in exc.placed]
                logger.error(f"[hedge:{row_id}] Unwind partial failure: {exc}")
            except Exception as exc:
                logger.error(f"[hedge:{row_id}] Unwind error: {exc}")

            await _db_update(
                pool, row_id,
                status           = "sell_rejected",
                unwind_order_ids = unwind_ids,
                notes            = f"sell error: {sell_err}",
            )
            return {
                "status":           "sell_rejected_hedge_unwound",
                "message":          sell_err,
                "hedge_pair_id":    row_id,
                "hedge_order_ids":  hedge_order_ids,
                "unwind_order_ids": unwind_ids,
            }

        # ── 8. Success ────────────────────────────────────────────────────
        await _db_update(
            pool, row_id,
            status        = "complete",
            sell_order_id = sell_result.get("order_id"),
            sell_qty      = req.sell_qty,
        )
        logger.info(
            f"[hedge:{row_id}] Complete. sell_order={sell_result.get('order_id')}"
        )
        return {
            "status":          "success",
            "hedge_pair_id":   row_id,
            "hedge_order_ids": hedge_order_ids,
            "avg_hedge_price": avg_hedge_price,
            "sell_order_id":   sell_result.get("order_id"),
            "sell_order_ids":  sell_result.get("order_ids"),
        }

    except Exception as exc:
        # Safety net — structured response even for unexpected errors
        logger.exception(f"[hedge:{row_id}] Unhandled error in place_hedged_sell: {exc}")
        await _db_update(pool, row_id, status="error", notes=str(exc))
        return {
            "status":        "error",
            "message":       f"Internal error: {exc}",
            "hedge_pair_id": row_id,
        }
