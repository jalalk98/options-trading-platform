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

    # Contracts per lot (e.g. NIFTY = 65). Enables the proactive margin cap on
    # the sell leg: sell_qty is reduced to what available funds allow before
    # placing. 0 disables capping (legacy callers).
    lot_size: int = 0

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


class HedgeMaxQtyRequest(BaseModel):
    hedge_symbol:   str
    hedge_exchange: str = "NFO"
    sell_symbol:    str
    sell_exchange:  str = "NFO"
    # Contracts per lot for the underlying index (e.g. NIFTY = 75)
    lot_size:       int

    @field_validator("lot_size")
    @classmethod
    def lot_size_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"lot_size must be > 0 (got {v})")
        return v


class SellAffordableRequest(BaseModel):
    sell_symbol:   str
    sell_exchange: str = "NFO"
    lot_size:      int
    # Desired sell qty; response caps it to what funds allow. Omit to get the
    # pure maximum (used for the popup's auto-suggest after a hedge buy).
    requested_qty: Optional[int] = None

    @field_validator("lot_size")
    @classmethod
    def lot_size_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"lot_size must be > 0 (got {v})")
        return v


class HedgeMarginLadderRequest(BaseModel):
    sell_symbol:    str
    sell_exchange:  str = "NFO"
    hedge_exchange: str = "NFO"
    lot_size:       int
    # Hedge candidate symbols; one basket-margin call each, so capped.
    candidates:     list[str]

    @field_validator("lot_size")
    @classmethod
    def lot_size_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError(f"lot_size must be > 0 (got {v})")
        return v

    @field_validator("candidates")
    @classmethod
    def candidates_bounded(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("candidates must not be empty")
        if len(v) > 15:
            raise ValueError(f"max 15 candidates per request (got {len(v)})")
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
            # Chunks placed before the failure are live orders — report them so
            # the caller retries only the remainder, never a duplicate.
            return {
                "status":     "error",
                "message":    str(exc),
                "order_ids":  [o["order_id"] for o in exc.placed],
                "placed_qty": sum(o["qty"] for o in exc.placed),
            }

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
                return {
                    "status":     "error",
                    "message":    msg,
                    "order_ids":  order_ids_out,
                    "placed_qty": req.sell_qty - remaining,
                }
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
        return {
            "status":     "error",
            "message":    str(exc),
            "order_ids":  order_ids_out,
            "placed_qty": req.sell_qty - remaining,
        }


# ─────────────────────────────────────────────
# Affordable-qty helper (proactive margin cap)
# ─────────────────────────────────────────────

async def _sell_margin_total(exchange: str, symbol: str, qty: int) -> float:
    """Margin needed to place SELL qty×symbol given existing positions.

    consider_positions=true makes the broker account for the just-filled hedge
    long, so this returns the post-hedge-benefit (spread) margin.
    """
    data = await kite1.basket_order_margins(
        [{
            "exchange": exchange, "tradingsymbol": symbol,
            "transaction_type": "SELL", "variety": "regular",
            "product": "NRML", "order_type": "MARKET",
            "quantity": qty, "price": 0, "trigger_price": 0,
        }],
        consider_positions=True,
    )
    return float(data["final"]["total"])


async def _affordable_sell_qty(
    exchange: str, symbol: str, requested_qty: int, lot_size: int
) -> tuple[int, dict]:
    """Largest sell qty ≤ requested_qty (lot-multiple cap) that fits available funds.

    Fails OPEN: any error in the margin math returns requested_qty unchanged so
    the order flow behaves exactly as if capping were disabled — the broker
    remains the final authority.

    Returns (qty, info) where info carries the numbers for logging/response.
    """
    try:
        margins   = await kite1.get_user_margins(segment="equity")
        available = float(margins["net"])

        per_lot = await _sell_margin_total(exchange, symbol, lot_size)
        if per_lot <= 0:
            # Zero margin = closing an existing long (square-off) or broker quirk:
            # nothing to cap.
            return requested_qty, {"available": available, "per_lot": per_lot}

        max_lots = int(available // per_lot)
        info     = {"available": round(available, 2), "per_lot": round(per_lot, 2),
                    "max_lots": max_lots}

        if max_lots * lot_size >= requested_qty:
            return requested_qty, info   # full size fits — no cap

        # Verify the estimate at actual size; margin is near-linear, not exact.
        for _ in range(3):
            if max_lots < 1:
                return 0, info
            required = await _sell_margin_total(exchange, symbol, max_lots * lot_size)
            if required <= available:
                break
            max_lots = min(int(available // (required / max_lots)), max_lots - 1)
        info["max_lots"] = max_lots
        return max(max_lots, 0) * lot_size, info

    except Exception as exc:
        logger.error(f"_affordable_sell_qty failed for {symbol}: {exc} — failing open")
        return requested_qty, {"error": str(exc)}


# ─────────────────────────────────────────────
# POST /api/hedge/place-hedged-sell
# ─────────────────────────────────────────────

@router.post("/hedge/place-hedged-sell")
async def place_hedged_sell(req: HedgedSellRequest, request: Request):
    """Sequential hedge gate: BUY → confirm fill → margin-cap the sell → SELL.

    After the hedge fills, the sell qty is proactively capped to what available
    funds allow (broker basket margin, hedge benefit included). If the sell is
    still rejected, the affordable qty is recomputed once and the remainder
    retried at that size. The hedge is NEVER auto-unwound on sell failure — it
    stays in the account. (Unwind still applies when the hedge BUY itself
    fails to fill: partial hedge fills are sold back.)

    Every outcome returns a structured JSON response. No exception escapes.
    All hedge_pairs DB writes are best-effort — a write failure logs and
    continues so that real-money orders are never blocked by audit machinery.

    Response status values:
      "success"                  — hedge filled + sell placed (qty_reduced flags a cap)
      "hedge_rejected"           — hedge BUY rejected or mid-chunk failure; sell blocked
      "hedge_timeout"            — fill poll exhausted; sell blocked
      "sell_rejected_hedge_kept" — sell placed 0 qty (blocked/rejected); hedge kept
      "sell_partial_hedge_kept"  — some sell qty went live, remainder failed; hedge kept
      "error"                    — unexpected internal error (safety net)

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

        # ── 6. Proactive margin cap on the sell leg ───────────────────────
        # With the hedge now held, ask the broker how much SELL actually fits
        # in available funds and shrink the order instead of letting it bounce.
        requested_qty = req.sell_qty
        can_cap       = not req.is_sl_convert and req.lot_size > 0
        if can_cap:
            affordable, cap_info = await _affordable_sell_qty(
                req.sell_exchange, req.sell_symbol, req.sell_qty, req.lot_size
            )
            if affordable <= 0:
                logger.error(
                    f"[hedge:{row_id}] Funds cover 0 lots — sell not placed, "
                    f"hedge kept ({cap_info})"
                )
                await _db_update(
                    pool, row_id,
                    status = "sell_rejected",
                    notes  = f"0 lots affordable, hedge kept: {cap_info}",
                )
                return {
                    "status":          "sell_rejected_hedge_kept",
                    "message":         (f"Funds cover 0 lots of {req.sell_symbol} "
                                        f"(₹{cap_info.get('available')} free, "
                                        f"₹{cap_info.get('per_lot')}/lot) — hedge kept"),
                    "hedge_pair_id":   row_id,
                    "hedge_order_ids": hedge_order_ids,
                }
            if affordable < requested_qty:
                logger.info(
                    f"[hedge:{row_id}] Sell qty capped {requested_qty} → "
                    f"{affordable} ({cap_info})"
                )
                req.sell_qty = affordable

        # ── 7. Place the sell ─────────────────────────────────────────────
        sell_result = await _place_sell(req)
        logger.info(
            f"[hedge:{row_id}] Sell: status={sell_result.get('status')} "
            f"order_id={sell_result.get('order_id')}"
        )

        if sell_result.get("status") == "success":
            placed_qty = req.sell_qty
            placed_ids = list(sell_result.get("order_ids") or [])
        else:
            placed_qty = int(sell_result.get("placed_qty") or 0)
            placed_ids = [i for i in (sell_result.get("order_ids") or []) if i]

        # ── 7b. Rejected → recompute affordable qty once, retry remainder ─
        # Only the qty NOT already live is retried, and only at a smaller
        # size (same size would bounce again — the rejection wasn't margin).
        if sell_result.get("status") != "success" and can_cap:
            remaining = req.sell_qty - placed_qty
            if remaining > 0:
                retry_qty, retry_info = await _affordable_sell_qty(
                    req.sell_exchange, req.sell_symbol, remaining, req.lot_size
                )
                if 0 < retry_qty < remaining:
                    logger.info(
                        f"[hedge:{row_id}] Sell rejected — retrying remainder "
                        f"{remaining} at affordable {retry_qty} ({retry_info})"
                    )
                    req.sell_qty = retry_qty
                    sell_result  = await _place_sell(req)
                    if sell_result.get("status") == "success":
                        placed_qty += retry_qty
                        placed_ids += list(sell_result.get("order_ids") or [])
                    else:
                        placed_qty += int(sell_result.get("placed_qty") or 0)
                        placed_ids += [i for i in (sell_result.get("order_ids") or []) if i]

        if sell_result.get("status") != "success":
            # ── 7c. Sell failed → hedge KEPT (no auto-unwind) ─────────────
            sell_err = sell_result.get("message", "sell rejected")
            logger.error(
                f"[hedge:{row_id}] Sell failed ({sell_err}) — hedge kept, "
                f"placed {placed_qty}/{requested_qty}"
            )
            await _db_update(
                pool, row_id,
                status = "sell_rejected",
                notes  = (f"sell error: {sell_err}; hedge kept; "
                          f"placed {placed_qty}/{requested_qty}"),
            )
            return {
                "status":             ("sell_partial_hedge_kept" if placed_qty > 0
                                       else "sell_rejected_hedge_kept"),
                "message":            sell_err,
                "hedge_pair_id":      row_id,
                "hedge_order_ids":    hedge_order_ids,
                "sell_order_ids":     placed_ids,
                "sell_placed_qty":    placed_qty,
                "requested_sell_qty": requested_qty,
            }

        # ── 8. Success ────────────────────────────────────────────────────
        qty_reduced = placed_qty < requested_qty
        await _db_update(
            pool, row_id,
            status        = "complete",
            sell_order_id = sell_result.get("order_id"),
            sell_qty      = placed_qty,
            **({"notes": f"sell qty auto-capped {requested_qty} → {placed_qty}"}
               if qty_reduced else {}),
        )
        logger.info(
            f"[hedge:{row_id}] Complete. sell_order={sell_result.get('order_id')} "
            f"qty={placed_qty}/{requested_qty}"
        )
        return {
            "status":             "success",
            "hedge_pair_id":      row_id,
            "hedge_order_ids":    hedge_order_ids,
            "avg_hedge_price":    avg_hedge_price,
            "sell_order_id":      sell_result.get("order_id"),
            "sell_order_ids":     placed_ids,
            "sell_qty":           placed_qty,
            "requested_sell_qty": requested_qty,
            "qty_reduced":        qty_reduced,
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


# ─────────────────────────────────────────────
# POST /api/hedge/max-qty
# ─────────────────────────────────────────────

def _basket_orders(req: HedgeMaxQtyRequest, qty: int) -> list[dict]:
    """Hedge BUY + SELL pair as a basket-margin payload (mirrors the MARKET/NRML
    orders that place_hedged_sell actually sends)."""
    common = {"variety": "regular", "product": "NRML", "order_type": "MARKET",
              "quantity": qty, "price": 0, "trigger_price": 0}
    return [
        {"exchange": req.hedge_exchange, "tradingsymbol": req.hedge_symbol,
         "transaction_type": "BUY", **common},
        {"exchange": req.sell_exchange, "tradingsymbol": req.sell_symbol,
         "transaction_type": "SELL", **common},
    ]


async def _basket_margin_total(req: HedgeMaxQtyRequest, qty: int) -> float:
    """Post-hedge-benefit margin (span + exposure + option premium) for the pair."""
    data = await kite1.basket_order_margins(_basket_orders(req, qty))
    return float(data["final"]["total"])


@router.post("/hedge/max-qty")
async def hedge_max_qty(req: HedgeMaxQtyRequest):
    """How many lots of the hedged pair (BUY hedge + SELL) fit in available funds.

    Basket margin is queried for 1 lot to get a per-lot estimate, then re-checked
    at the estimated size — hedged-spread margin is near-linear in lots but not
    exactly, so the estimate is stepped down until it fits (bounded iterations).
    Purely informational: no orders are placed.
    """
    try:
        margins   = await kite1.get_user_margins(segment="equity")
        available = float(margins["net"])

        per_lot = await _basket_margin_total(req, req.lot_size)
        if per_lot <= 0:
            return {"status": "error",
                    "message": f"Broker returned margin {per_lot} for 1 lot"}

        est = int(available // per_lot)
        if est < 1:
            return {
                "status":          "success",
                "max_lots":        0,
                "max_qty":         0,
                "margin_per_lot":  round(per_lot, 2),
                "available_cash":  round(available, 2),
                "required_at_max": 0,
            }

        # Verify at the estimate; shrink until it actually fits.
        required = per_lot
        if est > 1:
            for _ in range(5):
                required = await _basket_margin_total(req, est * req.lot_size)
                if required <= available:
                    break
                shrunk = int(available // (required / est))
                est    = min(shrunk, est - 1)
                if est < 1:
                    est, required = 0, 0.0
                    break

        return {
            "status":          "success",
            "max_lots":        est,
            "max_qty":         est * req.lot_size,
            "margin_per_lot":  round(required / est, 2) if est else round(per_lot, 2),
            "available_cash":  round(available, 2),
            "required_at_max": round(required, 2),
        }

    except Exception as exc:
        logger.error(f"hedge/max-qty failed for {req.hedge_symbol}/{req.sell_symbol}: {exc}")
        return {"status": "error", "message": str(exc)}


# ─────────────────────────────────────────────
# POST /api/hedge/sell-affordable
# ─────────────────────────────────────────────

@router.post("/hedge/sell-affordable")
async def hedge_sell_affordable(req: SellAffordableRequest):
    """Max SELL qty that fits current funds given positions ALREADY held.

    Unlike /api/hedge/max-qty (which prices the hedge BUY + SELL as a fresh
    pair), this uses consider_positions on the SELL alone — so after a
    standalone hedge buy, the held hedge provides the spread benefit. Powers
    the popup's SELL-only margin cap and the auto-suggested sell qty.
    """
    # A huge default lets the verification loop find the pure maximum.
    requested = req.requested_qty or req.lot_size * 10000
    qty, info = await _affordable_sell_qty(
        req.sell_exchange, req.sell_symbol, requested, req.lot_size
    )
    if "error" in info:
        # _affordable_sell_qty fails open (returns requested) — that's right
        # for the order path, but a suggestion must never echo the huge default.
        return {"status": "error", "message": info["error"]}
    per_lot = info.get("per_lot") or 0
    if req.requested_qty is None and per_lot <= 0:
        # Zero margin (e.g. sell closes an existing long) — no finite maximum.
        return {"status": "error",
                "message": f"margin per lot came back {per_lot} — no finite max"}
    return {
        "status":         "success",
        "qty":            qty,
        "lots":           qty // req.lot_size,
        "available_cash": info.get("available"),
        "per_lot":        info.get("per_lot"),
    }


# ─────────────────────────────────────────────
# POST /api/hedge/order-status
# ─────────────────────────────────────────────

class OrderStatusRequest(BaseModel):
    order_ids: list[str]

    @field_validator("order_ids")
    @classmethod
    def ids_bounded(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("order_ids must not be empty")
        if len(v) > 20:
            raise ValueError(f"max 20 order ids per request (got {len(v)})")
        return v


@router.post("/hedge/order-status")
async def hedge_order_status(req: OrderStatusRequest):
    """Broker status snapshot for a set of orders (powers the popup fill ticks)."""
    async def one(oid: str):
        snap = await kite1.get_order_status(oid, KITE_API_KEY, KITE_ACCESS_TOKEN)
        return oid, {
            "status":          snap.get("status", "ERROR"),
            "filled_quantity": int(snap.get("filled_quantity", 0) or 0),
            "average_price":   float(snap.get("average_price", 0) or 0),
        }
    orders = dict(await asyncio.gather(*(one(o) for o in req.order_ids)))
    return {"status": "success", "orders": orders}


# ─────────────────────────────────────────────
# POST /api/hedge/margin-ladder
# ─────────────────────────────────────────────

@router.post("/hedge/margin-ladder")
async def hedge_margin_ladder(req: HedgeMarginLadderRequest):
    """Per-candidate hedged margin for the popup ladder.

    For each hedge candidate: 1-lot basket margin of (BUY candidate + SELL
    sell_symbol) and the estimated lots that fit in available cash. Estimates
    only (no step-down verification — /api/hedge/max-qty verifies the selected
    pair). Failed candidates return null instead of failing the batch.
    """
    try:
        margins   = await kite1.get_user_margins(segment="equity")
        available = float(margins["net"])
    except Exception as exc:
        logger.error(f"hedge/margin-ladder margins fetch failed: {exc}")
        return {"status": "error", "message": str(exc)}

    sem = asyncio.Semaphore(3)   # stay well under broker API rate limits

    async def one(hedge_sym: str):
        async with sem:
            try:
                pair = HedgeMaxQtyRequest(
                    hedge_symbol=hedge_sym, hedge_exchange=req.hedge_exchange,
                    sell_symbol=req.sell_symbol, sell_exchange=req.sell_exchange,
                    lot_size=req.lot_size,
                )
                per_lot = await _basket_margin_total(pair, req.lot_size)
                if per_lot <= 0:
                    return hedge_sym, None
                return hedge_sym, {
                    "per_lot":  round(per_lot, 2),
                    "max_lots": int(available // per_lot),
                }
            except Exception as exc:
                logger.warning(f"hedge/margin-ladder {hedge_sym}: {exc}")
                return hedge_sym, None

    results = dict(await asyncio.gather(*(one(s) for s in req.candidates)))
    return {
        "status":         "success",
        "available_cash": round(available, 2),
        "results":        results,
    }
