import asyncio
import time
import logging
import datetime
import httpx
from collections import deque
from typing import Optional

import backend.state as state
from backend.core.custom_connect import KiteConnect_custom
from backend.api.streaming import manager
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN

logger = logging.getLogger(__name__)

# symbol → asyncio.Task
_active_tasks: dict = {}

# DB pool — injected by chart_server on startup via set_pool()
_pool = None


def set_pool(pool) -> None:
    global _pool
    _pool = pool


def _get_exchange(symbol: str) -> str:
    return "BFO" if symbol.startswith("SENSEX") else "NFO"


def _trigger_buffer(symbol: str) -> float:
    return 1.75 if symbol.startswith("SENSEX") else 0.85


def _extract_order_id(result) -> Optional[str]:
    if isinstance(result, dict):
        data = result.get("data") or {}
        return data.get("order_id")
    return None


def _ist_now() -> datetime.datetime:
    IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
    return datetime.datetime.now(IST)


def _is_market_open() -> bool:
    now = _ist_now()
    if now.weekday() >= 5:
        return False
    hhmm = now.hour * 100 + now.minute
    return 915 <= hhmm < 1530


# ── Order helpers ─────────────────────────────────────────────────────────────

async def _cancel_order(kite, order_id: str, symbol: str, reason: str) -> None:
    logger.info("[RSNIP] CANCEL order_id=%s symbol=%s reason=%s", order_id, symbol, reason)
    try:
        await kite.hard_code_regular_cancel_order(order_id, KITE_ACCESS_TOKEN, KITE_API_KEY)
        logger.info("[RSNIP] CANCEL_OK order_id=%s symbol=%s", order_id, symbol)
    except Exception as e:
        logger.error("[RSNIP] CANCEL_FAILED order_id=%s symbol=%s error=%s", order_id, symbol, e)


async def _place_slb(kite, symbol: str, exchange: str,
                     trigger: float, limit: float, qty: int) -> Optional[str]:
    logger.info(
        "[RSNIP] PLACE_SLB symbol=%s exchange=%s trigger=%.2f limit=%.2f qty=%d",
        symbol, exchange, trigger, limit, qty,
    )
    try:
        result = await kite.hard_code_regular_buy_order(
            exchange=exchange, trade_symbol=symbol,
            qty=qty, price=limit, trig_price=trigger,
            api_key=KITE_API_KEY, access_token=KITE_ACCESS_TOKEN,
        )
        oid = _extract_order_id(result)
        if oid:
            logger.info("[RSNIP] PLACE_SLB_OK symbol=%s order_id=%s trigger=%.2f", symbol, oid, trigger)
        else:
            logger.warning("[RSNIP] PLACE_SLB_NO_OID symbol=%s result=%s", symbol, result)
        return oid
    except Exception as e:
        logger.error("[RSNIP] PLACE_SLB_EXCEPTION symbol=%s error=%s", symbol, e, exc_info=True)
        return None


async def _place_sl(kite, symbol: str, exchange: str,
                    sl_price: float, trig_buf: float, qty: int) -> Optional[str]:
    trigger = round(sl_price + trig_buf, 2)
    logger.info(
        "[RSNIP] PLACE_SL symbol=%s sl_price=%.2f trigger=%.2f qty=%d",
        symbol, sl_price, trigger, qty,
    )
    try:
        result = await kite.hard_code_regular_sell_order(
            exchange=exchange, trade_symbol=symbol,
            qty=qty, stop_loss_price=sl_price, trig_price=trigger,
            api_key=KITE_API_KEY, access_token=KITE_ACCESS_TOKEN,
        )
        oid = _extract_order_id(result)
        if oid:
            logger.info("[RSNIP] PLACE_SL_OK symbol=%s order_id=%s sl=%.2f", symbol, oid, sl_price)
        else:
            logger.warning("[RSNIP] PLACE_SL_NO_OID symbol=%s result=%s", symbol, result)
        return oid
    except Exception as e:
        logger.error("[RSNIP] PLACE_SL_EXCEPTION symbol=%s error=%s", symbol, e, exc_info=True)
        return None


async def _modify_slb(kite, slb_order_id: str, symbol: str,
                      new_trigger: float, new_limit: float) -> bool:
    logger.info(
        "[RSNIP] MODIFY_SLB symbol=%s order_id=%s new_trigger=%.2f new_limit=%.2f",
        symbol, slb_order_id, new_trigger, new_limit,
    )
    try:
        result = await kite.hard_code_regular_modify_order(
            order_id=slb_order_id,
            price=new_limit,
            trig_price=new_trigger,
            access_token=KITE_ACCESS_TOKEN,
            api_key=KITE_API_KEY,
        )
        ok = isinstance(result, dict) and result.get("status") == "success"
        if ok:
            logger.info("[RSNIP] MODIFY_SLB_OK symbol=%s order_id=%s trigger=%.2f", symbol, slb_order_id, new_trigger)
        else:
            logger.warning("[RSNIP] MODIFY_SLB_FAILED symbol=%s order_id=%s result=%s", symbol, slb_order_id, result)
        return ok
    except Exception as e:
        logger.error("[RSNIP] MODIFY_SLB_EXCEPTION symbol=%s error=%s", symbol, e, exc_info=True)
        return False


async def _modify_sl(kite, sl_order_id: str, symbol: str,
                     new_sl: float, trig_buf: float) -> bool:
    new_trigger = round(new_sl + trig_buf, 2)
    logger.info(
        "[RSNIP] MODIFY_SL symbol=%s order_id=%s new_sl=%.2f new_trigger=%.2f",
        symbol, sl_order_id, new_sl, new_trigger,
    )
    try:
        result = await kite.hard_code_regular_modify_order(
            order_id=sl_order_id,
            price=new_sl,
            trig_price=new_trigger,
            access_token=KITE_ACCESS_TOKEN,
            api_key=KITE_API_KEY,
        )
        ok = isinstance(result, dict) and result.get("status") == "success"
        if ok:
            logger.info("[RSNIP] MODIFY_SL_OK symbol=%s order_id=%s new_sl=%.2f", symbol, sl_order_id, new_sl)
        else:
            logger.warning("[RSNIP] MODIFY_SL_FAILED symbol=%s order_id=%s result=%s", symbol, sl_order_id, result)
        return ok
    except Exception as e:
        logger.error("[RSNIP] MODIFY_SL_EXCEPTION symbol=%s error=%s", symbol, e, exc_info=True)
        return False


async def _get_order_status(kite, order_id: str, symbol: str) -> str:
    try:
        data = await kite.get_order_status(order_id, KITE_API_KEY, KITE_ACCESS_TOKEN)
        return data.get("status", "UNKNOWN")
    except Exception as e:
        logger.error("[RSNIP] STATUS_EXCEPTION symbol=%s order_id=%s error=%s", symbol, order_id, e)
        return "ERROR"


async def _get_fill_price(kite, order_id: str, symbol: str) -> Optional[float]:
    try:
        data = await kite.get_order_status(order_id, KITE_API_KEY, KITE_ACCESS_TOKEN)
        price = data.get("average_price")
        logger.info("[RSNIP] FILL_PRICE symbol=%s order_id=%s avg_price=%s", symbol, order_id, price)
        return float(price) if price else None
    except Exception as e:
        logger.error("[RSNIP] FILL_PRICE_EXCEPTION symbol=%s order_id=%s error=%s", symbol, order_id, e)
        return None


async def _get_net_position(symbol: str) -> Optional[int]:
    headers = {
        "X-Kite-Version": "3",
        "User-Agent": "Kiteconnect-python/5.0.1",
        "Authorization": f"token {KITE_API_KEY}:{KITE_ACCESS_TOKEN}",
    }
    try:
        async with httpx.AsyncClient() as client:
            r = await client.get(
                "https://api.kite.trade/portfolio/positions",
                headers=headers, timeout=7,
            )
            r.raise_for_status()
            data = r.json()
            for pos in data.get("data", {}).get("day", []):
                if pos.get("tradingsymbol") == symbol:
                    qty = int(pos.get("quantity", 0))
                    logger.info("[RSNIP] POSITION symbol=%s net_qty=%d", symbol, qty)
                    return qty
            logger.info("[RSNIP] POSITION symbol=%s net_qty=0 (not in list)", symbol)
            return 0
    except Exception as e:
        logger.error("[RSNIP] POSITION_FETCH_ERROR symbol=%s error=%s", symbol, e)
        return None


# ── DB persistence ────────────────────────────────────────────────────────────

async def _save_trade(
    symbol: str, config: dict,
    entry_price: float, entry_time: datetime.datetime,
    exit_price: float, exit_time: datetime.datetime,
    spike_low: float, close_reason: str, sl_at_cost: bool,
) -> None:
    if _pool is None:
        logger.error("[RSNIP] SAVE_TRADE_SKIP symbol=%s — pool not set", symbol)
        return

    qty     = int(config["qty"])
    pnl_pts = round(exit_price - entry_price, 2)
    pnl_inr = round(pnl_pts * qty, 2)

    logger.info(
        "[RSNIP] SAVE_TRADE symbol=%s entry=%.2f exit=%.2f pnl_pts=%.2f pnl_inr=%.2f reason=%s",
        symbol, entry_price, exit_price, pnl_pts, pnl_inr, close_reason,
    )
    try:
        async with _pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO reverse_snipper_trades (
                    symbol, entry_price, entry_time, exit_price, exit_time,
                    pnl_pts, pnl_inr, close_reason, spike_low,
                    spike_distance, spike_window_secs, recovery_buffer, sl_buffer,
                    lookback_secs, spike_timeout_secs, qty, cooldown_secs,
                    auto_increment_sl, increment_step, increment_interval_secs, sl_at_cost
                ) VALUES (
                    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21
                )
                """,
                symbol,
                entry_price, entry_time, exit_price, exit_time,
                pnl_pts, pnl_inr, close_reason, spike_low,
                float(config["spike_distance"]),
                float(config["spike_window_secs"]),
                float(config["recovery_buffer"]),
                float(config["sl_buffer"]),
                max(float(config["spike_window_secs"]) * 3, 10.0),  # derived
                float(config["spike_timeout_secs"]),
                qty,
                float(config["cooldown_secs"]),
                bool(config.get("auto_increment_sl", False)),
                float(config.get("increment_step", 2)),
                float(config.get("increment_interval_secs", 0.25)),
                sl_at_cost,
            )
        logger.info("[RSNIP] SAVE_TRADE_OK symbol=%s", symbol)
    except Exception as e:
        logger.error("[RSNIP] SAVE_TRADE_ERROR symbol=%s error=%s", symbol, e, exc_info=True)


async def _broadcast_trade_closed(
    symbol: str, config: dict,
    entry_price: float, entry_time: datetime.datetime,
    exit_price: float, exit_time: datetime.datetime,
    spike_low: float, close_reason: str, sl_at_cost: bool,
) -> None:
    qty     = int(config["qty"])
    pnl_pts = round(exit_price - entry_price, 2)
    pnl_inr = round(pnl_pts * qty, 2)

    event = {
        "type"                   : "reverse_snipper_trade_closed",
        "symbol"                 : symbol,
        "entry_price"            : entry_price,
        "entry_time"             : entry_time.strftime("%H:%M:%S"),
        "exit_price"             : exit_price,
        "exit_time"              : exit_time.strftime("%H:%M:%S"),
        "pnl_pts"                : pnl_pts,
        "pnl_inr"                : pnl_inr,
        "close_reason"           : close_reason,
        "spike_low"              : spike_low,
        "sl_at_cost"             : sl_at_cost,
        "spike_distance"         : float(config["spike_distance"]),
        "spike_window_secs"      : float(config["spike_window_secs"]),
        "recovery_buffer"        : float(config["recovery_buffer"]),
        "sl_buffer"              : float(config["sl_buffer"]),
        "qty"                    : qty,
        "auto_increment_sl"      : bool(config.get("auto_increment_sl", False)),
        "increment_step"         : float(config.get("increment_step", 5)),
        "increment_interval_secs": float(config.get("increment_interval_secs", 30)),
    }
    await manager.broadcast_all(event)
    logger.info(
        "[RSNIP] BROADCAST_CLOSED symbol=%s reason=%s pnl_pts=%.2f pnl_inr=%.2f",
        symbol, close_reason, pnl_pts, pnl_inr,
    )


# ── Core loop ─────────────────────────────────────────────────────────────────

async def _snipper_loop(symbol: str, config: dict) -> None:
    kite      = KiteConnect_custom(KITE_API_KEY)
    kite.set_access_token(KITE_ACCESS_TOKEN)
    exchange  = _get_exchange(symbol)
    trig_buf  = _trigger_buffer(symbol)

    spike_distance          = float(config["spike_distance"])
    spike_window_secs       = float(config["spike_window_secs"])
    recovery_buffer         = float(config["recovery_buffer"])
    sl_buffer               = float(config["sl_buffer"])
    lookback_secs           = max(spike_window_secs * 3, 10.0)  # derived — not user-configurable
    spike_timeout_secs      = float(config["spike_timeout_secs"])
    qty                     = int(config["qty"])
    cooldown_secs           = float(config["cooldown_secs"])
    auto_increment_sl       = bool(config.get("auto_increment_sl", False))
    increment_step          = float(config.get("increment_step", 5))
    increment_interval_secs = float(config.get("increment_interval_secs", 30))

    logger.info(
        "[RSNIP] LOOP_START symbol=%s exchange=%s spike_dist=%.1f spike_win=%.1fs "
        "rec_buf=%.1f sl_buf=%.1f lookback=%.1fs(derived) timeout=%.1fs qty=%d cooldown=%.1fs "
        "auto_sl=%s step=%.1f interval=%.2fs",
        symbol, exchange, spike_distance, spike_window_secs,
        recovery_buffer, sl_buffer, lookback_secs, spike_timeout_secs,
        qty, cooldown_secs, auto_increment_sl, increment_step, increment_interval_secs,
    )

    # Rolling price history — (monotonic_ts, ltp) for up to lookback_secs
    price_history: deque = deque()

    # ── Per-phase state vars ──────────────────────────────────────────────────
    phase = "WATCHING"

    # SPIKE_DETECTED
    spike_low         = None   # running minimum since spike detected (for SL placement)
    recovery_high     = None   # running maximum since spike detected (drives SLB modification)
    spike_detected_at = None
    slb_order_id      = None

    # ENTRY_ACTIVE
    entry_price       = None
    entry_time_ist    = None
    sl_order_id       = None
    current_sl        = None
    last_increment_at = None
    sl_at_cost        = False

    # COOLDOWN
    cooldown_start    = None

    ltp_wait_logged = False
    pre_market_logged = False

    try:
        while True:
            # ── Market close guard ────────────────────────────────────────
            if not _is_market_open():
                now = _ist_now()
                hhmm = now.hour * 100 + now.minute
                # Before market open (e.g. toggled at 09:10): wait for 09:15
                if hhmm < 915 and now.weekday() < 5:
                    if not pre_market_logged:
                        logger.info(
                            "[RSNIP] PRE_MARKET symbol=%s — waiting for 09:15",
                            symbol,
                        )
                        pre_market_logged = True
                    await asyncio.sleep(5)
                    continue
                # After market close or weekend: stop the loop
                logger.info(
                    "[RSNIP] MARKET_CLOSED symbol=%s phase=%s — stopping loop",
                    symbol, phase,
                )
                if slb_order_id:
                    await _cancel_order(kite, slb_order_id, symbol, "MARKET_CLOSE")
                if sl_order_id and phase == "ENTRY_ACTIVE":
                    await _cancel_order(kite, sl_order_id, symbol, "MARKET_CLOSE")
                state.reverse_snipper_state.pop(symbol, None)
                _active_tasks.pop(symbol, None)
                await manager.broadcast_all({"type": "reverse_snipper_stopped", "symbol": symbol, "reason": "MARKET_CLOSED"})
                break

            ltp = state.latest_ltp.get(symbol)
            if ltp is None:
                if not ltp_wait_logged:
                    logger.warning("[RSNIP] WAITING_FOR_LTP symbol=%s", symbol)
                    ltp_wait_logged = True
                await asyncio.sleep(0.2)
                continue
            ltp_wait_logged = False

            now_mono = time.monotonic()
            now_ist  = _ist_now()

            # Maintain rolling price history
            price_history.append((now_mono, ltp))
            while price_history and (now_mono - price_history[0][0]) > lookback_secs:
                price_history.popleft()

            # ── WATCHING ──────────────────────────────────────────────────
            if phase == "WATCHING":
                window = [p for (ts, p) in price_history if (now_mono - ts) <= spike_window_secs]
                if not window:
                    await asyncio.sleep(0.1)
                    continue

                recent_max = max(window)
                drop       = recent_max - ltp

                logger.info(
                    "[RSNIP] WATCH symbol=%s ltp=%.2f recent_max=%.2f drop=%.2f threshold=%.2f",
                    symbol, ltp, recent_max, drop, spike_distance,
                )

                if drop >= spike_distance:
                    spike_low         = ltp
                    recovery_high     = ltp   # will track upward from here
                    spike_detected_at = now_mono
                    slb_trigger       = round(recovery_high + recovery_buffer, 2)
                    slb_limit         = round(slb_trigger + 2.0, 2)

                    logger.warning(
                        "[RSNIP] SPIKE_DETECTED symbol=%s ltp=%.2f recent_max=%.2f "
                        "drop=%.2f spike_low=%.2f slb_trigger=%.2f slb_limit=%.2f",
                        symbol, ltp, recent_max, drop, spike_low, slb_trigger, slb_limit,
                    )

                    oid = await _place_slb(kite, symbol, exchange, slb_trigger, slb_limit, qty)
                    if oid:
                        slb_order_id = oid
                        phase        = "SPIKE_DETECTED"
                        logger.info(
                            "[RSNIP] PHASE→SPIKE_DETECTED symbol=%s slb_order=%s",
                            symbol, slb_order_id,
                        )
                    else:
                        logger.error(
                            "[RSNIP] SLB_PLACE_FAILED symbol=%s — staying WATCHING", symbol,
                        )

                await asyncio.sleep(0.1)

            # ── SPIKE_DETECTED ────────────────────────────────────────────
            elif phase == "SPIKE_DETECTED":
                elapsed = now_mono - spike_detected_at

                # 1. Timeout — cancel SL-B and reset
                if elapsed > spike_timeout_secs:
                    logger.info(
                        "[RSNIP] TIMEOUT symbol=%s elapsed=%.1fs > timeout=%.1fs",
                        symbol, elapsed, spike_timeout_secs,
                    )
                    await _cancel_order(kite, slb_order_id, symbol, "TIMEOUT")
                    slb_order_id      = None
                    spike_low         = None
                    recovery_high     = None
                    spike_detected_at = None
                    phase             = "WATCHING"
                    await asyncio.sleep(0.1)
                    continue

                # 2. Check if SL-B filled → entry triggered
                slb_status = await _get_order_status(kite, slb_order_id, symbol)
                logger.info(
                    "[RSNIP] SLB_STATUS symbol=%s order_id=%s status=%s spike_low=%.2f elapsed=%.1fs",
                    symbol, slb_order_id, slb_status, spike_low, elapsed,
                )

                if slb_status == "COMPLETE":
                    fill_price     = await _get_fill_price(kite, slb_order_id, symbol)
                    entry_price    = fill_price or round(spike_low + recovery_buffer, 2)
                    entry_time_ist = now_ist
                    current_sl     = round(spike_low - sl_buffer, 2)
                    sl_at_cost     = False

                    logger.warning(
                        "[RSNIP] ENTRY_FILLED symbol=%s entry=%.2f spike_low=%.2f sl=%.2f",
                        symbol, entry_price, spike_low, current_sl,
                    )

                    oid = await _place_sl(kite, symbol, exchange, current_sl, trig_buf, qty)
                    if oid:
                        sl_order_id       = oid
                        last_increment_at = now_mono
                        slb_order_id      = None
                        phase             = "ENTRY_ACTIVE"
                        logger.info(
                            "[RSNIP] PHASE→ENTRY_ACTIVE symbol=%s sl_order=%s sl=%.2f",
                            symbol, sl_order_id, current_sl,
                        )
                    else:
                        logger.error(
                            "[RSNIP] SL_PLACE_FAILED symbol=%s — entry filled but no SL placed!",
                            symbol,
                        )
                        slb_order_id = None
                        phase        = "ENTRY_ACTIVE"

                    await asyncio.sleep(1)
                    continue

                # 3. Track new low (for SL placement after entry — does NOT move SLB)
                if ltp < spike_low:
                    spike_low = ltp
                    logger.info(
                        "[RSNIP] NEW_LOW symbol=%s spike_low=%.2f", symbol, spike_low,
                    )

                # 4. Track recovery high — modify SLB upward on every new high
                #    SLB always sits recovery_buffer ahead of the recovery high.
                #    Slow recovery: SLB keeps moving ahead, never fills.
                #    Reversal spike: price jumps past the SLB → fills on exchange.
                if ltp > recovery_high:
                    recovery_high   = ltp
                    new_slb_trigger = round(recovery_high + recovery_buffer, 2)
                    new_slb_limit   = round(new_slb_trigger + 2.0, 2)
                    logger.info(
                        "[RSNIP] RECOVERY_HIGH symbol=%s recovery_high=%.2f new_slb_trigger=%.2f",
                        symbol, recovery_high, new_slb_trigger,
                    )
                    await _modify_slb(kite, slb_order_id, symbol, new_slb_trigger, new_slb_limit)

                await asyncio.sleep(0.5)

            # ── ENTRY_ACTIVE ──────────────────────────────────────────────
            elif phase == "ENTRY_ACTIVE":

                # 1. Time-based SL auto-increment (no LTP dependency)
                if auto_increment_sl and sl_order_id and not sl_at_cost:
                    if (now_mono - last_increment_at) >= increment_interval_secs:
                        new_sl = min(round(current_sl + increment_step, 2), entry_price)
                        if new_sl > current_sl:
                            ok = await _modify_sl(kite, sl_order_id, symbol, new_sl, trig_buf)
                            if ok:
                                logger.warning(
                                    "[RSNIP] SL_INCREMENTED symbol=%s %.2f→%.2f entry=%.2f",
                                    symbol, current_sl, new_sl, entry_price,
                                )
                                current_sl = new_sl
                                if current_sl >= entry_price:
                                    sl_at_cost = True
                                    logger.warning(
                                        "[RSNIP] SL_AT_COST symbol=%s sl=%.2f — no further increments",
                                        symbol, current_sl,
                                    )
                        last_increment_at = now_mono

                # 2. Check if SL order hit
                if sl_order_id:
                    sl_status = await _get_order_status(kite, sl_order_id, symbol)
                    logger.info(
                        "[RSNIP] SL_STATUS symbol=%s order_id=%s status=%s current_sl=%.2f",
                        symbol, sl_order_id, sl_status, current_sl,
                    )

                    if sl_status == "COMPLETE":
                        exit_price = await _get_fill_price(kite, sl_order_id, symbol)
                        exit_price = exit_price or current_sl
                        exit_time  = now_ist

                        logger.warning(
                            "[RSNIP] SL_HIT symbol=%s entry=%.2f exit=%.2f sl_at_cost=%s",
                            symbol, entry_price, exit_price, sl_at_cost,
                        )

                        await _save_trade(
                            symbol, config, entry_price, entry_time_ist,
                            exit_price, exit_time, spike_low, "SL_HIT", sl_at_cost,
                        )
                        await _broadcast_trade_closed(
                            symbol, config, entry_price, entry_time_ist,
                            exit_price, exit_time, spike_low, "SL_HIT", sl_at_cost,
                        )

                        sl_order_id    = None
                        cooldown_start = now_mono
                        phase          = "COOLDOWN"
                        logger.info("[RSNIP] PHASE→COOLDOWN symbol=%s cooldown=%.1fs", symbol, cooldown_secs)
                        await asyncio.sleep(1)
                        continue

                # 3. Manual close — net position dropped to 0
                net_qty = await _get_net_position(symbol)
                if net_qty is not None and net_qty == 0:
                    exit_price = state.latest_ltp.get(symbol) or entry_price
                    exit_time  = now_ist

                    logger.warning(
                        "[RSNIP] MANUAL_EXIT symbol=%s entry=%.2f exit_approx=%.2f",
                        symbol, entry_price, exit_price,
                    )

                    if sl_order_id:
                        await _cancel_order(kite, sl_order_id, symbol, "MANUAL_EXIT")
                        sl_order_id = None

                    await _save_trade(
                        symbol, config, entry_price, entry_time_ist,
                        exit_price, exit_time, spike_low, "MANUAL", sl_at_cost,
                    )
                    await _broadcast_trade_closed(
                        symbol, config, entry_price, entry_time_ist,
                        exit_price, exit_time, spike_low, "MANUAL", sl_at_cost,
                    )

                    cooldown_start = now_mono
                    phase          = "COOLDOWN"
                    logger.info("[RSNIP] PHASE→COOLDOWN symbol=%s cooldown=%.1fs", symbol, cooldown_secs)

                await asyncio.sleep(2)

            # ── COOLDOWN ──────────────────────────────────────────────────
            elif phase == "COOLDOWN":
                elapsed = now_mono - cooldown_start
                if elapsed >= cooldown_secs:
                    logger.info(
                        "[RSNIP] COOLDOWN_DONE symbol=%s elapsed=%.1fs — re-arming WATCHING",
                        symbol, elapsed,
                    )
                    spike_low = recovery_high = spike_detected_at = slb_order_id = None
                    entry_price = entry_time_ist = sl_order_id = None
                    current_sl = last_increment_at = None
                    sl_at_cost    = False
                    cooldown_start = None
                    phase          = "WATCHING"
                else:
                    logger.info(
                        "[RSNIP] COOLDOWN symbol=%s %.1fs / %.1fs remaining",
                        symbol, elapsed, cooldown_secs,
                    )
                await asyncio.sleep(5)

    except asyncio.CancelledError:
        logger.info("[RSNIP] LOOP_CANCELLED symbol=%s phase=%s", symbol, phase)
        if slb_order_id:
            await _cancel_order(kite, slb_order_id, symbol, "TOGGLE_OFF")
        if sl_order_id and phase == "ENTRY_ACTIVE":
            await _cancel_order(kite, sl_order_id, symbol, "TOGGLE_OFF")
        raise
    except Exception as e:
        logger.error(
            "[RSNIP] UNEXPECTED_ERROR symbol=%s phase=%s error=%s",
            symbol, phase, e, exc_info=True,
        )
        state.reverse_snipper_state.pop(symbol, None)
        _active_tasks.pop(symbol, None)
        await manager.broadcast_all({"type": "reverse_snipper_stopped", "symbol": symbol, "reason": "ERROR"})


# ── Public API ────────────────────────────────────────────────────────────────

def start_snipper(symbol: str, config: dict) -> bool:
    if symbol in _active_tasks and not _active_tasks[symbol].done():
        logger.warning("[RSNIP] START_IGNORED symbol=%s — already running", symbol)
        return False
    task = asyncio.create_task(_snipper_loop(symbol, config))
    _active_tasks[symbol] = task
    state.reverse_snipper_state[symbol] = config
    logger.info("[RSNIP] STARTED symbol=%s config=%s", symbol, config)
    return True


def stop_snipper(symbol: str) -> bool:
    task = _active_tasks.pop(symbol, None)
    state.reverse_snipper_state.pop(symbol, None)
    if task and not task.done():
        task.cancel()
        logger.info("[RSNIP] STOPPED symbol=%s", symbol)
        return True
    logger.info("[RSNIP] STOP_NOOP symbol=%s — no active task", symbol)
    return False


def get_active() -> dict:
    return {
        sym: cfg
        for sym, cfg in state.reverse_snipper_state.items()
        if sym in _active_tasks and not _active_tasks[sym].done()
    }


async def get_trades(pool, symbol: str = None, date: str = None) -> list:
    """Fetch historical reverse snipper trades for chart marker rendering."""
    try:
        # asyncpg needs a datetime.date object, not a string
        date_obj = datetime.date.fromisoformat(date) if date else None
        async with pool.acquire() as conn:
            if symbol and date_obj:
                rows = await conn.fetch(
                    """
                    SELECT * FROM reverse_snipper_trades
                    WHERE symbol = $1 AND entry_time::date = $2
                    ORDER BY entry_time
                    """,
                    symbol, date_obj,
                )
            elif symbol:
                rows = await conn.fetch(
                    """
                    SELECT * FROM reverse_snipper_trades
                    WHERE symbol = $1
                    ORDER BY entry_time DESC LIMIT 50
                    """,
                    symbol,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT * FROM reverse_snipper_trades
                    ORDER BY entry_time DESC LIMIT 100
                    """
                )
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error("[RSNIP] GET_TRADES_ERROR error=%s", e, exc_info=True)
        return []
