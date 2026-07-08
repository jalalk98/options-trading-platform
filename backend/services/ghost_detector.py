import asyncio
import time
import logging
import datetime
from typing import Optional

import backend.state as state
from backend.core.custom_connect import KiteConnect_custom
from backend.api.streaming import manager
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN

logger = logging.getLogger(__name__)

# symbol → asyncio.Task
_active_tasks: dict = {}


def _get_exchange(symbol: str) -> str:
    return "BFO" if symbol.startswith("SENSEX") else "NFO"


def _extract_order_id(result) -> Optional[str]:
    if isinstance(result, dict):
        data = result.get("data") or {}
        return data.get("order_id")
    return None


def _tick_confirms_price(symbol: str, trigger_price: float, side: str) -> tuple[bool, dict]:
    """
    Check tick_history for a tick that crossed trigger_price.
    Returns (confirmed: bool, debug_info: dict) so callers can log the evidence.
    """
    hist = state.tick_history.get(symbol)
    if not hist:
        return False, {"tick_count": 0, "reason": "no tick history for symbol"}

    ticks = list(hist)  # snapshot to avoid mutation during iteration
    prices = [ltp for (_, ltp) in ticks]
    confirming_tick = None

    for (_ts, ltp) in ticks:
        if side == "BUY" and ltp >= trigger_price:
            confirming_tick = ltp
            break
        if side == "SELL" and ltp <= trigger_price:
            confirming_tick = ltp
            break

    debug = {
        "tick_count"     : len(ticks),
        "price_min"      : min(prices) if prices else None,
        "price_max"      : max(prices) if prices else None,
        "trigger_price"  : trigger_price,
        "confirming_tick": confirming_tick,
    }
    return confirming_tick is not None, debug


async def _probe_loop(symbol: str, config: dict):
    kite = KiteConnect_custom(KITE_API_KEY)
    kite.set_access_token(KITE_ACCESS_TOKEN)

    exchange   = _get_exchange(symbol)
    distance   = float(config["distance"])
    buf        = float(config["buffer"])
    qty        = int(config["qty"])
    direction  = config["direction"]   # "BUY" | "SELL" | "BOTH"
    cycle_time = float(config["cycle_time"])
    cycle_num  = 0
    ltp_wait_logged = False

    logger.info(
        "[GHOST] LOOP_START symbol=%s exchange=%s direction=%s "
        "distance=%s buffer=%s qty=%s cycle_time=%ss",
        symbol, exchange, direction, distance, buf, qty, cycle_time
    )

    try:
        while True:
            ltp = state.latest_ltp.get(symbol)
            if ltp is None:
                if not ltp_wait_logged:
                    logger.warning("[GHOST] WAITING_FOR_LTP symbol=%s — no tick received yet", symbol)
                    ltp_wait_logged = True
                await asyncio.sleep(0.2)
                continue
            ltp_wait_logged = False  # reset once we have an LTP

            cycle_num += 1
            cycle_ts = datetime.datetime.now().strftime("%H:%M:%S.%f")[:-3]
            logger.info(
                "[GHOST] CYCLE #%d symbol=%s ltp=%.2f direction=%s time=%s",
                cycle_num, symbol, ltp, direction, cycle_ts
            )

            placed = []  # list of {side, order_id, trigger_price, ltp}

            # ── Place probe order(s) ──────────────────────────────────
            if direction in ("BUY", "BOTH"):
                trigger = round(ltp + distance, 2)
                limit   = round(trigger + buf, 2)
                logger.info(
                    "[GHOST] PLACING BUY PROBE symbol=%s ltp=%.2f trigger=%.2f limit=%.2f qty=%d",
                    symbol, ltp, trigger, limit, qty
                )
                try:
                    result = await kite.hard_code_regular_buy_order(
                        exchange=exchange, trade_symbol=symbol,
                        qty=qty, price=limit, trig_price=trigger,
                        api_key=KITE_API_KEY, access_token=KITE_ACCESS_TOKEN,
                    )
                    oid = _extract_order_id(result)
                    if oid:
                        placed.append({"side": "BUY", "order_id": oid,
                                       "trigger_price": trigger, "ltp": ltp})
                        logger.info(
                            "[GHOST] BUY PROBE PLACED symbol=%s order_id=%s trigger=%.2f",
                            symbol, oid, trigger
                        )
                    else:
                        logger.warning(
                            "[GHOST] BUY PROBE PLACEMENT FAILED symbol=%s raw_result=%s",
                            symbol, result
                        )
                except Exception as e:
                    logger.error(
                        "[GHOST] BUY PROBE EXCEPTION symbol=%s error=%s",
                        symbol, e, exc_info=True
                    )

            if direction in ("SELL", "BOTH"):
                trigger = round(ltp - distance, 2)
                limit   = round(trigger - buf, 2)
                logger.info(
                    "[GHOST] PLACING SELL PROBE symbol=%s ltp=%.2f trigger=%.2f limit=%.2f qty=%d",
                    symbol, ltp, trigger, limit, qty
                )
                try:
                    result = await kite.hard_code_regular_sell_order(
                        exchange=exchange, trade_symbol=symbol,
                        qty=qty, stop_loss_price=limit, trig_price=trigger,
                        api_key=KITE_API_KEY, access_token=KITE_ACCESS_TOKEN,
                    )
                    oid = _extract_order_id(result)
                    if oid:
                        placed.append({"side": "SELL", "order_id": oid,
                                       "trigger_price": trigger, "ltp": ltp})
                        logger.info(
                            "[GHOST] SELL PROBE PLACED symbol=%s order_id=%s trigger=%.2f",
                            symbol, oid, trigger
                        )
                    else:
                        logger.warning(
                            "[GHOST] SELL PROBE PLACEMENT FAILED symbol=%s raw_result=%s",
                            symbol, result
                        )
                except Exception as e:
                    logger.error(
                        "[GHOST] SELL PROBE EXCEPTION symbol=%s error=%s",
                        symbol, e, exc_info=True
                    )

            if not placed:
                logger.warning(
                    "[GHOST] CYCLE #%d NO PROBES PLACED symbol=%s — skipping check",
                    cycle_num, symbol
                )
                await asyncio.sleep(cycle_time)
                continue

            # ── Wait, then check status ───────────────────────────────
            logger.info(
                "[GHOST] CYCLE #%d WAITING %.2fs before status check symbol=%s",
                cycle_num, cycle_time, symbol
            )
            await asyncio.sleep(cycle_time)

            triggered = None

            for p in placed:
                logger.info(
                    "[GHOST] STATUS CHECK symbol=%s order_id=%s side=%s trigger=%.2f",
                    symbol, p["order_id"], p["side"], p["trigger_price"]
                )
                try:
                    status_data = await kite.get_order_status(
                        p["order_id"], KITE_API_KEY, KITE_ACCESS_TOKEN
                    )
                    status = status_data.get("status", "UNKNOWN")
                    logger.info(
                        "[GHOST] STATUS RESULT symbol=%s order_id=%s status=%s",
                        symbol, p["order_id"], status
                    )
                except Exception as e:
                    logger.error(
                        "[GHOST] STATUS CHECK EXCEPTION symbol=%s order_id=%s error=%s",
                        symbol, p["order_id"], e, exc_info=True
                    )
                    status = "ERROR"

                if status in ("OPEN", "COMPLETE"):
                    logger.warning(
                        "[GHOST] PROBE TRIGGERED symbol=%s order_id=%s side=%s "
                        "trigger=%.2f status=%s — checking tick history",
                        symbol, p["order_id"], p["side"], p["trigger_price"], status
                    )
                    triggered = p
                    # Cancel any sibling probe placed in the same cycle
                    for sibling in placed:
                        if sibling["order_id"] != p["order_id"]:
                            logger.info(
                                "[GHOST] CANCELLING SIBLING symbol=%s order_id=%s",
                                symbol, sibling["order_id"]
                            )
                            try:
                                await kite.hard_code_regular_cancel_order(
                                    sibling["order_id"], KITE_ACCESS_TOKEN, KITE_API_KEY
                                )
                                logger.info(
                                    "[GHOST] SIBLING CANCELLED symbol=%s order_id=%s",
                                    symbol, sibling["order_id"]
                                )
                            except Exception as ce:
                                logger.warning(
                                    "[GHOST] SIBLING CANCEL FAILED symbol=%s order_id=%s error=%s",
                                    symbol, sibling["order_id"], ce
                                )
                    break

                else:
                    # TRIGGER PENDING or ERROR — cancel probe
                    logger.info(
                        "[GHOST] CANCELLING PROBE symbol=%s order_id=%s status=%s",
                        symbol, p["order_id"], status
                    )
                    try:
                        await kite.hard_code_regular_cancel_order(
                            p["order_id"], KITE_ACCESS_TOKEN, KITE_API_KEY
                        )
                        logger.info(
                            "[GHOST] PROBE CANCELLED OK symbol=%s order_id=%s",
                            symbol, p["order_id"]
                        )
                    except Exception as ce:
                        logger.warning(
                            "[GHOST] PROBE CANCEL FAILED symbol=%s order_id=%s error=%s",
                            symbol, p["order_id"], ce
                        )

            if triggered:
                confirmed, tick_debug = _tick_confirms_price(
                    symbol, triggered["trigger_price"], triggered["side"]
                )
                is_ghost = not confirmed

                logger.warning(
                    "[GHOST] SPIKE VERDICT symbol=%s side=%s trigger=%.2f ltp_before=%.2f "
                    "is_ghost=%s tick_count=%d price_min=%s price_max=%s confirming_tick=%s",
                    symbol, triggered["side"], triggered["trigger_price"], triggered["ltp"],
                    is_ghost,
                    tick_debug["tick_count"],
                    tick_debug["price_min"],
                    tick_debug["price_max"],
                    tick_debug["confirming_tick"],
                )

                event = {
                    "type"            : "ghost_detector_triggered",
                    "symbol"          : symbol,
                    "side"            : triggered["side"],
                    "spike_price"     : triggered["trigger_price"],
                    "ltp_before"      : triggered["ltp"],
                    "distance"        : distance,
                    "timestamp"       : datetime.datetime.utcnow().isoformat(),
                    "is_ghost"        : is_ghost,
                    "detector_stopped": True,
                }
                await manager.broadcast_all(event)
                logger.info("[GHOST] BROADCAST SENT symbol=%s event_type=ghost_detector_triggered", symbol)

                # Remove from active state and exit loop
                state.ghost_detector_state.pop(symbol, None)
                _active_tasks.pop(symbol, None)
                logger.info("[GHOST] LOOP_END symbol=%s total_cycles=%d", symbol, cycle_num)
                return

            # No trigger this cycle — immediately start the next one

    except asyncio.CancelledError:
        logger.info(
            "[GHOST] LOOP_CANCELLED symbol=%s total_cycles=%d",
            symbol, cycle_num
        )
        raise
    except Exception as e:
        logger.error(
            "[GHOST] UNEXPECTED ERROR symbol=%s cycle=%d error=%s — loop terminated",
            symbol, cycle_num, e, exc_info=True
        )
        # Clean up state so the frontend toggle can be re-enabled
        state.ghost_detector_state.pop(symbol, None)
        _active_tasks.pop(symbol, None)


def start_detector(symbol: str, config: dict) -> bool:
    """Start the probe loop for symbol. Returns False if already running."""
    if symbol in _active_tasks and not _active_tasks[symbol].done():
        logger.warning("[GHOST] START IGNORED symbol=%s — already running", symbol)
        return False

    task = asyncio.create_task(_probe_loop(symbol, config))
    _active_tasks[symbol] = task
    state.ghost_detector_state[symbol] = config
    logger.info(
        "[GHOST] DETECTOR STARTED symbol=%s config=%s",
        symbol, config
    )
    return True


def stop_detector(symbol: str) -> bool:
    """Cancel the probe loop for symbol. Returns True if a running task was cancelled."""
    task = _active_tasks.pop(symbol, None)
    state.ghost_detector_state.pop(symbol, None)
    if task and not task.done():
        task.cancel()
        logger.info("[GHOST] DETECTOR STOPPED (manual) symbol=%s", symbol)
        return True
    logger.info("[GHOST] STOP called but no active task for symbol=%s", symbol)
    return False


def get_active() -> dict:
    """Return config dict for every symbol whose probe loop is currently running."""
    return {
        sym: cfg
        for sym, cfg in state.ghost_detector_state.items()
        if sym in _active_tasks and not _active_tasks[sym].done()
    }
