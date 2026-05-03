import asyncio
import logging
import threading
import json
import requests
from pathlib import Path
from backend.core.async_tickers import MainTicker
from backend.core.custom_connect import KiteConnect_custom
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN
from config.logging_config import logger
from backend.core.redis_client import redis_client
from backend.services.gap_processor import process_tick
from backend.state import app_config
from backend.services import tick_metrics


def _is_market_hours() -> bool:
    """Return True if current IST time is within trading hours (Mon–Fri, 09:15–15:30) and not a holiday."""
    from datetime import datetime, timezone, timedelta
    IST = timezone(timedelta(hours=5, minutes=30))
    now = datetime.now(IST)
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        return False
    hhmm = now.hour * 100 + now.minute
    if hhmm < 915 or hhmm >= 1530:
        return False
    today = now.strftime("%Y-%m-%d")
    holidays_path = Path.home() / ".trading_holidays"
    try:
        for line in holidays_path.read_text().splitlines():
            if line.startswith(today):
                return False
    except Exception:
        pass
    return True


def _send_telegram(text: str) -> None:
    """Send a Telegram message using credentials from ~/.kite_secrets."""
    secrets_path = Path.home() / ".kite_secrets"
    try:
        secrets = {}
        for line in secrets_path.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                secrets[k.strip()] = v.strip()
        token   = secrets.get("TELEGRAM_BOT_TOKEN")
        chat_id = secrets.get("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            logger.error("Telegram credentials missing in ~/.kite_secrets")
            return
        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": text},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {e}")

# Configure logging
logging.getLogger('websockets.client').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('kiteconnect').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)

# Global variables for WebSocket
websocket_running = {"running": False}
# loop = asyncio.new_event_loop()  # Dedicated event loop for WebSocket
tokens = []

# KiteConnect initialization
kite1 = KiteConnect_custom(KITE_API_KEY)
kite1.set_access_token(KITE_ACCESS_TOKEN)

# WebSocket Initialization
kws = MainTicker(KITE_API_KEY, KITE_ACCESS_TOKEN, None, kite1, token_to_OrderId_Mod_Dic=None, kws=None, debug=True)

active_positions = {}
# Tracks per order_id how many qty units already have an SL placed,
# so partial-fill SL placement doesn't double-count on subsequent fill events.
sl_covered = {}


def _post_sl_state(symbol: str, state: str, retries: int = 3, timeout: int = 5) -> None:
    """POST sl state to chart_server with retries. Runs in calling thread (use threading for non-blocking)."""
    for attempt in range(1, retries + 1):
        try:
            requests.post(
                "http://localhost:8000/api/sl/set",
                json={"symbol": symbol, "state": state},
                timeout=timeout,
            )
            return
        except Exception as _e:
            logger.warning(f"_post_sl_state attempt {attempt}/{retries} failed for {symbol}: {_e}")
    logger.error(f"_post_sl_state gave up after {retries} attempts for {symbol} state={state}")


def setup_websocket_events():
    """
    Attach event handlers to the WebSocket instance, dynamically passing required variables.
    """
    global kws

    _feed_dropped = {"sent": False, "at": None}  # track whether drop alert was already sent

    # tick_count = 0
    async def on_ticks(ws, ticks):
        # print("Received ticks:", len(ticks))
        try:
            tick_metrics.record_batch(ticks)
            pipe = redis_client.pipeline()

            for tick in ticks:
                # print("Raw tick received:", tick["instrument_token"], tick["exchange_timestamp"])
                result = process_tick(tick)

                if result:
                    pipe.xadd(
                        "ticks_stream",
                        {"data": json.dumps(result, default=str)},
                        maxlen=50000,
                        approximate=True
                    )

            await pipe.execute()

        except Exception as e:
            logger.error(f"Error processing tick batch: {e}")

    
    async def on_order_update(ws, data):
        try:
            result = await handle_order_update(ws, data)

            # Only log real errors
            if result and result.get("status") == "error":
                logger.error(f"handle_order_update returned error: {result}")

        except Exception as e:
            logger.error(f"Error in on_order_update: {e}")


    async def handle_order_update(ws, data):

        global active_positions, sl_covered

        try:

            # Check if UI requested a position cache reset
            reset_flag = Path("/tmp/reset_positions")
            if reset_flag.exists():
                active_positions.clear()
                reset_flag.unlink(missing_ok=True)
                logger.info("active_positions cache cleared via UI reset button")

            status      = data.get("status")
            order_id    = data.get("order_id", "")
            filled_qty  = int(data.get("filled_quantity", 0) or 0)
            is_complete = (status == "COMPLETE")
            is_partial  = (status == "OPEN" and filled_qty > 0)

            # Detect SL order completion → update position tracker + mark state as 'hit'
            if data.get("order_type") == "SL":
                if is_complete:
                    sym = data.get("tradingsymbol")
                    tx  = data.get("transaction_type")
                    qty = int(data.get("quantity", 0))
                    # Update active_positions so subsequent manual orders see the correct
                    # position. Without this, a manual BUY placed at the same time as an
                    # SL BUY would see stale active_positions and be mis-classified as
                    # "closing a short" instead of opening a new long.
                    if sym and tx and qty:
                        current = active_positions.get(sym, 0)
                        if tx == "BUY":
                            active_positions[sym] = current + qty
                        elif tx == "SELL":
                            active_positions[sym] = current - qty
                        logger.info(f"SL order filled for {sym} ({tx} {qty}) — active_positions updated: {current} → {active_positions[sym]}")
                    if sym:
                        logger.info(f"SL hit for {sym} — posting 'hit' to chart_server")
                        def _hit_then_clear(s=sym):
                            import time
                            _post_sl_state(s, "hit")
                            time.sleep(3)
                            _post_sl_state(s, "none")
                        threading.Thread(target=_hit_then_clear, daemon=True).start()
                return {"status": "ignored"}

            # CANCELLED: if partially filled before cancel, still place SL for the filled qty
            if status == "CANCELLED":
                already_covered = sl_covered.pop(order_id, 0)
                unfilled_qty = filled_qty - already_covered
                if unfilled_qty <= 0:
                    return {"status": "ignored"}
                # Fall through with unfilled_qty as the qty needing SL coverage
                logger.info(
                    f"Cancelled order {order_id} had partial fill: "
                    f"filled={filled_qty} already_covered={already_covered} — placing SL for {unfilled_qty}"
                )
                is_partial  = True   # treat remainder as a partial fill for SL logic below
                is_complete = True   # ensures sl_covered is cleaned up (no more fills coming)
                filled_qty  = unfilled_qty

            # Only process entry orders that have new fills
            if not is_complete and not is_partial:
                return {"status": "ignored"}

            trade_symbol = data.get("tradingsymbol")
            exchange = data.get("exchange")
            transaction_type = data.get("transaction_type")
            last_price = float(data.get("average_price", 0))

            if last_price == 0:
                logger.warning(f"Average price is zero for {trade_symbol}")
                return {"status": "ignored"}

            # How much of this order already has SL placed
            already_covered = sl_covered.get(order_id, 0)
            # New qty needing SL: delta between current fills and already-covered
            new_sl_qty = filled_qty - already_covered
            if new_sl_qty <= 0:
                return {"status": "ignored"}

            # Update coverage tracking; clean up on final fill or complete
            sl_covered[order_id] = filled_qty
            if is_complete:
                sl_covered.pop(order_id, None)

            # Use new_sl_qty for position tracking and SL placement
            qty = new_sl_qty

            if is_partial:
                logger.info(
                    f"Partial fill for {trade_symbol} order {order_id}: "
                    f"filled={filled_qty} already_covered={already_covered} new_sl_qty={new_sl_qty}"
                )

            # -----------------------------
            # Current position tracking
            # -----------------------------
            current = active_positions.get(trade_symbol, 0)

            # ============================
            # BUY ORDER
            # ============================
            if transaction_type == "BUY":

                if current < 0:
                    # Was short — this BUY is closing it (partial or full)
                    new_pos = current + qty
                    active_positions[trade_symbol] = new_pos
                    if new_pos >= 0:
                        threading.Thread(target=_post_sl_state, args=(trade_symbol, "none"), daemon=True).start()
                        logger.info(f"Short position CLOSED for {trade_symbol}")
                    else:
                        logger.info(f"Short position partial close for {trade_symbol}: {current} → {new_pos}")
                    return {"status": "ignored"}

                # Was flat or long — new/additional BUY entry (chunk)
                active_positions[trade_symbol] = current + qty

            # ============================
            # SELL ORDER
            # ============================
            elif transaction_type == "SELL":

                if current > 0:
                    # Was long — this SELL is closing it (partial or full)
                    new_pos = current - qty
                    active_positions[trade_symbol] = new_pos
                    if new_pos <= 0:
                        threading.Thread(target=_post_sl_state, args=(trade_symbol, "none"), daemon=True).start()
                        logger.info(f"Long position CLOSED for {trade_symbol}")
                    else:
                        logger.info(f"Long position partial close for {trade_symbol}: {current} → {new_pos}")
                    return {"status": "ignored"}

                # Was flat or short — new/additional SELL entry (chunk)
                active_positions[trade_symbol] = current - qty

            # -----------------------------
            # Helper
            # -----------------------------
            def round_to_tick(price, tick_size=0.05):
                return round(round(price / tick_size) * tick_size, 2)

            # Read SL state from chart_server (single source of truth)
            # tick_collector and chart_server are separate processes with separate memory,
            # so we fetch the SL line price the user dragged via the REST API.
            stored_sl_state = {}
            try:
                resp = requests.get(f"http://localhost:8000/api/sl/{trade_symbol}", timeout=2)
                if resp.status_code == 200:
                    stored_sl_state = resp.json()
            except Exception as _e:
                logger.warning(f"Could not fetch SL state from API for {trade_symbol}: {_e}")

            trigger_buffer = stored_sl_state.get("trigger_buffer", 0.20)

            # Use SL price from dragged line if available for this symbol,
            # otherwise fall back to default distance from entry price.
            stored_sl = stored_sl_state.get("price")

            logger.info(
                f"SL state for {trade_symbol}: price={stored_sl} "
                f"state={stored_sl_state.get('state')} entry={last_price} side={transaction_type}"
            )

            # ============================
            # BUY ENTRY → SELL SL
            # ============================
            if transaction_type == "BUY":

                if stored_sl and stored_sl < last_price:
                    # Use dragged SL line — must be below entry for a long
                    stop_loss_price = round_to_tick(stored_sl)
                    sl_source = "line"
                else:
                    if stored_sl and stored_sl >= last_price:
                        logger.warning(
                            f"SL line ignored for {trade_symbol} BUY entry: "
                            f"SL line ({stored_sl}) is ABOVE entry ({last_price}). "
                            f"For a long, SL must be below entry. Using default SL."
                        )
                    stop_loss_price = round_to_tick(last_price - app_config["default_sl_dist"])
                    sl_source = "default"

                trigger_price = round_to_tick(stop_loss_price + trigger_buffer)

                logger.info(
                    f"Placing SL SELL for {trade_symbol} | Entry: {last_price} | "
                    f"SL: {stop_loss_price} | Trigger: {trigger_price} | "
                    f"Source: {sl_source}"
                )

                result = await kite1.hard_code_regular_sell_order(
                    exchange=exchange,
                    trade_symbol=trade_symbol,
                    qty=qty,
                    stop_loss_price=stop_loss_price,
                    trig_price=trigger_price,
                    api_key=KITE_API_KEY,
                    access_token=KITE_ACCESS_TOKEN
                )

                order_id = None
                if isinstance(result, dict):
                    order_id = (result.get("data") or {}).get("order_id")

                # Write order_id back to chart_server so drag-to-modify works
                try:
                    requests.post("http://localhost:8000/api/sl/set", json={
                        "symbol":         trade_symbol,
                        "price":          stop_loss_price,
                        "trigger_buffer": trigger_buffer,
                        "order_id":       order_id,
                        "side":           "SELL",
                        "qty":            qty,
                        "exchange":       exchange,
                        "state":          "placed",
                    }, timeout=2)
                except Exception as _e:
                    logger.warning(f"Could not sync SL state to API for {trade_symbol}: {_e}")
                logger.info(f"SL SELL placed for {trade_symbol}: order_id={order_id}")
                return {"status": "success"}

            # ============================
            # SELL ENTRY → BUY SL
            # ============================
            elif transaction_type == "SELL":

                if stored_sl and stored_sl > last_price:
                    # Use dragged SL line — must be above entry for a short
                    stop_loss_price = round_to_tick(stored_sl)
                    sl_source = "line"
                else:
                    if stored_sl and stored_sl <= last_price:
                        logger.warning(
                            f"SL line ignored for {trade_symbol} SELL entry: "
                            f"SL line ({stored_sl}) is BELOW entry ({last_price}). "
                            f"For a short, SL must be above entry. Using default SL."
                        )
                    stop_loss_price = round_to_tick(last_price + app_config["default_sl_dist"])
                    sl_source = "default"

                trigger_price = round_to_tick(stop_loss_price - trigger_buffer)

                logger.info(
                    f"Placing SL BUY for {trade_symbol} | Entry: {last_price} | "
                    f"SL: {stop_loss_price} | Trigger: {trigger_price} | "
                    f"Source: {sl_source}"
                )

                result = await kite1.hard_code_regular_buy_order(
                    exchange=exchange,
                    trade_symbol=trade_symbol,
                    qty=qty,
                    price=stop_loss_price,  # limit: at SL line price
                    trig_price=trigger_price,  # trigger: SL line - buffer
                    api_key=KITE_API_KEY,
                    access_token=KITE_ACCESS_TOKEN
                )

                order_id = None
                if isinstance(result, dict):
                    order_id = (result.get("data") or {}).get("order_id")

                # Write order_id back to chart_server so drag-to-modify works
                try:
                    requests.post("http://localhost:8000/api/sl/set", json={
                        "symbol":         trade_symbol,
                        "price":          stop_loss_price,
                        "trigger_buffer": trigger_buffer,
                        "order_id":       order_id,
                        "side":           "BUY",
                        "qty":            qty,
                        "exchange":       exchange,
                        "state":          "placed",
                    }, timeout=2)
                except Exception as _e:
                    logger.warning(f"Could not sync SL state to API for {trade_symbol}: {_e}")
                logger.info(f"SL BUY placed for {trade_symbol}: order_id={order_id}")
                return {"status": "success"}

            return {"status": "ignored"}

        except Exception as e:
            logger.error(f"handle_order_update error: {e}")
            return {"status": "error", "message": str(e)}


    async def on_connect(ws):
        """
        Triggered when WebSocket connects successfully.
        """
        tick_metrics.start()
        main_ticker = ws.ws_instance
        if kws.first_connect:
            logging.info("First connection: subscribing to tokens.")
            await main_ticker.subscribe(tokens)
            await main_ticker.set_mode("full", tokens)
            kws.first_connect = False
        else:
            import time
            attempts = kws.reconnect_attempts
            down_for = ""
            if _feed_dropped["at"]:
                elapsed = int(time.time() - _feed_dropped["at"])
                mins, secs = divmod(elapsed, 60)
                down_for = f" (down for {mins}m {secs}s)" if mins else f" (down for {secs}s)"
            logging.info(f"Reconnection successful after {attempts} attempt(s){down_for}.")
            _feed_dropped["sent"] = False
            _feed_dropped["at"] = None
            if _is_market_hours():
                _send_telegram(f"🟢 WebSocket feed restored after {attempts} attempt(s){down_for}. Feed is live.")
    
    async def on_close(ws, code, reason):
        global websocket_running
        """
        Triggered when WebSocket connection closes.
        """
        logger.warning(f"WebSocket closed: {code} - {reason}")
        websocket_running["running"] = False
        if not _feed_dropped["sent"] and _is_market_hours():
            import time
            _feed_dropped["sent"] = True
            _feed_dropped["at"] = time.time()
            _send_telegram(f"🟡 WebSocket feed dropped (code={code}). Reconnecting automatically...")
        if code is None:
            code = 1000
        if reason is None:
            reason = "Normal closure"

    async def on_error(ws, code, reason):
        """
        Triggered when WebSocket encounters an error.
        """
        logger.error(f"WebSocket error: {code} - {reason}")
        if code is None:
            code = "UnknownError"
        if reason is None:
            reason = "An unknown error occurred"

    # Callback when reconnect is in progress
    def on_reconnect(ws, attempts_count):
        logger.warning(f"WebSocket reconnecting: attempt {attempts_count} of {kws.reconnect_max_tries}")

    def on_noreconnect(ws):
        logger.error("WebSocket reconnect FAILED — all retry attempts exhausted. Feed is dead.")
        if _is_market_hours():
            _send_telegram("🔴 WebSocket feed DEAD — all reconnect attempts exhausted. Restart trading session manually.")

        
    # Attach handlers to the WebSocket instance
    kws.on_ticks = on_ticks  # Pass dynamically via `partial`
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error
    kws.on_reconnect = on_reconnect
    kws.on_noreconnect = on_noreconnect
    kws.on_order_update = on_order_update
    

async def run_websocket(dynamic_tokens):
    global websocket_running, kws, tokens

    if not dynamic_tokens:
        logger.error("Websocket start aborted: Empty token list")
        return

    if websocket_running["running"]:
        logger.warning("WebSocket is already running.")
        return

    tokens = dynamic_tokens
    websocket_running["running"] = True

    setup_websocket_events()

    # 🔥 IMPORTANT — use current running loop
    current_loop = asyncio.get_running_loop()
    kws.loop = current_loop

    try:
        logger.info(f"Starting WebSocket with tokens: {tokens}")
        kws.connect(threaded=True)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        websocket_running["running"] = False






