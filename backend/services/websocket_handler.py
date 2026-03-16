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
from backend.state import sl_state


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

        global active_positions

        try:

            # Only process completed orders
            if data.get("status") != "COMPLETE":
                return {"status": "ignored"}

            # Detect SL order completion → mark state as 'hit', then ignore to prevent loop
            if data.get("order_type") == "SL":
                if data.get("status") == "COMPLETE":
                    sym = data.get("tradingsymbol")
                    if sym and sym in sl_state:
                        sl_state[sym]["state"] = "hit"
                        logger.info(f"SL hit for {sym} — state updated to 'hit'")
                return {"status": "ignored"}

            trade_symbol = data.get("tradingsymbol")
            qty = int(data.get("quantity", 0))
            exchange = data.get("exchange")
            transaction_type = data.get("transaction_type")
            last_price = float(data.get("average_price", 0))

            if last_price == 0:
                logger.warning(f"Average price is zero for {trade_symbol}")
                return {"status": "ignored"}

            # -----------------------------
            # Current position tracking
            # -----------------------------
            position = active_positions.get(trade_symbol, 0)

            # ============================
            # BUY ORDER
            # ============================
            if transaction_type == "BUY":

                if position < 0:
                    # Closing short position
                    active_positions[trade_symbol] = 0
                    logger.info(f"Short position CLOSED for {trade_symbol}")
                    return {"status": "ignored"}

                # New BUY entry
                active_positions[trade_symbol] = qty

            # ============================
            # SELL ORDER
            # ============================
            elif transaction_type == "SELL":

                if position > 0:
                    # Closing long position
                    active_positions[trade_symbol] = 0
                    logger.info(f"Long position CLOSED for {trade_symbol}")
                    return {"status": "ignored"}

                # New SELL entry
                active_positions[trade_symbol] = -qty

            # -----------------------------
            # Helper
            # -----------------------------
            def round_to_tick(price, tick_size=0.05):
                return round(round(price / tick_size) * tick_size, 2)

            trigger_buffer = 0.20

            # Use SL price from dragged line if available for this symbol,
            # otherwise fall back to ±10 points from entry price.
            stored_sl = sl_state.get(trade_symbol, {}).get("price")

            # ============================
            # BUY ENTRY → SELL SL
            # ============================
            if transaction_type == "BUY":

                if stored_sl and stored_sl < last_price:
                    # Use dragged SL line — must be below entry for a long
                    stop_loss_price = round_to_tick(stored_sl)
                else:
                    stop_loss_price = round_to_tick(last_price - 10)

                trigger_price = round_to_tick(stop_loss_price + trigger_buffer)

                logger.info(
                    f"Placing SL SELL for {trade_symbol} | Entry: {last_price} | "
                    f"SL: {stop_loss_price} | Trigger: {trigger_price} | "
                    f"Source: {'line' if stored_sl else 'default'}"
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

                sl_state[trade_symbol] = {
                    "price":    stop_loss_price,
                    "order_id": order_id,
                    "side":     "SELL",
                    "qty":      qty,
                    "exchange": exchange,
                    "state":    "placed",
                }
                logger.info(f"SL state updated for {trade_symbol}: order_id={order_id}")
                return {"status": "success"}

            # ============================
            # SELL ENTRY → BUY SL
            # ============================
            elif transaction_type == "SELL":

                if stored_sl and stored_sl > last_price:
                    # Use dragged SL line — must be above entry for a short
                    stop_loss_price = round_to_tick(stored_sl)
                else:
                    stop_loss_price = round_to_tick(last_price + 10)

                trigger_price = round_to_tick(stop_loss_price - trigger_buffer)

                logger.info(
                    f"Placing SL BUY for {trade_symbol} | Entry: {last_price} | "
                    f"SL: {stop_loss_price} | Trigger: {trigger_price} | "
                    f"Source: {'line' if stored_sl else 'default'}"
                )

                result = await kite1.hard_code_regular_buy_order(
                    exchange=exchange,
                    trade_symbol=trade_symbol,
                    qty=qty,
                    price=round_to_tick(stop_loss_price + 5),
                    trig_price=round_to_tick(trigger_price + 5),
                    api_key=KITE_API_KEY,
                    access_token=KITE_ACCESS_TOKEN
                )

                order_id = None
                if isinstance(result, dict):
                    order_id = (result.get("data") or {}).get("order_id")

                sl_state[trade_symbol] = {
                    "price":    stop_loss_price,
                    "order_id": order_id,
                    "side":     "BUY",
                    "qty":      qty,
                    "exchange": exchange,
                    "state":    "placed",
                }
                logger.info(f"SL state updated for {trade_symbol}: order_id={order_id}")
                return {"status": "success"}

            return {"status": "ignored"}

        except Exception as e:
            logger.error(f"handle_order_update error: {e}")
            return {"status": "error", "message": str(e)}


    async def on_connect(ws):
        """
        Triggered when WebSocket connects successfully.
        """
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
            _send_telegram(f"🟢 WebSocket feed restored after {attempts} attempt(s){down_for}. Feed is live.")
    
    async def on_close(ws, code, reason):
        global websocket_running
        """
        Triggered when WebSocket connection closes.
        """
        logger.warning(f"WebSocket closed: {code} - {reason}")
        websocket_running["running"] = False
        if not _feed_dropped["sent"]:
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






