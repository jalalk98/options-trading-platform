import asyncio
import logging
from backend.core.async_tickers import MainTicker
from backend.core.custom_connect import KiteConnect_custom
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN
from config.logging_config import logger
from backend.core.redis_client import redis_client
import json
from backend.services.gap_processor import process_tick
from backend.services.tick_queue import tick_queue
import threading

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

    # tick_count = 0
    async def on_ticks(ws, ticks):
        # print("Received ticks:", len(ticks))
        
        for tick in ticks:
            try:
                # print("Raw tick received:", tick["instrument_token"], tick["exchange_timestamp"])
                result = process_tick(tick)

                if result:
                    await redis_client.xadd(
                        "ticks_stream",
                        {"data": json.dumps(result,default=str)}
                    )

            except Exception as e:
                logger.error(f"Error processing tick: {e}")
    
    async def on_order_update(ws, data):
        try:
            result = await handle_order_update(ws, data)
            if result is None or result.get("status") != "success":
                logger.error(f"handle_order_update returned an error: {result}")
        except Exception as e:
            logger.error(f"Error in on_order_update: {e}")


    async def handle_order_update(ws, data):
        try:
            # Only act when order is fully completed
            if data.get("status") != "COMPLETE":
                return {"status": "ignored"}

            # Ignore SL orders to prevent infinite loop
            if data.get("order_type") == "SL":
                return {"status": "ignored"}

            trade_symbol = data.get("tradingsymbol")
            qty = int(data.get("quantity", 0))
            exchange = data.get("exchange")
            transaction_type = data.get("transaction_type")
            last_price = float(data.get("average_price", 0))

            # Safety check
            if last_price == 0:
                logger.warning(f"Average price is zero for {trade_symbol}. Ignoring SL placement.")
                return {"status": "ignored"}

            def round_to_tick(price, tick_size=0.05):
                return round(round(price / tick_size) * tick_size, 2)

            sl_points = 5
            trigger_buffer = 0.3

            # ==============================
            # BUY ENTRY → PLACE SELL SL
            # ==============================
            if transaction_type == "BUY":

                stop_loss_price = round_to_tick(last_price - sl_points)
                trigger_price = round_to_tick(stop_loss_price + trigger_buffer)

                logger.info(
                    f"Placing SL SELL for {trade_symbol} | Qty: {qty} | SL: {stop_loss_price} | Trigger: {trigger_price}"
                )

                await kite1.hard_code_regular_sell_order(
                    exchange=exchange,
                    trade_symbol=trade_symbol,
                    qty=qty,
                    stop_loss_price=stop_loss_price,
                    trig_price=trigger_price,
                    api_key=KITE_API_KEY,
                    access_token=KITE_ACCESS_TOKEN
                )

                return {"status": "success"}

            # ==============================
            # SELL ENTRY → PLACE BUY SL
            # ==============================
            elif transaction_type == "SELL":

                stop_loss_price = round_to_tick(last_price + sl_points)
                trigger_price = round_to_tick(stop_loss_price - trigger_buffer)

                logger.info(
                    f"Placing SL BUY for {trade_symbol} | Qty: {qty} | SL: {stop_loss_price} | Trigger: {trigger_price}"
                )

                await kite1.hard_code_regular_buy_order(
                    exchange=exchange,
                    trade_symbol=trade_symbol,
                    qty=qty,
                    price=stop_loss_price,
                    trig_price=trigger_price,
                    api_key=KITE_API_KEY,
                    access_token=KITE_ACCESS_TOKEN
                )

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
            logging.info("Reconnection: skipping first-connect subscription.")
    
    async def on_close(ws, code, reason):
        global websocket_running
        """
        Triggered when WebSocket connection closes.
        """
        logger.info(f"WebSocket closed: {code} - {reason}")
        websocket_running["running"] = False
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
        logger.info("Reconnecting: Attempt {}".format(attempts_count))

    def on_noreconnect(ws):
        logger.info("Reconnect failed. No more attempts will be made.")

        
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






