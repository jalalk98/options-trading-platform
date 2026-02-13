import asyncio
import logging
import threading
from backend.core.async_tickers import MainTicker
from backend.core.custom_connect import KiteConnect_custom
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN
from config.logging_config import logger

from backend.services.gap_processor import process_tick
from backend.services.tick_queue import tick_queue
from backend.services.db_writer import db_writer

# Configure logging
logging.getLogger('websockets.client').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('kiteconnect').setLevel(logging.WARNING)
logging.getLogger('asyncio').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)

# Global variables for WebSocket
websocket_running = {"running": False}
loop = asyncio.new_event_loop()  # Dedicated event loop for WebSocket
tokens = []

# KiteConnect initialization
kite1 = KiteConnect_custom(KITE_API_KEY)
kite1.set_access_token(KITE_ACCESS_TOKEN)

# WebSocket Initialization
kws = MainTicker(KITE_API_KEY, KITE_ACCESS_TOKEN, loop, kite1, token_to_OrderId_Mod_Dic=None, kws=None, debug=True)


def setup_websocket_events():
    """
    Attach event handlers to the WebSocket instance, dynamically passing required variables.
    """
    global kws

    # tick_count = 0
    async def on_ticks(ws, ticks):
        # global tick_count
        # tick_count += len(ticks)

        # if tick_count % 100 == 0:
        #     logger.info(f"Processesd {tick_count} ticks so far")
         
        for tick in ticks:
            try:
                result = process_tick(tick)

                if result:
                    await tick_queue.put(result)

            except Exception as e:
                logger.error(f"Error processing tick: {e}")


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
    

def run_websocket(dynamic_tokens):
    """
    Start the WebSocket connection in a separate thread with dynamically passed tokens and references.
    Ensures the Tkinter app remains responsive.
    """
    global websocket_running, kws, loop, tokens

    if not dynamic_tokens:
        logger.error("Websocket start aborted:Empty token list")
        return

    if websocket_running["running"]:
        logger.warning("WebSocket is already running. Ignoring start request.")
        return

    tokens = dynamic_tokens  # Update the tokens dynamically

    def run_async_loop():
        asyncio.set_event_loop(loop)

        async def setup_and_listen():
            setup_websocket_events()
            try:
                logger.info(f"Starting WebSocket connection with tokens: {tokens}")

                # 🔥 START DB WRITER TASK
                asyncio.create_task(db_writer())

                kws.connect(threaded=True)
                await asyncio.Event().wait()  # Keeps the loop running
            except asyncio.CancelledError:
                logger.info("WebSocket listening loop canceled.")
            except Exception as e:
                logger.error(f"Error in WebSocket setup and listen: {e}")
            finally:
                websocket_running["running"] = False
                logger.info("Exiting WebSocket listening loop.")

        loop.run_until_complete(setup_and_listen())

    websocket_running["running"] = True
    threading.Thread(target=run_async_loop).start()
    logger.info("WebSocket task started in the background.")




