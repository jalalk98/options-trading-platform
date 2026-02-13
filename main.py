from frontend.ui.websocket_setup import run_websocket
from backend.services.instrument_registry import get_tokens_by_strikes
from config.logging_config import logger
import time

if __name__ == "__main__":

    strikes = ["25600-PE", "25500-CE"]
    expiry = "17-2-2026"
    index_name = "NIFTY"

    tokens = get_tokens_by_strikes(strikes, expiry, index_name)

    if not tokens:
        logger.error("No valid tokens found. WebSocket will NOT start.")
    else:
        run_websocket(tokens)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting Down")