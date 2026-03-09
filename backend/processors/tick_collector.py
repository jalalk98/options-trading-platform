import asyncio
from frontend.ui.websocket_setup import run_websocket
from backend.services.instrument_registry import get_tokens_by_strikes

async def main():

    strikes = [
        "24500-CE","24550-CE","24600-CE",
        "24400-CE","24300-CE","24200-CE",
        "24500-PE","24600-PE","24400-PE"
    ]

    expiry = "10-3-2026"
    index_name = "NIFTY"

    tokens = get_tokens_by_strikes(strikes, expiry, index_name)

    await run_websocket(tokens)

    # 🔥 Keep process alive so websocket thread runs
    while True:
        await asyncio.sleep(3600)

asyncio.run(main())