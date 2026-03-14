import asyncio
from backend.services.websocket_handler import run_websocket

from backend.services.instrument_registry import (
    load_instruments,
    get_tokens_by_strikes,
    get_nearest_expiry
)

from kiteconnect import KiteConnect
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN
from config.logging_config import logger
from datetime import datetime

logger.info("Initializing Kite client")

kite = KiteConnect(api_key=KITE_API_KEY)
kite.set_access_token(KITE_ACCESS_TOKEN)


def should_subscribe(index_name, expiry_str):
    """
    Determines whether tokens should be subscribed
    based on expiry proximity.
    """

    expiry_date = datetime.strptime(expiry_str, "%d-%m-%Y").date()
    today = datetime.today().date()

    days_left = (expiry_date - today).days

    logger.info(f"{index_name} expiry in {days_left} days")

    # NIFTY always subscribe
    if index_name == "NIFTY":
        return True

    # SENSEX & BANKNIFTY only in last 2 days
    if index_name in ["SENSEX", "BANKNIFTY"]:
        return 0 <= days_left <= 1

    return True


def get_index_ltp(symbol):

    logger.info(f"Fetching LTP for {symbol}")

    data = kite.ltp(symbol)

    ltp = data[symbol]["last_price"]

    logger.info(f"LTP for {symbol} = {ltp}")

    return ltp



def generate_strikes(index_ltp, gap, count=8):

    logger.info(f"Generating strikes using LTP={index_ltp}, gap={gap}, count={count}")

    atm = round(index_ltp / gap) * gap

    logger.info(f"Calculated ATM strike = {atm}")

    strikes = []

    for i in range(-count, count + 1):

        strike = atm + (i * gap)

        strikes.append(f"{strike}-CE")
        strikes.append(f"{strike}-PE")

    logger.info(f"Generated {len(strikes)} option strikes")

    logger.debug(f"Strike list: {strikes}")

    return strikes


async def main():

    logger.info("========== Tick Collector Started ==========")

    logger.info("Loading instrument file into memory")

    load_instruments()

    logger.info("Instrument file loaded successfully")

    subscriptions = [
    {
        "index": "NIFTY",
        "ltp_symbol": "NSE:NIFTY 50",
        "gap": 50
    },
    {
        "index": "SENSEX",
        "ltp_symbol": "BSE:SENSEX",
        "gap": 100
    },
    {
        "index": "BANKNIFTY",
        "ltp_symbol": "NSE:NIFTY BANK",
        "gap": 100
    }
    ]

    all_tokens = []

    for sub in subscriptions:

        logger.info(f"Processing subscription for {sub['index']}")

        expiry = get_nearest_expiry(sub["index"])

        logger.info(f"Nearest expiry selected for {sub['index']} = {expiry}")

        # 🔹 Apply expiry condition
        if not should_subscribe(sub["index"], expiry):
            logger.info(
                f"Skipping {sub['index']} because expiry is not within last 2 days"
            )
            continue

        ltp = get_index_ltp(sub["ltp_symbol"])

        strikes = generate_strikes(
            ltp,
            sub["gap"],
            8
        )

        logger.info(
            f"Fetching tokens for {sub['index']} | Expiry={expiry} | Strikes={len(strikes)}"
        )

        tokens = get_tokens_by_strikes(
            strikes,
            expiry,
            sub["index"]
        )

        logger.info(f"Tokens fetched for {sub['index']} = {len(tokens)}")

        all_tokens.extend(tokens)

    logger.info("Removing duplicate tokens")

    all_tokens = list(set(all_tokens))

    logger.info(f"Total unique tokens to subscribe = {len(all_tokens)}")

    logger.info("Starting websocket subscription")

    if not all_tokens:
        logger.warning("No tokens to subscribe. Exiting.")
        return

    await run_websocket(all_tokens)

    logger.info("Websocket started successfully")

    while True:
        await asyncio.sleep(3600)


asyncio.run(main())