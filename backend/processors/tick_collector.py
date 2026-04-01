import asyncio
from backend.services.websocket_handler import run_websocket

from backend.services.instrument_registry import (
    load_instruments,
    get_tokens_by_strikes,
    get_nearest_expiry,
    register_index_instrument,
)

# 5 indexes to always subscribe and record ticks for
INDEX_DEFINITIONS = [
    {"exchange_sym": "NSE:NIFTY 50",         "symbol": "NIFTY"},
    {"exchange_sym": "BSE:SENSEX",            "symbol": "SENSEX"},
    {"exchange_sym": "NSE:NIFTY BANK",        "symbol": "BANKNIFTY"},
    {"exchange_sym": "NSE:NIFTY FIN SERVICE", "symbol": "FINNIFTY"},
    {"exchange_sym": "NSE:NIFTY MID SELECT",  "symbol": "MIDCPNIFTY"},
]

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

    # NIFTY and SENSEX always subscribe
    if index_name in ["NIFTY", "SENSEX"]:
        return True

    # BANKNIFTY, FINNIFTY, MIDCPNIFTY only in last 2 trading days of expiry.
    # Using <= 2 calendar days to correctly capture Friday when expiry is Monday.
    if index_name in ["BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"]:
        return 0 <= days_left <= 2

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
        {"index": "NIFTY",      "ltp_symbol": "NSE:NIFTY 50",          "gap": 50,  "count": 10},
        {"index": "SENSEX",     "ltp_symbol": "BSE:SENSEX",             "gap": 100, "count": 10},
        {"index": "BANKNIFTY",  "ltp_symbol": "NSE:NIFTY BANK",         "gap": 100, "count": 10},
        {"index": "FINNIFTY",   "ltp_symbol": "NSE:NIFTY FIN SERVICE",  "gap": 50,  "count": 10},
        {"index": "MIDCPNIFTY", "ltp_symbol": "NSE:NIFTY MID SELECT",   "gap": 25,  "count": 10},
    ]

    all_tokens = []

    # ── Register and subscribe to the 5 index instruments ──────────────────
    logger.info("Resolving index instrument tokens via LTP call")
    try:
        ltp_data = kite.ltp([d["exchange_sym"] for d in INDEX_DEFINITIONS])
        for defn in INDEX_DEFINITIONS:
            entry = ltp_data.get(defn["exchange_sym"], {})
            token = entry.get("instrument_token")
            if token:
                register_index_instrument(int(token), defn["symbol"])
                all_tokens.append(int(token))
                logger.info(f"Index token resolved: {defn['symbol']} → {token}")
            else:
                logger.warning(f"Could not resolve token for {defn['exchange_sym']}")
    except Exception as e:
        logger.error(f"Failed to resolve index tokens: {e}")

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
            sub["count"]
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