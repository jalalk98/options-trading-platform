import pandas as pd
import logging
from pathlib import Path
from kiteconnect import KiteConnect
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN

BASE_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

INSTRUMENT_CSV_PATH = DATA_DIR / "Combined_Instruments.csv"

df = None

# 🔥 Global active instrument metadata
active_instruments = {}

def get_nearest_expiry(index_name):
    """
    Returns the nearest expiry date for the given index.
    """

    global df

    today = pd.Timestamp.today().normalize()

    expiry_series = df[
        (df["name"] == index_name) &
        (df["expiry"] >= today)
    ]["expiry"]

    if expiry_series.empty:
        raise ValueError(f"No expiry found for {index_name}")

    nearest_expiry = expiry_series.min()

    logging.info(f"Nearest expiry for {index_name} = {nearest_expiry}")

    return nearest_expiry.strftime("%d-%m-%Y")

def extract_instrument_file():
    """
    Download latest instruments from Zerodha
    """

    try:
        kite = KiteConnect(api_key=KITE_API_KEY)
        kite.set_access_token(KITE_ACCESS_TOKEN)

        logging.info("Downloading instruments from Zerodha...")

        nfo = kite.instruments("NFO")
        bfo = kite.instruments("BFO")

        df_nfo = pd.DataFrame(nfo)
        df_bfo = pd.DataFrame(bfo)

        df_combined = pd.concat([df_nfo, df_bfo], ignore_index=True)

        df_combined["expiry"] = pd.to_datetime(df_combined["expiry"], errors="coerce")

        df_combined.to_csv(INSTRUMENT_CSV_PATH, index=False)

        logging.info("Instrument file updated successfully")

    except Exception as e:
        logging.error(f"Instrument extraction failed: {e}")


def load_instruments():
    """
    Load instruments into memory
    """

    global df

    if not INSTRUMENT_CSV_PATH.exists():
        raise FileNotFoundError("Combined_Instruments.csv not found")

    df = pd.read_csv(INSTRUMENT_CSV_PATH)
    df["expiry"] = pd.to_datetime(df["expiry"])

    logging.info(f"Instruments loaded: {len(df)} rows")


def get_tokens_by_strikes(strike_list, expiry_date, index_name):
    """
    Returns list of instrument tokens and builds active metadata map.
    """

    global df
    global active_instruments

    if df is None:
        load_instruments()

    active_instruments.clear()

    expiry_date = pd.to_datetime(expiry_date, dayfirst=True)

    tokens = []

    for strike_entry in strike_list:
        strike_price, option_type = strike_entry.split("-")

        row = df[
            (df["name"] == index_name)
            & (df["expiry"] == expiry_date)
            & (df["strike"] == int(strike_price))
            & (df["instrument_type"] == option_type)
        ]

        if row.empty:
            logging.warning(
                f"No instrument found for {strike_entry} expiry {expiry_date}"
            )
            continue

        record = row.iloc[0]

        token = int(record["instrument_token"])

        active_instruments[token] = {
            "symbol": record["tradingsymbol"],
            "expiry_date": record["expiry"],
            "strike": int(record["strike"]),
            "option_type": record["instrument_type"],
        }

        tokens.append(token)

    print("\nActive Instruments Map:")
    for token, meta in active_instruments.items():
        print(token, "=>", meta["symbol"], "| Expiry:", meta["expiry_date"])

    print("======================================\n")

    return tokens


def get_metadata(token):
    return active_instruments.get(token)