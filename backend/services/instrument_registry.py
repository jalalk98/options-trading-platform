import pandas as pd
import logging

INSTRUMENT_CSV_PATH = r"D:\User\Desktop\Trade_Project\trade\data\Combined_Instruments.csv"

df = pd.read_csv(INSTRUMENT_CSV_PATH)
df["expiry"] = pd.to_datetime(df["expiry"])

# 🔥 Global active instrument metadata
active_instruments = {}


def get_tokens_by_strikes(strike_list, expiry_date, index_name):
    """
    Returns list of instrument tokens and builds active metadata map.
    """

    global active_instruments
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

    return tokens


def get_metadata(token):
    return active_instruments.get(token)
