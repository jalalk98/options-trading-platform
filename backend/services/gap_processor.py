# backend/services/gap_processor.py

from datetime import datetime
from backend.services.instrument_registry import get_metadata

last_tick_cache = {}

PRICE_THRESHOLD = 1.0          # NIFTY/BANKNIFTY
TIME_THRESHOLD  = 0.75         # seconds — NIFTY
MAX_SPREAD_PCT  = 0.25         # NIFTY
REQUIRE_ZERO_VOLUME = True

# SENSEX options (BFO) tick differently: volume always changes with price moves,
# ticks are batched at 1s; genuine instantaneous gaps arrive at time_diff=0.0
# All thresholds scaled ~3x relative to NIFTY equivalents
SENSEX_PRICE_THRESHOLD = 3.0   # 3x NIFTY price threshold
SENSEX_TIME_THRESHOLD  = 0.0   # must be exactly instantaneous (time_diff=0)
SENSEX_MAX_SPREAD_PCT  = 0.75  # 3x NIFTY spread threshold


def process_tick(tick: dict):

    if tick.get("mode") != "full":
        return None

    # print("Tick received at:", datetime.now())

    instrument_token = tick.get("instrument_token")
    metadata = get_metadata(instrument_token)

    # print("\nTICK RECEIVED")
    # print("Token:", instrument_token)
    # print("Metadata Symbol:", metadata["symbol"])
    # print("Metadata Expiry:", metadata["expiry_date"])

    if not metadata:
        return None

    curr_price = tick.get("last_price")
    curr_volume = tick.get("volume_traded")
    timestamp = tick.get("exchange_timestamp")
    depth = tick.get("depth", {})

    if not curr_price or not timestamp:
        return None

    # Best bid / ask
    best_bid = None
    best_ask = None

    try:
        best_bid = depth["buy"][0]["price"] if depth["buy"] else None
        best_ask = depth["sell"][0]["price"] if depth["sell"] else None
    except Exception:
        pass

    spread = None
    spread_pct = None

    if best_bid and best_ask:
        spread = best_ask - best_bid
        if curr_price != 0:
            spread_pct = (spread / curr_price) * 100

    prev_data = last_tick_cache.get(instrument_token)

    if not prev_data:
        last_tick_cache[instrument_token] = {
            "price": curr_price,
            "volume": curr_volume,
            "timestamp": timestamp
        }
        return None

    prev_price = prev_data["price"]
    prev_volume = prev_data["volume"]
    prev_timestamp = prev_data["timestamp"]

    price_jump = curr_price - prev_price

    direction = None
    if price_jump > 0:
        direction = "UP"
    elif price_jump < 0:
        direction = "DOWN"
    else:
        direction = "FLAT"

    vol_change = None
    if curr_volume is not None and prev_volume is not None:
        vol_change = curr_volume - prev_volume

    time_diff = None
    if isinstance(timestamp, datetime) and isinstance(prev_timestamp, datetime):
        time_diff = (timestamp - prev_timestamp).total_seconds()

    # Gap flags
    only_gap = False
    gap_with_spread = False
    is_gap = False

    price_jump_abs = abs(curr_price - prev_price)
    symbol = metadata["symbol"]
    is_sensex = symbol.startswith("SENSEX")

    if is_sensex:
        # SENSEX: instantaneous jump, no volume constraint, wider spread tolerance
        only_gap = (
            price_jump_abs >= SENSEX_PRICE_THRESHOLD
            and time_diff is not None
            and time_diff <= SENSEX_TIME_THRESHOLD
        )
        gap_with_spread = (
            only_gap
            and spread_pct is not None
            and spread_pct <= SENSEX_MAX_SPREAD_PCT
        )
    else:
        vol_ok = (vol_change == 0) if REQUIRE_ZERO_VOLUME else True
        only_gap = (
            price_jump_abs >= PRICE_THRESHOLD
            and vol_ok
            and time_diff is not None
            and time_diff <= TIME_THRESHOLD
        )
        gap_with_spread = (
            only_gap
            and spread_pct is not None
            and spread_pct <= MAX_SPREAD_PCT
        )

    is_gap = gap_with_spread

    # Update cache
    last_tick_cache[instrument_token] = {
        "price": curr_price,
        "volume": curr_volume,
        "timestamp": timestamp
    }
    
    # print("Row created at:",datetime.now())
    return {
        "instrument_token": instrument_token,

        "symbol": metadata["symbol"],
        "expiry_date": metadata["expiry_date"],
        "strike": metadata["strike"],
        "option_type": metadata["option_type"],

        "timestamp": timestamp,

        "prev_price": prev_price,
        "curr_price": curr_price,
        "price_jump": price_jump,
        "direction": direction,

        "prev_volume": prev_volume,
        "curr_volume": curr_volume,
        "vol_change": vol_change,
        "time_diff": time_diff,

        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "spread_pct": spread_pct,

        "only_gap": only_gap,
        "gap_with_spread": gap_with_spread,
        "is_gap": is_gap
    }
