# backend/services/gap_processor.py

from datetime import datetime
from backend.services.instrument_registry import get_metadata

last_tick_cache = {}

# Configurable thresholds
PRICE_GAP_PERCENT = 0.05     # 0.05%
SPREAD_GAP_PERCENT = 0.10    # 0.10%
TIME_GAP_MS = 500


def process_tick(tick: dict):

    if tick.get("mode") != "full":
        return None

    instrument_token = tick.get("instrument_token")
    metadata = get_metadata(instrument_token)

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

    price_gap_pct = 0
    if prev_price != 0:
        price_gap_pct = abs((price_jump / prev_price) * 100)

    if price_gap_pct >= PRICE_GAP_PERCENT:
        only_gap = True

    if only_gap and spread_pct and spread_pct >= SPREAD_GAP_PERCENT:
        gap_with_spread = True

    if only_gap or gap_with_spread or (time_diff and time_diff * 1000 > TIME_GAP_MS):
        is_gap = True

    # Update cache
    last_tick_cache[instrument_token] = {
        "price": curr_price,
        "volume": curr_volume,
        "timestamp": timestamp
    }

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
