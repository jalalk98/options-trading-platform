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

    instrument_token = tick.get("instrument_token")
    metadata = get_metadata(instrument_token)

    if not metadata:
        return None

    # Index ticks: record every LTP — no gap detection, no depth data
    if metadata.get("option_type") == "INDEX":
        curr_price = tick.get("last_price")
        timestamp  = tick.get("exchange_timestamp")
        if not curr_price or not timestamp:
            return None
        return {
            "instrument_token": instrument_token,
            "symbol":           metadata["symbol"],
            "expiry_date":      metadata["expiry_date"],
            "strike":           metadata["strike"],
            "option_type":      "INDEX",
            "timestamp":        timestamp,
            "prev_price":       None,
            "curr_price":       curr_price,
            "price_jump":       None,
            "direction":        None,
            "prev_volume":      None,
            "curr_volume":      None,
            "vol_change":       None,
            "time_diff":        None,
            "best_bid":         None,
            "best_ask":         None,
            "spread":           None,
            "spread_pct":       None,
            "only_gap":         False,
            "gap_with_spread":  False,
            "is_gap":           False,
            "last_quantity"   : None,
            "average_price"   : None,
            "last_trade_time" : None,
            "oi"              : None,
            "oi_day_high"     : None,
            "oi_day_low"      : None,
            "buy_quantity"    : None,
            "sell_quantity"   : None,
            "depth"           : None,
            "bid_depth_qty"   : None,
            "ask_depth_qty"   : None,
            "depth_imbalance" : None,
        }

    if tick.get("mode") != "full":
        return None

    curr_price = tick.get("last_price")
    curr_volume = tick.get("volume_traded")
    timestamp = tick.get("exchange_timestamp")
    depth = tick.get("depth", {})

    last_quantity   = tick.get("last_quantity")
    average_price   = tick.get("average_price")
    last_trade_time = tick.get("last_trade_time")
    oi              = tick.get("oi")
    oi_day_high     = tick.get("oi_day_high")
    oi_day_low      = tick.get("oi_day_low")
    buy_quantity    = tick.get("buy_quantity")
    sell_quantity   = tick.get("sell_quantity")

    # Compute depth derived fields
    buy_levels      = depth.get("buy", [])  if depth else []
    sell_levels     = depth.get("sell", []) if depth else []
    bid_depth_qty   = sum(l.get("quantity", 0) for l in buy_levels)
    ask_depth_qty   = sum(l.get("quantity", 0) for l in sell_levels)
    total_depth     = bid_depth_qty + ask_depth_qty
    depth_imbalance = round(bid_depth_qty / total_depth, 4) if total_depth > 0 else None
    depth_json      = depth if depth else None

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
        "is_gap": is_gap,

        "last_quantity"   : last_quantity,
        "average_price"   : average_price,
        "last_trade_time" : last_trade_time,
        "oi"              : oi,
        "oi_day_high"     : oi_day_high,
        "oi_day_low"      : oi_day_low,
        "buy_quantity"    : buy_quantity,
        "sell_quantity"   : sell_quantity,
        "depth"           : depth_json,
        "bid_depth_qty"   : bid_depth_qty,
        "ask_depth_qty"   : ask_depth_qty,
        "depth_imbalance" : depth_imbalance,
    }
