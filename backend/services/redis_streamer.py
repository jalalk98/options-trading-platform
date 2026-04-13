import asyncio
import json
import math
import datetime
from backend.core.redis_client import redis_client
from backend.api.streaming import manager

# Track first jump seen per symbol (reset on service restart, which is fine)
_first_jump_seen: dict = {}


def _get_price_filter(symbol: str) -> float:
    if symbol.startswith('SENSEX'):
        return 450.0
    elif symbol.startswith('BANKNIFTY'):
        return 300.0
    elif symbol.startswith('MIDCPNIFTY'):
        return 100.0
    elif symbol.startswith('FINNIFTY'):
        return 250.0
    return 250.0  # NIFTY and default


def _get_jump_threshold(symbol: str) -> float:
    if symbol.startswith('SENSEX'):
        return 15.0
    if symbol.startswith('BANKNIFTY'):
        return 10.0
    if symbol.startswith('MIDCPNIFTY'):
        return 3.0
    if symbol.startswith('FINNIFTY'):
        return 3.0
    return 5.0


async def redis_streamer():

    last_id = "0"

    while True:

        streams = await redis_client.xread(
            {"ticks_stream": last_id},
            block=100,
            count=50
        )

        if not streams:
            continue

        for stream_name, messages in streams:

            for message_id, data in messages:

                row = json.loads(data["data"])

                ts = row["timestamp"]

                if isinstance(ts, str):
                    ts = datetime.datetime.fromisoformat(ts)

                epoch_time = ts.timestamp()

                symbol     = row["symbol"]
                curr_price = float(row["curr_price"])
                price_jump = float(row.get("price_jump") or 0)

                # ── Jump detection ────────────────────────────────
                price_filter  = _get_price_filter(symbol)
                threshold     = _get_jump_threshold(symbol)

                # Opening noise window: 09:15:00–09:15:15 IST
                # Stored as IST wall-clock, so UTC hours match IST hours
                tick_dt  = datetime.datetime.utcfromtimestamp(epoch_time)
                utc_secs = (tick_dt.hour * 3600
                            + tick_dt.minute * 60
                            + tick_dt.second)
                opening_noise = 33300 <= utc_secs <= 33302  # 09:15:00–09:15:02 IST

                is_jump = (
                    abs(price_jump) > threshold
                    and curr_price > 0
                    and curr_price < price_filter
                    and not opening_noise
                )

                is_first = False
                if is_jump:
                    is_first = symbol not in _first_jump_seen
                    if is_first:
                        _first_jump_seen[symbol] = True

                bucket = math.floor(epoch_time / 5) * 5

                # ── Broadcast ─────────────────────────────────────
                payload = {
                    "symbol"    : symbol,
                    "time"      : epoch_time,
                    "value"     : curr_price,
                    "is_gap"    : row["is_gap"],
                    "vol_change": row["vol_change"],
                    "direction" : row["direction"],
                    "prev_price": row["prev_price"],
                    "price_jump": price_jump,
                    "is_jump"   : is_jump,
                    "is_first"  : is_first,
                    "bucket"    : bucket,
                }

                await manager.broadcast(symbol, payload)

                last_id = message_id
