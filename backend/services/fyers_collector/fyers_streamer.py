"""
fyers_streamer.py
-----------------
Reads from fyers_ticks_stream (Redis) via XREAD and broadcasts live tick
payloads to WebSocket clients subscribed to FY:-prefixed symbols.

Mirrors redis_streamer.py pattern but:
  - Reads from FYERS_REDIS_STREAM (not ticks_stream)
  - No consumer group — fire-and-forget broadcast, same as redis_streamer
  - Symbol = display_symbol ("FY:NIFTY...") — channel key in ConnectionManager
  - No jump/gap/fill logic — raw ltp broadcast only
  - Sends gap-compatible payload shape so the frontend handler doesn't branch
"""

import asyncio
import json
import logging
import math
from datetime import datetime

from backend.core.redis_client import redis_client
from backend.api.streaming import manager
from backend.services.fyers_collector.config import FYERS_REDIS_STREAM

logger = logging.getLogger(__name__)


async def fyers_streamer() -> None:
    last_id = "0"

    while True:
        try:
            streams = await redis_client.xread(
                {FYERS_REDIS_STREAM: last_id},
                block=100,
                count=50,
            )

            if not streams:
                continue

            for _stream_name, messages in streams:
                for message_id, data in messages:
                    try:
                        row = json.loads(data["data"])

                        ts_raw = row.get("timestamp")
                        if isinstance(ts_raw, str):
                            ts = datetime.fromisoformat(ts_raw)
                        else:
                            ts = ts_raw

                        # IST-naive timestamp → epoch (same convention as Zerodha ticks)
                        epoch_time = ts.timestamp()
                        bucket = math.floor(epoch_time / 5) * 5

                        display_symbol = row.get("display_symbol", "")
                        ltp = row.get("ltp")

                        if not display_symbol or ltp is None:
                            last_id = message_id
                            continue

                        payload = {
                            "symbol"     : display_symbol,
                            "time"       : epoch_time,
                            "value"      : float(ltp),
                            "is_gap"     : False,
                            "vol_change" : 0,
                            "direction"  : "UP",
                            "prev_price" : float(ltp),
                            "price_jump" : 0.0,
                            "is_jump"    : False,
                            "is_first"   : False,
                            "bucket"     : bucket,
                            "fill_events": [],
                        }

                        await manager.broadcast(display_symbol, payload)

                    except Exception as e:
                        logger.warning(
                            "fyers_streamer: skipping message %s — %s: %s",
                            message_id, type(e).__name__, e,
                        )

                    last_id = message_id

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("fyers_streamer outer error: %s", e)
            await asyncio.sleep(1)
