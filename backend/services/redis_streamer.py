import asyncio
import json
from backend.core.redis_client import redis_client
from backend.api.streaming import manager
from datetime import datetime

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
                    ts = datetime.fromisoformat(ts)

                epoch_time = ts.timestamp()

                await manager.broadcast(
                    row["symbol"],
                    {
                        "time": epoch_time,
                        "value": float(row["curr_price"]),
                        "is_gap": row["is_gap"],
                        "vol_change": row["vol_change"],
                        "direction": row["direction"],
                        "prev_price": row["prev_price"]
                    }
                )

                last_id = message_id