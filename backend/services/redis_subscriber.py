from backend.core.redis_client import redis_client
import json

async def listen_ticks():

    last_id = "0"

    while True:

        streams = await redis_client.xread(
            {"ticks_stream": last_id},
            block=0
        )

        for stream_name, messages in streams:
            for message_id, data in messages:

                tick = json.loads(data["data"])
                last_id = message_id

                yield tick