# backend/services/db_writer.py

from config.logging_config import logger
import asyncio
import asyncpg
from backend.services.tick_queue import tick_queue
from backend.api.streaming import manager
from datetime import timezone, timedelta
from config.credentials import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
)

BATCH_SIZE = 5
FLUSH_INTERVAL = 0.07  # seconds

# IST = timezone(timedelta(hours=5, minutes=30))

async def create_pool():
    return await asyncpg.create_pool(
        host=DB_HOST,
        port=int(DB_PORT),  # ensure integer
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        min_size=1,
        max_size=5
    )


INSERT_QUERY = """
INSERT INTO gap_ticks (
    instrument_token,
    symbol,
    expiry_date,
    strike,
    option_type,
    timestamp,
    prev_price,
    curr_price,
    price_jump,
    direction,
    prev_volume,
    curr_volume,
    vol_change,
    time_diff,
    best_bid,
    best_ask,
    spread,
    spread_pct,
    only_gap,
    gap_with_spread,
    is_gap
)
VALUES (
    $1,$2,$3,$4,$5,$6,
    $7,$8,$9,$10,
    $11,$12,$13,$14,
    $15,$16,$17,$18,
    $19,$20,$21
)
"""


async def db_writer():
    pool = await create_pool()
    buffer = []

    try:
        while True:
            try:
                item = await asyncio.wait_for(
                    tick_queue.get(),
                    timeout=FLUSH_INTERVAL
                )

                buffer.append(item)

                if len(buffer) >= BATCH_SIZE:
                    await flush(pool, buffer)
                    buffer.clear()

            except asyncio.TimeoutError:
                # 🔥 Flush partial batch every 1 second
                if buffer:
                    await flush(pool, buffer)
                    buffer.clear()

            except Exception as e:
                print(f"DB Writer Inner Error: {e}")

    except asyncio.CancelledError:
        # Graceful shutdown
        if buffer:
            await flush(pool, buffer)
        await pool.close()
        print("DB Writer stopped gracefully.")

    except Exception as e:
        print(f"DB Writer Fatal Error: {e}")
        await pool.close()


async def flush(pool, buffer):
    if not buffer:
        return

    tasks = []

    for row in buffer:
        try:
            ts = row["timestamp"]
            if ts is None:
                continue
            if ts.year < 2000:
                continue

            IST = timezone(timedelta(hours=5, minutes=30))

            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=IST)

            # print("\nDB WRITER BROADCAST")
            # print("Symbol:", row["symbol"])
            # print("Active WS Symbols:", list(manager.active_connections.keys()))

            tasks.append(
                manager.broadcast(
                    row["symbol"],
                    {
                    "time": ts.timestamp(),
                    "value": float(row["curr_price"]),
                    "is_gap": row["is_gap"],
                    "vol_change": row["vol_change"],
                    "direction": row["direction"],
                    "prev_price": row["prev_price"]
                    }
                )
            )

        except Exception as e:
            print("Broadcast preparation error:", e)

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    records = []

    for row in buffer:
        records.append((
            row["instrument_token"],
            row["symbol"],
            row["expiry_date"],
            row["strike"],
            row["option_type"],
            row["timestamp"],
            row["prev_price"],
            row["curr_price"],
            row["price_jump"],
            row["direction"],
            row["prev_volume"],
            row["curr_volume"],
            row["vol_change"],
            row["time_diff"],
            row["best_bid"],
            row["best_ask"],
            row["spread"],
            row["spread_pct"],
            row["only_gap"],
            row["gap_with_spread"],
            row["is_gap"]
        ))

    async with pool.acquire() as conn:
        await conn.executemany(INSERT_QUERY, records)
    logger.info(f"Inserted {len(buffer)} rows in to DB")
    # print("Inserted symbols in this batch:")
    # for r in buffer:
    #     print(" -", r["symbol"])




