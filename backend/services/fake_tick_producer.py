import asyncio
import random
from datetime import datetime, timezone
from backend.services.tick_queue import tick_queue


async def fake_tick_producer():
    print("🚀 Fake tick producer started")

    price = 220.0  # starting price

    while True:
        # Random walk
        price += random.uniform(-1, 2)

        fake_tick = {
            "instrument_token": 123456,
            "symbol": "TEST_SYMBOL",

            # ✅ FIXED VALUES
            "expiry_date": datetime(2026, 2, 27).date(),
            "strike": 22000,
            "option_type": "CE",

            "timestamp": datetime.utcnow(),

            "prev_price": price - 1,
            "curr_price": price,
            "price_jump": 0,
            "direction": "UP",
            "prev_volume": 0,
            "curr_volume": 0,
            "vol_change": 0,
            "time_diff": 0,
            "best_bid": price - 1,
            "best_ask": price + 1,
            "spread": 2,
            "spread_pct": 0,
            "only_gap": False,
            "gap_with_spread": False,
            "is_gap": False,
        }

        await tick_queue.put(fake_tick)

        await asyncio.sleep(1)  # 1 tick per second
