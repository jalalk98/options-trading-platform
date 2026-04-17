# backend/api/chart_server.py

import asyncio
import asyncpg
import datetime as _dt
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from config.credentials import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)
from backend.services.redis_streamer import redis_streamer
from fastapi.staticfiles import StaticFiles
from datetime import timezone, timedelta, datetime
from backend.api.strikes import router as strikes_router, prewarm_strikes_cache, refresh_b2_cache
from backend.api.sl import router as sl_router
from backend.api.streaming import manager
from fastapi import WebSocketDisconnect

    
app = FastAPI()

app.include_router(strikes_router, prefix="/api")
app.include_router(sl_router, prefix="/api")

app.add_middleware(GZipMiddleware, minimum_size=500)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

async def create_pool():
    return await asyncpg.create_pool(
        host=DB_HOST,
        port=int(DB_PORT),
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        min_size=2,
        max_size=8,
        max_inactive_connection_lifetime=300,   # close idle connections after 5 min → pool shrinks back to min_size
        server_settings={"statement_timeout": "30000"},
    )


@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()


async def _load_pending_fills_on_startup(pool):
    """Load today's qualifying jumps from DB into _pending_fills so fill tracking
    survives server restarts without losing intraday context."""
    from backend.services.redis_streamer import (
        _pending_fills, _get_price_filter, _get_jump_threshold
    )
    try:
        today = _dt.date.today()
        async with pool.acquire() as conn:
            await conn.execute("SET statement_timeout = 0")
            rows = await conn.fetch("""
                SELECT symbol,
                       curr_price,
                       prev_price,
                       price_jump,
                       floor(EXTRACT(EPOCH FROM timestamp) / 5) * 5 AS bucket_epoch
                FROM gap_ticks
                WHERE timestamp >= $1::date + TIME '09:15:03'
                  AND timestamp <= $1::date + TIME '15:35:00'
                  AND curr_price  > 0
                  AND ABS(price_jump) > 3
                ORDER BY timestamp ASC
            """, today)

        count = 0
        for row in rows:
            sym  = row['symbol']
            pj   = float(row['price_jump'] or 0)
            curr = float(row['curr_price'])
            prev = float(row['prev_price'] or 0)

            if curr >= _get_price_filter(sym) or abs(pj) <= _get_jump_threshold(sym):
                continue

            direction  = 'UP' if pj > 0 else 'DOWN'
            # bucket_epoch from SQL = EXTRACT(EPOCH FROM IST-naive-as-UTC timestamp)
            # = ts.timestamp() + 19800 on an IST server — matches strikes.py convention
            # and the bucket stored in API-loaded _jumpMarkers on the frontend.
            ist_bucket = int(row['bucket_epoch'])

            if sym not in _pending_fills:
                _pending_fills[sym] = []
            _pending_fills[sym].append({
                "bucket"   : ist_bucket,
                "direction": direction,
                "pre_price": prev,
                "is_first" : False,
            })
            count += 1

        print(f"[startup] Loaded {count} pending fills from today's DB", flush=True)
    except Exception as e:
        print(f"[startup] Could not load pending fills: {e}", flush=True)


@app.on_event("startup")
async def startup():
    app.state.pool = await create_pool()

    asyncio.create_task(redis_streamer())
    await prewarm_strikes_cache(app.state.pool)  # blocking — cache must be warm before serving requests
    asyncio.create_task(_load_pending_fills_on_startup(app.state.pool))

    # Ensure jump-lookup index exists (no-op if already present).
    # Partial predicate WHERE curr_price > 0 AND ABS(price_jump) > 3 means only
    # big-jump rows are indexed — tiny index, fast scan for Query 1 in get_jump_history().
    # Runs in background so it doesn't block startup (14GB table can take minutes to index).
    async def _ensure_jump_index():
        async with app.state.pool.acquire() as _conn:
            await _conn.execute("SET statement_timeout = 0")
            await _conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_gap_ticks_jump_lookup
                ON gap_ticks (symbol, timestamp)
                INCLUDE (curr_price, prev_price, price_jump)
                WHERE curr_price > 0 AND ABS(price_jump) > 3
            """)
    asyncio.create_task(_ensure_jump_index())

    # B2 manifest is slow (blocking S3 call) — skip during market hours to
    # avoid stalling the event loop while charts are actively being used.
    def _in_market_hours() -> bool:
        ist = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        mins = ist.hour * 60 + ist.minute
        return 9 * 60 + 15 <= mins < 15 * 60 + 30

    async def _b2_refresh_loop():
        while True:
            if _in_market_hours():
                print("[B2 cache] market hours — skipping refresh", flush=True)
            else:
                try:
                    await refresh_b2_cache(app.state.pool)
                except Exception as e:
                    print(f"[B2 cache] refresh error: {e}")
            await asyncio.sleep(300)  # check every 5 minutes

    asyncio.create_task(_b2_refresh_loop())

@app.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):

    print("\nCLIENT CONNECTING TO SYMBOL:", symbol)

    await manager.connect(symbol, websocket)

    print("Active WS symbols after connect:",
          list(manager.active_connections.keys()))

    try:
        while True:
            await asyncio.sleep(20)
            await websocket.send_json({"type": "ping"})  # heartbeat — prevents idle timeout
    except (WebSocketDisconnect, Exception):
        manager.disconnect(symbol, websocket)


app.mount("/", StaticFiles(directory="frontend/ui", html=True), name="static")