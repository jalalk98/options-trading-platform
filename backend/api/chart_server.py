# backend/api/chart_server.py

import asyncio
import asyncpg
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


@app.on_event("startup")
async def startup():
    app.state.pool = await create_pool()

    asyncio.create_task(redis_streamer())
    await prewarm_strikes_cache(app.state.pool)  # blocking — cache must be warm before serving requests

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