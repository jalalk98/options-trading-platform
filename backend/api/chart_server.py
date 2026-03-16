# backend/api/chart_server.py

import asyncio
import asyncpg
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from config.credentials import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)
from backend.services.redis_streamer import redis_streamer
from fastapi.staticfiles import StaticFiles
from datetime import timezone, timedelta, datetime
from backend.api.strikes import router as strikes_router, prewarm_strikes_cache
from backend.api.sl import router as sl_router
from backend.api.streaming import manager
from fastapi import WebSocketDisconnect

    
app = FastAPI()

app.include_router(strikes_router, prefix="/api")
app.include_router(sl_router, prefix="/api")

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
        min_size=1,
        max_size=5,
    )


@app.on_event("startup")
async def startup():
    app.state.pool = await create_pool()

    asyncio.create_task(redis_streamer())
    await prewarm_strikes_cache(app.state.pool)  # blocking — cache must be warm before serving requests

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