# backend/api/chart_server.py

import asyncio
import asyncpg
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from config.credentials import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)

from fastapi.staticfiles import StaticFiles
from datetime import timezone, timedelta, datetime
from backend.api.strikes import router as strikes_router
from backend.api.streaming import manager
from backend.services.db_writer import db_writer
from frontend.ui.websocket_setup import run_websocket
from backend.services.instrument_registry import get_tokens_by_strikes
from fastapi import WebSocketDisconnect
from backend.services.fake_tick_producer import fake_tick_producer


app = FastAPI()

app.include_router(strikes_router, prefix="/api")

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

    # Start DB writer
    asyncio.create_task(db_writer())

    # asyncio.create_task(fake_tick_producer())

    # 🔥 Prepare market tokens
    strikes = ["24500-CE","24550-CE","24600-CE","24400-CE","24300-CE","24200-CE","24500-PE","24600-PE","24400-PE","24700-CE","24650-CE"]
    expiry = "10-3-2026"
    index_name = "NIFTY"

    tokens = get_tokens_by_strikes(strikes, expiry, index_name)

    if tokens:
        asyncio.create_task(run_websocket(tokens))
    else:
        print("No valid tokens found. Market feed not started.")


@app.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):

    print("\nCLIENT CONNECTING TO SYMBOL:", symbol)

    await manager.connect(symbol, websocket)

    print("Active WS symbols after connect:",
          list(manager.active_connections.keys()))

    try:
        while True:
            # Keep connection alive
            await asyncio.sleep(60)
    except WebSocketDisconnect:
        print("Client disconnected from:", symbol)
        manager.disconnect(symbol, websocket)
        print("Remaining WS symbols:",
        list(manager.active_connections.keys()))


app.mount("/", StaticFiles(directory="frontend/ui", html=True), name="static")