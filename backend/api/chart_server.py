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

    # 🔥 Prepare market tokens
    strikes = ["25500-CE", "25600-CE", "25700-PE", "25600-PE"]
    expiry = "17-2-2026"
    index_name = "NIFTY"

    tokens = get_tokens_by_strikes(strikes, expiry, index_name)

    if tokens:
        asyncio.create_task(run_websocket(tokens))
    else:
        print("No valid tokens found. Market feed not started.")


@app.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):

    await manager.connect(symbol, websocket)

    try:
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(symbol, websocket)

app.mount("/", StaticFiles(directory="frontend/ui", html=True), name="static")