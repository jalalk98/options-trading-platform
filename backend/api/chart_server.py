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


@app.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):
    await websocket.accept()

    pool = websocket.app.state.pool

    last_timestamp = None

    while True:
        async with pool.acquire() as conn:

            if last_timestamp is None:
                # get latest timestamp from DB
                row = await conn.fetchrow(
                    """
                    SELECT MAX(timestamp) as max_ts
                    FROM gap_ticks
                    WHERE symbol = $1
                    """,
                    symbol
                )

                if row and row["max_ts"]:
                    last_timestamp = row["max_ts"]
                else:
                    await asyncio.sleep(0.5)
                    continue

            rows = await conn.fetch(
                """
                SELECT timestamp, curr_price
                FROM gap_ticks
                WHERE symbol = $1
                AND timestamp > $2
                ORDER BY timestamp ASC
                """,
                symbol,
                last_timestamp
            )

            for row in rows:
                last_timestamp = row["timestamp"]

                await websocket.send_json({
                    "time": row["timestamp"].timestamp(),
                    "value": float(row["curr_price"])
                })

        await asyncio.sleep(0.5)



app.mount("/", StaticFiles(directory="frontend/ui", html=True), name="static")