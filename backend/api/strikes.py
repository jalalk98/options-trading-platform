# backend/api/strikes.py

from fastapi import APIRouter, Request
import asyncpg
from config.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

router = APIRouter()

@router.get("/strikes")
async def get_strikes():

    conn = await asyncpg.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    rows = await conn.fetch("""
        SELECT DISTINCT symbol, strike, option_type
        FROM gap_ticks
        ORDER BY strike
    """)

    await conn.close()

    return [
        {
            "symbol": r["symbol"],
            "display": f'{int(r["strike"])} {r["option_type"]}'
        }
        for r in rows
    ]

@router.get("/resolve-symbol")
async def resolve_symbol(display: str):

    strike, option_type = display.split()

    conn = await asyncpg.connect(
        host=DB_HOST,
        port=int(DB_PORT),
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    row = await conn.fetchrow(
        """
        SELECT symbol
        FROM gap_ticks
        WHERE strike = $1 AND option_type = $2
        LIMIT 1
        """,
        float(strike),
        option_type
    )

    await conn.close()

    if row:
        return {"symbol": row["symbol"]}

    return {"symbol": None}

@router.get("/history/{symbol}")
async def get_history(symbol: str, request: Request):

    pool = request.app.state.pool

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT timestamp, curr_price
            FROM gap_ticks
            WHERE symbol = $1
            AND timestamp >= CURRENT_DATE
            ORDER BY timestamp ASC
        """, symbol)

    return [
        {
            "time": row["timestamp"].timestamp(),
            "value": float(row["curr_price"])
        }
        for row in rows
    ]
