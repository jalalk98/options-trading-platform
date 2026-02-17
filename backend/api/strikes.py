# backend/api/strikes.py

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/strikes")
async def get_strikes(request: Request):

    pool = request.app.state.pool

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT symbol, strike, option_type
            FROM gap_ticks
            ORDER BY strike
        """)

    return [
        {
            "symbol": r["symbol"],
            "display": f'{int(r["strike"])} {r["option_type"]}'
        }
        for r in rows
    ]


@router.get("/resolve-symbol")
async def resolve_symbol(display: str, request: Request):

    strike, option_type = display.split()

    pool = request.app.state.pool

    async with pool.acquire() as conn:
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
