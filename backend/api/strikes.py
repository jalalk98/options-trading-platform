# backend/api/strikes.py

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/strikes")
async def get_strikes(request: Request):

    pool = request.app.state.pool

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT symbol, strike, option_type, expiry_date
            FROM gap_ticks
            ORDER BY expiry_date, strike
        """)

    return [
        {
            "symbol": r["symbol"],
            "display": f'{r["expiry_date"]} | {int(r["strike"])} {r["option_type"]}'
        }
        for r in rows
    ]


@router.get("/resolve-symbol")
async def resolve_symbol(display: str, request: Request):

    try:
        # Example display:
        # "2026-03-02 | 24600 CE"

        parts = display.split("|")
        expiry = parts[0].strip()
        strike_part = parts[1].strip()

        strike, option_type = strike_part.split()

        pool = request.app.state.pool

        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT symbol
                FROM gap_ticks
                WHERE strike = $1
                AND option_type = $2
                AND expiry_date = $3
                LIMIT 1
                """,
                float(strike),
                option_type,
                expiry
            )

        if row:
            return {"symbol": row["symbol"]}

        return {"symbol": None}

    except Exception as e:
        return {"symbol": None}


@router.get("/history/{symbol}")
async def get_history(symbol: str, request: Request):

    pool = request.app.state.pool

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT timestamp, curr_price, is_gap, vol_change
            FROM gap_ticks
            WHERE symbol = $1
            AND timestamp > '2000-01-01'
            ORDER BY timestamp ASC
        """, symbol)
    
    return [
        {
            "time": row["timestamp"].timestamp(),
            "value": float(row["curr_price"]),
            "is_gap": row["is_gap"],
            "vol_change": row["vol_change"]
        }
        for row in rows
    ]
