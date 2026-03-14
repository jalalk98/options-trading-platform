# backend/api/strikes.py

from fastapi import APIRouter, Request
from pydantic import BaseModel
from backend.services.websocket_handler import kite1
from config.credentials import KITE_API_KEY, KITE_ACCESS_TOKEN

router = APIRouter()


@router.get("/strikes")
async def get_strikes(request: Request):

    pool = request.app.state.pool

    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT symbol, strike, option_type, expiry_date
            FROM gap_ticks
            ORDER BY expiry_date DESC, strike
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

        SELECT
            bucket,
            (ARRAY_AGG(curr_price ORDER BY timestamp))[1] AS open,
            MAX(curr_price) AS high,
            MIN(curr_price) AS low,
            (ARRAY_AGG(curr_price ORDER BY timestamp DESC))[1] AS close
        FROM (
            SELECT
                FLOOR(EXTRACT(EPOCH FROM timestamp)/5)*5 AS bucket,
                curr_price,
                timestamp
            FROM gap_ticks
            WHERE symbol = $1
            AND timestamp >= NOW() - INTERVAL '1 day'
        ) t
        GROUP BY bucket
        ORDER BY bucket ASC

        """, symbol)

    return [
        {
            "time": int(row["bucket"]),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"])
        }
        for row in rows
    ]

class SLOrder(BaseModel):
    symbol: str
    price: float
    side: str   # BUY or SELL

@router.post("/place-sl-order")
async def place_sl_order(order: SLOrder):

    try:
        print(f"Order request received: {order.symbol} {order.side} {order.price}")
        def round_to_tick(price, tick_size=0.05):
            return round(round(price / tick_size) * tick_size, 2)

        trigger_buffer = 0.10
        price = round_to_tick(order.price)

        if order.side == "BUY":

            trigger = round_to_tick(price - trigger_buffer)

            await kite1.hard_code_regular_buy_order(
                exchange="NFO",
                trade_symbol=order.symbol,
                qty=65,
                price=price,
                trig_price=trigger,
                api_key=KITE_API_KEY,
                access_token=KITE_ACCESS_TOKEN
            )

        elif order.side == "SELL":

            trigger = round_to_tick(price + trigger_buffer)

            await kite1.hard_code_regular_sell_order(
                exchange="NFO",
                trade_symbol=order.symbol,
                qty=65,
                stop_loss_price=price,
                trig_price=trigger,
                api_key=KITE_API_KEY,
                access_token=KITE_ACCESS_TOKEN
            )

        return {
            "status": "success",
            "price": price,
            "trigger": trigger
        }

    except Exception as e:
        return {"status": "error", "message": str(e)}