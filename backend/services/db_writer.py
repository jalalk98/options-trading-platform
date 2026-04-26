# backend/services/db_writer.py

from config.logging_config import logger
import asyncio
import asyncpg
from backend.core.redis_client import redis_client
import json
import time as _time
import requests as _requests
from pathlib import Path
from backend.api.streaming import manager
from collections import defaultdict
from datetime import timezone, timedelta, datetime, date
from config.credentials import (
    DB_HOST,
    DB_PORT,
    DB_NAME,
    DB_USER,
    DB_PASSWORD,
)

BATCH_SIZE = 5
FLUSH_INTERVAL = 0.07  # seconds

# ── Resilience: rate limited Telegram alerting ────────────────
_db_error_count    = 0
_db_last_alert_at  = 0.0
_DB_ALERT_INTERVAL = 300  # seconds between repeat alerts (5 min)

def _send_db_telegram(text: str) -> None:
    """Send Telegram alert for db_writer failures.
    Reads credentials from ~/.kite_secrets (same as tick_collector).
    """
    secrets_path = Path.home() / ".kite_secrets"
    try:
        secrets = {}
        for line in secrets_path.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, _, v = line.partition("=")
                secrets[k.strip()] = v.strip()
        token   = secrets.get("TELEGRAM_BOT_TOKEN")
        chat_id = secrets.get("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            print("Telegram credentials missing in ~/.kite_secrets")
            return
        _requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": text},
            timeout=5,
        )
    except Exception as e:
        print(f"Telegram alert failed: {e}")


def _maybe_alert(error: Exception, context: str = "") -> None:
    """
    Rate limited alert — fires immediately on first error
    then at most once every 5 minutes.
    """
    global _db_error_count, _db_last_alert_at
    _db_error_count += 1
    now = _time.monotonic()

    if _db_error_count == 1 or \
       (now - _db_last_alert_at) >= _DB_ALERT_INTERVAL:
        _send_db_telegram(
            f"🔴 DB Writer Error\n"
            f"Context: {context}\n"
            f"Error: {str(error)[:300]}\n"
            f"Total failures this session: {_db_error_count}\n"
            f"Next alert in 5 min if still failing"
        )
        _db_last_alert_at = now


def _reset_alert_count() -> None:
    """Call after successful flush to reset error counter."""
    global _db_error_count, _db_last_alert_at
    if _db_error_count > 0:
        _send_db_telegram(
            f"✅ DB Writer recovered\n"
            f"Ticks flowing normally again\n"
            f"Total errors this incident: {_db_error_count}"
        )
        _db_error_count   = 0
        _db_last_alert_at = 0.0

def _is_market_hours_ist() -> bool:
    """
    Return True if current IST time is within market hours
    (Mon-Fri, 09:15-15:30) and today is not a market holiday.
    Reads holidays from ~/.trading_holidays
    (same file used by websocket_handler.py).
    """
    now = datetime.now(_IST)
    if now.weekday() >= 5:
        return False
    hhmm = now.hour * 100 + now.minute
    if not (915 <= hhmm <= 1530):
        return False
    today         = now.strftime("%Y-%m-%d")
    holidays_path = Path.home() / ".trading_holidays"
    try:
        for line in holidays_path.read_text().splitlines():
            if line.startswith(today):
                return False
    except Exception:
        pass  # if file missing → assume not a holiday
    return True


async def health_monitor(pool):
    """
    Every 60 seconds during market hours:
    Check if gap_ticks received data in last 2 minutes.
    Alert via Telegram if no data found.
    Alert again on recovery.
    """
    _health_alert_sent = False

    while True:
        await asyncio.sleep(60)
        try:
            if not _is_market_hours_ist():
                _health_alert_sent = False
                continue

            async with pool.acquire() as conn:
                # Query today's partition directly — avoids the 40ms planning overhead
                # of scanning all 66 partition descriptors.  Falls back to the parent
                # table if today's partition doesn't exist (e.g., cron failed overnight).
                # No midnight-rollover risk: health_monitor only runs 09:15-15:30 IST.
                today_partition = (
                    "gap_ticks_"
                    + datetime.now(_IST).date().strftime("%Y_%m_%d")
                )
                try:
                    last_tick = await conn.fetchval(
                        f"SELECT MAX(timestamp) FROM {today_partition}"   # nosec: date-only f-string
                        " WHERE timestamp > NOW() - INTERVAL '2 minutes'"
                    )
                except Exception:
                    last_tick = await conn.fetchval(
                        "SELECT MAX(timestamp) FROM gap_ticks"
                        " WHERE timestamp > NOW() - INTERVAL '2 minutes'"
                    )

            if last_tick is None:
                if not _health_alert_sent:
                    _send_db_telegram(
                        f"🔴 HEALTH CHECK FAILED\n"
                        f"No ticks in gap_ticks for 2+ minutes\n"
                        f"Market is open — check immediately:\n"
                        f"db_writer, tick_collector, Redis, PostgreSQL"
                    )
                    _health_alert_sent = True
            else:
                if _health_alert_sent:
                    _send_db_telegram(
                        f"✅ Health check recovered\n"
                        f"Ticks flowing again\n"
                        f"Last tick: {last_tick}"
                    )
                _health_alert_sent = False

        except Exception as e:
            print(f"Health monitor error: {e}")


# ── Pre-aggregated tables ──────────────────────────────────────────────────
# candles_5s and gap_events are maintained alongside gap_ticks so that
# history and gap queries can read from small pre-aggregated tables (~375 rows
# per symbol per day) instead of scanning all raw ticks (~30k rows).
# To revert to gap_ticks-only queries, set _USE_FAST_TABLES=False in strikes.py.

_PG_EPOCH = datetime(1970, 1, 1)

# Candle gap threshold — minimum gap up to trigger monitoring
CANDLE_GAP_THRESHOLD = 0.05

# IST timezone for session date calculation
_IST = timezone(timedelta(hours=5, minutes=30))

# Candle level cache — tracks state per symbol between flushes
# Keys per symbol:
#   close             ← previous candle close
#   gap_ref_price     ← current gap reference (carried forward)
#   gap_ref_bucket    ← bucket when gap formed
#   candles_above_gap ← consecutive closes above gap
#   cum_delta         ← cumulative delta for session
#   oi_close          ← OI at last candle close
#   session_date      ← IST date of last candle (for reset)
_candle_cache = {}

def _pg_bucket(ts: datetime) -> int:
    """Compute the same bucket value PostgreSQL uses:
    FLOOR(EXTRACT(EPOCH FROM timestamp)/5)*5 for a timestamp without timezone."""
    return int((ts.replace(tzinfo=None) - _PG_EPOCH).total_seconds() // 5) * 5


def _candle_session_date(bucket: int) -> date:
    """Convert bucket (unix seconds) to IST date."""
    return datetime.fromtimestamp(bucket, tz=_IST).date()


def _build_fast_records(buffer):
    """Build (candle_records, gap_records) from a flush buffer.

    candle_records: 24 fields including gap monitoring
    gap_records:    (symbol, bucket, direction, prev_price,
                     curr_price, vol_change)
    """
    candle_groups = defaultdict(list)
    gap_map       = {}

    for row in buffer:
        ts = row.get("timestamp")
        if not isinstance(ts, datetime):
            continue
        price = row.get("curr_price")
        if price is None:
            continue
        sym    = row["symbol"]
        bucket = _pg_bucket(ts)
        candle_groups[(sym, bucket)].append(row)

        if row.get("is_gap") and (sym, bucket) not in gap_map:
            gap_map[(sym, bucket)] = (
                sym, bucket,
                row.get("direction"),
                row.get("prev_price"),
                float(price),
                int(row.get("vol_change") or 0),
            )

    candle_records = []

    for (sym, bucket), rows in candle_groups.items():
        rows.sort(key=lambda r: r["timestamp"])

        prices = [float(r["curr_price"]) for r in rows]
        o      = prices[0]
        h      = max(prices)
        l      = min(prices)
        c      = prices[-1]

        # ── Volume and delta (tick rule) ──────────────────────
        # Flat ticks (price_jump=0) carry forward last direction
        # instead of being dropped — gives accurate delta
        volume          = sum(
            int(r.get("vol_change") or 0) for r in rows
        )
        buy_vol         = 0
        sell_vol        = 0
        _last_direction = None  # 'buy' or 'sell'

        for r in rows:
            jump = r.get("price_jump") or 0
            vol  = int(r.get("vol_change") or 0)

            if jump > 0:
                _last_direction = "buy"
            elif jump < 0:
                _last_direction = "sell"
            # else: flat → keep _last_direction as is

            if _last_direction == "buy":
                buy_vol  += vol
            elif _last_direction == "sell":
                sell_vol += vol
            # if _last_direction is still None
            # (very first tick is flat) → skip

        delta = buy_vol - sell_vol

        # ── OI and depth at candle close (last tick) ──────────
        last_row        = rows[-1]
        oi_close        = last_row.get("oi")
        depth_imbalance = last_row.get("depth_imbalance")

        # ── Session reset ─────────────────────────────────────
        candle_date = _candle_session_date(bucket)
        cache       = _candle_cache.get(sym, {})
        cached_date = cache.get("session_date")

        if cached_date != candle_date:
            cache = {
                "close":             None,
                "gap_ref_price":     None,
                "gap_ref_bucket":    None,
                "candles_above_gap": 0,
                "cum_delta":         0.0,
                "oi_close":          None,
                "session_date":      candle_date,
            }

        # ── Previous candle close ─────────────────────────────
        prev_close = cache.get("close")

        # ── Candle gap detection ──────────────────────────────
        candle_gap    = None
        is_candle_gap = False

        if prev_close is not None:
            candle_gap    = round(o - prev_close, 4)
            is_candle_gap = candle_gap >= CANDLE_GAP_THRESHOLD

        # ── Gap reference ─────────────────────────────────────
        if is_candle_gap:
            gap_ref_price  = o       # open of the gapping candle
            gap_ref_bucket = bucket
        else:
            gap_ref_price  = cache.get("gap_ref_price")
            gap_ref_bucket = cache.get("gap_ref_bucket")

        # ── Distance from gap ─────────────────────────────────
        dist_from_gap     = None
        dist_from_gap_pct = None
        closed_above_gap  = None

        if gap_ref_price is not None:
            dist_from_gap     = round(c - gap_ref_price, 4)
            dist_from_gap_pct = round(
                (dist_from_gap / gap_ref_price) * 100, 4
            )
            closed_above_gap  = c > gap_ref_price

        # ── Consecutive candles above gap ─────────────────────
        candles_above_gap = cache.get("candles_above_gap", 0)

        if closed_above_gap is True:
            candles_above_gap += 1
        elif closed_above_gap is False:
            candles_above_gap = 0

        seconds_above_gap = candles_above_gap * 5

        # ── Cumulative delta ──────────────────────────────────
        cum_delta = cache.get("cum_delta", 0.0) + delta

        # ── OI change ─────────────────────────────────────────
        prev_oi_close = cache.get("oi_close")
        oi_change     = None
        if oi_close is not None and prev_oi_close is not None:
            oi_change = oi_close - prev_oi_close

        # ── Update candle cache ───────────────────────────────
        _candle_cache[sym] = {
            "close":             c,
            "gap_ref_price":     gap_ref_price,
            "gap_ref_bucket":    gap_ref_bucket,
            "candles_above_gap": candles_above_gap,
            "cum_delta":         cum_delta,
            "oi_close":          oi_close,
            "session_date":      candle_date,
        }

        candle_records.append((
            sym, bucket,
            o, h, l, c,
            prev_close,
            candle_gap,
            is_candle_gap,
            gap_ref_price,
            gap_ref_bucket,
            dist_from_gap,
            dist_from_gap_pct,
            closed_above_gap,
            candles_above_gap,
            seconds_above_gap,
            volume,
            buy_vol,
            sell_vol,
            delta,
            cum_delta,
            oi_close,
            oi_change,
            depth_imbalance,
        ))

    return candle_records, list(gap_map.values())


_CANDLE_UPSERT = """
    INSERT INTO candles_5s (
        symbol, bucket,
        open, high, low, close,
        prev_close,
        candle_gap,
        is_candle_gap,
        gap_ref_price,
        gap_ref_bucket,
        dist_from_gap,
        dist_from_gap_pct,
        closed_above_gap,
        candles_above_gap,
        seconds_above_gap,
        volume,
        buy_vol,
        sell_vol,
        delta,
        cum_delta,
        oi_close,
        oi_change,
        depth_imbalance
    )
    VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
        $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
        $21,$22,$23,$24
    )
    ON CONFLICT (symbol, bucket) DO UPDATE SET
        high              = GREATEST(candles_5s.high, EXCLUDED.high),
        low               = LEAST(candles_5s.low,    EXCLUDED.low),
        close             = EXCLUDED.close,
        prev_close        = EXCLUDED.prev_close,
        candle_gap        = EXCLUDED.candle_gap,
        is_candle_gap     = EXCLUDED.is_candle_gap,
        gap_ref_price     = EXCLUDED.gap_ref_price,
        gap_ref_bucket    = EXCLUDED.gap_ref_bucket,
        dist_from_gap     = EXCLUDED.dist_from_gap,
        dist_from_gap_pct = EXCLUDED.dist_from_gap_pct,
        closed_above_gap  = EXCLUDED.closed_above_gap,
        candles_above_gap = EXCLUDED.candles_above_gap,
        seconds_above_gap = EXCLUDED.seconds_above_gap,
        volume            = EXCLUDED.volume,
        buy_vol           = EXCLUDED.buy_vol,
        sell_vol          = EXCLUDED.sell_vol,
        delta             = EXCLUDED.delta,
        cum_delta         = EXCLUDED.cum_delta,
        oi_close          = EXCLUDED.oi_close,
        oi_change         = EXCLUDED.oi_change,
        depth_imbalance   = EXCLUDED.depth_imbalance
"""

_GAP_EVENT_INSERT = """
    INSERT INTO gap_events (symbol, bucket, direction, prev_price, curr_price, vol_change)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (symbol, bucket) DO NOTHING
"""


async def create_pool():
    return await asyncpg.create_pool(
        host=DB_HOST,
        port=int(DB_PORT),
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        min_size=1,
        max_size=5,
        max_inactive_connection_lifetime=300,  # close idle connections after 5 min → drops to 0 outside trading hours
    )


INSERT_QUERY = """
INSERT INTO gap_ticks (
    instrument_token,
    symbol,
    expiry_date,
    strike,
    option_type,
    timestamp,
    last_trade_time,
    curr_price,
    last_quantity,
    average_price,
    curr_volume,
    oi,
    oi_day_high,
    oi_day_low,
    buy_quantity,
    sell_quantity,
    depth,
    bid_depth_qty,
    ask_depth_qty,
    depth_imbalance,
    prev_price,
    prev_volume,
    price_jump,
    direction,
    vol_change,
    time_diff,
    best_bid,
    best_ask,
    spread,
    spread_pct,
    only_gap,
    gap_with_spread,
    is_gap
)
VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
    $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
    $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
    $31,$32,$33
)
"""


async def db_writer():

    pool = await create_pool()

    # Start health monitor as background task
    asyncio.create_task(health_monitor(pool))

    buffer = []

    last_id = "$"  # Redis stream pointer

    try:

        while True:

            try:

                streams = await redis_client.xread(
                    {"ticks_stream": last_id},
                    count=BATCH_SIZE,
                    block=100
                )

                if not streams:
                    continue

                for stream_name, messages in streams:

                    for message_id, data in messages:

                        row = json.loads(data["data"])

                        # Convert timestamp strings back to datetime
                        if isinstance(row.get("timestamp"), str):
                            row["timestamp"] = datetime.fromisoformat(row["timestamp"])

                        if isinstance(row.get("expiry_date"), str):
                            row["expiry_date"] = datetime.fromisoformat(row["expiry_date"])

                        if isinstance(row.get("last_trade_time"), str):
                            row["last_trade_time"] = datetime.fromisoformat(row["last_trade_time"])

                        buffer.append(row)
                        last_id = message_id

                if len(buffer) >= BATCH_SIZE:
                    await flush(pool, buffer)
                    buffer.clear()

            except Exception as e:
                print(f"DB Writer Inner Error: {e}")

    except asyncio.CancelledError:

        if buffer:
            await flush(pool, buffer)

        await pool.close()

        print("DB Writer stopped gracefully.")

    except Exception as e:

        print(f"DB Writer Fatal Error: {e}")

        await pool.close()


async def flush(pool, buffer):

    if not buffer:
        return

    records = []

    for row in buffer:
        try:
            records.append((
                row["instrument_token"],
                row["symbol"],
                row["expiry_date"],
                row["strike"],
                row["option_type"],
                row["timestamp"],
                row.get("last_trade_time"),
                row.get("curr_price"),
                row.get("last_quantity"),
                row.get("average_price"),
                row.get("curr_volume"),
                row.get("oi"),
                row.get("oi_day_high"),
                row.get("oi_day_low"),
                row.get("buy_quantity"),
                row.get("sell_quantity"),
                json.dumps(row.get("depth"))
                    if row.get("depth") else None,
                row.get("bid_depth_qty"),
                row.get("ask_depth_qty"),
                row.get("depth_imbalance"),
                row.get("prev_price"),
                row.get("prev_volume"),
                row.get("price_jump"),
                row.get("direction"),
                row.get("vol_change"),
                row.get("time_diff"),
                row.get("best_bid"),
                row.get("best_ask"),
                row.get("spread"),
                row.get("spread_pct"),
                row.get("only_gap"),
                row.get("gap_with_spread"),
                row.get("is_gap"),
            ))
        except Exception as e:
            print(f"Row build error (skipping row): {e}")
            continue

    candle_records, gap_records = _build_fast_records(buffer)

    async with pool.acquire() as conn:
        async with conn.transaction():

            # ── gap_ticks ─────────────────────────────────────────
            try:
                await conn.executemany(INSERT_QUERY, records)
            except Exception as e:
                _maybe_alert(e, context=f"gap_ticks INSERT "
                    f"({len(records)} rows, "
                    f"first symbol: {buffer[0].get('symbol','?')})")
                raise

            # ── tracked_symbols ───────────────────────────────────
            try:
                unique_symbols = {
                    (r["symbol"], r["strike"],
                     r["option_type"], r["expiry_date"])
                    for r in buffer
                }
                await conn.executemany("""
                    INSERT INTO tracked_symbols
                        (symbol, strike, option_type, expiry_date)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (symbol) DO NOTHING
                """, list(unique_symbols))
            except Exception as e:
                _maybe_alert(e, context=f"tracked_symbols INSERT "
                    f"({len(buffer)} rows, "
                    f"first symbol: {buffer[0].get('symbol','?')})")
                raise

            # ── candles_5s ────────────────────────────────────────
            try:
                if candle_records:
                    await conn.executemany(
                        _CANDLE_UPSERT, candle_records
                    )
            except Exception as e:
                _maybe_alert(e, context=f"candles_5s UPSERT "
                    f"({len(candle_records)} rows, "
                    f"first symbol: {buffer[0].get('symbol','?')})")
                raise

            # ── gap_events ────────────────────────────────────────
            try:
                if gap_records:
                    await conn.executemany(
                        _GAP_EVENT_INSERT, gap_records
                    )
            except Exception as e:
                _maybe_alert(e, context=f"gap_events INSERT "
                    f"({len(gap_records)} rows, "
                    f"first symbol: {buffer[0].get('symbol','?')})")
                raise

            _reset_alert_count()  # all four writes succeeded

    logger.info(f"Inserted {len(records)} rows into DB")