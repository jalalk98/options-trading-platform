"""
fyers_db_writer.py
------------------
Consumes from fyers_ticks_stream (Redis) via consumer group fyers_writer_group
and writes raw ticks to the fyers_ticks PostgreSQL table.

Mirrors the semantics of backend/services/db_writer.py:
  - BATCH_SIZE = 5, FLUSH_INTERVAL ≈ 0.07s (same as Zerodha writer)
  - ACK Redis messages only after successful DB commit
  - ON CONFLICT DO NOTHING (stream_id as idempotency key)
  - Also UPSERTs display_symbol into tracked_symbols so /api/strikes shows FY: entries

DDL is created at startup (CREATE TABLE / INDEX IF NOT EXISTS).
The fyers_ticks table is NOT partitioned — 8 symbols × test period = tiny table.
"""

import asyncio
import json
import logging
import re
import sys
from datetime import datetime, date, timedelta, timezone
from typing import Optional

import asyncpg

from backend.core.redis_client import redis_client
from config.credentials import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
from backend.services.fyers_collector.config import (
    FYERS_REDIS_STREAM, FYERS_REDIS_GROUP, FYERS_DB_TABLE,
)

logger = logging.getLogger(__name__)

BATCH_SIZE     = 5
FLUSH_INTERVAL = 0.07  # seconds — same as Zerodha db_writer

_CONSUMER = "fyers_writer_1"

_REQUIRED_FIELDS = ["fyers_symbol", "display_symbol", "timestamp", "source"]

# ── DDL ──────────────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {FYERS_DB_TABLE} (
    id             BIGSERIAL,
    fyers_symbol   TEXT                        NOT NULL,
    display_symbol TEXT                        NOT NULL,
    timestamp      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    ltt            TIMESTAMP WITHOUT TIME ZONE,
    ltp            DOUBLE PRECISION,
    ltq            BIGINT,
    volume         BIGINT,
    oi             BIGINT,
    bid_price      DOUBLE PRECISION,
    ask_price      DOUBLE PRECISION,
    bid_qty        BIGINT,
    ask_qty        BIGINT,
    depth          JSONB,
    stream_id      TEXT,
    source         TEXT NOT NULL DEFAULT 'fyers'
);
"""

_CREATE_INDEXES_SQL = [
    f"CREATE INDEX IF NOT EXISTS idx_fyers_ticks_symbol_ts "
    f"ON {FYERS_DB_TABLE} (display_symbol, timestamp DESC);",

    f"CREATE INDEX IF NOT EXISTS idx_fyers_ticks_ts "
    f"ON {FYERS_DB_TABLE} (timestamp);",

    f"CREATE UNIQUE INDEX IF NOT EXISTS idx_fyers_ticks_stream_id "
    f"ON {FYERS_DB_TABLE} (stream_id) WHERE stream_id IS NOT NULL;",
]

# ── INSERT query (column order must match _build_record) ─────────────────────

_INSERT_SQL = f"""
INSERT INTO {FYERS_DB_TABLE} (
    fyers_symbol, display_symbol, timestamp, ltt,
    ltp, ltq, volume, oi,
    bid_price, ask_price, bid_qty, ask_qty,
    depth, stream_id, source
) VALUES (
    $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15
)
ON CONFLICT DO NOTHING
"""

# ── Tracked symbols UPSERT (FY:-prefixed display_symbol → existing dropdown) ─

_TRACKED_SYM_SQL = """
INSERT INTO tracked_symbols (symbol, strike, option_type, expiry_date)
VALUES ($1, $2, $3, $4)
ON CONFLICT (symbol) DO NOTHING
"""

# ── Symbol parser for tracked_symbols insert ─────────────────────────────────

_MONTH_MAP = {m: i for i, m in enumerate(
    ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"], 1
)}
_OPT_RE = re.compile(
    r"^(?:FY:)?(NIFTY|BANKNIFTY|FINNIFTY|MIDCPNIFTY|SENSEX)"
    r"(\d{2})"
    r"([A-Z]{3}|\d{1,2}\d{2})"
    r"(\d+)"
    r"(CE|PE)$"
)


def _parse_display_symbol(display_symbol: str) -> Optional[tuple]:
    """
    Parse FY:-prefixed display symbol into (strike, option_type, expiry_date).
    Returns None if parsing fails.
    """
    base = display_symbol.removeprefix("FY:")
    m = _OPT_RE.match(base)
    if not m:
        return None
    _, yy, date_code, strike_s, opt_type = m.groups()
    year   = 2000 + int(yy)
    strike = float(strike_s)

    if date_code.isalpha():
        month  = _MONTH_MAP.get(date_code.upper(), 1)
        expiry = date(year, month, 28)  # approximate; good enough for tracking
    else:
        if len(date_code) == 3:
            month, day = int(date_code[0]), int(date_code[1:])
        else:
            month, day = int(date_code[:2]), int(date_code[2:])
        expiry = date(year, month, day)

    return (strike, opt_type, expiry)


# ── Pool + consumer group setup ───────────────────────────────────────────────

async def _create_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(
        host=DB_HOST, port=int(DB_PORT),
        user=DB_USER, password=DB_PASSWORD, database=DB_NAME,
        min_size=1, max_size=5,
        max_inactive_connection_lifetime=300,
    )


async def _ensure_schema(pool: asyncpg.Pool) -> None:
    """Create fyers_ticks table and indexes if they don't exist."""
    async with pool.acquire() as conn:
        await conn.execute(_CREATE_TABLE_SQL)
        for idx_sql in _CREATE_INDEXES_SQL:
            await conn.execute(idx_sql)
    logger.info("fyers_ticks schema ready.")


async def _ensure_consumer_group() -> None:
    """Idempotent: create consumer group at current tail if absent."""
    try:
        await redis_client.xgroup_create(
            FYERS_REDIS_STREAM, FYERS_REDIS_GROUP, id="$", mkstream=True
        )
        logger.info("Consumer group '%s' created.", FYERS_REDIS_GROUP)
    except Exception as e:
        if "BUSYGROUP" in str(e):
            logger.info("Consumer group '%s' already exists — ok.", FYERS_REDIS_GROUP)
        else:
            raise


# ── Message parsing ───────────────────────────────────────────────────────────

def _ingest_messages(streams, buffer: list) -> None:
    """Parse XREADGROUP response into buffer rows. Poison rows are tagged."""
    for _stream_name, messages in streams:
        for message_id, data in messages:
            try:
                row = json.loads(data["data"])
            except Exception as e:
                logger.warning("Poison (bad JSON) %s: %s", message_id, e)
                buffer.append({"stream_id": message_id, "_poison": "bad_json"})
                continue

            missing = [f for f in _REQUIRED_FIELDS if f not in row or row[f] is None]
            if missing:
                logger.warning("Poison (missing %s) %s", missing, message_id)
                buffer.append({"stream_id": message_id, "_poison": f"missing_{missing}"})
                continue

            # Parse timestamp (ISO string → naive datetime)
            ts_raw = row.get("timestamp")
            if isinstance(ts_raw, str):
                row["timestamp"] = datetime.fromisoformat(ts_raw)

            ltt_raw = row.get("ltt")
            if isinstance(ltt_raw, str):
                row["ltt"] = datetime.fromisoformat(ltt_raw)

            row["stream_id"] = message_id
            buffer.append(row)


def _split_buffer(buffer: list) -> tuple[list, list]:
    valid, poison_ids = [], []
    for row in buffer:
        if row.get("_poison"):
            poison_ids.append(row["stream_id"])
        else:
            valid.append(row)
    return valid, poison_ids


def _build_record(row: dict) -> tuple:
    return (
        row["fyers_symbol"],
        row["display_symbol"],
        row["timestamp"],
        row.get("ltt"),
        row.get("ltp"),
        row.get("ltq"),
        row.get("volume"),
        row.get("oi"),        # always None — SDK removes OI (documented limitation)
        row.get("bid_price"),
        row.get("ask_price"),
        row.get("bid_qty"),
        row.get("ask_qty"),
        json.dumps(row.get("depth")) if row.get("depth") else None,
        row.get("stream_id"),
        row.get("source", "fyers"),
    )


# ── Flush ─────────────────────────────────────────────────────────────────────

async def flush(pool: asyncpg.Pool, buffer: list) -> None:
    if not buffer:
        return

    valid_buffer, poison_ids = _split_buffer(buffer)

    if poison_ids:
        await redis_client.xack(FYERS_REDIS_STREAM, FYERS_REDIS_GROUP, *poison_ids)

    if not valid_buffer:
        return

    records = []
    for row in valid_buffer:
        try:
            records.append(_build_record(row))
        except Exception as e:
            logger.warning("Row build error (skipping): %s", e)

    # Build tracked_symbols records (de-duplicated)
    seen_syms: set[str] = set()
    tracked_records = []
    for row in valid_buffer:
        disp = row.get("display_symbol", "")
        if disp and disp not in seen_syms:
            seen_syms.add(disp)
            parsed = _parse_display_symbol(disp)
            if parsed:
                strike, opt_type, expiry = parsed
                tracked_records.append((disp, strike, opt_type, expiry))

    async with pool.acquire() as conn:
        async with conn.transaction():
            if records:
                await conn.executemany(_INSERT_SQL, records)
            if tracked_records:
                await conn.executemany(_TRACKED_SYM_SQL, tracked_records)

    # ACK only after successful commit
    valid_ids = [r["stream_id"] for r in valid_buffer if r.get("stream_id")]
    if valid_ids:
        await redis_client.xack(FYERS_REDIS_STREAM, FYERS_REDIS_GROUP, *valid_ids)

    logger.info("Inserted %d fyers_ticks rows.", len(records))


# ── Main consumer loop ────────────────────────────────────────────────────────

async def fyers_db_writer() -> None:
    pool = await _create_pool()
    await _ensure_schema(pool)
    await _ensure_consumer_group()

    buffer: list = []

    try:
        # ── Drain pending (un-ACKed from prior crash) ─────────────────────
        while True:
            pending = await redis_client.xreadgroup(
                FYERS_REDIS_GROUP, _CONSUMER,
                {FYERS_REDIS_STREAM: "0"},
                count=BATCH_SIZE,
            )
            if not pending or not any(msgs for _, msgs in pending):
                break
            _ingest_messages(pending, buffer)
            if buffer:
                await flush(pool, buffer)
                buffer.clear()

        if buffer:
            await flush(pool, buffer)
            buffer.clear()

        # ── Live read loop ─────────────────────────────────────────────────
        while True:
            try:
                streams = await redis_client.xreadgroup(
                    FYERS_REDIS_GROUP, _CONSUMER,
                    {FYERS_REDIS_STREAM: ">"},
                    count=BATCH_SIZE,
                    block=100,
                )
                if not streams:
                    continue

                _ingest_messages(streams, buffer)

                if len(buffer) >= BATCH_SIZE:
                    await flush(pool, buffer)
                    buffer.clear()

            except Exception as e:
                logger.error("fyers_db_writer inner error: %s", e)

    except asyncio.CancelledError:
        if buffer:
            await flush(pool, buffer)
        await pool.close()
        logger.info("fyers_db_writer stopped gracefully.")
    except Exception as e:
        logger.error("fyers_db_writer fatal: %s", e)
        await pool.close()


if __name__ == "__main__":
    asyncio.run(fyers_db_writer())
