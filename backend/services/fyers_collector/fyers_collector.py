"""
fyers_collector.py
------------------
Connects to the Fyers WebSocket data feed, subscribes to the 8 nearest
NIFTY ATM strikes resolved at startup, and XADDs every tick to Redis
stream `fyers_ticks_stream`.

SDK:        fyers-apiv3 == 3.1.12
Subscribe:  SymbolUpdate (LTP, volume, OI-removed-by-SDK, bid/ask)
            + DepthUpdate (5-level bid/ask book)
            Both subscriptions are active simultaneously on the same symbols.
            DepthUpdate ticks update an in-process depth cache; the depth
            snapshot is embedded in the next SymbolUpdate tick for the symbol.

Timestamp:  Fyers `last_traded_time` (ltt) is a Unix epoch int (UTC).
            Converted to IST wall-clock naive datetime to match Zerodha's
            storage convention: datetime.fromtimestamp(ltt, tz=IST).replace(tzinfo=None)

Known SDK limitation:
            fyers-apiv3 3.1.12 removes `OI` from the SymbolUpdate tick dict
            in __response_output() (line ~970 in data_ws.py). OI will always
            be absent from the tick payload. Field is omitted (not null) per
            the spec — "include only fields Fyers actually delivers."

Reconnect:  The SDK's built-in reconnect is used (reconnect=True).
            Backoff: the SDK adds 5s per 5 failed reconnect attempts (steps
            up to max delay of ~50s over 50 attempts). We log each reconnect
            by tracking the attempt count in on_connect.
            On every reconnect, on_connect re-subscribes the full symbol list.
"""

import asyncio
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import redis as redis_sync

from backend.services.fyers_collector.config import (
    FYERS_APP_ID, FYERS_ACCESS_TOKEN,
    FYERS_REDIS_STREAM,
    REDIS_HOST, REDIS_PORT,
)
from backend.services.fyers_collector.symbol_resolver import (
    resolve_symbols, print_resolved,
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("fyers_collector")

# ── IST timezone constant ────────────────────────────────────────────────────
_IST = timezone(timedelta(hours=5, minutes=30))


def epoch_to_ist_naive(ts: int | float) -> datetime:
    """
    Convert Unix epoch (UTC) to IST wall-clock naive datetime.
    This matches Zerodha's timestamp convention:
      IST wall-clock stored as TIMESTAMP WITHOUT TIME ZONE (treated as UTC by PG).
    """
    return datetime.fromtimestamp(float(ts), tz=_IST).replace(tzinfo=None)


def ist_now_naive() -> datetime:
    """Return current IST time as naive datetime (for receive-time logging)."""
    return datetime.now(_IST).replace(tzinfo=None)


# ── Per-symbol depth cache (populated by DepthUpdate ticks) ─────────────────
_depth_cache: dict[str, dict] = {}

# ── Reconnect counter for logging ───────────────────────────────────────────
_connect_count = 0

# ── Redis sync client (used from WS callback thread) ────────────────────────
_redis: redis_sync.Redis | None = None

# ── Resolved symbol lists (set at startup) ──────────────────────────────────
_symbol_meta: list[dict] = []          # list of dicts from symbol_resolver
_fyers_symbols: list[str] = []         # ["NSE:NIFTY...", ...]


def _build_tick_payload(msg: dict) -> dict | None:
    """
    Build the Redis XADD JSON payload from a SymbolUpdate ('sf') tick message.
    Only includes fields Fyers actually delivers — missing fields are omitted.
    Returns None if the message is not a tradable tick (control messages, etc.).
    """
    if msg.get("type") not in ("sf", "if"):
        return None

    fyers_symbol = msg.get("symbol", "")
    if not fyers_symbol:
        return None

    # Look up display_symbol from our known symbol list
    display_symbol = None
    for meta in _symbol_meta:
        if meta["fyers_symbol"] == fyers_symbol:
            display_symbol = meta["display_symbol"]
            break
    if display_symbol is None:
        # Symbol not in our subscription (e.g. stray message)
        return None

    # ── Timestamp conversion ──────────────────────────────────────────────
    # Fyers `last_traded_time` (ltt) = exchange last-trade epoch (UTC, int seconds)
    # `exch_feed_time` = exchange feed epoch (UTC, int seconds, may differ from ltt)
    # We use ltt as the canonical tick timestamp (matches Zerodha's exchange_timestamp).
    ltt_raw  = msg.get("last_traded_time")
    feed_raw = msg.get("exch_feed_time")

    if ltt_raw:
        timestamp_ist = epoch_to_ist_naive(ltt_raw)
    else:
        # Fallback: current IST time (should not happen for normal ticks)
        timestamp_ist = ist_now_naive()
        logger.warning("ltt missing for %s — using receive time", fyers_symbol)

    exch_feed_ist = epoch_to_ist_naive(feed_raw) if feed_raw else None

    # ── Build payload (raw passthrough — no derived fields) ───────────────
    payload: dict = {
        "source":         "fyers",
        "fyers_symbol":   fyers_symbol,
        "display_symbol": display_symbol,
        "timestamp":      timestamp_ist.isoformat(timespec="milliseconds"),
    }

    if exch_feed_ist:
        payload["exch_feed_time"] = exch_feed_ist.isoformat(timespec="milliseconds")

    # Price fields
    if "ltp" in msg:
        payload["ltp"] = msg["ltp"]
    if "last_traded_qty" in msg:
        payload["ltq"] = msg["last_traded_qty"]
    if "vol_traded_today" in msg:
        payload["volume"] = msg["vol_traded_today"]

    # OI is intentionally absent: fyers-apiv3 removes it in __response_output.
    # The fyers_ticks.oi column will always be NULL for data from this SDK version.

    # Bid/ask (top of book)
    if "bid_price" in msg:
        payload["bid_price"] = msg["bid_price"]
    if "ask_price" in msg:
        payload["ask_price"] = msg["ask_price"]
    if "bid_size" in msg:
        payload["bid_qty"] = msg["bid_size"]
    if "ask_size" in msg:
        payload["ask_qty"] = msg["ask_size"]

    # Aggregate quantities
    if "tot_buy_qty" in msg:
        payload["tot_buy_qty"] = msg["tot_buy_qty"]
    if "tot_sell_qty" in msg:
        payload["tot_sell_qty"] = msg["tot_sell_qty"]

    # Embed latest depth snapshot if available from DepthUpdate cache
    depth = _depth_cache.get(fyers_symbol)
    if depth:
        payload["depth"] = depth

    return payload


def _build_depth_snapshot(msg: dict) -> dict | None:
    """
    Build a structured depth dict from a DepthUpdate ('dp') tick message.
    Returns None if the message is not a depth tick.
    """
    if msg.get("type") != "dp":
        return None

    bids = []
    asks = []
    for i in range(1, 6):
        bp = msg.get(f"bid_price{i}")
        bq = msg.get(f"bid_size{i}")
        bo = msg.get(f"bid_order{i}")
        if bp is not None:
            bids.append({"price": bp, "qty": bq or 0, "orders": bo or 0})

        ap = msg.get(f"ask_price{i}")
        aq = msg.get(f"ask_size{i}")
        ao = msg.get(f"ask_order{i}")
        if ap is not None:
            asks.append({"price": ap, "qty": aq or 0, "orders": ao or 0})

    return {"bids": bids, "asks": asks}


def on_message(msg: dict) -> None:
    """
    Fyers WebSocket tick callback — runs in the WS background thread.
    Handles control messages, SymbolUpdate ticks, and DepthUpdate ticks.
    """
    msg_type = msg.get("type")

    # ── Control messages (auth, subscribe confirmations) ─────────────────
    if msg_type in ("cn", "sub", "unsub", "ful", "lit", "cp", "cr"):
        logger.info("Fyers control: type=%s code=%s msg=%s",
                    msg_type, msg.get("code"), msg.get("message"))
        return

    # ── DepthUpdate — update per-symbol depth cache only ─────────────────
    if msg_type == "dp":
        fyers_symbol = msg.get("symbol", "")
        depth = _build_depth_snapshot(msg)
        if depth and fyers_symbol:
            _depth_cache[fyers_symbol] = depth
        return

    # ── SymbolUpdate ('sf') — build payload and XADD to Redis ────────────
    if msg_type in ("sf", "if"):
        payload = _build_tick_payload(msg)
        if payload is None:
            return

        try:
            _redis.xadd(
                FYERS_REDIS_STREAM,
                {"data": json.dumps(payload, default=str)},
                maxlen=50000,
                approximate=True,
            )
        except Exception as e:
            logger.error("Redis XADD failed: %s", e)
        return

    # Unknown message type — log for debugging
    logger.debug("Unhandled Fyers message type=%s: %s", msg_type, str(msg)[:200])


def on_error(err) -> None:
    logger.error("Fyers WebSocket error: %s", err)


def on_close(msg) -> None:
    logger.warning("Fyers WebSocket closed: %s", msg)


def on_connect() -> None:
    """
    Called on initial connect and on every reconnect.
    Re-subscribes to the full symbol list so reconnect is seamless.
    """
    global _connect_count
    _connect_count += 1

    if _connect_count == 1:
        logger.info("Fyers WebSocket connected (initial). Subscribing to %d symbols.",
                    len(_fyers_symbols))
    else:
        logger.info("Fyers WebSocket reconnected (attempt #%d). Re-subscribing to %d symbols.",
                    _connect_count, len(_fyers_symbols))

    if not _fyers_symbols:
        logger.error("No symbols to subscribe — aborting.")
        return

    from fyers_apiv3.FyersWebsocket import data_ws as _ws_mod
    fyers = _ws_mod.FyersDataSocket.__new__(_ws_mod.FyersDataSocket)

    # Subscribe to SymbolUpdate (LTP + top-of-book bid/ask + volume)
    fyers.subscribe(symbols=_fyers_symbols, data_type="SymbolUpdate")
    logger.info("Subscribed SymbolUpdate for %d symbols.", len(_fyers_symbols))

    # Subscribe to DepthUpdate (5-level order book) — separate subscription,
    # depth ticks update _depth_cache and are embedded in subsequent sf ticks.
    time.sleep(0.5)
    fyers.subscribe(symbols=_fyers_symbols, data_type="DepthUpdate")
    logger.info("Subscribed DepthUpdate for %d symbols.", len(_fyers_symbols))

    fyers.keep_running()


def main() -> None:
    global _redis, _symbol_meta, _fyers_symbols

    # ── Validate config ───────────────────────────────────────────────────
    if not FYERS_ACCESS_TOKEN:
        logger.error("FYERS_ACCESS_TOKEN is not set in .env.fyers — cannot connect.")
        sys.exit(1)
    if not FYERS_APP_ID:
        logger.error("FYERS_APP_ID is not set in .env.fyers — cannot connect.")
        sys.exit(1)

    # ── Resolve symbols from Zerodha's tracked_symbols ───────────────────
    logger.info("Resolving NIFTY strikes from Zerodha tracked_symbols …")
    _symbol_meta = asyncio.run(resolve_symbols())

    if not _symbol_meta:
        logger.error("No symbols resolved — exiting.")
        sys.exit(1)

    _fyers_symbols = [s["fyers_symbol"] for s in _symbol_meta]
    print_resolved(_symbol_meta)

    # ── Redis sync client (used from WS callback thread) ─────────────────
    _redis = redis_sync.Redis(
        host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
    )
    try:
        _redis.ping()
    except Exception as e:
        logger.error("Redis connection failed: %s — exiting.", e)
        sys.exit(1)
    logger.info("Redis connected: %s:%d", REDIS_HOST, REDIS_PORT)

    # ── Initialise Fyers WebSocket ────────────────────────────────────────
    from fyers_apiv3.FyersWebsocket import data_ws

    fyers = data_ws.FyersDataSocket(
        access_token=FYERS_ACCESS_TOKEN,
        log_path="",               # logs to stdout via our logger
        litemode=False,            # full mode — receives all data_val fields
        write_to_file=False,
        reconnect=True,
        reconnect_retry=50,        # SDK will step up delay 5s per 5 reconnects
        on_message=on_message,
        on_error=on_error,
        on_connect=on_connect,
        on_close=on_close,
    )

    logger.info("Connecting to Fyers WebSocket …")
    fyers.connect()   # blocks for 2s, calls on_connect, then returns

    # ── Keep main thread alive — WS runs in non-daemon background thread ──
    logger.info("Fyers collector running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("Shutting down Fyers collector.")
        fyers.close_connection()


if __name__ == "__main__":
    main()
