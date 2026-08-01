# backend/api/chart_server.py

import asyncio
import asyncpg
import datetime as _dt
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from config.credentials import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
)
from backend.services.redis_streamer import redis_streamer
from backend.services.fyers_collector.fyers_streamer import fyers_streamer
from fastapi.staticfiles import StaticFiles
from datetime import timezone, timedelta, datetime
from backend.api.strikes import router as strikes_router, prewarm_strikes_cache, refresh_b2_cache
from backend.api.sl import router as sl_router
from backend.api.hedge import router as hedge_router
from backend.api.journal import router as journal_router
from backend.api.straddle import router as straddle_router
from backend.api.streaming import manager
from fastapi import WebSocketDisconnect
from fastapi import Body
from backend.services import ghost_detector
from backend.services import reverse_snipper
from backend.services import straddle_monitor

    
app = FastAPI()

app.include_router(strikes_router, prefix="/api")
app.include_router(sl_router, prefix="/api")
app.include_router(hedge_router, prefix="/api")
app.include_router(journal_router, prefix="/api")
app.include_router(straddle_router, prefix="/api")

app.add_middleware(GZipMiddleware, minimum_size=500)
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
        min_size=2,
        max_size=8,
        max_inactive_connection_lifetime=300,   # close idle connections after 5 min → pool shrinks back to min_size
        server_settings={"statement_timeout": "30000"},
    )


@app.on_event("shutdown")
async def shutdown():
    await app.state.pool.close()


async def _load_pending_fills_on_startup(pool):
    """Load today's qualifying jumps from DB into _pending_fills so fill tracking
    survives server restarts without losing intraday context."""
    from backend.services.redis_streamer import (
        _pending_fills, _get_price_filter, _get_jump_threshold
    )
    try:
        today = _dt.date.today()
        async with pool.acquire() as conn:
            await conn.execute("SET statement_timeout = 0")
            rows = await conn.fetch("""
                SELECT symbol,
                       curr_price,
                       prev_price,
                       price_jump,
                       floor(EXTRACT(EPOCH FROM timestamp) / 5) * 5 AS bucket_epoch
                FROM gap_ticks
                WHERE timestamp >= $1::date + TIME '09:15:03'
                  AND timestamp <= $1::date + TIME '15:35:00'
                  AND curr_price  > 0
                  AND ABS(price_jump) > 3
                ORDER BY timestamp ASC
            """, today)

        count = 0
        for row in rows:
            sym  = row['symbol']
            pj   = float(row['price_jump'] or 0)
            curr = float(row['curr_price'])
            prev = float(row['prev_price'] or 0)

            if curr >= _get_price_filter(sym) or abs(pj) <= _get_jump_threshold(sym):
                continue

            direction  = 'UP' if pj > 0 else 'DOWN'
            # bucket_epoch from SQL = EXTRACT(EPOCH FROM IST-naive-as-UTC timestamp)
            # = ts.timestamp() + 19800 on an IST server — matches strikes.py convention
            # and the bucket stored in API-loaded _jumpMarkers on the frontend.
            ist_bucket = int(row['bucket_epoch'])

            if sym not in _pending_fills:
                _pending_fills[sym] = []
            _pending_fills[sym].append({
                "bucket"   : ist_bucket,
                "direction": direction,
                "pre_price": prev,
                "is_first" : False,
            })
            count += 1

        print(f"[startup] Loaded {count} pending fills from today's DB", flush=True)
    except Exception as e:
        print(f"[startup] Could not load pending fills: {e}", flush=True)


@app.on_event("startup")
async def startup():
    app.state.pool = await create_pool()

    asyncio.create_task(redis_streamer())
    asyncio.create_task(fyers_streamer())
    await prewarm_strikes_cache(app.state.pool)  # blocking — cache must be warm before serving requests
    asyncio.create_task(_load_pending_fills_on_startup(app.state.pool))

    # ── _pending_fills size monitor (runs every 30s, logs per-symbol count) ──
    # Phase 1: instrumentation only — no eviction.
    # After one trading day, review [FILL_AGE] log lines in chart_server logs
    # to pick the eviction TTL, then uncomment the prune block below.
    async def _monitor_pending_fills():
        from backend.services.redis_streamer import _pending_fills
        import logging as _logging
        _log = _logging.getLogger("redis_streamer")
        while True:
            await asyncio.sleep(30)
            if _pending_fills:
                summary = {sym: len(entries) for sym, entries in _pending_fills.items() if entries}
                total = sum(summary.values())
                top = sorted(summary.items(), key=lambda x: -x[1])[:5]
                _log.info("[PENDING_FILLS] total=%d symbols=%d top=%s", total, len(summary), top)
            # ── Prune block (DISABLED — enable after TTL is chosen from FILL_AGE data) ──
            # import time as _time
            # TTL_SECONDS = ???   # set from FILL_AGE log analysis
            # now = _time.monotonic()
            # for sym in list(_pending_fills):
            #     before = len(_pending_fills[sym])
            #     _pending_fills[sym] = [
            #         pf for pf in _pending_fills[sym]
            #         if now - pf.get("registered_at", now) < TTL_SECONDS
            #     ]
            #     evicted = before - len(_pending_fills[sym])
            #     if evicted:
            #         _log.info("[PRUNE] %s evicted=%d remaining=%d", sym, evicted, len(_pending_fills[sym]))
    asyncio.create_task(_monitor_pending_fills())

    # Ensure jump-lookup index exists (no-op if already present).
    # Partial predicate WHERE curr_price > 0 AND ABS(price_jump) > 3 means only
    # big-jump rows are indexed — tiny index, fast scan for Query 1 in get_jump_history().
    # Runs in background so it doesn't block startup (14GB table can take minutes to index).
    async def _ensure_jump_index():
        async with app.state.pool.acquire() as _conn:
            await _conn.execute("SET statement_timeout = 0")
            await _conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_gap_ticks_jump_lookup
                ON gap_ticks (symbol, timestamp)
                INCLUDE (curr_price, prev_price, price_jump)
                WHERE curr_price > 0 AND ABS(price_jump) > 3
            """)
    asyncio.create_task(_ensure_jump_index())

    async def _ensure_hedge_pairs_table():
        async with app.state.pool.acquire() as _conn:
            await _conn.execute("""
                CREATE TABLE IF NOT EXISTS hedge_pairs (
                    id               SERIAL PRIMARY KEY,
                    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    hedge_order_ids  TEXT[]      NOT NULL DEFAULT '{}',
                    hedge_symbol     VARCHAR(50) NOT NULL,
                    hedge_qty        INTEGER     NOT NULL,
                    avg_hedge_price  NUMERIC(10,2),
                    sell_symbol      VARCHAR(50) NOT NULL,
                    sell_qty         INTEGER,
                    sell_order_id    VARCHAR(50),
                    avg_sell_price   NUMERIC(10,2),
                    unwind_order_ids TEXT[]      NOT NULL DEFAULT '{}',
                    status           VARCHAR(30) NOT NULL,
                    notes            TEXT
                )
            """)
            await _conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_hedge_pairs_created_at "
                "ON hedge_pairs (created_at DESC)"
            )
            await _conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_hedge_pairs_status "
                "ON hedge_pairs (status)"
            )
    asyncio.create_task(_ensure_hedge_pairs_table())

    async def _ensure_reverse_snipper_table():
        async with app.state.pool.acquire() as _conn:
            await _conn.execute("""
                CREATE TABLE IF NOT EXISTS reverse_snipper_trades (
                    id                      SERIAL PRIMARY KEY,
                    symbol                  VARCHAR(50)    NOT NULL,
                    entry_price             NUMERIC(10,2)  NOT NULL,
                    entry_time              TIMESTAMPTZ    NOT NULL,
                    exit_price              NUMERIC(10,2)  NOT NULL,
                    exit_time               TIMESTAMPTZ    NOT NULL,
                    pnl_pts                 NUMERIC(10,2)  NOT NULL,
                    pnl_inr                 NUMERIC(12,2)  NOT NULL,
                    close_reason            VARCHAR(20)    NOT NULL,
                    spike_low               NUMERIC(10,2)  NOT NULL,
                    spike_distance          NUMERIC(8,2)   NOT NULL,
                    spike_window_secs       NUMERIC(6,2)   NOT NULL,
                    recovery_buffer         NUMERIC(8,2)   NOT NULL,
                    sl_buffer               NUMERIC(8,2)   NOT NULL,
                    lookback_secs           NUMERIC(6,2)   NOT NULL,
                    spike_timeout_secs      NUMERIC(6,2)   NOT NULL,
                    qty                     INTEGER        NOT NULL,
                    cooldown_secs           NUMERIC(6,2)   NOT NULL,
                    auto_increment_sl       BOOLEAN        NOT NULL DEFAULT FALSE,
                    increment_step          NUMERIC(8,2)   NOT NULL DEFAULT 5,
                    increment_interval_secs NUMERIC(6,2)   NOT NULL DEFAULT 30,
                    sl_at_cost              BOOLEAN        NOT NULL DEFAULT FALSE
                )
            """)
            await _conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_rsnip_symbol_entry "
                "ON reverse_snipper_trades (symbol, entry_time DESC)"
            )
    asyncio.create_task(_ensure_reverse_snipper_table())

    # Inject DB pool into reverse_snipper so it can persist trades
    reverse_snipper.set_pool(app.state.pool)

    # Restore today's straddle positions (restarts open-straddle monitors)
    straddle_monitor.load_positions()

    # B2 manifest is slow (blocking S3 call) — skip during market hours to
    # avoid stalling the event loop while charts are actively being used.
    def _in_market_hours() -> bool:
        ist = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        mins = ist.hour * 60 + ist.minute
        return 9 * 60 + 15 <= mins < 15 * 60 + 30

    async def _b2_refresh_loop():
        while True:
            if _in_market_hours():
                print("[B2 cache] market hours — skipping refresh", flush=True)
            else:
                try:
                    await refresh_b2_cache(app.state.pool)
                except Exception as e:
                    print(f"[B2 cache] refresh error: {e}")
            await asyncio.sleep(300)  # check every 5 minutes

    asyncio.create_task(_b2_refresh_loop())

@app.post("/api/ghost-detector/start")
async def ghost_detector_start(payload: dict = Body(...)):
    symbol = payload.get("symbol", "").strip()
    config = payload.get("config", {})
    if not symbol:
        return {"status": "error", "message": "symbol required"}
    required = {"distance", "buffer", "qty", "direction", "cycle_time"}
    missing = required - config.keys()
    if missing:
        return {"status": "error", "message": f"missing config keys: {missing}"}
    started = ghost_detector.start_detector(symbol, config)
    return {"status": "ok", "started": started}


@app.post("/api/ghost-detector/stop")
async def ghost_detector_stop(payload: dict = Body(...)):
    symbol = payload.get("symbol", "").strip()
    if not symbol:
        return {"status": "error", "message": "symbol required"}
    stopped = ghost_detector.stop_detector(symbol)
    return {"status": "ok", "stopped": stopped}


@app.get("/api/ghost-detector/active")
async def ghost_detector_active():
    return {"active": ghost_detector.get_active()}


@app.post("/api/reverse-snipper/start")
async def reverse_snipper_start(payload: dict = Body(...)):
    symbol = payload.get("symbol", "").strip()
    config = payload.get("config", {})
    if not symbol:
        return {"status": "error", "message": "symbol required"}
    required = {
        "spike_distance", "spike_window_secs", "recovery_buffer", "sl_buffer",
        "spike_timeout_secs", "qty", "cooldown_secs",
    }
    missing = required - config.keys()
    if missing:
        return {"status": "error", "message": f"missing config keys: {missing}"}
    started = reverse_snipper.start_snipper(symbol, config)
    return {"status": "ok", "started": started}


@app.post("/api/reverse-snipper/stop")
async def reverse_snipper_stop(payload: dict = Body(...)):
    symbol = payload.get("symbol", "").strip()
    if not symbol:
        return {"status": "error", "message": "symbol required"}
    stopped = reverse_snipper.stop_snipper(symbol)
    return {"status": "ok", "stopped": stopped}


@app.get("/api/reverse-snipper/active")
async def reverse_snipper_active():
    return {"active": reverse_snipper.get_active()}


@app.get("/api/reverse-snipper/trades")
async def reverse_snipper_trades(symbol: str = None, date: str = None):
    trades = await reverse_snipper.get_trades(app.state.pool, symbol=symbol, date=date)
    # Convert datetime objects to ISO strings for JSON serialisation
    for t in trades:
        for k, v in t.items():
            if hasattr(v, "isoformat"):
                t[k] = v.isoformat()
    return {"trades": trades}


@app.websocket("/ws/{symbol}")
async def websocket_endpoint(websocket: WebSocket, symbol: str):

    print("\nCLIENT CONNECTING TO SYMBOL:", symbol)

    await manager.connect(symbol, websocket)

    print("Active WS symbols after connect:",
          list(manager.active_connections.keys()))

    try:
        while True:
            await asyncio.sleep(20)
            await websocket.send_json({"type": "ping"})  # heartbeat — prevents idle timeout
    except (WebSocketDisconnect, Exception):
        manager.disconnect(symbol, websocket)


app.mount("/", StaticFiles(directory="frontend/ui", html=True), name="static")