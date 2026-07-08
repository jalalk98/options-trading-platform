# backend/state.py
# Shared in-memory state for the uvicorn process.
# Both the API routes (sl.py) and the websocket_handler read/write here.

from collections import deque

# sl_state[symbol] = {
#   "price":           float        — user-set SL price (dragged line position)
#   "trigger_buffer":  float        — gap between SL line price and exchange trigger (SENSEX: 1.75, NIFTY/others: 0.85)
#   "order_id":        str | None   — Kite order ID after SL placement
#   "side":            str | None   — 'BUY' or 'SELL' (direction of SL order)
#   "qty":             int | None   — quantity
#   "exchange":        str | None   — 'NFO' etc.
#   "state":           str          — 'pending' | 'placed' | 'hit'
# }

sl_state: dict = {}

# Mutable app-wide config (updated via API endpoints, persisted in memory only)
app_config: dict = {
    "default_sl_dist": 10.0,   # fallback SL distance for L/M orders when no SL line is drawn
}

# Latest LTP per symbol — updated by redis_streamer on every tick.
# Used by ghost_detector to anchor probe order prices.
latest_ltp: dict = {}  # symbol → float

# Rolling tick history per symbol — last 1.5 seconds worth of (monotonic_time, ltp) tuples.
# Used by ghost_detector to cross-check whether a triggered probe had a matching tick in our feed.
# Each value is a deque of (time.monotonic(), ltp_float); entries older than 1.5s are pruned on insert.
tick_history: dict = {}  # symbol → deque[(monotonic_ts, ltp)]

# Ghost detector toggle state — tracks which panels have the detector currently ON.
# Set by the REST start/stop endpoints; read by the frontend via GET /api/ghost-detector/active.
# ghost_detector_state[symbol] = config dict when active, absent when stopped.
ghost_detector_state: dict = {}

# Reverse snipper toggle state — tracks which panels have the snipper currently ON.
# Set by the REST start/stop endpoints; read by the frontend via GET /api/reverse-snipper/active.
# reverse_snipper_state[symbol] = config dict when active, absent when stopped.
reverse_snipper_state: dict = {}
