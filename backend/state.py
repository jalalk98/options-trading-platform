# backend/state.py
# Shared in-memory state for the uvicorn process.
# Both the API routes (sl.py) and the websocket_handler read/write here.

# sl_state[symbol] = {
#   "price":           float        — user-set SL price (dragged line position)
#   "trigger_buffer":  float        — gap between SL limit and trigger (default 0.20)
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
