# backend/state.py
# Shared in-memory state for the uvicorn process.
# Both the API routes (sl.py) and the websocket_handler read/write here.

# sl_state[symbol] = {
#   "price":     float        — user-set SL price (dragged line position)
#   "order_id":  str | None   — Kite order ID after SL placement
#   "side":      str | None   — 'BUY' or 'SELL' (direction of SL order)
#   "qty":       int | None   — quantity
#   "exchange":  str | None   — 'NFO' etc.
#   "state":     str          — 'pending' | 'placed' | 'hit'
# }

sl_state: dict = {}
