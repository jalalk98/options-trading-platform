-- Hedge pairs table — one row per hedged-sell attempt.
-- Tracks hedge BUY + sell + optional unwind order IDs through the full lifecycle.
--
-- status values:
--   hedge_placed    — BUY placed, waiting for fill
--   complete        — BUY filled + sell sent successfully
--   sell_rejected   — BUY filled but sell was rejected; unwind attempted
--   unwound         — sell_rejected path: hedge position successfully unwound
--   unwind_failed   — sell_rejected path: unwind also failed (stray long, needs manual action)
--   hedge_rejected  — BUY rejected or partially failed before sell was attempted
--   timeout         — BUY fill poll timed out; hedge position unknown

CREATE TABLE IF NOT EXISTS hedge_pairs (
    id               SERIAL PRIMARY KEY,
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    -- Hedge BUY side (may be multiple order IDs when freeze-qty chunking splits the order)
    hedge_order_ids  TEXT[]          NOT NULL DEFAULT '{}',
    hedge_symbol     VARCHAR(50)     NOT NULL,
    hedge_qty        INTEGER         NOT NULL,
    avg_hedge_price  NUMERIC(10, 2),
    -- Sell side
    sell_symbol      VARCHAR(50)     NOT NULL,
    sell_qty         INTEGER,
    sell_order_id    VARCHAR(50),
    avg_sell_price   NUMERIC(10, 2),
    -- Unwind side (present when sell_rejected → unwind attempted)
    unwind_order_ids TEXT[]          NOT NULL DEFAULT '{}',
    -- Lifecycle status
    status           VARCHAR(30)     NOT NULL,
    notes            TEXT
);

CREATE INDEX IF NOT EXISTS idx_hedge_pairs_created_at ON hedge_pairs (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_hedge_pairs_status     ON hedge_pairs (status);
