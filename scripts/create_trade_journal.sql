-- Trade journal table — one row per executed trade leg (entry or exit).
-- Populated by syncing from Zerodha /trades API.
-- Notes and setup tag are the only manually entered fields.
--
-- outcome values:
--   open    — trade entered but not yet exited
--   win     — exited at a profit
--   loss    — exited at a loss
--   be      — exited at breakeven

CREATE TABLE IF NOT EXISTS trade_journal (
    id                          SERIAL PRIMARY KEY,
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    -- Date of the trade (IST)
    trade_date                  DATE            NOT NULL,

    -- Instrument details (parsed from Zerodha symbol)
    symbol                      VARCHAR(50)     NOT NULL,
    underlying                  VARCHAR(20)     NOT NULL,
    strike                      NUMERIC(10, 2),
    option_type                 VARCHAR(2),
    expiry_date                 DATE,

    -- Trade execution (from Zerodha)
    direction                   VARCHAR(4)      NOT NULL,   -- BUY or SELL
    quantity                    INTEGER         NOT NULL,
    entry_price                 NUMERIC(10, 2),
    exit_price                  NUMERIC(10, 2),
    entry_time                  TIMESTAMPTZ,
    exit_time                   TIMESTAMPTZ,

    -- Underlying index price at time of entry/exit (from our own tick data)
    underlying_price_at_entry   NUMERIC(10, 2),
    underlying_price_at_exit    NUMERIC(10, 2),

    -- P&L (calculated on exit: (exit_price - entry_price) * quantity)
    pnl                         NUMERIC(12, 2),

    -- Zerodha order reference — used for deduplication on sync
    order_id                    VARCHAR(50)     UNIQUE,

    -- Manual fields (user fills in)
    setup                       VARCHAR(50),
    notes                       TEXT,
    outcome                     VARCHAR(4)      DEFAULT 'open'
);

CREATE INDEX IF NOT EXISTS idx_trade_journal_trade_date ON trade_journal (trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_trade_journal_symbol     ON trade_journal (symbol);
CREATE INDEX IF NOT EXISTS idx_trade_journal_order_id   ON trade_journal (order_id);
