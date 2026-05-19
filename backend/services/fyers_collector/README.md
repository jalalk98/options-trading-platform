# Fyers Collector

Parallel tick feed from Fyers WebSocket alongside the existing Zerodha pipeline.
Completely isolated — zero modification to any Zerodha service.

## Architecture

```
fyers_collector.py      ← WebSocket → Redis XADD (fyers_ticks_stream)
fyers_db_writer.py      ← Redis XREADGROUP (fyers_writer_group) → PostgreSQL fyers_ticks
fyers_streamer.py       ← Redis XREAD → ConnectionManager.broadcast(FY:symbol)
symbol_resolver.py      ← queries tracked_symbols + candles_5s for 8 nearest ATM NIFTY strikes
config.py               ← reads .env.fyers at project root
```

## Setup

1. Copy `.env.fyers` (in project root) and fill in your credentials:
   ```
   FYERS_APP_ID=your_app_id
   FYERS_ACCESS_TOKEN=your_access_token
   ```

2. The access token expires daily. Regenerate via Fyers API v3 auth flow and update the file.

3. Start services (Phase 4 — do this during market hours for validation):
   ```bash
   # Dry-run first — verify symbols without connecting to Fyers WebSocket
   PYTHONPATH=. python -m backend.services.fyers_collector.symbol_resolver

   # Start DB writer first (creates fyers_ticks table)
   PYTHONPATH=. python -m backend.services.fyers_collector.fyers_db_writer &

   # Start tick collector
   PYTHONPATH=. python -m backend.services.fyers_collector.fyers_collector
   ```

4. To install as systemd services (NOT YET — validate manually first):
   ```bash
   sudo cp fyers-collector.service /etc/systemd/system/
   sudo cp fyers-db-writer.service /etc/systemd/system/
   sudo systemctl daemon-reload
   sudo systemctl enable fyers-collector fyers-db-writer
   sudo systemctl start fyers-collector fyers-db-writer
   ```

## Symbols

8 nearest NIFTY ATM strikes (4 CE + 4 PE) for the nearest weekly expiry.
Resolved dynamically at startup from `tracked_symbols` + latest NIFTY LTP in `candles_5s`.
No hardcoded strikes — automatically tracks Zerodha's live subscription.

## Data flow

| Stage         | Key                    | Notes                                     |
|---------------|------------------------|-------------------------------------------|
| Redis stream  | `fyers_ticks_stream`   | maxlen=50000, raw JSON ticks              |
| Consumer group| `fyers_writer_group`   | DB writer — ACK after commit              |
| Consumer group| `fyers_streamer_group` | (unused — streamer uses XREAD instead)   |
| DB table      | `fyers_ticks`          | Not partitioned — 8 symbols, small table  |
| tracked_symbols| symbol = `FY:NIFTY…` | Auto-inserted; appears in /api/strikes    |

## Known limitations

- **OI always NULL**: fyers-apiv3 3.1.12 removes OI from SymbolUpdate ticks internally.
- **No gap detection**: `fyers_ticks` has no `is_gap`/`price_jump` columns.
  The `/api/gaps/FY:*` endpoint returns an empty array.
- **No candles_5s integration**: history is aggregated on-the-fly from `fyers_ticks`.
  This is slower than `candles_5s` for large date ranges but fine for the 8-symbol set.
- **Access token expires daily**: must be refreshed before each trading session.

## Timestamp convention

Fyers `last_traded_time` (UTC epoch int) is converted to IST wall-clock naive datetime:
```python
datetime.fromtimestamp(ltt, tz=IST).replace(tzinfo=None)
```
This matches Zerodha's storage convention exactly, ensuring 5s bucket alignment.
