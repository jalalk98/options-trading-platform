# Operations Runbook — Options Trading Platform

## Architecture Overview

Three systemd services work together. Two are managed by cron; one is always-on.

```
KiteConnect WebSocket
        │
        ▼
tick-collector.service     ← reads market ticks, writes to Redis stream
        │
        ▼ (Redis ticks_stream, consumer group)
db-writer.service          ← reads Redis, writes to PostgreSQL
        │
        ▼ (PostgreSQL: gap_ticks, candles_5s, gap_events, tracked_symbols)
trading-api.service        ← uvicorn API, always running, reads from DB for charts
```

---

## Daily Cron Schedule (fully automated on weekdays)

| Time (IST) | What runs | File |
|---|---|---|
| 07:00 | Kite token refresh (Playwright) | `~/run_refresh_token.sh` → log: `~/token_refresh.txt` |
| 07:05 | Instrument file update (Kite API) | `run_instrument_update.py` → log: `~/instrument_update.txt` |
| 07:10 | VACUUM ANALYZE all tables | crontab → log: `~/vacuum_analyze.txt` |
| 09:08 | **Start tick ingestion** | `start_trading.sh` → log: `~/trading_start.txt` |
| 09:15–15:30 | Watchdog every 5 min | `watchdog_tick_collector.sh` → log: `~/watchdog.txt` |
| 15:30 | **Stop tick ingestion** | `stop_trading.sh` → log: `~/trading_stop.txt` |
| 15:35 | Daily perf report | `scripts/daily_perf_report.py` |
| 15:36 | Partition vacuum | `scripts/vacuum_today_partition.sh` |

**Under normal conditions you don't need to do anything manually** — the cron handles start and stop. Use the manual procedures below only when recovering from a missed start or after debugging.

---

## Manual Start (normal morning, or recovery)

```bash
cd /home/ubuntu/projects/options-trading-platform
bash start_trading.sh
```

`start_trading.sh` does the following in order:
1. Checks for holiday mode (`~/.trading_paused`) and NSE holidays (`~/.trading_holidays`)
2. `redis-cli DEL ticks_stream` — clears yesterday's stale stream entries
3. `sudo systemctl restart db-writer.service`
4. (2s pause)
5. `sudo systemctl restart tick-collector.service`
6. Verifies both services are active
7. Validates the Kite token via `api.kite.trade/user/profile`
8. Sends Telegram notification with status

**Order matters:** db-writer must start before tick-collector so the Redis consumer group exists before the first ticks arrive.

---

## Manual Stop (normal close, or for maintenance)

```bash
cd /home/ubuntu/projects/options-trading-platform
bash stop_trading.sh
```

Or directly:

```bash
sudo systemctl stop tick-collector.service db-writer.service
```

Both services handle SIGTERM gracefully:
- `tick-collector` disconnects from the KiteConnect WebSocket cleanly
- `db-writer` flushes its in-memory tick buffer to PostgreSQL before exiting, then closes the DB connection pool

---

## Service Status Check

```bash
# Quick status
systemctl is-active db-writer.service tick-collector.service trading-api.service

# Detailed status + recent logs
systemctl status db-writer.service
systemctl status tick-collector.service

# Live logs (tail -f equivalent)
journalctl -u db-writer.service -f
journalctl -u tick-collector.service -f

# API server logs
tail -f /var/log/trading-api.log
```

---

## Service Unit File Locations

| Service | Unit file | Manages |
|---|---|---|
| `trading-api.service` | `/etc/systemd/system/trading-api.service` | uvicorn (always-on, restart=always) |
| `db-writer.service` | `/etc/systemd/system/db-writer.service` | `backend.processors.db_writer_runner` |
| `tick-collector.service` | `/etc/systemd/system/tick-collector.service` | `backend.processors.tick_collector` |

`trading-api.service` starts at boot and restarts automatically. The ingestion services do NOT auto-start at boot — they're controlled by `start_trading.sh` at 09:08 via cron.

---

## Log Files Reference

| Log | Contents |
|---|---|
| `~/token_refresh.txt` | Daily Playwright token refresh results |
| `~/trading_start.txt` | Start script output + Telegram confirmation |
| `~/trading_stop.txt` | Stop script output |
| `~/watchdog.txt` | Watchdog restarts during market hours |
| `~/instrument_update.txt` | Daily Kite instruments file update |
| `~/vacuum_analyze.txt` | PostgreSQL VACUUM ANALYZE results |
| `~/tick_rate_watchdog.log` | Tick throughput watchdog (alerts on throttling) |
| `/var/log/trading-api.log` | uvicorn access + app logs |
| `journalctl -u db-writer` | db_writer insert activity |
| `journalctl -u tick-collector` | Websocket connection + subscription logs |

---

## Troubleshooting

### tick-collector died and won't restart

Check the exit code:
```bash
systemctl status tick-collector.service
journalctl -u tick-collector.service -n 30 --no-pager
```

- **`code=killed, signal=TERM`** → normal scheduled stop by `stop_trading.sh`
- **`RestartPreventExitStatus=3` / exit code 3`** → Kite access token is expired or invalid. Services will NOT restart in a loop (by design). Fix:
  ```bash
  bash ~/run_refresh_token.sh
  # then
  bash /home/ubuntu/projects/options-trading-platform/start_trading.sh
  ```
- **Any other crash** → check logs for Python traceback, restart normally:
  ```bash
  sudo systemctl restart tick-collector.service
  ```

### No ticks are flowing but services are running

```bash
# 1. Check if Redis stream is growing
redis-cli XLEN ticks_stream

# 2. Check if db_writer is reading (should log "Inserted N rows" every ~0.75s during market hours)
journalctl -u db-writer.service -n 10 --no-pager

# 3. Check for pending (unACKed) messages in the consumer group
redis-cli XPENDING ticks_stream db_writer_group

# 4. Check tick-collector WebSocket connection in logs
journalctl -u tick-collector.service --since "5 minutes ago" --no-pager
```

### db-writer is not inserting

```bash
# Check PostgreSQL is reachable
psql -h localhost -U postgres -d tickdata -c "SELECT 1;"

# Check pending messages in Redis PEL (should be near 0 normally)
redis-cli XPENDING ticks_stream db_writer_group

# Restart db-writer (safe at any time — it drains the PEL on startup before reading new messages)
sudo systemctl restart db-writer.service
```

### trading-api (uvicorn) is not responding

```bash
systemctl status trading-api.service
tail -50 /var/log/trading-api.log

# Restart (note: loses in-memory chart cache, first loads will be slower)
sudo systemctl restart trading-api.service
```

### Kite token invalid mid-session

The daily token refresh runs at 07:00 AM automatically. Tokens expire at midnight and the 07:00 refresh takes ~45 seconds (Playwright browser). If you need to refresh manually:

```bash
cd /home/ubuntu/projects/options-trading-platform
source venv/bin/activate
python refresh_token.py
# then restart the API server so it loads the new token from .env:
sudo systemctl restart trading-api.service
# and restart ingestion:
bash start_trading.sh
```

### Today is a holiday — skip session

Touch the flag file before the 09:08 cron fires:
```bash
touch ~/.trading_paused
# To re-enable for the next session:
rm ~/.trading_paused
```

`start_trading.sh` and `stop_trading.sh` both check for this file and exit early if it exists.

---

## Full Daily Startup Sequence (manual walkthrough)

In practice this is fully automated. If you need to run it manually (e.g., after a server reboot mid-morning):

```bash
# Step 1: Verify token is fresh (run_refresh_token.sh auto-refreshes at 07:00)
tail -5 ~/token_refresh.txt
# Should show "Token refresh SUCCESSFUL" with today's date

# Step 2: Verify instrument file was updated (auto at 07:05)
tail -3 ~/instrument_update.txt
# Should show "Instrument file updated successfully"

# Step 3: Verify trading-api (uvicorn) is running
systemctl is-active trading-api.service

# Step 4: Start ingestion
bash /home/ubuntu/projects/options-trading-platform/start_trading.sh

# Step 5: Confirm ticks are flowing (within ~30 seconds)
journalctl -u db-writer.service -f
# Should see: "Inserted N rows into DB" every ~0.75 seconds
```

---

## Full Shutdown Sequence (manual walkthrough)

```bash
# Step 1: Stop ingestion services (db-writer and tick-collector)
bash /home/ubuntu/projects/options-trading-platform/stop_trading.sh
# Or directly: sudo systemctl stop tick-collector.service db-writer.service

# Step 2: Verify they stopped
systemctl is-active db-writer.service tick-collector.service
# Both should show "inactive"

# Note: trading-api.service is always-on and should NOT be stopped unless
# you are doing maintenance on PostgreSQL or the application itself.
```

---

## Redis Stream Maintenance

The `ticks_stream` accumulates all day. `start_trading.sh` runs `redis-cli DEL ticks_stream` each morning to prevent unbounded growth. If you need to check its state manually:

```bash
redis-cli XLEN ticks_stream              # total message count
redis-cli XPENDING ticks_stream db_writer_group   # unACKed message count (should be ~0)
```

A non-zero PEL count after db-writer restarts is normal — it drains those messages automatically in Phase 1 before entering the live read loop.
