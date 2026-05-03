# TODO

## Status after Apr 28–30 reports

Fix 1 (conviction index): **confirmed working** — conviction gone from pg_stat_statements top 10 on all three days. Pool wait p99 = 0.1ms.

Chart slowness: **not resolved**. New primary bottleneck is the chart query itself (`candles_5s` full-day scan). Chart query mean: 52ms (Apr 27, baseline) → 253ms (Apr 28) → 1,221ms (Apr 29) → 163ms (Apr 30). Cause of Apr 29 spike: dead tuple bloat (autovacuum hadn't run) + heap scatter. Autovacuum caught up Apr 29 overnight; Apr 30 recovered but still elevated.

---

## candles_5s chart query slowness — fix deployed

**Root cause confirmed**: Statistics freshness, not heap scatter or plan choice.
- Apr 29 (no autovacuum all session, stale stats): 1,221ms mean
- Apr 30 (autoanalyze at 09:34 UTC pre-open, fresh stats): 163ms mean
- Same dead-tuple ratio (~6–7%) both days — bloat not the driver
- EXPLAIN: Bitmap Heap Scan on candles_5s_pkey, 143ms for 4501 rows (BANKNIFTY). Plan is correct.

**Fix deployed (May 2)**: Scheduled ANALYZE candles_5s (no VACUUM) via cron:
- 09:00 IST Mon–Fri — pre-open fresh stats guarantee
- 12:00 IST Mon–Fri — mid-session refresh
- Logs to `~/perf_reports/manual_analyze.log`

**Verification (Monday EOD)**:
- Section 7 (stats freshness): 09:30 n_mod_pct ~0%, 11:58 shows accumulated %, 12:02 ~0% (post-ANALYZE), 15:25 final state
- Chart query mean should land 150–250ms (matching Apr 30). If >400ms despite cron firing, diagnosis is incomplete.

**NOT doing**: pg_repack/CLUSTER, autovacuum tuning, covering index. 163ms at fresh-stats baseline is within SLO.

**`WHERE symbol` mystery (Apr 28–30 reports)**: Identified as EXISTS subquery inside `get_hist_symbols()` (strikes.py:1234). Runs once per tracked symbol (~1100 executions) per uncached hist-symbols call. Not a bottleneck — fires once per restart per historical date, cached after first hit.

---

## Fix 3 — get_jump_history Query 2 rewrite (deferred)

Rewrite `get_jump_history` Query 2 to source fill state from `_pending_fills`
rather than replaying all ticks from the first jump timestamp.

**Current cost (after Fix 2 vacuum)**: ~786ms mean × ~420 calls = ~5.5 min DB time/day (Apr 30).
Improving gradually. Still worth doing in a maintenance week but not blocking.

**Constraints:**
- `_pending_fills` is populated server-side in real-time but wiped on restart
- The replay path must remain as fallback for cold-start (server restart wipes `_pending_fills`)
- Full equivalence testing required: run both paths in parallel for one day, diff outputs

---

## Pool size — closed

Pool contention solved by Fix 1. p99 wait = 0.1ms. max_size=8 is sufficient.
Do not change pool config.

---

## TTL for `_pending_fills` (deferred)

p99 fill age across observed days: Apr 27=8,335s, Apr 28=5,212s, Apr 29=534s, Apr 30=16,823s.
High variance across days — not stable enough to set a TTL yet. Revisit after chart query
slowness is resolved and the system is in steady state.
