# TODO

## Status after Apr 28–30 reports

Fix 1 (conviction index): **confirmed working** — conviction gone from pg_stat_statements top 10 on all three days. Pool wait p99 = 0.1ms.

Chart slowness: **not resolved**. New primary bottleneck is the chart query itself (`candles_5s` full-day scan). Chart query mean: 52ms (Apr 27, baseline) → 253ms (Apr 28) → 1,221ms (Apr 29) → 163ms (Apr 30). Cause of Apr 29 spike: dead tuple bloat (autovacuum hadn't run) + heap scatter. Autovacuum caught up Apr 29 overnight; Apr 30 recovered but still elevated.

---

## Open: candles_5s chart query slowness (new primary bottleneck)

Chart query (`AND bucket >= EXTRACT` in pg_stat_statements) is now #1–2 by DB time on every day. Mean 163–1,221ms depending on day and table state. Was hidden behind conviction's 41% load.

**Root cause**: candles_5s heap scatter worsens through the day as new candles insert into random heap positions (HOT updates go to existing pages but new symbols scatter). By 13:00–15:00, bucket correlation drops to 0.1–0.4, forcing random I/O per chart query.

**Requires investigation before any fix is proposed.** Need to understand:
- Why Apr 29 was 1,221ms mean but Apr 30 (worse correlation) was only 163ms
- What the `WHERE symbol` query in top 10 is (new entry, 300–479ms, 9–15% of DB time)
- Whether CLUSTER/pg_repack on candles_5s is the right intervention or if a covering index solves it cheaper

Do not propose a fix until the cause is fully explained by data.

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
