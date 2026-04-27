# TODO

## Fix 3 — get_jump_history Query 2 rewrite (deferred)

Rewrite `get_jump_history` Query 2 to source fill state from `_pending_fills`
rather than replaying all ticks from the first jump timestamp.

**Current cost after Fix 2** (visibility map VACUUM): ~500ms × 742 calls = ~6 min DB time/day.
Worth doing in a maintenance week — not blocking anything now.

**Constraints:**
- `_pending_fills` is populated server-side in real-time but wiped on restart
- The replay path must remain as fallback for cold-start (server restart wipes `_pending_fills`)
- Full equivalence testing required: run both paths in parallel for one day, diff outputs
- Fill detection logic (`pre_price` crossing) is the same in both paths — no semantic change needed

**Implementation sketch:**
1. In `get_jump_history`, check if `_pending_fills[symbol]` has an entry for each jump bucket
2. If yes: use stored `filled` / `filled_bucket` from `_pending_fills` — skip Query 2 entirely
3. If no (cold start or symbol not yet tracked): fall back to current replay scan
4. Add `[FILL_SKIP]` log line when the fill scan is skipped to confirm the fast path is used

**Acceptance criteria:** `[FILL_SKIP]` appears for all warmed symbols; fill display identical to current in UI.

---

## Pool contention re-check (deferred — revisit after Tuesday's report)

After Fix 1 (conviction index), check Tuesday's daily report:
- Pool wait p99 at ≥15:25 < 5ms? → no action needed
- If p99 still high → investigate whether conviction is no longer the cause

Do not increase `max_size` until data shows it's necessary.

---

## TTL for `_pending_fills` (deferred)

p99 fill age on Apr 27 was 8,335s (~138 min). p95 was 3,425s (~57 min).

Revisit once conviction + jump-history fixes have landed. If TTL becomes relevant:
- Target ~10,002s (p99 × 1.2 safety margin)
- Shadow mode first: log evictions without deleting for one full trading day
- Activate only after shadow mode confirms no false evictions at p99+

Do not set TTL until post-Fix-3 data is available.
