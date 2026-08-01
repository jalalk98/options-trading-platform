## Backlog detection and capped backfill (added 2026-05-16)

Phase A now scans for unarchived older dates before processing today's target.

**Behavior:**
- Runs only on cron-driven invocations (no `--date` argument)
- Caps at MAX_BACKLOG_PER_RUN (currently 5) dates per run
- Processes oldest first
- Skips dates with status='failed' (operator must reset to pending manually)
- Sends Telegram alert when backlog is detected
- Continues to next backlog date if one fails — does not block the whole run
- Regular target runs after all backlog dates (or all attempts) complete

**To adjust the cap:** edit `MAX_BACKLOG_PER_RUN` constant near top of script.

**To force-process a 'failed' backlog date:** reset it to 'pending':
```sql
UPDATE archive_status SET status='pending', failure_reason=NULL
WHERE partition_date='<date>' AND status='failed';
```
Then the next cron run will pick it up as backlog.

**Manual runs unaffected:** `--phase export --date YYYY-MM-DD` still processes
only the specified date, no backlog scan.
