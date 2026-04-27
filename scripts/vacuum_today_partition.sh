#!/bin/bash
# Run VACUUM (ANALYZE) on today's gap_ticks partition.
# Scheduled at 15:36 IST Mon–Fri (after market writes settle).
# Updates the visibility map so get_jump_history Index Only Scans avoid heap fetches.
PARTITION="gap_ticks_$(TZ=Asia/Kolkata date +%Y_%m_%d)"
echo "$(TZ=Asia/Kolkata date '+%Y-%m-%d %H:%M:%S IST'): VACUUM (ANALYZE) ${PARTITION}"
PGPASSWORD='MustafaHasnain@123' PGOPTIONS='-c statement_timeout=0' \
    psql -h localhost -U postgres -d tickdata \
    -c "VACUUM (ANALYZE) ${PARTITION};" \
    && echo "Done: ${PARTITION}" \
    || echo "FAILED: ${PARTITION}"
