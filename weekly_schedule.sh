#!/bin/bash
# weekly_schedule.sh — Runs every Sunday at 10:30pm.
# Sends a summary of all cron job schedules for the coming week.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NEXT_MONDAY=$(date -d "next monday" '+%d %b %Y')

"$SCRIPT_DIR/notify.sh" "📅 Weekly Cron Schedule — Week of ${NEXT_MONDAY}

Every weekday (Mon–Fri):
  07:00 AM — Token refresh
  07:10 AM — VACUUM ANALYZE
  09:08 AM — Start trading session
  03:30 PM — Stop trading session
  03:35 PM — Holiday check (sets pause flag if holiday tomorrow)
  03:40 PM — Daily summary

Every Monday only:
  07:05 AM — Instrument update

Every day (incl. weekends):
  03:40 PM — Daily summary
  09:30 PM — Resume reminder (only fires if holiday mode is ON)"
