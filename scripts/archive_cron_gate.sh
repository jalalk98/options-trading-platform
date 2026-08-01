#!/bin/bash
# Gate for the archive_to_b2_v3 crons. The UI Settings toggles write
# config/archive_control.json; each phase runs only when its flag is true.
#   export → data_archival        (02:00 cron, Phase A: export to B2)
#   drop   → delete_archive_data  (06:45 cron, Phase B: drop verified partitions)
# Missing/unreadable file fails safe: the phase is skipped.

PHASE="$1"
DIR=/home/ubuntu/projects/options-trading-platform
CONTROL="$DIR/config/archive_control.json"

case "$PHASE" in
    export) KEY="data_archival" ;;
    drop)   KEY="delete_archive_data" ;;
    *) echo "$(date '+%F %T') usage: archive_cron_gate.sh export|drop"; exit 1 ;;
esac

ENABLED=$("$DIR/venv/bin/python3" -c "
import json
print(json.load(open('$CONTROL')).get('$KEY') is True)
" 2>/dev/null)

if [ "$ENABLED" != "True" ]; then
    echo "$(date '+%F %T') --phase $PHASE skipped: '$KEY' toggle is OFF in $CONTROL"
    exit 0
fi

exec "$DIR/venv/bin/python3" "$DIR/scripts/archive_to_b2_v3.py" --phase "$PHASE"
