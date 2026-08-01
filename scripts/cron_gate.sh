#!/bin/bash
# Generic cron job gate. Crontab lines are prefixed with:
#     /bin/bash .../scripts/cron_gate.sh <job_key> && <original command>
# The UI Settings menu writes config/cron_control.json ({"<job_key>": false}
# to disable). Missing file or missing key = enabled, so platform-critical
# jobs keep running if the config is ever lost.
# Skips are logged to ~/cron_skips.log.

KEY="$1"
CONTROL=/home/ubuntu/projects/options-trading-platform/config/cron_control.json

ENABLED=$(/usr/bin/python3 -c "
import json
print(json.load(open('$CONTROL')).get('$KEY', True) is not False)
" 2>/dev/null)

if [ "$ENABLED" = "False" ]; then
    echo "$(date '+%F %T') skipped: $KEY (toggle OFF)" >> /home/ubuntu/cron_skips.log
    exit 1
fi
exit 0
