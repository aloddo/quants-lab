#!/bin/bash
# listing_monitor_launcher.sh — wrapper for listing_monitor.py under launchd.
#
# Used by /Library/LaunchDaemons/com.quantslab.listing-monitor.plist.
# Note: no flock; launchd KeepAlive guarantees one supervised instance.

set -euo pipefail

WORKDIR="/Users/hermes/quants-lab"
PYTHON="/Users/hermes/miniforge3/envs/quants-lab/bin/python"
SCRIPT="data_pipeline/listing_monitor.py"

cd "$WORKDIR"

if [ -f .env ]; then
  set -a
  # shellcheck source=/dev/null
  source .env
  set +a
fi

echo "[$(date '+%F %T %Z')] listing_monitor_launcher: starting $SCRIPT --loop --interval 30"
exec "$PYTHON" "$SCRIPT" --loop --interval 30
