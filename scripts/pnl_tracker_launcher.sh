#!/bin/bash
# pnl_tracker_launcher.sh — wrapper for pnl_tracker.py under launchd.
set -euo pipefail
WORKDIR="/Users/hermes/quants-lab"
PYTHON="/Users/hermes/miniforge3/envs/quants-lab/bin/python"
SCRIPT="tools/pnl_tracker.py"
cd "$WORKDIR"
if [ -f .env ]; then set -a; source .env; set +a; fi
echo "[$(date '+%F %T %Z')] pnl_tracker_launcher: starting $SCRIPT --tg --epoch --loop 15"
exec "$PYTHON" "$SCRIPT" --tg --epoch --loop 15
