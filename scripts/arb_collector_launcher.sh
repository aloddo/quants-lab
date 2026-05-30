#!/bin/bash
# arb_collector_launcher.sh — wrapper for arb_hl_bybit_collector.py under launchd.
set -euo pipefail

WORKDIR="/Users/hermes/quants-lab"
PYTHON="/Users/hermes/miniforge3/envs/quants-lab/bin/python"
SCRIPT="data_pipeline/arb_hl_bybit_collector.py"

cd "$WORKDIR"

if [ -f .env ]; then
  set -a
  # shellcheck source=/dev/null
  source .env
  set +a
fi

echo "[$(date '+%F %T %Z')] arb_collector_launcher: starting $SCRIPT"
exec "$PYTHON" "$SCRIPT"
