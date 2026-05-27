#!/bin/bash
# hl_wallet_collector_launcher.sh — wrapper for hl_wallet_collector.py under launchd.
set -euo pipefail

WORKDIR="/Users/hermes/quants-lab"
PYTHON="/Users/hermes/miniforge3/envs/quants-lab/bin/python"
SCRIPT="scripts/hl_wallet_collector.py"

cd "$WORKDIR"

if [ -f .env ]; then
  set -a
  # shellcheck source=/dev/null
  source .env
  set +a
fi

echo "[$(date '+%F %T %Z')] hl_wallet_collector_launcher: starting $SCRIPT"
exec "$PYTHON" "$SCRIPT"
