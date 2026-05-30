#!/bin/bash
# v12_launcher.sh — wrapper for hl_copy_trader_v11.py under launchd.
#
# Used by /Library/LaunchDaemons/com.quantslab.v12-copy-trader.plist.
# Note: no flock; launchd KeepAlive guarantees one supervised instance.
# Do not start V12 manually via tmux while the plist is loaded.

set -euo pipefail

WORKDIR="/Users/hermes/quants-lab"
PYTHON="/Users/hermes/miniforge3/envs/quants-lab/bin/python"
SCRIPT="strategies/live/hl_copy_trader_v11.py"
CONFIG="config/copy_trader_wallets.json"

cd "$WORKDIR"

# 2026-05-22: pause flag for ops maintenance (drift heal, schema changes, etc).
# Touch /tmp/v12_pause to make launcher exit 0 cleanly; remove to resume.
if [ -f /tmp/v12_pause ]; then
  echo "[$(date '+%F %T %Z')] v12_launcher: /tmp/v12_pause present, exiting (paused)"
  sleep 5
  exit 0
fi

# Source .env safely (auto-export, then disable)
if [ -f .env ]; then
  set -a
  # shellcheck source=/dev/null
  source .env
  set +a
fi

echo "[$(date '+%F %T %Z')] v12_launcher: starting $SCRIPT"
exec "$PYTHON" "$SCRIPT" --config "$CONFIG"
