#!/bin/bash
# hl_mark_collector_launcher.sh — supervised wrapper for the live HL mark collector under launchd KeepAlive.
#
# 2026-07-10 (Phase-1 cleanup): the mark collector previously ran UNSUPERVISED via nohup (PID orphaned to
# PPID 1). If it died, nothing relaunched it and the live 1m mark stream (app/data/hl_mark_1m_hot) silently
# gapped — the exact OOS-blocking failure it exists to prevent. This wrapper gives it the same KeepAlive
# discipline as the engine. Used by com.quantslab.hl-mark-collector.plist (see ops/launchd/).
#
# Pause: touch /tmp/hl_mark_collector_pause to make the launcher exit cleanly; remove to resume.
set -euo pipefail

WORKDIR="/Users/hermes/quants-lab"
PYTHON="/Users/hermes/miniforge3/envs/quants-lab/bin/python"
cd "$WORKDIR"

if [ -f /tmp/hl_mark_collector_pause ]; then
  echo "[$(date '+%F %T %Z')] hl_mark_collector_launcher: pause flag present, exiting (paused)"
  sleep 5
  exit 0
fi

# Source .env (auto-export) — the collector reads HL query address / any endpoint overrides from it.
if [ -f .env ]; then
  set -a
  # shellcheck source=/dev/null
  source .env
  set +a
fi

echo "[$(date '+%F %T %Z')] hl_mark_collector_launcher: starting hl_live_mark_collector (interval 60s)"
exec "$PYTHON" data_pipeline/hl_live_mark_collector.py --interval 60 --flush-every 5
