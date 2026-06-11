#!/bin/bash
# SPRINT continuation: retry the node-fills download until the S3 throttle clears, then run the
# full 18d forward chain automatically. Leaves verdict markers for the heartbeat to pick up.
# Launch: nohup bash scripts/sprint_forward_loop.sh > /tmp/forward_loop.log 2>&1 &
set -uo pipefail
P=/Users/hermes/miniforge3/envs/quants-lab/bin/python
R=/Users/hermes/quants-lab
cd "$R"; set -a; source .env; set +a

while true; do
  LAST=$(ls "$R/app/data/hl_s3_fills_v2/" | sort | tail -1 | cut -d. -f1)
  if [ "$LAST" -ge 20260610 ] 2>/dev/null; then
    echo "$(date) fills complete through $LAST -> running forward chain"
    bash "$R/scripts/sprint_forward_chain.sh" 2026-06-10 > /tmp/forward_chain_18d.log 2>&1
    RC=$?
    echo "$(date) chain exit $RC" | tee /tmp/FORWARD_18D_DONE.marker
    tail -50 /tmp/forward_chain_18d.log >> /tmp/FORWARD_18D_DONE.marker
    exit $RC
  fi
  echo "$(date) fills at $LAST; retrying download (single proc, 3 workers)"
  pkill -f v13_s3_fills_downloader 2>/dev/null
  sleep 5
  # no GNU timeout on macOS; the downloader self-bounds (as_completed timeout=1800/day)
  $P "$R/research/v13/v13_s3_fills_downloader_enriched.py" \
    --start 2026-05-28 --end 2026-06-11 --n-workers 3 >> /tmp/s3_loop.log 2>&1
  echo "$(date) attempt ended (rc $?); cooldown 20min"
  sleep 1200
done
