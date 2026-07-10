#!/bin/bash
# SPRINT continuation: retry the canonical S3 fills refresh until S3 catches up, then run the
# full 18d forward chain automatically. Leaves verdict markers for the heartbeat to pick up.
# Launch: nohup bash scripts/sprint_forward_loop.sh > /tmp/forward_loop.log 2>&1 &
set -uo pipefail
P=/Users/hermes/miniforge3/envs/quants-lab/bin/python
R=/Users/hermes/quants-lab
cd "$R"; set -a; source .env; set +a

while true; do
  LAST=$(ls "$R/app/data/hl_s3_fills_v2_hot/" | sort | tail -1 | cut -d. -f1)
  if [ "$LAST" -ge 20260610 ] 2>/dev/null; then
    echo "$(date) fills complete through $LAST -> running forward chain"
    bash "$R/scripts/sprint_forward_chain.sh" 2026-06-10 > /tmp/forward_chain_18d.log 2>&1
    RC=$?
    echo "$(date) chain exit $RC" | tee /tmp/FORWARD_18D_DONE.marker
    tail -50 /tmp/forward_chain_18d.log >> /tmp/FORWARD_18D_DONE.marker
    exit $RC
  fi
  echo "$(date) fills at $LAST; retrying canonical S3 refresh (3 workers)"
  pkill -f hl_s3_fills_daily_refresh.py 2>/dev/null
  sleep 5
  $P "$R/data_pipeline/hl_s3_fills_daily_refresh.py" \
    --wallet-source "$R/app/data/v13/equity_universe_20k.parquet" \
    --wallet-source "$R/config/copy_trader_wallets_gate1_v4.json" \
    --start 2026-05-28 --end 2026-06-11 \
    --out-dir "$R/app/data/hl_s3_fills_v2_hot" \
    --candles-out-dir "$R/app/data/hl_s3_candles_1m_hot" \
    --manifest "$R/app/data/hl_s3_fills_v2_hot_manifest.json" \
    --n-workers 3 --no-prune >> /tmp/s3_loop.log 2>&1
  echo "$(date) attempt ended (rc $?); cooldown 20min"
  sleep 1200
done
