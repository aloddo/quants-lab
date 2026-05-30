#!/usr/bin/env bash
# Sequential chunk runner (post chunk_00 success on 2026-05-29).
# Single lane to stay well under the 16GB ceiling given observed 4.6GB peak RSS.
# Each chunk launched with: preflight gate, 10s-poll watchdog, 5GB budget.
#
# Usage: bash scripts/run_chunks_sequential.sh START END
#        bash scripts/run_chunks_sequential.sh 01 16

set -euo pipefail

REPO=/Users/hermes/quants-lab
PYTHON=/Users/hermes/miniforge3/envs/quants-lab/bin/python
CHUNK_DIR="$REPO/app/data/v13/journey_chunk_lists"
OUT_DIR="$REPO/app/data/v13/journey_chunks"
LOG_DIR="$OUT_DIR/logs"
EQUITY="$REPO/app/data/v13/wallet_equity_series_v7.parquet"
START_DATE="2025-12-01"
END_DATE="2026-05-22"
BUDGET_GB=5.0
POLL_INTERVAL=10
CHECKPOINT_EVERY=200

mkdir -p "$OUT_DIR" "$LOG_DIR"

FIRST="${1:-01}"
LAST="${2:-16}"

echo "Sequential run: chunks $FIRST..$LAST"
for ((n=10#$FIRST; n<=10#$LAST; n++)); do
  ci=$(printf "%02d" "$n")
  chunk_path="$CHUNK_DIR/chunk_${ci}.txt"
  out_path="$OUT_DIR/chunk_${ci}.parquet"
  job_log="$LOG_DIR/chunk_${ci}.log"
  wd_log="$LOG_DIR/chunk_${ci}.watchdog.log"

  if [[ ! -f "$chunk_path" ]]; then
    echo "[$(date '+%H:%M:%S')] SKIP chunk_$ci (file not found: $chunk_path)"
    continue
  fi
  if [[ -f "$out_path" ]]; then
    echo "[$(date '+%H:%M:%S')] SKIP chunk_$ci (already done: $out_path)"
    continue
  fi

  # Preflight: 1.5GB est per worker (chunk_00 mean; transient peak ~4.6GB
  # is caught by the 5GB rss_watchdog ceiling — watchdog is the real guard).
  # Safety margin 1.0GB (down from default 2.0GB) — appropriate because
  # the watchdog is the active runtime ceiling, not preflight.
  # Retry-and-wait if RAM tight; cap retries so true low-RAM still surfaces.
  pre_tries=0
  pre_max=12  # 12 * 30s = 6 min max wait
  while true; do
    echo "[$(date '+%H:%M:%S')] PREFLIGHT chunk_$ci try=$((pre_tries+1))/$pre_max"
    if "$PYTHON" "$REPO/scripts/preflight_rss_budget.py" \
        --estimated-gb 1.5 --workers 1 --safety-margin-gb 1.0; then
      break
    fi
    pre_tries=$((pre_tries+1))
    if (( pre_tries >= pre_max )); then
      echo "[$(date '+%H:%M:%S')] PREFLIGHT exhausted retries on chunk_$ci. Aborting."
      exit 1
    fi
    echo "[$(date '+%H:%M:%S')] preflight tight — sleeping 30s for inactive pages to reclaim"
    sleep 30
  done

  echo "[$(date '+%H:%M:%S')] LAUNCH chunk_$ci → $out_path"
  "$PYTHON" "$REPO/scripts/rss_watchdog.py" \
    --budget-gb "$BUDGET_GB" --interval "$POLL_INTERVAL" --log "$wd_log" \
    -- "$PYTHON" "$REPO/scripts/v13_journey_trace.py" \
      --start "$START_DATE" --end "$END_DATE" \
      --wallets "$chunk_path" \
      --equity-series "$EQUITY" \
      --output "$out_path" \
      --checkpoint-every "$CHECKPOINT_EVERY" \
    > "$job_log" 2>&1
  rc=$?
  echo "[$(date '+%H:%M:%S')] DONE chunk_$ci rc=$rc"
  if [[ $rc -ne 0 ]]; then
    echo "[$(date '+%H:%M:%S')] chunk_$ci FAILED (rc=$rc); see $job_log"
    exit "$rc"
  fi
done

echo "[$(date '+%H:%M:%S')] ALL CHUNKS DONE"
ls -la "$OUT_DIR/"chunk_*.parquet 2>/dev/null
