#!/usr/bin/env bash
# Launch v13 journey_trace in 2-lane parallel mode.
#
# OOM-hardened (post 2026-05-29 OOM-2):
#  - preflight RSS gate refuses to start if RAM budget impossible
#  - rss_watchdog SIGKILLs any chunk that exceeds 4GB RSS
#  - journey_trace --checkpoint-every 200 flushes partial parquet every 200 wallets
#  - all logs + outputs go to app/data/v13/journey_chunks/, NEVER /tmp
#
# Usage:
#   bash scripts/launch_v13_journey_parallel.sh           # process all 17 chunks, 2 at a time
#   bash scripts/launch_v13_journey_parallel.sh 0 3       # process chunks 0..3 only
#
# Each chunk takes ~1-2h. With 2 lanes, 17 chunks finishes in ~10-12h wall time.
# Resume is automatic: kill at any point and re-run; only un-done wallets per
# chunk get re-processed.

set -euo pipefail

REPO=/Users/hermes/quants-lab
PYTHON=/Users/hermes/miniforge3/envs/quants-lab/bin/python
CHUNK_DIR="$REPO/app/data/v13/journey_chunk_lists"
OUT_DIR="$REPO/app/data/v13/journey_chunks"
LOG_DIR="$REPO/app/data/v13/journey_chunks/logs"
EQUITY="$REPO/app/data/v13/wallet_equity_series_v7.parquet"
START="2025-12-01"
END="2026-05-22"
LANES=2
BUDGET_GB=4.0
CHECKPOINT_EVERY=200

mkdir -p "$OUT_DIR" "$LOG_DIR"

# Discover chunks
mapfile -t ALL_CHUNKS < <(ls "$CHUNK_DIR"/chunk_*.txt | sort)
N_CHUNKS=${#ALL_CHUNKS[@]}
if [[ $N_CHUNKS -eq 0 ]]; then
  echo "ERROR: no chunk_*.txt in $CHUNK_DIR" >&2
  exit 2
fi

FIRST=${1:-0}
LAST=${2:-$((N_CHUNKS - 1))}
echo "Launching chunks $FIRST..$LAST of $N_CHUNKS (lanes=$LANES, budget=${BUDGET_GB}GB, checkpoint_every=$CHECKPOINT_EVERY)"

# Preflight: need 2 chunks * 1.5GB = 3GB at minimum
"$PYTHON" "$REPO/scripts/preflight_rss_budget.py" \
  --estimated-gb 1.5 --workers "$LANES" || {
    echo "PREFLIGHT REFUSED. Free RAM before retrying."
    exit 1
  }

run_chunk() {
  local chunk_path="$1"
  local chunk_name
  chunk_name=$(basename "$chunk_path" .txt)
  local out_path="$OUT_DIR/${chunk_name}.parquet"
  local job_log="$LOG_DIR/${chunk_name}.log"
  local wd_log="$LOG_DIR/${chunk_name}.watchdog.log"

  echo "[$(date '+%H:%M:%S')] LAUNCH $chunk_name → $out_path"
  "$PYTHON" "$REPO/scripts/rss_watchdog.py" \
    --budget-gb "$BUDGET_GB" --interval 30 --log "$wd_log" \
    -- "$PYTHON" "$REPO/scripts/v13_journey_trace.py" \
      --start "$START" --end "$END" \
      --wallets "$chunk_path" \
      --equity-series "$EQUITY" \
      --output "$out_path" \
      --checkpoint-every "$CHECKPOINT_EVERY" \
    > "$job_log" 2>&1
  local rc=$?
  echo "[$(date '+%H:%M:%S')] DONE $chunk_name rc=$rc"
  return $rc
}

# Process the requested range with at most $LANES concurrent jobs.
# Use background jobs + wait -n (bash 4.3+) for the lane scheduler.
declare -A LANE_PID
LAUNCHED=0
for ((i=FIRST; i<=LAST; i++)); do
  chunk_path="${ALL_CHUNKS[$i]}"
  # Wait for an open lane if at capacity
  while (( ${#LANE_PID[@]} >= LANES )); do
    if wait -n "${!LANE_PID[@]}" 2>/dev/null; then
      # one finished; figure out which
      for pid in "${!LANE_PID[@]}"; do
        if ! kill -0 "$pid" 2>/dev/null; then
          unset 'LANE_PID[$pid]'
          break
        fi
      done
    else
      # wait -n failed (maybe no children); break to avoid spin
      break
    fi
  done
  # Re-check budget every launch (other processes may have grown)
  "$PYTHON" "$REPO/scripts/preflight_rss_budget.py" \
    --estimated-gb 1.5 --workers 1 || {
      echo "PREFLIGHT REFUSED mid-run, waiting for in-flight to finish before retrying $chunk_path"
      wait
      "$PYTHON" "$REPO/scripts/preflight_rss_budget.py" \
        --estimated-gb 1.5 --workers 1 || {
          echo "Still no RAM. Aborting."
          exit 1
        }
    }
  run_chunk "$chunk_path" &
  pid=$!
  LANE_PID[$pid]=1
  LAUNCHED=$((LAUNCHED+1))
  echo "[$(date '+%H:%M:%S')] lane filled: pid=$pid chunk=$(basename "$chunk_path") (lanes=${#LANE_PID[@]}/$LANES)"
done

# Drain remaining
wait
echo "ALL DONE. Launched=$LAUNCHED chunks."
echo "Outputs in $OUT_DIR/, logs in $LOG_DIR/"
