#!/usr/bin/env bash
# Launch copyability_3split_runner.py under preflight + rss_watchdog.
#
# STAGED 2026-05-29 06:55 CEST. Does NOT auto-fire because Alberto's TG 7194
# A/B/C decision is still pending and this is a long-running job.
#
# OOM-hardening notes:
#  - preflight RSS gate (1.5GB est * 1 worker, 1.0GB margin)
#  - rss_watchdog 6GB ceiling (this script loads more fills than journey_trace)
#  - output + logs to app/data/v13/copyability_3split/ (NEVER /tmp)
#  - LIMITATION: copyability_3split_runner.py has NO checkpoint/resume yet.
#    A reboot loses the whole run. Per-fold checkpointing is a TODO (8 folds;
#    each fold output should write to disk before starting next). Surface to
#    Alberto before launching for runs >2h.
#
# Usage:
#   bash scripts/launch_copyability_3split.sh           # dry-print the command
#   bash scripts/launch_copyability_3split.sh --execute # actually launch
#
# Inputs (verified by this script before launching):
#   app/data/v13/wallet_journeys_costed.parquet     ← built by concat after orchestrator
#   app/data/v13/wallet_equity_series_v7.parquet    ← walker output (exists)
#   app/data/hl_s3_fills/YYYYMMDD.parquet           ← raw fills (exists)

set -euo pipefail

REPO=/Users/hermes/quants-lab
PYTHON=/Users/hermes/miniforge3/envs/quants-lab/bin/python
JOURNEYS="$REPO/app/data/v13/wallet_journeys_costed.parquet"
EQUITY="$REPO/app/data/v13/wallet_equity_series_v7.parquet"
FILLS_DIR="$REPO/app/data/hl_s3_fills"
OUT_DIR="$REPO/app/data/v13/copyability_3split"
WD_LOG="$OUT_DIR/watchdog.log"
JOB_LOG="$OUT_DIR/run.log"
BUDGET_GB=6.0

mkdir -p "$OUT_DIR"

# Verify inputs exist
if [[ ! -f "$JOURNEYS" ]]; then
  echo "ERROR: $JOURNEYS not found. Run scripts/v13_concat_journey_chunks.py first."
  exit 2
fi
if [[ ! -f "$EQUITY" ]]; then
  echo "ERROR: $EQUITY not found."
  exit 2
fi

CMD=(
  "$PYTHON" "$REPO/scripts/copyability_3split_runner.py"
    --journeys "$JOURNEYS"
    --equity-series "$EQUITY"
    --fills-dir "$FILLS_DIR"
    --window-start 2025-12-01
    --window-end   2026-05-22
    --latencies     120,300
    --capital-scales 500,1000,5000,10000
    --fee-rt-bps    8.64
    --max-copy-leverage 10.0
    --min-order-notional 10.0
    --primary-capital-scale 1000
    --primary-latency 120
    --out-dir       "$OUT_DIR"
)

WATCHDOG=(
  "$PYTHON" "$REPO/scripts/rss_watchdog.py"
    --budget-gb "$BUDGET_GB" --interval 15
    --log "$WD_LOG"
    --
)

if [[ "${1:-}" != "--execute" ]]; then
  echo "=== STAGED LAUNCH (dry; pass --execute to fire) ==="
  echo "Preflight:"
  echo "  $PYTHON $REPO/scripts/preflight_rss_budget.py --estimated-gb 1.5 --workers 1 --safety-margin-gb 1.0"
  echo "Command:"
  printf "  %s" "${WATCHDOG[@]}"
  printf "\n"
  printf "  %s" "${CMD[@]}"
  printf "\n"
  echo "Output dir:   $OUT_DIR"
  echo "Job log:      $JOB_LOG"
  echo "Watchdog log: $WD_LOG"
  exit 0
fi

echo "Preflight..."
"$PYTHON" "$REPO/scripts/preflight_rss_budget.py" \
  --estimated-gb 1.5 --workers 1 --safety-margin-gb 1.0 || exit 1

echo "LAUNCH at $(date '+%F %T %Z')"
nohup "${WATCHDOG[@]}" "${CMD[@]}" > "$JOB_LOG" 2>&1 &
PID=$!
echo "PID=$PID"
echo "  job log: $JOB_LOG"
echo "  watchdog log: $WD_LOG"
echo "  output dir: $OUT_DIR"
