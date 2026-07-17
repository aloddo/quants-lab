#!/bin/bash
# M2 daily pipeline: funding/ledger hot-refresh -> M2 daily-incremental journeys.
# Runs AFTER com.quantslab.hl-s3-fills-daily (06:20) so fills for the new day are present.
# Sequential + fail-fast: if the funding refresh fails, DO NOT run the journeys job
# (journeys would carry incomplete funding). Both jobs are idempotent + fail-closed
# internally (nonzero exit + error manifest, checkpoint not advanced on partial failure).
#
# Owned by quant; INSTALLED/scheduled by CoS (Rule 12). Cron plist: com.quantslab.m02-journeys-daily.
# INSTALLED + loaded 2026-07-12 (CoS), fires daily 06:45 after fills refresh (06:20).
set -euo pipefail

REPO=/Users/hermes/quants-lab
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
cd "$REPO"
set -a; [ -f "$REPO/.env" ] && source "$REPO/.env"; set +a

TS=$(date +%Y%m%dT%H%M%S)
echo "=== [$TS] M2 daily pipeline start ==="

echo "--- step 1/2: funding+ledger hot refresh (incremental) ---"
"$PY" data_pipeline/hl_s3_misc_daily_refresh.py --n-workers 3
echo "--- funding+ledger refresh OK ---"

echo "--- step 2/2: M2 daily-incremental journeys (only-new) ---"
# MANDATORY mem_safe_run backstop (decision 2026-06-04; the direct launch here OOM-panicked the box 2026-07-16,
# postmortem projects/quant/postmortems/2026-07-17-m02-oom-kernel-panic). --floor-gb 4 = 4GB job-tree RSS
# CEILING (the group is killed above it); it also kills immediately on kernel-critical pressure.
"$REPO/scripts/mem_safe_run.sh" --floor-gb 4 --label m02-daily -- \
  "$PY" data_pipeline/m02_journeys_daily.py --procs 4
echo "=== [$(date +%Y%m%dT%H%M%S)] M2 daily pipeline done OK ==="
