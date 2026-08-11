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

# SINGLE-WRITER GUARD (2026-08-11). This script had NO lock, so the 06:45 cron would happily start a
# SECOND m02_journeys_daily against the same store while a long catch-up was still running. Two writers
# share the closed/ store, the actions store and one checkpoint.json -- the loser silently corrupts the
# winner. Caught today: a 16h catch-up was due to finish at 07:38, 53 minutes AFTER the cron fires.
# Refuse rather than race. Fail-closed and idempotent: a skipped daily is recovered by the next run,
# a corrupted store is not.
if pgrep -f "m02_journeys_daily\.py" >/dev/null 2>&1; then
  echo "REFUSING to start: an m02_journeys_daily process is already running (pids: $(pgrep -f 'm02_journeys_daily\.py' | tr '\n' ' '))." >&2
  echo "Two writers on the same journeys store + checkpoint corrupt each other. Skipping this run." >&2
  exit 0
fi

echo "=== [$TS] M2 daily pipeline start ==="

echo "--- step 1/2: funding+ledger hot refresh (incremental) ---"
"$PY" data_pipeline/hl_s3_misc_daily_refresh.py --n-workers 3
echo "--- funding+ledger refresh OK ---"

echo "--- step 2/2: M2 daily-incremental journeys (only-new) ---"
# MANDATORY mem_safe_run backstop (decision 2026-06-04; the direct launch here OOM-panicked the box 2026-07-16,
# postmortem projects/quant/postmortems/2026-07-17-m02-oom-kernel-panic). --floor-gb 4 = 4GB job-tree RSS
# CEILING (the group is killed above it); it also kills immediately on kernel-critical pressure.
#
# RETRY-ON-RESOURCE (2026-07-27). This step had started 7 times and completed ZERO times: every run
# lost the coin-flip on free RAM at fire time and gave up until the next day, leaving m02_actions /
# m02_journeys FIVE DAYS stale and blocking all replay validation. The five aborts were
# 4038 / 3842 / 3969 / 2936 / 3924 MB against the 4096MB floor -- four of them within 6% of clearing.
# The floor is NOT the problem and is not being lowered: it exists because the direct launch
# OOM-panicked the box. The problem was that a transient resource shortage was treated as a terminal
# failure. Poll for a window instead.
#
# Retryable == a RESOURCE verdict from mem_safe_run only:
#   3 = refused launch (kernel pressure already critical)
#   9 = aborted mid-run (guard killed the job group)
# Any OTHER nonzero is the JOB's own exit code -- a real failure. Fail closed immediately, unchanged:
# m02_journeys_daily is fail-closed internally (checkpoint not advanced on partial failure), and
# retrying a genuine error would just burn hours re-hitting it.
RETRY_DEADLINE_S=${M02_RETRY_DEADLINE_S:-14400}     # 4h; fires 06:45 -> gives up ~10:45
RETRY_SLEEP_S=${M02_RETRY_SLEEP_S:-1200}            # 20min between attempts
_started=$(date +%s)
_attempt=0
while :; do
  _attempt=$((_attempt + 1))
  set +e
  "$REPO/scripts/mem_safe_run.sh" --floor-gb 4 --label m02-daily -- \
    "$PY" data_pipeline/m02_journeys_daily.py --procs 4
  _rc=$?
  set -e
  [ "$_rc" -eq 0 ] && break
  if [ "$_rc" -ne 3 ] && [ "$_rc" -ne 9 ]; then
    echo "M2 daily: journeys step FAILED rc=$_rc (job error, not a resource abort) -- not retrying." >&2
    exit "$_rc"
  fi
  _elapsed=$(( $(date +%s) - _started ))
  if [ "$_elapsed" -ge "$RETRY_DEADLINE_S" ]; then
    echo "M2 daily: giving up after ${_attempt} attempts / $((_elapsed / 60))min waiting for a RAM window (last rc=$_rc)." >&2
    exit "$_rc"
  fi
  echo "M2 daily: attempt ${_attempt} hit a resource abort (rc=$_rc) after $((_elapsed / 60))min; retrying in $((RETRY_SLEEP_S / 60))min."
  sleep "$RETRY_SLEEP_S"
done
echo "=== [$(date +%Y%m%dT%H%M%S)] M2 daily pipeline done OK (journeys took ${_attempt} attempt(s)) ==="
