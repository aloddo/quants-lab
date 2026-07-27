#!/bin/bash
# Step an M2 backlog forward ONE DAY AT A TIME instead of jumping it in a single run.
#
# WHY (2026-07-27). After 5 days of failed daily runs, the catch-up had a 5-DAY window and 12,655
# affected wallets, and its peak RSS (~1.8-2.0GB) meant it needed roughly floor(4096) + peak = ~5.9GB
# system-available AT LAUNCH to survive `mem_safe_run`. This box sits at 4.3-5.5GB, so it aborted
# every time -- including with --procs 1 --parent-gb 0.25, because the window, not the parallelism, is
# what drives the footprint.
#
# A ONE-DAY step hits the chunk `hi` clamp (see _bounded_chunk: at a 1-day window even
# --parent-gb 0.25 yields the same 2000-wallet chunk), so each step behaves like a NORMAL daily
# incremental -- the workload the memory profile was designed around -- and fits in the RAM we have.
#
# Same retry-on-resource-abort contract as scripts/m02_daily_pipeline.sh: rc 3/9 = RESOURCE, retry;
# anything else = the job's own failure, stop immediately (m02 is fail-closed internally and does not
# advance its checkpoint on partial failure, so a stopped backlog is safe to resume).
set -uo pipefail

REPO=/Users/hermes/quants-lab
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
cd "$REPO"
set -a; [ -f "$REPO/.env" ] && source "$REPO/.env"; set +a

DAYS=${*:-}
[ -n "$DAYS" ] || { echo "usage: m02_catchup_stepped.sh YYYYMMDD [YYYYMMDD ...]" >&2; exit 2; }

RETRY_SLEEP_S=${M02_RETRY_SLEEP_S:-360}
RETRY_MAX=${M02_RETRY_MAX:-8}
# MEASURED 2026-07-27: M2 pulls ~4GB of SYSTEM-AVAILABLE (more than its RSS -- mmap/page cache, which
# is exactly what the available-floor check exists to catch and what a pure RSS gate would miss). At
# --floor-gb 4 it therefore needs ~8GB free AT LAUNCH; this box has ~5GB, so it aborted every time
# even at --procs 1 --parent-gb 0.25. Ladder actually observed: floor 4 -> abort at 3894MB;
# floor 3 -> abort at 2834MB; floor 2 -> RUNS.
# Lowering the floor is defensible ONLY because f126e42 fixed the guard's silent kill-miss and the
# INDEPENDENT kernel-pressure trigger (pl>=4, which fires immediately before jetsam) is untouched. A
# 2GB floor with verified kills is stronger protection than the 4GB floor was at 09:00 today, when
# the kills were silently failing. Do NOT lower this further without re-verifying the guard.
# NOTE --parent-gb is a NO-OP here: at a 1-day window _bounded_chunk hits its `hi` clamp regardless.
FLOOR_GB=${M02_FLOOR_GB:-2}

for DAY in $DAYS; do
  echo "=== [$(date +%Y%m%dT%H%M%S)] STEP -> $DAY ==="
  _try=0
  while :; do
    _try=$((_try + 1))
    "$REPO/scripts/mem_safe_run.sh" --floor-gb "$FLOOR_GB" --label "m02-step-$DAY" -- \
      "$PY" data_pipeline/m02_journeys_daily.py --procs 2 --target-day "$DAY"
    _rc=$?
    [ "$_rc" -eq 0 ] && { echo "--- $DAY OK (attempt $_try) ---"; break; }
    if [ "$_rc" -ne 3 ] && [ "$_rc" -ne 9 ]; then
      echo "STEP $DAY FAILED rc=$_rc (job error, not a resource abort) -- stopping backlog." >&2
      exit "$_rc"
    fi
    if [ "$_try" -ge "$RETRY_MAX" ]; then
      echo "STEP $DAY: gave up after $_try resource aborts." >&2
      exit "$_rc"
    fi
    echo "STEP $DAY: resource abort (rc=$_rc), attempt $_try/$RETRY_MAX; retrying in $((RETRY_SLEEP_S / 60))min."
    sleep "$RETRY_SLEEP_S"
  done
done
echo "=== [$(date +%Y%m%dT%H%M%S)] BACKLOG COMPLETE ==="
