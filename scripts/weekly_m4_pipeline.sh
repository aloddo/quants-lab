#!/bin/bash
# WEEKLY M4 authenticity full recompute (Alberto decision 2026-07-17, TG 11429):
#   M4 (full, ~3.4hr single-proc hot_prefetch) -> M5 (picks up the new tiers via _entity_m4_hash)
# M4 authenticity is scored over a wallet's whole tracked history; it moves on the scale of that history, not
# day-to-day, so a weekly full recompute is both correct-by-construction and fresh enough. The anchored/stage-A
# DAILY-incremental was proven UNSOUND by codex FINAL (weekly-anchor drift silently restages idle wallets).
# Decision: projects/quant/decisions/2026-07-17-m4-weekly-full-recompute.
#
# Schedule (CoS, Rule 12): once a week, OVERNIGHT, BEFORE the next daily run (e.g. Sunday, ahead of Monday's
# daily_selection_pipeline.sh). MUST run at least once BEFORE the first M4-less daily run (rollout ordering:
# daily M5 crashes without m4_tiers.parquet).
#
# Owned by quant; scheduled by CoS.
set -euo pipefail
REPO=/Users/hermes/quants-lab
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
cd "$REPO"
set -a; [ -f "$REPO/.env" ] && source "$REPO/.env"; set +a
TS=$(date +%Y%m%dT%H%M%S)
ASOF=$(date +%Y%m%d)

# --- shared lock with the daily funnel: serialize so M4's tiers rewrite never overlaps a daily M5 read.
# macOS has no `flock`; use the same portable mkdir lock + dead-holder stealing as daily_selection_pipeline.sh.
# NOTE on S3 overlap (codex P1 #3): this lock does NOT cover the S3 fills/funding refresh (a separate CoS
# launchd job). The refusal-to-publish coherence guard lives INSIDE m04 (it fingerprints the hot stores before
# and after the ~3.4hr run and aborts without publishing if a source day-file changed mid-run). CoS should
# still schedule this weekly run in a window when the S3 refresh is idle so the guard rarely trips. ---
LOCKDIR="$REPO/app/data/v15/.selection_pipeline.lock.d"
_tries=0
while ! mkdir "$LOCKDIR" 2>/dev/null; do
  _pid=$(cat "$LOCKDIR/pid" 2>/dev/null || true)
  if [ -n "${_pid:-}" ]; then
    if ! kill -0 "$_pid" 2>/dev/null; then echo "WARN: stealing lock from dead pid $_pid"; rm -rf "$LOCKDIR"; continue; fi
  else
    _age=$(( $(date +%s) - $(stat -f %m "$LOCKDIR" 2>/dev/null || echo 0) ))
    if [ "$_age" -gt 120 ]; then echo "WARN: stealing ownerless lock (age ${_age}s)"; rm -rf "$LOCKDIR"; continue; fi
  fi
  # WAIT for a live daily run to finish rather than skip: a skipped weekly = no M4 refresh for a week. Bounded
  # retry (10 x 60s = 10min), then exit NONZERO so CoS/launchd logs the miss loudly (codex P2).
  _tries=$((_tries + 1))
  if [ "$_tries" -gt 10 ]; then echo "FATAL: could not acquire lock after 10min (pid ${_pid:-unknown}); weekly M4 SKIPPED." >&2; exit 1; fi
  echo "selection pipeline holds the lock (pid ${_pid:-unknown}); waiting 60s (try ${_tries}/10)..."; sleep 60
done
echo $$ > "$LOCKDIR/pid"
trap 'rm -rf "$LOCKDIR" 2>/dev/null || true' EXIT

echo "=== [$TS] weekly M4 full recompute start (as-of $ASOF) ==="
echo "--- M4 authenticity (FULL, ~3.4hr single-proc) ---"
$PY data_pipeline/m04_authenticity_daily.py --run --as-of "$ASOF"

# Re-run the DAILY M5 driver (run_daily) so it re-reads the fresh m4_tiers.parquet and PERSISTS its
# entity_m4_hash state -- this is the entry point that propagates the weekly tier changes into the pool
# (do NOT use any other M5 entry point here, or Monday's daily M5 would redo the whole propagation).
echo "--- M5 eligibility (picks up new M4 tiers via _entity_m4_hash) ---"
$PY data_pipeline/m05_eligibility_daily.py --run

echo "=== [$(date +%Y%m%dT%H%M%S)] weekly M4 done OK -> tiers at app/data/v15/m04_authenticity_daily/, pool refreshed ==="
