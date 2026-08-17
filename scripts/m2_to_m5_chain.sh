#!/bin/bash
# One-shot chain behind a long M2 catch-up: wait for it, verify it, then M3 and the weekly M4/M5.
#
# 2026-08-12. Every failure in this pipeline over the last two days was in a SEAM, not a stage:
# a stale shard nobody checked, a second writer with no lock, a 3.4h M4 that discards itself when a
# source file moves under it, and an "exit 0" that produced no output file. The stages are fine.
# This script owns ONLY the seams and delegates every stage to the existing scripts:
#   M3      -> data_pipeline/m03_folds_daily.py --run       (as daily_selection_pipeline.sh calls it)
#   M4 + M5 -> scripts/weekly_m4_pipeline.sh                (already holds the selection lock + trap)
# It deliberately does NOT re-implement either, and does NOT call daily_selection_pipeline.sh, which
# would re-run M2 and FATAL on its M4 age gate before tiers exist.
#
# Not a cron. Run once, by hand, behind a catch-up.
set -uo pipefail

REPO=/Users/hermes/quants-lab
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
LOG=/tmp/ql-chain-m2m5.log
M2LOG=${M2LOG:-/tmp/ql-m02-final2.log}
TARGET_DAY=${TARGET_DAY:-20260810}
FP_SNAP=${FP_SNAP:-/tmp/qe/hot_fp_preRefresh_20260811.json}
TIERS="$REPO/app/data/v15/m04_authenticity_daily/m4_tiers.parquet"
cd "$REPO"

say() { echo "[$(date '+%F %T')] $*" | tee -a "$LOG"; }

# ------------------------------------------------------------------ 1. wait for the running M2
say "WAIT: for the M2 catch-up to finish"
while pgrep -f "m02_journeys_daily\.py --lookback-days 0" >/dev/null 2>&1; do sleep 120; done

if ! grep -q "wall:" "$M2LOG" 2>/dev/null; then
  say "ABORT: M2 process gone with no completion marker. Last line:"
  grep -vE "stale part|removed" "$M2LOG" | tail -2 | tee -a "$LOG"
  exit 1
fi
say "M2 finished: $(grep 'wall:' "$M2LOG" | tail -1)"

# ------------------------------------------------------------------ 2. verify, never assume
WM=$($PY -c "import json;print(json.load(open('app/data/v15/m02_daily_state/checkpoint.json'))['watermark_day'])")
if [ "$WM" != "$TARGET_DAY" ]; then
  say "ABORT: watermark is $WM, expected $TARGET_DAY. M2 did not land cleanly; refusing to build on it."
  exit 1
fi
say "VERIFIED: watermark advanced to $WM"

# Did a source day-file move under the run? M2 traces from a STATIC shard but writes a manifest
# describing HOT files, so a day rewritten mid-run is recorded as current while its new content was
# never read -- silent staleness, the exact class that capped everything at July 26. Name it loudly.
$PY - "$FP_SNAP" <<'PY' 2>&1 | tee -a "$LOG"
import sys, json, pathlib
sys.path.insert(0,"data_pipeline"); sys.path.insert(0,"research/v15")
from m02_journeys_daily import day_fingerprint, hot_available_days, fingerprints_differ
p = pathlib.Path(sys.argv[1])
if not p.exists():
    print("WARN: no pre-refresh fingerprint snapshot; cannot check for mid-run source drift"); raise SystemExit(0)
snap = json.loads(p.read_text()); hot = set(hot_available_days())
# codex P2 2026-08-17: this compared fingerprints with != . A legacy 3-part snapshot against a new
# v2 digest differs textually while denoting IDENTICAL content -> it would have cried source drift
# on every run after the migration. Route through the same comparator the pipeline uses.
moved = sorted(d for d in snap if d in hot and fingerprints_differ(snap[d], day_fingerprint(d)))
new   = sorted(d for d in hot if d not in snap)
print(f"SOURCE DRIFT during the run: {len(moved)} changed {moved or '-> NONE'} | {len(new)} new {new or '-> NONE'}")
if moved:
    print("  -> traced from the pre-refresh shard. Re-shard + reprocess these before trusting")
    print("     journeys on those days. Not fatal for M3/M4 (they read journeys), but do not forget it.")
PY

# ------------------------------------------------------------------ 3. M3
say "M3: folds (incremental)"
if ! $PY data_pipeline/m03_folds_daily.py --run >>"$LOG" 2>&1; then
  say "ABORT: M3 failed"; exit 1
fi
say "M3 OK"

# ------------------------------------------------------------------ 4. M4 + M5, timed around the refresh
# m04 fingerprints its hot inputs and REFUSES TO PUBLISH if any change mid-run. The S3 fills refresh
# fires 06:20 daily and M4 runs ~3.4h, so starting inside [03:00, 06:30) means the refresh lands
# mid-run and 3.4 hours are binned. Wait rather than gamble.
while :; do
  NOW=$((10#$(date +%H)*60 + 10#$(date +%M)))
  if [ "$NOW" -ge $((3*60)) ] && [ "$NOW" -lt $((6*60+30)) ]; then
    say "HOLD: starting M4 now would span the 06:20 refresh (coherence guard would bin the run). Waiting."
    sleep 600
  else
    break
  fi
done

# weekly_m4_pipeline.sh runs M4 then M5 under the shared selection lock. It has never been wrapped in
# mem_safe_run while every other heavy stage is; its first act is a global ledger prefetch over ~10k
# wallets. Wrap it here.
say "M4+M5: scripts/weekly_m4_pipeline.sh (~3.4h), wrapped in mem_safe_run"
if ! scripts/mem_safe_run.sh --floor-gb 1 --label m4m5-chain -- bash scripts/weekly_m4_pipeline.sh >>"$LOG" 2>&1; then
  say "ABORT: weekly M4/M5 failed. Check for the coherence-guard refusal or a memory kill:"
  tail -5 "$LOG"
  exit 1
fi

# An exit code is not evidence. m4_tiers.parquet has NEVER been produced; assert the artifact.
if [ ! -f "$TIERS" ]; then
  say "ABORT: M4 exited 0 but m4_tiers.parquet does NOT exist. Refusing to claim success."
  exit 1
fi
say "M4 OK -> m4_tiers.parquet EXISTS ($(du -h "$TIERS" | cut -f1)) -- first time ever"
say "CHAIN COMPLETE: m2-m5 current through $TARGET_DAY"
