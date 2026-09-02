#!/bin/bash
# Daily SELECTION FUNNEL (Alberto architecture confirm 2026-07-16 TG 11418; M4 cadence TG 11429 2026-07-17):
#   M2 (journeys) -> M3 (folds/activity) -> M5 (eligibility)
# M4 (authenticity tiers) is NOT in the daily chain -- it runs WEEKLY as a FULL recompute
# (scripts/weekly_m4_pipeline.sh; decision projects/quant/decisions/2026-07-17-m4-weekly-full-recompute).
# M5 reads the LAST weekly M4 tiers and auto-recomputes any entity whose tier changed (via _entity_m4_hash).
# Each daily stage is INCREMENTAL off the M2 delta, so the funnel refreshes in minutes/day. Runs AFTER the S3
# fills+funding refresh. Fail-fast: if a stage fails, STOP (a partial funnel would mis-select). M6-M10
# (per-strategy backtest) are NOT part of this daily run -- they are ad hoc.
#
# Owned by quant; scheduled by CoS (Rule 12). Chain after com.quantslab.hl-s3-fills-daily.
set -euo pipefail
REPO=/Users/hermes/quants-lab
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
cd "$REPO"
set -a; [ -f "$REPO/.env" ] && source "$REPO/.env"; set +a
TS=$(date +%Y%m%dT%H%M%S)
ASOF=$(date +%Y%m%d)

# --- serialize against the weekly M4 run (shared lock): M4 rewrites m4_tiers.parquet while M5 reads it.
# Atomic writes make a torn read impossible; the lock also prevents wasted concurrent compute. macOS has no
# `flock`, so use a portable mkdir lock (atomic create) with dead-holder stealing (steal if the recorded PID
# is gone -> a crashed run never wedges the pipeline forever). ---
LOCKDIR="$REPO/app/data/v15/.selection_pipeline.lock.d"
while ! mkdir "$LOCKDIR" 2>/dev/null; do
  _pid=$(cat "$LOCKDIR/pid" 2>/dev/null || true)
  if [ -n "${_pid:-}" ]; then
    if ! kill -0 "$_pid" 2>/dev/null; then echo "WARN: stealing lock from dead pid $_pid"; rm -rf "$LOCKDIR"; continue; fi
  else
    # ownerless dir: holder died between mkdir and the pid write (pid is written immediately, so a missing pid
    # after the grace window means a dead acquirer). Steal it so the pipeline never wedges permanently (codex P2).
    _age=$(( $(date +%s) - $(stat -f %m "$LOCKDIR" 2>/dev/null || echo 0) ))
    if [ "$_age" -gt 120 ]; then echo "WARN: stealing ownerless lock (age ${_age}s)"; rm -rf "$LOCKDIR"; continue; fi
  fi
  echo "another selection/weekly pipeline holds the lock (pid ${_pid:-unknown}); exiting"; exit 0
done
echo $$ > "$LOCKDIR/pid"
trap 'rm -rf "$LOCKDIR" 2>/dev/null || true' EXIT

echo "=== [$TS] daily selection funnel start (as-of $ASOF) ==="

# --- STALENESS GUARD: the daily pool rests on the premise that M4 tiers are <=7 days old (weekly cadence).
# Fail LOUD if the weekly M4 artifact is missing or older than 9 days (a silently-dead weekly cron must not
# leave the pool running on ancient tiers forever). ---
M4_DIR="$REPO/app/data/v15/m04_authenticity_daily"
M4_TIERS="$M4_DIR/m4_tiers.parquet"
if [ ! -f "$M4_TIERS" ]; then
  echo "FATAL: M4 tiers missing ($M4_TIERS). Run scripts/weekly_m4_pipeline.sh first (rollout ordering)." >&2
  exit 1
fi
# Age from the M4 SOURCE WATERMARK (source_watermark_day = newest day common to all hot stores at M4 run time),
# NOT file mtime and NOT the launcher clock: a weekly recompute against a dead/lagging S3 store still bumps the
# parquet mtime and stamps today's as_of, so either would pass the guard forever (codex P1). The source
# watermark stops advancing exactly when S3 stops, which is the failure we must catch.
M4_AGE_DAYS=$($PY - "$M4_DIR" <<'PY' 2>/dev/null || echo 999
import json, sys, datetime, pathlib
d = pathlib.Path(sys.argv[1])
st = json.loads((d / "m4_run_state.json").read_text())
wm = str(st.get("source_watermark_day") or "")
if len(wm) != 8:
    raise SystemExit("no source_watermark_day")   # -> outer '|| echo 999' -> FATAL (fail closed)
wm_date = datetime.datetime.strptime(wm, "%Y%m%d").date()
print((datetime.date.today() - wm_date).days)
PY
)
if [ "$M4_AGE_DAYS" -gt 9 ]; then
  echo "FATAL: M4 SOURCE watermark is ${M4_AGE_DAYS}d old (> 9d) -> weekly M4 cron dead OR S3 fills refresh stalled. Refusing to select on stale authenticity." >&2
  exit 1
fi
echo "--- M4 source-watermark age: ${M4_AGE_DAYS}d (OK) ---"

# --- M2: stateful incremental journeys after migration; proven 1c-1f incremental before it ---
# Detection is fail-closed (codex P2): a checkpoint file that EXISTS but is malformed (bad JSON) must NOT be
# silently treated as "not seeded" -- exit codes distinguish absent(2)/malformed(3)/unseeded(1)/seeded(0).
M2_CKPT="$REPO/app/data/v15/m02_stateful_state/checkpoint.json"
M2_RC=0
$PY - "$M2_CKPT" <<'PY' || M2_RC=$?
import json, sys, pathlib
p = pathlib.Path(sys.argv[1])
if not p.exists(): sys.exit(2)          # absent -> not seeded yet (expected pre-swap)
try: cp = json.loads(p.read_text())
except Exception: sys.exit(3)           # malformed JSON -> FAIL CLOSED, do not guess
if not isinstance(cp, dict): sys.exit(3)
# SEEDED marker = the 'wallet_state' KEY is present (the stateful seed writes it; the pre-seed 1c-1f checkpoint
# does not have it). Presence, NOT non-emptiness: a validly seeded universe with zero currently-open holders
# has an EMPTY wallet_state and must still use the stateful path (codex P2). The stateful driver handles empty.
sys.exit(0 if "wallet_state" in cp else 1)
PY
if [ "$M2_RC" -eq 0 ]; then
  # MANDATORY mem_safe_run backstop (decision 2026-06-04; m02 OOM-panicked the box 2026-07-16) — even the
  # stateful path can trigger a heavy 2b late-fill replay. --floor-gb 4 = 4GB job-tree RSS ceiling + kill on
  # kernel-critical pressure.
  echo "--- M2 stateful ---"; "$REPO/scripts/mem_safe_run.sh" --floor-gb 4 --label m02-stateful -- \
    $PY data_pipeline/m02_journeys_daily.py --stateful
elif [ "$M2_RC" -eq 3 ]; then
  echo "FATAL: M2 stateful checkpoint exists but is unreadable/malformed ($M2_CKPT). Refusing to guess." >&2; exit 1
else
  # Migration must not freeze the canonical store. The previous fallback skipped
  # M2 entirely, so every pre-seed daily run advanced M3/M5 over stale journeys.
  # Keep using the already-proven 1c-1f incremental driver until the separate
  # stateful checkpoint is genuinely seeded; both drivers share the output store.
  echo "--- M2 stateful checkpoint not seeded yet (rc=$M2_RC); running canonical incremental fallback ---"
  "$REPO/scripts/mem_safe_run.sh" --floor-gb 4 --label m02-incremental -- \
    $PY data_pipeline/m02_journeys_daily.py
fi

echo "--- M3 folds (incremental) ---"; $PY data_pipeline/m03_folds_daily.py --run
echo "--- M5 eligibility (incremental; reads last weekly M4 tiers) ---"; $PY data_pipeline/m05_eligibility_daily.py --run
echo "=== [$(date +%Y%m%dT%H%M%S)] daily selection funnel done OK -> eligible pool at app/data/v15/m05_eligibility_daily/ ==="
