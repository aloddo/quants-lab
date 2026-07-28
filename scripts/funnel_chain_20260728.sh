#!/bin/bash
# V15 funnel chain on the 13-fold calendar: m05 -> m06a -> m07 pretest -> m07 test -> m06b (BH-FDR).
# Prereq: m03 + m04 f1..f13 present in $DIR (scripts/m04_funnel_20260728.sh).
#
# Recorded run decisions (do not silently change):
#  - m05 COPYABILITY lane, non-strict. --mode equity needs an m01 artifact with
#    gross_position_notional_usd; m01_universe_20k_series.parquet is from May 30 and lacks it.
#    No accessible-coins JSON exists, so --strict is impossible. Non-strict fails OPEN on
#    accessibility = PERMISSIVE. A NO-GO under permissive settings is the STRONGER conclusion;
#    a GO would require the strict rerun (m01 rebuild + accessible-coins) before any capital.
#  - m06a manifest v1-profile-20260728: shortlist_n_per_fold=2000, i.e. ABOVE the 1,630-wallet
#    universe, so the engine runs on EVERY eligible entity and nothing is truncated. The default
#    N=1000 is a multiple-testing control for picking a pool; the deliverable here is Alberto's
#    WALLET PROFILE (2026-07-28: "I just want the right wallet profile to copy live"), and a
#    top-1000 cut would truncate the attribute range and could hide the very gradient being
#    measured. Raising N REMOVES selection rather than adding any -> conservative for profiling.
#    Everything else (recency_gate, score_basis, horizon) is left at the pre-registered v1 values.
#  - m07 --copy-latency-ms 4000 = MEASURED live V17 latency (matches m05 P95_COPY_LATENCY_S=4.0).
#    NOT the 2000ms default. Consistency between the copyability gate and the sim is the point.
#  - FINAL stamp requires BOTH version strings; verified present:
#    fees "2026-06-01-userFees-master-0x11ca" (hl_fee_schedule.json) + slippage "v11-fills-v2".
set -u
cd /Users/hermes/quants-lab
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
DIR=app/data/v15/funnel_20260728
LOG=/tmp/ql_funnel_chain_20260728.log
FEE_VER="2026-06-01-userFees-master-0x11ca"
SLIP_VER="v11-fills-v2"

say() { echo "[$(date -u +%FT%TZ)] $*" >> "$LOG"; }
die() { say "FAILED at $1 (rc=$2) - chain stops, fail closed"; exit 1; }

say "=== funnel chain start ==="

# Fail closed if the m04 chain is incomplete: a missing fold would silently shrink the pool.
missing=""
for f in $(seq 1 13); do
  [ -s "$DIR/m04_authenticity_f${f}.parquet" ] && [ -s "$DIR/m04_entities_f${f}.parquet" ] || missing="$missing f$f"
done
if [ -n "$missing" ]; then say "ABORT: m04 incomplete ->$missing"; exit 1; fi
say "m04 complete: 13/13 folds"

# --- M5 eligibility (copyability lane) ---
if [ ! -s "$DIR/m05_eligibility.parquet" ]; then
  say "M5 start"
  $PY research/v15/v15_m05_eligibility.py \
    --mode copyability \
    --folds "$DIR/m03_folds.parquet" \
    --journeys "$DIR/m02_journeys.parquet" \
    --m04-dir "$DIR" \
    --m03-activity "$DIR/m03_wallet_activity_summary.parquet" \
    --outdir "$DIR" >> "$LOG" 2>&1 || die M5 $?
  say "M5 done"
else say "M5 SKIP (exists)"; fi

# --- M6a shortlist (N pre-registered) ---
if [ ! -s "$DIR/m06a_shortlist.parquet" ]; then
  say "M6a start"
  $PY research/v15/v15_m06a_shortlist.py \
    --eligibility "$DIR/m05_eligibility.parquet" \
    --pool-summary "$DIR/m05_pool_summary.parquet" \
    --folds "$DIR/m03_folds.parquet" \
    --m04-dir "$DIR" \
    --actions "$DIR/m02_actions.parquet" \
    --manifest "$DIR/m06a_manifest_profile.json" \
    --outdir "$DIR" >> "$LOG" 2>&1 || die M6a $?
  say "M6a done"
else say "M6a SKIP (exists)"; fi

# --- M7 engine, both windows ---
for W in pretest test; do
  if [ ! -s "$DIR/m07_${W}/m07_summary.parquet" ] && [ -z "$(ls $DIR/m07_${W}/m07_summary*.parquet 2>/dev/null)" ]; then
    say "M7 $W start"
    $PY research/v15/v15_m07_engine.py \
      --actions "$DIR/m02_actions.parquet" \
      --shortlist "$DIR/m06a_shortlist.parquet" \
      --folds "$DIR/m03_folds.parquet" \
      --out "$DIR/m07_${W}" \
      --window "$W" \
      --slip-calib "$DIR/slippage_calib_13fold.json" \
      --copy-latency-ms 4000 >> "$LOG" 2>&1 || die "M7 $W" $?
    say "M7 $W done"
  else say "M7 $W SKIP (exists)"; fi
done

# --- WALLET PROFILE (Alberto's actual deliverable, 2026-07-28) ---
# Which observable wallet ATTRIBUTES predict a positive COPIED after-cost return.
# Attributes from each fold's PRETEST positions, outcome from the SAME fold's TEST positions.
# RUNS BEFORE M6b ON PURPOSE: walk_forward_confirm hard-requires m07_positions and has never executed
# against real data (no m07_positions exists in ANY prior run dir), so it is the most likely step to
# crash. The profile must not be hostage to it.
say "PROFILE start"
$PY research/v15/copy_wallet_profile.py --dir "$DIR" >> "$LOG" 2>&1 || say "PROFILE rc=$? (non-fatal)"
say "PROFILE done"

# --- M6b ranking + pooled BH-FDR OOS confirmation ---
# PRE-REGISTERED OOS GATE (written BEFORE any result exists, 2026-07-28 08:55 CEST).
# PRIMARY = the CODEX STANDARD: oos_min_folds=4, oos_min_journeys_pooled=50, majority of OOS folds
# net-positive, BH-FDR q=0.10, H0 margin 0 (break-even). The M6bManifest defaults (2 folds / 30
# journeys) are the temporary FLOOR adopted when only 3 OOS folds existed; the module docstring says
# to raise to the standard "once the FULL 12-fold action bootstrap exists". The 13-fold calendar now
# exists, so the standard is the primary gate. The floor is run SECOND, as a sensitivity only, and
# its result does NOT override the primary. Declaring both up front so neither is a post-hoc pick.
say "M6b start (PRIMARY = codex standard 4 folds / 50 journeys / q=0.10)"
$PY research/v15/v15_m06b_ranking.py \
  --m07-dir "$DIR/m07_pretest" \
  --m07-test-dir "$DIR/m07_test" \
  --data-dir "$DIR" \
  --m04-dir "$DIR" \
  --out "$DIR" \
  --oos-min-folds 4 \
  --oos-min-journeys-pooled 50 \
  --oos-min-frac-folds-pos 0.5 \
  --fdr-q 0.10 \
  --oos-margin 0.0 \
  --fee-schedule-version "$FEE_VER" \
  --slippage-calibration-version "$SLIP_VER" >> "$LOG" 2>&1 || say "M6b PRIMARY rc=$? (non-fatal; profile already written)"
say "M6b done (primary)"

# SENSITIVITY ONLY (floor gate). Separate out dir so it can never overwrite the primary artefacts.
mkdir -p "$DIR/m06b_floor_sensitivity"
say "M6b sensitivity start (floor 2 folds / 30 journeys)"
$PY research/v15/v15_m06b_ranking.py \
  --m07-dir "$DIR/m07_pretest" \
  --m07-test-dir "$DIR/m07_test" \
  --data-dir "$DIR" \
  --m04-dir "$DIR" \
  --out "$DIR/m06b_floor_sensitivity" \
  --oos-min-folds 2 \
  --oos-min-journeys-pooled 30 \
  --fee-schedule-version "$FEE_VER" \
  --slippage-calibration-version "$SLIP_VER" >> "$LOG" 2>&1 || say "M6b sensitivity rc=$? (non-fatal)"
say "M6b sensitivity done"
say "=== funnel chain COMPLETE ==="
