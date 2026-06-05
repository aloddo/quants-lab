#!/bin/bash
# Resume from M6a on the NEW M5 eligibility (Alberto copy-spec gates: days-green>=80%, lev<=10x).
# KEY DIFFERENCE vs the prior chain: M7 now gets --slip-calib slippage_calib_v11.json so the sim runs
# COST-CALIBRATED (V11 real-fill, liquidity-approximated, per-fold as-of). This clears m06b's
# investable=False / no_slippage_calibration_version, and makes the profitability gate (net of per-fill
# fees + calibrated slippage) real. M2-M5 already done; do NOT re-run them.
set -e
set -o pipefail
cd /Users/hermes/quants-lab
set -a && source .env 2>/dev/null && set +a
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
D=app/data/v15
CEIL=${1:-10}
SAFE="scripts/mem_safe_run.sh --floor-gb $CEIL"
SLIP=$D/slippage_calib_v11.json
log(){ echo "[recal-m6 $(date +%H:%M:%S)] $*"; }

for f in $D/m05_eligibility.parquet $D/m05_pool_summary.parquet $D/m03_folds.parquet $D/m04_entities_f1.parquet $D/m02_actions.parquet $SLIP; do
  [ -f "$f" ] || { echo "MISSING $f -- abort (need fold-pure M4: run scripts/build_m4_perfold.sh)"; exit 2; }
done

log "M6a shortlist (on new M5 eligibility)"
$SAFE --label m06a -- $PY research/v15/v15_m06a_shortlist.py --eligibility $D/m05_eligibility.parquet --pool-summary $D/m05_pool_summary.parquet --folds $D/m03_folds.parquet --m04-dir $D --actions $D/m02_actions.parquet --outdir $D 2>&1 | tail -3
log "M7 engine PRETEST (slippage CALIBRATED)"
$SAFE --label m07_ -- $PY research/v15/v15_m07_engine.py --actions $D/m02_actions.parquet --shortlist $D/m06a_shortlist.parquet --folds $D/m03_folds.parquet --out $D/m07_pretest_final --window pretest --band base --start-equity 10000 --require-cache --slip-calib $SLIP 2>&1 | tail -3
log "M7 engine TEST (slippage CALIBRATED)"
$SAFE --label m07_ -- $PY research/v15/v15_m07_engine.py --actions $D/m02_actions.parquet --shortlist $D/m06a_shortlist.parquet --folds $D/m03_folds.parquet --out $D/m07_test_final --window test --band base --start-equity 10000 --require-cache --slip-calib $SLIP 2>&1 | tail -3
log "M6b ranking (now cost-calibrated)"
$SAFE --label m06b -- $PY research/v15/v15_m06b_ranking.py --m07-dir $D/m07_pretest_final --m04-dir $D --out $D --fee-schedule-version hl_fee_schedule_2026-06-01 2>&1 | tail -3
log "M8 survival"
$SAFE --label m08_ -- $PY research/v15/v15_m08_survival.py --m07-dir $D/m07_pretest_final --out $D --slip-calib $SLIP 2>&1 | tail -3
log "M6a->M8 (calibrated) DONE"
echo "RECAL_M6_DONE"
