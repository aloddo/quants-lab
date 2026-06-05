#!/bin/bash
# Resume the 2026-06-04 recal from M3 (M2 already completed: m02_actions.parquet 4.65GB +
# m02_journeys.parquet stitched). M2 took 58 min; do NOT redo it. The backstop killed M3 at
# 8GB>6GB ceiling -- correct (box stayed safe, pressure 1), but M3/M6a/M7/M8 are SINGLE-process
# full-table readers (M3 does actions.copy() on 4.65GB -> ~9GB), not fan-outs, so they need a
# higher per-module ceiling than the procs=3 fan-outs. CEIL=10 is safe on 16GB at pressure 1
# (single process; the standing pipeline+agents fit in the remaining ~6GB, and the critical-
# pressure secondary trigger still guards). M4 is procs=3 (~4GB) so 10 is generous-but-safe.
set -e
set -o pipefail
cd /Users/hermes/quants-lab
set -a && source .env 2>/dev/null && set +a
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
D=app/data/v15
WALLETS=${1:-$D/m01_nonerroring_wallets.txt}
PROCS=${2:-3}
CEIL=${3:-10}
SAFE="scripts/mem_safe_run.sh --floor-gb $CEIL"
EQUITY=$D/m01_universe_corrected_series.parquet
log(){ echo "[recal-resume $(date +%H:%M:%S)] $*"; }

# guard: M2 outputs must exist (don't silently run M3 on stale data)
for f in $D/m02_actions.parquet $D/m02_journeys.parquet $EQUITY; do
  [ -f "$f" ] || { echo "MISSING required input $f -- abort"; exit 2; }
done

log "M3 fold_geometry (ceil ${CEIL}GB)"
$SAFE --label m03_ -- $PY research/v15/v15_m03_fold_geometry.py --actions $D/m02_actions.parquet --journeys $D/m02_journeys.parquet --outdir $D 2>&1 | tail -3
log "M4 authenticity (FOLD-PURE: per-fold as-of each fold test_start -- no cross-fold leak)"
PY="$PY" SAFE="$SAFE" PROCS="$PROCS" scripts/build_m4_perfold.sh "$D/m03_folds.parquet" "$WALLETS" "$D" 2>&1 | tail -6
log "M5 eligibility (corrected equity)"
$SAFE --label m05_ -- $PY research/v15/v15_m05_eligibility.py --folds $D/m03_folds.parquet --journeys $D/m02_journeys.parquet --equity $EQUITY --m04-dir $D --m03-activity $D/m03_wallet_activity_summary.parquet --outdir $D 2>&1 | tail -3
log "M6a shortlist"
$SAFE --label m06a -- $PY research/v15/v15_m06a_shortlist.py --eligibility $D/m05_eligibility.parquet --pool-summary $D/m05_pool_summary.parquet --folds $D/m03_folds.parquet --m04-dir $D --actions $D/m02_actions.parquet --outdir $D 2>&1 | tail -3
log "M7 engine PRETEST"
$SAFE --label m07_ -- $PY research/v15/v15_m07_engine.py --actions $D/m02_actions.parquet --shortlist $D/m06a_shortlist.parquet --folds $D/m03_folds.parquet --out $D/m07_pretest_final --window pretest --band base --start-equity 10000 --require-cache 2>&1 | tail -3
log "M7 engine TEST"
$SAFE --label m07_ -- $PY research/v15/v15_m07_engine.py --actions $D/m02_actions.parquet --shortlist $D/m06a_shortlist.parquet --folds $D/m03_folds.parquet --out $D/m07_test_final --window test --band base --start-equity 10000 --require-cache 2>&1 | tail -3
log "M6b ranking"
$SAFE --label m06b -- $PY research/v15/v15_m06b_ranking.py --m07-dir $D/m07_pretest_final --m04-dir $D --out $D --fee-schedule-version hl_fee_schedule_2026-06-01 2>&1 | tail -3
log "M8 survival"
$SAFE --label m08_ -- $PY research/v15/v15_m08_survival.py --m07-dir $D/m07_pretest_final --out $D --slip-calib $D/slippage_calib_v11.json 2>&1 | tail -3
log "M3->M8 CHAIN DONE. Next: M9/M10 via v15_forward_select.py"
echo "RECAL_CHAIN_DONE"
