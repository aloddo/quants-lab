#!/bin/bash
# V15 M2->M8 recalibration on the burst-aware-corrected M1 equity (Alberto 2026-06-04:
# "your job is recalibrating the whole M2-10"). Uses the BUILT module CLIs only -- no custom
# scripts. Waits for the M1 daily re-run, consolidates the corrected series+audit, backs up the
# pre-fix derived outputs, then chains M2->M8. M9/M10 (v15_forward_select.py) run after, once
# m07_{pretest,test}_final exist. set -e: stop on first failure (corrected M2 is reusable).
set -e
set -o pipefail   # a mem_safe_run backstop-kill (exit 9) through `| tail` must halt the chain,
                  # not silently feed partial output into the next module.
cd /Users/hermes/quants-lab
set -a && source .env 2>/dev/null && set +a
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
D=app/data/v15
WALLETS=${1:-$D/m01_nonerroring_wallets.txt}
# procs lowered 8->3 after the 2026-06-04 OOM reboot (8 workers x ~2GB overran 16GB box).
PROCS=${2:-3}
# MANDATORY OOM backstop (decisions/2026-06-04-mem-safe-run-backstop): every heavy module
# runs through mem_safe_run, which kills the job's process group if its tree RSS exceeds the
# ceiling or the kernel hits critical memory pressure. SAFE = wrapper prefix; CEIL = GB ceiling.
CEIL=${3:-6}
SAFE="scripts/mem_safe_run.sh --floor-gb $CEIL"
TS=$(date +%Y%m%d_%H%M)
log(){ echo "[recal $(date +%H:%M:%S)] $*"; }

# 0) wait for the M1 daily re-run to finish
while [ "$(ps aux | grep v15_m01_equity_reconstruct | grep -v grep | wc -l | tr -d ' ')" != "0" ]; do sleep 30; done
log "M1 daily re-run done; consolidating corrected series + audit"
$PY - <<'PY'
import glob, pandas as pd, numpy as np
ser=sorted(glob.glob('app/data/v15/m01_rerun/shard_*.parquet'))
ser=[p for p in ser if not p.endswith('.audit.parquet')]
df=pd.concat([pd.read_parquet(p) for p in ser], ignore_index=True)
df.to_parquet('app/data/v15/m01_universe_corrected_series.parquet', index=False, compression='snappy')
print('corrected series rows:', len(df), 'wallets:', df['wallet'].nunique())
au=sorted(glob.glob('app/data/v15/m01_rerun/shard_*.audit.parquet'))
a=pd.concat([pd.read_parquet(p) for p in au], ignore_index=True)
a.to_parquet('app/data/v15/m01_rerun_universe.audit.parquet', index=False, compression='snappy')
# Despite the legacy ``_pct`` suffix, these fields are fractional returns:
# 0.001 = 0.1%, 0.10 = 10%. Keep thresholds in fractional units and label
# rendered percentages after multiplying by 100.
md=pd.to_numeric(a['median_inter_anchor_drift_pct'],errors='coerce').dropna()
mx=pd.to_numeric(a['max_inter_anchor_drift_pct'],errors='coerce').dropna()
print('CORRECTED universe MEDIAN drift (gate >10%):', '<0.1%%:%d 0.1-0.5%%:%d 0.5-1%%:%d 1-5%%:%d >5%%:%d'%(
 (md<0.001).sum(),((md>=0.001)&(md<0.005)).sum(),((md>=0.005)&(md<0.01)).sum(),((md>=0.01)&(md<0.05)).sum(),(md>=0.05).sum()))
print('  MAX-drift gate >50%% fails: %d/%d (%.1f%%)  | quarantined %d/%d (%.1f%%)'%(
 int((mx>0.50).sum()),len(mx),100*(mx>0.50).mean(),
 int(a['quarantined'].sum()),len(a),100*a['quarantined'].mean()))
PY
EQUITY=$D/m01_universe_corrected_series.parquet

# back up pre-fix derived outputs (DERIVED, not raw; keep for comparison)
mkdir -p $D/prefix_backup_$TS
for f in m02_actions m02_journeys m03_folds m03_wallet_activity_summary m03_wallet_fold_activity m04_authenticity m04_entities m05_eligibility m05_pool_summary m06a_shortlist m06b_pool; do
  [ -f $D/$f.parquet ] && cp $D/$f.parquet $D/prefix_backup_$TS/ 2>/dev/null || true
done
log "backed up pre-fix outputs -> $D/prefix_backup_$TS"

log "M2 journey_trace (corrected M1 seed) on $(wc -l < $WALLETS) wallets, $PROCS procs"
$SAFE --label m02_ -- $PY research/v15/v15_m02_journey_trace.py --wallets-file "$WALLETS" --start 2025-12-01 --end 2026-05-23 \
  --actions-out $D/m02_actions.parquet --journeys-out $D/m02_journeys.parquet --procs $PROCS --skip-marks-cache \
  ${M2_EQUITY_ENRICH:+--equity-enrichment} --headroom-gb "${M2_HEADROOM_GB:-1.5}" \
  --per-worker-gb "${M2_PER_WORKER_GB:-1.5}" 2>&1 | tail -6
log "M3 fold_geometry"
$SAFE --label m03_ -- $PY research/v15/v15_m03_fold_geometry.py --actions $D/m02_actions.parquet --journeys $D/m02_journeys.parquet --outdir $D 2>&1 | tail -3
log "M4 authenticity (FOLD-PURE: per-fold as-of each fold test_start -- no cross-fold leak)"
PY="$PY" SAFE="$SAFE" PROCS="$PROCS" scripts/build_m4_perfold.sh "$D/m03_folds.parquet" "$WALLETS" "$D" 2>&1 | tail -6
log "M5 eligibility (corrected equity)"
$SAFE --label m05_ -- $PY research/v15/v15_m05_eligibility.py --folds $D/m03_folds.parquet --journeys $D/m02_journeys.parquet --equity $EQUITY --m01-audit $D/m01_rerun_universe.audit.parquet --m04-dir $D --m03-activity $D/m03_wallet_activity_summary.parquet --outdir $D 2>&1 | tail -3
log "M6a shortlist"
$SAFE --label m06a -- $PY research/v15/v15_m06a_shortlist.py --eligibility $D/m05_eligibility.parquet --pool-summary $D/m05_pool_summary.parquet --folds $D/m03_folds.parquet --m04-dir $D --actions $D/m02_actions.parquet --outdir $D 2>&1 | tail -3
log "M7 engine PRETEST"
$SAFE --label m07_ -- $PY research/v15/v15_m07_engine.py --actions $D/m02_actions.parquet --shortlist $D/m06a_shortlist.parquet --folds $D/m03_folds.parquet --out $D/m07_pretest_final --window pretest --band base --start-equity 10000 --require-cache 2>&1 | tail -3
log "M7 engine TEST"
$SAFE --label m07_ -- $PY research/v15/v15_m07_engine.py --actions $D/m02_actions.parquet --shortlist $D/m06a_shortlist.parquet --folds $D/m03_folds.parquet --out $D/m07_test_final --window test --band base --start-equity 10000 --require-cache 2>&1 | tail -3
log "M6b ranking"
$SAFE --label m06b -- $PY research/v15/v15_m06b_ranking.py --m07-dir $D/m07_pretest_final --m04-dir $D --out $D --fee-schedule-version hl_fee_schedule_2026-06-01 2>&1 | tail -3
log "M8 survival"
$SAFE --label m08_ -- $PY research/v15/v15_m08_survival.py --m07-dir $D/m07_pretest_final --out $D --m04-dir $D --slip-calib $D/slippage_calib_v11.json 2>&1 | tail -3
log "M2->M8 CHAIN DONE. Next: M9/M10 via v15_forward_select.py"
echo "RECAL_CHAIN_DONE"
