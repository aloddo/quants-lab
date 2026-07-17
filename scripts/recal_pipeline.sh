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

# M1 is OUT OF SCOPE (Alberto 2026-07-17: "M1 must be deleted, never reference again"). The chain is
# M2 -> M3 -> M4 -> M5(copyability), none of which use M1/equity. No M1 wait, no shard stitch, no equity artifact.
log "M1 out of scope -> starting at M2 (copyability chain)"

# back up pre-fix derived outputs (DERIVED, not raw; keep for comparison)
mkdir -p $D/prefix_backup_$TS
for f in m02_actions m02_journeys m03_folds m03_wallet_activity_summary m03_wallet_fold_activity m04_authenticity m04_entities m05_eligibility m05_pool_summary m06a_shortlist m06b_pool; do
  [ -f $D/$f.parquet ] && cp $D/$f.parquet $D/prefix_backup_$TS/ 2>/dev/null || true
done
log "backed up pre-fix outputs -> $D/prefix_backup_$TS"

log "M2 journeys on $(wc -l < $WALLETS) wallets, $PROCS procs"
if [ -n "${M2_EQUITY_ENRICH:-}" ]; then
  # EQUITY-ENRICHMENT lane (deprecated TG11298, opt-in): per-wallet loader (v15_m02_journey_trace) -- reads
  # every day-file per wallet, memory-heavy. Only if you explicitly need equity-enriched journeys.
  $SAFE --label m02_ -- $PY research/v15/v15_m02_journey_trace.py --wallets-file "$WALLETS" --start 2025-12-01 --end 2026-05-23 \
    --actions-out $D/m02_actions.parquet --journeys-out $D/m02_journeys.parquet --procs $PROCS --skip-marks-cache \
    --equity-enrichment --headroom-gb "${M2_HEADROOM_GB:-1.5}" --per-worker-gb "${M2_PER_WORKER_GB:-3}" 2>&1 | tail -6
else
  # CORE journeys via the MEMORY-SAFE BATCHED runner reading fills from the WALLET-PARTITIONED SHARD
  # (build_fills_wallet_shard.py): each batch reads only its wallets' fills (partition pruning) instead of
  # re-scanning the 11GB store per batch -> fast on a RAM-tight box (25 wallets 21s->7.6s; far bigger at scale).
  # byte-identical to the day-file path (order_wallet_fills_causally). Falls back to the day-file scan if the
  # shard is absent. process_wallet_preloaded is byte-identical to process_wallet(core).
  FILLS_SHARD="$D/m2_fills_wallet_shards"
  SHARD_ARG=""; [ -f "$FILLS_SHARD/._complete" ] && SHARD_ARG="--fills-shard-dir $FILLS_SHARD"
  $SAFE --label m02b -- $PY scripts/m2_batched_run.py --wallets-file "$WALLETS" --start 2025-12-01 --end 2026-05-23 \
    --actions-out $D/m02_actions.parquet --journeys-out $D/m02_journeys.parquet $SHARD_ARG \
    --batch-size "${M2_BATCH_SIZE:-250}" --procs "$PROCS" --worker-gb "${M2_WORKER_GB:-2.5}" 2>&1 | tail -6
fi
log "M3 fold_geometry"
$SAFE --label m03_ -- $PY research/v15/v15_m03_fold_geometry.py --actions $D/m02_actions.parquet --journeys $D/m02_journeys.parquet --outdir $D 2>&1 | tail -3
log "M4 authenticity (FOLD-PURE: per-fold as-of each fold test_start -- no cross-fold leak)"
PY="$PY" SAFE="$SAFE" PROCS="$PROCS" scripts/build_m4_perfold.sh "$D/m03_folds.parquet" "$WALLETS" "$D" 2>&1 | tail -6
log "M5 eligibility (corrected equity)"
# M5 in COPYABILITY mode: the M1/equity lane is OUT OF SCOPE (Alberto 2026-07-17) -> no --equity/--m01-audit,
# no M1 dependency. Override to the equity lane only via M5_MODE=equity (requires a current M1 artifact).
$SAFE --label m05_ -- $PY research/v15/v15_m05_eligibility.py --mode "${M5_MODE:-copyability}" --folds $D/m03_folds.parquet --journeys $D/m02_journeys.parquet --m04-dir $D --m03-activity $D/m03_wallet_activity_summary.parquet --outdir $D 2>&1 | tail -3
# M2-M5 = the one-shot FOUNDATION (Alberto 2026-07-17). Stop here for a base build; M6+ (the backtest) runs
# separately. Set M2M5_ONLY= to stop; unset to continue into the M6-M8 backtest chain.
if [ -n "${M2M5_ONLY:-}" ]; then log "M2-M5 FOUNDATION DONE (M2M5_ONLY set) -> base ready; M6+ backtest runs separately"; exit 0; fi
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
