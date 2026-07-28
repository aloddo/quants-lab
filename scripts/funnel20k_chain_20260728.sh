#!/bin/bash
# V15 funnel on the REAL 20,189-wallet universe, folds 1-8: m06a -> m07 pretest+test -> PROFILE.
#
# Alberto 2026-07-28: "I don't understand why pre filter and re run the fucking m2-m5 / I thought
# those only need to run once on the 20k universe." He was right. The full-universe M02 store already
# existed (prefix_backup_20260717_1652: 125,060,683 rows, 20,189 wallets, Dec-01 -> Jul-13) and the
# 20k M4 fold files f1..f8 already existed and are provenance-verified (as_of_ms matches each fold's
# test_start exactly, one value per file). So: no M02 rerun, no pre-filter, no M4 rebuild.
#
# FOLDS 1-8 ONLY: M4 exists at 20k scale for f1..f8. f9-f12 need ~2.4h/fold at 20k = an overnight job.
# The M02 store also ends Jul-13, which caps the calendar at 12 folds anyway.
#
# M6B IS DELIBERATELY NOT RUN. entity_id is a POSITIONAL index, not an identity: 16,457/17,082 (96.3%)
# of ids map to >1 wallet across folds. m06b's walk_forward_confirm pools held-out journeys ACROSS
# folds by entity_id, so it would confirm chimeras of up to 8 different wallets. See
# findings/quant/2026-07-28-entity-id-positional-index-collides-across-folds. Fixing it means keying
# cross-fold ops on primary_wallet, which needs codex and is not a mid-run change.
# The PROFILE is unaffected: it joins on (entity_id, fold_id), which IS consistent within a fold, and
# both M7 windows are driven by the SAME m06a shortlist.
set -u
cd /Users/hermes/quants-lab
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
D=app/data/v15/funnel20k_20260728
LOG=/tmp/ql_funnel20k_20260728.log
say() { echo "[$(date -u +%FT%TZ)] $*" >> "$LOG"; }

say "=== funnel20k chain start ==="

[ -s "$D/m05_eligibility.parquet" ] || { say "ABORT: m05 missing"; exit 1; }

# --- M6a shortlist. N=2000/fold (vs the pre-registered 1000) to widen the ATTRIBUTE RANGE the profile
#     is measured over. This is not outcome-selection: m06a ranks on PRETEST score only, so a wider N
#     strictly REDUCES selection. Engine budget: 2000 x 8 folds = 16,000 seats/window. ---
if [ ! -s "$D/m06a_shortlist.parquet" ]; then
  say "M6a start"
  $PY research/v15/v15_m06a_shortlist.py \
    --eligibility "$D/m05_eligibility.parquet" \
    --pool-summary "$D/m05_pool_summary.parquet" \
    --folds "$D/m03_folds.parquet" \
    --m04-dir "$D" \
    --actions "$D/m02_actions.parquet" \
    --manifest "$D/m06a_manifest_profile20k.json" \
    --outdir "$D" >> "$LOG" 2>&1 || { say "ABORT: M6a failed"; exit 1; }
  say "M6a done"
else say "M6a SKIP (exists)"; fi

# --- M7 both windows. --copy-latency-ms 4000 = MEASURED live V17 latency, matching m05's
#     P95_COPY_LATENCY_S=4.0. The first window pays the wallet-shard build; the second gets a
#     content-hash cache HIT (concurrent runs into a shared cache are explicitly out of scope, so
#     these MUST stay sequential). ---
for W in pretest test; do
  if [ -z "$(ls $D/m07_${W}/m07_summary*.parquet 2>/dev/null)" ]; then
    say "M7 $W start"
    $PY research/v15/v15_m07_engine.py \
      --actions "$D/m02_actions.parquet" \
      --shortlist "$D/m06a_shortlist.parquet" \
      --folds "$D/m03_folds.parquet" \
      --out "$D/m07_${W}" \
      --window "$W" \
      --slip-calib "$D/slippage_calib.json" \
      --copy-latency-ms 4000 >> "$LOG" 2>&1 || { say "ABORT: M7 $W failed"; exit 1; }
    say "M7 $W done"
  else say "M7 $W SKIP (exists)"; fi
done

# --- THE DELIVERABLE ---
say "PROFILE start"
$PY research/v15/copy_wallet_profile.py --dir "$D" >> "$LOG" 2>&1 || say "PROFILE rc=$? (non-fatal)"
say "PROFILE done"
say "=== funnel20k chain COMPLETE ==="
