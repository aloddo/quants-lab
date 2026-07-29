#!/bin/bash
# CENSUS: every rankable+eligible (wallet,fold) seat — 56,732 seats over 14,730 wallets, folds 1-8.
# NO sampling, NO top-N. This exists because the 2026-07-28 profile sampled 2,000 seats/fold at random
# and I then reported a longitudinal verdict off a cross-section. The census removes the sampling
# objection entirely: if a copyable trader exists in this universe, it is IN this run.
#
# Ordering: census FIRST, then m04 folds 9-12. Sequential, never concurrent — the box is 16GB and the
# 2026-07-27 lesson was that a second worker made everything SLOWER, not faster. m04 f9-12 skips folds
# whose parquet already exists, so stopping it now costs only fold 9's partial pass.
#
# COST (anchored on MEASURED m07: 15,993 seats = 64.7min pretest + 22.5min test):
#   56,732 seats = 3.55x  ->  ~3.8h pretest + ~1.3h test  =  ~5.2h, then profile (~5min).
#   Then m04 f9-12 at the measured ~3.5h/fold => ~14h. Total ~19h.
set -u
cd /Users/hermes/quants-lab
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
D=app/data/v15/census20k_20260728
LOG=/tmp/ql_census20k.log

echo "=== census20k chain start $(date -u +%FT%TZ) ===" >> "$LOG"

for w in pretest test; do
  if [ -s "$D/m07_${w}/m07_positions.parquet" ]; then
    echo "[m07 $w] SKIP (exists)" >> "$LOG"; continue
  fi
  echo "[m07 $w] START $(date -u +%FT%TZ)" >> "$LOG"
  $PY research/v15/v15_m07_engine.py \
    --actions "$D/m02_actions.parquet" \
    --shortlist "$D/m06a_shortlist.parquet" \
    --folds "$D/m03_folds.parquet" \
    --out "$D/m07_${w}" \
    --window "$w" \
    --slip-calib "$D/slippage_calib.json" \
    --copy-latency-ms 4000 \
    --sizing-mode fixed_position \
    --fixed-target-exposure 0.10 >> "$LOG" 2>&1
  rc=$?
  echo "[m07 $w] rc=$rc END $(date -u +%FT%TZ)" >> "$LOG"
  [ $rc -ne 0 ] && { echo "[m07 $w] FAILED - stopping (fail closed)" >> "$LOG"; exit 1; }
done

echo "[profile] START $(date -u +%FT%TZ)" >> "$LOG"
$PY research/v15/copy_wallet_profile.py --dir "$D" >> "$LOG" 2>&1
echo "[profile] rc=$? END $(date -u +%FT%TZ)" >> "$LOG"

echo "[candidates] START $(date -u +%FT%TZ)" >> "$LOG"
$PY research/v15/copy_candidate_report.py --dir "$D" >> "$LOG" 2>&1
echo "[candidates] rc=$? END $(date -u +%FT%TZ)" >> "$LOG"

echo "=== census20k done $(date -u +%FT%TZ) — handing the box to m04 f9-12 ===" >> "$LOG"

# m04 folds 9-12 resumes here, with the box to itself.
bash scripts/m04_20k_folds9to12.sh >> "$LOG" 2>&1
echo "=== m04 f9-12 done $(date -u +%FT%TZ) ===" >> "$LOG"
