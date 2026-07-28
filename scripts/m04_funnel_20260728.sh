#!/bin/bash
# m04 authenticity, fold-pure, for the 13-fold calendar of funnel_20260728.
#
# WHY all 13 folds and not just the new 9-13: entity_id is joined ACROSS folds
# (m05 L391 dedups prim on entity_id; m06a L441 groups shortlist by entity_id;
# m06b groups by (entity_id, fold_id)). The pre-existing app/data/v15/m04_*_f1..f8
# were built over the 20,378-wallet universe, so their entity_ids are NOT
# comparable to ids from a 1,630-wallet union-find. Mixing them silently
# mis-joins entities. One universe for all 13 folds or nothing.
#
# WHY the 1,630 screen set and not the 20k universe: the box has ~3.2GB available
# (8.4GB swapped) and plan_memory_budget correctly aborts a 20k run. Measured cost
# of restricting to 1,630 (against f8): ZERO pool-dedup loss (no two screen wallets
# share an entity) and a bounded 29-wallet (1.8%) blind spot on the cross-set
# internal-hedge danger signal. Recorded, not hidden.
#
# as-of per fold = M3 test_start (fold-pure: signals use only ts < as_of).
set -u
cd /Users/hermes/quants-lab
PY=/Users/hermes/miniforge3/envs/quants-lab/bin/python
OUT=app/data/v15/funnel_20260728
W=$OUT/screen_wallets_1630.txt
LOG=/tmp/ql_m04_funnel_20260728.log

# fold_id:as_of (test_start), from m03_folds.parquet
FOLDS="1:2026-01-26 2:2026-02-09 3:2026-02-23 4:2026-03-09 5:2026-03-23 6:2026-04-06 7:2026-04-20 8:2026-05-04 9:2026-05-18 10:2026-06-01 11:2026-06-15 12:2026-06-29 13:2026-07-13"

echo "=== m04 funnel_20260728 start $(date -u +%FT%TZ) ===" >> "$LOG"
printf '%s\n' $FOLDS | while read -r pair; do
  fid="${pair%%:*}"; asof="${pair##*:}"
  o="$OUT/m04_authenticity_f${fid}.parquet"
  e="$OUT/m04_entities_f${fid}.parquet"
  if [ -s "$o" ] && [ -s "$e" ]; then
    echo "[f$fid] SKIP (exists)" >> "$LOG"; continue
  fi
  echo "[f$fid] as-of $asof START $(date -u +%FT%TZ)" >> "$LOG"
  $PY research/v15/v15_m04_authenticity.py \
    --wallets-file "$W" --as-of "$asof" \
    --out "$o" --entities-out "$e" \
    --procs 1 --per-worker-gb 0.8 --headroom-gb 0.4 >> "$LOG" 2>&1
  rc=$?
  echo "[f$fid] rc=$rc END $(date -u +%FT%TZ)" >> "$LOG"
  if [ $rc -ne 0 ]; then
    echo "[f$fid] FAILED - stopping chain (fail closed)" >> "$LOG"
    exit 1
  fi
done
echo "=== m04 funnel_20260728 done $(date -u +%FT%TZ) ===" >> "$LOG"
