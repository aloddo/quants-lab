#!/bin/bash
# Build fold-pure V15 M4 authenticity/entity files.
#
# Usage:
#   scripts/build_m4_perfold.sh [folds.parquet] [wallets.txt] [outdir]
#
# This is a helper only. It intentionally does not run as part of the pipeline unless invoked.

set -euo pipefail

FOLDS=${1:-app/data/v15/m03_folds.parquet}
WALLETS=${2:-app/data/v15/m01_universe_20k_wallets.txt}
OUTDIR=${3:-app/data/v15}
PY=${PY:-/Users/hermes/miniforge3/envs/quants-lab/bin/python}
SAFE=${SAFE:-scripts/mem_safe_run.sh --floor-gb 4}
PROCS=${PROCS:-3}

mkdir -p "$OUTDIR"

"$PY" - "$FOLDS" <<'PY' | while IFS=$'\t' read -r fid test_start; do
import sys
import pandas as pd

folds = pd.read_parquet(sys.argv[1]).sort_values("fold_id")
for r in folds.itertuples():
    ts = pd.Timestamp(r.test_start)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    print(f"{int(r.fold_id)}\t{ts.strftime('%Y-%m-%d')}")
PY
  echo "[build_m4_perfold] fold ${fid}: M4 as-of ${test_start}"
  # --headroom-gb default (6) is too conservative for a fleet-loaded box (aborts at <7.5GB free); lower it so
  # M4 fits available RAM while mem_safe_run still backstops. Override via M4_HEADROOM_GB / M4_PER_WORKER_GB.
  $SAFE --label "m04_f${fid}" -- "$PY" research/v15/v15_m04_authenticity.py \
    --wallets-file "$WALLETS" \
    --as-of "$test_start" \
    --out "$OUTDIR/m04_authenticity_f${fid}.parquet" \
    --entities-out "$OUTDIR/m04_entities_f${fid}.parquet" \
    --headroom-gb "${M4_HEADROOM_GB:-1.5}" --per-worker-gb "${M4_PER_WORKER_GB:-2}" \
    --procs "$PROCS"
done
