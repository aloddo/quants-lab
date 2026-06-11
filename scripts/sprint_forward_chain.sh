#!/bin/bash
# SPRINT forward-test chain: run when (a) fill pullers done through --end day, (b) marks restored.
# partition cohort fills -> m02 cohort slice -> forward replay -> analysis.
set -euo pipefail
P=/Users/hermes/miniforge3/envs/quants-lab/bin/python
R=/Users/hermes/quants-lab
END_DAY=${1:-2026-06-10}

echo "=== 1/4 partition cohort fills ==="
$P $R/scripts/sprint_partition_cohort.py --end 20260612

echo "=== 2/4 cohort wallets file ==="
$P - <<'EOF'
import json
cfg = json.load(open('/Users/hermes/quants-lab/config/copy_trader_wallets_v16.json'))
with open('/tmp/v16_cohort_wallets.txt','w') as f:
    for w in cfg['wallets']:
        f.write(w.lower() + '\n')
print(f"{len(cfg['wallets'])} wallets -> /tmp/v16_cohort_wallets.txt")
EOF

echo "=== 3/4 m02 cohort slice (2025-12-01 -> 2026-06-12) ==="
$P $R/research/v15/v15_m02_journey_trace.py \
  --wallets-file /tmp/v16_cohort_wallets.txt \
  --start 2025-12-01 --end 2026-06-12 \
  --actions-out $R/app/data/v16/m02_cohort_slice.parquet \
  --journeys-out $R/app/data/v16/m02_cohort_journeys.parquet \
  --procs 6 --skip-marks-cache

echo "=== 3.5/4 merge sprint marks (assetctx + tape bridge) ==="
$P $R/scripts/sprint_merge_marks.py

echo "=== 4/4 forward replay + analysis ==="
V16_SPRINT_MARKS_DIR=$R/app/data/v15/assetctx_marks_sprint \
$P $R/research/v16/forward_test.py --forward --end "$END_DAY" \
  --m02-slice $R/app/data/v16/m02_cohort_slice.parquet
DAYS=$($P -c "import pandas as pd; print((pd.Timestamp('$END_DAY')-pd.Timestamp('2026-05-23')).days)")
$P $R/research/v16/sprint_analysis.py --in $R/app/data/v16/forward_trades.parquet --days "$DAYS"
echo "=== CHAIN DONE ==="
