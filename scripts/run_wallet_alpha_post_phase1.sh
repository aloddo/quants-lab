#!/bin/bash
# Run wallet alpha pipeline phases 2-6 after Phase 1 completes
# Usage: bash scripts/run_wallet_alpha_post_phase1.sh

set -e
cd ~/quants-lab
PYTHON=/Users/hermes/miniforge3/envs/quants-lab/bin/python

echo "=== Phase 1b: Universe Summary ==="
$PYTHON -c "
from app.research.wallet_alpha.phase1_data_prep import compute_universe_summary
from pathlib import Path
import pandas as pd

fills_dir = Path('app/data/wallet_alpha/fills')
output_dir = Path('app/data/wallet_alpha')

summary = compute_universe_summary(fills_dir)
summary.to_csv(output_dir / 'universe_summary.csv', index=False)
print(f'Universe: {len(summary):,} wallets')

# Apply filters
filtered = summary[
    (summary['fill_count'] >= 50) &
    (summary['coins_traded'] >= 5) &
    (summary['total_notional'] >= 10_000) &
    (summary['fills_per_day'] <= 10_000)
]
filtered.to_csv(output_dir / 'universe_filtered.csv', index=False)
print(f'Filtered: {len(filtered):,} wallets')
"

echo ""
echo "=== Phase 1c: Fill-derived mid-prices ==="
$PYTHON -m app.research.wallet_alpha.build_fill_midprice

echo ""
echo "=== Phase 2: Event construction ==="
$PYTHON -m app.research.wallet_alpha.phase2_events

echo ""
echo "=== Phase 3: Feature engineering ==="
$PYTHON -m app.research.wallet_alpha.phase3_features

echo ""
echo "=== Phase 4-5: Scoring ==="
$PYTHON -m app.research.wallet_alpha.phase4_scoring

echo ""
echo "=== DONE ==="
echo "Results at: app/data/wallet_alpha/"
