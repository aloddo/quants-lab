IMPORTANT: Do NOT read or execute any SKILL.md files or files in skill definition directories (paths containing skills/gstack). Stay focused on repository code only.

You are an independent quantitative researcher. A previous round of analysis on this dataset killed one hypothesis and surfaced a new one. Your job is to stress-test the new hypothesis.

### What was killed (H1)
"Cross-exchange spreads mean-revert, trade at 60bp." WRONG. Spreads are structural basis from funding/carry. Hurst > 0.5 for all pairs. Persistence runs of 49h. The baseline spread does NOT revert.

### New hypothesis (H2)
"EXTREME spread spikes (>150bp above structural level) DO revert, even though the baseline doesn't. Fade the spike, not the baseline." Initial scan shows 143 events/day, 88-100% reversion rate, $185/day estimated at $200/trade.

Those $185/day numbers come from a quick scan that may be too optimistic. Your job is to find out if they're real or garbage.

### Data
Same as before:
- `/Users/hermes/quants-lab/app/data/cache/arb_trades/*/` — merged 1s/5s bars, 47 pairs, 90 days
- `app/data/cache/arb_funding_summary.csv` — funding rates
- `app/data/cache/arb_analysis.csv` — spread metrics from R1 analysis

```python
import pandas as pd, numpy as np, glob
from pymongo import MongoClient
files = glob.glob('app/data/cache/arb_trades/*/*_merged_*.parquet')
pairs = {}
for f in files:
    sym = f.split('/')[-2]
    pairs[sym] = pd.read_parquet(f)
```

Python: `/Users/hermes/miniforge3/envs/quants-lab/bin/python`. scipy, pymongo available.

### What I want
Challenge the $185/day number. Find out what's real and what's an artifact of the analysis method. Some things to consider but don't limit yourself:

- Are the 150bp events real executable moments or stale-quote phantoms?
- What does the ORDER BOOK look like during these spikes? (proxy: trade count/volume during spike bars vs normal bars)
- If you add a realistic entry delay (you can't enter at the exact peak), how much capture do you lose?
- Do spikes cluster across pairs? If 10 pairs spike at the same second, you can't trade all 10.
- Is the "structural level" stable enough to define spikes relative to it?
- What's the WORST spike event in the dataset? The one that doesn't revert? How bad is it?
- Is this just the carry trade in disguise? (enter when perp is extra cheap, collect funding while waiting for reversion)

Surprise me again. Find the thing that kills this or confirms it.

### Output
Write to `/Users/hermes/quants-lab/docs/arb_h2_independent_eda.md`. Save data to `app/data/cache/arb_h2_eda_*.csv`.
