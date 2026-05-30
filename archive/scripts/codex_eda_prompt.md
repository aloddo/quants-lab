# Codex Independent EDA — Cross-Exchange Arb

## Launch Command
```bash
codex exec "$(cat scripts/codex_eda_prompt.md)" -C /Users/hermes/quants-lab -s workspace-write
```

## Prompt

IMPORTANT: Do NOT read or execute any SKILL.md files or files in skill definition directories (paths containing skills/gstack). Stay focused on repository code only.

You are an independent quantitative researcher. You have been given trade-level data from two crypto exchanges (Binance SPOT and Bybit perpetual futures) covering 47 pairs across all liquidity tiers, from BTC down to micro-caps.

The hypothesis under investigation: cross-exchange price differences (spreads) between these two venues are exploitable for profit after transaction costs (~24bp round-trip).

Your job is NOT to confirm or deny this hypothesis. Your job is to find what's actually in the data. What patterns exist? What risks hide in the tails? What would a smart adversary see that the strategy designer missed?

### Data

**Trade-level merged bars:**
`/Users/hermes/quants-lab/app/data/cache/arb_trades/*/`
Each subdirectory is a pair (e.g., BTCUSDT, BANDUSDT). Inside:
- `*_merged_*d.parquet` — synchronized bars from both exchanges. Columns include: bn_vwap, bb_vwap, spread_vwap_bps, abs_spread_vwap, bn_volume_usd, bb_volume_usd, bn_trade_count, bb_trade_count, bn_buy_count, bb_buy_count, and more.
- Some pairs have 1-second bars (90 days), others 5-second (14 days).
- `spread_vwap_bps` = (bb_vwap - bn_vwap) / bn_vwap * 10000. Positive means Bybit is more expensive.

**Universe snapshot:** `app/data/cache/arb_universe.csv` — 339 pairs with volumes and tiers.

**Funding rates:** `app/data/cache/arb_funding_summary.csv` — annualized funding rates per pair.

**MongoDB** (localhost:27017, database quants_lab, no auth):
- `bybit_funding_rates` — 536K docs, field: pair (e.g., "BAND-USDT"), funding_rate, timestamp_utc (ms)

### Quick-start (so you don't waste tokens on discovery)

```python
import pandas as pd, numpy as np, glob
from pymongo import MongoClient

# Load all merged files
files = glob.glob('app/data/cache/arb_trades/*/*_merged_*.parquet')
pairs = {}
for f in files:
    sym = f.split('/')[-2]
    pairs[sym] = pd.read_parquet(f)
# pairs['BANDUSDT'] is a DatetimeIndex DataFrame with bn_vwap, bb_vwap, spread_vwap_bps, etc.

# Funding
funding = pd.read_csv('app/data/cache/arb_funding_summary.csv')

# MongoDB
db = MongoClient('mongodb://localhost:27017')['quants_lab']
```

Python is at `/Users/hermes/miniforge3/envs/quants-lab/bin/python`. scipy and pymongo are available.

### What I want from you

Surprise me. Find something we don't know. The strategy team has been looking at this data through the lens of "is the spread wide enough to trade?" — that's a narrow question. You have the full dataset. Ask better questions.

Some starting points if you need them, but don't limit yourself:
- Is this spread behavior or is it noise? What's the signal-to-noise ratio?
- What kills this strategy? Not the theoretical risks — the ones hiding in the data.
- If you had to bet AGAINST this strategy, what would you point to?
- What's the relationship between spread, volume, volatility, and time?
- Are there patterns the strategy designer is likely to overfit to?

Be quantitative. Show your work. Use real numbers from the data.

### Output
Write findings to `/Users/hermes/quants-lab/docs/arb_independent_eda.md`. Save computed data to `app/data/cache/arb_eda_*.csv`. Print key findings as you go.
