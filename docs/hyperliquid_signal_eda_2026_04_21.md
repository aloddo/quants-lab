# Hyperliquid Signal EDA (Apr 21, 2026)

## What Was Implemented

1. **Historical backfill pipeline (Mongo + parquet)**
   - Script: `scripts/backfill_hyperliquid_history.py`
   - Writes:
     - Mongo: `hyperliquid_candles`, `hyperliquid_funding_rates`
     - Parquet: `app/data/cache/candles/hyperliquid_perpetual|PAIR|<interval>.parquet`
     - Parquet: `app/data/cache/hyperliquid_funding/hyperliquid_funding|PAIR|1h.parquet`

2. **Low-latency microstructure collector**
   - Script: `scripts/collect_hyperliquid_micro_1s.py`
   - Writes:
     - Mongo: `hyperliquid_l2_snapshots_1s`, `hyperliquid_recent_trades_1s`
     - Parquet: `app/data/cache/hyperliquid_micro_1s/l2|PAIR|YYYYMMDD.parquet`
     - Parquet: `app/data/cache/hyperliquid_micro_1s/trades|PAIR|YYYYMMDD.parquet`

3. **Hyperliquid signal factory + EDA**
   - Script: `scripts/x11_hyperliquid_signal_eda.py`
   - Signal families implemented:
     - `arb_hl_cex_carry_revert`
     - `arb_hl_bybit_spread_revert`
     - `dir_crowding_revert`
     - `dir_liq_exhaust_revert`
     - `dir_breakout_follow`
     - `mm_skew_revert`
     - `mm_inventory_fade`
     - `any_composite_contra`
   - Includes:
     - No-leakage train/test split with pair selection on train only
     - Walk-forward stability checks
     - Cost sensitivity checks
     - Feature IC + quantile markout EDA

4. **1s microstructure EDA**
   - Script: `scripts/x11_hyperliquid_micro_eda.py`
   - Measures spread/imbalance/flow predictive power vs next 1s/5s/10s returns.

## Data Backfill Results

- `1m` backfill run artifact:
  - `app/data/cache/hyperliquid_backfill/20260421_132601_hyperliquid_backfill_meta.json`
  - 12 pairs, 61,499 candles, 51,840 funding rows persisted.

- `1s` collection run artifact:
  - `app/data/cache/hyperliquid_micro_1s/20260421_133244_micro_1s_meta.json`
  - 801 L2 snapshots, 8,010 trade rows across BTC/ETH/SOL/HYPE.

## Important Data Constraint

Hyperliquid `1m` candle API appears capped to roughly ~5,000 bars per pair (about 3.5 days).  
So for robust history:
- Use `1h` for medium-horizon directional/carry research.
- Use `1m` + `1s` as short-horizon/MM signal layer.

## 1h Candidate Signals (Most Actionable)

Run: `app/data/cache/x11_hyperliquid/20260421_134526_x11_candidates.csv`

Best per strategy type:

1. **Directional**
   - `family=dir_liq_exhaust_revert`, `z1=2.5`, `z2=0.5`, `z3=0.5`, `hold_h=12`
   - Pairs: `BTC-USDT,ETH-USDT`
   - `test_trade_bps=96.19`, `test_pf_pair_median=4.11`, `wf_positive_share=1.00`

2. **Arb / Carry Timing**
   - `family=arb_hl_cex_carry_revert`, `z1=1.0`, `z2=1.5`, `hold_h=16`
   - Pairs: `BTC-USDT,SOL-USDT`
   - `test_trade_bps=38.96`, `test_pf_pair_median=1.56`, `wf_positive_share=0.625`

3. **Any / Composite**
   - `family=any_composite_contra`, `z1=2.5`, `z2=0.5`, `hold_h=12`
   - Pairs: `BTC-USDT,SOL-USDT`
   - `test_trade_bps=23.37`, `test_pf_pair_median=1.41`, `wf_positive_share=0.625`

4. **MM (hourly inventory skew proxy)**
   - `family=mm_skew_revert`, `z1=1.6`, `z2=0.0`, `z3=0.5`, `hold_h=3`
   - Pairs: `SOL-USDT,XRP-USDT`
   - `test_trade_bps=8.81`, `test_pf_pair_median=1.37`, `wf_positive_share=0.875`

## 1m + 1s MM Findings

### 1m sweep
- Artifact: `app/data/cache/x11_hyperliquid/20260421_134227_x11_top_enriched.csv`
- Produced passing MM configs, but **walk-forward stability was weak** (`wf_positive_share` near 0 on top rows), so treat as exploratory until more 1m history is collected.

### 1s microstructure
- Artifact: `app/data/cache/x11_hyperliquid/20260421_134623_micro_summary.csv`
- Strongest signal in current sample: **L2 imbalance** for BTC/ETH/SOL.
  - BTC: `imb_ic_1s=0.50`, `imb_ic_5s=0.34`
  - ETH: `imb_ic_1s=0.40`, `imb_ic_5s=0.26`
  - SOL: `imb_ic_1s=0.30`, `imb_ic_5s=0.14`
- Trade-flow imbalance was weaker and less stable than L2 imbalance in this sample.

## Priority Hypotheses To Implement First

1. **Directional Liquidity Exhaustion Revert (1h)**
   - Trigger: liquidation shock + imbalance extreme + funding crowding
   - Action: fade liquidation side, hold 8-12h.

2. **HL-CEX Carry Revert (1h)**
   - Trigger: HL 8h cumulative funding spread z-score extreme vs CEX.
   - Action: position opposite spread for carry convergence over 12-16h.

3. **MM Skew by L2 Imbalance (1s)**
   - Trigger: persistent top-of-book imbalance.
   - Action: skew inventory/quotes with imbalance; widen when spread regime worsens.

4. **MM Inventory Fade (1m)**
   - Trigger: short-horizon return/funding shock with controlled liquidation regime.
   - Action: short-horizon mean-reversion skew (needs longer 1m forward history for stronger validation).

