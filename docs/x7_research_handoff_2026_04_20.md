# X7 Research Handoff (April 20, 2026)

## Scope and Data
- Objective: directional signal discovery for Bybit perps using on-chain data.
- Assets analyzed: BTC, ETH, SOL, XRP, DOGE.
- On-chain source: Dune hourly data, 90-day lookback.
- Price source: local parquet hourly candles (`bybit_perpetual|*-USDT|1h.parquet`).
- Validation target: Spearman rank correlation vs forward returns (1h, 4h, 24h), then tradability tests.

## Important Methodological Correction
- Initial advanced pass had potential same-hour alignment leakage.
- All follow-up validation uses strict **+1h lag** on predictive features:
  - feature at `t-1` predicts return from `t` onward.
- For 4h targets, validation also uses 4h decision timestamps (`hour % 4 == 0`) to avoid overlap bias.

## Stage 1 Baseline Query Results (Simple)
Artifacts:
- `app/data/cache/onchain_x7/20260420_153742_pooled_correlations.csv`
- `app/data/cache/onchain_x7/20260420_153742_asset_correlations.csv`

Pooled significant hits (`p < 0.05`, `N > 100`):
- `x7_h1_dex_flow_imbalance` vs 1h (`rho=0.0498`, `p=1.9e-07`)
- `x7_h1_dex_flow_imbalance` vs 4h (`rho=0.0395`, `p=3.7e-05`)
- `x7_h2_dex_breadth_imbalance` vs 4h (`rho=0.0211`, `p=0.0279`)
- `x7_h3_whale_transfer_intensity` vs 24h (`rho=0.0236`, `p=0.0142`)

## Stage 2 Advanced Dune Feature Set
Script:
- `scripts/x7_advanced_research.py`

Advanced queries:
1. `x7_adv_q1_dex_features`:
   - flow imbalance, large-trade imbalance, breadth imbalance, stablecoin conversion imbalance, stablecoin pair share.
2. `x7_adv_q2_transfer_features`:
   - transfer volume/count, unique senders/receivers, whale buckets (`>=100k`, `>=1m`), p90 transfer size.
3. `x7_adv_q3_chain_dispersion`:
   - DEX chain HHI / max chain share / chain count.

Artifacts (latest run):
- `app/data/cache/onchain_x7_advanced/20260420_154932_metadata.json`
- `app/data/cache/onchain_x7_advanced/20260420_154932_feature_panel.csv`
- `app/data/cache/onchain_x7_advanced/20260420_154932_univariate_ic.csv`
- `app/data/cache/onchain_x7_advanced/20260420_154932_walkforward_strategy.csv`
- `app/data/cache/onchain_x7_advanced/20260420_154932_model_metrics.json`

## What Survived After Leak-Free Recheck
Using +1h lag:
- `stable_buy_usd_z24` retains strong signal at 4h and 24h:
  - 4h: `rho=0.0568`, `p=3.76e-09`
  - 24h: `rho=0.0542`, `p=2.25e-08`
- Many 1h effects from initial pass weaken materially after lagging.

Interpretation:
- Best exploitable structure is **slower horizon (4h/24h) stablecoin-linked buy pressure**, not fast 1h scalp.

## Tradability Validation (Lagged, 4h)
Script:
- `scripts/x7_stable_buy_signal_validation.py`

Signal under test:
- Feature: `stable_buy_usd_z24`
- Lag: 1h
- Horizon: 4h
- Position: sign(feature), with per-asset orientation learned on train split.
- Cost assumption: `fee_oneway=0.00055` (conservative one-way execution/friction).

Latest outputs:
- `app/data/cache/onchain_x7_advanced/20260420_155838_stable_buy_validation_summary.json`
- `app/data/cache/onchain_x7_advanced/20260420_155838_stable_buy_train_hourly.csv`
- `app/data/cache/onchain_x7_advanced/20260420_155838_stable_buy_test_hourly.csv`
- `app/data/cache/onchain_x7_advanced/20260420_155838_stable_buy_test_per_asset.csv`

Key metrics (50/50 chronological split):
- Train:
  - total return `+1.42%`
  - annualized Sharpe `0.48`
- Test:
  - total return `+10.35%`
  - annualized Sharpe `2.98`
  - permutation test p-value `0.002` (mean-return null rejected)

Per-asset test diagnostics:
- Strong: ETH, SOL
- Positive but weaker: BTC, DOGE
- Unstable/negative: XRP (should be excluded or separately modeled)

## Practical Exploitation Path (Immediate)
1. Production candidate signal (V1):
   - Universe: ETH, SOL, DOGE (+ optional BTC), exclude XRP initially.
   - Decision cadence: every 4h.
   - Signal: lagged `stable_buy_usd_z24`.
   - Side: sign per-asset orientation matrix updated weekly.
2. Execution on Bybit perps:
   - Maker-first entry and exit.
   - Taker fallback with max slippage guard.
   - Position sizing from signal magnitude with hard cap per market.
3. Cross-venue checks (Binance/Bitvavo spot):
   - Use spot/perp basis and spot liquidity only as execution safety gates (not as alpha source yet).
   - Avoid opening new positions during spot/perp dislocation spikes.

## Required Next Validation Before Live
1. True walk-forward with rolling re-training windows (not single split).
2. Parameter stability:
   - lag choices (1h, 2h),
   - z-window choices (24h, 48h, 72h),
   - fee sensitivity (0.00035 to 0.001 one-way).
3. Venue robustness:
   - Bybit perp execution simulation with realistic fills,
   - compare with Binance spot proxy returns for stress.
4. Regime segmentation:
   - high-vol vs low-vol,
   - trend vs chop,
   - Asia/EU/US sessions.
5. Kill criteria:
   - rolling 30-trade Sharpe < 0,
   - slippage > configured threshold,
   - signal drift (IC collapse).

## Commands for Engineer
Run advanced pull + feature build:
```bash
/Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/x7_advanced_research.py
```

Run focused signal validation:
```bash
/Users/hermes/miniforge3/envs/quants-lab/bin/python \
  scripts/x7_stable_buy_signal_validation.py \
  --panel app/data/cache/onchain_x7_advanced/20260420_154932_feature_panel.csv
```
