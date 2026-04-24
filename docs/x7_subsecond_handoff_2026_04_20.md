# X7 Subsecond EDA Update (2026-04-20)

## Scope
Extended EDA for fast CEX execution using local 5s Binance-Bybit merged data and lagged on-chain gate (`stable_buy_usd_z24`).

Artifacts generated:
- `app/data/cache/x7_cex_advanced_eda_20260420_tail_sweep.csv`
- `app/data/cache/x7_cex_advanced_eda_20260420_latency_decay.csv`
- `app/data/cache/x7_cex_advanced_eda_20260420_spillover.csv`
- `app/data/cache/x7_cex_advanced_eda_20260420_model_wf_folds.csv`
- `app/data/cache/x7_cex_advanced_eda_20260420_model_summary.csv`
- `app/data/cache/x7_cex_advanced_eda_20260420_perm.csv`
- `app/data/cache/x7_cex_advanced_eda_20260420_rolling_daily_validation.csv`
- `app/data/cache/x7_cex_advanced_eda_20260420_doge_latency_daily_ttest.csv`
- `app/data/cache/x7_cex_advanced_eda_20260420_doge_cost_daily_ttest.csv`
- `app/data/cache/x7_cex_advanced_eda_20260420_doge_event_q_sweep.csv`
- `app/data/cache/x7_cex_advanced_eda_20260420_event_nonoverlap.csv`
- `app/data/cache/x7_cex_advanced_eda_20260420_doge_event_gate_q_grid.csv`

Code:
- `scripts/x7_cex_advanced_eda.py`

## What Survived and What Failed
1. **Fast lead-lag tail events remain the only meaningful edge family.**
- Best signal family: `lead2 = (BN 2-bar return - BB 2-bar return)`.
- DOGE strongest by far; SOL weak; ETH/XRP/BTC mostly flat/negative after realistic costs.

2. **Latency is decisive.**
- Bar-level scan shows DOGE positive only at `latency_5s = 0`.
- With +1 bar delay (5s), daily mean flips negative with strong significance.
- See `..._doge_latency_daily_ttest.csv`.

3. **Cost ceiling is tight.**
- In realistic non-overlap event simulation (`hold=30s`, round-trip fees):
  - Edge is positive and significant up to roughly `one-way <= 0.30 bps`.
  - Breakeven around `one-way ~0.35-0.40 bps`.
  - At `one-way = 0.50 bps` (round-trip 1.0 bps), strategy is negative.
- See `..._doge_event_cost_sweep.csv`.

4. **Non-overlap realism changes conclusions.**
- Bar-level proxy backtests can look attractive for DOGE.
- After enforcing non-overlap fixed-hold event logic with round-trip costs:
  - `DOGE, q=0.95, one-way=0.5 bps` is negative (`avg_daily_net_bps ~ -0.0097`, `p ~ 0.035`).
  - On-chain gating reduces damage but is not significantly positive at this fee level.
- See `..._event_nonoverlap.csv` and `..._doge_event_gate_q_grid.csv`.

5. **Model complexity did not beat simple tails.**
- Purged walk-forward XGBoost micro model is negative net after costs on all assets.
- Permutation tests do not support positive model alpha.
- See `..._model_summary.csv` and `..._perm.csv`.

6. **Cross-asset spillover (BTC leader -> alts) is not monetizable.**
- Statistically detectable, but net negative after costs.
- See `..._spillover.csv`.

## Practical Exploitation Constraints
Deploy only if all are true:
- Effective **one-way all-in cost <= 0.30 bps** (fee + slippage + adverse selection).
- Effective action latency materially below a 5s bar (current sample granularity already shows 5s delay kills edge).
- Use **event engine with non-overlap hold logic**, not bar-by-bar PnL proxy.

Candidate live config for pilot:
- Universe: `DOGEUSDT` only initially.
- Signal: `lead2`.
- Entry threshold: adaptive rolling quantile (`q in [0.97, 0.98]`, trained on trailing 3 days).
- Hold: 30s equivalent.
- Risk: hard daily kill-switch if rolling 1-day net below control limit.
- Optional gate: `stable_buy_usd_z24 > 1` only marginally helps at high costs; do not rely on it for rescue.

## Capacity Reality at Current Clip Size
Using best realistic non-overlap DOGE config from this sample (`q=0.97`, `one-way=0.25 bps`):
- Avg trade net edge: ~`0.548 bps` per filled trade.
- At `$300` clip and ~`492` trades/day, expected mean is only ~`$8/day`.
- At `one-way=0.5 bps`, expected mean drops near noise-level (~`$1.6/day` on best q).

Interpretation: this is latency/fee-sensitive micro alpha, not large standalone PnL at current clip.

## Session/Regime Notes (DOGE)
- Better UTC windows in this 14-day sample: `13`, `14`, `00`, `18`, `23`, `22`.
- Weak/negative windows include `15`, `11`, `01`.
- See `..._doge_hour_regime.csv` and `..._doge_dow_regime.csv`.

## Next Research That Matters
1. Replace 5s bars with true tick/L2 event data; current cadence hides subsecond path and queue effects.
2. Add maker/taker classification and queue-position proxy; fee tier/rebate reality is decisive.
3. Add venue microstructure features unavailable in current panel (best bid/ask imbalance, depth slope, cancel bursts).
4. Add strict paper-trade replay with your production gateway latency and partial-fill model before capital.
