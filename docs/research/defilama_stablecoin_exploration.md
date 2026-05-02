# DefiLlama Stablecoin Flow Exploration

**Date:** 2026-05-01
**Status:** EXPLORED -- signal too weak for standalone strategy
**Source:** DefiLlama Stablecoins API (no auth, no key)

## API Summary

| Endpoint | Data | Granularity | History |
|----------|------|-------------|---------|
| `/stablecoins` | 363 stablecoins listed with mcap | snapshot | -- |
| `/stablecoin/{id}` | Total supply per day (USDT id=1, USDC id=2) | daily | 2017-11+ |
| `/stablecoincharts/{chain}?stablecoin={id}` | Per-chain supply + minted/bridged | daily | 2017-11+ |
| `/stablecoinchains` | 198 chains with current totals | snapshot | -- |

Top chains: Ethereum ($164B), Tron ($88B), Solana ($15B), BSC ($14B).
USDT mcap: $189.5B. USDC: $77.3B. Combined: $266.8B.

## Analysis (n=1820 days, 2021-04 to 2026-05)

### Hypothesis 1: 7-day supply delta predicts BTC forward returns

| Forward window | Pearson r | p-value | Spearman rho | p-value |
|----------------|-----------|---------|--------------|---------|
| 1d | -0.034 | 0.144 | -0.002 | 0.950 |
| 3d | -0.047 | 0.044 | -0.000 | 1.000 |
| 7d | -0.075 | 0.001 | -0.005 | 0.840 |

Pearson shows weak negative (r ~ -0.07), marginally significant. Spearman (rank) is near zero -- the relationship is driven by outliers, not monotonic signal.

### Hypothesis 2: Large mint days (z > 2) predict positive returns

62 large mint days vs 1381 normal days.
- 7d forward: mint mean +1.52% vs normal +0.46%. t=1.07, p=0.28. **Not significant.**

### Hypothesis 3: Large burn days (z < -2)

49 large burn days. 3d forward: burn mean +1.59% vs normal +0.10%. t=2.08, p=0.04. Marginally significant but small sample.

### Quintile analysis (7d supply delta -> 7d forward return)

| Quintile | Mean fwd 7d | Std | n |
|----------|-------------|-----|---|
| Q1 (low) | -0.54% | 8.0% | 364 |
| Q2 | +0.31% | 6.7% | 364 |
| Q3 | +1.51% | 7.3% | 364 |
| Q4 | +1.73% | 7.4% | 364 |
| Q5 (high) | -0.96% | 8.1% | 364 |

Non-monotonic. Both extremes (Q1 + Q5) underperform. Not a directional signal.

### Year-by-year stability

| Year | r | p | n |
|------|---|---|---|
| 2021 | -0.287 | 0.000 | 245 |
| 2022 | +0.013 | 0.808 | 365 |
| 2023 | -0.118 | 0.024 | 365 |
| 2024 | -0.036 | 0.492 | 366 |
| 2025 | -0.064 | 0.224 | 365 |
| 2026 | +0.516 | 0.000 | 114 |

Regime-dependent. Flips sign across years. 2021 showed decent negative correlation (-0.29), 2026 YTD shows strong positive (+0.52). This instability kills standalone use.

### Lag analysis (supply delta leads price?)

| Lag (days) | r | p |
|------------|---|---|
| 1 | -0.073 | 0.002 |
| 3 | -0.083 | 0.000 |
| 7 | -0.097 | 0.000 |
| 14 | -0.040 | 0.086 |
| 21 | +0.035 | 0.143 |

Weak but consistent negative at 1-7 day lag. Peaks at 7-day lag (r=-0.097). This is the most interesting finding -- supply increases weakly predict negative 7d returns with a 7-day lag. However, |r| < 0.1 is too weak for standalone trading.

## Verdict

**WEAK -- not tradeable as standalone signal.** The best finding (7d lagged supply delta -> 7d fwd return, r=-0.097) explains less than 1% of variance. The relationship is regime-dependent (flips sign across years) and non-monotonic (quintile analysis shows no clean edge).

**Potential as composite feature:** Could serve as a macro regime filter (like BTC regime in E3) rather than a trade signal. When 7d supply delta is in Q4-Q5 (strong expansion), BTC forward returns are unpredictable -- possible risk-off filter. Would need to test in combination with existing signals (E3 regime filter, X14 crowd fade).

**Next steps if pursuing:**
1. Build a BybitDefiLlamaTask to collect daily supply snapshots to MongoDB
2. Create DefiLlamaFeature (FeatureBase) computing 7d/30d supply deltas + z-scores
3. Test as auxiliary filter on E3 or X14 (not as primary signal)
4. Priority: LOW -- other P2 sources (Google Trends, alt exchange OI) likely have stronger signal
