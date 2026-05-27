# Research Upgrade Plan: From Napkin Math to Quant Rigor

## The Problem

Our current research is sloppy:
- **Data poverty**: Bybit funding 66 days, OI 41 days, LS ratio 33 days. Can't test across regimes.
- **No statistical rigor**: Z-scores and t-stats without multiple hypothesis correction, no bootstrap CI, no permutation tests.
- **No modeling**: Linear z-score thresholds only. No regime detection, no factor decomposition, no stochastic modeling.
- **No data hygiene**: No coverage checks, no stationarity tests, no outlier handling, no survivorship bias control.

## Part 1: Data Acquisition (URGENT)

### What we have vs what we need

| Data | Current Coverage | Minimum Needed | Gap |
|------|-----------------|----------------|-----|
| Bybit funding rates | 66 days | 730 days (2yr) | **664 days** |
| Bybit OI | 41 days | 365 days (1yr) | **324 days** |
| Bybit LS ratio | 33 days | 365 days | **332 days** |
| HL hourly funding | 375 days | OK (expand coins) | Expand to 100+ coins |
| HL candles | 222 days / 42 coins | 365 days / 100 coins | Expand both |
| Deribit options | 23 days | 365 days | **342 days** |
| Coinglass | EMPTY | 365 days | **Everything** |
| Fear & Greed | 23 days | 730 days | **707 days** |
| Cross-exchange OI | None | 365 days | **Everything** |
| On-chain exchange flows | None | 365 days | **Everything** |
| Bybit 1m candles | 7.5 months / ~50 pairs | 2 years / 100 pairs | **Expand significantly** |
| Macro (DXY, rates, SPX) | None | 2 years daily | **Everything** |
| Liquidation tick data | None | 1 year | **Everything** |

### Data Sources to Add

1. **Bybit historical backfill** (FREE, API already integrated)
   - Funding: `/v5/market/funding/history` — paginate back 2 years
   - OI: `/v5/market/open-interest` — paginate back 1 year
   - LS ratio: `/v5/market/account-ratio` — limited to ~17 days per call but can paginate

2. **Hyperliquid historical** (FREE, API available)
   - Funding: already collecting, expand coin coverage
   - Candles: backfill to full available history for all 100+ coins
   - Liquidations: `/info` endpoint with `type: userFills` or similar

3. **Coinglass** (API key exists in .env, tasks exist but EMPTY)
   - Fix the collection task (likely broken)
   - Cross-exchange OI, liquidation heatmaps, long/short ratios

4. **Alternative.me Fear & Greed** (FREE)
   - Full history available: `https://api.alternative.me/fng/?limit=0`
   - Backfill entire history (2018+)

5. **Deribit historical options** (FREE API)
   - Longer history needed for vol surface research
   - Can query historical trades and orderbook snapshots

6. **Macro data** (FREE via yfinance or FRED)
   - DXY (dollar index), US10Y, SPX, VIX
   - Daily granularity sufficient
   - yfinance: `pip install yfinance`

7. **On-chain data** (FREE tier available)
   - Glassnode free tier: exchange inflows/outflows, active addresses
   - CryptoQuant free tier: exchange reserves, miner flows
   - Alternative: Dune Analytics (we have MCP tools for this!)

### Priority Order
1. Bybit funding/OI backfill (can do NOW, largest coverage gap)
2. Fear & Greed full history (5 min task)
3. Fix Coinglass collection
4. Macro data via yfinance
5. HL candle expansion
6. On-chain via Dune Analytics
7. Deribit options expansion

## Part 2: Statistical Framework

### Phase 0 Gates (before ANY code)

Every signal hypothesis must pass these BEFORE proceeding:

1. **Stationarity test** (ADF test, KPSS test) on the signal series
   - Non-stationary signals can't be z-scored meaningfully
   - Use fractional differentiation (Marcos Lopez de Prado) to achieve stationarity while preserving memory

2. **Information Coefficient (IC)** analysis
   - Rank correlation (Spearman) between signal and forward returns
   - IC must be > 0.02 with p < 0.01
   - IC decay curve across lags (how fast does predictive power decay?)

3. **Permutation test** (not just parametric t-test)
   - Shuffle signal timestamps 10,000 times
   - Real IC must exceed 99th percentile of shuffled distribution
   - Accounts for autocorrelation and non-normality

4. **Multiple hypothesis correction**
   - When testing N coins/pairs: Benjamini-Hochberg FDR control
   - When testing M holding periods: Bonferroni or Holm-Bonferroni
   - Report adjusted p-values, not raw

5. **Minimum data requirements**
   - >= 2 years of daily data OR >= 6 months of hourly data
   - >= 2 complete market regimes (bull + bear + ranging)
   - >= 100 non-overlapping signal occurrences
   - >= 30 non-overlapping trades per validation window

### Signal Quality Metrics (replace raw bps averages)

| Metric | What it measures | Minimum |
|--------|-----------------|---------|
| Information Coefficient (IC) | Rank correlation signal vs returns | > 0.02 |
| IC Information Ratio (ICIR) | IC / std(IC) — consistency | > 0.5 |
| Turnover-adjusted IC | IC accounting for trading frequency | > 0.01 |
| Deflated Sharpe Ratio | Sharpe adjusted for multiple testing (Bailey & Lopez de Prado) | > 1.5 |
| Probability of Backtest Overfitting (PBO) | CSCV method | < 0.5 |
| Maximum drawdown duration | Longest losing streak in time | < 90 days |

### Regime Detection (replace ad-hoc regimes)

Use Hidden Markov Model (HMM) with 2-3 states:
- Fit on BTC returns + volatility
- States emerge as: trending-up, trending-down, ranging
- All signal analysis MUST be conditioned on regime state
- Python: `hmmlearn` library

### Advanced Modeling Techniques

1. **Cointegration (Engle-Granger, Johansen)** for pairs/spread trading
   - Half-life of mean reversion via Ornstein-Uhlenbeck fit
   - Hurst exponent for mean-reversion strength (H < 0.5)

2. **Kalman Filter** for dynamic signal estimation
   - Adaptive z-score thresholds (not static)
   - Online estimation of signal mean and variance

3. **Factor decomposition** (PCA on returns)
   - Separate systematic risk from idiosyncratic signal
   - Market-neutral signals must have zero beta to PC1

4. **Entropy-based signal quality**
   - Transfer entropy: does signal carry information about future returns beyond what returns carry about themselves?
   - Mutual information: non-linear dependency measure

5. **Bayesian parameter estimation**
   - Prior on strategy parameters from economic reasoning
   - Posterior updates as data arrives
   - Credible intervals instead of point estimates

## Part 3: Research Skill Upgrade

The research-process skill needs these additions:

### New Phase -1: Data Readiness Check
Before any research begins:
- [ ] Signal data covers >= 2 years or >= 2 regimes
- [ ] Price data covers same period at required resolution
- [ ] No gaps > 24h in any series
- [ ] Stationarity confirmed (ADF p < 0.05)
- [ ] All data sources have coverage overlap

### New Phase 0: Rigorous EDA
Replace current Phase 0 with:
- [ ] Compute IC and ICIR across full sample
- [ ] Permutation test (10K shuffles, p < 0.01)
- [ ] IC by regime (HMM states)
- [ ] IC decay curve (lags 1h to 168h)
- [ ] Non-overlapping signal count >= 100
- [ ] Multiple hypothesis correction applied
- [ ] Report: IC, ICIR, permutation p-value, regime breakdown, adjusted p-values

### Upgraded Phase 1: Proper Backtesting
- [ ] Walk-forward with purged cross-validation (no lookahead)
- [ ] Embargo period between train/test (>= max holding period)
- [ ] Deflated Sharpe Ratio reported
- [ ] PBO (Probability of Backtest Overfitting) < 0.5
- [ ] Transaction cost sensitivity (0, 2, 5, 10 bps tiers)

## Part 4: Implementation Roadmap

### Week 1 (NOW)
- [ ] Build historical data backfill scripts (Bybit funding, OI)
- [ ] Backfill Fear & Greed full history
- [ ] Fix Coinglass collection task
- [ ] Install statistical libraries: `hmmlearn`, `arch`, `statsmodels`
- [ ] Build `app/research/statistical_tests.py` utility module

### Week 2
- [ ] Complete Bybit backfill (funding 2yr, OI 1yr)
- [ ] Add macro data collection (yfinance)
- [ ] Implement HMM regime detector
- [ ] Implement IC/ICIR/permutation test framework
- [ ] Re-test HL funding signal with proper methodology

### Week 3
- [ ] Re-test ALL existing strategies (X9, X14, X17) with new rigor
- [ ] Identify which strategies survive proper testing
- [ ] Cointegration analysis for pairs trading candidates
- [ ] Begin on-chain data collection via Dune

### Week 4
- [ ] New strategy candidates from rigorous EDA
- [ ] Factor model: decompose crypto returns into systematic factors
- [ ] Cross-exchange signal aggregation with proper weighting
