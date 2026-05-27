---
name: research-process
description: "HB-native strategy lifecycle: idea → self-contained controller → backtest → walk-forward → deploy. One controller, one signal path, same code for backtest and live. Load when designing, backtesting, or validating any strategy."
---

# Quant Research & Deployment Process (HB-Native)

## MANDATORY: Codex Loop Process (applies to ALL research and implementation)

Every strategy, every analysis, every build MUST follow this process. No exceptions.

### Phase A: Research Plan (before touching data)
1. Ask Codex for independent, unbiased research/data requirements plan
2. Loop with Codex until you reach consensus on what to analyze and how
3. Send plan to Alberto for approval
4. DO NOT touch data until Alberto approves

### Phase B: Technical Implementation Plan (after research results)
1. Plan the technical implementation, loop with Codex until agreement
2. Send implementation plan to Alberto for approval
3. DO NOT build until Alberto approves

### Phase C: Build (after implementation approved)
1. Build the implementation
2. Loop with Codex for adversarial review until no bugs remain
3. Ship

**NEVER skip phases. NEVER jump to analysis or code without the Codex loop.**
**"No bias no bias no bias" -- Codex provides the independent check on your thinking.**

---

**The controller IS the strategy.** One self-contained HB V2 controller runs in both
BacktestingEngine (validation) and HB bot container (live). No separate eval functions,
no custom signal pipelines, no breakout monitors.

```
scaffold → implement controller → register → backtest → walk-forward → deploy
python cli.py scaffold-strategy --name EN --display "..."
python cli.py trigger-task --task eN_bulk_backtest
python cli.py trigger-task --task eN_walk_forward
python cli.py deploy --engine EN
```

Controllers must be self-contained: only `hummingbot.*`, `pandas`, `pandas_ta`, `numpy`,
`pydantic`, stdlib. No `app.*` imports. Inline any helpers.

---

## Research Principles (applies to every phase)

**1. Separate signal edge vs execution edge.**
Signal = entry logic quality (R, PF, win rate). Execution = slippage, fill rate, latency. Never mix.

**2. Distribution over point metrics.**
Report full R distribution, not just mean/Sharpe. Highlight skew, tails, clustering. Call out sample size.

**3. Regime is first-class.**
Always explain results by regime (trend, range, shock). Never generalize without regime breakdown.

**4. Detect selection bias.**
If a filter reduces trades, check whether it removes winners disproportionately.
Example: E1's 5m post-close filter killed PF by selecting stalls and rejecting fast movers.

**5. Be explicit about unknowns.**
Call out where N < 30. Distinguish hypothesis vs validated finding.

**6. One variable at a time.**
No parallel engine development during active validation.

**7. Adversarial thinking.**
Always ask: "How could this result be misleading?" Check boundary effects, lookahead bias, sample leakage.

**8. Execution realism is mandatory.**
A strategy without executable edge is invalid. Backtest PF is not enough.

**9. Optimize for robustness, not peak.**
Prefer stable interior params over boundary solutions.

**10. Never build a custom backtester.**
Always use Hummingbot BacktestingEngine via QuantsLab. Custom loops overstate results (E2: custom PF 2.17 vs engine PF 1.36). Same code path for backtest and live is non-negotiable.

---

## Phase -1 -- Data Readiness (BEFORE any analysis)

No research begins until data passes these gates:

- [ ] Signal data covers >= 1 year OR >= 2 distinct market regimes (bull + bear + range)
- [ ] Price data covers same period at required resolution (1m for backtest, 1h for EDA)
- [ ] No gaps > 24h in any series (check with coverage audit script)
- [ ] Data source documented: exchange, endpoint, collection frequency, known limitations
- [ ] Cross-reference: if using HL data as signal for BB execution, confirm price correlation > 0.99
- [ ] Survivorship bias: only use pairs that existed at the START of the window, not just now
- [ ] Minimum assets: any cross-sectional signal needs >= 20 assets with full coverage

Data quality commands:
```python
# Coverage audit (run before ANY EDA)
from app.research.data_quality import audit_coverage
audit_coverage(collection="bybit_funding_rates", pair="BTC-USDT", expected_interval_h=8)

# Stationarity test (run on every signal series)
from app.research.statistical_tests import test_stationarity
test_stationarity(signal_series, method="adf+kpss")
```

**If data is insufficient: go GET more data first. Never approximate or work around gaps.**

---

## Phase 0 -- Idea Validation (rigorous EDA, before controller code)

### 0.1 Hypothesis definition
- Define the market hypothesis: what inefficiency are you exploiting?
- Define the economic mechanism: WHY does this edge exist? Who is on the other side?
- Define success criteria: Avg R > 0 AND Median R > 0
- Set walk-forward split BEFORE seeing any results:
  - **Train**: param tuning only -- must include the strategy's HOME REGIME
  - **Validation**: one-shot, locked after first use -- >= 4 months, >= 30 trades, >= 1 regime shift
  - **Out-of-sample**: never touch until deployment decision
- For range/mean-reversion strategies: train window MUST include chop periods
- Conditional strategies: hard-disable outside home regime in V1. Don't "let data decide."

### 0.2 Statistical signal validation (MANDATORY before proceeding)

Every signal hypothesis must pass ALL of these:

1. **Information Coefficient (IC)**: Spearman rank correlation between signal and forward returns
   - IC > 0.02 with p < 0.01 required
   - Compute IC Information Ratio (ICIR = mean(IC) / std(IC)) -- needs ICIR > 0.5
   - IC decay curve across lags 1h to 168h -- characterizes signal half-life

2. **Permutation test**: shuffle signal timestamps 10,000 times
   - Real IC must exceed 99th percentile of shuffled distribution
   - This catches autocorrelation artifacts that inflate parametric t-tests

3. **Multiple hypothesis correction**: when testing N coins or M holding periods
   - Benjamini-Hochberg FDR control (q < 0.05) for cross-sectional tests
   - Bonferroni for small number of independent tests

4. **Non-overlapping analysis**: ALWAYS compute signal quality on non-overlapping trades
   - Overlapping forward returns inflate sample size by hold_period/signal_freq
   - Apply cooldown >= max hold period between signal occurrences
   - Require >= 100 non-overlapping signals for any claim

5. **Regime conditioning**: use HMM (2-3 state) fitted on BTC returns + volatility
   - Report IC and trade stats SEPARATELY per regime state
   - A signal that only works in one regime is conditional -- gates required

6. **Stationarity**: ADF test (p < 0.05) on the signal series
   - Non-stationary signals: apply fractional differentiation (d ~ 0.3-0.5) to achieve stationarity while preserving memory

```python
# Standard EDA template
from app.research.statistical_tests import (
    compute_ic_analysis,      # IC, ICIR, decay curve
    permutation_test,         # 10K shuffles
    fdr_correction,           # Benjamini-Hochberg
    non_overlapping_signals,  # Apply cooldown
    regime_condition,          # HMM-based regime split
)
```

### 0.3 Report format for Phase 0

Every Phase 0 report MUST include:
- Hypothesis + economic mechanism
- Data coverage (dates, assets, gaps)
- IC and ICIR (with confidence interval)
- Permutation p-value
- Non-overlapping trade count and average return
- Regime breakdown (IC per state)
- Multiple hypothesis adjusted p-values (if testing multiple assets/periods)
- **Explicit statement of what could make this result misleading**

---

## Phase 1 -- Research (QuantsLab notebooks)

### 1.1 Baseline backtest
- Use QuantsLab `BacktestingEngine(load_cached_data=True)` -- fresh engine per run
- **E1 resolution**: `backtesting_resolution="5m"` (two-layer 1h+5m architecture needs 5m steps)
- **E2 resolution**: `backtesting_resolution="1h"` (single-layer 1h-only)
- Fee: `trade_cost=0.000375` (0.02% maker + 0.055% taker = 0.075% RT)
- ATR percentile: must use full historical feed (2160 bars = 90d x 24h), not windowed slice
- **E1 candles_config**: must include BOTH 1h and 5m CandlesConfig entries -- model_post_init only adds 1h
- Document all TODOs before declaring anything "done"

### 1.2 Regime stress tests (BEFORE optimization)
Run across 4 regime windows with identical params:
- Bear crash (sustained trend)
- Shock event (FTX-style, 1 month)
- Low-vol ranging
- Bull validation (locked)

Look for break conditions, NOT best windows.

### 1.3 Long/short asymmetry check
Split trade analysis by side before anything else:
- PF, mean R, win rate, PnL contribution -- separately
- If long >> short: implement directional filter before Optuna
- Run 4 tests: baseline (both) -> long-only -> short-only -> both with EMA gate for shorts
- Direction first, stops second -- changing stops blurs which side is broken

### 1.3b Discriminator analysis
When one side/regime underperforms, find what distinguishes winners from losers:
- Compare: range width/ATR, distance to midpoint/ATR, body/ATR, volume z-score, range expansion
- 30% separation = moderate discriminator, 40% = strong
- Goal: ONE clean filter, not multiple weak ones
- E2 lesson: range_expanding had 55% separation -- PF 0.999 -> 1.683 with one filter

### 1.3c Mini sensitivity check (BEFORE optimization, AFTER structural fixes)
- Perturb 2-3 key params +/- 1 step, all else locked
- If PF collapses on +/- 1 step -> fix is fragile
- Sequence: structural fix -> sensitivity check -> Phase 2 -> THEN Optuna

### 1.4 Parameter optimization (Optuna)
- Only after regime tests + Phase 2 pre-deployment checks
- Train window ONLY -- hard-coded dates
- 4 params max at once
- Custom objective: composite score (Sharpe + bonuses - DD penalty), not raw Sharpe
- Check boundary pinning, stability (top-10 clustering), trade count drift
- Duplicate suppression: hash param cache

### 1.5 Validation (ONE SHOT)
- Run locked validation window once
- Compare: train vs val Sharpe (delta > 0.3 = suspicious)
- Check: trade count drop (regime mismatch), time-in-market (identity drift)

---

## Phase 2 -- Pre-deployment checks

### P0 -- Degradation stress tests
**A1**: Enter on 2nd valid trigger, not 1st (5m resolution, not 1h delay)
**A2**: Slippage tiers: 2 bps, 5 bps, 10 bps
**Setup expiry sweep**: 1h / 2h / 4h
**Trade distribution**: max consecutive losses <= 3, top-5 contribution < 80%, R-multiple distribution

### P1 -- Production readiness
**Monte Carlo (block bootstrap)**: block 5-10 trades, 10k simulations, ruin < 1% at intended sizing
**Fee realism**: 0.000375 per side (maker in + taker out)
**Fill rate risk**: missed fills = selection bias. P3 must measure filled vs missed PnL.

---

## Phase 3 -- Paper Trading (Execution Validation ONLY)

P3 is NOT signal re-validation. Signal is already proven.
P3 answers: Can I get filled? At what slippage? Does execution drift kill edge?

### Log for every signal
- Trigger -> execution: timestamps, decision_price, fill_price, delay
- Slippage vs decision_price in bps: safe <10, borderline 10-20, danger >20
- Fill outcome: filled/not-filled, time-to-fill
- Would-have-won: hypothetical outcome even if not filled

### Hard stop conditions
```
avg slippage > 15 bps         -> PAUSE
>30% trades in danger bucket  -> PAUSE
fill rate < 70%               -> PAUSE
```

### Primary KPI: edge after slippage
`edge_after_slip = backtest_avg_R - (slippage_bps / sl_distance_bps)`
If negative -> not deployable.

---

## Key Lessons (learned the hard way)

1. Define the split before seeing results -- validation window is sacred
2. Don't optimize what you intend to validate
3. Asymmetric edge is structural, not sample noise -- always split by side
4. ATR lookback: 90 bars != 90 days. Use 2160 bars for 90d on 1h
5. Breakout = event, not state -- debounce or you get 60 signals per hour
6. 1h-close entry IS the edge for breakout strategies. Post-close 5m filter selects losers.
7. Never build a custom backtester -- same code path for backtest and live
8. Structural fixes before param optimization -- one filter > Optuna squeezing
9. Range expansion is the anti-transition filter for range-fade strategies
10. Conditional strategies need hard regime gates -- mean reversion in chop != everywhere
11. Entry quality gates can destroy edge -- E1's 5m gate rejected 98.6% of valid triggers. Disabled gate: 9x more signals, 6x more PnL, PF still robust at 4.0. Validate with funnel analysis before trusting any filter.
12. Live and backtest parameters MUST match -- different code paths drift silently. After validation, explicitly verify feature configs, signal scan thresholds, and engine params align with what was tested.
13. Feature store should store raw values, not pre-baked threshold flags -- changing a threshold shouldn't require recomputing all features across all pairs.
14. Use CLI scripts for long-running analysis, not notebooks -- faster iteration, no kernel state issues, output captured reliably.
15. Smoke test one pair before bulk runs -- catches formatting bugs, import errors, and data issues in 60 seconds instead of hours.