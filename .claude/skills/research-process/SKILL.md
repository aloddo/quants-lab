---
name: research-process
description: "Repeatable research and deployment process for quant strategy development. Covers Phase 0 (idea) through Phase 4 (paper trading). Built from E1/E2 development sessions. Load when designing, backtesting, or validating any strategy."
---

# Quant Research & Deployment Process

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

## Phase 0 -- Idea Validation (before any code)

- Define the market hypothesis: what inefficiency are you exploiting?
- Define success criteria: Avg R > 0 AND Median R > 0
- Set walk-forward split BEFORE seeing any results:
  - **Train**: param tuning only -- must include the strategy's HOME REGIME
  - **Validation**: one-shot, locked after first use -- >= 4 months, >= 30 trades, >= 1 regime shift
  - **Out-of-sample**: never touch until deployment decision
- For range/mean-reversion strategies: train window MUST include chop periods
- Conditional strategies: hard-disable outside home regime in V1. Don't "let data decide."

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