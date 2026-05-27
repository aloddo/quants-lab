# Hyperliquid Copy Trading Wallet Selection and Post-Validation Methodology

Date: 2026-05-25

Status: reset specification. This replaces the V13/V14/V14B design lineage as a clean methodology, while reusing existing data and backtest infrastructure where appropriate.

## 0. Objective

Design a wallet-selection and copy-trading framework for Hyperliquid that has:

1. Forward-tested statistical edge on copyable percent return, not historical dollar PnL.
2. Survival under realistic execution, fees, slippage, latency, position limits, and risk controls.
3. A validation process that rejects lottery portfolios where one wallet dominates or one wallet can destroy the book.
4. A deployment path from historical backtest to small live capital without pretending that paper fills are equivalent to real fills.

The output of the research pipeline is not "top wallets". The output is a reproducible policy:

```text
At selection time T, from only data available before T:
  build an eligible universe,
  rank wallets with a predeclared score,
  construct a capped portfolio,
  simulate executable copying rules,
  validate against null portfolios and concentration stress,
  deploy only if live attribution remains consistent with backtest expectations.
```

## 1. Thesis: What Wallet-Copy Alpha Can and Cannot Be

### 1.1 Core thesis

Wallet-copy alpha can exist if a subset of Hyperliquid wallets repeatedly expresses information or behavioral edge through trades that are:

1. Observable quickly enough from public fills or live feeds.
2. Copyable at small notional without large market impact.
3. Not purely explained by size, rebates, latency, market making, liquidation flow, or one-off lottery trades.
4. Persistent across adjacent time periods, but not necessarily continuously active.

The copy strategy is not trying to reproduce the wallet's full account. It is trying to extract the copyable component of its flow.

### 1.2 Non-thesis

The methodology explicitly rejects these assumptions:

1. High lifetime PnL means copyable alpha.
2. More active wallets are better sampling candidates.
3. A high composite score built from in-sample z-scores is evidence of edge.
4. A wallet with no historical losses is attractive.
5. Equal-weighting top-ranked wallets is safe.
6. Paper fills are a valid substitute for small live execution on HL.

### 1.3 Primary target variable

Rank and validate against forward executable percent return on allocated copy capital:

```text
forward_copy_return_pct = executable_net_pnl / allocated_copy_capital
```

Dollar PnL is secondary and must never drive ranking unless normalized by capital and exposure. Historical raw PnL is allowed only as a diagnostic feature after deconfounding for size.

## 2. Data Objects and PnL Semantics

### 2.1 Source data

Use the existing HL S3 fills archive as historical truth:

1. 174 days of fills.
2. Approximately 570M fills.
3. Approximately 306K wallets.
4. Per-fill wallet, coin, side, size, price, timestamp.

Use the V11 live copy-trader MongoDB collection `unified_copy_trades` as live execution truth for realized copy fills and per-wallet attribution.

### 2.2 Journey-level reconstruction

All wallet metrics that depend on trade outcome must use per-`(wallet, coin)` journey semantics, not naive fill aggregation.

Required journey fields:

1. `wallet`
2. `coin`
3. `start_ts`
4. `end_ts`
5. `direction`
6. `entry_notional`
7. `exit_notional`
8. `realized_pnl`
9. `return_pct_on_entry_notional`
10. `max_position_notional`
11. `hold_minutes`
12. `num_fills`
13. `fees_estimated`
14. `copyable_flag`

Existing `v13_journey_trace.py` should remain the canonical basis for journey PnL semantics.

### 2.3 Copyable return is not wallet return

For selection and validation, compute three layers:

1. Wallet-native outcome: what the wallet made.
2. Naive mirror outcome: what a copy trader would have made with immediate fixed-notional mirroring before risk controls.
3. Executable copy outcome: after latency, fees, slippage model, min order sizes, position caps, stop loss, trailing stop, and max hold.

Ranking features can use wallet-native history, but validation must use executable copy outcome.

## 3. Universe Definition

### 3.1 Sampling frame

Use a recent-plus-seasoned sampling frame, not lifetime active-days top-N.

At each selection timestamp `T`, candidate wallets are the union of:

1. Recent-active pool:
   - At least 8 active trading days in the last 30 calendar days.
   - At least 20 closed journeys in the last 60 calendar days.
   - At least $2,000 total traded notional in the last 60 calendar days.

2. Seasoned-intermittent pool:
   - At least 20 active trading days in the last 120 calendar days.
   - At least 40 closed journeys in the last 120 calendar days.
   - At least one active trading day in the last 21 calendar days.
   - At least $5,000 total traded notional in the last 120 calendar days.

3. Focused-specialist pool:
   - At least 25 closed journeys on one to three coins in the last 90 calendar days.
   - At least 6 active days in the last 45 calendar days.
   - Coin-level copyable-return metrics must be computed separately.

Reason: lifetime active days over-samples HFT/MM behavior and misses intermittent discretionary wallets. Recent-only misses good wallets that trade event-driven bursts. The union keeps both.

### 3.2 Exclusions before feature computation

Exclude wallets if any of the following hold in the train window:

1. Median journey hold time below 90 seconds and journey count above 200.
2. More than 65% of journeys round-trip within 5 minutes and gross notional above $100,000.
3. Both-side same-coin flip rate above 0.45 within 10 minutes.
4. Median absolute journey return below 3 bps with high journey count above 200.
5. More than 30% of fills occur in bursts of at least 10 fills within 2 seconds on the same coin.
6. More than 70% of volume is in obvious basis/arbitrage pairs if cross-exchange or funding signals later identify them.
7. Wallet has fewer than 8 losing journeys unless total journeys are below 20, in which case it is already ineligible.
8. Any single journey contributes more than 60% of train net PnL.
9. Any single day contributes more than 60% of train net PnL.
10. More than 50% of train volume is in coins that are not copyable at the intended fixed notional due to spread, depth, or min order constraints.

Reason: these remove HFT/MM contamination, lucky samples, dust accounts, and lottery outcomes before the ranking model sees them.

### 3.3 Inclusion of quiet but good wallets

Do not require continuous activity. Require recent evidence of being alive plus enough closed journeys in a trailing longer window.

Use an activity recency multiplier only in portfolio construction, not as a hard ranking feature:

```text
activity_multiplier =
  1.00 if last_active_days_ago <= 3
  0.75 if 4 <= last_active_days_ago <= 7
  0.50 if 8 <= last_active_days_ago <= 14
  0.25 if 15 <= last_active_days_ago <= 21
  0.00 otherwise
```

This prevents stale wallets from occupying capital while preserving intermittent alpha candidates for future refreshes.

### 3.4 Refresh cadence

Universe and ranking refresh cadence:

1. Historical walk-forward: once per fold at the fold selection timestamp.
2. Production candidate refresh: weekly.
3. Full research re-fit: monthly, only after a frozen evaluation report.
4. Emergency removal: immediate if live kill rules trigger.

Weekly refresh is fast enough to avoid dead wallets but slow enough to reduce churn and data-snooping.

## 4. Eligibility Gates

Eligibility gates must answer: "Does this wallet have interpretable, copyable behavior?" They must not answer: "Is this wallet good?"

### 4.1 Minimum statistical mass

Per train window:

1. `closed_journeys >= 25`
2. `losing_journeys >= 8`
3. `active_days >= 8`
4. `gross_notional >= $2,000`
5. `median_journey_notional >= $20`
6. `median_hold_minutes >= 1.5`
7. `p95_hold_days <= 14`

Rationale: require enough trades and losses to estimate behavior, but avoid eliminating swing traders.

### 4.2 Real-trader signature

Pass all:

1. Loss rate between 10% and 75%.
2. Median journey return absolute value at least 5 bps.
3. Position size dispersion is bounded:
   - `p95_journey_notional / median_journey_notional <= 30`
   - `max_journey_notional / median_journey_notional <= 100`
4. No single coin contributes more than 80% of closed journeys unless wallet is classified as focused-specialist.
5. For focused-specialist wallets, no single journey may contribute more than 35% of train net PnL.

Rationale: real traders lose sometimes, have nonzero directional exposure, and show some size discipline.

### 4.3 Market-maker/HFT rejection

Reject if two or more are true:

1. Median hold below 3 minutes.
2. Journey count above 300 in 60 days.
3. Same-coin direction flip rate above 0.35 within 15 minutes.
4. Average fills per journey above 12 with low median return below 10 bps.
5. Gross volume to absolute net position change ratio above 15.
6. Win rate above 75% with median win below 8 bps.

Rationale: market makers can have attractive in-sample stats but are not copyable with fixed delayed taker execution.

### 4.4 Manipulator and ratcheted-equity rejection

Reject if:

1. Train PnL is dominated by one isolated event:
   - top one journey > 40% of net PnL, or
   - top three journeys > 70% of net PnL.
2. Return distribution has extreme downside:
   - worst journey return < -250%, or
   - worst 5% average journey return < -80%.
3. Wallet frequently doubles down:
   - losing-side add rate above 0.55 after adverse move of 1%.
4. Wallet has liquidation-like exits in more than 5% of journeys.

Rationale: V14B showed that one -958% wallet can erase the portfolio. Such wallets are not acceptable even if their rank score is high.

### 4.5 Eligibility output

Each wallet receives:

1. `eligible_bool`
2. `exclusion_reason`
3. `wallet_type`: `swing`, `intraday`, `focused_specialist`, `unknown`
4. `copyability_tier`: `A`, `B`, `C`, `reject`

Only `A` and `B` wallets are rankable. `C` wallets can be monitored but not deployed.

## 5. Ranking Score

### 5.1 Ranking target

Rank wallets by expected forward executable copy return per unit of allocated capital, with bounded downside.

Primary label during research:

```text
test_median_copy_return_pct
```

Secondary labels:

1. `test_mean_copy_return_pct`
2. `test_net_pnl_per_allocated_capital`
3. `test_max_drawdown_pct`
4. `test_tail_loss_p05`
5. `test_trade_participation_rate`

Use median as primary because mean is too sensitive to one wallet or one journey. The deployed portfolio still optimizes total net PnL after risk caps.

### 5.2 Features that should be eligible for ranking

Feature families:

1. Directional hit quality:
   - journey win rate after fees.
   - median journey return pct.
   - fraction of active days with positive copyable PnL.
   - positive-day persistence over rolling 7-day blocks.

2. Payoff shape:
   - profit factor capped at 5.
   - median win / median loss absolute ratio.
   - p25 journey return.
   - worst 5% journey return.

3. Copyability:
   - median hold time.
   - p25 hold time.
   - average spread/depth cost on traded coins.
   - fraction of journeys above intended min notional.
   - delayed-entry degradation at 5s, 15s, and 60s where reconstructable.

4. Stability:
   - active-day count.
   - effective number of profitable days.
   - effective number of coins.
   - top-journey PnL share.
   - top-coin PnL share.

5. Anti-lottery:
   - concentration penalty.
   - downside convexity penalty.
   - loss streak severity.
   - max adverse excursion where reconstructable.

6. Recentness:
   - last active age.
   - rolling 14-day degradation from 60-day baseline.

### 5.3 Ranking score form

Do not use a broad additive z-score across many attractive-looking metrics.

Use a simple, predeclared, monotonic score with caps and penalties:

```text
base_edge =
  0.45 * rank_pct(win_rate_after_fees)
  + 0.25 * rank_pct(median_journey_return_pct)
  + 0.15 * rank_pct(positive_active_day_rate)
  + 0.15 * rank_pct(capped_profit_factor)

copyability =
  min(1.0, median_hold_minutes / 30.0)
  * min(1.0, p25_hold_minutes / 5.0)
  * copyable_coin_fraction

stability =
  sqrt(min(1.0, closed_journeys / 100.0))
  * sqrt(min(1.0, active_days / 30.0))

penalty =
  concentration_penalty
  * tail_loss_penalty
  * stale_activity_penalty
  * hft_similarity_penalty

rank_score = base_edge * copyability * stability * penalty
```

Default penalties:

```text
concentration_penalty = 1 - max(0, top_1_journey_pnl_share - 0.20) / 0.40
tail_loss_penalty = 1 - max(0, abs(worst_journey_return_pct) - 0.50) / 1.50
stale_activity_penalty = activity_multiplier
hft_similarity_penalty = 0.5 if exactly one HFT rejection condition is true, else 1.0
```

All penalties are clipped to `[0, 1]`.

Reason: V14B found `win_rate` and median percent-style scores carried signal. The score should preserve that signal while directly suppressing the failure mode that destroyed the equal-weight copy sim.

### 5.4 Single score vs multiple features

Use one primary predeclared score for selection. Do not select by manually comparing many feature leaderboards after seeing outcomes.

However, validate the score in three slices:

1. Intraday wallets: median hold 5 minutes to 6 hours.
2. Swing wallets: median hold 6 hours to 7 days.
3. Focused specialists: effective coin count below 2.5.

If a slice has positive IC but negative executable PnL, it can remain a research signal but cannot receive capital.

Do not deploy separate category scores until each category passes the same null and survival tests independently. Category-specific scores are a second-generation extension, not part of the first reset.

### 5.5 Ranking output

Per fold and production refresh, output:

1. Top eligible wallets by `rank_score`.
2. Feature table.
3. Exclusion table.
4. Score decile forward performance.
5. Concentration diagnostics before portfolio construction.

## 6. Portfolio Construction

Ranking wallets is not portfolio construction. The portfolio must be robust to one wallet being wrong.

### 6.1 Default construction

Use capped rank-weighted sleeves:

1. Select top `K = 25` eligible wallets by rank score.
2. Minimum `K = 15`; otherwise the fold/stage fails.
3. Wallet sleeve weight:

```text
raw_weight_i = rank_score_i
weight_i = min(raw_weight_i / sum(raw_weight), 0.08)
renormalize after caps
```

4. Hard max wallet capital share: 8%.
5. Hard max coin exposure share: 25% of current equity.
6. Hard max correlated wallet cluster share: 20% if clustering is available.

Reason: V14B equal-weight failed because the portfolio was not tail-robust. Rank weighting is acceptable only with hard caps.

### 6.2 Wallet removal and replacement

If a wallet becomes inactive or hits a kill rule, its sleeve goes to cash until the next weekly refresh. Do not immediately replace intraperiod unless fewer than 12 active wallets remain.

Reason: immediate replacement creates hidden high-turnover selection and data-snooping.

### 6.3 Portfolio null benchmark

Every selected portfolio must be compared to random eligible portfolios matched on:

1. Number of wallets.
2. Wallet type mix.
3. Activity recency bucket.
4. Median hold bucket.
5. Gross notional bucket.

This prevents the result from merely being "eligible wallets are better than trash".

## 7. Execution Model

### 7.1 Execution model to validate first

Primary model: V11-style fixed-notional mirror with per-wallet sleeves and risk controls.

Default initial parameters:

1. Base copy notional: `$11` per copied entry at small live capital.
2. Per-wallet sleeve capital: portfolio equity times wallet weight.
3. Max open notional per wallet: `min(3 * sleeve_capital, sleeve_capital + unrealized_pnl_cap)`.
4. Max open notional per coin: 25% of total equity.
5. Entry delay scenarios: 0s, 5s, 15s, 60s.
6. Fees: taker fees plus conservative slippage.
7. Ignore wallet trades below executable min notional.

Reason: this is closest to actual V11 live mechanics and produces real per-trade attribution. It preserves wallet alpha better than consensus aggregation if the alpha is wallet-specific timing and sizing.

### 7.2 Risk controls

Risk controls are mandatory, not optional overlays.

Default controls to validate:

1. Stop loss:
   - `sl_bps = 150` for intraday wallets.
   - `sl_bps = 250` for swing wallets.
   - `sl_bps = 100` for focused specialists on illiquid coins.

2. Trailing stop:
   - activate after +100 bps unrealized.
   - trail distance 80 bps intraday, 150 bps swing.

3. Max hold:
   - intraday: 24 hours.
   - swing: 7 days.
   - focused specialist: category-specific, capped at 72 hours unless historical p75 hold is longer and tail risk passes.

4. Per-wallet daily loss stop:
   - stop copying wallet for the day at `-2.0%` portfolio equity or `-25%` of sleeve capital, whichever is smaller.

5. Portfolio daily loss stop:
   - stop new entries at `-3.0%` daily equity.
   - flatten all non-core copied positions at `-5.0%` daily equity.

6. Catastrophic journey guard:
   - no copied position may lose more than `4x` its intended fixed notional without forced exit.

Reason: the V14B failure was a tail event, not lack of average signal. The execution layer must truncate tails or the selection edge is irrelevant.

### 7.3 Consensus aggregation

Consensus by coin across top wallets is not the primary model. It should be tested as a secondary model only.

Consensus model:

1. For each coin, aggregate signed desired exposure across selected wallets.
2. Trade only if at least 3 wallets agree or aggregate score-weighted exposure exceeds threshold.
3. Size by capped aggregate conviction.

Use consensus only if it improves:

1. Net executable return.
2. Max drawdown.
3. Top-wallet removal survival.
4. Turnover after fees.

Reason: consensus may reduce idiosyncratic blowups, but it can erase the timing edge of specialists and create crowded delayed entries.

### 7.4 Model selection rule

Select the execution model by frozen walk-forward results, not preference:

1. Primary fixed-notional mirror wins if it passes all validation gates.
2. Consensus can replace it only if it beats fixed-notional mirror on pooled return and drawdown while passing the same null tests.
3. If neither passes, do not deploy.

## 8. Walk-Forward Validation

### 8.1 Fold design

Use non-overlapping test folds with rolling train windows.

Default:

1. Train window: 60 days.
2. Test window: 14 days.
3. Step size: 14 days.
4. Minimum folds: 6 if data allows.
5. Preferred folds: 8 or more as archive grows.
6. Embargo: 1 day between train and test to avoid boundary leakage.

Reason: 14-day tests are long enough for wallets to trade and short enough for selection decay to be visible. Non-overlap prevents inflated significance.

### 8.2 Per-fold outputs

Each fold must produce:

1. Eligible universe size.
2. Selected wallet list and weights.
3. Rank-score IC against forward labels.
4. Score decile performance.
5. Executable copy simulation results.
6. Random portfolio null distribution.
7. Remove-top-1 and remove-top-3 results.
8. Worst-wallet and best-wallet attribution.
9. Coin concentration.
10. Wallet-type attribution.

### 8.3 Per-fold pass criteria

Per-fold criteria are diagnostics, not hard all-or-nothing gates.

Flag a fold as healthy if:

1. Eligible universe size >= 500.
2. Selected active wallets in test >= 12.
3. Spearman IC of `rank_score` vs `test_median_copy_return_pct` > 0.
4. Selected portfolio net return > median matched-random portfolio.
5. Remove-top-1 return is not worse than `-5%`.
6. No single wallet contributes more than 35% of positive PnL.

Do not require every fold to be positive. That was a V13-style overconstraint risk. Instead require pooled survival.

### 8.4 Aggregate pass criteria

A methodology passes historical validation only if all are true:

1. Pooled selected portfolio net return > 0 after fees and slippage.
2. Pooled selected portfolio beats matched-random median by at least 1.5x.
3. Pooled selected portfolio is above the 90th percentile of matched-random portfolios.
4. Bootstrap probability that pooled return <= 0 is below 5%.
5. Median fold return > 0.
6. At least 60% of folds beat matched-random median.
7. Remove-top-1 pooled return > 0.
8. Remove-top-3 pooled return > 0 or max drawdown improves enough that risk-adjusted return remains above random 75th percentile.
9. No single wallet contributes more than 25% of pooled net PnL.
10. No single coin contributes more than 35% of pooled net PnL.
11. Worst selected wallet cannot lose more than 1.5x the median positive wallet contribution after risk controls.
12. Aggregate Spearman IC > 0.10 with p < 0.01 across fold-level wallet observations.

Reason: the system must make money, beat a fair null, and survive concentration stress.

### 8.5 Random portfolio null

Run at least 1,000 matched-random trials for final reports. Use 200 trials only for fast iteration.

Each trial:

1. Samples `K` wallets from the eligible universe after all eligibility gates.
2. Matches selected portfolio bucket mix.
3. Applies the same construction, caps, execution model, and risk controls.
4. Computes the same metrics.

Report:

1. Percentile rank of selected portfolio.
2. Random median, p75, p90, p95.
3. Probability random portfolio beats selected.
4. Probability random portfolio loses money.
5. Probability selected portfolio result is explained by one wallet under same concentration rules.

### 8.6 IC tests

Compute rank IC at wallet level per fold:

1. Spearman IC between rank score and forward median copy return.
2. Spearman IC between rank score and forward mean copy return.
3. Spearman IC after winsorizing forward returns at 1st/99th percentile.
4. IC by wallet type.
5. IC after removing top and bottom 1% outcomes.

Pass condition is aggregate IC, not every-fold IC.

### 8.7 Anti-snooping protocol

Before running a final validation:

1. Freeze universe rules.
2. Freeze eligibility gates.
3. Freeze ranking formula.
4. Freeze execution parameters.
5. Freeze fold boundaries.
6. Freeze pass/fail criteria.

Any change after seeing final results invalidates that final run and requires a new holdout or a new future live period.

## 9. Post-Validation Beyond Historical Backtest

### 9.1 Historical validation is necessary but insufficient

Historical fills do not prove:

1. Live feed latency.
2. Order acknowledgement latency.
3. Real slippage at copy notional.
4. Missed fills.
5. Position state errors.
6. Wallet behavior changes after selection.

Therefore live small-capital validation is mandatory.

### 9.2 V11 live result interpretation

The current V11 live result is a small-capital live experiment, not a validated strategy:

1. +$43 over 11 days is useful execution evidence.
2. It is not statistically meaningful by itself.
3. One of 19 wallets dominating is a concentration warning.
4. 8 positive, 7 negative, 1 zero out of 16 traded wallets is not enough for strong inference.

Use V11 data to calibrate execution costs and operational failure modes, not to justify scale.

### 9.3 Required post-validation report

After each live stage, report:

1. Real net PnL.
2. Real return on allocated capital.
3. Fill participation rate.
4. Median entry latency.
5. Median slippage vs signal price.
6. Fees as percent of gross PnL.
7. Per-wallet attribution.
8. Top-1 and top-3 wallet contribution.
9. Worst wallet loss.
10. Strategy PnL with each wallet removed.
11. Comparison against historical expectation for same selected wallets.
12. Decision: continue, reduce, modify research only, or kill.

## 10. Deployment Process

### 10.1 Stage 0: Frozen historical backtest

Capital: none.

Duration: all available historical folds.

Pass if all aggregate criteria in Section 8.4 pass.

Fail if:

1. Pooled return <= 0.
2. Selected portfolio below random p90.
3. Remove-top-1 or remove-top-3 fails survival.
4. Concentration caps are breached.
5. Any rule was changed after viewing final outcomes.

### 10.2 Stage 1: Small live execution probe

Capital: $100 to $300.

Duration: minimum 14 calendar days or 100 copied trades, whichever comes later.

Purpose:

1. Verify infrastructure.
2. Measure real latency, slippage, and fees.
3. Confirm kill switches.
4. Confirm per-wallet attribution.

Pass if:

1. No position accounting errors.
2. Fill participation >= 80% of intended executable signals.
3. Median slippage does not exceed backtest assumption by more than 50%.
4. Daily loss and wallet loss kill switches function.
5. Net PnL is not worse than expected 10th percentile historical live-sim outcome.

Net profit is desirable but not required at this stage.

### 10.3 Stage 2: Small live validation

Capital: $500 to $1,000.

Duration: minimum 30 calendar days.

Pass if:

1. Net return > 0 after fees.
2. Wallet hit rate is directionally consistent with backtest.
3. At least 12 wallets trade.
4. Top wallet contributes <= 35% of gross positive PnL.
5. Remove-top-1 realized PnL remains >= 0.
6. Maximum drawdown is within historical p90 drawdown.
7. No operational incident causes more than 0.5% equity loss.

Fail if any catastrophic journey guard triggers twice from the same wallet or if live slippage invalidates backtest assumptions.

### 10.4 Stage 3: Controlled scale

Capital: $1,000 to $5,000.

Duration: rolling 30-day reviews.

Pass to remain active if:

1. Rolling 30-day return > 0 or within expected drawdown while IC remains positive.
2. Top-3 wallets contribute <= 60% of PnL.
3. No single wallet drawdown exceeds sleeve loss cap.
4. Slippage remains within model.
5. Capacity checks pass on traded coins.

Scale only by increasing number of wallets and sleeve capital gradually. Do not simply increase fixed copy notional on all signals.

### 10.5 Stage 4: Production

Capital: above $5,000 only after two consecutive profitable 30-day controlled-scale windows and concentration survival.

Production constraints:

1. Weekly selection refresh.
2. Daily risk report.
3. Automatic wallet disable.
4. Manual review for any new wallet before first live trade if its historical worst loss exceeds 100%.
5. No wallet over 8% capital.
6. No coin over 25% exposure.
7. No strategy variant deployed unless it has its own frozen validation.

## 11. Kill Switches

### 11.1 Wallet-level kill switches

Disable a wallet immediately if:

1. Realized sleeve loss exceeds 25% of wallet sleeve.
2. Daily wallet loss exceeds 10% of sleeve.
3. It triggers catastrophic journey guard once.
4. It opens a position in a coin outside the allowed liquidity set.
5. It opens directionally opposite trades rapidly enough to resemble rejected HFT/MM behavior.
6. Its live slippage is more than 2x expected on three consecutive trades.

Wallet remains disabled until next weekly review. Re-enable only if the reason was operational and resolved.

### 11.2 Portfolio-level kill switches

Stop new entries if:

1. Daily equity drawdown reaches 3%.
2. Rolling 7-day drawdown reaches 8%.
3. Live fill participation falls below 60% for more than 6 hours.
4. Price data or position reconciliation is stale by more than 60 seconds.
5. MongoDB attribution or exchange state reconciliation fails.

Flatten non-core copied positions if:

1. Daily equity drawdown reaches 5%.
2. Position state is uncertain.
3. Exchange API errors prevent exits.
4. Total gross exposure exceeds configured cap.

### 11.3 Research kill switch

Stop the research path and do not deploy if the only profitable configurations require:

1. A single wallet.
2. A single coin.
3. A single fold.
4. Ignoring fees.
5. Removing a catastrophic loser after seeing test outcomes.
6. Post-hoc gates based on V11 wallet membership.

## 12. Expected Return and Capacity

### 12.1 Realistic target

The V11 result of about +7.6% in 11 days is not a realistic base-rate expectation. Annualizing it is misleading because:

1. The sample is too small.
2. One wallet dominates.
3. Selection was manual.
4. The live period may have been favorable.
5. Small fixed notional can benefit from noise that does not scale.

Initial realistic target after validation:

1. Research hurdle: 3% to 8% net monthly in historical executable simulation with drawdown below 10%.
2. Small live hurdle: 1% to 5% net monthly after fees.
3. Production expectation: 2% to 6% net monthly if edge survives; assume decay until proven otherwise.

Any backtest showing 20%+ monthly should be treated as suspect until concentration, tail, and null tests explain it.

### 12.2 Fee and slippage budget

The strategy must remain positive after:

1. Taker fees on entries and exits.
2. Conservative spread crossing.
3. Entry delay degradation.
4. Missed trades.
5. Forced exit costs from stops.

Minimum gross edge threshold:

```text
expected_gross_edge_per_trade >= 3 * expected_round_trip_cost
```

If median copied trade edge is not at least 3x round-trip cost, the strategy is too fragile.

### 12.3 Capacity

Capacity is bounded by:

1. Coin depth on HL.
2. Wallet trade frequency.
3. Copy notional relative to signal trade notional.
4. Slippage on exits during stops.
5. Number of eligible active wallets.

Initial capacity assumptions:

1. $100 to $1,000: likely execution-capacity unconstrained.
2. $1,000 to $10,000: feasible only with caps, multiple wallets, and liquid coin filters.
3. $10,000 to $50,000: requires capacity model by coin and dynamic sizing.
4. Above $50,000: do not assume this methodology scales without becoming a different strategy.

Copy notional should never exceed:

1. 10% of the source wallet's observed trade notional for that fill or journey, if inferable.
2. 1% of top-of-book plus near-book depth available at expected entry.
3. The per-coin exposure cap.

## 13. Implementation Plan

### 13.1 Reuse existing infrastructure

Use:

1. `scripts/v13_universe_rebuild.py` for forward-only candidate pool building, modified to implement Section 3.
2. `scripts/v13_journey_trace.py` for journey reconstruction and PnL semantics.
3. `scripts/v13_wallet_metrics.py` for feature generation, replacing the broken z-score composite.
4. `scripts/v13_walk_forward.py` for parallel walk-forward, checkpoints, resume, and anchor cache.
5. `scripts/v13_reconstruct_1m_candles_from_fills.py` for delay/slippage proxies where needed.
6. V11 live copy-trader MongoDB `unified_copy_trades` for live attribution.

### 13.2 New modules or outputs

Add or refactor into:

1. `wallet_eligibility.py`
   - Implements Section 4 gates.
   - Emits exclusion reasons.

2. `wallet_rank_score.py`
   - Implements the frozen rank score.
   - No access to test labels.

3. `copy_execution_sim.py`
   - Implements fixed-notional mirror, sleeves, stops, trailing stops, max hold, fees, slippage.

4. `portfolio_constructor.py`
   - Rank-weighted sleeves, caps, wallet removal, coin caps.

5. `random_portfolio_null.py`
   - Matched random trials.

6. `validation_report.py`
   - Produces fold and pooled reports with pass/fail.

### 13.3 Required report files per run

Each frozen run must write:

1. `config.json`
2. `folds.csv`
3. `eligible_wallets.parquet`
4. `excluded_wallets.parquet`
5. `selected_wallets.parquet`
6. `wallet_features.parquet`
7. `fold_results.csv`
8. `pooled_results.json`
9. `random_null_results.parquet`
10. `concentration_report.json`
11. `ic_report.csv`
12. `decision.md`

The `config.json` hash must be included in every output file metadata where feasible.

## 14. Acceptance Criteria for the Reset

This reset is successful only if implementation produces a frozen report where:

1. Selection rules are fully forward-only.
2. Eligibility gates remove HFT/MM/lucky/tail-unsafe wallets before ranking.
3. Ranking score has positive aggregate IC on copyable percent-return labels.
4. Executable portfolio net return is positive after costs.
5. Portfolio beats matched-random p90.
6. Remove-top-1 and remove-top-3 tests survive.
7. Live small-capital execution costs fit backtest assumptions.
8. Scaling is blocked unless live attribution remains diversified.

If the methodology fails these conditions, the correct conclusion is not "tune V14C". The correct conclusion is that the current wallet-copy thesis is unproven at the tested horizon and execution style.

## 15. Immediate Next Steps

1. Freeze this document as methodology v1.
2. Implement eligibility gates separately from ranking.
3. Replace additive z-score composite with the predeclared capped rank score.
4. Add matched-random portfolio null to walk-forward.
5. Add remove-top-1/top-3 survival to validation.
6. Run a fast 200-trial null over all available folds for debugging only.
7. Freeze config and run the 1,000-trial final validation.
8. Only if historical validation passes, map the selected wallets into the V11 live execution stack for a small live execution probe.
