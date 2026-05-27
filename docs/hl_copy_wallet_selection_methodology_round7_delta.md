# Hyperliquid Copy Trading Methodology Round 7 Delta

Date: 2026-05-25

Status: revision delta against `docs/hl_copy_wallet_selection_methodology.md`.

## Executive Reframe

Round 6 was directionally right on rigor, but wrong in four ways:

1. It confused "do not over-engineer" with "be conservative." The goal is a crypto hedge fund strategy, not a small hobby bot.
2. It pre-rejected some high-frequency wallets before measuring whether their alpha is actually copyable at Hyperliquid's practical latency.
3. It treated multi-wallet same-coin collisions as a secondary concern. That is a first-order portfolio construction problem.
4. It set return targets too low before the evidence demands that. If the data supports 10% to 30% monthly net with survival, the methodology must allow it.

The revised stance:

```text
Be ambitious on alpha.
Be ruthless on evidence.
Simplify implementation, not validation.
Do not ban high Sharpe or high trade count.
Make copyability, concentration, and collision handling empirical.
```

## 1. Methodology Deltas vs Round 6

### Replace Section 3.2 and 4.3: no arbitrary HFT rejection

Remove these as hard exclusion logic:

1. `median_hold < 90s and journey_count > 200`
2. `fills_per_day` or equivalent trade-count ceilings
3. HFT/MM rejection solely from high journey count, high fill count, or short hold time

Replace with:

```text
candidate_fast_wallet = true if wallet has:
  median_hold_seconds < 180
  or p50_entry_to_exit_seconds < 300
  or fills_per_active_day > fast_wallet_p75
  or journey_count_60d > 300

Fast wallets are not rejected.
They are routed to the empirical copyability test.
```

Reject only if the wallet fails copyability after costs:

```text
copyable_alpha_ratio_1s < 0.30
or executable_net_return_1s <= 0
or fee_share_of_gross_edge_1s > 0.60
or latency_edge_decay_slope is catastrophically steep
```

The methodology must learn whether fast wallets are copyable instead of assuming they are not.

### Replace Section 5.1: target label

Keep forward executable percent return as the main label, but add capacity-aware alpha as a co-primary diagnostic:

```text
primary_label = forward_executable_return_pct_at_target_latency
secondary_label = forward_executable_pnl_after_costs_at_capacity_bucket
diagnostic_label = copyable_alpha_ratio_by_latency
```

This avoids ranking wallets that produce high percent returns only at dust capacity.

### Replace Section 6 and 7.3: portfolio construction must be coin-state aware

Round 6's capped sleeves are not sufficient because 50 wallets can all express the same BTC long or BTC short. Portfolio state must be built from wallet signals projected into `(coin, time)` exposure.

Every execution model must emit a desired signed exposure:

```text
s_i,c,t = wallet i's desired signed exposure on coin c at time t
          long positive, short negative, flat zero

w_i = current portfolio weight assigned to wallet i
r_i = normalized rank score in [0, 1]
q_i,c,t = copyability-adjusted desired notional from wallet i on coin c at time t
```

Then portfolio construction resolves collisions before orders are sent.

### Replace Section 10: validation stages are evidence gates, not capital ladders

Remove fixed capital floors:

1. `$100-$300`
2. `$500-$1,000`
3. `$1,000-$5,000`
4. `above $5,000`

Replace with validation stages:

```text
Stage 0: historical frozen validation
Stage 1: live execution fidelity
Stage 2: live alpha validation
Stage 3: live capacity validation
Stage 4: fund-grade operating process
```

Capital is chosen only to make the measurement valid:

```text
stage_capital = max(
  exchange_minimum_required_to_execute_signals,
  capital_needed_for intended_notional without dust rejections,
  capital_needed_for risk caps to work,
  minimum capital where fees/slippage measurement is representative
)
```

No stage passes because capital was small or large. It passes because the measurement answers the stage question.

### Replace Section 12: return ambition

Remove "production expectation: 2% to 6% monthly."

Replace with:

```text
Research objective: find a strategy that can underwrite 10% to 30% net monthly at small-to-medium capacity.
Deployment hurdle: positive live net return with diversified attribution and execution fidelity.
Fundraising hurdle: repeatable 10%+ net monthly expectation, with drawdown, capacity, and decay evidence.
```

High returns are allowed only if they survive:

1. fees and slippage,
2. 1s to 60s latency degradation,
3. matched-random nulls,
4. remove-top-wallet tests,
5. remove-top-coin tests,
6. multi-wallet same-coin collision tests,
7. live execution fidelity,
8. capacity scaling diagnostics.

Do not cap the upside in the methodology. Falsify it empirically.

## 2. Multi-Wallet-Per-Coin Aggregation Models

At time `t`, for coin `c`, wallet `i` has target signed unit exposure:

```text
x_i,c,t in {-1, 0, +1}
```

or signed notional:

```text
s_i,c,t = x_i,c,t * n_i,c,t
```

where:

```text
n_i,c,t = min(
  wallet_sleeve_notional_i,
  copy_notional_cap_i,
  source_trade_fraction_cap * source_trade_notional_i,c,t,
  coin_depth_cap_c,t
)
```

Define normalized wallet quality:

```text
a_i = normalized_rank_score_i * copyability_score_i * activity_multiplier_i
```

Define total positive and negative vote mass:

```text
L_c,t = sum_i a_i * max(s_i,c,t, 0)
S_c,t = sum_i a_i * max(-s_i,c,t, 0)
G_c,t = L_c,t + S_c,t
N_c,t = L_c,t - S_c,t
consensus_c,t = abs(N_c,t) / max(G_c,t, epsilon)
```

### Option A: Net Consensus

```text
target_coin_exposure_c,t = clip(N_c,t, -coin_cap_c, +coin_cap_c)
```

Strengths:

1. Simple.
2. Avoids holding long and short on the same coin.
3. Reduces duplicate exposure when many wallets pile into the same trade.

Weaknesses:

1. A strong specialist can be canceled by mediocre opposite wallets.
2. It can erase wallet-level timing edge.
3. It turns copy trading into crowd sentiment, which may be worse than the best wallet.

Use as baseline, not first deployment.

### Option B: Per-Wallet Sleeves

Each wallet is independently simulated:

```text
target_i,c,t = clip(s_i,c,t, -wallet_coin_cap_i,c, +wallet_coin_cap_i,c)
portfolio_exposure_c,t = sum_i target_i,c,t
```

The exchange position is the net:

```text
exchange_net_c,t = sum_i target_i,c,t
```

but accounting remains sleeve-level:

```text
pnl_i,c,t = attributed by virtual sleeve state, entry price, exit price, fees, and funding
```

Strengths:

1. Preserves wallet timing and specialist alpha.
2. Makes attribution clean.
3. Closest to V11 live mechanics.
4. Allows long and short sleeves to offset exchange exposure while preserving independent research accounting.

Weaknesses:

1. Can churn fees if opposing wallets flip rapidly.
2. Gross exposure can become too high even when net exposure is small.
3. Requires robust virtual sleeve accounting.

This should be the first model validated because it is the least assumption-heavy about where alpha lives.

### Option C: Consensus-Weighted Net

```text
target_coin_exposure_c,t =
  clip(N_c,t * f(consensus_c,t), -coin_cap_c, +coin_cap_c)
```

where:

```text
f(consensus) = consensus^gamma
gamma in {0.5, 1.0, 2.0}
```

or:

```text
target_coin_exposure_c,t =
  clip(sum_i a_i * s_i,c,t, -coin_cap_c, +coin_cap_c)
```

Strengths:

1. Gives higher-ranked and more-copyable wallets more influence.
2. Reduces low-quality opposite noise.
3. More fund-like than raw per-wallet copying.

Weaknesses:

1. More model degrees of freedom.
2. Can data-snoop if `gamma` and thresholds are tuned after outcomes.
3. Still may erase contrarian specialist alpha.

Validate second, after per-wallet sleeves.

### Option D: Anti-Consensus Threshold

Trade coin `c` only if agreement exceeds a threshold:

```text
trade_allowed_c,t = consensus_c,t >= theta
theta in {0.60, 0.70, 0.80}

target_coin_exposure_c,t =
  trade_allowed_c,t * clip(N_c,t, -coin_cap_c, +coin_cap_c)
```

Alternative count-based threshold:

```text
long_votes = count_i(s_i,c,t > 0)
short_votes = count_i(s_i,c,t < 0)

trade_allowed = max(long_votes, short_votes) / (long_votes + short_votes) >= theta
```

Strengths:

1. Avoids coin exposure when selected wallets disagree.
2. Can reduce drawdown and chop.
3. Clean risk-management story.

Weaknesses:

1. May enter late, after consensus is already crowded.
2. May eliminate the best early signals.
3. Can reduce trade count too much.

Validate as a risk overlay, not as the first alpha model.

### Option E: Specialist Per Coin

For each coin `c`, choose one wallet:

```text
i_star(c, T) = argmax_i score_i,c,T
```

where:

```text
score_i,c,T =
  rank_score_i
  * coin_specific_copy_return_score_i,c
  * copyability_score_i,c
  * liquidity_fit_i,c
```

Then:

```text
target_coin_exposure_c,t = s_i_star,c,t
```

Strengths:

1. Eliminates same-coin collision.
2. Very clean implementation.
3. Good if alpha is coin-specialist specific.

Weaknesses:

1. Throws away useful independent signals.
2. Sensitive to coin-level overfit.
3. May under-diversify if a few wallets dominate many coins.

Validate third, especially for specialist wallets.

### Recommendation: Validate First

Validate in this order:

1. **B: Per-wallet sleeves with coin gross/net caps**.
2. **C: Consensus-weighted net**.
3. **E: Specialist per coin**.
4. **A: Raw net consensus**.
5. **D: Anti-consensus threshold as overlay**, tested on B and C.

Reason:

Per-wallet sleeves answer the cleanest first question:

```text
Do selected wallets individually contain copyable forward alpha after costs?
```

If that fails, consensus is unlikely to save the thesis without becoming a different signal. If it passes but has coin-collision drawdowns, then C/D/E are legitimate portfolio improvements.

## 3. Required Coin Collision Controls

Even in per-wallet sleeves, portfolio-level exposure must obey:

```text
net_coin_exposure_c,t = abs(sum_i target_i,c,t)
gross_coin_exposure_c,t = sum_i abs(target_i,c,t)
directional_conflict_c,t =
  min(L_c,t, S_c,t) / max(G_c,t, epsilon)
```

Hard caps:

```text
abs(net_coin_exposure_c,t) <= net_coin_cap_pct * equity_t
gross_coin_exposure_c,t <= gross_coin_cap_pct * equity_t
sum_c gross_coin_exposure_c,t <= portfolio_gross_cap_pct * equity_t
```

Default values to validate:

```text
net_coin_cap_pct = 0.20
gross_coin_cap_pct = 0.35
portfolio_gross_cap_pct = 1.50
```

Conflict throttling:

```text
if directional_conflict_c,t > 0.40:
  reduce new exposure on coin c by 50%

if directional_conflict_c,t > 0.60:
  block new entries on coin c, allow exits only
```

These thresholds are predeclared test parameters, not tuned after outcomes.

Required reports:

1. PnL by coin.
2. Max net and gross exposure by coin.
3. PnL during high-conflict windows.
4. Return with top coin removed.
5. Return with all BTC/ETH exposure removed.
6. Return when same-coin opposite sleeves are blocked.
7. Fee drag caused by opposing sleeves.

## 4. Empirical Copyability Test

The question is:

```text
Can our 1s-latency taker execution capture a meaningful fraction of this wallet's alpha?
```

Not:

```text
Does this wallet trade too often?
```

### Inputs

For every wallet journey or fill-derived signal:

```text
signal_time = observed wallet fill timestamp
signal_price = wallet fill price or reconstructed market mid
direction = copied direction
size = target copy notional
coin = traded coin
```

For each latency `l`:

```text
l in {1s, 3s, 5s, 15s, 60s}
```

simulate entry:

```text
copy_entry_price_l =
  taker_price_at(signal_time + l)
  including spread and slippage proxy
```

Exit is one of:

1. wallet exit mirrored with the same latency,
2. stop loss,
3. trailing stop,
4. max hold,
5. liquidity/risk forced exit.

### Metrics

Native wallet edge:

```text
native_edge_j = wallet_native_net_pnl_j / reference_notional_j
```

Executable copied edge:

```text
copy_edge_j,l = copy_net_pnl_j,l / allocated_copy_notional_j
```

Alpha capture ratio:

```text
copyable_alpha_ratio_l =
  sum_j max(copy_net_pnl_j,l, -loss_cap_j)
  / max(sum_j wallet_native_positive_pnl_j, epsilon)
```

Latency degradation:

```text
latency_decay_l =
  copy_return_l / max(copy_return_1s, epsilon)
```

Fee burden:

```text
fee_share_l = total_fees_l / max(gross_profit_l, epsilon)
```

Participation:

```text
participation_l =
  executable_signals_l / intended_signals
```

Adverse selection:

```text
post_signal_move_bps_l,h =
  direction * (mid_price(t + l + h) - mid_price(t + l)) / mid_price(t + l)
```

where:

```text
h in {5s, 30s, 1m, 5m, 15m}
```

### Copyability Tiers

```text
Tier A:
  executable_net_return_1s > 0
  copyable_alpha_ratio_1s >= 0.50
  fee_share_1s <= 0.40
  participation_1s >= 0.80
  latency_decay_5s >= 0.70

Tier B:
  executable_net_return_1s > 0
  copyable_alpha_ratio_1s >= 0.30
  fee_share_1s <= 0.60
  participation_1s >= 0.60
  latency_decay_5s >= 0.50

Tier C:
  positive native edge but weak or unstable copyability
  monitor only

Reject:
  executable_net_return_1s <= 0
  or copyable_alpha_ratio_1s < 0.30
  or participation_1s < 0.60
```

Fast wallets can pass. Slow wallets can fail.

### Special Fast-Wallet Test

For wallets with high turnover:

```text
round_trip_cost_share = total_fees_and_slippage / gross_edge
median_signal_half_life =
  first h where post_signal_move_bps_1s,h gives back 50% of expected move
```

Fast wallet passes only if:

```text
median_signal_half_life > operational_latency_p90
and copy_return_1s remains positive after taker fees
and copy_return_3s is not destroyed
```

This is the right way to handle HFT: measure the alpha half-life.

## 5. Ambitious but Defensible Return Targets

### Target ladder

Research should explicitly look for:

```text
Base fund-grade target: 10% net monthly
Strong target: 20% net monthly
Exceptional target: 30% net monthly
```

These are acceptable only at the capacity being claimed.

For each target, report:

```text
expected_monthly_return
monthly_volatility
max_drawdown
calmar_like_ratio = monthly_return / max_drawdown
worst_14d_return
probability_monthly_loss
capacity_at_target_return
```

### Falsification diagnostics

The 10% to 30% monthly target is falsified if any of these hold:

1. Historical executable walk-forward net return is below 10% monthly after costs.
2. Return falls below 10% monthly after remove-top-1 wallet.
3. Return falls below 10% monthly after remove-top-1 coin.
4. Matched-random percentile is below p90.
5. Bootstrap probability of non-positive monthly return is above 5%.
6. Live 1s slippage exceeds modeled slippage by more than 50%.
7. Fee share exceeds 60% of gross edge.
8. Copyable alpha ratio at 1s is below 0.30 for most selected PnL contributors.
9. Capacity needed for fundraise drops expected return below 10% monthly.
10. Same-coin collision controls cut more than 50% of gross PnL while only modestly reducing drawdown.

The answer then is not "lower the ambition." It is:

```text
This wallet-copy configuration is not yet a fund-grade strategy.
```

## 6. Validation Protocol to Preserve Rigor

Keep from round 6:

1. forward-only folds,
2. walk-forward validation,
3. matched-random null portfolios,
4. remove-top-1 and remove-top-3 wallet survival,
5. rank IC tests,
6. anti-snooping freeze,
7. live execution attribution,
8. kill switches,
9. no post-hoc removal of losers.

Add:

1. remove-top-1 and remove-top-3 coin survival,
2. conflict-window attribution,
3. latency alpha half-life,
4. copyability tiers by empirical execution degradation,
5. capacity curve by coin and copy notional,
6. model comparison across B/C/E/A/D with frozen parameters.

## 7. Implementation Priority

### Tomorrow: one-day build

Build the minimum research spine that answers the unresolved problem.

1. Implement `coin_collision_report.py`.
   - Input: selected wallet journey/fill signals.
   - Output: `(time, coin)` long mass, short mass, net exposure, gross exposure, conflict ratio.

2. Implement per-wallet sleeve simulator v0.
   - Virtual sleeve accounting.
   - Exchange-level net position per coin.
   - Sleeve-level PnL attribution.
   - Net/gross coin caps.

3. Implement empirical copyability metrics v0.
   - Latencies: `1s, 5s, 15s, 60s`.
   - Fees and simple slippage.
   - Output Tier A/B/C/reject.

4. Remove all hard high-trade-count exclusions from the config.
   - Route fast wallets into copyability testing.

5. Produce one debug report:
   - top selected wallets,
   - copyability tier distribution,
   - highest collision coins,
   - return with and without coin caps,
   - top coin removal,
   - top wallet removal.

Success criterion for tomorrow:

```text
We can see whether the current selected wallets are making money because of real copyable alpha,
or because we ignored coin collisions and latency decay.
```

### This week: five-day build

1. Finish model B/C/E/A comparison.
   - B: per-wallet sleeves.
   - C: consensus-weighted net.
   - E: specialist per coin.
   - A: raw net consensus baseline.
   - D: anti-consensus overlay on B and C.

2. Add frozen parameter grid:

```text
coin_net_cap_pct in {0.15, 0.20, 0.30}
coin_gross_cap_pct in {0.25, 0.35, 0.50}
consensus_gamma in {0.5, 1.0, 2.0}
anti_consensus_theta in {0.60, 0.70, 0.80}
```

Use this grid for research diagnostics only. Freeze one configuration before final validation.

3. Add matched-random nulls under the new execution models.

4. Add return target report:

```text
monthly_return
drawdown
remove_top_wallet
remove_top_coin
random_percentile
copyability_alpha_ratio
capacity_curve
```

5. Decide the first live validation candidate model by evidence.

Success criterion for this week:

```text
One execution model is selected for frozen walk-forward,
or the strategy is rejected before wasting live time.
```

### This month: four-week build

Week 1:

1. Complete historical model comparison.
2. Freeze methodology and parameters.
3. Run 1,000-trial matched-random validation.

Week 2:

1. Wire selected model into V11 live stack.
2. Add live sleeve-level attribution.
3. Add live coin collision dashboard.
4. Add latency/slippage measurement against expected entry prices.

Week 3:

1. Run live execution fidelity stage.
2. Do not require profit yet.
3. Require infrastructure, latency, participation, and slippage to match assumptions.

Week 4:

1. Run live alpha validation if fidelity passed.
2. Compare live results to historical p10/p50/p90 expectations.
3. Produce investor-grade internal memo:
   - strategy thesis,
   - validation evidence,
   - risk controls,
   - capacity,
   - failure modes,
   - live attribution.

Success criterion for the month:

```text
Either we have a defensible live candidate for a crypto hedge fund strategy,
or we have a precise falsification showing what must change.
```

## 8. Adversarial Conclusion

The ambitious version is not "copy the top Sharpe wallets and hope."

The ambitious version is:

```text
Let 4+ Sharpe wallets into the arena.
Let high-frequency wallets into the arena.
Let 10, 50, or 100 wallets into the arena.
Then force every one of them through copyability, collision, null, survival, and live attribution tests.
```

If they pass, the fund target should be aggressive.

If they fail, the methodology should say so cleanly before capital, time, or investor trust is spent.
