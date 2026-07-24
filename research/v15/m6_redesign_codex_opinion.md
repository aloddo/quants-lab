The current design is misaligned. M6a ranks aggregate ROE; M6b double-counts aggregate return through realized ROE and Calmar, then adds win rate. Neither directly judges the distribution of net return per copied position. `n_pool=100` is also not an investable pool at $950—it is a research list.

## Common position definitions

A “position” must be a complete journey/round trip, aggregating partial fills and scale-ins.

For position \(i\):

\[
A_i=\max_t|\text{marked notional}_{i,t}|
\]

\[
r_i=\frac{\text{realized PnL after fees, slippage, lag and trailing-stop execution}}{A_i}
\]

Use peak notional, not margin, wallet equity, or raw dollars. This is leverage- and wallet-size-neutral and does not reward DCA escalation.

Core statistics, equally weighted by position:

\[
\mu=\frac1n\sum_i r_i,\qquad
s=\sqrt{\frac1{n-1}\sum_i(r_i-\mu)^2}
\]

Also calculate a one-sided lower confidence bound \(LCB_\mu\) using a 14-day block bootstrap. Do not pretend thousands of correlated fills are thousands of independent observations.

Preserve conviction during execution separately:

\[
\text{child target fraction}
=\text{wallet sleeve}\times
\operatorname{clip}\left(\frac{\text{leader notional}}{\text{leader equity}},0,f_{\max}\right)
\]

Never return to fixed-dollar child positions.

## M6a: high-recall bouncer

### Hard gates

Keep these deliberately permissive:

- M5-eligible, canonical copyable entity, valid lifecycle.
- At least 20 closed positions and 3 active days pretest.
- At least one action in the last 14 days.
- No catastrophic hidden bag:

\[
B_{\rm eq}=
\frac{\sum_{j\in open}\max(0,-UPnL^{liq}_j)}
{\text{wallet equity}}
\le 15\%
\]

- For positions opened at least seven days before cutoff, at least 50% of opened notional must have been closed.
- Finite position return data; no performance, benchmark, concentration, or significance hard gate.

M6a must not reject merely because estimated net edge is negative. Its cheap cost estimate is too crude.

### Soft metrics

For leader position \(i\), calculate:

\[
r^{proxy}_i
=
\frac{\text{leader realized PnL}_i}{A_i}
-
c_i^{direct}
\]

where \(c_i^{direct}\) is the canonical all-taker follower fee plus calibrated coin-class slippage. Do not invent a lag estimate without M7.

Rank using within-fold percentiles:

\[
S_a =
0.30P(\mu_{proxy})
+0.20P(LCB_{\mu,proxy})
+0.20P(-s_{proxy})
+0.10P(F_{fast})
+0.10P(-B)
+0.05P(-C_{position})
+0.05P(\alpha_{benchmark})
\]

Definitions:

- \(F_{fast}\): fraction closed within 72 hours, with additional reporting of median and p90 hold time.
- \(B\): maximum of open-loss/equity and open-loss/positive-realized-PnL.
- \(C_{position}\): largest positive-PnL contribution divided by total positive PnL.
- \(\alpha_{benchmark}\): position return minus direction-matched 50/50 BTC/ETH return over the same holding interval.

Shortlist 1,000, but reserve routes to avoid one composite score determining recall:

- 750 by \(S_a\).
- 150 remaining by \(\mu_{proxy}\).
- 50 remaining by lowest \(s_{proxy}\), conditional on \(\mu_{proxy}>0\).
- 50 remaining by benchmark residual \(LCB\).

Top-1,000 is an engine-budget constraint, not “multiple-testing discipline.” It does nothing statistically to control false discoveries.

## M6b: final judge

Only M7 positions under the canonical execution model count. No fallback from realized copy metrics to source-wallet ROE in an investable run.

### Hard gates

1. **Support and activity**

- \(n_{\rm closed}\ge100\).
- At least four active 14-day blocks.
- At least 20 active calendar days.
- Last-28-day frequency at least 3 closed positions/day; full soft credit at 10/day.
- Median hold \(\le48\) hours and p90 hold \(\le7\) days.

Five round trips is noise, not evidence.

2. **Net edge after our execution**

- \(LCB_\mu>0\).
- One-sided block-bootstrap p-value passes pre-registered 5% fold-level FDR control across the M7 shortlist.
- M7 stress replay at 1.5× fees and slippage still has \(\mu_{stress}>0\).

3. **Cost barrier**

Run an otherwise identical frictionless/leader-timestamp replay:

\[
d_i=r_i^{ideal}-r_i^{net}
\]

\[
CCR=\frac{\mu_{ideal}}{\max(\bar d,\epsilon)}
\]

Require \(CCR\ge2.0\). A wallet whose gross signal merely equals costs is not an edge; it is model-error exposure.

Maker economics count only if M7 models limit-order fill probability and adverse selection. Otherwise judge on all-taker execution.

4. **Hidden bags**

Do not credit unrealized winners, but debit unrealized losers:

\[
P_{conservative}
=P_{realized}
+\sum_{open}\min(0,UPnL^{liq})
\]

Require, on both source-wallet diagnostics and follower replay:

- Open-loss burden \(\le2\%\) of equity.
- Open losses \(\le25\%\) of positive realized PnL.
- At least 80% of notional aged 72 hours is closed.
- Conservative net PnL \(>0\).

“Realized PnL only” by itself makes the hidden-bag problem worse. Realized winners plus a mandatory open-loss debit is the correct formulation.

5. **Concentration**

For positive PnL contributions \(p_i^+\):

\[
C_{position}=\max_i
\frac{p_i^+}{\sum_jp_j^+}
\]

Require:

- \(C_{position}\le35\%\).
- Removing the best position leaves positive net PnL.
- Follower max drawdown \(\le15\%\).

Do not hard-reject a one-coin specialist solely for specialization. Instead impose pool-level constraints:

- Expected gross exposure to any one coin \(\le25\%\).
- No wallet contributes more than 20% of expected pool PnL.
- Leave-one-wallet-out and leave-one-coin-out pool conservative PnL must both remain positive.

6. **Copy fidelity**

Require:

- Executed/intended notional \(\ge70\%\).
- Capacity-capped intended notional \(\le20\%\).
- Tracking error \(\le20\%\).
- Calibrated, versioned costs.

Skipped trades can create fake performance, especially if losing trades are systematically harder to fill.

7. **Benchmark**

Using daily M7 net equity returns:

\[
R^{copy}_t=\alpha+\beta R^{50/50BTCETH}_t+\epsilon_t
\]

Require individual \(LCB_\alpha\ge0\). Then apply a final pool veto: the selected pool must beat 50/50 BTC/ETH on both conservative return and Calmar/Sortino over the pretest window. If it does not, hold BTC/ETH or cash. Do not force deployment.

### Soft judge score

After the gates, use percentile ranks so unlike units are not mixed:

\[
S_b =
0.25P(\mu_{net})
+0.20P(-s_{net})
+0.15P(LCB_{\mu})
+0.15P(\mu_{stress})
+0.10P(\alpha)
+0.10P(F_{fast}\times realization\ coverage)
+0.025P(-concentration)
+0.025P(copy\ health)
\]

This makes mean and standard deviation of per-position return the dominant 45%, as requested. Win rate gets zero weight; it is redundant and payoff-blind.

## Pool and weighting

Keep a ranked top-100 as a research/audit list. Do not call it investable.

For deployment, select at most 12 wallets, subject to the coin and wallet concentration constraints. The actual \(K\) must also satisfy:

\[
\frac{0.8E}{q_{95}(\text{simultaneous child positions})}
\ge2\times\text{minimum executable order}
\]

At $900–950, this will often produce roughly 6–12 wallets, not 100.

Use 75% equal weights and 25% shrunk inverse-risk edge weights:

\[
w_e^{raw}=0.75/K+
0.25\frac{\max(LCB_{\mu,e},0)/(s_e^2+s_0^2)}
{\sum_j\max(LCB_{\mu,j},0)/(s_j^2+s_0^2)}
\]

Cap wallet weight at 20%. Quintile weights create arbitrary discontinuities and should be removed.

## Keep, change, kill

**Keep**

- Fold-pretest purity and fail-closed provenance.
- M6a top-N fixed by engine capacity.
- Recency/activity logic.
- M7 as the sole final copyability judge.
- Calibrated fee/slippage versions.
- Capacity, fidelity, and drawdown as diagnostics.

**Change**

- Aggregate ROE to equal-weighted per-position realized return.
- Five closed trades to 100 for final rankability.
- Binary positive-block “consistency” to actual \(s\), block-bootstrap \(LCB\), and stress edge.
- Soft-only capacity/fidelity into minimum investability gates.
- Hidden-bag handling to realized PnL plus mandatory open-loss debit.
- Pool selection to portfolio-aware wallet/coin concentration constraints.

**Kill**

- ROE × journey-count × persistence multiplicative M6a score.
- Aggregate realized ROE as the primary M6b term.
- Calmar as a second return term.
- Win rate.
- Mixed z-scores and raw \([0,1]\) terms in one score.
- `n_pool=100` as a deployable pool.
- Quintile bucket weights and the 10% ceiling.
- Any production fallback to uncalibrated cost, missing fidelity, or unrealized/MTM return.

Thresholds must be frozen before viewing any of the 12 OOS folds. Use external/pre-2026 development data if available; otherwise use the economic thresholds above unchanged. Do not retune them after observing fold outcomes.

## Ideal-world judge

The ideal judge does not rank wallets independently. It estimates the posterior distribution of the **causal, incremental PnL from copying each wallet into the current portfolio**, under our exact latency, order type, fill probability, adverse selection, capital constraints, market impact, stops, and cross-wallet/coin correlations. It then maximizes expected log growth subject to drawdown, CVaR, liquidity, and concentration constraints.

The correct object is marginal portfolio utility after execution—not wallet profitability.
