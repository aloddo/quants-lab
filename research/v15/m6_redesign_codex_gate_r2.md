## r1 closure audit

| r1 finding | Status | v2.1 result |
|---|---|---|
| L1 exit-before-cutoff | Genuine | Strict `exit_ts < cutoff` removes post-cutoff close leakage. New censoring bias remains. |
| L2 carry-in | Partial | Correct fallback is stated, but “proven snapshot” and flat-point provenance are not defined/emitted. |
| L3 policy selection | Not closed | Parameters are absent; §5 says “each wallet’s pre-registered policy,” contradicting one global policy. |
| L4 point-in-time universe | Genuine if fail-closed | Membership is causal, but costs, metadata, clusters, and calibrations are not required point-in-time. |
| L5 output leak | Partial | Per-fold evidence is pretest-only, but the global cross-fold aggregate can use future folds for earlier validation. |
| C-BH1 | Not closed | Next-fold replication reduces errors; it does not control FDR across ~1000 wallets. |
| C-BH2 | Not closed | Relative rank/winner’s curse is never confirmed. |
| C-boot | Not closed | Confidence level, resampling scheme, effective-n estimator, and spanning-journey treatment are undefined. |
| S4 dust/materiality | Partial | M6b definition works; no-engine M6a cannot know follower `child_peak_notional`. |
| S5 entries-not-exits | Genuine | Assuming causal action labels. |
| D1 DCA | Not closed | Cross-wallet bypass is explicitly accepted; ratio and escalation denominators remain undefined. |
| D2 FIFO bag principal | Genuine | Definition is sufficient if implemented cohort-wise. |
| D3 CCR | Partial | Required matched-path and per-action execution information is not in the emit contract. |
| E1 capacity/capital | Not closed | §1 requires a curve, §4 deletes it, §6 consumes it. |
| E-canon | Partial | Importing the current canonical module gives static, size-invariant slippage—not S\*-specific impact. |
| E-bags | Partial | Historical control is added, but sampling cadence and `committed_capital` are undefined. |
| E-bench | Not closed | The same-coin/same-entry/same-exit shadow is degenerate. |
| E-dd | Partial | Deployment placement is correct; threshold, exact series, and portfolio construction protocol are absent. |
| E-1coin | Partial | Clusters are not required as-of, and synchronized coin exposure is not emitted. |
| F1 actor mismatch | Genuine | Child PnL divided by child peak notional. |
| F-policyB | Not closed | No executable policy specification or causal intrabar convention exists. |
| F-perpos | Not closed | Emit set cannot compute all gates. |
| F-veto | Partial | Correct layer, but optimized and tested on the same pretest data. |
| F-Sb | Genuine | One formula replaces the conflicting weights, though “pool” needs a precise denominator. |
| F-Kfeas | Genuine for K<5 | Residual cash is explicit; general cap/renormalization remains broken. |

## Blocking findings

- [P1] **§4 / §7 — trailing-TP policy is still selectable.** No numeric distance, activation rule, gap fill, addon rule, re-entry rule, bar convention, or immutable manifest is supplied. “Each wallet’s pre-registered policy” permits 1000 wallet-specific parameter choices. Trying several distances, choosing 7%, then declaring it the sole policy merely relabels the winner.

- [P1] **§7 — the “next fold” is not defined as independent held-out data.** If confirmation uses fold `k+1` pretest, it heavily overlaps fold `k` pretest. If it uses the short OOS interval, it generally cannot re-estimate a bootstrap LCB requiring 100 positions and eight 14-day blocks. Exact selection cutoff, confirmation interval, embargo, and whether confirmation data later enter rank are missing.

- [P1] **§7 — replication is not FDR control.** With 1000 null wallets and a 5% false-pass probability at each independent stage, roughly `1000×0.05² = 2.5` false wallets still survive in expectation. No confirmation-stage multiplicity adjustment or simultaneous bound exists.

- [P1] **§5 / §6 — winner’s curse survives.** Confirmation only rechecks `LCB_mu` and `LCB_alpha`; it does not confirm `S_b` ordering. Deployment still walks the extreme in-sample rank top-down. A wallet can barely confirm the floors but receive priority because its selection-period score was the largest noise realization.

- [P1] **§1 / §4 / §6 — capacity contract is contradictory.** §1 says replay `S*`, `2S*`, `4S*`; §4 says “no size-response curve”; §6 says “read `r(S)` at deploy sleeve.” There is no artifact from which §6 can read it.

- [P1] **§1 — S\* does not standardize wallet capital.** Each concurrent coin receives up to `f_max×S*`; ten simultaneous positions can consume ten times the gross capital of a one-position wallet. DD, hidden-bag burden, margin rejection, and fidelity are therefore not measured on a common sleeve. A wallet-level gross/collateral normalization is required.

- [P1] **§1 — the S\* sensitivity test is non-binding and gameable.** Spearman ≥0.9 is only reported, does not test gate-membership stability, and has no defined universe. The common survivors can have ρ=0.95 while 30% of passers disappear at `2S*`. Require fail-closed rank and pass-set stability, including the actual deployment sleeve.

- [P1] **§3 — M6a cannot compute its stated gates without M7.** “20 MATERIAL positions” and `c_proxy` divided by follower `child_peak_notional` require S\* execution, rounding, min-order rejection, and capacity fills. Using leader quantities silently restores the actor mismatch.

- [P1] **§4 / gates 1, 3, 5, 7, 8 / §6 — M7-v2 emit set is insufficient.** Missing or unspecified:

  - follower `entry_ts`, `exit_ts`, side and block key for holds, active days, bootstrap, and recency;
  - intended policy targets and rejected-order notional for fidelity;
  - per-action reference marks/quantities or explicit frictionless and stressed PnL;
  - exposure-weighted underwater-add numerator/denominator;
  - `shadow_i`/`alpha_i`;
  - timestamped coin, side, gross notional, cash, realized PnL and total equity for synchronized cluster exposure and portfolio DD.

  The current equity output only contains account equity and position count, not those exposures ([engine](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:857)).

- [P1] **§4 / gate 8 — same-coin shadow is degenerate.** For a normal one-entry/one-exit trade, the replica and a same-side shadow with identical entry and exit times have the same gross return. With equal costs, `alpha=0`; with a frictionless shadow, alpha is negative. `LCB_alpha>0` rejects essentially every simple directional wallet and favors path-manipulation/addon effects instead.

- [P1] **§1 / gates 2 and 4 — closed-position censoring remains gameable.** A wallet can realize quick +2% TP winners while retaining many −1% losers below the aggregate bag ceiling. Only winners enter μ/LCB; historical underwater can remain below its ceiling; the same pattern repeats in confirmation. Mark all eligible entries to cutoff, impose an entry embargo, or use a censoring-aware estimator.

- [P1] **§2 / §5 output — global cross-fold rank can leak into historical OOS.** A wallet that fails test fold 2 has that failure inside later pretests; a global mean over all folds can then retroactively lower its rank for fold-2 reporting. Historical outputs must use expanding aggregates containing only folds available before that test.

- [P1] **§4 — trailing trigger has unresolved intrabar look-ahead.** With 1-minute OHLC, a candle can establish a new high and cross the trailing stop low in the same bar; ordering is unknowable. High-then-low processing fabricates a profitable exit. Require prior-completed-bar state plus next-bar execution, or a fixed conservative same-bar ordering.

- [P1] **§4 / gate 7 — canonical-cost closure is not operational.** The current canonical model uses static per-coin slippage and a fixed 1-second latency; it has no order-size/time-aware impact API ([execution model](/Users/hermes/quants-lab/research/v15/execution_model.py:25)). Importing it “for ALL costs” cannot price S\* capacity or deploy sleeves without first extending and versioning that API. Fee, liquidity, metadata, and calibration versions must also be as-of each event/fold.

- [P1] **§2 / gate 5 — cross-wallet DCA is explicitly left open.** “Accepted residual risk” means an operator can split alternate underwater adds across unclustered wallets and clear both per-wallet gates. Missing clustering must fail closed or impose an entity-uncertainty penalty/gate.

- [P1] **§6 — portfolio selection and benchmark veto are in-sample.** Wallet subset, clusters, caps, and weights are chosen on pretest and then judged against BTC/ETH on that same pretest. Searching many subsets can produce a benchmark-beating portfolio by chance even when individual gates confirm. The final portfolio construction needs its own untouched confirmation window.

- [P2] **§7 — bootstrap specification is not reproducible.** “Stationary/circular” names different methods; no one-sided confidence level, resample count, seed, geometric block parameter, or effective-n estimator is frozen. Entry-block assignment also treats multi-block journeys as block-local.

- [P2] **§5.5 — DCA gate remains mechanically gameable.** `underwater_add_ratio` does not say count-, dollar-, or exposure-time-weighted. Dust adds can dilute a count ratio, and waiting `T_reset+ε` defeats episode collapse.

- [P2] **§5.4 — `committed_capital` is undefined.** Starting sleeve, peak margin, cumulative deposits, and realized-equity capital give materially different bag ratios. “Defeats deposit dilution” is unsupported without a frozen formula.

- [P2] **§6 — weights do not necessarily sum to one.** With `K=5`, equal weight contributes 15% each and the inverse-risk component can add up to 25% to one wallet. Clipping that wallet at 20% leaves unallocated mass; no redistribution or cash rule is stated for `K≥5`.

DO-NOT-SHIP
