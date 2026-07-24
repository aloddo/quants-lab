**Verdict: PLAN-NEEDS-CHANGES.** The structure is directionally right, but it can still promote an overfit, cost-barriered, concentrated portfolio to real capital.

### Critical weaknesses

1. **Missing gate: portfolio construction and operational risk.**  
   Wallet-level passes do not imply a viable wallet set. Correlated wallets may copy the same HYPE trade simultaneously. Require portfolio replay with covariance, coin/wallet concentration, concurrent margin usage, liquidation risk, stale/conflicting signals, exposure caps, and kill switches.

2. **The “OOS” can become contaminated.**  
   Once TEST results influence wallet, fold, threshold, benchmark, or trailing-TP choices, they are no longer OOS. Use nested walk-forward validation plus one sealed final holdout that is opened once. Eligibility and selection must be reconstructed strictly as known at each historical timestamp.

3. **Gate 2 is vague and statistically weak.**  
   “Top-ranking in N consecutive folds” is gameable, and adjacent folds are correlated. Multiple-testing correction must cover the full search family: 1,832 wallets × metrics × thresholds × folds × policies × trailing parameters—not merely the 17 finalists. Use block bootstrap and FDR/reality-check-style correction, with minimum independent positions and trading days.

4. **Benchmarking is underspecified.**  
   “Hold what they traded” does not control for changing long/short beta, exposure timing, leverage, or alt momentum. Compare against exposure- and coin-matched passive/factor benchmarks, and report incremental alpha per unit of turnover.

5. **Live-small validates execution—not statistical edge.**  
   Two to four weeks may reveal fill and slippage errors, but cannot establish persistence across regimes. Many trades are not many independent observations: positions can be clustered by wallet, coin, and market event.

### Better sequence

1. **Gate 0: freeze protocol and audit data/replay.** Define metrics, thresholds, exclusions, policy grid, cost model, and final holdout before viewing results.
2. **Immediate economic-feasibility screen.** Reject wallets whose gross edge per action/position cannot clear break-even costs with a confidence margin.
3. **Nested walk-forward WHO + HOW selection.** Policy selection belongs inside each training window, followed by untouched forward evaluation.
4. **Portfolio-level OOS replay and stress tests.**
5. **Sealed final holdout.**
6. **Live-small execution calibration, followed by staged scaling.**

Human/Codex/Fable sign-off is governance, not empirical Gate 5.

### Fixed-size caveat

Fixed-size direction copying does **not** invalidate the exercise if that exact strategy is the proposed initial product. It validates only:

> “This wallet’s directions are profitable under our fixed-size copy policy.”

It does not validate the wallet’s intrinsic skill or establish that conviction-weighted copying will work. Do not later change sizing without revalidation. Also test volatility-normalized sizing; otherwise equal dollars can create radically unequal risk across coins.

### Cost barrier

The plan still treats cost as a stress test instead of a primary admission criterion. Add:

- Gross edge and confidence bound **per copied action and per round trip**.
- Full-mirror turnover inflation from adds/trims.
- Break-even fee, slippage, and latency for every wallet/policy.
- Costs conditional on coin liquidity, volatility, order size, time, and simultaneous wallet signals.
- Minimum-edge/no-trade thresholds; copying every action should not be presumed optimal.
- Replay at actual exchange minimum sizes for a $50–200 account.
- Tail scenarios beyond a generic 2× multiplier.

A wallet should pass only if the lower confidence bound on expected gross edge clears conservatively modeled costs—not merely if aggregate backtest PnL remains positive.

### Live-small success criteria

Do not use elapsed weeks alone. Require preregistered thresholds such as:

- Minimum independent position clusters and multiple market regimes.
- Realized slippage/latency within modeled confidence bounds.
- Live-vs-replay attribution reconciled trade by trade.
- Positive net edge with uncertainty bounds, not just positive PnL.
- No wallet/coin dominating results.
- No breaches of exposure, drawdown, or operational limits.

Scale in stages—e.g. $50 → $200 → $500—not directly from a lucky two-week sample to the full book.

### Top five changes

1. Add a strict portfolio construction, concentration, margin, and kill-switch gate.
2. Introduce a frozen protocol and one genuinely sealed final holdout.
3. Replace consecutive-fold ranking with nested walk-forward testing and search-wide multiple-testing control.
4. Move cost feasibility to the front and require lower-bound edge to exceed costs per copied action.
5. Make live-small an execution-calibration and staged-scaling gate, not proof of alpha.
