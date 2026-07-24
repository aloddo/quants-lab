### Look-ahead / leakage

- [P1] **Complete round-trip unit** — no explicit `exit_ts < test_start_k` rule. A loser open at cutoff but closed profitably afterward can enter `r_i`, `n_closed`, hold-time, realization, DCA, and benchmark evidence. Enforce every action, mark, exit, and policy trigger strictly before cutoff.

- [P1] **M7 carry-in** — current `_seed_carry_in` uses the first later in-window fill’s `startPosition` to seed exposure at window start ([v15_m07_engine.py](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:914)). That backdates future knowledge. `future_delta_only` avoids the leak but then cannot copy profits from pre-window entries. Require an actual state snapshot at `train_start_k`.

- [P1] **Best copy-policy** — choosing the better of FULL-MIRROR and TRAILING-TP on the same sample used for `LCB_mu` and BH creates winner-selection bias. A zero-edge wallet gets two chances to manufacture significance. Policy must be nested-selected or included in the multiple-testing family.

- [P1] **Point-in-time universe** — fold-pure metrics do not prove fold-pure wallet discovery. A July-built universe can omit wallets that died before July, improving all historical folds. Require point-in-time candidate membership, coins, entity mapping, and delistings; fail closed on global/current snapshots.

### Statistical and gameable gates

- [P1] **BH FDR surface** — M6a selects wallets using `mu_proxy`, proxy LCB, and benchmark LCB from the same positions tested by M6b. Post-selection p-values are not null-uniform. BH over only the selected shortlist does not control FDR.

- [P1] **BH family definition** — the real family is wallet × fold × policy, plus any trail thresholds tried. “Fold-level BH” separately at 5% does not control the union/full ranked list. Cross-wallet coin dependence also need not satisfy ordinary BH assumptions.

- [P1] **14-day block-bootstrap LCB** — support allows only four active blocks. One hundred trades inside four regimes are four effective observations, not 100. Bootstrap output will show fake precision. Specify calendar-block assignment, boundary-spanning journeys, null recentering, and materially more independent blocks.

- [P1] **Equal-position weighting / `n_closed`** — a wallet can create 100 dust round trips with high percentage returns while its economically material trades have no edge. Dust dominates `mu`, `s`, LCB, `F_fast`, and support while contributing little to notional-weighted fidelity. Require executable-position support and cap influence from sub-min-order positions.

- [P1] **Support/activity gate** — `last-28d freq >= 3 closed/day` counts exits, not new copy opportunities. A wallet unwinding old inventory plus dust entries passes while a new follower cannot enter the profitable inventory.

- [P1] **DCA hard gate / `underwater_add_ratio`** — close-and-reopen resets entry VWAP, converting continued averaging-down into a new `ENTRY`. Cross-wallet or correlated-coin averaging also bypasses it. A martingale with 99 recoveries and no bag exactly at cutoff passes. Add MAE, time-underwater, exposure-escalation, and path-capital gates.

- [P1] **Realization gates** — “opened notional closed” is undefined for trims/reopens. Repeatedly closing and re-adding the same 10% can report >80% closed while 90% of the original bag remains. Track cohort-level remaining principal once, not cumulative close turnover.

- [P1] **CCR** — `d_i = r_ideal-r_net` can be negative when lag is favorable or policies change paths. `max(mean(d), eps)` then makes CCR enormous. One lucky delayed fill passes the “cost wall.” Require a matched policy/path, nonnegative friction decomposition, and tail-cost statistic.

- [P2] **Stress gate** — a wallet may fail 1.5× costs yet remain investable because stress is only soft. That does not close the cost-barrier failure under calibration error or spread regime change.

- [P2] **Percentile scores** — ranks depend on who else happens to enter a fold and discard economic distance. A microscopic metric difference can equal a huge one; tied/discrete metrics need an explicit rule. Fold percentiles are not directly comparable for a cross-fold final rank.

### Copy-trading failures still open

- [P1] **Capital-agnostic ranking versus capacity/fidelity** — min orders, impact, rejections, and capacity depend on child dollars. A wallet can pass at $10k simulated equity and fail at $950—or reverse. “Our-cost/capacity gate” cannot be capital-agnostic without ranking standardized size-response curves.

- [P1] **Canonical execution model** — current M7 reimplements fees, slippage, impact, and latency instead of importing `execution_model.py`; defaults already differ, including 2s versus 1s latency ([v15_m07_engine.py](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:480), [execution_model.py](/Users/hermes/quants-lab/research/v15/execution_model.py:52)). Updating the alleged canonical model need not update ranking results.

- [P1] **Hidden bags** — open-loss gates are cutoff snapshots. A recovered martingale has zero current burden but retains catastrophic live tail risk; deposits can dilute equity-based burdens. `P_conservative > 0` does not measure maximum capital committed or historical underwater exposure.

- [P1] **Benchmark gate** — BTC/ETH interval return is not exposure-matched for adds/trims and does not remove coin beta. A levered SOL/HYPE beta specialist can show positive “alpha” versus 50/50 without timing skill. Use an action-matched same-coin/sector shadow strategy and a confidence bound, not merely `mean(x)>0`.

- [P1] **Follower maxDD** — per-wallet standalone DD does not constrain portfolio DD. Many wallets can hold different but highly correlated altcoins and crash together. Coin caps and leave-one-out profitability do not close cross-wallet correlation/CVaR risk.

- [P1] **One-coin deployment gate** — “expected gross exposure <=25%” can hide synchronized peak exposure, and leave-one-coin-out positive PnL is not a risk test. Four correlated meme coins can each satisfy 25% while behaving as one trade.

### Internal/code-breaking contradictions

- [P1] **`r_i` denominator** — numerator is follower PnL under our sizing, but `A_i` is leader peak dollars. That is not size-neutral. If numerator is leader-sized instead, impact is no longer “our execution.” Policy B also may exit before the leader reaches `A_i`. Use child PnL/child peak notional with an explicit standardized sleeve.

- [P1] **Policy B implementation** — current `follower_trail` is an account-level drawdown breaker that flattens every coin and halts the fold, not a per-position trailing take-profit ([v15_m07_engine.py](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:568)). Addon handling, trail distance, trigger prices, gaps, and re-entry are unspecified.

- [P1] **Per-position M7 evidence** — current M7 emits aggregate `realized_roe`; open round trips are discarded at fold end ([v15_m07_engine.py](/Users/hermes/quants-lab/research/v15/v15_m07_engine.py:1645)). It does not emit the proposed policy-specific `r_i`, `A_i`, MAE, underwater-add state, or shadow benchmark. This is not merely an M6a/M6b configuration change.

- [P1] **M6b output semantics** — “full ranked list,” “fold-level BH,” and 12 folds never specify whether output is latest-fold only, union, intersection, or aggregated wallet rank. Aggregating OOS fold outcomes would leak into earlier decisions.

- [P1] **Pool benchmark veto** — M6b supposedly emits no selected pool, yet gate family 7 vetoes “the selected pool.” The veto belongs after M9 selection and must be recomputed for each deployed portfolio.

- [P1] **`S_b + quality_weight`** — no formula defines this addition, while quality weighting is also assigned to deployment and quintile weights were explicitly killed. Two implementations will produce different ranks.

- [P1] **Deployment cap feasibility** — the executability rule can produce `K_eff < 5`, but 20% wallet caps cannot allocate 100% with fewer than five wallets. This contradicts “utilize capital as much as possible” unless cash or cap relaxation is explicit.

- [P2] **Copy-fidelity family** — denominators for executed/intended notional, capacity-capped intended, and tracking error are unspecified for rejected entries, ignored trims, forced exits, zero targets, and policy B. These choices are highly gameable.

- [P2] **Concentration family** — `C_position` does not say dollar PnL versus `r_i`; “drop-best-position net-positive” does not define best by dollars or return. Dollar definitions reintroduce leader-size bias.

- [P2] **M6a reserved routes** — 750+150+50+50 does not guarantee 1,000 unique wallets because routes overlap. Backfill order and tie handling are missing, so the engine budget and recall surface are nondeterministic.

- [P2] **`c_direct_i`** — subtracting a single fee/slippage rate underprices journeys with many adds/trims. It must be turnover-weighted: total per-action friction divided by the same position denominator.

**VERDICT: DO-NOT-SHIP.**
