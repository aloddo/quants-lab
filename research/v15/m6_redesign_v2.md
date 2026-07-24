---
type: project
title: M6/M7 Redesign v2 — post-Codex-gate (leak-closed, capacity-true, copyability-real)
date: '2026-07-23T08:45:00Z'
agent: quant-engineer
owner: quant-engineer
status: v2.2 FINAL DESIGN — 25 P1 (r1) + Fable red-team (v2.1) + 6 Pile-A design-bug fixes (Codex gate r2). Alberto GO (TG 11577) = build M7-v2, codex/fable eng-review the implementation. Prose-gate loop CLOSED.
venture: quant
tags: [v15, m06, m07, copy-trading, consensus, gate-r2]
---

# M6/M7 Redesign v2 — resolves Codex gate r1 (DO-NOT-SHIP, 25 P1 + 6 P2)

Supersedes v1 (projects/quant/v15/m06-redesign-consensus). Same OBJECTIVE: rank the best, most
profitable COPYABLE wallets, capital-agnostic; deploy separately. v2 closes every P1. Gate r1 findings:
research/v15/m6_redesign_codex_gate_r1.md. Per-finding closure table at the end.

## 0. HONEST SCOPE (gate finding #1 — I mis-framed v1 as "configure only")
This is NOT a config change to m06a/m06b. It requires an **M7 ENGINE WORKSTREAM** first, because today's
M7 (verified in code): emits AGGREGATE realized_roe, DISCARDS open round-trips at fold end, has NO
per-position output, its `follower_trail` is an account-level DD breaker (not a per-position trailing
TP), and it REIMPLEMENTS costs instead of importing execution_model.py (2s vs 1s latency drift).
Build order: **M7 v2 (per-position, policy-aware, canonical-cost) → M6a config → M6b config → deploy layer.**

## 1. THE UNIT + STANDARDIZED SLEEVE (fixes r_i actor-mismatch #F1; capital-agnostic-vs-capacity #E1)
- Evidence unit = complete round-trip journey (flat-to-flat on a coin, aggregating all adds/trims),
  **with exit_ts STRICTLY < test_start_k** (leak fix #L1: a loser open at cutoff, closed profitably
  after, is EXCLUDED from all return/hold/DCA/benchmark evidence; its open exposure still counts in the
  cutoff hidden-bag debit).
- **STANDARDIZED SLEEVE S\*** (the bridge that keeps ranking capital-agnostic while respecting capacity):
  every wallet is replayed by M7 at a fixed reference sleeve S\* (a constant $, not our bankroll), sized
  proportional to leader conviction: child_target = S\* × clip(leader_notional/leader_equity, 0, f_max).
  All ranking metrics are computed on OUR follower fills at S\*. Capital enters ONLY at deployment, which
  reads a size-response curve r(S) (M7 replays at S\*, 2×S\*, 4×S\* to trace capacity decay).
- **G\* = a wallet-level GROSS CAPITAL budget** (FIXED v2.2 — a per-position S\* did NOT equalize wallets:
  one running 10 simultaneous positions at f_max·S\* ate 10× the gross of a 1-position wallet, so DD/bag/
  fidelity weren't on a common sleeve). Every wallet is normalized to the SAME gross budget G\* (total
  collateral, order of our real per-position×positions size): child_target_c = G\* · leverage_cap ·
  (leader_notional_c / leader_gross), so total follower gross ≤ G\*·leverage_cap regardless of how many
  coins the leader spreads across. Per-position return, DD, bag burden, and fidelity are then all measured
  on the common G\*. This is what makes "capital-agnostic ranking" real: we price copyability at a
  realistic gross budget (a wallet that only works at huge size ranks poorly), and bankroll enters ONLY at
  deployment (pool SIZE), never as a rank cap.
- **Censoring-aware return distribution** (#F-censoring, FIXED v2.2 — realize-quick-winners-hold-losers
  was gameable): the {r_i} distribution includes BOTH (a) CLOSED positions (exit_ts<cutoff) at realized r,
  AND (b) positions still OPEN at cutoff, marked to the cutoff price (r_i = cutoff MTM / peak notional).
  A held loser therefore enters mu/s/LCB at its losing mark — it can no longer be hidden by not closing.
  (Nothing after cutoff is ever used; open positions are simply valued AT cutoff, not dropped.)
- Per-position return = **child PnL / child peak notional**, BOTH from our follower execution at S\*
  (fixes #F1: v1's follower_pnl / leader_peak_notional mixed actors → not size-neutral). Denominator =
  max |our marked notional| over the journey under the evaluated policy.
- Core stats equal-weighted over MATERIAL positions only (dust fix #S4): a position counts iff its
  child peak notional at S\* ≥ K_dust × venue_min_order (K_dust pre-registered, e.g. 3). Sub-min
  positions are recorded but excluded from mu/s/LCB/support. **GATE on EQUAL-weighted mu only** (matches
  Alberto's per-position steer); notional-weighted mu is REPORTED as a diagnostic, not a second gate
  (dual-gate = extra tuning surface — Fable red-team).
- **S\* is PRE-REGISTERED with justification** (Fable: S\* is load-bearing for mu/s/LCB/dust-cut/
  concentration — a different S\* changes the ranking). Required: register S\* once, and REPORT a
  ranking-insensitivity check (Spearman of the rank at S\* vs 0.5·S\* and 2·S\* ≥ 0.9) so "capital-
  agnostic" is provable, not asserted.
- mu = mean(r_i), s = std(r_i), and LCB_mu via a **stationary/circular block bootstrap** over calendar
  blocks (see §7). Conviction preserved in execution, never fixed-dollar.

## 2. POINT-IN-TIME DISCIPLINE (fixes survivorship-in-discovery #L4, carry-in leak #L2, output leak #L5)
- **Point-in-time universe per fold**: candidate membership, tradable coins, entity/agent-parent mapping,
  and delistings must be as-of test_start_k. FAIL CLOSED if only a global/current snapshot exists (a
  July-built wallet list silently omits wallets that died earlier = survivorship). This is a build item.
- **M7 carry-in**: seed opening exposure from an ACTUAL proven state snapshot at train_start_k. If no
  snapshot exists, use future_delta_only (copy only in-window entries; do NOT credit pre-window entries).
  No backdating from the first later fill (#L2). REGIME NOTE (Fable): our M2 tracer proves flat points
  (last_all_flat), so most wallets HAVE a causal snapshot; wallets with no in-window flat run
  future_delta_only — state that selection effect (biases mildly toward in-window-entry edge, acceptable
  for a fast-realize book) rather than hiding it.
- **Cross-wallet DCA** (#D1 residual, Fable): a DCA ladder split across two agent wallets shows clean
  per-wallet stats. Collapse cross-wallet episodes via the M4 entity co-movement clustering before
  computing underwater_add_ratio; where clustering is unavailable, mark as ACCEPTED RESIDUAL RISK (not
  silently ignored).
- **Output semantics** (#L5): M6b computes each wallet's rank from PRETEST-only evidence per fold
  [train_start_k, test_start_k). The cross-fold wallet score = mean of per-fold pretest percentile ranks
  over folds where the wallet was rankable, with min_support_folds ≥ 3. OOS test-window outcomes are
  **validation reporting only** — they NEVER feed rank (aggregating OOS into rank = leak).

## 3. M6a — HIGH-RECALL BOUNCER (cheap proxy, no engine)
Hard gates (permissive): M5-eligible/canonical/valid lifecycle; ≥20 MATERIAL closed positions (exit<cutoff)
AND ≥3 active days; ≥1 NEW ENTRY in last 14d (#S5 fix: entries not exits — a copy opportunity is a new
entry we could follow); catastrophic-bag B_eq = Σ_open max(0,-UPnL_liq)/committed_capital ≤ 15%;
FIFO cohort-level remaining-principal of week-old bags ≤ 50% (#D2 fix: track original-bag remaining, not
cumulative close turnover); finite data. No performance/benchmark/significance HARD gate at M6a.
Proxy cost is turnover-weighted (#P2c): c_proxy_i = Σ_actions friction_a / child_peak_notional.
Soft score = within-fold percentile blend (dominant on mean, LCB, −std, +anti-DCA, −bag). 4 RESERVED
recall routes DEDUPED to 1000 UNIQUE with pre-registered backfill order + tie rule (#P2e). Top-N = engine
budget, NOT inference (no FDR here).

## 4. M7 v2 ENGINE (the workstream — emits per-position, policy-aware, canonical-cost evidence)
- **Import execution_model.py** for ALL fees/slippage/latency (#E-canon). Kill M7's private cost reimpl.
- **Emit per-position records** (not aggregate): r_i, child_peak_notional A_i, MAE_i (max adverse
  excursion), time_underwater_i, underwater-add state, friction decomposition (fees/slip/lag ≥0 per
  component), open-at-cutoff snapshot for the hidden-bag debit, AND a per-timestamp unrealized-PnL /
  committed-capital series (needed for gate-4b historical underwater + the deploy-level portfolio
  simultaneity — Fable) (#F-perpos). Open round-trips no longer silently discarded.
- **ONE copy policy, pre-registered — MIRROR-ENTRY + per-position TRAILING-TP exit** (#F-policyB, and
  this CLOSES #L3 for free — one fixed policy = no selection = no winner-bias, per Fable). Trail distance,
  trigger price, gap handling, addon handling, re-entry rule ALL specified; the current account-level
  follower_trail is REPLACED, not reused. FULL-MIRROR is kept only as a reported DIAGNOSTIC (never a
  selection candidate) — it's not a policy we'd deploy at our size (child trims fall below min order), so
  there is NO policy-selection step and NO X/Y knobs to tune.
- **No size-response curve** (Fable: at our size we never reach the capacity knee — 6× compute for
  nothing on a RAM-tight box). Replay at the single pre-registered S\*; capacity handled by a single
  executability check at the deploy sleeve in §6.
- **Passive-hold same-coin benchmark** (#E-bench, FIXED v2.2 — same-timing shadow was DEGENERATE: for a
  1-in/1-out trade it returns identical to the trade → alpha≡0). Benchmark = PASSIVE buy&hold of the same
  coin over the pretest window (coin beta), exposure-weighted to the wallet's average exposure on that
  coin. alpha_wallet = wallet's exposure-weighted realized return on coin c − passive-hold(c) return over
  the same window; aggregate across the wallet's coins. This rewards TIMING/SELECTION skill (did they beat
  simply holding what they traded) and removes coin beta. LCB on the cross-coin alpha via block bootstrap.
  (Per-coin mark series exists: hl_live_mark_collector + 1m candles — causal, confirmed.)
- **Intrabar-safe trailing stop** (#F-intrabar, FIXED v2.2): trail level set from PRIOR-COMPLETED-bar
  high; within the current 1m bar the stop is checked against the bar LOW and, if crossed, filled at
  trigger − slippage — a new high in the SAME bar does NOT retroactively raise the trail (worst-case
  same-bar ordering: adverse extreme first). Kills the fabricated-profitable-exit leak from unknowable
  intrabar high/low ordering.

## 5. M6b — FINAL JUDGE (post-M7-v2, capital-agnostic, full ranked list)
All metrics from M7 v2 at S\* under each wallet's pre-registered policy. Hard gates (each maps to a
V1-V17 death) — ALL thresholds ECONOMIC/pre-registered/frozen-before-folds (this REMOVES the multiple-
testing surface → drops BH-FDR entirely, #C-BH1/#C-BH2 resolved: no data-chosen thresholds = no FDR to control):
1. Support: ≥100 MATERIAL closed positions; ≥8 active calendar blocks AND effective-n (block-bootstrap
   independent-block count) ≥ 8 GATED, not just reported (#C-boot: 4 blocks = fake precision — Fable);
   ≥20 active days; NEW-ENTRY freq ≥3/day last 28d (full soft credit at 10); median hold ≤48h, p90 ≤7d.
2. Net edge after OUR execution at S\*: equal-weighted LCB_mu > pre-registered economic floor (e.g.
   > 1.5× RT cost), via block bootstrap (§7). PLUS held-out confirmation (§7). (Notional-weighted mu > 0
   reported, not gated.)
3. Cost wall (#D3 fix): matched-path frictionless replay (same fills, zero friction), nonneg friction
   per component; CCR = mu_ideal / robust_friction ≥ 2.0 where robust_friction = mean of POSITIVE d_i
   only (a favorable-lag fluke can't inflate it). 1.5× fee/slip stress: mu_stress > 0 (soft term; the 2×
   CCR buffer is the hard cost gate — #P2-stress acknowledged).
4. Hidden bags (#E-bags): (a) cutoff: P_conservative = P_realized + Σ_open min(0,UPnL_liq) > 0, open-loss
   burden ≤2% of committed_capital, open losses ≤25% of positive realized PnL; (b) HISTORICAL (new):
   peak underwater exposure over pretest = max_t(unrealized_loss_t / peak_committed_capital) ≤ pre-reg
   ceiling — closes the recovered-martingale tail-risk hole; committed-capital denominator defeats deposit
   dilution.
5. DCA-whale (#D1 hardened): reject if underwater_add_ratio > 0.60 OR (adds-while-underwater exposure-
   escalation present AND MAE distribution p90 > ceiling AND time_underwater fraction > ceiling). Close-
   and-reopen chains on the same coin within T_reset collapsed into one economic episode before computing
   (defeats VWAP-reset bypass). Soft term P(−underwater_add_ratio).
6. Concentration (per-wallet): C_position (on S\*-standardized dollar PnL, #P2-conc) ≤35%; drop-best-
   position (by S\* dollar PnL) leaves net-positive; per-wallet maxDD ≤15% (DIAGNOSTIC; portfolio DD is
   enforced at deployment — #E-dd).
7. Copy fidelity (#P2-fid, denominators specified): executed/intended notional ≥70% (intended =
   capacity-capped target at S\*; rejected entries count against; ignored trims under policy b are
   expected, excluded; forced exits counted); tracking error ≤20% vs the followed policy path; calibrated
   versioned costs.
8. Benchmark (#E-bench): per-journey alpha_i vs SAME-COIN shadow; gate LCB_alpha > 0 via block bootstrap
   (not mean>0). Removes coin beta → a levered SOL/HYPE beta specialist no longer shows fake alpha.
Soft score S_b = within-fold percentile blend, mean+std dominant ~45%: 0.25 P(mu_net) + 0.20 P(−s_net)
+ 0.15 P(LCB_mu) + 0.10 P(mu_stress) + 0.10 P(alpha_LCB) + 0.10 P(F_fast×realization) + 0.05
P(−underwater_add_ratio) + 0.05 P(copy_health). Win rate = 0.
**quality_weight** (#F-Sb, one formula): quality_weight_wallet = normalized cross-fold mean percentile
of S_b (∈[0,1], Σ=1 over pool). This is M6b's ONLY output weight; deployment weights are separate (§6).
No quintile buckets, no 10% ceiling.
M6b OUTPUT = the FULL ranked list of every wallet clearing the gates, capital-agnostic. Not capped.

## 6. DEPLOYMENT LAYER (capital enters ONLY here; #E1, #F-veto, #F-Kfeas)
Given the full M6b ranked list + equity E: walk top-down, funding subject to caps, to utilize capital max.
- Executability from the capacity curve: fund wallet w only if our actual sleeve at E clears
  K_dust×min_order and impact at that sleeve (read r(S) at deploy sleeve, not S\*) stays tolerable.
- Correlation-aware caps (#E-1coin fix): cluster coins by correlation; cap CLUSTER gross exposure ≤25%
  and SYNCHRONIZED-PEAK (max simultaneous, not average) exposure; leave-one-cluster-out pool PnL > 0.
- Portfolio DD constraint (#E-dd): enforce cross-wallet correlation caps + max portfolio drawdown +
  synchronized-peak cluster-exposure (from §6 clusters). NO CVaR (Fable: a tail you can't estimate from
  ≤12 correlated wallets over one pretest — false precision); the cluster synchronized-peak cap + max DD
  are the honest small-book risk controls.
- Pool benchmark VETO on a HELD-OUT window (#F-veto, FIXED v2.2 — was in-sample: choosing subset/caps/
  weights AND judging vs BTC/ETH on the SAME pretest lets subset-search beat the benchmark by chance):
  the portfolio (subset/clusters/caps/weights) is CONSTRUCTED on the selection pretest, then the veto is
  evaluated on the UNTOUCHED confirmation window (§7) — the portfolio must beat 50/50 BTC/ETH there on
  conservative return AND SORTINO (not Calmar — killed as short-window-gameable), else hold BTC/ETH/cash.
  No subset is judged on the data it was optimized on.
- K feasibility (#F-Kfeas): if executable K_eff < 5, the 20% wallet cap cannot fully allocate → explicit
  residual to CASH/BTC-ETH (utilize capital "as much as possible" = subject to caps, remainder parked; no
  silent cap violation).
- Weights over funded set = 75% equal (0.75/K_eff) + 25% shrunk inverse-risk max(LCB_mu,0)/(s²+s0²),
  cap 20%.

## 7. STATISTICAL DISCIPLINE
- **EMBARGOED OUT-OF-SAMPLE CONFIRMATION** (#C-BH1/2, FIXED v2.2 — "next-fold pretest" overlapped the
  selection pretest since folds are chained): SELECT a wallet on fold k's pretest [train_k, test_k);
  CONFIRM on fold k's TEST window (test_k, test_{k+1}) — genuinely held out of selection by construction,
  with a 1-block embargo between them to defeat autocorrelation bleed. The test window is short, so the
  confirmation bar is DELIBERATELY weaker/robust: sign-consistent edge (mean per-position alpha > 0 AND
  conservative net > 0 AND no new catastrophic bag), NOT a full 100-position LCB. Edge must PERSIST out of
  sample or the wallet is dropped.
- **Residual multiplicity, stated not hidden** (#C-BH2): with ~1000 wallets even a two-stage OOS filter
  leaves an expected O(few) false survivors. Controls: (a) the LAST fold is a FINAL UNTOUCHED HOLDOUT used
  in NO selection or confirmation — pool edge must survive there before any live capital; (b) report the
  expected false-survivor count each run; (c) the live-small validation ($50-200, 2-4wk) is the ultimate
  arbiter. No BH ceremony (no data-chosen thresholds to correct).
- **Block bootstrap** (#C-boot specified): stationary/circular bootstrap over CALENDAR blocks (14d);
  boundary-spanning journeys assigned by ENTRY block; null recentered to 0; require ≥8 active blocks for
  LCB rankability (4 = fake precision). Report effective-sample diagnostic.
- **Percentiles** (#P2-pct): within-fold rank for scoring; cross-fold aggregate = mean per-fold percentile
  (uniform per fold → comparable); ties = average ranks; discrete/degenerate-fold rule stated.
- Thresholds frozen before viewing ANY of the 12 OOS folds; if calibration needed → fold 1 only, stamp
  exploratory, freeze, judge 2-12.

## 8. PER-FINDING CLOSURE (25 P1)
L1 exit<cutoff §1 · L2 carry-in snapshot §2 · L3 pre-registered policy §4 · L4 point-in-time universe §2
· L5 output pretest-only §2 · C-BH1/BH2 drop FDR→economic thresholds §5/§7 · C-boot ≥8 blocks+spec §7 ·
S4 material-position/dust §1 · S5 entries-not-exits §3/§5 · D1 DCA hardened (MAE/time-uw/escalation/
episode-collapse) §5 · D2 FIFO remaining-principal §3 · D3 CCR robust-friction §5 · E1 standardized sleeve
+capacity curve §1/§6 · E-canon import execution_model §4 · E-bags historical underwater+committed-cap §5 ·
E-bench same-coin shadow+LCB §4/§5 · E-dd portfolio CVaR at deploy §6 · E-1coin correlation clusters §6 ·
F1 child/child at S\* §1 · F-policyB real per-position trailing TP §4 · F-perpos M7 emits per-position §4 ·
F-veto veto→deployment §6 · F-Sb quality_weight formula §5 · F-Kfeas K_eff<5→cash §6.
P2: stress soft+CCR-buffer ack · percentiles §7 · fidelity denominators §5 · concentration S\*-dollar §5 ·
route dedup/backfill §3 · c_direct turnover-weighted §3.

## 9. v2.1 DELTAS (Fable red-team of v2)
- L3 closed FOR FREE by dropping to ONE pre-registered policy (no selection, no X/Y knobs) §4.
- C-BH real close = HELD-OUT next-fold confirmation §7 (not "no FDR needed").
- C-boot: effective-n GATED not just reported §5.1.
- S\* pre-registered + rank-insensitivity check §1 (S\* is load-bearing).
- Gate-2: gate EQUAL-weighted mu only; notional-weighted reported §1/§5.
- §4 emits per-timestamp unrealized/committed series (for gate-4b + portfolio simultaneity).
- Deploy veto Calmar→Sortino; CVaR→cluster synchronized-peak + max DD §6.
- COMPUTE BUDGET: 1 policy + no size-curve = 1× M7 (was 6×) → fits the RAM-tight box; still smoke a
  slice with /usr/bin/time -l before the full 1000×12 run (mandatory streaming-IO rule).
- Cross-wallet DCA via M4 entity clustering or accepted-residual §2.

## 10. v2.2 — Pile-A design-bug fixes (Codex gate r2), the 6 that change the design
1. **Benchmark** §4: same-timing shadow was DEGENERATE (alpha≡0 for 1-in/1-out) → PASSIVE same-coin HOLD
   over pretest, exposure-weighted; alpha = wallet return − passive-hold; rewards timing, removes beta.
2. **G\* wallet gross budget** §1: per-position S\* didn't equalize wallets (10 positions = 10× gross) →
   normalize every wallet to a common GROSS budget G\*; per-position child derived from G\* (so every "at
   S\*" in gates now means "at the G\*-derived child size"); DD/bag/fidelity on common G\*.
3. **Censoring-aware {r_i}** §1: include open-at-cutoff positions marked to cutoff → can't hide losers by
   not closing.
4. **Intrabar-safe trailing stop** §4: prior-completed-bar trail + worst-case same-bar ordering → no
   fabricated exits from unknowable 1m high/low order.
5. **Held-out portfolio veto** §6: construct portfolio on pretest, evaluate BTC/ETH veto on the untouched
   confirmation window → no subset-search-beats-benchmark-by-chance.
6. **Embargoed OOS confirmation** §7: select on fold-k pretest, confirm on fold-k TEST window (+1-block
   embargo); final untouched holdout fold before any live capital; report expected false-survivors.

## 11. WHAT STAYS FOR BUILD-TIME (Pile B — pinned in CODE, codex-reviews the DIFF, not prose)
Exact trailing-TP numeric params (distance/activation/re-entry) as an immutable manifest; full M7-v2 emit
schema (follower entry/exit_ts, side, block key, intended targets, rejected notional, per-action frictionless
+ stressed PnL, underwater-add num/denom, shadow/alpha, timestamped coin/side/gross/cash/realized/equity for
cluster + portfolio DD); block-bootstrap params (one-sided level, resamples, seed, block length, effective-n
estimator, spanning-journey rule) frozen in a manifest; committed_capital formula; cluster/coin/fee/calib
point-in-time as-of; weight renormalization for K≥5 (cap = max(0.20, 1/K), redistribute residual to cash).
M6a materiality: since M6a is pre-engine it CANNOT know follower child_peak_notional — decide at build:
a cheap leader-side proxy (accept mild mismatch, documented) OR a lightweight M7 pre-pass. Codex flagged
this (r2 §3) — resolve in code with the eng review.

## NEXT (BUILD — Alberto GO TG 11577; A = build then codex/fable eng-review the implementation)
GATE 0 orientation FIRST (M7 = battle-tested v15_m07_engine.py → EXTEND, do NOT rebuild). Build order:
(1) M7-v2 engine extension (per-position policy-aware canonical-cost emit + intrabar-safe trailing TP +
censoring marks + G\* normalization) — the biggest lift, streaming-IO mandatory, smoke a slice with
/usr/bin/time -l first; (2) M6a config; (3) M6b config; (4) deployment layer; (5) re-run on recency base
(12 folds to Jul-13). CODEX + FABLE ENG REVIEW on each real diff (Alberto TG 11577). NO live capital until
the validation ledger passes on the final untouched holdout + live-small.
