---
type: project
title: M6 Redesign — Fable+Codex Consensus (relative per-position, copyability-true)
date: '2026-07-23T06:15:00Z'
agent: quant-engineer
owner: quant-engineer
status: CONSENSUS reached (2 strategist rounds) + Alberto correction 2026-07-23 (rank is CAPITAL-AGNOSTIC; deploy/capital moved out of ranking). Pending Alberto GO + final Codex adversarial gate before code.
venture: quant
tags:
  - v15
  - m06
  - copy-trading
  - filtering
  - ranking
  - consensus
---

# M6 Redesign — Fable + Codex Consensus (2026-07-23)

Method: Alberto (TG 11559) — two independent strategists (Fable + Codex) on M6 pre-filter + judge,
relative per-position metrics + V1-V17 kill-lessons, loop to consensus. Round 1 = independent
opinions (research/v15/m6_redesign_fable_opinion.md, m6_redesign_codex_opinion.md). Round 2 = Fable
adjudicated Codex's 6 deltas → ACCEPT all 6 with 2 de-scopes.

## ⚠️ CORRECTION APPLIED (Alberto TG 11566, 2026-07-23) — RANKING IS CAPITAL-AGNOSTIC
I had injected "~$950 capital" into the shared strategist brief → biased BOTH panelists toward a
bankroll-capped pool (Codex's "deploy ≤12", Fable conceded). WRONG on two counts:
(1) it contaminated the independent panel; (2) it conflated RANKING with DEPLOYMENT.
FIX (binding): M6 ranks the best, most profitable COPYABLE wallets on their OWN merit, capital-agnostic
— full ranked list, NO bankroll-driven pool cap. Capital utilization + deploy-count + relative sleeve
sizing live in a SEPARATE DEPLOYMENT layer (below / M9), objective = utilize capital as much as
possible. Correction: corrections/quant-engineer/2026-07-23-1.
NOTE: the return metric r = pnl / leader PEAK notional is already a ratio → leader $ never distorts
the ranking (whale ≡ minnow at equal %); relative sleeve sizing is handled in M7 replay; capacity/
liquidity (can we fill our sleeve on their coins) stays a copyability GATE. Bankroll is NOT a rank input.
Consensus below, with all $950/≤12 logic moved OUT of ranking into the DEPLOYMENT section.

## THE UNIT (both agree)
Evidence unit = the complete round-trip POSITION (journey; aggregate partial fills + scale-ins).
Per-position relative return:
  A_i = peak notional = max_t |marked_notional_{i,t}|
  r_i = (realized PnL after fees + slippage + lag + trailing-stop execution) / A_i
Peak notional — NOT margin, equity, or raw $ → leverage- and wallet-size-neutral; does not reward
DCA escalation. Core stats equally weighted by position: mu = mean(r_i), s = std(r_i), and a
one-sided 14-day BLOCK-BOOTSTRAP lower confidence bound LCB_mu (fills/journeys are correlated — a
naive t-stat overstates significance). Conviction is preserved in EXECUTION (M9), not erased:
child_target_fraction = wallet_sleeve × clip(leader_notional/leader_equity, 0, f_max). Never
fixed-dollar child positions (V1-V12 root cause #3).

## INTRA-JOURNEY DYNAMICS (Alberto TG 11568-11570, confirmed 2026-07-23)
A journey's internal add/trim structure changes how we HANDLE and COPY it. Already modeled in our
pipeline: tracer runs an ENTRY/ADDON/TRIM/EXIT/REVERSE state machine, records n_entry/n_addon/n_trim/
n_exit fills + peak notional + realized PnL through the true scale-in/out VWAPs, and classifies each
journey (fast-flip=open→close · accumulation=adds-only · scale-out=trims-only · position=adds+trims ·
scalp/swing by duration). M7 already copies at CHILD-ACTION granularity (targets leader exposure% per
add/trim, pays our fee+slip+lag on each child fill, FOLLOWER_TRAIL_EXIT override available). Therefore
r = pnl/peak_notional already prices any add/trim pattern, and the cost/fidelity/tracking gates already
penalize high-churn journeys. TWO ADDITIONS (confirmed):
- **DCA-WHALE DETECTOR (new, real gap — nothing flags this today).** underwater_add_ratio =
  (addon notional added while position in unrealized LOSS vs entry VWAP) / (total addon notional),
  per journey; aggregate per wallet (notional-weighted). Adds-while-underwater = averaging down =
  deep-pocket edge we cannot follow at $-neutral sleeve (V1-V12 killer). Adds-while-in-profit =
  scaling into winners = copyable skill. USE: M6b soft term P(-underwater_add_ratio) + a HARD gate at
  a pre-registered ceiling (e.g. wallet-level underwater_add_ratio > 0.60 → reject as un-copyable DCA).
  Also useful as an M6a soft term. Threshold economic/pre-registered, frozen before folds.
- **COPY-POLICY as a per-wallet tested dimension.** M7 evaluates each wallet under >=2 policies:
  (a) FULL-MIRROR every add/trim; (b) MIRROR-ENTRY + OUR trailing-TP exit (ignore their trims —
  our standing "always trailing TP"). Some wallets' edge is in entries (→ policy b), others in full
  scale management (→ policy a). M6b judges each wallet under its BEST-net-of-cost policy and records
  which policy won (that policy is what deployment executes). This is the "how we copy it" lever.

## M6a — HIGH-RECALL BOUNCER (cheap, no engine)
Hard gates (permissive — do NOT reject on estimated negative net edge; M6a's cost proxy is crude):
- M5-eligible, canonical copyable entity, valid lifecycle.
- n_closed_positions >= 20 AND >= 3 active days pretest.  (was n>=5 — conceded as too loose)
- >= 1 action in the last 14 days (recency).
- Catastrophic-bag gate: B_eq = Σ_open max(0, -UPnL_liq)/equity <= 15%.
- Realization gate: for positions opened >= 7d before cutoff, >= 50% of opened notional closed.
- Finite return data.
Soft score (WITHIN-FOLD PERCENTILE ranks P(·), no z-scores):
  S_a = 0.30 P(mu_proxy) + 0.20 P(LCB_mu,proxy) + 0.20 P(-s_proxy)
      + 0.10 P(F_fast) + 0.10 P(-B) + 0.05 P(-C_position) + 0.05 P(alpha_bench)
  r_proxy_i = leader_realized_PnL_i / A_i − c_direct_i  (c_direct = all-taker follower fee +
  calibrated coin-class slippage; no invented lag without M7).
  F_fast = fraction closed <72h (report median + p90 hold). B = max(open-loss/equity,
  open-loss/positive-realized-PnL). C_position = largest positive-PnL contribution / Σ positive PnL.
  alpha_bench = position return − direction-matched 50/50 BTC/ETH over the same interval.
Shortlist N=1000 via 4 RESERVED RECALL ROUTES (so one composite can't gate recall):
  750 by S_a · 150 by mu_proxy · 50 by lowest s_proxy (cond mu_proxy>0) · 50 by benchmark-residual LCB.
  Top-N is an ENGINE-BUDGET cut, NOT multiple-testing discipline (FDR lives at M6b).

## M6b — FINAL JUDGE (post-M7 our-cost replay; locks pool)
Only M7 positions under canonical execution_model.py. NO fallback to source-wallet ROE in an
investable run. Hard gates (7 families — each maps to a V1-V17 death):
1. Support/activity: n_closed >= 100; >= 4 active 14d blocks; >= 20 active calendar days; last-28d
   freq >= 3 closed/day (full soft credit at 10/day); median hold <= 48h, p90 <= 7d. (5 RT = noise.)
2. Net edge after OUR execution: LCB_mu > 0; passes pre-registered 5% fold-level Benjamini-Hochberg
   FDR across the M7 shortlist (FDR at M6b ONLY — M6a's cut is budget, not inference).
3. Cost barrier (the wall): frictionless/leader-timestamp replay d_i = r_ideal − r_net;
   CCR = mu_ideal / max(mean(d), eps) >= 2.0. Maker economics count only if M7 models limit fill-prob
   + adverse selection, else all-taker. (1.5× fee/slip stress stays SOFT in S_b, not a 2nd hard gate.)
4. Hidden bags: debit losers, never credit winners →
   P_conservative = P_realized + Σ_open min(0, UPnL_liq). Gates: open-loss burden <= 2% equity;
   open losses <= 25% of positive realized PnL; >= 80% of notional aged >72h closed; P_conservative > 0.
5. Concentration: C_position <= 35%; drop-best-position stays net-positive; follower maxDD <= 15%.
   One-coin specialists NOT hard-rejected for specialization — handled at POOL level (below).
6. Copy fidelity: executed/intended notional >= 70%; capacity-capped intended <= 20%; tracking
   error <= 20%; calibrated + versioned costs.
7. Benchmark: PRIMARY = per-position excess gate mean(x_i) > 0, x_i = r_i − direction-matched 50/50
   BTC/ETH over same interval. Pool VETO = selected pool must beat 50/50 on conservative return
   (else hold BTC/ETH/cash — do not force deployment). [De-scoped from Codex: the daily
   R_copy=α+βR_50/50 CAPM regression + LCB_α gate is statistically empty on ~10 daily pretest points
   → demote to reported diagnostic only.]
Soft judge score (percentiles): S_b = 0.25 P(mu_net) + 0.20 P(-s_net) + 0.15 P(LCB_mu)
   + 0.15 P(mu_stress) + 0.10 P(alpha) + 0.10 P(F_fast × realization_coverage)
   + 0.025 P(-concentration) + 0.025 P(copy_health). Mean+std = dominant 45%. Win rate = ZERO weight.

## M6b OUTPUT = FULL RANKED LIST (capital-agnostic)
M6b emits EVERY wallet that clears the 7 gate families, ranked by S_b + a quality_weight — the
complete ranked investable universe, NOT capped by our bankroll. However many pass, pass (could be
dozens to hundreds). This is the answer to "the best and most profitable copyable wallets." Ranking
inputs are all per-position, our-cost, sizing-neutral ratios — account size is never an input.

## DEPLOYMENT LAYER (SEPARATE from ranking — M9; capital enters ONLY here)
Given the full M6b ranked list + current equity E, deploy to utilize capital as much as possible:
- Walk the ranked list top-down, funding wallets subject to portfolio caps: expected gross exposure
  per coin <= 25%; no wallet > 20% of expected pool PnL; leave-one-wallet-out AND leave-one-coin-out
  conservative pool PnL both > 0 (HYPE risk lives here).
- Executability floor (why a small book can't fund the whole list): only fund a wallet if our
  proportional sleeve clears the venue min order — K_effective bounded by
  0.8·E / q95(simultaneous child positions) >= 2 × min executable order. At small E this naturally
  lands ~single digits; at larger E it grows. This is a DEPLOYMENT constraint, not a rank filter —
  the ranked list is identical regardless of E.
- Sleeve sizing preserves conviction: child_target_fraction = wallet_sleeve ×
  clip(leader_notional/leader_equity, 0, f_max). Never fixed-dollar.
- Weights across the funded set = 75% equal (0.75/K_eff) + 25% shrunk inverse-risk edge:
  w_raw = 0.75/K_eff + 0.25 · [max(LCB_mu,0)/(s²+s0²)] normalized; cap 20%. KILL quintile buckets +
  10% ceiling.

## KEEP / CHANGE / KILL
KEEP: fold-pretest purity, fail-closed provenance, M6a top-N as engine cap, recency/activity, M7 as
sole final copyability judge, calibrated+versioned fee/slippage, capacity/fidelity/DD as diagnostics.
CHANGE: aggregate ROE → equal-weighted per-position realized return on peak notional; n≥5 → n≥20
(M6a)/n≥100 (M6b); binary positive-block "consistency" → actual s + block-bootstrap LCB + stress edge;
soft-only capacity/fidelity → minimum investability GATES; realized-only → realized + open-loss debit;
n_pool=100 fixed cap → FULL ranked capital-agnostic list (deploy-count decided downstream, not in rank);
mixed z-scores → percentiles.
KILL: ROE×count×persistence M6a score; aggregate realized ROE as primary M6b term; Calmar as 2nd
return term; WIN RATE (payoff-blind, selects martingale/hidden-bag profile); mixed z/[0,1] scoring;
ANY bankroll/capital input in the RANKING (moved to deployment); quintile buckets + 10% ceiling; any
production fallback to uncalibrated cost / missing fidelity / unrealized MTM return.
THRESHOLD DISCIPLINE: all frozen before viewing any of the 12 OOS folds; economic/structural, not
tuned on fold outcomes. If calibration needed → fold 1 only, stamp exploratory, freeze, judge 2-12.

## RESIDUAL DELTAS (Fable de-scoped Codex; minor; final Codex gate will rule)
1. Benchmark: per-position excess gate (primary) + pool veto (diagnostic), NOT the daily CAPM α-LCB
   regression (empty at 14d). 2. 1.5× stress = soft term, not a 2nd hard gate (CCR≥2 is the cost gate).

## IDEAL-WORLD JUDGE (both, first-principles)
Don't rank wallets independently or score their PnL. Score the CAUSAL marginal contribution of
copying each wallet INTO the current portfolio, under our exact latency/order-type/fill-prob/adverse
selection/capital/impact/stops/cross-wallet+coin correlations; then maximize expected log growth
s.t. drawdown/CVaR/liquidity/concentration. Object = marginal portfolio utility after execution, not
wallet profitability. M7 replay + per-position excess is the honest practical approximation.

## NEXT
Alberto GO on this consensus → then FINAL Codex adversarial gate on the written spec → then M6a/M6b
code changes (CONFIGURE existing v15_m06a_shortlist.py + v15_m06b_ranking.py, not rebuild) → re-run
on the fresh recency base (12 folds to Jul-13). No code until GO + gate.
