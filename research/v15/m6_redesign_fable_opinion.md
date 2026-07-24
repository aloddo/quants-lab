# M6 redesign — Fable strategist opinion (2026-07-23, round 1)

Context: Alberto (TG 11559) asked for two independent strategist opinions (Fable + Codex) on
redesigning M6a pre-filter + M6b judge around RELATIVE per-position metrics (avg return/position,
std-dev of return, realized PnL to expose hidden bags) + lessons from why V1-V17 cohorts were killed.
Brief: /tmp/m6_strategist_brief.md (also summarized in card recency-true-base-rebuild).
Codex was usage-limited until Jul 28 19:27 — round-1 has Fable only; Codex/2nd-model pass pending Alberto's choice (TG 11561 A/B/C).

## Fable's opinion (verbatim)

CORE REFRAME: unit of evidence = the round-trip journey, not the wallet-window. Per-journey relative
return r_i = realized_pnl_i / max_notional_i (size-invariant; notional-relative measures skill,
margin-ROE just rewards leverage). Everything builds on the triple (mu, sigma, n) of {r_i} and its
excess vs the coin traded.

M6a PRE-FILTER (bouncer):
- Hard gates: (1) n_journeys_closed >= 5 pretest; (2) recency active last 14d (KEEP); (3) STALE-BAG
  gate — no open position older than 14d at test_start AND unrealized_drag = min(0, open_mtm)/equity
  >= -10% (direct hidden-bags defense at the door, cheap); (4) median hold <= 48h (realize-fast).
- Soft score: score = t_stat × recency_term, t = mu_realized / (sigma/sqrt(n)) on {r_i}. The t-stat
  IS the correct fusion of Alberto's mean + std + sample size; sqrt(n) replaces the hamfisted
  log1p(n). Keep top-N=1000/fold pre-registered (no threshold on score → nothing to overfit).
- Cost at M6a: loose sanity only, mu_gross >= 1x c_RT (~10-12bps). Do NOT demand margin here (recall).
- KEEP: pre-registered N, recency gate, pretest purity, fold-pure M4, fail-closed contracts, cache.
- CHANGE: score basis → per-journey t-stat.
- KILL: the roe × persistence × log1p(nj) × (1-dd) multiplicative score (aggregate/absolute, ugly
  sign behavior under COPYABILITY_ONLY, dd-clamp redundant once sigma in denominator, persistence
  double-counts recency + sqrt(n)). Keep persistence as a reported diagnostic only.

M6b JUDGE (after M7 our-cost replay, locks pool):
- Hard gates: n_round_trips>=5, n_fills>=30, exposure>=3d (KEEP); realized-only basis (KEEP);
  COST-MARGIN gate mu_gross_our >= 2x c_paid per round-trip (the wall; buffer set from economics,
  not data); CONCENTRATION leave-one-out — mu_realized excluding single best coin AND single best
  journey must both stay > 0 (HYPE failure as a gate); BENCHMARK per-journey excess x_i = r_i −
  r_coin,i over the journey's own window, gate mean(x_i) > 0 (kills beta + DCA-deep-pockets).
- Soft score: 0.35 z(t_stat_net) + 0.20 z(mu_excess) + 0.15 consistency + 0.10 capacity_health +
  0.10 fidelity − 0.10 z(sigma) − 0.15 survivability_penalty.
- KEEP: consistency, capacity_health, fidelity, survivability, provenance fail-closed, investable
  stamp, n_pool=100, 10% ceiling, quintile buckets.
- CHANGE: replace z_realized_roe + z_calmar with z(t_stat_net) + add excess-vs-coin term.
- KILL: the 0.15 win_rate weight — high win-rate selects the martingale/hidden-bag profile (many
  small wins, one fat hidden loss) we're defending against.
- Threshold discipline: all economic / structural / pre-registered rank cuts; none tuned on scores.
  If calibration needed → fold 1 only, stamp exploratory_calibration, freeze, judge folds 2-12.

IDEAL-WORLD JUDGE: don't score the wallet's PnL — score it as a signal generator: information
coefficient of its entries, E[coin return over our realizable horizon | wallet entered, at our lag]
− c_our, shrunk toward zero ∝ 1/sqrt(n). M7 replay + realized per-journey excess is the honest
practical approximation.

## Pending
- Alberto to choose (TG 11561): A) 2nd Claude model now + Codex gate Jul 28; B) wait for Codex; C) top up now.
- Then round 2: converge the two opinions, resolve disagreements, bring consensus back to Alberto.
