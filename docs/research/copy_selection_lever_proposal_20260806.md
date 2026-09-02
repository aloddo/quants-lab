# Lever Proposal: Wallet-Cohort Selection for Faithful Copy (v1, 2026-08-06)

Prepared for LP review. Nothing below runs before explicit approval. Every threshold is a pre-registered hypothesis to validate, not a discovered optimum (audit contract, final report section 6). The 2026-08-03 selection contract is adopted in full; where we extend it, the extension is argued inline. Every lever traces to a named failure of ours, a cited paper, or the audit contract; levers that traced to none were deleted during drafting (leader ROE, win-rate, Calmar, and leader-alpha-vs-passive-benchmark did not survive, see section 6).

**Capital math, stated up front (verified live 2026-08-06: HL $452.96 + Bybit $465.96 = $918.92, flat).** At $100/position and a validated +25-50 bps/position net, 5-15 positions/day yields roughly **$37-$225/month**. This deployment does NOT reach $500 MRR at current equity; what it buys is a VALIDATED, K-robust, matched-null-significant edge — the asset that scales with capital (the 74x equity gap already on record) or sells as a signal. That is the honest objective of this proposal.

---

## 1. UNIVERSE & DATA PRECONDITIONS

No selection computation starts until all of the following are true. These are the audit P0s plus the census-integrity lessons; they are preconditions, not levers.

1. **Funding backfill complete.** The 15-day hole 2026-05-25..2026-06-08 in `app/data/hl_s3_funding_hot/` is still missing as of 2026-08-06 (verified). Backfill first; funding sits inside the research window and M07 models funding hourly.
2. **Fresh full M02 rebuild.** Canonical census actions/journeys are stale at 2026-07-13 (24 days); the incremental path last advanced to 2026-07-24 and its latest run OOM-aborted (`m02_journeys_daily/closed/run_000002.errors.json`). Rebuild the full corpus through the least-fresh raw watermark (2026-08-05), memory-guard-compliant per the 2026-05-31 streaming-IO directive, then M03-M07 from complete inputs. All existing M07/M06b artifacts predate open-position marking and are unusable (audit P0 #3).
3. **Immutable as-of data manifest.** Expected UTC days for fills/candles/funding/ledger/marks, schema fingerprints, hashes, event-time bounds, wallet-universe hash; fail closed on any missing period (inspection gate A.1). Registered in `app/data/v15/experiment_registry.jsonl` before anything reads it.
4. **Full census, no truncation.** M06a manifest `shortlist_n_per_fold=25000` (universe ~20,378), `score_basis=activity_only`, `recency_gate=true`. The top-1,000 activity truncation invalidated the A-F panel (audit P0); never again.
5. **Lifecycle-invalid exclusion before features.** Position/journey invariants replayed from `startPosition` chains; duplicates, non-finite rows, broken causal chains excluded and quantified by reason/date (inspection gate A.2-A.3). No invalid row reaches selection.
6. **M04 authenticity fold-pure and provably non-empty.** The `census20k_20260728` empty-parquet incident ran an entire cohort search with zero authenticity filtering; the loader is now fatal on empty files. Keep it fatal.
7. **Entity views: wallet_only and high_confidence as co-primary; broader as sensitivity only.** All observed links are medium-confidence temporal (audit P1; fold 12 has a 476-wallet component). All longitudinal pooling keyed on `primary_wallet`, never positional `entity_id` (the 07-29 key-collision P0).
8. **Holdout reserved and locked now**, before any lever is tuned: the most recent 4 weeks of data are fenced, touched exactly once by the single frozen configuration (audit contract; one-touch rule).

---

## 2. HARD FILTERS (eligibility, binary in/out)

All filters conjunctive (Alberto TG 11994: the conjunctive absolute bar beat the composite by +91.9bps OOS vs +2.3). All measured on the wallet_only view and re-checked on high_confidence.

| Lever | Proposed threshold (hypothesis) | WHY: our failure + literature | Code knob (or GAP) |
|---|---|---|---|
| Authenticity kills (wash, delta-neutral, funding-farm, entity-too-big, member-kill) | M4 defaults (wash>0.50 kill, net/gross<0.20 AND price-var<0.30 kill, funding>0.5 kill, entity>8 wallets uncertain) | Empty-M4 incident ran unfiltered; sybil/operator dupes topped the 07-26 screen | M4 constants; fatal-on-empty loader |
| **Lifetime dollar perp PnL > 0** (HL API, full history), non-disablable | > $0 | recent9: 8/9 lifetime-negative leaders armed (~$166k destroyed), the gate's own default passed 0/9 and was disabled on a thesis, not a measurement (Alberto TG 12182/12185: hard gate). Carhart 1997: the loser leg is the ONLY robust persistence; exclude demonstrated negative expectancy permanently | `account_gate.py MIN_LIFETIME_PERP_PNL` (arm-time only). **GAP: not enforced in the research funnel**; add as blocking M5/M6b gate. M5 net-PnL floor is currently non-blocking in the copyability lane; make it blocking in all lanes |
| Leader account equity floor | perp equity >= $10k as-of | double-validated-8: sole FDR survivor 0x1bc1 was a real-money loser with a $0 account; gate1 L1 used $20k-5M | `account_gate.py MIN_PERP_EQUITY_USD`; research-side GAP (M5 MIN_EQUITY_USD is equity-lane only) |
| Recency | last fill <= 7d at selection as-of | totalreturn5: 143/285 top-screened wallets were DEAD; 07-26 dead-81-days operators | m06a `recency_gate` (14d block) + arm gate `MAX_STALE_DAYS=7`; tighten m06a to 7d or add M5 check |
| Sample floor | >= 25 completed replica positions AND >= 3 active pretest folds AND >= 50 pooled OOS journeys | audit contract starting thresholds; Bailey & Lopez de Prado 2012/2014 MinTRL: heavy-tailed per-trade PnL needs far more track than Gaussian intuition; 07-24 thin-6-fold overfit | m06b `pp_min_positions=25`, `g5_min_active_pretest_folds=3`, `oos_min_journeys_pooled=50` |
| Hold-time band | 30min <= median hold <= 48h; p90 <= 168h | Floor: totalreturn5 kill (4s latency = 229% of median round trip; 0/1,630 wallets positive) plus alpha5 resolution failure (1m candles cannot resolve 1-5min holds). Ceiling: Alberto TG 11854 "<2 days"; V9 DCA whales | M5 `HOLD_FLOOR_S` (currently 60s, raise), `SWING_MAX_HOLD_S=48h`; m06b `pp_max_med_hold_h=48`, `pp_max_p90_hold_h=168` (**GAP: p90 knob is dataclass-only, not CLI/yml; expose it**) |
| Latency-feasibility | share of journeys closing < 4.0s <= 0.25; copy-latency / median-hold <= 2% | totalreturn5 (kill sat at 229%; the 07-29 risk-11 all under 2.8%); live measured 4.0s | M5 `P95_COPY_LATENCY_S` / `SHARE_BELOW_LATENCY_CAP`; ratio gate is a GAP (add to M5) |
| Bag-risk / martingale veto (each component separate, not compressed into one score, per audit) | underwater-add share <= 0.20; loser-hold/winner-hold <= 2.0; MAE p90 <= 15% (pp_max_mtm_dd); p99 MAE <= 600bps; closure fraction >= 0.90; open-loss/realized-profit <= 0.5; liq rate <= 0.5% | V9 DCA blind spot; V17 skill cohort 30% martingale after the 06-05 veto was dropped from the builder; econ20 0x1efb (winners 3.14h, losers 33.49h); printalpha3 p99 MAE 1,629bps; census 7/53 uw-add>0.20, 4/53 liq>2%. Odean 1998 disposition (PGR/PLR); Doering 2015 lottery payoffs; Bailey & LdP drawdown under serial correlation. Veto must live INSIDE the builder (06-14 lesson) | m06b `pp_max_mtm_dd=0.15` exists; uw-add exists only in A-F diagnostic (loose 0.60); **GAP: loser/winner hold ratio, p99 MAE, closure fraction, open-loss/realized-profit, liq rate are not m06b gates; add all as CLI/yml-settable gates** |
| Two-sidedness | 25% <= long share <= 75% | Alberto wallet spec TG 11793; econ20 0x1efb 100%-long martingale; gate1 55%-correlated single beta bet | **GAP** (profiled in TG 12006 output, never gated); add to M5 |
| Leverage | median daily gross/equity <= 10x | census liq-tail finding; audit cap list | M5 `LEVERAGE_CAP` (equity lane only; **GAP: enforce in copyability lane**); arm `MAX_ACCOUNT_LEVERAGE` |
| Accessibility / feasibility | notional-weighted accessible coins >= 0.80; >= 50% of actions clear $10 min notional at $100/pos | mrr-gate-unreachable (07-29): edge on inaccessible frequency is worthless at our capital | M5 `ACCESSIBLE_FRAC_MIN`; M9 `min_order_notional` |
| Operator dedup | one seat per entity (union-find) + coin-set Jaccard >= 0.5 collapses to one seat | 07-26: four proposed wallets were one dead operator (Jaccard 0.67-1.00) | M4 union-find (on-chain provable); **GAP: Jaccard temporal-overlap dedup not in funnel; add at M6a** |
| Copyability confirm (the exit filter into ranking) | pooled OOS net copied return positive in >= 60% of >= 4 OOS folds, BH-FDR q=0.10 across the full eligible census | SKILL-but-copy-negative wallets dragged the cohort to -4.16% (06-22); Barras-Scaillet-Wermers 2010: naive top-decile = lucky zero-alpha; Harvey-Liu-Zhu 2016 raised discovery bar | m06b `oos_min_folds=4`, `oos_min_frac_folds_pos=0.60`, `oos_min_journeys_pooled=50`, `fdr_q=0.10` |

Everything above is measured on the **replica we will actually trade**: M07 in parity configuration (fixed $100 notional, flatten-only reversal, live exit-latency model, live stops), never on the leader's own journey economics (alpha5 lesson: 86.1% of journeys had addons/trims we do not copy).

---

## 3. RANKING (ordering the eligible)

**Primary statistic: the one-sided 95% lower confidence bound (moving-block bootstrap) on the median per-position net copied return, computed on conservative-marked economics, pooled OOS, keyed on primary_wallet.**

- **Copied return, not leader return.** First established 06-05, violated repeatedly since; SKILL-vs-copy+ stratification proved leader skill and copyability are different axes. Platform slippage-gap evidence (section 6.2 of the literature review): the copy gap alone flips thin leaders negative.
- **Median, not mean.** econ20 admitted 0x1efb on mean alpha +67.6 while its median was +1.0 (lesson `2026-07-25-median-not-mean-and-always-benchmark`); Bailey & LdP: heavy-tailed per-trade PnL makes the mean a tail lottery; Doering 2015: platform incentives manufacture exactly that payoff shape.
- **Conservative-marked, not closed-only.** Audit P0: closed-only economics rewarded delayed loss realization. Every open position marked at every rebalance boundary; ranking return includes all negative open PnL; realized and marked views reported side by side (m06b `conservative_roe` v3 path, requires post-audit M07 artifacts).
- **LCB as shrinkage.** The LCB shrinks noisy and short histories toward zero automatically: James-Stein 1961 (sample means inadmissible), Jones-Shanken 2005 (the top of the raw leaderboard is where estimation error concentrates), and our own 07-23 result (in-sample rank rho 0.045 = noise). This is the v25 pre-registration lever, finally executed.
- **Minimum effect size after FDR: pooled OOS mean net >= +25bps/position AND LCB > 0.** recent9 rule 3: the +4.6/+6.2/+6.9bps FDR survivors were post-correction noise costing -$2,183/30d; after ~20k tests the bottom of an FDR list is where false discoveries live. +25bps is roughly 3x that noise band and ~3x our RT fee. Implemented via the never-yet-set knobs `oos_margin` and `pp_min_lcb_mean_r`.
- **Lookback and refresh.** Rank on trailing ~120d (the pretest fold stack); refresh every 14d. Agarwal-Naik 2000: persistence is quarterly at best; Hendricks 1993: winner rank decays within one evaluation period; and 12 non-overlapping 14d folds can only identify a 14d refresh decision (audit).
- **Ordering is for seat assignment only; weights are equal** (section 4). Where the evidence is genuinely split: the A-F panel had F (shrunk ensemble) winning at K=10/20 and flipping negative at K=40. Primary configuration is therefore **equal-weight of all FDR-plus-effect-size qualifiers, seats filled in LCB order** (the audit contract's "14-day equal-weight or strongly shrunk ensemble" candidate); the F-style percentile ensemble is the single named alternative, and the full-census A-F rerun with K-concordance decides between them. No third option.
- **K protocol.** Evaluate K=10/20/40 on the full-census rerun. Deploy only if the economic conclusion (positive after-cost compounded return, acceptable DD, matched-null significance after family-wise correction) is the SAME at all three (audit contract; the 08-03 sign-flip at K=40 is the decisive falsifier). Target K=20; if fewer qualify, empty seats stay in cash. Never relax a threshold to fill a roster.

---

## 4. PORTFOLIO CONSTRUCTION

- **Equal weight, fixed $100 notional per position** (the validated, authorized live sizing; validator refuses non-fixed). DeMiguel-Garlappi-Uppal 2009: 1/N beats optimized weights at our sample sizes; audit contract mandates conservative equal weights. No per-wallet weight optimization, ever.
- **Opportunity-weighting fix for the frequency trap.** recent9: the worst wallet (1,679 fills/7d) supplied 31/33 of our round trips while the best (211 fills/30d) was barely sampled; frequency was inversely correlated with quality, and per-signal equal treatment IS frequency weighting. Fix: a per-wallet entry budget so each seat contributes approximately equal expected notional-days per refresh window. Hypothesis: max 5 new entries/wallet/day and no wallet > 20% of the cohort's copied entries per window; excess signals dropped, logged as budget-capped. **GAP: not in code; add to M09 and the live engine symmetrically** (parity requires both sides).
- **Caps** (audit contract: per-entity, coin, gross, leverage, capacity):
  - Per-entity <= 15% of gross exposure (M9 `per_entity_cap`, tighten from 0.40).
  - Per-coin <= 20% of gross; same-side coin cap 2.0x (live `coin-side` gate exists; gate1's 55%-correlated HYPE bet is the trace).
  - Gross <= 2.0x equity (live `gross_open` gate; tighter than M9's 3.0 default). At HL-deployable $452.96 that is ~9 concurrent $100 legs; consolidating the Bybit $465.96 into HL doubles it to ~18, consistent with K=20 minus vacancy (LP decision, question 2).
  - Pairwise correlation: M9 `rho_max=0.70` exists but the correlation dict is currently passed empty, a silent no-op. **GAP: wire real same-side overlap correlations into M9.**
- **Cash fallback.** Unfilled seats earn cash (A-static ran at 0.417 seat coverage by design). Cash is a position, not a failure.
- Sizing basis divergence (live $100 fixed vs M07 10%-of-sleeve) is closed by teaching M07 `fixed_notional_usd` before any validation run (parity map recommendation B1).

---

## 5. REFRESH & DEMOTION

- **Cadence: 14 days** (hypothesis; the only cadence our fold structure identifies; 28d run as sensitivity, never as primary). Hendricks 1993 and Agarwal-Naik 2000 both say winner persistence decays within one evaluation period, so slower is not safer.
- **Demotion triggers** (any one demotes the seat to cash at the next boundary, immediately on the starred items):
  1. *Stale data: no leader fill 7d, or our data watermark falls behind.
  2. *Mark loss / bag breach: open-loss/realized-profit > 0.5, MAE p90 breach, underwater-add detected post-inclusion (audit contract).
  3. Eligibility failure at refresh (any section 2 filter).
  4. Entity ambiguity: wallet joins a multi-wallet component or dedup flag (audit contract).
  5. *Target-vs-actual reconciliation failure (audit contract; parity P0 #5).
  6. **Post-copy behavior drift**: rising disposition signature (loser-hold/winner-hold, PGR/PLR) after we start copying. Pelster & Hofmann 2018: leaders' disposition effect strengthens once they gain followers; Apesteguia 2020: copied leaders escalate risk. One-shot ranking is insufficient; this monitor runs on every refresh.
- **Fallback discipline.** A demoted seat goes to CASH, not to the next untested wallet (audit contract). Repeated demotions across the cohort move the whole sleeve to cash.
- **Permanent exclusion ledger.** Wallets demonstrated negative-expectancy or MAE-dropped are never silently re-admitted (0xc6ab8b64 precedent; Carhart: repeat-loser persistence is the strong signal). Re-admission requires a new registry entry with new evidence.

---

## 6. WHAT WE DELIBERATELY DO NOT SELECT ON

| Rejected lever | What killed it |
|---|---|
| Raw / total / recent leader return, leaderboard rank | Method C: -2.54% at K=20, negative in every decile; totalreturn5 killed at -1.04%/fold universe-wide; wikifolio and CHI 2024 crypto copy studies: leaderboard-chasing yields zero/negative subsequent returns; Carhart: the winner leg is weak |
| Mean per-trade anything | econ20 (mean +67.6 vs median +1.0 admitted a martingale); Bailey & LdP heavy tails |
| Leader win-rate / Sharpe / Calmar composites | V17 skill cohort: z(win)+z(sharpe)+z(-DD) re-selected 30% martingales; high win-rate + low realized DD is the martingale signature, not skill; Alberto explicitly rejected Calmar/ROE/win-rate (TG 11793). Drawdown risk is handled by the component bag gates instead, per the audit's "do not compress into one opaque score" |
| Raw Sharpe without deflation/MinTRL | Bailey & LdP 2012/2014: max Sharpe over 20k wallets is noise by construction; enforced via LCB + FDR + effect floor instead |
| Regime-switched rosters | Only 1 down/high-vol fold and 0 up/high-vol folds exist (audit); Bailey et al. 2014 AMS: per-regime ranking on sparse labels is fit on 2-3 observations. Regime is a stress test (survival in the worst sub-window), never a switch |
| Leader's own journey PnL (with their adds/trims/exits) | alpha5: validated the leader's strategy, deployed a single-entry replica; 86.1% of journeys had addons/trims. Only the parity-configured replica sim counts |
| Alpha vs same-coin passive benchmark at 1m resolution | alpha5: same-bar entry/exit forced beta to 0, inflating alpha to raw return for exactly the minute-hold wallets; Alberto TG 11854. Stays a diagnostic for wallets with holds >> 15min, never a selection statistic |
| Per-wallet optimized weights | DeMiguel 2009; estimation error swamps gains |
| Recent-window-only bootstrap ranking | 07-24 thin-6-fold cohort: judged net-negative-EV; correction `2026-07-24-1` |
| Per-decision / per-trade copy selection | Killed twice (06-25 KILL at -16bps; V10 class) |
| Broad temporal entity graph as primary population | Audit P1: all links medium-confidence; 476-wallet components; sensitivity view only |

---

## 7. OPEN QUESTIONS FOR ALBERTO

1. **Lifetime-PnL gate scope.** We propose lifetime dollar perp PnL > 0 as non-disablable at BOTH research (M5/M6b) and arm time, per your post-recent9 rule. This will cut the eligible pool hard (possibly by half or more). Confirm: binding at both stages, no thesis-based override, only a measurement-based one?
2. **Capital geometry.** Verified live: HL $452.96 deployable + Bybit $465.96 idle = $918.92. At $100/position with gross <= 2.0x, HL alone supports ~9 concurrent legs; consolidating Bybit into HL supports ~18 (K=20 with cash fallback). Three sub-decisions: (a) consolidate Bybit -> HL, yes/no; (b) $100/position K=20, or $50/position widening effective K toward 40 (better diversification per DeMiguel; same bps fee drag, half the $ per seat, but closer to the $10 min-notional floor on trims/dust); (c) confirm $100 stays the validated sizing the validator enforces. Our proposal: consolidate, K=20 at $100.
3. **Two-sidedness as hard filter.** 25-75% long share is your own spec (TG 11793) and would have excluded econ20's 0x1efb, but it will also exclude honest one-directional specialists. Hard filter (our proposal) or refresh-time diagnostic only?
4. **Effect-size floor.** +25bps/position pooled OOS mean (with LCB > 0) is our hypothesis, ~3x the recent9 noise band and ~3x RT fees. Approve, or set a different number? This is the single knob that most directly trades roster size against false-discovery cost.
5. **Holdout burn authorization.** We reserve the most recent 4 weeks now and touch it once with the single frozen configuration; if it fails, the answer is "no deploy", not a second configuration. Approve the fence dates and the one-touch commitment, and confirm the pilot after a passing M10 dev verdict is live_small at $100/pos only.

---

## 8. VALIDATION PLAN SKETCH

Ordered; each step is a registered, hash-pinned run. No step starts before its predecessor's registry row exists.

**Phase 0, correctness (audit P0s):**
1. Funding backfill 2026-05-25..2026-06-08; S3 completeness green.
2. Full M02 rebuild through 2026-08-05 (streaming writer + memory guard; smoke slice with `/usr/bin/time -l` first per the binding directive), then M03/M04 (fold-pure, non-empty) and entity views (wallet_only, high_confidence, broader).
3. **Parity work before any scoring run:** teach M07 `fixed_notional_usd` sizing, flatten-only reversal semantics, exit-latency model (~25-30s + 90s grace + $10 dust floor), and optional live-stop layers (SL -25%, global -15%); align latency constants to measured 3.2-4.0s across `execution_model.py` and M07 CLI. **Compliance finding to close here: M07 today does NOT price fills through the canonical `execution_model.py`** — it carries its own SLIP_BANDS cost model, and three research latency constants disagree (execution_model 1000ms, M07 CLI 2000ms, M07 params 4000ms) vs 3210ms measured live. The CLAUDE.md binding rule ("no sim may hardcode its own slippage/fee/latency") requires unifying these on one execution contract before any number is produced. Build and pass the shared event-sequence parity fixture (reversal, disconnect, duplicate, rejection, partial fill, restart, multi-leader; run once with stops on both sides, once off both sides). Live-side fixes queued in parallel: partial-fill residual retry, target-vs-actual entry reconciliation.
4. Code gaps from section 2/4: blocking lifetime-PnL and net-PnL gates, bag-gate components, two-sidedness, latency-ratio, Jaccard dedup, p90-hold CLI exposure, per-wallet entry budget, M9 correlation wiring. Each lands with tests; codex review on the M07 changes.

**Phase 1, funnel run (one immutable manifest, full_census-style yml):**
5. M05 copyability lane with the new blocking gates -> M06a full census (25,000 seats, recency-gated) -> M07 parity-configured replica sim (pretest + test, marked opens, `start_equity=937.47`) -> M06b V2.2 (pp gates + FDR q=0.10 + `oos_margin=0.0025` + `pp_min_lcb_mean_r>0` + conservative_roe, investable must come out true) -> M08 survival -> M09 (b0=937.47, equal weight, caps per section 4, kill thresholds set to live's -15% for the parity variant) -> M10 (G1-G7, matched-null dev 95th, baselines: cash, market beta, equal-weight eligible, trailing-return, trailing-Sharpe, static, behavior-only, matched-random).
6. **A-F rerun on the full census** (audit requirement): identical eligibility/execution/as-of contracts, marked opens, all three entity views, K=10/20/40. This is what adjudicates equal-weight vs F-shrunk-ensemble (section 3) and provides the K-concordance verdict. All attempted variants logged with dispositions, winners and losers alike.
7. **Holdout: one touch.** Frozen configuration only. Pass = proceed to arm; fail = no deploy, back to research with a new fence.

**Go-live ledger rows** (append-only registry + lineage artifacts, all SHA-256 pinned): data_manifest; m02_rebuild; entity_views; parity_fixture_result; m05/m06a/m07/m06b/m08/m09/m10 runs; AF_full_census (with every K and view); holdout_touch; deployment_manifest (selected_wallets == live config); lineage gate PASS (`tools/research_lineage_gate.py`, all seven REQUIRED_GATES true); `.ARM_COPY` with config sha256 and roster-declared thesis thresholds set to STRICT defaults this time (min_equity 10k, min_lifetime_pnl 0, max_stale_days 7, max_leverage 10); pre-registered live kill thresholds recorded before the first order. Pilot is live_small at $100/pos; `lp_confirmatory` requires the holdout-percentile M10 pass.