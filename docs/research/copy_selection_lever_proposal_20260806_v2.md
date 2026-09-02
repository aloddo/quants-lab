# Lever Proposal v2: Wallet-Cohort Selection for Faithful Copy (2026-08-06)

v2 changes vs v1 (both on record): every lever now stated MECHANISM-FIRST with an evidence-strength
grade (Alberto TG 12207: "not telling me why"); capital geometry corrected (Alberto TG 12209: per-coin
leverage = HL defaults, TOTAL gross cap 5.0x); Phase-2 parity plan is now Fable-APPROVED (v1 rejected
with 9 findings, all incorporated; record on the card). Nothing runs before lever agreement. Every
threshold is a hypothesis to validate, not a discovered optimum.

**How to read the grades:**
- **[MEASURED]** = this exact failure burned us, dated, with numbers.
- **[LIT]** = established finance literature, cited.
- **[HYP]** = untested hypothesis; we say so and mark what would demote/promote it.

**Jargon, decoded once:**
- **Fold** = a consecutive 14-day time block of history.
- **OOS** (out-of-sample) = a wallet is picked using earlier folds and judged ONLY on later folds it
  was not picked on. Anything else is curve-fitting.
- **Copied return** = the return of OUR simulated replica of the wallet ($100 fixed, our 4s latency,
  our 8.64bps round-trip fees, our exit rule) — never the leader's own PnL.
- **Marked** = every still-open position is valued at market at every measurement boundary, so
  unrealized losses count. Closed-only accounting lets a bag-holder look clean by never selling.
- **BH-FDR q=0.10** = Benjamini-Hochberg false-discovery-rate control: testing ~20,000 wallets hands
  you hundreds of lucky fakes by chance alone; this caps the expected share of fakes among survivors
  at ~10%.
- **LCB** = lower confidence bound: instead of ranking on the estimate, rank on the pessimistic end
  of its confidence interval, which automatically punishes short/noisy histories.
- **Census** = every eligible wallet, no top-N pre-cut.

**Capital math, honest (verified live 2026-08-06: HL $452.96 + Bybit $465.96 = $918.92, flat):** a
validated +25-50 bps/position at 5-15 positions/day is roughly $37-$225/month. This deployment does
not reach $500 MRR at current equity; it buys a VALIDATED, K-robust, matched-null-significant edge —
the asset that scales with capital or sells as a signal.

---

## 1. UNIVERSE & DATA PRECONDITIONS (audit P0s — not levers, just truth-of-inputs)

1. **Funding backfill 2026-05-25..06-08** — RUNNING since 10:38 (15 missing days inside the research
   window; M07 models funding hourly, a hole biases every carry-heavy wallet).
2. **Fresh full M02 rebuild through 2026-08-05** — census actions/journeys are 24 days stale; all old
   M07/M06b artifacts predate open-position marking and are unusable (audit P0).
3. **Immutable as-of data manifest**, hash-pinned, fail-closed on any missing period, registered
   before anything reads it.
4. **Full census, no truncation** (the old top-1,000 activity cut silently changed a universe verdict).
5. **Lifecycle-invalid journeys excluded before features**, quantified by reason.
6. **Entity views**: wallet-only and high-confidence as co-primary; the broad temporal graph
   (476-wallet components) as sensitivity only. All longitudinal joins keyed on wallet address
   (entity_id collided twice and manufactured false conclusions both times).
7. **Holdout fenced NOW**: most recent 4 weeks locked, touched exactly once by the frozen final
   configuration.

---

## 2. HARD FILTERS — mechanism first, one subsection per lever

All conjunctive (pass ALL or out). All measured on OUR replica's copied flow, never the leader's own
journey economics. Order = cheap structural kills first, expensive statistical confirm last.

### 2.1 Authenticity kills (wash / delta-neutral / funding-farm / entity-size) — [MEASURED]
**Mechanism:** a wallet whose fills are self-trades, hedged-neutral legs, or funding harvesting has a
PnL stream that does not come from directional skill; copying its entries buys noise. **Evidence:**
the census run that accidentally skipped this filter (empty-parquet incident) let sybil/operator
duplicates top the screen. **Knob:** M4 defaults; loader now fatal on empty inputs.

### 2.2 Lifetime dollar perp PnL > $0, non-disablable — [MEASURED, the recent9 killer]
**Mechanism:** we copy entries AND exits faithfully — their exits ARE our exits. A trader who has
destroyed money over their life has demonstrated negative expectancy of exactly the decision stream
we will mirror. "We size differently so their losses don't transfer" was tested and is FALSE
(equal-weighted returns were WORSE than their size-weighted: -26.5 vs -12.2 bps on 0x8c10d629).
**Evidence:** recent9 armed 8/9 lifetime-negative leaders (~$166k destroyed between them) after I
disabled this gate on a thesis; result -$18.60 in 43h at 18.2% win rate. Carhart 1997: the LOSER leg
is the only robust persistence in performance data — repeat losers keep losing. **Knob:** arm-gate
default exists; GAP: add as blocking research-funnel gate (M5/M6b) so it can never be "disabled at
arm time" again. Override only by measurement (the equal-vs-size-weighted decomposition), never by
argument.

### 2.3 Leader account equity >= $10k — [HYP + one measured incident; weakest lever, says so]
**Mechanism:** (a) skin in the game: a $0-equity wallet's track record belongs to someone who left
or blew up — the signal is stale by construction; (b) HL specifically is full of point-farming and
bot-test wallets that optimize airdrop points, not profit; low equity is the cheapest proxy for
"trades for money"; (c) dust-sized fills are noisy to classify, so tiny accounts are also less
measurable. **Evidence:** the 07-24 run's sole FDR survivor (0x1bc1) had a $0 account and -$7,876
lifetime. **Honest status:** weakest lever proposed. If rejected, demote to diagnostic. Its real
value is shrinking the multiple-testing load: every junk wallet excluded before testing makes the
FDR correction less punishing for genuine wallets. **Knob:** account_gate MIN_PERP_EQUITY_USD;
GAP research-side.

### 2.4 Recency: last fill <= 7d — [MEASURED]
**Mechanism:** a selection is a forecast that the wallet will keep trading; a dormant wallet has
zero forward opportunity and its seat is dead capital. **Evidence:** totalreturn5 screen: 143 of 285
top-screened wallets were DEAD (some 81+ days), four of the proposed five were one dormant operator.
**Knob:** m06a recency_gate (tighten 14d -> 7d) + arm MAX_STALE_DAYS=7.

### 2.5 Sample floor: >= 25 replica positions, >= 3 active folds, >= 50 pooled OOS journeys — [LIT + MEASURED]
**Mechanism:** per-trade crypto PnL is heavy-tailed; small samples make lucky wallets
indistinguishable from skilled ones, and the ranking then selects FOR luck (winner's curse).
**Evidence:** Bailey & Lopez de Prado (2012/2014) minimum track-record length: heavy tails demand
far more history than Gaussian intuition suggests; our 07-24 thin-6-fold roster was judged
net-negative-EV. **Knob:** m06b pp_min_positions / g5_min_active_pretest_folds /
oos_min_journeys_pooled.

### 2.6 Hold-time band: 30min <= median <= 48h, p90 <= 168h — [MEASURED twice, floor and ceiling]
**Mechanism (floor):** our copy arrives ~4s late and our benchmark data is 1-minute bars. A wallet
holding 2 minutes gives back a structural fraction of its edge to our latency (it was 229% of the
fast wallets' median round trip on totalreturn5 — every one of 1,630 wallets negative at our delay),
and 1m bars cannot even MEASURE sub-5-min holds (16-30% of journeys were same-bar, which silently
inflated the alpha5 metric). **Mechanism (ceiling):** multi-day holders are bag-carry profiles
(your <2d rule) whose risk lives in the open-position tail our bag gates then have to catch.
**Knob:** M5 hold floor/ceiling; m06b pp_max_med_hold_h; GAP: expose p90 knob to CLI/yml.

### 2.7 Latency feasibility: copy-latency / median-hold <= 2% — [MEASURED]
**Mechanism:** the fraction of the trade's life we miss is roughly the fraction of its edge we
forfeit; 2% caps the structural forfeit. **Evidence:** the totalreturn5 kill sat at 229%; the
copyable risk-11 all sat under 2.8%. **Knob:** GAP in M5 (components exist).

### 2.8 Bag-risk / martingale veto — each component separate — [MEASURED repeatedly + LIT]
**Mechanism:** the martingale profile (average down, never realize, wait) produces beautiful
win-rates and medians right up to the ruin event; every component below catches one face of it, and
compressing them into one score is how it kept slipping through. Components: underwater-add share
<= 0.20 (adding to losers IS the martingale act); loser-hold/winner-hold <= 2.0 (Odean 1998
disposition effect: holding losers longer than winners); MAE p90 <= 15% and p99 <= 600bps (how deep
they let positions bleed); closure fraction >= 0.90 and open-loss/realized-profit <= 0.5 (profits
realized while losses stay open = the hiding pattern the marked accounting exposes); liq rate
<= 0.5% (forced exits are information-free and we copy them). **Evidence:** econ20's 0x1efb — 100%
long, winners held 3.14h vs losers 33.49h, 97.7% win rate that was survivorship of one regime;
the 06-14 skill cohort re-admitted 30% martingales the moment the veto was dropped from the builder;
7/53 census confirms had uw-add > 0.20; printalpha3's best name had p99 MAE 1,629bps. **Knob:**
pp_max_mtm_dd exists; GAP: the other components must become CLI/yml-settable m06b gates, INSIDE the
builder (the 06-14 lesson: a veto in a side analysis does not survive the next rebuild).

### 2.9 Two-sidedness: 25% <= long share <= 75% — [MEASURED + your spec]
**Mechanism:** a 100%-one-direction wallet is one beta bet plus averaging, not a decision stream;
its "skill" is regime luck, and it concentrates the whole cohort on one market direction.
**Evidence:** econ20's 0x1efb (100% long martingale); gate1's cohort was ~55% correlated = one
crypto-beta bet. Your own spec (TG 11793). **Trade-off named:** also excludes honest directional
specialists — that is Q3. **Knob:** GAP, add to M5 (profiled already, never gated).

### 2.10 Leverage <= 10x median gross/equity — [MEASURED tail + LIT]
**Mechanism:** we copy exits faithfully, and a liquidation IS an exit — forced, worst-price,
information-free. A high-leverage wallet lives near its liq price; its PnL has a ruin branch that a
handful of folds can miss entirely (looks clean until it doesn't — the same shape as the martingale
class). **Evidence:** census: the 1.2% of positions ending in liquidation carried ~30x the average
loss. **Knob:** M5 LEVERAGE_CAP (enforce in copyability lane); arm MAX_ACCOUNT_LEVERAGE.

### 2.11 Coin accessibility >= 80% of notional — [MEASURED class]
**Mechanism:** we can only copy flow that (i) trades coins in our live universe and (ii) clears
HL's $10 min order at our $100 size. If only 60% of a wallet's notional is copyable, its measured
edge sits on flow we cannot capture and does not transfer to what we deploy. Same failure class as
totalreturn5 (selected on 2-min holds our latency could never touch) — time dimension there, coin
dimension here. Each wallet is measured ONLY on its copyable flow. **Knob:** M5 ACCESSIBLE_FRAC_MIN
+ M9 min_order_notional; parity plan adds the coin-admission filter to M07 (Fable divergence row 8).

### 2.12 Operator dedup: one seat per entity — [MEASURED]
**Mechanism:** one human running four addresses is one decision stream; four seats = 4x concentration
masquerading as diversification. **Evidence:** 07-26: four proposed "wallets" were one dead operator
(coin-set Jaccard 0.67-1.00). **Knob:** M4 union-find (on-chain provable); GAP: temporal/Jaccard
dedup at M6a.

### 2.13 Copyability confirm (the exit into ranking) — [MEASURED + LIT]
**Mechanism:** everything above is structural; this is the only statistical filter, and it asks ONE
question: did the simulated COPY of this wallet make money on data it was not selected on,
repeatedly? Positive pooled OOS copied return in >= 60% of >= 4 folds, BH-FDR q=0.10, on the full
census. **Evidence:** in-sample rank was proven noise on our data (07-23: pretest->test rank
correlation 0.045); half the "skilled" leaders were copy-negative once OUR costs were applied
(06-22: skill-but-copy-negative cohort at -4.16%); Barras-Scaillet-Wermers 2010: most apparent
fund alpha is lucky zero-alpha; Harvey-Liu-Zhu 2016: after mass testing, raise the discovery bar.
**Knob:** m06b oos_min_folds / oos_min_frac_folds_pos / fdr_q.

---

## 3. RANKING — ordering the survivors

**Statistic: one-sided 95% lower confidence bound on the MEDIAN per-position net copied return,
conservative-MARKED, pooled OOS, keyed on wallet address.**

- **Median, not mean** [MEASURED]: econ20 admitted 0x1efb on mean +67.6 while its median was +1.0 —
  the mean is a tail lottery in heavy-tailed per-trade PnL (Bailey-LdP), and platform incentives
  manufacture exactly that shape (Doering 2015).
- **Marked, not closed-only** [MEASURED/audit P0]: closed-only accounting rewards delayed loss
  realization — the bag-holder's core trick.
- **LCB as shrinkage** [LIT]: ranking on the pessimistic bound auto-shrinks short/noisy histories
  toward zero (James-Stein 1961; Jones-Shanken 2005: the top of a raw leaderboard is where
  estimation error concentrates).
- **Minimum effect size AFTER FDR: pooled OOS mean >= +25bps/position AND LCB > 0** [MEASURED]:
  recent9's FDR survivors at +4.6/+6.2/+6.9 bps were post-correction noise costing -$2,183/30d.
  +25bps ≈ 3x that noise band ≈ 3x our RT fee. This is Q4 — the one knob that trades roster size
  against false-discovery cost.
- **K protocol** [MEASURED]: the A-F comparison's winner (+10.52% at K=20) flipped to -2.38% at
  K=40 — a conclusion that dies when you change cohort size is not a conclusion. Deploy only if the
  economic verdict agrees at K=10/20/40. Target K=20; short rosters leave seats in CASH; thresholds
  never relax to fill seats.
- **Primary configuration: equal weight of all qualifiers, seats in LCB order** (audit contract's
  "14-day equal-weight or strongly shrunk ensemble"). The F-style shrunk ensemble is the single
  named alternative; the full-census A-F rerun adjudicates. DeMiguel 2009: 1/N beats optimized
  weights at our sample sizes.

---

## 4. PORTFOLIO CONSTRUCTION — corrected geometry (Alberto TG 12209)

- **Sizing: $100 notional per position, equal weight** (the validated live sizing; validator
  enforces). Per-position leverage: **HL coin-level defaults (3/5/10/20/40x), never overridden.**
- **TOTAL gross exposure cap: 5.0x equity** (your directive). At HL $452.96 today: ~$2,265 gross
  ceiling ≈ 22 concurrent $100 legs. Secondary guard margin_util <= 0.7 (~$317 usable margin):
  never binds before 5x on 10-40x majors; binds first (~$950-1,585 gross) only if the book
  concentrates in 3-5x tail coins. **Encoded identically in three places or parity is fiction:**
  M09 manifest (gross budget 5.0x — its default 3.0x), live config gross gates at deploy (old
  2.0/3.0/4.0x superseded), arm-gate roster declaration.
- **Bybit consolidation is NOT required for K=20** (my v1 arithmetic conflated notional with margin
  — corrected, see corrections page). Consolidation only adds dollar-distance to the -15% global
  stop and burst headroom. Q2 is now buffer-preference only.
- **Opportunity budget — the frequency-trap fix** [MEASURED]: recent9's worst wallet emitted 1,679
  fills/7d and supplied 31 of our 33 round trips while the best emitted 211/30d — per-signal equal
  treatment IS frequency weighting, and frequency was inversely correlated with quality. Fix: max 5
  new entries/wallet/day and no wallet > 20% of the cohort's copied entries per window; excess
  logged as budget-capped. GAP: add to M09 and live symmetrically (parity requires both).
- **Caps:** per-entity <= 15% of gross; per-coin <= 20% of gross, coin-side 2.0x (gate1's ~55%
  correlated one-beta-bet is the trace); pairwise-correlation cap exists in M9 (rho_max 0.70) but
  currently receives an EMPTY dict — GAP: wire real overlap correlations.
- **Cash is a position.** Unfilled seats stay cash; nothing is relaxed to fill a roster.

---

## 5. REFRESH & DEMOTION

- **Cadence 14d** [structural]: 14d folds can only certify a 14d refresh decision; 28d runs as
  sensitivity. Faster is untestable on our fold geometry; slower is not safer (Hendricks 1993,
  Agarwal-Naik 2000: winner rank decays within one evaluation period).
- **Demotion goes to CASH, never to the next untested wallet** (audit contract). Triggers: stale
  data (7d) · bag breach on marked economics · eligibility failure at refresh · entity ambiguity ·
  target-vs-actual reconciliation failure · **post-copy behavior drift** (leaders take MORE risk
  once copied — Pelster-Hofmann 2018, Apesteguia 2020 — so the disposition signature is re-measured
  every refresh, not once at selection).
- **Permanent exclusion ledger:** negative-expectancy or MAE-dropped wallets never silently
  re-admitted (0xc6ab8b64 precedent); re-admission needs a new registry entry with new evidence.

---

## 6. WHAT WE DELIBERATELY DO NOT SELECT ON — and the mechanism of each rejection

| Rejected lever | Mechanism of failure | Killed by |
|---|---|---|
| Raw/recent leader return, leaderboard rank | Selects regime luck + tail lotteries; chases what just mean-reverts | Method C -2.54%; totalreturn5 0/1,630; Carhart's weak winner leg |
| Mean per-trade anything | Tail lottery in heavy tails | econ20 mean +67.6 vs median +1.0 [MEASURED] |
| Win-rate / Sharpe / Calmar composites | High win + low realized DD IS the martingale signature when losses stay open | 06-14 cohort: 30% martingales re-admitted [MEASURED]; your TG 11793 |
| Regime-switched rosters | 1 down-high-vol fold, 0 up-high-vol folds: a regime model memorizes 2-3 observations | audit; regime survives only as a stress test |
| Leader's own journey PnL | 86.1% of journeys contain adds/trims we do not copy; validates THEIR strategy, deploys a different one | alpha5 halt [MEASURED] |
| Alpha vs 1m-candle benchmark for minute-holders | Same-bar entry/exit forces beta to 0, inflating alpha to raw return | alpha5 resolution break [MEASURED] |
| Per-wallet optimized weights | Estimation error swamps the gains at our n | DeMiguel 2009 |
| Per-decision copy selection | Killed twice at -16bps | 06-25, V10 class [MEASURED] |

---

## 7. OPEN QUESTIONS (updated after TG 12209)

1. **Lifetime-PnL gate scope** — binding at research AND arm, override only by measurement? (§2.2)
2. **Capital geometry, restated** — 5.0x gross at HL defaults is now encoded as your directive.
   Remaining sub-question only: consolidate Bybit -> HL for stop-distance buffer, yes/no? (K=20 at
   $100 fits HL alone.)
3. **Two-sidedness** — hard filter (proposal) or diagnostic-only? It excludes honest one-direction
   specialists (§2.9 trade-off).
4. **Effect-size floor** — +25bps/position with LCB > 0: approve or amend. (§3)
5. **Holdout burn** — fence most recent 4 weeks now, one touch, fail = no deploy: approve.

---

## 8. VALIDATION PLAN (Phase 2 parity plan is Fable-APPROVED as of 2026-08-06)

**Phase 0 (correctness):** funding backfill (running) -> full M02 rebuild (quiet-RAM window) ->
M03/M04/entity views -> parity work per the APPROVED v2 plan: m07 opt-in features (fixed-$ sizing,
flatten-only reversal mode, exit-latency, optional stop layers) each with disabled==byte-identical
golden tests -> shared decision fixture -> flag-gated live fixes (entry reconciliation chase-bounded,
partial-fill retry) AFTER the fixture exists, codex per diff -> execution-model unification staged
(fees+latency+interface first; m07's richer slippage becomes the provider BEHIND execution_model.py;
old-vs-new A/B on pinned fills) -> blocking research-side gates from §2 GAPs.

**Certification (Fable P0 redefinition):** three registered stages — (1) decision-parity fixture,
(2) signal parity on >= 10 real leader-days (TWAP-heavy / xyz / dust classes), (3) end-to-end
reconciliation against the REAL recent9 live window (71 fills, n=33 round trips) with pre-registered
residual budget, engine-SHA pinning, and measured capture rate. Deliverable language is always
"equivalent on certified paths, capture X%, residual Y bps over n=33" — never blanket "equivalent".

**Phase 1 (one immutable manifest):** M05 (new blocking gates) -> M06a full census -> M07
parity-configured -> M06b (FDR + effect floor + LCB + marked) -> M08 -> M09 (gross 5.0x, caps §4,
kills at live's -15%) -> M10 (matched-null + baselines incl. cash, trailing-return, trailing-Sharpe,
static, behavior-only, matched-random) -> **A-F rerun on full census at K=10/20/40 across entity
views** -> one-touch holdout -> ledger rows -> arm small at $100/pos on your GO.
