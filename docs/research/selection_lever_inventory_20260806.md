# V15 Copy-Selection Funnel — Complete Lever Inventory (from code, 2026-08-06)

Funnel order: M4 authenticity → entity views → M5 eligibility → M6a shortlist → M7 engine (sim knobs that feed selection) → M6b ranking + OOS confirm → M8 survival → M9 portfolio sim → M10 gates → research-lineage gate → arm/account gate. Orchestrated by `scripts/experiment.sh` (yml key → CLI flag maps are hard-fail on unknown keys).

## Stage M4 — Authenticity kills + entity dedup (`research/v15/v15_m04_authenticity.py`, constants in `v15_m025_authenticity_gate.py`)

| stage | lever/knob | what it measures | default | where set | status notes |
|---|---|---|---|---|---|
| M4 | `LOOKBACK_DAYS` (`--lookback-days`) | signal lookback window before as-of | 90 | m025:44, m04 CLI `--lookback-days` | fold-pure runs stamped `as_of_ms == test_start` |
| M4 | `WASH_EXCLUDE` | wash-trade fraction → KILL "wash" | 0.50 | m025:51 | module constant, not CLI/yml |
| M4 | `WASH_REVIEW` | wash fraction band → SUSPICIOUS "wash_borderline" | 0.20 (0.20<x<=0.50) | m025:52, m04:88 | module constant |
| M4 | `NET_GROSS_NEUTRAL` + `PRICE_VAR_NEUTRAL` | delta-neutral KILL (BOTH: net/gross <0.20 AND price-pnl var frac <0.30) | 0.20 / 0.30 | m025:48-49, m04:78 | module constants |
| M4 | `NET_GROSS_BORDER` | quality reason code band (`q:net_gross_borderline`, handed to M5, never tiers) | 0.30 | m025:50, m04:93 | reporting only |
| M4 | `FUNDING_FARM_FRAC` | funding-dominated PnL → KILL "carry_pnl" | 0.5 | m025:72, m04:80 | module constant |
| M4 | `ENTITY_MAX_WALLETS` | union-find entity too big → UNCERTAIN "entity_too_big", no primary | 8 | m025:64, m04:274 | module constant |
| M4 | `ALLOC_WEIGHT` tier→weight | confidence multiplier consumed by sizing chain | KILL 0.0 / SUSPICIOUS 0.10 / UNCERTAIN 0.25 / CLEAN 1.0 | m04:61 | mirrored as `conf_map` in M8:297 and M9:452 |
| M4 | CLEAN criterion | `l3_pass_standalone` AND confidence != LOW; else UNCERTAIN | n/a (rule) | m04:103-106 | thin history / nan metric → UNCERTAIN |
| M4 | entity member-KILL rule | any member own-KILL kills the whole entity; internal-hedge between members → KILL | n/a (rule) | m04:268-272, 316-327 | provable-on-HL only |
| M4 | `copyable` | tier != KILL AND has primary AND is primary | derived | m04:368, 399 | the predicate M5/M6a/M6b consume |
| M4 | **CRITICAL data status** | empty fold-pure M4 files = zero filtering | — | m06b `_load_m04_by_fold` :150-191 | 2026-07-30: `census20k_20260728` m04 files were 30-byte EMPTY parquets → whole cohort search ran with NO authenticity/entity filtering; now fatal. Configs still pointing `m04_dir: census20k_20260728` (golden, cohort_search*, asym_std, smoke) depend on those rebuilt files |

## Entity-resolution sensitivity views (`research/v15/entity_resolution_views.py`)

| stage | lever/knob | what it measures | default | where set | status notes |
|---|---|---|---|---|---|
| M4-view | `view` ∈ `wallet_only` / `high_confidence` / `broader` | entity-linking hypothesis: every wallet its own seat / keep only `entity_confidence=="high"` links / keep every M4 heuristic link | all 3 emitted | views:23, 35-88; `--source --output --folds` (default folds "1-12") | 2026-08-03 audit P1: ALL observed links are medium-confidence temporal → high_confidence view == wallet_only on observed graph; broad view must not be sole production view. Only `full_census_wallet_marked.yml` uses a view (`wallet_only`) |

## Stage M5 — Eligibility floors (`research/v15/v15_m05_eligibility.py`)

All module-level constants (not yml-settable) unless noted. Two lanes: `--mode equity` (needs M1) / `--mode copyability` (equity gates skipped by design).

| stage | lever/knob | what it measures | default | where set | status notes |
|---|---|---|---|---|---|
| M5 | `--mode` equity\|copyability | whether M1-equity gates (roe, DD, leverage, days-green, median equity, ruin, quarantine) apply | equity | m05:488; yml `m05.mode` | ALL experiment ymls set `copyability` (M1 out of scope, Alberto 2026-07-17/30). Finding 2026-07-29: copyability lane disables account-health gates |
| M5 | net PnL floor | pretest closed-journey net realized PnL > 0 | >0 | m05:284 | in copyability lane recorded but NON-blocking (:336) |
| M5 | `MIN_EQUITY_USD` | median pretest equity floor | $2,000 | m05:61, :293 | equity lane only |
| M5 | `MAXDD_CAP` | pretest flow-adj max drawdown | ≤0.80 | m05:42, :295 | equity lane only |
| M5 | roe floor + `structural_ruin` | flow-adj TWR ROE > 0; ruin guard (`RUIN_EQUITY_FLOOR` $1) | >0 / $1 | m05:59-60, :287-290 | equity lane; roe non-blocking under `M5_COPYABILITY_ONLY` |
| M5 | `MIN_JOURNEYS_PRETEST` | closed pretest journeys | ≥3 | m05:43, :297 | blocking in ALL lanes |
| M5 | `HOLD_FLOOR_S` | median hold must exceed copy-path floor | >60s | m05:44, :302 | blocking all lanes |
| M5 | `SWING_MAX_HOLD_S` | reject multi-day holders (median hold) + censored open-at-boundary hold | ≤48h (172,800s) | m05:45, :305-312 | blocking all lanes |
| M5 | `P95_COPY_LATENCY_S` / `SHARE_BELOW_LATENCY_CAP` | share of journeys closing faster than our measured 4.0s copy latency | ≤0.25 below 4.0s | m05:47-57, :313 | 4.0s MEASURED live 2026-07-27; must stay in step with M7 `copy_latency_ms=4000` |
| M5 | `ACCESSIBLE_FRAC_MIN` | notional-weighted fraction of coins accessible as-of test_start | ≥0.80 | m05:58, :329 | unknown → doesn't fire (loose) / fails closed (`--strict`) |
| M5 | `LEVERAGE_CAP` | median daily gross notional/equity | ≤10x | m05:63, :317 | equity lane only |
| M5 | `DAYS_GREEN_MIN` / `MIN_ACTIVE_DAYS_GREEN` | ≥80% of active days flow-adj green, only when ≥20 active days | 0.80 / 20 | m05:64-66, :323 | equity lane; non-blocking under COPYABILITY_ONLY |
| M5 | env `M5_COPYABILITY_ONLY` | keeps copyability/risk gates, makes performance gates (net_pnl/roe/days_green) non-blocking | "0" (off) | m05:237 | never set in any yml (the ymls use `mode: copyability` instead — a DIFFERENT lever: mode drops equity gates entirely) |
| M5 | `--strict` | fail-closed on non-finite inputs, unknown accessibility, non-fold-pure M4 | off | m05:501 | experiment.sh does not pass it |
| M5 | reconstruction quarantine | M1 audit `quarantined=true` wallets fail closed | on (equity lane) | m05:425-428, 528-531 | copyability lane: empty set |
| M5 | G5 pool candidate (reported) | active_test_folds≥3, total journeys≥5, full-window ROE≥`ROE_FULL_FLOOR_G5` 0.50 | 3/5/0.50 | m05:59, :470 | REPORTED here, enforced at M6b/M9 |

## Stage M6a — Cheap shortlist (`research/v15/v15_m06a_shortlist.py`)

| stage | lever/knob | what it measures | default | where set | status notes |
|---|---|---|---|---|---|
| M6a | `shortlist_n_per_fold` (N) | pre-registered per-fold top-N seat budget | 1000 | m06a:83 `DEFAULT_MANIFEST`; JSON manifests: cohort=1000, full_census=25000 | 25,000 in `m06a_manifest_full_census.json` = no-truncation census (universe is 20,378) |
| M6a | `mode` shortlist\|rank_only | whether the cut is applied | shortlist | m06a:79 | rank_only never used in any manifest |
| M6a | `recency_gate` | drop entities with no action in most recent 14d block before test_start | true | m06a:81; both JSON manifests true | disabling requires `contamination_status != clean_oos` (:194) |
| M6a | `contamination_status` | provenance label (clean_oos / exploratory_calibration / alberto_override) | clean_oos | m06a:82 | only clean_oos ever used |
| M6a | `score_basis` equity_roe\|activity_only | score = roe·persistence·log1p(nj)·dd_clamp vs persistence·log1p(nj) | equity_roe | m06a:84; both JSON manifests set **activity_only** | equity_roe fails closed in the no-M1 lane (asserts roe finite/positive) |
| M6a | `BLOCK_DAYS` / `HORIZON_DAYS` / `PERS_SAT_BLOCKS` | persistence: active 14d blocks over 168d, saturates at 6 blocks | 14 / 168 / 6 | m06a:64-66 | LOCKED by design r6; `persistence_horizon_days` echoed in manifests but constant in code |
| M6a | `DD_LO` | dd term clamp floor `clamp(1-max_dd, 0.20, 1.0)` | 0.20 | m06a:67 | equity_roe basis only |
| M6a | I9 rankable contract | eligible ∧ copyable primary ∧ (recency active if gated) ∧ finite score | n/a | m06a:347 | rails: n_journeys≥3 asserted, max_dd≤0.80 asserted (equity lane) |
| M6a | content-hash cache / `--no-cache` | reuse identical-input runs | cache on | m06a:466-553 | keyed on inputs+manifest+env+code+runtime |

## Stage M7 — Engine knobs that shape selection (`v15_m07_engine.py` EngineParams :480-506; yml `m07:` block)

| stage | lever/knob | what it measures | default | where set | status notes |
|---|---|---|---|---|---|
| M7 | `copy_latency_ms` | signal→fill delay priced into every sim | 4000 | engine:487; all ymls 4000 | measured live 2026-07-27; paired with M5's 4.0s gate |
| M7 | `sizing_mode` | fixed_position \| leader_equity(DEPRECATED, refuses on null column) | fixed_position | engine:494; all ymls fixed_position | leader_equity refuses (m09:238-247 too) |
| M7 | `fixed_target_exposure` | follower exposure per position (fraction of equity) | 0.10 | engine:495; all ymls 0.10 | |
| M7 | `copy_policy` full_mirror\|entry_trail (+`trail_pct` 0.15) | mirror everything vs entry-only + trailing TP | full_mirror / 0.15 | engine:502-503; all ymls full_mirror | A/B 2026-07-30: full_mirror beat entry_trail at every threshold |
| M7 | `follower_trail` | flatten + sit out fold if OUR equity draws down ≥ this from peak | None (disabled) | engine:491 | never set in any yml |
| M7 | `slippage_band` / `adl_stress` / `start_policy` | execution stress knobs | base / False / future_delta_only | engine:488-490 | M8 uses "high"+adl for stress runs |
| M7 | `start_equity` | sim bankroll — decides which leader actions clear the $10 min | REQUIRED (no default in runner) | ymls: 10,000 (golden, cohort_search, slip0) vs **937.47** (recent12, live, recent, asym via golden reuse, full_census, smoke) | 10k was a silent CLI default inherited historically; 937.47 = real equity |
| M7 | `limit_entities` | bounded smoke slice | unset | smoke.yml 400 | smoke only |
| M7 | `windows: [pretest, test]` | pretest ranks, test confirms; not interchangeable | both required | all ymls | m06b provenance gate refuses test-as-ranking |
| M7 | `workers` | parallel shards | 1 | full_census(1), m10 workers 2 | |

## Stage M6b — Final ranking + walk-forward confirm (`research/v15/v15_m06b_ranking.py`, `M6bManifest` :54-131)

V2.2 per-position path is ACTIVE whenever `m07_positions.parquet` exists (all current runs). Legacy path listed for completeness.

| stage | lever/knob | what it measures | default | where set | status notes |
|---|---|---|---|---|---|
| M6b score | `w_pp_mean_r` | +z(mean per-position net return) — edge magnitude | 0.45 | :84; yml `m06b.w_pp_mean_r`; CLI `--w-pp-mean-r` | asym_std.yml sets 0.20 |
| M6b score | `w_pp_t` | +z(t-stat) edge/noise consistency | 0.15 | :85; yml/CLI | |
| M6b score | `w_pp_std` | −z(std of per-position return) | 0.05 | :86; yml/CLI | asym_std.yml sets 0.35 (asymmetry proxy) |
| M6b score | `w_pp_mtm_dd` | −z(MAE p90 position drawdown; falls back to account max_dd) | 0.20 | :87; yml/CLI | |
| M6b score | `w_pp_quick` | +z(fraction closed ≤48h) | 0.15 | :88; yml/CLI | |
| M6b score | `w_survivability_penalty` | −(backstops + 2·ruin)/journeys, clipped [0,1] | 0.15 | :69, :958 | NOT CLI/yml-settable |
| M6b gate | `pp_min_positions` | min closed round-trips to be rankable | 25 | :90; yml/CLI | all ymls 25 |
| M6b gate | `pp_min_lcb_mean_r` | 95% one-sided LCB on per-journey NET return must exceed | 0.0 | :91; CLI `--pp-min-lcb-mean-r` | never set in any yml |
| M6b gate | `pp_max_med_hold_h` | median hold ceiling | 48h | :92; CLI | never set in any yml |
| M6b gate | `pp_max_p90_hold_h` | p90 hold ceiling | 168h (7d) | :93 | **NOT CLI-exposed, NOT in experiment.sh FLAG map** — dataclass-only |
| M6b gate | `pp_max_mtm_dd` | MTM/MAE-p90 drawdown ceiling | 0.15 | :94; CLI | never set in any yml |
| M6b gate | `MIN_ROUND_TRIPS` | rankable needs ≥ N closed round-trips (when column present) | 5 | :47 | module constant, not settable |
| M6b support | `min_fills_pretest` / `min_exposure_days` / `min_active_subsplits_support` | rankable min-support | 30 / 3.0d / 2 | :116-118 | dataclass-only; "active_subsplits<2" excluded an entire pool when m02_journeys was absent (2026-07-30, now a hard refusal :403-409) |
| M6b consistency | `block_days` / `consistency_active_min_journeys` / `consistency_active_min_fills` / `consistency_min_active_subsplits` | 14d-block after-cost ROE positivity share; active iff ≥1 journey AND ≥5 fills | 14 / 1 / 5 / 2 | :111-114 | dataclass-only; FINAL source m07_equity, else PROVISIONAL (investable=False) |
| M6b shaping | `dd_floor` (calmar) / `winsor_lo_pct`/`winsor_hi_pct` / `fidelity_B` | calmar=roe/max(dd,0.05); winsorize [p1,p99]; fidelity=1−clamp(TE/0.25) | 0.05 / 1 / 99 / 0.25 | :105-108 | legacy-path terms; dataclass-only |
| M6b legacy score | `w_realized_roe`/`w_calmar`/`w_win_rate`/`w_consistency`/`w_capacity_health`/`w_fidelity` | v2 aggregate score (used only when no per-position emit) | 0.25/0.20/0.15/0.15/0.10/0.10 | :63-68 | superseded by V2.2 whenever m07_positions exists |
| M6b pool | `n_pool` | per-fold pool size after G5 | 100 | :120 | dataclass-only |
| M6b pool | `g5_min_active_pretest_folds` / `g5_min_journeys_pretest` | G5 enforced at pool cut | 3 / 5 | :121-122, :1007 | dataclass-only |
| M6b alloc | `bucket_weights`/`n_buckets`/`top_bucket_consistency_gate`/`per_entity_quality_ceiling` | quintile quality weights (5,4,3,2,1); bucket-1 demotion if consistency<0.5; 10% ceiling | (5,4,3,2,1)/5/0.5/0.10 | :124-127 | dataclass-only |
| M6b OOS | `oos_min_folds` | min non-overlapping OOS test folds per wallet | 2 (floor; standard 4) | :99; yml/CLI | all ymls set 4 (codex standard) |
| M6b OOS | `oos_min_journeys_pooled` | min pooled OOS journeys | 30 (floor; standard 50) | :100; yml/CLI | all ymls set 50 |
| M6b OOS | `oos_min_frac_folds_pos` | net-positive in majority of OOS folds | 0.5 | :101; yml/CLI | only full_census_wallet_marked sets it (0.60) |
| M6b OOS | `oos_margin` | H0 margin on pooled mean net return (bootstrap p) | 0.0 | :102; CLI | never set in any yml |
| M6b OOS | `fdr_q` | Benjamini-Hochberg FDR across eligible wallets | 0.10 | :103; yml/CLI | all ymls 0.10 |
| M6b prov | `fee_schedule_version` / `slippage_calibration_version` | required non-None for `investable=True`; slippage cross-checked against M7 rows | None | :130-131; yml/CLI | ymls: `hl_fee_schedule_2026-06-01` / `v11-fills-v2` (slip0 uses `ZERO-SLIP-SENSITIVITY-v1`) |
| M6b investable | composite flag | calibrated costs + versions + real fidelity + real consistency + conservative return basis + complete open-position censoring + fold-pure M4 + slippage version match | derived | :1124-1134 | 2026-08-03 audit: `cohort_recent12` M6b manifest is investable=false; old M7 artifacts predate open-position marking → **stale, need rebuild** |
| M6b return basis | `conservative_roe` (+ censoring_coverage<1 → −100% bound) | closed PnL + loss-only debit for open positions | v3 behavior | :719-749 | requires the post-audit M7 (marked open rows); pre-fix artifacts fall back → never investable |
| M6b identity | pooling key = `primary_wallet` | cross-fold pooling must not use positional entity_id | n/a | :1254-1266 | P0 finding 2026-07-29 (entity_id collides across folds) |

## Stage M8 — Survival tiering (`research/v15/v15_m08_survival.py`, `M8Manifest` :37-66)

| stage | lever/knob | what it measures | default | where set | status notes |
|---|---|---|---|---|---|
| M8 | tier multipliers `mult_kill/suspicious/uncertain/full_weight` | survival multiplier in sizing chain | 0.0 / 0.10 / 0.25 / 1.0 | :41-44 | frozen (decisions/2026-06-01); dataclass-only |
| M8 | `staleness_max_days` | last action within N days before test_start else KILL | 14 | :60 | dataclass-only; live arm-gate staleness is a separate 7d knob |
| M8 | `stress_slippage_band` / `stress_adl` | counterfactual survival M7 run at high slippage + ADL | "high" / True | :47-48 | dataclass-only |
| M8 | `nominal_capital` | bankroll scale for absolute stress slices | 10,000 (CLI `--nominal-capital`) | :52, CLI :430 | experiment.sh passes `m09.b0` (or m07 start_equity) → 937.47 in full_census |
| M8 | `smaller_slice_frac` / `min_slice_capital` | multi-slice survival probe (0.25× and $50 min) | 0.25 / $50 | :54-55 | dataclass-only |
| M8 | `indeterminate_heavy_frac` | >25% indeterminate minutes → cap tier at UNCERTAIN | 0.25 | :65 | dataclass-only |
| M8 | `m9_static_per_entity_cap` | pre-M9 static cap in pre_m8_max formula | 0.10 | :50 | dataclass-only |
| M8 | zero-fill / bad-ts fail-closed | no replay evidence → KILL (data gap) | n/a | :112-130, :140 | codex P0 |
| M8 | inferential scorers | hedge_smell / funder_provenance / carry_timing / behavioral_linkage | phase2_stub (no_flag capped at UNCERTAIN) | :161-182 | INACTIVE — stub caps every non-kill at ≤UNCERTAIN (0.25 multiplier) until phase 2 |
| M8 | input contract | requires M6b pool with boolean `investable` all-True | n/a | :277-283 | means M8 can only run on a FINAL calibrated M6b → currently blocks all stale-artifact lineages |
| M8 | `--sizing-mode` / `--fixed-target-exposure` / `--allow-global-m04` / `--limit` | CLI | leader_equity*/0.10/off/None | :432-436 | *experiment.sh passes fixed_position from manifest |

## Stage M9 — Chained portfolio sim (`research/v15/v15_m09_sim.py`, `M9Manifest` :35-54; yml `m09:`)

| stage | lever/knob | what it measures | default | where set | status notes |
|---|---|---|---|---|---|
| M9 | `b0` | fixed live-small bankroll | 500.0 | :38, CLI, yml | full_census: 937.47 |
| M9 | `target_count` | non-binding seat ceiling (anti-corr stop) | 40 | :40, CLI, yml | full_census: 9; CLI refuses if infeasible at fixed exposure (:989-996) |
| M9 | `rho_max` | anti-corr: drop lower-ranked of pair with ρ> this (positive corr only) | 0.70 | :39, CLI, yml | corr dict currently passed empty in runner → no-op in practice |
| M9 | `gross_cap` | aggregate levered notional ≤ b0 × this | 3.0 | :41, CLI, yml | keyed off FIXED b0, not live equity |
| M9 | `global_dd_derisk` | chained-DD circuit breaker → flatten all | 0.35 | :42, CLI, yml | |
| M9 | `g4_intrafold_kill` | intra-fold portfolio kill at 50% of fold-initial, no hindsight recovery | 0.50 | :43, CLI, yml | |
| M9 | `per_entity_cap` | G7 allocation-time per-entity cap | 0.40 | :44, CLI, yml | |
| M9 | `suspicious_cohort_cap` | SUSPICIOUS-tier cohort ≤10% of equity | 0.10 | :45, CLI, yml | |
| M9 | `min_order_notional` / `min_accessible_frac` | HL $10 min-notional feasibility; ≥50% of actions must clear | 10.0 / 0.50 | :47-51, CLI, yml | fail-closed per entity; fixed_position lane treats accessibility as satisfied by construction (:681-687) |
| M9 | `sizing_mode` / `fixed_target_exposure` | sleeve sizing; CLI default fixed_position | fixed_position / 0.10 | :52-53, CLI :980-981 | full_census sets `fixed_target_exposure: 1.0` at M9 (sleeve-level, avoids double 10%) |
| M9 | sizing chain | quality(M6b) × confidence(M4) × survival(M8) → water-fill into caps | formula | :56-58, :556 | one multiplier per module (no double count) |
| M9 | `pool_provider` ranked\|matched_null (+seed) | M10 null randomizes peer ordering only | ranked | :446-450, :564 | matched_null used by M10 |
| M9 | only yml block exists in `full_census_wallet_marked.yml` | — | — | — | golden/cohort_search family have NO m09 block → M9/M10 never ran for them (2026-08-03 audit P0: "No current M09/M10 verdict exists") |

## Stage M10 — Deploy gates + matched null (`research/v15/v15_m10_gates.py`, `M10Manifest` :27-41; yml `m10:`)

| stage | lever/knob | what it measures | default | where set | status notes |
|---|---|---|---|---|---|
| M10 | G1 `g1_chained_roe` | chained ROE ≥ 2×/6mo pace | 0.5302 (8×14d); CLI `--g1-chained-roe` | :30; full_census yml 0.8928 (12 folds/168d) | horizon-scaled |
| M10 | G2 `g2_min_positive_folds` of `expected_n_folds` | positive folds with verified denominator | 6 of 8 | :31-32; CLI `--min-positive-folds` REQUIRED, `--expected-n-folds` REQUIRED | full_census: 8 of 12 |
| M10 | G3 `g3_max_chained_dd` | chained max DD | ≤0.50 | :33 | not CLI/yml-settable |
| M10 | G4 `g4_fold_floor` | fold-end ≥50% fold-initial + no intra-fold kill | 0.50 | :34 (evaluated from M9 booleans) | |
| M10 | G5 | pool-ok flag from M9 (`in_pool` non-empty) | bool | m09:1039 | |
| M10 | G6 `g6_gate_pct` / `g6_holdout_pct` + `n_null_dev` / `n_null_holdout` | bivariate ROE AND calmar > pctile of exact matched-null | 95 / 99, 1000 / 5000 | :35-39; CLI `--gate-percentile`, `--n-null-dev`; yml m10 | full_census: 95 / 1000, workers 2 |
| M10 | G7 `g7_max_top_entity_pnl` | top entity ≤40% of PnL OR conservative ablation still clears G1+G3+G6 | 0.40 | :37, :96-114 | |
| M10 | `ladder` | report p75/p90/p95/p99 | (75,90,95,99) | :40 | reporting |
| M10 | verdict | dev all-pass → `live_small`; holdout all-pass → `lp_confirmatory` | — | :186-193 | dev verdict gates LIVE-SMALL ONLY |

## Diagnostic A–F strategy comparison (`research/v15/selection_strategy_comparison.py`) — status: `diagnostic_only`, `deployable: false` by construction

| stage | lever/knob | what it computes | default | where set | status notes |
|---|---|---|---|---|---|
| A–F | `--k` (evaluated at k/2, k, 2k) | cohort size | 20 | :242, :249 | |
| A–F | `--random` / seed | random-cohort null draws | 1000 / 20260803+k | :243, :251 | |
| A–F | behaviour eligibility gate | `pp_n≥25 ∧ pp_p90_hold_h≤168 ∧ pp_mae_p90≤0.15 ∧ pp_uw_add≤0.60` | fixed | :39-42 | used by E, D-fallback, and inside robust/ensemble |
| A | A_static_robust | fold-1 top-k by `score_robust`, held static; missing seats earn cash | — | :113, :117 | |
| B | B_periodic_robust | re-pick top-k every fold on `score_robust` = 0.35·pct(m6b_score)+0.25·pct(pre_roe)+0.20·pct(pre_calmar)+0.20·score_behaviour | — | :50-53, :118 | |
| C | C_recent_winner | top-k on `score_recent` = pct(pretest realized ROE) | — | :49, :119 | diagnostics show trailing-return chasing (deciles + spearman) :200-232 |
| D | D_regime_conditioned | prior same-regime forward test_roe (0.70·pct(mean)+0.30·behaviour); falls back to E when no same-regime history; regimes = BTC trailing 14d up/down × ann-vol ≥0.80 highvol/lowvol | — | :62-78, :123-131 | |
| E | E_behaviour_first | top-k on `score_behaviour` = 0.25·pct(frac_quick)+0.20·pct(p90_hold↓)+0.20·pct(mae_p90↓)+0.15·pct(uw_add↓)+0.20·pct(mean_r), gated behaviour_ok | — | :44-48, :120 | |
| F | F_shrunk_ensemble | 0.40·pct(score_robust)+0.30·score_recent+0.30·score_behaviour | — | :54-57, :122 | |
| eval | forward metrics | mean/compounded forward follower return (fixed-k denominator, missing seat = cash), turnover, rank spearman, randomized-cohort percentile, hold/coin concentration | — | :138-197 | source M7 excludes cutoff-open positions; panel truncated to activity top-1000/fold → blocks greenlight (:253-263) |

## Arm gate + account-health gate (`scripts/arm_copy.sh`, `scripts/account_gate.py`)

| stage | lever/knob | what it measures | default | where set | status notes |
|---|---|---|---|---|---|
| ARM | research lineage gate | must pass BEFORE account gate; `--force` cannot bypass it | non-bypassable | arm_copy.sh:71-79 | see next table |
| ARM | `--force` | bypasses ONLY the exchange account-health gate, recorded in `.ARM_COPY` | off | arm_copy.sh:26, :114-119 | override recorded |
| ARM | roster resolution | gates the launcher's actual `CONFIG=` roster (last uncommented assignment) unless `--config` | derived | arm_copy.sh:51-58 | content-bound via `config_sha256` in `.ARM_COPY` |
| ARM | halt flags | `/tmp/v12_pause` or `.HALT_COPY` refuse arming | — | arm_copy.sh:62-66 | both present per 2026-08-03 audit (KEEP CASH) |
| GATE | `MIN_PERP_EQUITY_USD` (`--min-equity`) | leader perp equity floor | $10,000 | account_gate.py:56 | roster-overridable via `global.account_gate.min_equity` |
| GATE | `MIN_LIFETIME_PERP_PNL` (`--min-lifetime-pnl`) | lifetime perp PnL floor | 0.0 | account_gate.py:57 | roster-overridable |
| GATE | `MAX_STALE_DAYS` (`--max-stale-days`) | most recent fill recency | 7.0d | account_gate.py:58 | roster-overridable but treated as the only thesis-mandatory criterion |
| GATE | `MAX_ACCOUNT_LEVERAGE` (`--max-leverage`) | account leverage cap | 10.0 | account_gate.py:59 | roster-overridable; same constant as M5 LEVERAGE_CAP |
| GATE | roster-declared thesis thresholds | `global.account_gate.{min_equity,min_lifetime_pnl,max_leverage,max_stale_days}` + `_why` | strict defaults if absent | arm_copy.sh:95-108; `config/copy_trader_v15recent9_20260731.json` declares min_equity=0, min_lifetime_pnl=-1e12, max_leverage=inf, max_stale_days=7 | entries-only thesis: lifetime-negative leaders expected BY DESIGN; only staleness load-bearing |

## Research lineage gate (`tools/research_lineage_gate.py`, `tools/live_authorization.py`)

| stage | lever/knob | what it requires | where set | status notes |
|---|---|---|---|---|
| LINEAGE | `REQUIRED_GATES` | data_quality, point_in_time, m06b_investable, m08_survival, m09_chained, m10_verdict, live_replay_parity — all true | :24-32 | verifier only, cannot generate approval |
| LINEAGE | `REQUIRED_ARTIFACTS` | experiment_manifest, provenance, registry_receipt, roster_source, m06b_manifest, m08_result, m09_result, m10_verdict, live_replay_parity — SHA-256 pinned, in-repo, no symlinks | :33-43, :59-87 | |
| LINEAGE | semantic checks | m06b investable=true + slippage version present; m09 complete + finite ROE/DD/calmar; m10 all_pass + greenlight ∈ {live_small, lp_confirmatory}; parity pass; clean git tree; selected_wallets == live config wallets | :169-207 | current roster's lineage (cohort_recent12) is recorded REJECTED → cannot arm |
| LIVE | `verify_live_authorization` | `.ARM_COPY` present, config path+sha match, lineage still verifies, digest matches armed digest, no halt flags | live_authorization.py:15-44 | re-verified in-process at every launch |

---

## Levers that exist in code but were never used in any experiment yml

- **m06b (CLI-exposed but never set in any yml):** `pp_min_lcb_mean_r` (default 0.0), `pp_max_med_hold_h` (48h), `pp_max_mtm_dd` (0.15), `oos_margin` (0.0).
- **m06b (dataclass-only, no CLI, no yml path — changing them requires a code edit despite the "manifest-diff" doctrine):** `pp_max_p90_hold_h` (168h — in the dataclass and gate but ABSENT from both the CLI and the experiment.sh FLAG map), `w_survivability_penalty`, `min_fills_pretest`, `min_exposure_days`, `min_active_subsplits_support`, `n_pool`, `g5_min_active_pretest_folds`, `g5_min_journeys_pretest`, `bucket_weights`, `n_buckets`, `top_bucket_consistency_gate`, `per_entity_quality_ceiling`, `dd_floor`, `winsor_lo_pct/hi_pct`, `fidelity_B`, `block_days`, all `consistency_*`, the entire legacy weight vector, `MIN_ROUND_TRIPS` (module constant).
- **m05:** every floor is a module constant (only `mode` is yml-settable). `--strict`, `--accessible-coins`, `M5_COPYABILITY_ONLY` env, and the `equity` mode itself are unused by all current ymls.
- **m06a:** `mode: rank_only`, `recency_gate: false`, `contamination_status` other than clean_oos — never used.
- **m07:** `follower_trail` (follower trailing stop, disabled everywhere), `entry_trail`/`trail_pct` (tested in the 2026-07-30 A/B, not in any current manifest), `slippage_band` other than base, `adl_stress` (only via M8 stress).
- **m08:** NO yml block exists in any manifest; every `M8Manifest` field (`staleness_max_days`, multipliers, `indeterminate_heavy_frac`, `smaller_slice_frac`, `min_slice_capital`, `stress_*`) is defaults-only; experiment.sh forwards only nominal-capital/sizing-mode/fixed-target-exposure derived from m09/m07 keys.
- **m09/m10:** present only in `full_census_wallet_marked.yml`; golden and the whole cohort_search family never ran M9/M10 (matches the audit P0 "no current M09/M10 verdict").
- **entity_resolution_views:** `high_confidence` and `broader` views built but no experiment yml consumes them (only `wallet_only` in full_census_wallet_marked).
- **m10:** `g3_max_chained_dd`, `g7_max_top_entity_pnl`, holdout mode (`g6_holdout_pct`=99, `n_null_holdout`=5000) — not settable/never invoked.
- **selection_strategy_comparison:** the entire A–F apparatus is standalone-diagnostic; none of its scores/gates feed the pipeline.

## Levers referenced in configs that do not exist in code

None found that could silently no-op — experiment.sh hard-fails on unknown `m06b`/`m09` keys and validates `m10.workers`/`m07.workers`. Notes:
- `oos_min_frac_folds_pos: 0.60` (full_census_wallet_marked) — exists in the FLAG map and CLI; fine.
- `persistence_horizon_days` in the m06a JSON manifests is carried into the manifest record but the code reads the module constant `HORIZON_DAYS=168` (m06a:65, 83); it is documentation, not a functional lever — changing it in JSON alone would NOT change behavior.
- `m06a.manifest.manifest_version` / `contamination_status` are provenance labels, not behavior (except the recency-gate/clean_oos consistency check).
- `oos:` block keys (`tool`, `mark_source`, `allow_unverified_marks`, `min_trades`, `windows`) map to `forward_oos_hot.py` (not in this audit's read set); the family split is asset_ctxs+allow_unverified (historical reproductions, flagged suspect) vs candles+verified (full_census).

## Cross-cutting status (2026-08-03 audit)

- All M6b/M8-dependent results on pre-fix M7 artifacts are STALE: old M7 force-closed/omitted the 71,596 cutoff-open journeys; current code requires marked open rows + complete `censoring_coverage`, so every legacy artifact set fails `investable` and must be rebuilt (M02→M07 rebuild also pending the 15-day funding backfill).
- Configs whose `m04_dir` is `census20k_20260728` depend on the previously-empty M4 files being rebuilt; the canonical replacement is the fold-pure `entity_resolution_views_20260803/wallet_only` view used by `full_census_wallet_marked.yml`.
- The only funnel path that can currently produce an armable roster is: full_census-style manifest (marked M7, wallet_only M4, m09+m10 blocks) → immutable deployment manifest → lineage gate → account gate with roster-declared thesis thresholds.