# Hyperliquid copy-selection audit — inspection gate

Date: 2026-08-03  
Status: **outputs are not yet trustworthy for production cohort selection**  
Scope: repository, local Parquet stores, current experiment artifacts, local MongoDB, launchd wiring,
research tests, and the V17 live engine. No strategy or production code was changed in this pass.

## Executive finding

The repository contains substantial correctness hardening, but the current end-to-end production claim is
not supported by the current state.

Most decisively, the experiment that produced the currently configured nine-wallet roster
(`experiments/cohort_recent12`) emitted `investable=false` because its slippage calibration was not bound to
the M7 rows. M8 then failed closed and the experiment stopped. The launcher nevertheless points at a config
whose authorization text cites that experiment. The engine is currently paused by its halt flags, so this is
a deployment-readiness failure rather than a claim that it is trading now.

The repository also has multiple competing definitions of the canonical research flow, a stale/non-running
daily selection path, incomplete funding partitions, an ingestion manifest that can call a 23/24-hour day
`ok`, a broken current test suite, no current M9/M10 portfolio verdict, incomplete treatment of open losing
positions in leader-level selection, and no end-to-end replay/live parity fixture.

## 1. System map

### Repository and services

There is one repository, `/Users/hermes/quants-lab`.

| Surface | Canonical/current implementation | Current role |
|---|---|---|
| Historical fills ingestion | `scripts/hl_s3_fills_daily_refresh.sh` -> `data_pipeline/hl_s3_fills_daily_refresh.py` | Downloads requester-pays S3 fill blocks for the 20,378-wallet universe; writes filtered daily fills and all-market 1m candles |
| Funding/ledger ingestion | same shell job -> `data_pipeline/hl_s3_misc_daily_refresh.py` | Writes wallet-filtered daily funding and ledger events |
| Full journey reconstruction | `scripts/recal_pipeline.sh` -> `scripts/m2_batched_run.py` / `research/v15/v15_m02_journey_trace.py` | Builds atomic actions and journeys from wallet-partitioned fills |
| Incremental journey reconstruction | `data_pipeline/m02_journeys_daily.py` | Intended stateful daily M2 path; its active checkpoint is under `m02_daily_state` |
| Fold geometry | `research/v15/v15_m03_fold_geometry.py`; incremental variant `data_pipeline/m03_folds_daily.py` | Chronological train/validation/test boundaries and activity summaries |
| Entity resolution/authenticity | `research/v15/v15_m04_authenticity.py`, importing `v15_m025_authenticity_gate.py`; per-fold driver `scripts/build_m4_perfold.sh` | As-of wallet danger tiers and union-find wallet-to-entity map |
| Eligibility | `research/v15/v15_m05_eligibility.py` | Fold-pure activity/copyability gates; current experiments use `mode=copyability` without M1 equity |
| Candidate shortlist | `research/v15/v15_m06a_shortlist.py` | Activity-based top-N entity/fold seats before the expensive engine |
| Per-entity replay | `research/v15/v15_m07_engine.py` | Fees, latency, slippage, funding, capacity, risk, positions, fills, and equity |
| Ranking/forward confirmation | `research/v15/v15_m06b_ranking.py` | Pretest ranking plus held-out fold confirmation and BH-FDR |
| Survival tier | `research/v15/v15_m08_survival.py` | Refuses non-investable M6b pools; counterfactual danger sizing |
| Portfolio simulation/gates | `research/v15/v15_m09_sim.py`, `v15_m10_gates.py`, `v15_forward_select.py` | Libraries and stale artifacts exist, but they are not wired into the current experiment runner and no current cohort has a complete M9/M10 verdict |
| Experiment orchestration | `scripts/experiment.sh` + `config/experiments/*.yml` | M05 -> M06a -> M07 pretest/test -> M06b -> M08 -> forward OOS; does not run M9/M10 |
| Cohort config emission | `research/v16/select_cohort.py` plus several one-off/manual config paths | No single proven canonical path from an investable experiment artifact to a live config |
| Live activation | `scripts/arm_copy.sh` -> `.ARM_COPY`; `scripts/v12_launcher.sh` verifies path, config hash, and gate result | Positive authorization boundary before starting the engine |
| Live engine | `strategies/live/hl_copy_trader_v17.py` | WebSocket leader-fill handling, target tracking, IOC execution, exits/reversals, risk, persistence, and reconciliation |
| Live supervision | launchd `com.quantslab.v12-copy-trader`; marks via `com.quantslab.hl-mark-collector`; S3 daily job via `com.quantslab.hl-s3-fills-daily` | KeepAlive for engine/marks; daily 06:20 local ingestion |
| Reporting | engine logs + Mongo collections, `tools/pnl_tracker.py`, `scripts/daily_report*.py` | Several reporting paths; no single canonical research/live audit report |

### Canonical data flow actually used by the latest cohort experiment

```text
Hyperliquid S3 node archives
  -> daily filtered fill/funding/ledger Parquet
  -> census20k M02 actions + journeys (through 2026-07-13)
  -> 12 chronological folds
  -> fold-pure M04 entity/authenticity tables
  -> M05 copyability eligibility
  -> M06a activity shortlist (1,000 seats/fold)
  -> M07 pretest + test replay at $937.47 starting equity
  -> M06b ranking + fold confirmation
  -> investable=false (missing/mismatched slippage version)
  -> M08 refuses and experiment stops
  -X-> no M9/M10 portfolio verdict
  -X-> nevertheless used as provenance text for the nine-wallet live config
```

### Data stores and schemas

Research data is predominantly Parquet, not a relational database.

| Store | Current observed coverage | Current observed size/rows | Notes |
|---|---:|---:|---|
| `hl_s3_fills_v2_hot` | 2025-07-27–2026-08-02, with a 102-day 2025 gap | 270 files, 154,776,414 rows, 12.6 GB | The gap is 2025-08-21–2025-11-30; the intended recent research window begins 2025-12-01 |
| `hl_s3_candles_1m_hot` | 2026-06-24–2026-08-02 | 40 files, 11,965,902 rows | No filename gap in that interval |
| `hl_s3_funding_hot` | 2025-12-01–2026-08-02 | 230 files, 111,357,763 rows | Missing 15 consecutive days: 2026-05-25–2026-06-08 |
| `hl_s3_ledger_hot` | 2025-12-01–2026-08-02 | 245 files, 3,198,932 rows | No filename gap in that interval |
| Census M02 actions | 2025-12-01–2026-07-13 | 125,060,683 rows | Atomic action schema includes lifecycle and stream validity |
| Census M02 journeys | 2025-12-01–2026-07-13 | 11,147,770 rows | 71,596 open at cutoff; 56,019 lifecycle-invalid; 57,625 stream-invalid |
| V15 tree | mixed/stale parallel runs | 67 GB | Multiple top-level, census, recency, funnel, backup, and experiment lineages coexist |

The live engine uses MongoDB `quants_lab`. Current V17 collections include `v17_open_positions`,
`v17_pending_reverse`, `v17_order_ids`, `v17_exchange_fills`, `v17_target_fills`, `v17_gate_log`,
`v17_sweep_log`, and `v17_meta`. Observed collections have only Mongo's default `_id` index. Current data
had no duplicate `(wallet, coin)` open-position keys, no duplicate order IDs, and no duplicate exchange
trade IDs. `fill_key=(oid,time)` is not unique because distinct partial fills can share an order and
timestamp; `tid` is the actual deduplication key.

## 2. Critical correctness findings

| Severity | Component | Bug or risk | Current evidence | Affected outputs | Proposed correction | Confidence |
|---|---|---|---|---|---|---|
| P0 | Cohort deployment lineage | The currently configured roster was derived from a run explicitly marked non-investable | `experiments/cohort_recent12/m06b_manifest.json`: `investable=false`, no slippage calibration version and M7/version mismatch; its log shows M8 refusing and exiting 1; `v12_launcher.sh` points at `copy_trader_v15recent9_20260731.json`, whose authorization cites this run | Live cohort authorization, any production-readiness claim | Block config emission/arming unless the originating immutable run completed every required gate with `investable=true`; bind run ID and output hash in the config and arm record | High |
| P0 | Portfolio validation | The current experiment path stops at per-entity confirmation and omits M9/M10 | `scripts/experiment.sh` stages are M05/M06a/M07/M06b/M08/OOS only; latest M9 artifacts are stale June files; no current M10 verdict exists | Cohort return, drawdown, concentration, turnover, matched-null percentile, deploy/no-deploy verdict | Add M9/M10 as mandatory manifest-driven stages and refuse cohort emission without their hashed outputs | High |
| P0 | Daily selection | The advertised daily selection flow is not operational or canonical | No launchd plist invokes `daily_selection_pipeline.sh`; it checks `m02_stateful_state/checkpoint.json`, which is absent, while the actual checkpoint is `m02_daily_state/checkpoint.json`; required `m04_authenticity_daily/m4_run_state.json` is absent; M3 state last updated 2026-07-16 while S3 reaches 2026-08-02 | Fresh eligibility/rankings, dynamic-cohort research, monitoring | Decide whether the daily path is production or experimental; if production, correct state paths, install explicit scheduling, add end-to-end freshness tests and a single run manifest | High |
| P0 | Funding completeness | Fifteen funding partitions are absent and one partial day is labeled successful | Files are missing for 2026-05-25–2026-06-08; 2026-08-01 has `hours_ok=23`, `hours_error=1`, but day `status=ok` and run `last_run_failed_days=0` | Funding-dependent authenticity/P&L and completeness claims | Backfill gaps; make any non-404 failed hour yield `partial/error`, never `ok`; prevent downstream watermark advance until all 24 expected hours are proven or explicitly waived | High |
| P0 | Test trust | The current claimed-green research suite is broken | Current command: 424 passed, 3 failed, 29 errors; M02 tests still patch `m02.m01.get_mark`, but M02 no longer exposes `m01` | Position/journey invariants and regression confidence | Reconcile tests with the supported M02 architecture; retain equivalent behavioral coverage rather than deleting obsolete assertions; make this suite a required gate | High |
| P0 | Open-loss economics | Leader-level selection does not yet prove that open losses are fully included at every ranking timestamp | Journey rows carry realized P&L/funding/fees and an `open_at_window_end` flag, but no contemporaneous mark/unrealized P&L; 71,596 journeys are open at the census cutoff. M05 uses censored duration, while M06b's per-position metrics come from a follower sim that force-closes at fold end | Win rate, profit factor, leader economics, bag-holder filtering, eligibility | Build an as-of leader exposure/MTM table; include marked open positions in economic return, profit factor, loss realization, and bag metrics; report closed-only and all-economic-exposure views side by side | High |
| P0 | Canonical architecture | Documents and launch scripts contradict one another about whether M1 is canonical or prohibited | `SYSTEM.md` and `run_clean_rerun.sh` describe a causal M1 start; `recal_pipeline.sh` says M1 is out of scope and must never be referenced; latest experiments use copyability/no-M1 | Reproducibility, operator decisions, which outputs are valid | Establish two explicitly named lanes (equity-dependent and equity-independent) or retire one; make one machine-readable pipeline manifest authoritative and test docs/entrypoints against it | High |
| P1 | Entity resolution | Medium-confidence links are used as deterministic deduplication without required sensitivity views | Temporal matching unions any withdrawal followed by a similar deposit anywhere in the universe within 10 minutes; fold 12 has 583 multi-wallet medium-confidence entities, 1,972 linked wallets, a maximum component of 476, and 554 medium-confidence entities marked copyable | Entity P&L, primary selection, candidate population, double-count prevention | Preserve edge-level provenance and confidence; distinguish direct transfer from temporal match; run wallet-only, direct/high-confidence, and broad views; do not suppress wallets on medium links in the high-confidence production view | High |
| P1 | Experiment provenance | Per-run provenance is incomplete and does not constitute the required research registry | All six observed experiment provenance files recorded `git_dirty=true`; directory inputs such as M04 are recorded as "dir or absent" without hashes; no registry records pre-specification, nearby variants, disposition, or holdout touches | Reproducibility, multiple-testing audit, holdout integrity | Hash directory manifests/files, record code diff hash, schema versions and every output; add an append-only experiment registry with hypothesis, pre-spec flag, family size, result, disposition, and holdout access count | High |
| P1 | Live/replay parity | No shared fixture proves identical target exposure and intended actions across research and live | The live file claims a shared replay module that does not exist at the cited path; only `strategies/live/copy_convergence.py` imports are found. Existing parity tests cover isolated exit decisions/classification, not a complete leader-event sequence through both engines | Entries, adds, trims, exits, reversals, sizing, portfolio caps | Extract a side-effect-free decision core or adapter; feed identical ordered fixtures through M7/replay and live decision logic; compare target exposure and intended order stream before exchange I/O | High |
| P1 | Ingestion observability | The cron can hide failed fills and reports the wrong exit code | On 2026-08-01 a fills day timed out, but the wrapper logged `fills exited non-zero (0)` because `$(date ...)` overwrote `$?`; the shell intentionally continues into misc | Alerting and data-quality response | Capture `rc=$?` before command substitution/logging; surface pipeline status in a machine-readable supervisor report and alert on partial/error days | High |
| P1 | Live persistence/idempotency | Natural keys are not enforced by database indexes | All inspected V17 collections have only `_id`; code relies on application-level upserts for `(wallet,coin)`, `oid`, and `tid` | Restart/concurrency safety and audit consistency | After a duplicate audit/migration, create unique indexes for true natural keys; keep `fill_key` non-unique because same-order/same-time partial fills are valid | Medium-high |
| P1 | Documentation truth | `REPO_MAP.md`/`SYSTEM.md` contain stale assertions and cannot serve as audit evidence | They claim all modules trusted/green and describe a pipeline inconsistent with current scripts and artifacts | Operator trust and audit scope | Generate the architecture/status page from tested manifests and current entrypoints; label historical claims with run IDs and dates | High |

## 3. What is already supported by current evidence

- The latest S3 fill and candle partitions reach 2026-08-02 and report 24/24 hours for that date.
- Census journey keys `(wallet, coin, journey_id)` are unique in the current 11.1-million-row artifact.
- Census journey durations are nonnegative in the previously generated gate audit; the current duration
  distribution has median 0.81h, p75 7.46h, p90 48.0h, p95 143.96h, p99 792.08h, and max 5,399.99h.
- Current V17 Mongo state has no duplicate natural keys for open positions, order IDs, or exchange `tid`.
- M8 is correctly fail-closed on a non-investable M6b pool. The defect is that later workflow/configuration
  treated the failed upstream run as sufficient anyway.
- The launcher has strong positive-authorization checks: arm record, roster path, roster content hash, and gate
  result must agree. This protects runtime configuration integrity but does not validate the research lineage.
- The live engine is presently paused according to the launchd log; this audit did not arm, resume, or trade.

## 4. Exact analyses to run before selecting or changing a production cohort

The analyses below are deliberately limited to pre-declared families. Results will include all attempted
variants, not only winners.

### A. Correctness and lineage gates

1. **Immutable data manifest**
   - Enumerate expected UTC days/hours for fills, funding, ledger, candles, and marks.
   - Record schema fingerprints, row counts, file hashes, event-time bounds, late rewrite history, and source
     wallet-universe hash.
   - Fail on missing/partial periods, unexpected schema drift, or a downstream watermark beyond the least-fresh
     required input.

2. **Raw and normalized event integrity**
   - Full-history duplicate checks using exchange identity (`tid` where valid) plus fallback composite keys.
   - Validate wallet/coin normalization, numeric finiteness, UTC partition alignment, same-ms causal chains,
     retry idempotency, and daily-vs-wallet-shard parity.
   - Quantify invalid rows and exclusions by reason and date.

3. **Position and journey invariants**
   - For every wallet/coin, replay signed size from authoritative `startPosition` chains.
   - Reconcile each action's `position_after`; explicitly test entry/add/trim/partial exit/full exit/reversal,
     liquidations, day/fold boundaries, carry-in, gaps, and same-timestamp events.
   - Report resync and invalid-journey rates by wallet, coin, date, and entity; no invalid row may reach selection.

4. **Economic P&L identity**
   - Build as-of cash, signed position, mark value, realized P&L, unrealized P&L, fees, funding, and external-flow
     neutralization at daily and ranking timestamps.
   - Reconcile to account equity where trustworthy; keep the equity-independent lane separate when it is not.
   - Force-mark all open positions at every ranking boundary and show the delta from closed-only metrics.

5. **Point-in-time lineage**
   - Bind every fold output to input hashes, code SHA+diff hash, schema version, config, as-of timestamp, and output
     hashes.
   - Verify entity edges, universe membership, thresholds, normalization, regime labels, and features use only
     information observable before each decision.
   - Reserve a new final holdout once; log every access.

6. **Entity-resolution sensitivity**
   - Produce three parallel populations: wallet-only, direct-transfer/high-confidence entity, and broad
     direct+temporal entity.
   - Measure component sizes, simultaneous trading, exposure duplication, primary changes, selected-cohort overlap,
     and performance sensitivity. Manually inspect the largest/most consequential components.

### B. Behaviour, copyability, and selection research

7. **Bag-holding and realization panel**
   - At every rebalance timestamp compute median/mean/p75/p90/p95/max duration; capital-, profit-, and loss-weighted
     duration; flat-state frequency; closure fraction; open age buckets; time underwater; MAE/MFE; underwater adds;
     realized-loss share; marked-open profit factor; open-loss/realized-profit ratio; and winner-vs-loser exit speed.
   - Publish interpretable component metrics and example entities, including false-positive long-horizon/hedged cases.

8. **Leader-to-follower attribution**
   - For each entity and action, decompose raw leader economics into follower P&L, fee, latency, slippage,
     missed-action, min-order/precision, capital-cap, coin-cap, leverage, and aggregation drag.
   - Repeat at realistic capital levels and with measured dimension-specific execution assumptions.

9. **Regime discovery (simple first)**
   - Pre-declare interpretable real-time features: BTC/broad direction, realized volatility, cross-sectional
     dispersion, funding, volume/liquidity, and trend strength.
   - Cluster or threshold only after checking stability and minimum sample sizes; compare against a no-regime model.
   - Every label must be computable at the rebalance timestamp with no centered/future window.

10. **Walk-forward strategy family**
    - A: static robust cohort.
    - B: periodic refresh at 1d/3d/7d/14d/28d.
    - C: recent-winner rotation with a small pre-declared lookback/hold grid.
    - D: regime-conditioned selection.
    - E: behaviour-first selection.
    - F: ensemble/shrinkage toward equal weight.
    - Use identical eligibility, execution, portfolio, and as-of contracts so only selection differs.

11. **Baselines and falsification**
    - Cash/no trade, market beta where applicable, equal-weight eligible, simple trailing follower P&L, simple
      trailing Sharpe, static cohort, behaviour-only cohort, and many matched random eligible cohorts.
    - Report return, drawdown, risk-adjusted return, worst period, turnover, overlap/rank stability, entity/coin
      concentration, open-loss exposure, holding tails, random percentile, and parameter sensitivity.
    - Re-run after removing the best entity/coin/trades and after forced boundary marking/liquidation.

12. **Dynamic-cohort decision**
    - Estimate rank/persistence decay by forward horizon, conditional and unconditional on regime and behaviour.
    - Separate return continuation from beta/coin concentration and quantify refresh turnover/cost.
    - Recommend static, slow, frequent, regime, ensemble, or no reliable edge only after the above gates.

### C. Live-engine equivalence and resilience

13. **Shared replay/live fixtures**
    - Feed the same leader sequences through both systems: multi-fill entry, add, trim, partial close, full exit,
      reversal, multiple linked wallets, opposing leaders, dust/min-order, partial follower fill, rejection, duplicate,
      out-of-order event, disconnect gap, and restart.
    - Assert identical target exposure and intended follower adjustment at every step.

14. **Target-vs-actual reconciliation tests**
    - Make `target follower exposure - authoritative actual exposure = required adjustment` an explicit logged
      invariant per entity/coin and at net account level.
    - Fault-inject stale/missing state, open orders, concurrent actions, failed persistence, and restarts; verify
      idempotency, quarantine, and bounded recovery.

15. **Execution-contract parity**
    - Compare research and live fees, per-dex rules, slippage by coin/size/liquidity, latency definition, order type,
      partial/rejected behavior, precision/minimums, leverage setup, and portfolio/coin/gross caps.
    - Re-run historical results with the exact production config and realistic capital before any arm decision.

## Inspection-gate decision

Do not rank a new production cohort or arm the current one from the existing artifacts. The next work should be
P0 correctness: repair/cover the tests, make ingestion completeness fail closed, establish a single canonical
pipeline manifest, bind slippage provenance, require current M9/M10 portfolio gates, and enforce research-run
lineage in config emission/arming. Only then should the pre-declared empirical comparison begin.
