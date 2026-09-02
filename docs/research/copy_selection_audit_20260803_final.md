# Hyperliquid copy-selection and live-engine audit

Date: 2026-08-03  
Decision: **NO DEPLOY / KEEP CASH**  
Runtime state observed: `.HALT_COPY` and `/tmp/v12_pause` are present; nothing was armed, resumed, or traded.

This is the consolidated report. The companion inspection record contains the longer artifact inventory and
the pre-declared analysis checklist.

## 1. System map

```text
Hyperliquid S3 archives
  -> daily fills / candles / funding / ledger Parquet
  -> M02 action and journey reconstruction
  -> M03 chronological folds
  -> fold-pure M04 entity and authenticity views
  -> M05 eligibility
  -> M06a activity shortlist (1,000 seats/fold)
  -> M07 per-entity follower replay
  -> M06b pretest ranking and forward confirmation
  -> M08 survival
  -> M09 portfolio simulation
  -> M10 deploy/no-deploy gates
  -> immutable deployment manifest
  -> config hash + lineage digest in `.ARM_COPY`
  -> V17 live engine
```

The first eight research stages exist, but the current experiment runner stops after M08/OOS and does not
run M09/M10. The currently configured nine-wallet roster cites `cohort_recent12`; that run's M06b manifest is
`investable=false`, and M08 correctly refused it. A new deployment manifest now records this lineage as
rejected, and both arming and direct live startup fail closed on it.

| Surface | Current implementation | Audit conclusion |
|---|---|---|
| Daily ingestion | `hl_s3_fills_daily_refresh.py`, `hl_s3_misc_daily_refresh.py` | Partial/error days now fail closed; historical funding gap remains |
| Journey reconstruction | `v15_m02_journey_trace.py` | Large canonical census exists; invalid/open journeys require explicit exclusion/marking |
| Entity resolution | `v15_m04_authenticity.py` | All observed multi-wallet links are medium-confidence temporal matches, not high-confidence links |
| Eligibility/ranking | M05/M06a/M06b | Candidate truncation and previous closed-only economics make old selections non-deployable |
| Replay | `v15_m07_engine.py` | Now marks open positions conservatively at fold cutoff; old M7 artifacts predate the fix |
| Portfolio gates | M08/M09/M10 libraries | No current complete M09/M10 portfolio verdict |
| Experiment control | `scripts/experiment.sh` | Now hash-recorded and append-only registered; old runs are not retroactively valid |
| Activation | `arm_copy.sh`, `v12_launcher.sh`, in-process authorization | Now requires immutable, clean, complete lineage and matching config/roster hashes |
| Live execution | `hl_copy_trader_v17.py` | Event-driven; important reconciliation and reversal parity gaps remain |
| Persistence | MongoDB `quants_lab`, V17 collections | Natural-key unique indexes are now created at startup; existing rows had no observed duplicates |

## 2. Correctness findings

| Priority | Finding | Evidence / effect | Status |
|---|---|---|---|
| P0 | The live roster's source experiment is non-investable | Slippage provenance was absent/mismatched; M8 exited non-zero | **Contained:** rejected manifest plus non-bypassable arm/launch gate |
| P0 | No current M09/M10 verdict exists | Experiment runner omits portfolio simulation and matched-null deployment gates | **Open:** required before any candidate is deployable |
| P0 | Old M07/M06b selection ignored marked open losses | 71,596 census journeys were open at cutoff; old results force-closed/omitted rather than reporting as-of exposure | **Code fixed; artifacts stale:** M07 now emits marked open rows and M06b requires complete censoring coverage |
| P0 | Direct engine invocation bypassed launcher authorization and defaulted to real mode | Running the Python file did not require `.ARM_COPY`/lineage | **Fixed:** explicit mutually exclusive `--live`/`--shadow`; live verifies authorization in process |
| P0 | Research reversals and live defaults disagree | Fixed-position M07 flips target on reversal; live defaults to flatten-only when `copy_reverse_enabled` is false/absent | **Open:** do not enable by config alone; prove shared-fixture parity first |
| P0 | Missed entries are not reconciled after disconnect | Reconnect clears entry buffers; sweep primarily evaluates already-held legs; backfill is disabled/one-shot | **Open:** target-vs-actual reconciliation is required |
| P0 | Funding history is incomplete | Missing 2026-05-25 through 2026-06-08; a 23/24-hour day had been labelled `ok` | **Partly fixed:** future partial days fail; historical partitions still need backfill |
| P0 | Stateful daily M02 silently skipped when unseeded | Pipeline looked for an absent checkpoint and continued downstream | **Fixed:** canonical non-stateful incremental M02 runs under the memory wrapper |
| P1 | Medium-confidence temporal links collapse wallets | Fold 12: 583 multi-wallet components, 1,972 linked wallets, largest component 476 | **Diagnosed:** wallet/high/broad fold-pure views added; broad view must not be the sole production view |
| P1 | Experiment provenance was mutable/incomplete | Dirty runs and unhashed directory inputs; no experiment family registry | **Fixed prospectively:** directory inventories, run status, failure records, chained JSONL registry and receipts |
| P1 | Cron wrapper masked fills return code | Logging command overwrote `$?`, and wrapper returned the second job's code | **Fixed:** both return codes preserved and aggregated |
| P1 | Mongo natural keys were application-only | Only `_id` indexes existed | **Fixed prospectively:** unique `(wallet,coin)`, `oid`, and sparse `tid`; `(oid,time)` correctly remains non-unique |
| P1 | No full replay/live decision fixture | Existing tests cover exit and event classification, not one identical end-to-end event stream | **Open:** mandatory release gate |
| P2 | Documentation described competing canonical pipelines | M1-required and no-M1 paths were both presented as canonical | **Open:** publish separate named equity-dependent and equity-independent lanes or retire one |

The most consequential correction is fail-closed lineage: `--force` can only override the account-health
gate, not research provenance. A dirty/incomplete experiment, changed roster, missing artifact, or mismatched
hash prevents arming and direct live startup.

## 3. Data-quality findings

| Dataset | Observed coverage / rows | Finding |
|---|---:|---|
| Fills hot | 270 files; 154,776,414 rows; 2025-07-27–2026-08-02 | 102-day historical gap, 2025-08-21–2025-11-30; outside intended 2025-12-01 research start |
| Candles 1m hot | 40 files; 11,965,902 rows; 2026-06-24–2026-08-02 | No filename gap in observed range |
| Funding hot | 230 files; 111,357,763 rows; 2025-12-01–2026-08-02 | Missing 15 consecutive days, 2026-05-25–2026-06-08 |
| Ledger hot | 245 files; 3,198,932 rows; 2025-12-01–2026-08-02 | No filename gap in observed range |
| Census actions | 125,060,683; 2025-12-01–2026-07-13 | Canonical action corpus is stale relative to hot S3 |
| Census journeys | 11,147,770 | 71,596 open; 56,019 lifecycle-invalid; 57,625 stream-invalid |

Journey keys `(wallet, coin, journey_id)` were unique and durations nonnegative in the generated audit.
Observed Mongo state had no duplicate `(wallet,coin)` open keys, order IDs, or exchange `tid`s. Multiple
partial fills can legitimately share `(oid,time)`, so that pair must not be made unique.

The V15 artifact tree contains about 67 GB of mixed current, stale, backup, recency, and experiment lineages.
Path existence is therefore not evidence of freshness. Only a completed hash-pinned deployment manifest is
acceptable input to production.

## 4. Methodology diagnosis

The old methodology cannot answer whether a profitable copy-selection edge exists:

- It selected from the top-1,000 activity shortlist rather than the full eligible population.
- Its leader economics did not include as-of marked open losses; this can reward delayed loss realization.
- It used the broad medium-confidence entity graph without wallet-only/high-confidence sensitivity.
- The source run failed slippage provenance and never reached current portfolio/null gates.
- Multiple historical runs were dirty and nearby variants were not recorded as one multiple-testing family.
- Twelve non-overlapping 14-day test folds identify only a 14-day refresh decision. They do not identify daily,
  3-day, 7-day, 28/30-day, or genuinely adaptive refresh rules.
- Regime support is inadequate: seven down/low-vol folds, four up/low-vol folds, one down/high-vol fold, and no
  up/high-vol fold. A regime model would mostly memorize sparse labels.

Recent performance does have rank information, but it is mostly useful for avoiding the worst candidates.
Across 11 informative folds, pretest-to-forward Spearman was positive in all 11, mean 0.304 and median 0.295.
Yet the mean forward return was -0.211% for the top decile, -0.764% for the middle, and -5.955% for the bottom.
Behavior qualification left mean Spearman essentially unchanged at 0.304. This is evidence of relative
persistence, not positive absolute edge.

## 5. Empirical A–F comparison

The comparison is deliberately labelled **diagnostic-only / non-deployable**. It reuses the old M07 panel,
which predates open-position marking, has no M08/M09/M10 completion, is truncated to 1,000 seats/fold, and
uses only the broader entity view. Returns are after the costs present in that replay, but are not a validated
production portfolio backtest.

### K=20, 14-day refresh where applicable

| Method | Compounded return | Mean fold | Mean sleeve DD | Worst fold | Turnover | Coin conc. | Median / p90 hold | Seat coverage |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| A Static robust | +3.28% | +0.281% | 1.07% | -1.22% | 0.000 | 0.395 | 0.77h / 5.91h | 0.417 |
| B Periodic robust | +9.34% | +0.768% | 2.58% | -3.14% | 0.605 | 0.234 | 0.72h / 5.64h | 1.000 |
| C Recent winner | -2.54% | -0.202% | 4.57% | -3.16% | 0.695 | 0.082 | 2.26h / 16.22h | 1.000 |
| D Regime-conditioned | +4.15% | +0.343% | 1.57% | -0.91% | 0.609 | 0.138 | 0.29h / 2.26h | 1.000 |
| E Behavior-first | -1.86% | -0.155% | 0.87% | -0.91% | 0.455 | 0.263 | 0.12h / 0.93h | 1.000 |
| F Shrunk ensemble | +10.52% | +0.856% | 2.38% | -2.94% | 0.600 | 0.260 | 0.63h / 4.77h | 1.000 |

The apparent winner is not robust to cohort size. At K=10, compounded returns were F +14.01%, D +10.71%,
B +6.47%; at K=40 they were F -2.38%, B -2.52%, and D +0.53%. Several methods change sign. The reported
100th-percentile matched-random results are not deployment evidence because the comparison population is the
activity-truncated, old-economics panel and the full M10 matched-null protocol was not run.

Answers to the core research questions:

1. **Static or dynamic?** The diagnostic panel favors a slow dynamic ensemble at K=10/20, but size sensitivity
   prevents a production choice.
2. **Does recent performance persist?** Relatively yes; absolutely no. It chiefly avoids catastrophic laggards.
3. **Optimal refresh cadence?** Not identified beyond the tested 14-day folds.
4. **Do regimes help?** Not established; only one high-volatility fold exists.
5. **Do behavior filters explain persistence?** No; qualified rank correlation is virtually unchanged.
6. **Do behavior filters improve risk?** They reduce drawdown/holding tails but lose money alone at K=20/40.
7. **Does entity resolution matter?** Potentially enormously: a medium-confidence component contains 476 wallets.
8. **Is the result robust to K?** No; sign changes at K=40.
9. **Is concentration acceptable?** K=20 reduces entity concentration, but coin concentration remains 0.14–0.40.
10. **Are open losses measured?** Not in this diagnostic artifact; the metric is null, so it cannot authorize risk.
11. **Is there a matched-random edge?** Not proven until the full eligible population and M10 protocol are used.
12. **Is there a reliable production selection edge now?** No.

## 6. Recommended production methodology

The production decision today is cash/no trade. Once the remaining P0 gates are complete, the first candidate
to test should be a **14-day, equal-weight or strongly shrunk ensemble**, not pure recent-winner rotation and
not regime switching. This is a hypothesis for the reserved holdout, not an authorization.

Required selection contract:

- Use one immutable as-of run with complete fills, candles, funding, ledger, and marks through the least-fresh
  required watermark.
- Exclude every lifecycle-invalid or stream-invalid action/journey before feature construction.
- Evaluate wallet-only and direct/high-confidence entity views as co-primary robustness views; treat the broad
  temporal graph as sensitivity only.
- Mark every open position at each rebalance boundary. Rank on conservative return that includes all negative
  open P&L; report realized and marked views side by side.
- Require at least 25 completed positions and three independent time blocks, full mark/censoring coverage,
  finite economics, and pre-declared copyability limits. Treat these as starting thresholds to validate, not
  discovered optima.
- Add explicit bag-risk gates: closure fraction, open-age distribution, loss-weighted duration, MAE, time
  underwater, underwater adds, loss-realization share, and open-loss/realized-profit ratio. Do not compress
  these into one opaque score until each component is stable.
- Use conservative equal weights with per-entity, coin, gross, leverage, and capacity caps. If fewer than the
  required number qualify, leave unused seats in cash; never relax thresholds to fill the roster.
- Compare K=10/20/40 and require the same economic conclusion, positive after-cost return, acceptable drawdown,
  and matched-random significance after family-wise correction. No single K is currently supported.
- Run the exact live sizing, reversal, fee, latency, partial-fill, precision, minimum-order and cap contract in
  M09. M10 must include cash, simple trailing-return/Sharpe, static, behavior-only, and matched-random baselines.
- Touch the final holdout once. Record every attempted family member and disposition in the append-only registry.
- Demote a leader on stale data, mark loss, eligibility failure, bag-risk breach, entity ambiguity, or target-vs-
  actual reconciliation failure. Repeated fallback should move the sleeve to cash, not another untested leader.

## 7. Live-engine findings

1. **Authorization is now fail-closed.** Live mode requires halt flags to be clear, a matching arm record,
   config hash, lineage digest, and a freshly revalidated complete deployment manifest. Shadow mode is explicit.
2. **The current roster still cannot pass that gate.** Its manifest is intentionally rejected; this is the
   correct state.
3. **Disconnect recovery is incomplete.** A missed entry is generally not recreated because reconciliation is
   leg-oriented rather than target-oriented. Held-leg flat exits can recover via REST snapshots, but absent legs
   have no general target reconstruction.
4. **Reversal behavior differs.** Research fixed-position replay opens the far side; current live defaults flatten
   only. Changing the flag without a shared parity test would substitute one unproven behavior for another.
5. **Adds/trims are intentionally suppressed for fixed-position copying.** That matches a constant signed target;
   the misleading `full_mirror` wording should be corrected in manifests.
6. **Idempotency is improved but not complete.** Unique indexes enforce the correct durable natural keys, while
   WebSocket event deduplication remains partly in memory. Restart/out-of-order fault fixtures remain necessary.
7. **Reconciliation warns more than it repairs.** Size drift and orphan adoption exist, but the engine does not
   universally calculate `desired target - authoritative actual - open orders` and converge to zero.

## 8. Prioritized implementation plan

### P0 — required before any arm

- **Done:** fail-closed S3 daily completeness and cron exit propagation.
- **Done:** restored maintained M02 tests without obsolete M1 coupling.
- **Done:** immutable research-lineage gate at arm, launcher, and direct live entry.
- **Done:** conservative open-position marking in M07 and mandatory coverage in M06b.
- **Pending:** backfill and verify the 15 missing funding days; rebuild M02–M07 from complete inputs.
- **Pending:** wire mandatory M09/M10 stages and emit a complete hash-pinned deployment manifest.
- **Pending:** implement one side-effect-free replay/live decision core and full event-sequence parity fixture,
  including reversal, disconnect, duplicate, rejection, partial fill, restart, and multiple leaders.
- **Pending:** implement target-vs-actual reconciliation for both present and missing legs.
- **Pending:** rerun A–F on the full eligible population with marked opens and wallet/high/broad entity sensitivity.

### P1 — required for a credible pilot

- **Done:** append-only, hash-chained experiment registry and terminal receipts.
- **Done:** hashed directory inventories and prospective run/failure provenance.
- **Done:** fold-pure wallet-only, high-confidence, and broader entity-resolution generators.
- **Done:** Mongo unique indexes for true natural keys.
- **Pending:** schedule and freshness-test the daily pipeline or formally retire it.
- **Pending:** add edge-level entity provenance/manual review for consequential components.
- **Pending:** fault-injection reconciliation tests and durable event idempotency across restarts.
- **Pending:** reserve and lock a clean final holdout after the methodology is frozen.

### P2 — operational clarity

- Generate one canonical architecture/status page from manifests and tested entrypoints.
- Separate equity-dependent and equity-independent research lanes by name and contract.
- Consolidate reporting around target, actual, drift, costs, open-loss exposure, and lineage digest.
- Add alerting for stale watermarks, partial ingestion, registry failures, and reconciliation quarantine.

## 9. Safe code changes and verification

Implemented changes are deliberately protective: they reject incomplete data/lineage, improve provenance,
make open-loss treatment conservative, add sensitivity/diagnostic tools, enforce durable keys, and require
explicit live intent. They do not select a new roster, arm the system, or submit orders.

Key additions:

- `tools/research_lineage_gate.py`, `tools/live_authorization.py`, `tools/experiment_registry.py`
- `research/v15/entity_resolution_views.py`, `research/v15/selection_strategy_comparison.py`
- Conservative cutoff snapshots in `research/v15/v15_m07_engine.py`
- Conservative ranking/coverage gates in `research/v15/v15_m06b_ranking.py`
- Arm/launcher/experiment/daily-ingestion hardening and focused regression tests
- Live-mode authorization and Mongo natural-key indexes in `strategies/live/hl_copy_trader_v17.py`

Verification on 2026-08-03:

- `492 passed` in the maintained `tests/` suite.
- `git diff --check` passed.
- Changed Python modules passed `py_compile`.
- The current deployment manifest fails the lineage gate as intended.
- Both live pause flags remained present throughout the audit.

## Final decision

There is **no reliable production copy-selection edge demonstrated by the current artifacts**. The evidence is
consistent with modest relative persistence that avoids the worst leaders, while positive absolute returns and
method choice are unstable to cohort size and unverified under correct open-loss, entity, portfolio, and null
contracts. Keep the engine paused and capital in cash until every P0 item above produces a clean immutable run.
