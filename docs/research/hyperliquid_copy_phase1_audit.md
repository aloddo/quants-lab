# Hyperliquid copy trading — Phase 1 audit

Status: **NOT YET TRUSTWORTHY — BLOCKERS REMAIN**  
Audit date: 2026-07-01  
Phase 2 status: gated; no new strategy optimization is authorized from the current artifacts.

## Scope and canonical boundary

This audit treats `research/v15` as the canonical historical research spine and
`strategies/live/hl_copy_trader_v17.py` as the currently deployed execution line.
The historical strategy is perp-only. Spot pairs and HIP-4 outcome markets are
not valid perp actions and are excluded.

The intended development window remains 2025-12-01 through 2026-05-23 so that
data from 2026-05-24 onward can remain a genuinely later holdout. The raw data
extends beyond that boundary; extending the training window requires an explicit
new split, not an in-place change.

## Repository map

| Layer | Canonical implementation | Role | Audit status |
|---|---|---|---|
| M1 | `research/v15/v15_m01_equity_reconstruct.py` | Perp equity, external-flow and anchor reconstruction | Patched; full rebuild required |
| M2 | `research/v15/v15_m02_journey_trace.py` | Causal actions, lifecycle and exposure targets | Patched; full rebuild required |
| M3 | `research/v15/v15_m03_fold_geometry.py` | 42/14/14-day rolling train/validation/test folds | Unit-tested; existing artifact is stale |
| M4 | `research/v15/v15_m04_authenticity.py` | Fold-pure authenticity and entity deduplication | Unit-tested; existing artifact is stale |
| M5 | `research/v15/v15_m05_eligibility.py` | Eligibility and copyability floors | Unit-tested; existing artifact is stale |
| M6a | `research/v15/v15_m06a_shortlist.py` | Cheap causal shortlist | Unit-tested; existing artifact is stale |
| M7 | `research/v15/v15_m07_engine.py` | Per-entity execution and risk simulation | Patched; execution-model reconciliation remains |
| M6b | `research/v15/v15_m06b_ranking.py` | Post-cost ranking and investability gate | Existing manifest correctly says non-investable |
| M8 | `research/v15/v15_m08_survival.py` | Counterfactual survival tiers | Unit-tested; existing artifact is stale |
| M9 | `research/v15/v15_m09_sim.py` | Chained fixed-bankroll portfolio simulation | Library exists; no complete current artifact |
| M10 | `research/v15/v15_m10_gates.py` | Gates and matched-null evaluation | Null generation/integration is incomplete |
| Shared costs | `research/v15/execution_model.py` | Declared source of truth for simple replays | Subaccount and per-coin fee semantics fixed; M7 slippage/latency reconciliation remains |
| Equity-independent actions | `research/v15/v15_equity_independent_actions.py` | Causally ordered atomic leader actions without M1 equity | Patched and smoke-tested |
| Fixed-notional signals | `research/v15/v15_fixed_notional_signals.py` | V17-style open/add/reverse-flow lifecycle contract without M1 equity | New validation lane; risk/portfolio execution still pending |
| Live | `strategies/live/hl_copy_trader_v17.py` | Real-capital V17/V18-config execution | Running and user-modified; deliberately not patched |

`REPO_MAP.md` is stale: it describes V11 as live, labels later V15 modules as
TODO, and reports obsolete data sizes. It must not be used as operational truth
until refreshed.

## Data map

| Dataset | Coverage and size | Rows / keys | Audit result |
|---|---|---|---|
| `app/data/hl_s3_fills` | 178 daily files, 2025-12-01–2026-05-27, 39 GB | 1,224,664,202 rows | Legacy schema; not canonical |
| `app/data/hl_s3_fills_v2` | 232 files, 2025-07-27–08-20 then 2025-12-01–2026-06-24, 10 GB | 137,189,111 rows | 102-day calendar gap; empty 2026-06-25 file |
| `app/data/hl_s3_fills_v2_by_wallet` | 20,378 wallet files, 2025-12-01–2026-06-24, 5.3 GB | 123,330,915 rows | Exact aggregate parity with enriched dailies in canonical cache window |
| Canonical perp subset | 2025-12-01–2026-06-24 | 116,489,584 fills, 20,186 wallets, 365 markets | Perp-only classification after fixes |
| HIP-4 outcomes | Same window | 326,080 fills, 976 `#` symbols | Must not enter perp reconstruction |
| Named spot | Same window | 204,097 `PURR/USDC` fills | Must not enter perp reconstruction |
| Other spot | Same window | 6,311,154 rows (`@`/USDC forms) | Must not enter perp reconstruction |
| `app/data/v13/raw_funding_cache_20k` | Wallet funding histories, 4.4 GB | 31,449 JSON files | Used by M1 |
| `app/data/v13/raw_ledger_cache_20k` | Wallet ledger histories, 724 MB | 23,391 JSON files | Used by M1; taxonomy audited |
| `app/data/v13/wallet_anchor_state.parquet` | Per-wallet position snapshots | 4.7 million rows | Seed source; not a replacement for weekly anchors |
| `app/data/v15/marks_cache` | 1,830 coin series | 57.78 million 1-minute candles | Covers all 365 canonical perps at fill times |
| `app/data/v15/assetctx_marks` | Exact main-dex mark cache | 230 files, 912 MB | Preferred for main-dex MTM |
| `app/data/v15/ohlc_cache` | M7 OHLC cache | 565 legacy five-row files | Must be rebuilt to OHLCV before trusted M7 runs |

Fill integrity checks:

- No duplicate nonzero `(wallet, tid)` keys were found.
- No null/invalid size, price, fee or start-position values were found.
- Daily and per-wallet canonical caches match on row count, range, wallets and
  an order-independent hash over shared source fields.
- 146 rows are assigned to the adjacent daily block partition within
  milliseconds of UTC midnight; event timestamps remain authoritative.
- All canonical perp markets have a mark before their first fill and no perp
  fill is more than 15 minutes after its final available mark.

## Critical model assumptions

1. Weekly `portfolio.perpAllTime` account value is the reconstruction anchor.
2. The model is perp-only across all supported perp dexes; spot and outcomes are excluded.
3. Historical action sizing may use only events and marks available at or before the action.
4. Forward decisions use the prior completed one-minute bar; execution marks older than 15 minutes fail closed.
5. Hyperliquid `fee` is the signed total wallet fee. `builderFee` and `deployerFee` are component disclosures, not additive charges.
6. External capital flows affect cash but are neutralized from trading return.
7. Selection is fold-pure. Test-window behavior cannot affect selection, sizing or ranking for that fold.
8. A historical result is deployable only if the simulated order policy, sizing, latency, fees, slippage, leverage setup and risk gates match the live implementation.
9. M1 leader equity is not required for fixed-notional event copying. It is required for leader-return/drawdown/leverage metrics, equity-based selection or sizing, and any claim about the leader's risk-adjusted performance.

## Current live/replay contract

| Concern | Current V17/V18 live behavior | Validated historical requirement | Status |
|---|---|---|---|
| Leader state | Raw websocket trade deltas in `_v16_leader_pos`; no `startPosition` in the feed | Atomic actions ordered by authoritative start-position chains | Historical path fixed; live has no per-wallet/coin reconciliation after a missed event |
| Entry | Leader position below $1 notional to at least $1; instant guarded taker entry | `live_open_candidate` on each atomic fill, never timestamp-netted | Implemented in independent lane |
| Sizing | Fixed $50 in the current config | Fixed-notional replay; no M1 denominator | M1-independent |
| Adds | Track leader adds and accumulated notional; never copy | Same; adds affect the exit denominator | Implemented in signal lane |
| Leader exit | Full copy exit after reverse flow reaches 85% of accumulated dollar notional | Same dollar-flow state machine, including price-dependent denominator | Implemented; side-divergence edge case surfaced |
| Risk exit | SL -1,500 bps; trail arms +600/retraces 300; max hold 7 days | Causal mark-path replay with identical polling/fill timing and taker costs | Not yet integrated with atomic signals |
| Execution | IOC/taker, chase/spread/depth guards and order failures | Causal L2/mark price, latency, two-way impact, fee and failure sensitivity | Incomplete |
| Selection | Wallet cohort was derived from historical performance artifacts | Fold-pure rebuilt M1/M2 or a separately defined equity-independent selector | Existing cohort provenance invalidated by old M1/M2 defects |
| Portfolio gates | Margin, gross/net/coin, stop latch and operational state | One global time-ordered portfolio replay | Existing V16 engine replay is stale/invalid |

## Confirmed defects and fixes

| Severity | Defect | Impact | Resolution |
|---|---|---|---|
| P0 | M1 anchored to whole-account/all-time state while retaining spot fills whose cash delta was zero | Spot buys created synthetic wealth and contaminated M1/M2 | Restored perp-only anchors, fills, positions and ledger scope; regression tests added |
| P0 | M1/M2 subtracted `fee + builderFee + deployerFee` | Double-counted $11,015,135 across 38,273,510 perp fills and 14,997 wallets | Use total `fee` only; component regression test added |
| P0 | `#` HIP-4 outcomes were treated as main-dex perps | Invalid Buy/Sell/Settlement lifecycle and MTM | Excluded at M1, M2 and M7 boundaries; tests added |
| P0 | `PURR/USDC` named spot was treated as a main-dex perp | 204,097 invalid perp actions | Slash-delimited spot exclusion and tests added |
| P0 | M7 ADV was `mean_price * 1440`, independent of volume | Capacity depended on token price rather than liquidity | OHLCV cache plus trailing dollar volume; missing ADV rejects new exposure |
| P0 | M7's “daily” ADV used the last 1,440 observations rather than 24 wall-clock hours | Sparse HIP-3 series could contribute several days of volume to one-day capacity | Window now uses strict causal trailing 24-hour timestamps |
| P0 | M7 forced exits bypassed size caps but clamped price impact to 5% ADV | Large emergency exits were guaranteed full fills at artificially bounded cost | Full order/ADV now drives impact; missing-ADV forced exits use a conservative full-ADV fallback |
| P1 | M7 mark lookup had no staleness cap | A delisted or stale candle could become an executable price | 15-minute fail-closed cap and test added |
| P1 | Ledger `liquidation` summaries were classified unknown | 80 wallets were quarantined even though fills already carry liquidation economics | Recognized as informational zero-cash events |
| P1 | Drift fields with `_pct` suffix were reported as percentage points although stored as fractions | Misleading diagnostics and waterfall labels | Reporting script corrected; computational thresholds remain 10% median / 50% max |
| P1 | Clean-rerun live guard only recognized an obsolete strategy filename | Heavy rebuild could start while the current live trader was active | Guard expanded to current `hl_copy_trader*.py` names |
| P0 | M9 ignored `pool_provider="matched_null"` and still ran the ranked pool | A requested null could be the strategy itself while being labeled as a null sample | Unsupported providers now raise; matched-null construction remains an explicit blocker |
| P0 | M1 quarantine was not consumed by M5 | Wallets with proven anchor drift or incomplete reconstruction could still enter eligibility/ranking | M5 now requires the M1 audit, hard-excludes quarantined wallets and drops incomplete equity days |
| P0 | M5 estimated leverage from `abs(position_value_usd)`, the signed net MTM | Opposing long/short legs could net to zero and pass the 10x leverage gate | M1 now emits gross marked notional; production M5 requires and uses it |
| P0 | M1/M2 treated `tid` as causal order within same-ms partial-fill bursts | 44% of sampled transitions broke position continuity, corrupting ENTRY/ADDON/TRIM/EXIT labels | Bursts now follow the `startPosition -> endPosition` chain; remaining gaps resync causally and invalidate only interrupted journeys |
| P0 | Legacy `fidelity_replay.roundtrips` did not split reversal fills | Old and new legs merged into a corrupted lifecycle | Reversals now finalize the old leg and seed the residual opposite leg at the same fill |
| P0 | “High-fidelity” taker replay charged fees but omitted crossing slippage | Historical taker copy returns were overstated | Canonical two-way per-coin slippage is now charged; maker remains explicitly an optimistic bound |
| P1 | Shared fixed-copy `mark_at` reused indefinitely stale marks | Dormant/delisted prices could become executions | Added the same 15-minute freshness cap used by M1/M7 |
| P0 | Same-millisecond fills were netted into one equity-independent burst | Netting could erase live-observable opens, exits and reversals | Atomic fill actions are now the default; burst output is diagnostic-only |
| P0 | M2 labeled earlier actions valid before a later gap invalidated their journey | Downstream `lifecycle_valid=true` filters still admitted the proven-broken beginning of a lifecycle | Invalidity now propagates retroactively to all journey actions; raw-stream resync journeys have a separate fail-closed `stream_replay_valid` flag |
| P0 | Existing V16/V17 engine replay consumes the stale M2 stream without lifecycle enforcement and loses causal order when merging equal-time fills | Entry/add/close classification and the 85% exit denominator can differ from live | Existing engine-replay results are invalid; replacement must consume rebuilt atomic actions in explicit sequence |
| P0 | Existing selection/replay reconstructs round trips only from in-window signed deltas | Carry-in positions at lookback/fold boundaries can be misclassified as new entries | Must seed authoritative pre-window state or consume validated journey IDs before rerun |
| P1 | Live leader exit is dollar-notional-threshold based, with no authoritative flat-position override | A leader can fully close/reverse quantity below the 85% dollar ratio after a price move, leaving the copy open until SL/trail/max-hold | Reproduced in the independent lane and surfaced as `leader_side_diverged`; live fix/replay treatment remains open |
| P0 | M1 emitted causal daily equity but computed quarantine drift/reconciliation with a later snapshot-backed ex-post seed | The acceptance audit did not validate the artifact it admitted and could mask or invent drift | Drift and reconciliation now receive the emitted series' exact seed mode; causal-vs-future-snapshot regression test added; the first full rebuild was invalidated and must restart |
| P0 | Daily causal M1 omitted a pre-anchor carry first revealed by a post-anchor fill | Cash identity could look correct at flat prices while signed position, gross notional and subsequent MTM PnL were wrong | Ported the per-event causal carry-in fold into `compute_eq_at`; 27/1,356 pathological-smoke rows changed, with up to $65,759 equity and $1.36M position-value correction |
| P1 | M1 segment reconciliation added `closedPnl` to a mark/trade term that already contained realized PnL | A buy-100/sell-110 round trip reconciled as $20 rather than $10, making consistency evidence misleading | Replaced with the exact self-financing cash+marked-book identity; regression test added |
| P0 | All-dex ledger mapping ignored non-USDC `send` records | USDH/USDT0/USDE collateral transfers into HIP-3 perp dexes were absent from cash and external-flow neutralization | Use source `usdcValue` for any token whose transfer leg touches a perp dex; 2,764 unique wallet-events and $4.59M absolute flow were affected |
| P1 | Wallets with fewer than two usable inter-anchor checks could remain unquarantined | 56 wallets in the old universe audit had zero checks yet were eligible for equity-based selection | Equity-dependent lane now requires at least two checks; fixed-notional equity-independent lane is unaffected |
| P0 | Funding-cache coverage was treated as complete merely because some wallet files existed | Only 1,967/20,378 requested wallets have contiguous funding-file coverage through the M1 window; 14,737 have the same uncovered 2026-05-23 interval, yet M1 did not mark those walks incomplete | Backfill the missing raw S3 hourly funding day, add dataset coverage manifests, and make M1 fail closed on uncovered intervals before rebuilding equity-dependent artifacts |
| P0 | Shared execution model applied the parent referral discount to live subaccounts | Simple replays charged 8.64 bps taker round trip although the fee artifact says subaccounts pay 9.00 bps | Honor `effective_subaccount_taker_oneway` and the discount-applicability flag; tests added |
| P0 | Shared execution model charged main-dex fees to HIP-3 markets | All-coin/xyz replay understated fees by the configured conservative 2× multiplier when no market override existed | `fee_rt` now accepts `coin` and matches M7's per-market/multiplier contract; fidelity replay migrated |
| P0 | M7 loaded M2 actions without lifecycle/stream validity | Broken or causally resynced journeys still entered execution simulation despite being quarantined in M2/M5 | Runner now requires the rebuilt schema and filters to `stream_replay_valid=true`; stale M2 fails loudly |
| P0 | M3/M5/M6 counted lifecycle-invalid or raw-stream-unreplayable journeys in activity, persistence and consistency | Broken state paths could make wallets appear persistent and sufficiently supported even when M7 correctly rejected their actions | M2 now persists journey-level `stream_replay_valid`; M3/M5/M6 require and filter both validity dimensions |
| P0 | M4 Sharpe used raw account-value changes | Deposits/withdrawals could fabricate performance and change entity-primary selection | M4 anchor returns now use trading+funding PnL after external-flow neutralization |
| P0 | M9 carried-subaccount top-ups changed the scalar `start_equity` but not `AccountState` collateral | Portfolio cash was debited while engine cash vanished from the carried state; the prior fake-engine test did not inspect state | Top-ups are now applied to a copied carried state and a state-level conservation regression test was added |
| P0 | M9 consumed one global M4 entity map | A later/future primary wallet mapping could be used in earlier folds | M9 now consumes `(entity_id, fold_id)` mappings; forward selection requires all fold-pure M4 files |
| P1 | M8 could tier a provisional/uncalibrated M6b pool | Downstream output could look deployment-ready despite M6b explicitly reporting `investable=false` | M8 now fails closed unless every pooled row is investable |
| P1 | Equity-independent research stopped at M5/M7 | M1 quarantine unnecessarily blocked fixed-notional lifecycle execution | M5/M6a now expose copyability/activity lanes; M7/M8/M9 expose explicit `fixed_position` sizing without implicit fallback from missing equity |
| P1 | M7 liquidation orders always paid a flat 30 bps penalty | A forced order sent to the book had no size/ADV dependence, understating large liquidation tail losses | Liquidations now pay max(30 bps, half-spread + uncapped full-order/ADV impact); no clearance fee remains correct |

The fee correction alone invalidates every existing M1-derived action, journey,
ranking and strategy result. Existing artifacts cannot be repaired in place;
they must be regenerated from M1.

Equity-independent lifecycle audit: the initial deterministic 500-wallet sample
covered 9,044,402 perp fills. Naive `(time, tid)` ordering failed 43.85% of
comparable transitions. Position-chain ordering reduced failures to 0.069% on a
2.48-million-transition verification sample; residual gaps now produce explicit
state resynchronization and `lifecycle_valid=false` rather than silent drift.
The equity-independent lane now emits atomic actions by default. In the
11-wallet smoke, 312,503 source fills contained 2,309 valid dust-to-open signals;
diagnostic burst netting retained only 2,292 of them, a 0.74% undercount. The
V17-style 85% lifecycle pass produced 2,219 conservative signal rows: 2,114
leader-threshold closes, 67 right-censored opens, 7 stream-gap invalidations,
and 31 unresolved leader-side divergences. The divergence state deliberately
blocks later entries until the risk-overlay replay is integrated, so these
counts are validation diagnostics rather than performance results.

## Validation performed

- Exact canonical daily/per-wallet parity check.
- Exact nonzero wallet/trade-id duplicate check.
- Market-class and field-validity profiling.
- Perp fill/mark boundary and freshness check.
- Unit tests for spot/outcome isolation, fee semantics, liquidation ledger
  classification, dollar-volume ADV and stale-mark rejection.
- Current regression suite: 435 passed across `tests/v13`, `tests/v15` and the
  Phase 1 data-audit tests after the latest fixes.
- After memory was freed, the corrected causal-audit 11-wallet M1 validation
  completed in 1.0 minute with a 1 GB live-system reserve. Ten wallets were
  reconstructable; seven intentionally pathological wallets were quarantined.
  Clean examples showed median inter-anchor drift of 0.001%–0.14%, and all
  emitted rows obeyed
  `gross_position_notional_usd >= abs(position_value_usd)`.
- Full run `phase1_m01_rebuild_20260701_1826` completed with 2,986,973 daily
  rows for 20,156 wallets. Its validator reports `hard_fail=false` at
  `app/data/v15/phase1_rebuild_20260701_1826/m01_validation.json`.
- The M01 equity gate quarantines 10,475 wallets, but this is specifically an
  **equity-reconstruction** trust label, not a fill/lifecycle trust label.
  9,781 trip the current any-segment max-drift >50% rule and 5,786 trip that
  rule alone; only 3,556 fail median drift >10%. This conservative global veto
  must be replaced by causal segment-level quality for equity-dependent work.
- Core M02 is now equity-independent by default: ordered fill actions and
  journeys run without anchors, M01 reconstruction, quarantine, or mark cache.
  `--equity-enrichment` explicitly opts into leader-equity sizing. The full
  core rebuild is running separately under `phase1_rebuild_20260701_1826`.

## Artifact lineage audit

The current artifact tree is not a coherent build:

- Top-level M2 actions/journeys were produced on 2026-06-25.
- Top-level M3–M8 artifacts are primarily from 2026-06-01 through 2026-06-09.
- `app/data/v15/rebuild_chain` contains a newer partial M1–M7 chain but no
  complete M6b/M8/M9/M10 result.
- `m06b_manifest.json` says `investable: false` because no slippage calibration
  version was attached.
- Inputs are not bound to outputs by content hashes, code commit, schema version
  and config manifest.

Therefore no current table or wallet rank has trustworthy end-to-end provenance.

## Remaining blockers

1. Reconcile M7's detailed capacity/impact/latency model with the simpler
   `execution_model.py`. Base, subaccount and HIP-3 fee contracts now agree;
   slippage and latency implementations still differ.
2. Rebuild all 565 legacy M7 OHLC caches as OHLCV and validate volume units.
3. Add immutable run manifests containing git commit/diff hash, input hashes,
   schema versions, configs, split boundaries and output hashes.
4. Run a provenance-safe full M1 rebuild, compare anchor drift before/after the
   fee, asset and causal-audit fixes, then rebuild M2–M8. The rebuild started at
   17:23 on 2026-07-01 is explicitly invalidated and cannot feed M2.
5. Complete and exercise M9 chaining and M10 matched-null generation; unit-test
   functions alone are insufficient.
6. Prove historical/live equivalence. M7 mirrors exposure deltas while current
   V17/V18 live logic copies selected opens with cluster/k-net gates and its own exits.
7. Resolve live-only risk defects separately, without hot-editing the running
   user-modified process: marked equity is not the denominator for all admission
   gates, leverage setup is best-effort rather than fail-closed, and configured
   daily loss is not enforced.
8. Replace the stale engine-faithful replay input path with atomic, validity-
   enforced actions; seed fold-boundary leader state and integrate SL/trail/
   max-hold so `leader_side_diverged` copies can exit and later signals resume.
9. Resolve M7 fold-start semantics. `future_delta_only` currently converges to
   full source exposure on the first in-window action, while
   `causal_carry_in` seeds a position without an executable fill and its status
   predicate does not match M2's `SEEDED` label. Neither is a deployable live
   start policy as written.

## Phase 1 verdict

**Not yet, blockers remain.** Phase 2 results produced from the current artifact
tree would be invalid. The next deciding checkpoint is a clean, manifest-bound
M1 reconstruction showing acceptable out-of-sample anchor drift after the
confirmed accounting fixes.
