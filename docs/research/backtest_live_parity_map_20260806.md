# Backtest ↔ Live Equivalence Audit — Semantic Map of Leader-Event Handling

**Scope:** `research/v15/v15_m07_engine.py` (M07, per-subaccount replay), `research/v15/v15_m09_sim.py` (M09, chained portfolio sim — executes every event *through* M07), `strategies/live/hl_copy_trader_v17.py` (live engine, class chain `CopyTrader ← V16 ← V17`), `research/v15/execution_model.py` (canonical slippage/fees/latency), last live config `config/copy_trader_v15recent9_20260731.json`.

**Structural note before the table.** M09 does not have its own event semantics: it calls `eng.step_subaccount(...)` per selected wallet per fold (`v15_m09_sim.py:739-740`), so every per-event row below is inherited from M07; M09 only adds allocation (water-fill L620, gross budget 3.0×b0 L653-656), fold-boundary flattens of dropped wallets via synthetic `EXIT` rows priced through M07 with `copy_latency_ms=0` (L500-526), carried state across folds (winners compound, L722-731), and its own portfolio kills (G4 −50% L386-409/787-822, global-DD de-risk −35% L373-383/769-782). Also note: **M07 does NOT price fills through `execution_model.py`** — it has its own cost model (SLIP_BANDS + half-spread + impact + per-fold `slippage_calib_v11` + bar-drift latency haircut). `execution_model.py` is the canonical model for the *scorer* family (`forward_oos_hot.py`, `research/v16/select_cohort.py` — the source of the per-wallet "OOS mean +Xbps" strings in the live config) and is imported by the live engine only for `fee_rt` in the expansion-kill accounting (`hl_copy_trader_v17.py:6947-6948`). Two research cost models coexist; they agree on fees (both read `hl_fee_schedule.json`) but not on latency (`execution_model.LATENCY_MS = 1000` L53 vs M07 `copy_latency_ms = 4000` L487).

## Dimension-by-dimension semantic map

| Dimension | M07 replay (`v15_m07_engine.py`) | M09 sim (`v15_m09_sim.py`) | Live V17 (`hl_copy_trader_v17.py` + config) | DIVERGENT? |
|---|---|---|---|---|
| **Entry (leader flat→open)** | Trigger: any action row; target = `sign(position_after) × fixed_target_exposure` (default **0.10 of follower sleeve equity**, `_action_target_pct` L509-525); order = delta to target (L794-801). Latency: `our_ts = leader_ts + copy_latency_ms` (L740); default **4000ms** (L487, L2089) but CLI default **2000ms** (L2303) — internal inconsistency. Fill: causal prior-1m-bar close × (1 ± (half_spread+impact·participation^α)·band) (L1088-1092) **plus** always-adverse latency bar-drift haircut `LAT_DRIFT_K·(lat/60s)·|prior-bar return|` (L1094-1113). Guaranteed fill. | Same engine per sleeve (L739-740); sleeve start equity from water-fill (L620-656, cold-start L733-738); `fixed_target_exposure=0.10` of sleeve (L52-53, L980-981). | Trigger: WS trade classified `OPEN` by shared `classify_leader_fill` (L5476-5479); then gates: cooldown 30s (L3588), 300s post-exit cooldown (L1331-1335), margin util ≤0.7 / gross_open 2.0× / gross_entry 3.0× / per-coin cap (L1837-1900), **chase ≤15bps** (L3617-3620), **spread ≤20bps** (L3628-3633), **depth ≥$3000** (L3636-3640), knet stamp + stale-tracker 30s (L7466-7505), netx 2.5× / coin-side 2.0× caps. Sizing: **fixed $100 notional** (`order_size_usd`, L2247, L2274; config L4-5). Latency: real, measured median **3.21s** signal→fill (comment L481-487). Fill: IOC limit at `best_ask×1.003` / `best_bid×0.997` (L2336-2342) — can NOT fill (L2433-2437, signal dropped). | **YES** — sizing basis ($ fixed vs equity-fraction), entry gates that drop signals, guaranteed-fill vs IOC-may-miss, modeled vs real latency. |
| **Addon (leader adds)** | `full_mirror`: an ADDON row leaves the fixed target's sign unchanged → order is only the **equity-drift rebalance** `0.10×cur_eq − current`, skipped when < $10 min-notional (L1057). So adds are *not* scaled, but small re-size orders DO fire when sleeve equity has drifted ≥ ~1%. `entry_trail` policy ignores adds outright (L789). | Inherited. | Verb `ADD` → **tracked, never copied** unless `copy_adds_enabled` (default False L5361; **config `false`** L321): L5488-5530 updates tracker + `_position_accumulated` and returns before the base add-on merge. `max_addon_multiplier` is a documented **placebo** for add copying in fixed sizing (validator L5239-5251). If enabled: debounced converge-to-first-entry-ratio (`_converge_add_once` L5692-5751). | **Mostly NO** (both suppress) — residual divergence: M07 emits drift-rebalance orders live never emits. |
| **Trim (leader partial close)** | `position_after ≠ 0`, same sign → target unchanged (`exit_cond` false, L776) → no trade beyond drift rebalance. M07 fixed_position **holds constant size through partials**. | Inherited. | Verb `REDUCE` while held → routed to base, but under `exit_type=LEADER_FLAT` the FIRST_CLOSE trim machinery is bypassed (L2815-2853 runs before the L2855 buffer branch). Downward convergence gated on `copy_trims_enabled` (L183, L6433-6434); **config `false`** (L322) and the validator *requires* false under LEADER_FLAT (L5231-5234). Legacy FIRST_CLOSE path (ratio ≥ `exit_min_trim_pct` 0.85, `exit_min_trim_usd` 1e9 = disabled) exists at L2855-2934. | **NO** (matched by config + validator), with the same drift-rebalance caveat. |
| **Exit (leader flat)** | `exit_cond = action_type ∈ {EXIT,CLOSE} or position_after == 0.0` (L776) → target 0 (L797-798), full close at causal mark ± slip at `leader_ts + 4s`. Zero-test is on **size** (parity note cited at live L1236-1239). | Inherited; plus fold-boundary flatten of dropped wallets (L500-526) and intervention flattens (L805-822). | `exit_type=LEADER_FLAT` (config L345): REST `clearinghouseState` snapshot poll every **10s** (`_prefetch_leader_snapshots` L1193-1229), exit when leader `szi==0` or \|szi\|×mark < **$10** dust floor or flipped (L1298-1312), after **90s entry grace** + **2 fresh-snapshot confirms** (L1264-1265, L1319-1326); only the exchange may authorize a close, tracker can only HOLD (L1246-1252). Then straight-to-IOC taker exit (V16 pre-sets maker flags L5675-5679; escalating slip 0.3%/1%/2% L3318-3330). | **PARTIAL** — same *rule* (flat-or-flipped), different *detection*: effective exit latency ≈ 20-110s (poll×confirms+grace) vs 4s; $10 dust floor is an admitted deviation (L1241-1244). |
| **Reversal (flip through zero)** | Target flips sign in one order; `_apply_order` flip gates (isolated L1129-1144, cross L1145-1148ff) and `_book_fill` flip branch **closes old leg AND opens the opposite residual** (L1456-1483). **Replay FLIPS.** | Inherited — the chained sim also flips. | Verb `REVERSE` (flip while we hold, new leg ≥ `reverse_min_notional` $10): always **flatten** via durable `_pending_reverse` intent executed inside `_check_exits` (L5584-5647, L2624-2630, `_execute_pending_reverse` …L6198-6315); far side opened **only if `copy_reverse_enabled`** — default **False** (L5370-5373), key **absent from config** → flatten-only (L6231-6234). Audit claim confirmed (`docs/research/copy_selection_audit_20260803_final.md:56`). | **YES — P0.** Research holds the flipped side; live goes flat. |
| **Partial fills of OUR orders** | Not modeled — every order fills atomically at the modeled price. Systematic analogues only: capacity cap 5%·ADV downsizes (L1072-1081), $10 min-notional skip (L1057), lot rounding (L1052-1054), IM-gate rejections (L1064, L1143, L1157, L1176), stale-mark (>15min) skip → `metadata_uncertain` (L349, L768-771). | Inherited. | Entry IOC partial: position records actual `totalSz` (L2367) and the **unfilled remainder is dropped** (no retry; only pre-placement 429s retried L2334-2360). Exit partial: size reduced, retried next poll (L3391-3397); maker partial residual kept (L3276-3299). | **YES** — live under-sizes on partial entry fills and can miss whole entries; replay never does. |
| **Missed entries / disconnect / reconciliation** | No disconnect concept — 100% signal capture of fold-frozen actions; only skips: latency-pushed past fold end (L744-745, counted L653-654), NaN/stale mark. Target-vs-actual measured as time-weighted L1 `tracking_error` (L697-733, L972-989) but never "repaired". | Inherited; carried-state chaining is exact. | WS reconnect **wipes** `_twap_buffer`, mids, books (L4981-4989); leader opens missed during an outage are **never copied** (no target-vs-actual entry reconciliation; `backfill.enabled=false` in config L312-319 and one-shot by design; `_leader_book_sweep` alert-only, `sweep_auto_close=False` L195-199, L6326+); stale-tracker blocks entries 30s after last seen target fill (L7466-7470). Missed *closes* ARE caught (LEADER_FLAT REST poll). `_reconcile_positions` (L4087-4226, every 300s) removes phantoms/direction mismatches only — it never opens missing legs. | **YES — P0** (audit L57). Entry-side capture is structurally incomplete live and perfect in replay. |
| **Position sizing model** | `fixed_position`: 0.10 × **compounding sleeve equity** per open leader leg (L494-496, L509-525, L794); `leader_equity` deprecated and refuses on all-null stores (L1961-2050). | Manifest `fixed_target_exposure=0.10` of the water-filled sleeve; b0=$500; gross ≤ 3.0×b0; per-entity cap 40%; feasibility check `target_count ≤ b0/(min_notional/exposure)` (L989-996). | **`sizing_mode: "fixed"`, `order_size_usd: 100`** (config L4-5; validator forces fixed and $10-200 range L5198-5201): flat $100 per (wallet,coin) leg regardless of our equity or leader size; no compounding; tilt disabled (L350). | **YES** — dollar-fixed vs equity-fraction; live never re-sizes with equity, replay does. |
| **Fees / slippage / funding** | Fees: `FeeSchedule.taker` per fill from `hl_fee_schedule.json` `effective_subaccount_taker_oneway` (4.5bps one-way; HIP-3 ×2 or per-market) (L227-254, L1115-1116). Slippage: half-spread + `k·participation^α` × band + adverse bar-drift latency haircut; per-fold v11 calib override (L387-439, L2194-2201). Funding: **modeled hourly** from Mongo rates, boundary-exclusive (L1502-1524, L1567-1573). Liquidation ladder/backstop/ruin modeled (L1722-1896). | Inherited; installs per-fold slip calib before each fold (L532-534). | All real: fees charged by HL (same 4.5bps one-way subaccount taker — schedule file shared); slippage = actual IOC cross bounded at +30bps (L2337-2339) behind a 15bps chase gate; funding settles in real equity but is **not attributed** in the engine's fill-based PnL cache (`account_net = closedPnl − fees` over fills, L391, L4438+, L4526-4527 — funding is not a fill). | **PARTIAL** — fees matched by shared artifact; slippage modeled-vs-real (calibration risk, not semantic); funding accrual matched economically, mismatched in live attribution/telemetry. |
| **Stops / trailing / kill switches (live-only)** | **None** in `full_mirror`: no SL, no trail, no max-hold, no global stop. Optional `follower_trail` breaker default **None** (L490-506); `entry_trail` trailing-TP is a different policy (unused for the deployed cohort). Only forced exits: liquidation/backstop/ruin. | Adds G4 intra-fold kill at **−50%** of fold-initial (L386-409) and global-DD de-risk **−35%** (L373-383) → flatten-to-cash (L805-822); gross cap 3.0×b0. | Present and absent from replay: hard SL `sl_bps=-2500` (−25%) (L2714-2733); trailing stop `trail_activate_bps=100000` (+1000% → **effectively inert**)/`trail_bps=300` (L2735-2759); `max_hold_s=2592000` (30d, near-inert) (L2761-2787); **global stop −15% latched flatten-all** (L2532-2550, L4604-4625, `_emergency_flatten` L2447-2479); `max_daily_loss=-25` kill (config L8, `_kill_reasons` L386); gross backstop 4.0× auto-TRIM (L2596-2604); per-coin expansion kill −$25 or (n≥20 & mean<0), expansion-wide kill −$50, entry-blocking (L6954-6962, L7460-7463); margin util 0.7, netx 2.5×, coin-side 2.0×, gross entry/open gates 3.0×/2.0×. | **YES** — live truncates both tails (SL −25%, stop −15%, kills) with no replay counterpart; M09's kills use different thresholds (−50/−35) than live (−15). |
| **Order type** | Implicit taker, guaranteed fill at modeled price; no book-state rejections; forced liq orders at max(30bps, curve) (L1813-1836). | Inherited. | Entry: aggressive IOC limit (±30bps cross cap) — may partially fill or miss entirely; rejected/no-fill = dropped signal (L2361-2437). Exit: V16 forces taker/IOC (skips base maker-ALO leg, L5671-5679) with escalating slip and fill-verified retry. | **YES** on the entry side (fill probability + price bound unmodeled); exits approximately matched (taker both sides). |

## (A) Minimal divergence set to close for "backtest == live", ranked by PnL impact

1. **Sizing model** — $100 fixed live vs 10%-of-compounding-sleeve in M07/M09. Affects literally every fill and all compounding/DD statistics; also makes M07's drift-rebalance orders (addon/trim rows) fictitious. (`v15_m07_engine.py:509-525,794` vs config L4-5 + `hl_copy_trader_v17.py:2247`)
2. **Entry capture rate** — live drops signals via disconnect buffer-wipe (L4981-4989), stale-tracker (L7466), IOC no-fill (L2433), chase/spread/depth (L3617-3640), knet/cooldowns/margin/gross gates, with **no target-vs-actual entry reconciliation**; replay captures 100%. This is a biased filter (fast-moving entries — plausibly the best ones — are preferentially rejected by the chase gate).
3. **Reversal semantics** — M07 flips to the far side (L1456-1483); live flattens only (`copy_reverse_enabled` absent→False, L5373, L6231-6234). Every leader flip = full sign-exposure divergence for the rest of the leg.
4. **Risk stops present only live** — hard SL −25%, global stop −15% latched, per-coin/expansion kills, gross-backstop trim. These truncate exactly the tail paths where copy PnL concentrates; the backtest verdict never priced them.
5. **Exit detection latency** — LEADER_FLAT effective ≈ 20-110s (10s poll × 2 confirms + 90s grace + $10 dust floor) vs M07's 4s uniform latency on `position_after==0`.
6. **Partial-fill remainder dropped on entries** — persistent live under-sizing invisible to replay (L2367 vs no-op).
7. **Latency constants disagree within research** — `execution_model.LATENCY_MS=1000` (L53), M07 EngineParams/`run_shortlist` 4000 (L487/2089), M07 CLI default 2000 (L2303), live measured 3.21s median. The selection scorers (execution_model family) are the most optimistic.

## (B) Which side should move, per divergence

| Divergence | Move | Why |
|---|---|---|
| Sizing | **Teach replay**: add a `fixed_notional_usd` sizing mode to M07 (and thread through M09). | Live `fixed` is the validated/authorized deployment; live `proportional` is explicitly unvalidated and the validator refuses non-fixed (L5198-5199). |
| Reversal | **Teach replay** flatten-only (`copy_reverse=False` semantics: on sign flip, target→0 not flip). | Audit P0 says do NOT enable `copy_reverse_enabled` live by config alone; flatten-only is the codex-gated safe behavior, so research must measure what live actually does. |
| Entry capture | **Live moves** (target-vs-actual entry reconciliation: sweep-driven open of missed legs or reconnect backfill, behind the existing class-B caps) **and** replay gains a capture-rate/gate model or at minimum reports live capture-rate so replay results are reweighted. | The gap is a live defect (audit P0 #2); no config can restore lost signals. |
| Exit latency/dust floor | **Replay moves**: separate `exit_latency_ms ≈ 25-30s` (+90s grace on legs younger than grace) and $10 leader-dust-floor in M07's `exit_cond`. | Live's REST-confirmed exit is a deliberate safety property (only-exchange-authorizes-close, L1246-1252); replay must price it, not the reverse. |
| Stops/kills | Split: trail (+1000% activate) and max_hold (30d) are already inert live — leave. SL −25%, global −15%, per-coin/expansion kills are non-negotiable risk floor → **teach replay** optional matching layers (M07 flags; M09 thresholds set to live's −15% instead of G4 −50/DD −35 for the parity run). | Removing live protections to match a backtest inverts the burden of proof. |
| Partial fills | **Live moves**: bounded residual-retry of a partially-filled entry IOC to target size. | Cheaper and strictly closer to both M07 and the fill the scorer priced. |
| Latency constants | **Research moves**: set `execution_model.set_latency_ms` and the M07 CLI default to the measured 3.2-4.0s; keep the documented coupling with `v15_m05_eligibility.P95_COPY_LATENCY_S` (L481-487 note). | Live latency is physical. |
| Fees/funding | None (fees already unified via `hl_fee_schedule.json`); optionally add funding attribution to live PnL telemetry (`userFunding`) so live reporting matches the replay's after-funding numbers. | |

## (C) Shared event-sequence parity fixture — required coverage

One deterministic leader-event stream fed to BOTH (a) M07 in fixed-$ + flatten-only + live-stop configuration and (b) the live engine's decision core (shadow/dry-run, exchange mocked), asserting order-by-order equality of `(ts, coin, side, notional, reduce_only, reason)`. Per the audit (L208-209) it must cover, with the live-specific traps found above:

1. **Reversal**: single-fill flip; fast two-fill flip; double flip while the first flatten is in flight (generation binding L5617, L6252-6263); flip landing on dust (<$10 ⇒ CLOSE not REVERSE, L5578-5580); flip on a leg we never held (`REDUCE_NOT_HELD` L5532-5563); both `copy_reverse_enabled` states; knet-stamp consume/restore on persist failure (L6288-6296).
2. **Disconnect**: WS gap spanning a leader open (must assert live misses it and the reconciliation—once built—repairs it), a leader close (LEADER_FLAT REST must catch it), and a flip; buffer wipe on reconnect (L4981-4989); stale-tracker 30s window blocking the first post-reconnect entry (L7466-7470); LEADER_FLAT REST outage → UNKNOWN → hold + streak reset (L1287-1290) + outage alert (L1219-1226).
3. **Duplicate**: same `tid` redelivered (dedup L3456-3459, L5493-5495); `_seen_tids` 300s pruning then redelivery; duplicate order-status/WS fill vs REST fill (`_exit_recorded` dedup L3188-3191, L3343-3345).
4. **Rejection**: IOC returns error status (L2430-2431); IOC no-fill (L2433-2437 — assert no tracker/persistence mutation and the sim marks a missed trade); 429 retry path (L2334-2360, must not double-place); knet NO-STAMP reject (L7501-7505); chase/spread/depth/margin/gross-gate rejects with tracker still updated (V16 unconditional tracker L5340-5346).
5. **Partial fill**: entry IOC partial (`totalSz < sz`) — assert recorded size and the *dropped remainder* becomes an explicit tracking-error event; exit IOC partial retry loop (L3391-3397); maker partial residual (L3276-3299); sub-$10 uncloseable residual hold (L3090-3108).
6. **Restart**: position recovery + persisted `_pending_reverse` and `DB_PENDING_REVERSE` far-side obligation surviving a crash between flatten and open (L6272-6300); `_awaiting_retire` marker (L6305-6311); per-coin/expansion kill state reload fail-closed (L7056-7061); backfill ledger idempotency (L5418-5424); leg-scoped LEADER_FLAT streak keys not inherited by a new leg (L2834-2845, L1277).
7. **Multiple leaders**: two leaders same coin same side ($100 each — net 2 legs, exchange 1 position); opposite sides netting the exchange position through zero → exit direction-mismatch path (L3034-3046); add-on reconstruct guard when another wallet tracks the coin (L2141-2150); per-(wallet,coin) LEADER_FLAT decisions and exits independent while the exchange position is shared; `min-notional bump using exchange size only when sole holder` (L3075-3089).

Plus two fixture invariants the audit implies: (i) the M07 run and the live-core run must consume the *same* execution constants (fees from `hl_fee_schedule.json`, one latency value, one slippage source) and report `calibrated_share()`; (ii) the fixture must run once with the live risk stops enabled on both sides and once with them disabled on both sides, so stop-truncation is never the hidden explanation for a PnL delta.
---

# PLAN v2 (2026-08-06, post-Fable plan gate) — certification criterion, sequencing, and scope revisions

Fable plan-gate verdict on v1: **REJECTED with required changes** (9 findings; verbatim record on
card/quant-engineer/canonical-copy-backtest-path). v2 incorporates findings 1-6 as binding scope and
7-9 as parity-report amendments. The v1 semantic map, divergence ranking 1-7, who-moves assignments,
and fixture scenario classes stand unmodified; this section supersedes only the certification
criterion, the sequencing, and adds divergence row 8.

## Divergence 8 (new, P1): coin-universe admission
- Live trades ONLY the whitelist universe plus expansion-admitted coins (whitelist assertion L5328,
  static universe L2020-2060, expansion admission flag-gated L6858-6869); per-coin expansion kills
  remove coins mid-flight; new listings are invisible until admitted. M07 replays every perp coin the
  leader trades that has candle coverage, and conversely skips stale-mark coins live would trade.
- Who moves: parity-configured M07 gains a coin-admission filter matching the live config universe
  as-of the run (and reports excluded-notional and excluded-PnL share per run regardless).
- Fixture addition: leader opens a non-admitted coin -> BOTH sides must produce no order.

## Certification criterion (replaces "the fixture certifies equivalence") — Fable P0
Three stages, all registered in the experiment registry; the deliverable sentence to Alberto is
"equivalent on certified paths, with measured capture rate X% and unexplained residual Y bps/position",
NEVER blanket "equivalent":
1. **Decision parity (fixture):** the v1 section-C fixture + coin-admission case + refusal-to-chase
   case; asserts order-by-order identity of (ts, coin, side, notional, reduce_only, reason) between
   parity-M07 and the live decision core on one deterministic event stream; run once with stops on
   both sides, once off both sides; identical execution constants, calibrated_share() reported.
2. **Signal parity (new):** for >= 10 real leader-days spanning at minimum a TWAP-heavy leader, an
   xyz/HIP-3 coin, and a dust-trim leader: diff the M02-derived action stream against the live
   tracker event log on (verb, ts-basis, coin, sign, zero-crossing); report the disagreement rate by
   class. This catches classification divergence BEFORE execution modeling, which stage 1 is
   structurally blind to.
3. **End-to-end reconciliation (new):** run parity-configured M07 over the REAL recent9 live window
   (2026-07-31 12:37 -> 2026-08-02 07:37; 71 real fills, 33 round trips already on the exchange
   record) and reconcile trade-by-trade against actual fills, with a PRE-REGISTERED residual budget
   (unexplained PnL delta per position, bps) and measured capture rate. No shadow instance is run
   (paper trading banned; HL per-IP channel cap) — the historical live window IS the reconciliation
   set. If a future live window exists at reconciliation time, prefer the freshest.

## Bounded reconciliation & retries — Fable P1 finding 3
- Target-vs-actual reconciliation opens and partial-fill residual retries are CHASE-BOUNDED relative
  to the leader's entry price: open only if the adverse move since leader entry is within the bound,
  else log as missed (a priced, counted event — never silent).
- Flag-gated, default OFF; flag-off behavior byte-identical to the validated engine.
- Fixture disconnect case asserts BOTH branches: the repair (within bound) and the refusal-to-chase
  (outside bound).

## Live-change sequencing — Fable P1 finding 4
- ALL v17 changes land flag-gated with flag-off byte-identical to the validated engine (house
  pattern: expansion mechanism L6858-6862).
- Live edits merge only AFTER the fixture exists — the fixture is their test harness.
- codex adversarial review per diff; arming with ANY new flag ON requires Alberto's explicit GO.
  The engine being halted does not waive this.

## M07 regression protection — Fable P1 finding 5
- Every new M07 behavior (fixed_notional_usd sizing, flatten-only reversal MODE — flip stays
  available, exit-latency model, optional stop layers) is opt-in; defaults preserve current semantics.
- Each gets a "disabled == byte-identical" golden test (template:
  tests/v15/test_m07.py::test_follower_trail_disabled_is_byte_identical).
- fixed_notional mode gets a test asserting ZERO drift-rebalance orders.
- The CLI copy_latency_ms default is NOT changed; the parity manifest sets latency explicitly; every
  registry row pins the m07 code SHA + an engine version tag so pre/post numbers can never mix.
- M09 defaults (G4 0.50 / DD 0.35) untouched; the parity variant sets 0.15 via manifest only.

## Execution-model unification, staged — Fable P1 finding 6
- Phase 0 unifies FEES (already shared artifact) and LATENCY constants, and makes execution_model.py
  the single INTERFACE.
- M07's richer slippage model (participation impact, bar-drift latency haircut, per-fold calib) moves
  BEHIND that interface as the canonical provider — it is NOT replaced by the static per-coin table
  (that would be a fidelity downgrade).
- Before any funnel number: old-vs-new reconciliation on a pinned fill set (per-coin one-way bps
  delta + calibrated_share()), codex-gated. Pre-audit M07 artifacts are already unusable, so there is
  no history contamination; the risk is new-model correctness, which the A/B addresses.

## Parity-report amendments — Fable P2 findings 7-9
- Document the residual scope mismatch: live global stop is account-equity latched flatten-all;
  M09 kills are fold-scoped. Stated in the report, not silently absorbed.
- Stressed-exit sensitivity: exits priced at the escalating ladder's worst rung (2%) so fast-market
  exit cost is bounded, not assumed.
- The 2026-05-31 streaming-IO gate (small-slice /usr/bin/time -l, flat RSS, stitch check) applies to
  the parity-configured full-census M07 run, not only the M02 rebuild.

---

# STAGE-1 FIXTURE FINDINGS (2026-08-06, decision-parity harness build)

## Divergence row 9 (new): 300s post-exit cooldown — entry-capture class
Live blocks re-entry on a coin for 300s after our exit (`_is_opening_trade`, v17 L1331-1335); m07 has
no counterpart. A leader who exits and re-opens the same coin within 5 minutes is copied by
parity-m07 and refused live. Same class as divergence 2 (entry capture); needs its own priced
estimate in the Stage-3 reconciliation (count of leader re-opens within 300s of an exit).

## Candidate live defect L-F4: stale `_target_positions` after LEADER_FLAT (verify then fix)
`_prefetch_leader_snapshots` writes only coins PRESENT in the snapshot body (L1228-1229);
clearinghouseState omits flat coins, so a leader-flat never zeroes the tracker via the snapshot
path. If the leader's closing fill was missed on WS (exactly the orphan mode LEADER_FLAT exists
for), the stale same-sign tracker makes `_is_opening_trade` (L1337-1346) refuse the leader's next
OPPOSITE-side open. Missed entry, not a bad one — but structural. Needs a targeted test + fix in
the flag-gated live-fix batch.

## Candidate live race L-F5: global-stop fast latch does not block entries for <=60s
`_evaluate_global_stop_fast` (L2532-2548) sets `_kill_reasons` + `_flatten_requested` but NOT
`_kill_switch_active`; the entry block at base `_enter_position` L2096 keys on
`_kill_switch_active`, which is synced from `_kill_reasons` only in the 60s stats loop (~L4646).
Window: a WS entry between the fast latch and the next sync passes the block; bounded (every sweep
re-flattens) but a real open-then-flatten churn path paying fees during a stop event. Fix candidate:
set `_kill_switch_active` directly in the fast path.

## Decision-core extraction spec (from the harness build)
Driving ONE leader fill needs ~35 instance attributes across three class layers; `_check_exits`
needs ~21. The natural extraction seam: (verb router + entry-gate evaluator + exit-rule evaluator)
as pure functions over an explicit LegState snapshot, with `_on_hl_trade`/`_check_exits` as thin
I/O shells. `_v17_knet_pending` entangles signal classification with gate authorization (stamps
minted at signal time, consumed at execution time) — any extraction must carry that map explicitly.
