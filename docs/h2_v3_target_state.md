# H2 V3 Target State — Implementation Plan

**Date**: 2026-04-23
**Author**: Alberto + Claude
**Status**: DRAFT — pending adversarial review (appended below)

---

## Executive Summary

H2 V3 is a spike-fade cross-venue arb (Binance USDC spot vs Bybit linear perp). After 19 closed trades at $10/side, the system produces ~$0.30 total PnL with 68% WR. Fee tracking was recently fixed. This plan defines the target state across Alberto's 7 quality dimensions, with concrete implementation steps, dependencies, and deploy order.

**All-in RT fee budget**: ~30bp today (BB 11bp taker + BN 19bp taker). Target: ~21bp (BB 4bp maker + BN 17bp maker/taker + slippage buffer). This unlocks 9bp of margin on every trade.

---

## 1. REPORTING — Accurate PnL, No Wrong Spreads

### Current State
- PnL computed from actual fill prices per leg (correct since exit_flow.py rewrite)
- Binance fees captured via `/myTrades` REST endpoint after fill (correct since fee tracking fix)
- Equity chart sometimes fails (render_equity_curve_live exception handling)
- `/h2live` report shows session trades filtered by V3_EPOCH
- No per-trade slippage tracking persisted
- No fill rate analytics (don't know % of entries that fill vs miss)
- Spread displayed in report uses `signal_spread_bps` (signal time) not `entry.actual_spread_bps` (fill time)

### Target State
- Every closed trade has: `entry_slippage_bps`, `exit_slippage_bps`, `bb_entry_was_maker`, `bn_entry_was_maker`, `fill_rate_bb`, `fill_rate_bn`
- Per-pair analytics: fill rate, avg slippage, avg hold time, PnL
- Report uses actual fill spread, not signal spread
- Equity chart never fails (graceful degradation)
- New `/h2stats` command: per-pair breakdown with fill analytics

### Implementation Steps

| # | Step | File | Complexity | Depends |
|---|------|------|-----------|---------|
| R1 | Persist `entry_slippage_bps` and `exit_slippage_bps` to position doc | `entry_flow.py` L349, `exit_flow.py` L397 | S | — |
| R2 | Track maker/taker per leg from Bybit WS fill (`isMaker` field) and Binance `/myTrades` (`isMaker` field) | `fill_detector.py` TrackedLeg (add `is_maker: bool`), `order_feed.py` FillEvent | S | — |
| R3 | Persist `is_maker` per leg to position doc | `entry_flow.py`, `exit_flow.py` | S | R2 |
| R4 | Add fill rate tracking: new MongoDB collection `arb_h2_fill_analytics` with per-entry/exit attempt outcome (FILLED/MISSED/PARTIAL per leg) | `entry_flow.py` L414, `exit_flow.py` L498 — insert doc on every attempt | M | — |
| R5 | Build per-pair analytics query: aggregate fill_analytics for fill rate, avg slippage, avg hold, PnL | `arb_h2_live_v3.py` new method `_build_stats_text()` | M | R4 |
| R6 | Add `/h2stats` Telegram command | `arb_h2_live_v3.py` L146 (tg_poll_commands), add handler | S | R5 |
| R7 | Fix equity chart: wrap each step in try/except, return None on any failure, log but don't crash | `arb_h2_live_v3.py` L153-205 — already has outer try/except but inner failures can still propagate | S | — |
| R8 | Report: use `entry.actual_spread_bps` instead of `signal_spread_bps` for the entry spread display | `arb_h2_live_v3.py` L1057 | S | — |

---

## 2. ENTRY — Fast, at the Peak of the Spread Spike

### Current State
- Both legs submitted concurrently via `asyncio.gather()` (`entry_flow.py` L167)
- Bybit uses IOC (always taker, 5.5bp/side) — `order_api.py` L66 hardcoded `"timeInForce": "IOC"`
- Binance uses GTC (maker=taker at 9.5bp, so order type irrelevant)
- 3-second fill timeout (`entry_flow.py` L54)
- Spread verified at execution time with 80% threshold (`signal_engine.py` L440)
- No latency tracking per venue (only total entry latency)

### Target State
- **Bybit: PostOnly orders** — 2bp maker vs 5.5bp taker = 3.5bp savings per side, 7bp RT
- Sequential submit: Bybit PostOnly first, Binance GTC only after Bybit fill confirmed
- Per-venue latency tracked and persisted
- Entry price improvement: Bybit PostOnly at `best_bid + 1 tick` (for Buy) or `best_ask - 1 tick` (for Sell) to maximize maker probability

### Implementation Steps

| # | Step | File | Complexity | Depends |
|---|------|------|-----------|---------|
| E1 | Add `PostOnly` timeInForce option to Bybit order API | `order_api.py` L66 — add `order_mode` parameter: `"taker"` (IOC) or `"maker"` (PostOnly). PostOnly = `timeInForce: "PostOnly"` | S | — |
| E2 | Change entry_flow to **sequential submit**: BB PostOnly first, wait for fill (1.5s), then BN GTC | `entry_flow.py` L147-167 — replace `asyncio.gather()` with sequential logic | L | E1 |
| E3 | Handle PostOnly rejection: if Bybit rejects (price crossed spread), cancel BN if already submitted, mark as BOTH_MISSED | `entry_flow.py` new path in execute() | M | E2 |
| E4 | PostOnly price calculation: for Buy, use `best_bid + tick_size` (join the bid, one tick better). For Sell, use `best_ask - tick_size` | `arb_h2_live_v3.py` L908-911, `instrument_rules.py` (need tick_size from InstrumentRules) | M | E1 |
| E5 | Per-venue latency: track `bb_submit_ms`, `bn_submit_ms`, `bb_fill_ms` in EntryResult and persist | `entry_flow.py` EntryResult + execute() | S | — |
| E6 | Make entry order mode configurable: `BYBIT_ENTRY_MODE = "PostOnly"` constant, fallback to IOC on 3 consecutive PostOnly rejections per pair | `entry_flow.py` or `arb_h2_live_v3.py` config section | M | E2, E3 |

**CRITICAL DEPENDENCY**: E2 changes the entry from concurrent to sequential. This increases entry latency from ~200ms to ~1700ms (Bybit fill wait + BN submit). The spread may narrow during this window. Mitigation: verify spread AFTER Bybit fill, before BN submit. If spread dropped below threshold, unwind Bybit (market) and abort.

---

## 3. EXIT — Small Slippage, Spread Reverting as Expected

### Current State
- Exit limit fill timeout: 3s (`exit_flow.py` L53)
- Unfilled legs escalate to market after cancel + recheck (`exit_flow.py` L274)
- BB exit limits miss ~2/3 of time (from audit finding)
- No exit price improvement strategy
- Market escalation adds uncontrolled slippage
- MAX_EXIT_ATTEMPTS = 5 before forced market (`exit_flow.py` L56)

### Target State
- **Bybit exit: PostOnly** (same 3.5bp savings as entry)
- Smarter escalation: try PostOnly 2x, then limit IOC, then market
- Exit spread verified before submitting
- Track per-exit attempt outcome for analytics

### Implementation Steps

| # | Step | File | Complexity | Depends |
|---|------|------|-----------|---------|
| X1 | Bybit exit PostOnly: first attempt uses PostOnly, subsequent attempts use IOC, final attempt uses market | `exit_flow.py` L93 — add `attempt_order_type_for_bybit()` method | M | E1 |
| X2 | Exit price improvement: Bybit PostOnly at `best_ask - tick` (for Sell exit) to sit on book | `arb_h2_live_v3.py` L1026-1029 | S | E4 |
| X3 | Track exit attempt outcomes in fill_analytics collection (same as R4) | `exit_flow.py` — insert doc after each execute() | S | R4 |
| X4 | Extend exit timeout from 3s to 5s for PostOnly (needs more time to rest on book) | `exit_flow.py` L53 — make configurable per order_type | S | X1 |
| X5 | Smarter escalation ladder: PostOnly (5s) -> IOC (3s) -> Market (2s) with 3 total attempts before forced market | `exit_flow.py` execute() flow | M | X1, X4 |

---

## 4. FEES OPTIMIZED — Lowest Fees While Ensuring Fills

### Current State
- Bybit: IOC on all orders = taker 5.5bp/side = 11bp RT on Bybit alone
- Binance: GTC, maker=taker = 9.5bp/side = 19bp RT on Binance
- All-in RT: ~30bp
- No maker/taker awareness in reporting

### Target State
- Bybit: PostOnly = 2bp/side maker = 4bp RT on Bybit
- Binance: still 9.5bp/side (maker=taker, no optimization possible)
- All-in RT: ~23bp (4bp BB + 19bp BN)
- Saving: ~7bp per round trip
- At $10/side: $0.014/trade saved. At $100/side: $0.14/trade. At $1000/side: $1.40/trade.

### Implementation Steps

| # | Step | File | Complexity | Depends |
|---|------|------|-----------|---------|
| F1 | Update FEE_RT_BPS constant from 31 to 23 after PostOnly deployment confirmed working | `arb_h2_live_v3.py` L76, `signal_engine.py` L34 | S | E2 deployed + validated |
| F2 | Track actual vs expected fees per trade: `expected_fee_bps` vs `actual_fee_bps` | `exit_flow.py` PnL computation section | S | R2 (is_maker tracking) |
| F3 | Alert when actual fees exceed expected by >20% (indicates PostOnly fallback to taker) | `risk_manager.py` — new alert criterion | S | F2 |
| F4 | Binance VIP level check: at scale, maker fees drop. Log current fee tier on startup | `order_api.py` BinanceOrderAPI — query `/api/v3/account` for `makerCommission` | S | — |

---

## 5. SLIPPAGE MINIMIZED

### Current State
- Entry slippage: signal spread - actual fill spread (computed but only in `entry.slippage_bps`, not systematically analyzed)
- Exit slippage: not tracked separately
- No per-venue slippage breakdown
- Market escalation on exits adds uncontrolled slippage (biggest contributor)
- No systematic data: "we know avg ~2-5bp but don't track per-trade or per-pair"

### Target State
- Per-trade, per-venue, per-side slippage persisted to MongoDB
- Dashboard: avg entry slip, avg exit slip, avg total slip, per-pair breakdown
- Market escalation slippage isolated: track `escalation_slippage_bps` separately
- Pair-level slip alerts: if a pair's avg slip > 5bp over 10 trades, log warning

### Implementation Steps

| # | Step | File | Complexity | Depends |
|---|------|------|-----------|---------|
| S1 | Compute and persist per-leg slippage: `target_price - fill_price` for each leg, normalized to bps | `entry_flow.py` SUCCESS path L330-363, `exit_flow.py` SUCCESS path L394-445 | M | — |
| S2 | Separate escalation slippage: when market escalation fires, record `escalation_slippage_bps = market_fill_price - original_limit_price` | `exit_flow.py` L274-330 market escalation section | M | — |
| S3 | Per-pair rolling slippage tracker in SignalEngine or new AnalyticsEngine | New file or section in `arb_h2_live_v3.py` | M | S1 |
| S4 | Slippage alert: if rolling avg slip > 5bp for a pair over 10 trades, log + TG warning | `arb_h2_live_v3.py` main loop, every 5 min check | S | S3 |
| S5 | Include slippage in `/h2stats` report | `arb_h2_live_v3.py` `_build_stats_text()` | S | S3, R5 |

---

## 6. 10bp MARGIN OF SAFETY OVER FEES

### Current State
- MIN_EXCESS_BPS = 30bp (`signal_engine.py` L33) — this is the P90-P25 excess gate, NOT the margin over fees
- FEE_RT_BPS = 31bp (`signal_engine.py` L34, `arb_h2_live_v3.py` L76)
- Tier A excess > 35bp with 30bp fees = 5bp margin
- Tier B excess 31-35bp = 0-5bp margin
- No explicit "margin over fees" gate — viability only checks `excess > MIN_EXCESS_BPS`

### Target State
- After PostOnly: FEE_RT_BPS drops to ~23bp
- Tier A: excess > 35bp, fees 23bp = 12bp margin (target met)
- Tier B: excess 31-35bp, fees 23bp = 8-12bp margin (close to target)
- New explicit gate: `excess - FEE_RT_BPS >= 10` (hard gate, not just viable check)
- Tier thresholds adjusted: Tier B minimum moves to FEE_RT_BPS + 10bp = 33bp

### Implementation Steps

| # | Step | File | Complexity | Depends |
|---|------|------|-----------|---------|
| M1 | Add explicit margin gate to SignalEngine: `excess - fee_rt_bps >= margin_target` | `signal_engine.py` check_entry() around L313 | S | F1 |
| M2 | Pass FEE_RT_BPS to SignalEngine (currently hardcoded in two places) | `signal_engine.py` __init__(), `arb_h2_live_v3.py` constructor | S | — |
| M3 | Update Tier B threshold: `TIER_B_EXCESS = max(31, FEE_RT_BPS + 10)` so it auto-adjusts when fees change | `tier_engine.py` L28 | S | M2 |
| M4 | Add margin tracking to report: show `excess - fees` per pair, color-code <10bp as warning | `arb_h2_live_v3.py` `_build_report_text()` | S | M1 |

---

## 7. INVENTORY MANAGEMENT — Robust and Systematic

### Current State
- InventoryLedger tracks expected_qty, locked_qty, cost_basis per pair in MongoDB
- Reconciliation every 5 min against Binance REST balance
- Auto-heal when no position open: snap expected_qty to exchange balance
- Auto-seed on startup: buy tokens for pairs with insufficient inventory
- Cost basis drift detection and fix on startup
- SYMBOL_TO_BASE mapping is hardcoded for 8 pairs (`inventory_ledger.py` L58-67) — breaks when TierEngine adds new pairs

### Target State
- SYMBOL_TO_BASE auto-derived from symbol string (no hardcoded map needed for USDT/USDC pairs)
- Reconciliation tracks drift over time (not just point-in-time)
- Inventory health dashboard in report
- Stale lock detection improved: tag locks with position_id, release when position reaches terminal state
- Cost basis tracks sell proceeds, not just purchase cost (for true realized PnL)

### Implementation Steps

| # | Step | File | Complexity | Depends |
|---|------|------|-----------|---------|
| I1 | Auto-derive base_asset: `symbol.replace("USDT", "").replace("USDC", "")` — remove hardcoded SYMBOL_TO_BASE | `inventory_ledger.py` L58-67, L95 | S | — |
| I2 | Tag locks with position_id so stale locks can be identified and released | `inventory_ledger.py` lock() — add `position_id` param, store in `locked_for: dict[str, float]` | M | — |
| I3 | Add sell proceeds tracking: `sold()` records `sell_proceeds_usd += qty * fill_price` for true realized PnL | `inventory_ledger.py` sold() L180 | S | — |
| I4 | Reconciliation drift history: persist each reconcile result with timestamp to `arb_h2_inventory_history` for trend analysis | `inventory_ledger.py` reconcile() L221 | M | — |
| I5 | Include inventory health in `/h2live` report: per-pair health, drift alerts, cost basis | Already partially done in `_build_report_text()` L1219-1254 — just need to add drift history | S | I4 |

---

## Deploy Order (Phased)

### Phase 1: Reporting & Analytics Foundation (No behavior change)
**Items**: R1, R2, R3, R4, R7, R8, S1, S2, I1
**Risk**: Zero — only adds data capture and fixes display bugs
**Duration**: 1 session

### Phase 2: PostOnly Entry (Core fee optimization)
**Items**: E1, E2, E3, E4, E5, E6
**Risk**: Medium — changes entry mechanics. PostOnly rejection = missed trades. Sequential submit increases latency.
**Mitigation**: Deploy with `BYBIT_ENTRY_MODE = "PostOnly"` as config flag. Can revert to IOC instantly.
**Duration**: 1 session
**Validation**: Run 20 trades, check PostOnly fill rate. Must be >60% to keep. If <60%, revert to IOC.

### Phase 3: PostOnly Exit + Fee Update
**Items**: X1, X2, X3, X4, X5, F1, F2, F3, F4
**Risk**: Medium — exit PostOnly rejection means escalation. But escalation already works.
**Mitigation**: PostOnly only on first exit attempt; IOC/market fallback unchanged.
**Duration**: 1 session
**Validation**: Exit fill rate must not degrade vs current. Track market escalation rate.

### Phase 4: Margin & Threshold Tightening
**Items**: M1, M2, M3, M4
**Risk**: Low — may reduce trade frequency by filtering marginal trades.
**Mitigation**: Monitor trade count. If drops >50%, relax margin requirement.
**Duration**: 0.5 session

### Phase 5: Analytics, Inventory, Polish
**Items**: R5, R6, S3, S4, S5, I2, I3, I4, I5
**Risk**: Low — analytics and inventory improvements, no trading logic changes.
**Duration**: 1 session

### Phase 6: Scale Up
**Items**: Increase POSITION_USD from $10 to $100, then $1000.
**Prerequisite**: Phase 1-5 deployed + 50 trades at $10 with PostOnly + positive expectancy confirmed.
**Risk**: Higher — larger positions face worse fills on thin USDC books. PostOnly fill rate may degrade.
**Mitigation**: Scale in steps: $10 -> $50 -> $100 -> $500 -> $1000. Each step needs 20 trades.

---

## Summary: Expected Impact

| Metric | Current | Target | Delta |
|--------|---------|--------|-------|
| All-in RT fees | ~30bp | ~23bp | -7bp |
| Margin over fees (Tier A) | ~5bp | ~12bp | +7bp |
| Margin over fees (Tier B) | ~0-5bp | ~8-12bp | +8bp |
| Per-trade PnL at $10/side | ~$0.01 | ~$0.024 | +140% |
| Per-trade PnL at $100/side | ~$0.10 | ~$0.24 | +140% |
| Per-trade PnL at $1000/side | ~$1.00 | ~$2.40 | +140% |
| Trade analytics | None | Full breakdown | N/A |
| Slippage tracking | Ad-hoc | Systematic | N/A |

---

---

# ADVERSARIAL REVIEW

Self-review of the plan above. Looking for flaws, risks, edge cases, and failure modes.

---

## Finding 1: PostOnly Sequential Entry Creates a Timing Attack Surface
**Severity**: CRITICAL

The plan proposes sequential submit (Bybit PostOnly first, wait for fill, then Binance GTC). This creates a window of 1-2 seconds where Bybit is filled but Binance is not yet submitted. During this window:

- The spread can narrow or reverse entirely
- Binance price may move against us
- We have a naked Bybit position with no hedge

**Current concurrent submit**: both legs are in flight within ~50ms of each other. Sequential makes this 1500-2000ms.

**Mitigation**: Step E2 must include a spread re-verification AFTER Bybit fill and BEFORE Binance submit. If spread has narrowed below `threshold * 0.7`, unwind Bybit immediately via market order. This adds an unwind path to the entry flow that doesn't exist today for the sequential case.

**But**: The unwind itself has slippage. If PostOnly entry is at 2bp maker, and the unwind is market at 5.5bp taker, each aborted entry costs ~7.5bp * $10 = $0.015. If PostOnly fill rate is 70% and spread verification rejects 30% of those, you're paying $0.015 * 0.3 * 0.7 = $0.003 per entry attempt on wasted unwinds. At $1000/side this is $0.30 per wasted attempt.

**Recommendation**: Add unwind cost budget to risk manager. Track total unwind costs from sequential entry aborts.

---

## Finding 2: PostOnly Fill Rate Unknown — No Baseline Data
**Severity**: HIGH

The plan assumes PostOnly will fill frequently enough to justify the complexity. But we have zero empirical data on:
- What % of Bybit PostOnly orders fill within 1.5s on these pairs
- Whether Bybit liquidity at `best_bid + tick` is sufficient for our size
- Whether PostOnly rejection rate varies by pair, time of day, or volatility regime

**Mitigation**: Before deploying E2 (sequential), deploy E1 alone and add a shadow mode that logs what WOULD have happened with PostOnly. Run for 48 hours, collect data, then decide.

**Concrete step**: Add a `_shadow_postonly_check()` method that, on every real IOC entry, also queries what the PostOnly price would have been and whether the fill would have occurred within 1.5s based on price movement.

---

## Finding 3: Exit PostOnly Has Higher Risk Than Entry PostOnly
**Severity**: HIGH

On entry, a PostOnly rejection just means a missed trade (no harm). On exit, a PostOnly rejection means we're still holding a position that needs to close. The current exit flow already has a robust escalation ladder (limit -> cancel -> recheck -> market), but PostOnly adds a new failure mode:

- PostOnly rejected immediately (price crossed) — 0ms wasted
- PostOnly accepted but never fills in 5s — 5s wasted before escalation
- Multiple PostOnly rejections in succession — exit delay accumulates

At 5s per PostOnly attempt + 3s per IOC attempt + 2s per market attempt, worst case is 10s of exit delay. During a spike-fade, 10s of delay can mean the spread has fully reverted and then reversed again.

**Mitigation**: X5 proposes a ladder, but the plan doesn't specify a total exit time budget. Add `MAX_EXIT_TOTAL_SECONDS = 15` — if total time across all attempts exceeds 15s, force immediate market on all remaining legs.

---

## Finding 4: FEE_RT_BPS Is Duplicated in Two Files
**Severity**: MEDIUM

`FEE_RT_BPS` is defined in both `arb_h2_live_v3.py` L76 (31.0) and `signal_engine.py` L34 (31.0). The plan (M2) says to centralize but doesn't specify how. If one gets updated and the other doesn't:
- Signal engine uses old fee estimate for viability check
- Trader uses new fee estimate for PnL computation
- Trades may be entered that the signal engine correctly rejected, or vice versa

**Mitigation**: M2 must pass FEE_RT_BPS from the trader config to SignalEngine constructor. Signal engine should NOT have its own constant. Single source of truth.

---

## Finding 5: SYMBOL_TO_BASE Removal May Break Edge Cases
**Severity**: MEDIUM

Step I1 proposes auto-deriving base_asset via `symbol.replace("USDT", "")`. This works for `HIGHUSDT -> HIGH`, `ETHUSDT -> ETH`. But consider:
- `1000PEPEUSDT` -> `1000PEPE` (correct, but Binance balances show `1000PEPE` as asset)
- `BTCDOMUSDT` -> `BTCDOM` (correct)
- A symbol containing "USDT" in the base name (unlikely but not impossible)

The current hardcoded map has 8 entries for 8 pairs. TierEngine dynamically adds pairs, so the map is already stale. The auto-derive is strictly better than the hardcoded map because it handles new pairs.

**Mitigation**: Add unit test for edge cases. Log a warning if the derived base_asset looks unusual (length < 2 or > 10).

---

## Finding 6: PostOnly on Bybit Requires Position Mode Check
**Severity**: MEDIUM

Bybit PostOnly orders interact with position mode. In one-way mode (which H2 uses for perps), PostOnly should work fine. But if position mode ever gets changed to hedge mode, PostOnly behavior may differ.

Additionally, Bybit has a `reduceOnly` flag that interacts with PostOnly. For exit orders, we may need `reduceOnly=True` to ensure we're closing, not opening a new position. The current code doesn't use `reduceOnly`.

**Mitigation**: Add `reduceOnly=True` to all exit orders on Bybit. This is a good practice regardless of PostOnly. Add to E1 implementation.

---

## Finding 7: Scale-Up Phase Lacks Liquidity Analysis
**Severity**: HIGH

Phase 6 proposes scaling from $10 to $1000 per side. But USDC spot books on Binance are THIN for altcoins. The tier engine selects pairs based on spread excess, not liquidity depth.

At $1000/side, we need to fill 1000/price units. For a $0.50 altcoin, that's 2000 units. If the USDC order book has 500 units at best bid/ask, our order will sweep through multiple levels, adding significant slippage that the PostOnly maker strategy explicitly tries to avoid.

**Mitigation**: Before scaling, add a `liquidity_depth_usd` field to TierEngine that queries Binance order book depth. Only scale pairs where `liquidity_depth_usd > 5x position_size` at best bid/ask level.

**Concrete step**: Add `GET /api/v3/depth?symbol=X&limit=5` to BinanceOrderAPI. Query during tier recompute. Persist to TierInfo.

---

## Finding 8: Concurrent Position Risk During Sequential Entry
**Severity**: MEDIUM

With sequential entry (E2), the Bybit leg fills first. For 1-2 seconds, we have a naked perp position. If the system crashes during this window:
- Crash recovery will find an ENTERING position with one FILLED leg
- It should unwind the Bybit leg (existing logic in crash_recovery_v2.py)
- But if recovery is also sequential, and the unwind takes time...

The existing crash recovery handles ENTERING state with one filled leg by unwinding. This is correct. But the plan should explicitly verify that crash recovery handles the new sequential flow.

**Mitigation**: Add integration test: simulate crash after Bybit PostOnly fill, before Binance submit. Verify recovery unwinds correctly.

---

## Finding 9: No Rollback Plan for PostOnly
**Severity**: MEDIUM

The plan says "Can revert to IOC instantly" (Phase 2 mitigation). But what does "instantly" mean?
- Change `BYBIT_ENTRY_MODE` from "PostOnly" to "IOC" in config
- Restart the trader

This requires a restart, which means:
- Open positions stay open during restart
- Crash recovery runs
- 3-5 second downtime

Better: make the mode switchable at runtime via a Telegram command.

**Mitigation**: Add `/h2mode postonly` and `/h2mode ioc` Telegram commands that flip the mode without restart. Requires storing mode in an instance variable that `_handle_entry` checks, not a module-level constant.

---

## Finding 10: Tier B Margin May Be Insufficient Even After PostOnly
**Severity**: MEDIUM

With PostOnly, FEE_RT_BPS drops to ~23bp. Tier B excess is 31-35bp. Margin = 8-12bp.

But the 23bp fee estimate assumes 100% maker fills on Bybit. In reality:
- Some PostOnly orders will be rejected and fall back to IOC (5.5bp taker)
- Market escalation on exits adds more taker fees
- Slippage adds 2-5bp on average

Realistic all-in RT: ~26-28bp (blended maker/taker + slippage). Tier B margin drops to 3-7bp.

**Mitigation**: M3 should use a conservative `EFFECTIVE_FEE_RT_BPS = FEE_RT_BPS + 3` (slippage buffer) for tier gate calculations. This means the actual threshold check is `excess - (fee + buffer) >= margin_target`.

---

## Finding 11: Report V3_EPOCH Filter Creates Data Discontinuity
**Severity**: LOW

The report filters by `V3_EPOCH` (Apr 23 2026 10:30 UTC). When V3 has been running for weeks, the "all-time" counter in the report includes V2 trades but the "session" counter only shows V3. This is confusing but not wrong.

When the trader restarts, "session" resets but V3_EPOCH stays the same, so post-restart trades still appear in "this session" bucket. This is actually correct behavior.

**Mitigation**: None needed. Behavior is correct.

---

## Finding 12: Auto-Seed Buys at Startup Don't Use PostOnly
**Severity**: LOW

The auto-seed logic (`arb_h2_live_v3.py` L640-672) buys inventory at `ask + 0.1%` using a limit order. This is always a taker fill. At scale ($100+/pair for 20 pairs = $2000 USDC), the taker fees on seeding add up.

**Mitigation**: Low priority. Seeding is a one-time cost per pair. But if implementing E1 (PostOnly for Bybit), also add a `"GTC"` option for Binance spot that posts on the book at mid-price and waits 10s for fill. If unfilled, escalate to taker.

---

## Finding 13: Hardcoded `POSITION_USD = 10.0` Used in Multiple Places
**Severity**: LOW

`POSITION_USD` is used for:
1. Entry sizing (`arb_h2_live_v3.py` L879)
2. Exit PnL computation as denominator (`exit_flow.py` L83 parameter)
3. uPnL estimation in report (`arb_h2_live_v3.py` L1188)
4. USDC reservation for exit buy-backs (L576)
5. Auto-seed calculation (L613)

At scale, if POSITION_USD is changed without restarting, existing open positions would have incorrect PnL denominators. But since it's a module constant, it can't change without restart.

**Mitigation**: When scaling, track `position_usd` per position in the MongoDB doc (already done: `entry_flow.py` could persist it). Use the per-position value for PnL, not the global constant.

---

## Finding 14: PostOnly + Thin USDC Books = Queue Priority Risk
**Severity**: MEDIUM

PostOnly orders on thin USDC books face queue priority issues. If our PostOnly buy is at `best_bid + tick`:
- We're joining the queue at `best_bid + tick`
- Other market participants may be ahead of us
- Our order may never get hit if the spread doesn't move down to our level

On thick USDT books this is fine (continuous flow). On thin USDC altcoin books, the spread may not tick through our level during the 1.5s wait window.

**Mitigation**: The PostOnly fill rate validation in Phase 2 (20 trades, must be >60%) will catch this. But 60% may be too low — at 60% fill rate, 40% of entries are wasted attempts, which reduces throughput significantly.

**Recommendation**: Set PostOnly fill rate threshold at 70%. Below 70%, the throughput loss exceeds the fee savings.

---

## Summary of Findings

| # | Finding | Severity | Status |
|---|---------|----------|--------|
| 1 | Sequential entry timing attack surface | CRITICAL | Must add spread re-verification + unwind budget |
| 2 | No PostOnly fill rate baseline data | HIGH | Add shadow mode before deploying |
| 3 | Exit PostOnly higher risk than entry | HIGH | Add MAX_EXIT_TOTAL_SECONDS budget |
| 4 | FEE_RT_BPS duplicated | MEDIUM | Must centralize in M2 |
| 5 | SYMBOL_TO_BASE edge cases | MEDIUM | Add unit test |
| 6 | PostOnly + position mode interaction | MEDIUM | Add reduceOnly to exit orders |
| 7 | Scale-up lacks liquidity analysis | HIGH | Add order book depth to TierEngine |
| 8 | Crash during sequential entry | MEDIUM | Verify crash recovery handles new flow |
| 9 | No runtime rollback for PostOnly | MEDIUM | Add /h2mode Telegram command |
| 10 | Tier B margin still thin after PostOnly | MEDIUM | Use EFFECTIVE_FEE with slippage buffer |
| 11 | V3_EPOCH filter discontinuity | LOW | No action needed |
| 12 | Auto-seed doesn't use PostOnly | LOW | Low priority optimization |
| 13 | POSITION_USD hardcoded in multiple places | LOW | Persist per-position in MongoDB |
| 14 | PostOnly queue priority on thin books | MEDIUM | Set fill rate threshold at 70% |

**Gate assessment**: Plan is **CONDITIONAL PASS**. The CRITICAL finding (#1) and HIGH findings (#2, #3, #7) must have mitigations implemented before deployment. Specifically:
1. Sequential entry MUST include spread re-verification between legs (Finding 1)
2. PostOnly shadow mode MUST run for 48h before live deployment (Finding 2)
3. Exit time budget MUST be enforced (Finding 3)
4. Liquidity depth MUST be checked before scaling above $100/side (Finding 7)
