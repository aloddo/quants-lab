# H2 Dynamic Pair Screener — Plan V3 (Tiered, Edge-Based)

## Problem Statement

H2 spike-fade arb (Binance USDC spot vs Bybit perp) has a static 7-pair watchlist that went idle for 24h+ when spreads tightened. Only 12 of 198 monitored pairs cover the 31bp RT fee threshold, and the viable set shifts every 6 hours. Prior plan versions tried to solve this with static core sets and rotation — Codex's independent review showed that 3.8 days of data is too thin to declare anything "stable."

## Design Principles (from Codex adversarial + Alberto's framework)

1. **No pair gets permanent status.** Every pair earns its tier from rolling metrics.
2. **Decouple WS connection from tradeability.** Connect broadly, trade selectively.
3. **Expected-edge entry, not fixed percentile.** Enter only when spread - fees - slippage - buffer > 0.
4. **Event-aware throttling, not blunt cooldown.** One entry per spike cluster.
5. **Regime kill-switches.** Adaptive guardrails for a system built on 3.8 days of data.

## Architecture

```
arb_dual_collector (unchanged — REST, 5s, 198 pairs)
  └── arb_bn_usdc_bb_perp_snapshots (MongoDB)

arb_h2_live_v3.py
  ├── TierEngine (NEW — replaces static PAIRS list)
  │     ├── Recomputes tiers every 5 min from rolling WS spread data
  │     ├── Tier A: excess_1h > 35bp AND excess_6h > 31bp → full size, always tradable
  │     ├── Tier B: excess_6h > 31bp BUT excess_1h <= 35bp → half size, conditional
  │     │   Conditions: excess_1h rising AND event_rate_1h >= pair median
  │     ├── Tier C: excess_6h 25-31bp → watch only (WS connected, not tradable)
  │     ├── Below Tier C: not connected
  │     └── Tier transitions logged + Telegram notifications
  │
  ├── UniverseManager (NEW — WS connection lifecycle)
  │     ├── Maintains WS on top ~25-30 symbols by 6h excess (Tier A+B+C)
  │     ├── Recomputes WS universe every 4-6h (not hourly — reduce reconnect churn)
  │     ├── Hysteresis: pair must drop below threshold for 2 consecutive windows to demote
  │     ├── Bybit: dynamic subscribe/unsubscribe (native WS support)
  │     ├── Binance: hot-swap reconnect (new WS before closing old)
  │     │   Old connection serves kept pairs until new delivers first tick per pair
  │     │   Abort swap if new doesn't deliver within 30s
  │     │   REST fallback for exit prices during reconnect failure
  │     └── Max symbols: 30 (15% of 200 WS limit)
  │
  ├── EdgeBasedEntry (REPLACES fixed P90 gate in SignalEngine)
  │     ├── Per-pair adaptive quantile gate: q = 0.87-0.93 (not fixed P90 for all)
  │     │   Tier A: q = 0.87 (lower bar → more entries on proven pairs)
  │     │   Tier B: q = 0.90 (standard)
  │     │   Tier C: not tradable (entries blocked)
  │     ├── Expected net edge check:
  │     │   expected_edge = spread_bps - fee_bps - slippage_est_bps - safety_buffer_bps
  │     │   Enter only if expected_edge > 0
  │     │   fee_bps = 31 (fixed)
  │     │   slippage_est_bps = rolling avg slippage for this pair (or 5bp default)
  │     │   safety_buffer_bps = 3 (margin of safety)
  │     └── Exit logic unchanged (P25 revert, stop-loss, max hold)
  │
  ├── EventThrottler (REPLACES blunt cooldown)
  │     ├── Spike event = spread crosses quantile gate upward
  │     ├── Event cluster = consecutive ticks above gate within 60s
  │     ├── Max 1 entry per event cluster
  │     ├── Re-entry only if spread re-expands by +10bp after reverting below gate
  │     └── Replaces current cooldown_time per pair
  │
  ├── RegimeGuard (NEW — kill-switches)
  │     ├── CONDITION 1: #pairs with excess_6h > 31bp drops below 6 for 2h
  │     │   → Cut position size 50% across all tiers
  │     ├── CONDITION 2: Tier A median event rate drops >50% vs 3-day trailing baseline
  │     │   → Tighten all quantile gates by +0.03 (e.g., 0.87→0.90, 0.90→0.93)
  │     ├── CONDITION 3: Realized avg slippage worsens by >5bp vs model estimate
  │     │   → Pause Tier B entirely (only trade Tier A)
  │     ├── CONDITION 4: Total PnL for current day < -$2 (3-sigma loss at current sizing)
  │     │   → Pause all new entries for 1h, alert via Telegram
  │     └── All conditions auto-reset when metrics recover for 30 min
  │
  ├── PriceFeed (MODIFIED — dynamic add/remove)
  │     └── Same as V2 plan: Bybit dynamic sub, Binance hot-swap
  │
  ├── InventoryLedger (MODIFIED)
  │     ├── Position size: Tier A = POSITION_USD, Tier B = POSITION_USD * 0.5
  │     ├── Dormant inventory: hold on WS disconnection, max 5 dormant pairs
  │     ├── Liquidate dormant if balance < $2 (not worth spread cost)
  │     └── Re-use dormant inventory on reconnection
  │
  └── Everything else unchanged (OrderGateway, RiskManager, PositionStore)
```

## Threshold Derivation

All thresholds below are initial values derived from 3.8 days of collector data.
They are explicitly provisional and will be recalibrated after 2 weeks of V3 operation.

| Parameter | Value | Derivation | Recalibration |
|-----------|-------|-----------|---------------|
| Tier A excess_1h gate | 35bp | Top 5 "stable" pairs had excess 32-54bp on worst day. 35bp separates clear edge from marginal. | After 2 weeks: adjust to P75 of Tier A excess distribution |
| Tier A excess_6h gate | 31bp | RT fee threshold. Non-negotiable minimum. | Fixed (fee-derived) |
| Tier B excess_6h gate | 31bp | Same as A, but requires additional conditions for entry | Fixed |
| Tier C threshold | 25bp | Buffer below fee threshold. Catches pairs warming up. | After 2 weeks: check if any Tier C pair ever promoted to A |
| Tier A quantile | 0.87 | P87 = ~10% more entries vs P90. Proven pairs can run lower bar. | After 2 weeks: compare Tier A WR at q=0.87 vs q=0.90 |
| Tier B quantile | 0.90 | Current standard. Conditional pairs need higher bar. | After 2 weeks: compare Tier B WR |
| Safety buffer | 3bp | ~10% of excess on marginal pair (31bp). Prevents entries that are barely positive. | After 2 weeks: calibrate to observed slippage variance |
| Default slippage est | 5bp | Conservative estimate. Will be replaced by per-pair rolling avg. | After 10 trades per pair |
| Event cluster gap | 60s | From hot streak analysis: median streak 1min. 60s separates independent events. | After 2 weeks: histogram of inter-event gaps |
| Re-entry expansion | 10bp | ~25-30% of typical excess. Ensures meaningful re-expansion before re-entry. | After 2 weeks: optimize for WR on re-entries |
| Universe refresh interval | 4h | 6h turnover shows 2-5 pair changes. 4h catches shifts with margin. Hysteresis adds stability. | After 2 weeks: measure actual churn rate |
| Tier recomputation interval | 5min | Fast enough to catch intra-hour regime shifts. Cheap (in-memory WS data, no MongoDB). | Fixed |
| Regime guard: min viable pairs | 6 | Half of the 12 historically viable. Below this = structural market change. | After 1 month |
| Regime guard: event rate drop | 50% | Meaningful decay vs baseline, not noise. | After 2 weeks |
| Regime guard: slippage deterioration | 5bp | Doubles the safety buffer. Material execution quality change. | After 2 weeks |
| Daily loss pause | -$2 | ~3 sigma at $0.07/trade avg, 20 trades/day. | After 2 weeks: recalculate from actual P&L distribution |

## Implementation Phases

### Phase 1: TierEngine + EdgeBasedEntry (core logic, standalone testable)
**Files:** `app/services/arb/tier_engine.py`, modify `signal_engine.py`

- TierEngine: takes spread history dict, computes tiers per pair every 5min
- EdgeBasedEntry: replace fixed P90 check with per-tier quantile + expected edge
- EventThrottler: spike clustering + re-entry expansion gate
- Unit tests: verify tier transitions, edge calculation, throttle behavior
- **Standalone validation:** feed recorded spread data, verify tier assignments match manual analysis
- **Estimated: 3h**

### Phase 2: UniverseManager (WS lifecycle)
**Files:** `app/services/arb/universe_manager.py`, modify `price_feed.py`

- Dynamic Bybit subscribe/unsubscribe
- Binance hot-swap with overlap (old serves until new ready)
- Hysteresis: 2 consecutive windows below threshold before demotion
- Threshold seeding for new pairs from collector MongoDB
- **Estimated: 3h** (Binance hot-swap is the hard part)

### Phase 3: RegimeGuard
**File:** `app/services/arb/regime_guard.py`

- 4 kill-switch conditions with auto-reset
- Rolling baselines (3-day trailing for event rate, per-pair for slippage)
- Telegram alerts on activation/deactivation
- **Estimated: 1.5h**

### Phase 4: Integration (arb_h2_live_v3.py)
- Wire TierEngine, UniverseManager, RegimeGuard into main loop
- Replace static PAIRS with TierEngine.tradable_pairs()
- Position sizing from tier (full vs half)
- Dormant inventory tracking
- Telegram status report with tier breakdown
- **Estimated: 2-3h**

### Phase 5: Validation
1. **Backtest on recorded data:** Replay 3.8 days of collector snapshots through V3 logic offline. Compare signal count, tier assignments, edge distribution vs V2.
2. **Shadow mode:** Run V3 alongside V2, log signals but don't trade. Compare entry quality.
3. **Single-pair live:** V3 with 1 Tier A pair, verify full lifecycle.
4. **Small set live:** V3 with Tier A only (5 pairs), 24h.
5. **Full deployment:** All tiers active, replace V2.
6. **2-week recalibration:** Recompute all provisional thresholds from live data.
- **Estimated: 12h wall clock**

### Total: ~10h code + 12h validation

## What Changed from V2

| V2 | V3 | Why |
|----|-----|-----|
| Static core set (7 pairs) | Dynamic tiers (no pair gets permanent status) | 3.8 days too short for "stable" claims |
| Fixed P90 entry for all | Per-tier quantile (0.87/0.90) + expected edge | Higher entry quality, more entries on proven pairs |
| 25bp single screener gate | Two gates: structural 31bp + conditional warm-up | Prevents flooding low-quality pairs |
| Blunt cooldown (3600s) | Event-aware throttling (60s cluster + re-expansion) | Kills tick-spam, allows genuine re-entries |
| No regime awareness | 4 kill-switches with auto-reset | Critical with only 3.8 days of calibration data |
| Hourly rotation | 4-6h universe refresh + 5min tier recomputation | Separate connection (slow, expensive) from tradeability (fast, cheap) |
| Connection = tradeability | Decoupled: connect 25-30, trade Tier A+B only | Wider opportunity capture without execution risk |
| One position size | Tier A = full, Tier B = half | Scale exposure with confidence |

## Risk Analysis

| Risk | Mitigation |
|------|-----------|
| All thresholds wrong (3.8 days) | Every threshold has explicit 2-week recalibration plan. Regime guards limit damage. |
| Binance reconnect exit blackout | Hot-swap overlap + REST fallback (from V2 F1 fix) |
| Tier oscillation (pair bounces A↔B) | Hysteresis on universe changes (2 windows). Tier changes don't affect WS. |
| Expected edge calculation wrong | Safety buffer (3bp) + conservative default slippage (5bp). Updates with per-pair actuals. |
| Event throttler too aggressive | 60s gap is data-derived (median hot streak = 1min). Re-entry gate allows genuine re-expansion. |
| Regime guard false positive | 30-min recovery window prevents over-reaction. All conditions auto-reset. |
| asyncio.Lock is not RW-lock | Use asyncio.Lock for universe changes (rare, 4-6h). Main loop iterates snapshot of tradable set (no lock needed for reads). |
| MongoDB index mismatch | Fix collector indexes: add (symbol_bn, timestamp) compound index. One-time migration. |
| Spread rounding in collector | Increase to 4 decimal places. Affects screening accuracy near thresholds. |

## Realistic Expectations

- **Trades/day:** 3-8 (6-16x improvement over current 0.5/day)
- **Tier A pairs at any time:** 3-7 (varies with market regime)
- **Tier B pairs at any time:** 3-6
- **WS connected pairs:** 20-30
- **Revenue improvement:** Primarily from catching opportunities on pairs that V2 missed entirely (LSK, ACT, RPL, 1000CAT never traded before)
