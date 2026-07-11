# H2 Dynamic Pair Screener — Plan V2 (Data-Driven)

## Problem Statement

H2 spike-fade arb has a static 7-pair watchlist. When spreads tighten (Apr 21-23: zero signals in 24h+), the bot goes idle. The data shows 12 viable USDC pairs exist, but the viable set shifts every 6 hours.

## Data-Driven Findings

### Only 12 pairs cover fees (excess > 31bp RT)
```
Pair         3-day Excess   Stable?
KERNEL       63.7bp         NO  (drops to 15-24bp on Apr 21-22)
ENJ          60.2bp         NO  (drops to 19-21bp on Apr 21-22)
LSK          50.5bp         YES (43-54bp all days)
BLUR         46.6bp         NO  (drops to 13-19bp on Apr 21-22)
ACT          43.5bp         YES (41-44bp all days)
NOM          41.9bp         NO  (drops to 14-18bp on Apr 21-22)
1000CAT      36.2bp         YES (36-37bp all days)
ONT          36.2bp         NO  (drops to 17-30bp)
QTUM         35.7bp         NO  (drops to 11-29bp)
RARE         32.6bp         NO  (drops to 19-22bp)
DYM          32.5bp         YES (32-33bp all days)
RPL          31.2bp         YES (31-38bp all days)
```

### Core set: 7 pairs (LSK, ACT, 1000CAT, DYM, RPL, KERNEL, ENJ)
LSK/ACT/1000CAT/DYM/RPL maintain excess > 31bp on ALL days. KERNEL/ENJ are top-2 by excess when active and the P90/P25 engine auto-blocks entries when they go quiet (viable=false). All 7 are "always on" — never disconnected from WS.

### Rotation set: 7 slots that shift every 6 hours
The 6-hourly top-12 turnover is 2-5 pairs per window. This is the natural rotation rhythm.

### Signal density: ~1900 P90 spikes/day across 12 pairs
Even conservative filtering yields 50-100 tradeable signals/day vs current 0.5/day.

### Hot streaks are short (median 1min), so WS is required
Can't chase spikes with REST polling. Must have WS connected BEFORE the spike.

## Revised Architecture

### Key Insight: Wide static set + hourly refresh, not real-time chase

Since hot streaks are ephemeral (1-3 min) but the VIABLE SET is stable over hours, the right approach is:
1. Watch ALL potentially viable pairs on WS (the top ~20-25 by rolling excess)
2. Refresh which pairs are in the WS set every 1-6 hours (not every 5 minutes)
3. Let the existing P90/P25 adaptive threshold engine handle entry timing within the active set

```
arb_dual_collector (unchanged — REST, 5s, 198 pairs)
  └── arb_bn_usdc_bb_perp_snapshots (MongoDB)

arb_h2_live_v3.py
  ├── PairScreener (NEW — hourly universe refresh)
  │     ├── Reads last 6h of collector snapshots from MongoDB
  │     ├── Computes per-pair: P90, P25, excess, avg_spread
  │     ├── Filters: excess > 25bp (below 31bp fee threshold but close enough
  │     │   to catch pairs warming up — the P90/P25 entry gate blocks bad entries)
  │     ├── Ranks by excess, returns top-K (default K=20)
  │     └── Runs every ROTATION_INTERVAL (default: 1 hour)
  │
  ├── PairRotator (NEW — manages WS transitions)
  │     ├── Diffs current active set vs screener output
  │     ├── PROMOTE: add new pairs → Bybit WS subscribe (dynamic), Binance WS
  │     │   reconnect (batched, hot-swap), seed thresholds from collector MongoDB
  │     ├── DEMOTE: remove pairs → only if no open position, mark inventory dormant
  │     ├── PROTECT: never demote the 7 core pairs (LSK,ACT,1000CAT,DYM,RPL,KERNEL,ENJ)
  │     ├── Max churn: 5 changes per rotation (total adds + removes)
  │     └── Cooldown: 2h before re-adding a demoted pair
  │
  ├── PriceFeed (MODIFIED)
  │     ├── Bybit: dynamic subscribe/unsubscribe on existing WS (native support)
  │     ├── Binance: hot-swap reconnect (start new WS, then close old)
  │     │   Batch all pending changes, reconnect at most once per rotation cycle
  │     │   During overlap: both connections live, use new one's data
  │     │   Fallback: REST price poll for exit decisions if reconnect fails
  │     └── Max symbols: 30 (15% of 200 WS limit)
  │
  ├── SignalEngine (MINOR)
  │     ├── seed_from_collector(symbol): load 720 snapshots from MongoDB,
  │     │   apply 0.85x P90 scaling (REST-to-WS distribution correction),
  │     │   mark pair as "warming" until 360 WS ticks received
  │     ├── Warming pairs: thresholds computed but entries BLOCKED
  │     └── cleanup_pair(symbol): remove history on demotion
  │
  ├── InventoryLedger (MINOR)
  │     ├── Dormant inventory: track pairs that were demoted with inventory
  │     ├── Re-activate on promotion: use existing inventory, skip buy
  │     ├── Hard cap: if total dormant > $50, liquidate oldest position
  │     └── Report dormant inventory in Telegram status
  │
  └── Everything else unchanged
```

## Thresholds (data-derived)

| Parameter | Value | Derivation |
|-----------|-------|-----------|
| Screener excess threshold | 25bp | 12 pairs at 31bp, buffer of 6bp catches warming pairs |
| Top-K active pairs | 20 | 12 viable + 8 near-viable for coverage. 10% WS utilization. |
| Rotation interval | 1 hour | 6h turnover of 2-5 pairs → 1h granularity catches shifts within 1 window |
| Max churn per rotation | 5 | Matches observed 6h turnover rate (2-5 pairs). At 1h, ~1-2 changes typical |
| Core protected set | 5 pairs | LSK, ACT, 1000CAT, DYM, RPL — viable all 3 days |
| Threshold warm-up ticks | 360 | Half of window=720. ~6min at WS rate. |
| REST-to-WS P90 scaling | 1.15x | WS catches more extreme ticks than REST. Inflate to be conservative (F8 fix). Calibrate in Phase 5. |
| Binance WS reconnect debounce | rotation interval | Batch all changes, one reconnect per hour max |
| Dormant inventory cap | $50 | ~2.5 positions worth. Prevents capital lock-up. |
| Demotion cooldown | 2h | Prevents thrashing on borderline pairs |

## Implementation Phases

### Phase 1: PairScreener (standalone, read-only)
**File:** `app/services/arb/pair_screener.py`
- MongoDB aggregation pipeline: group by symbol_bn, compute avg, percentiles over last 6h
- Filter excess > 25bp, rank by excess, return top-K
- Standalone validation: run hourly, compare output vs manual analysis
- **Estimated: 1h**

### Phase 2: Dynamic PriceFeed
**File:** Modify `app/services/arb/price_feed.py`
- Bybit: `add_symbol()` / `remove_symbol()` — send subscribe/unsubscribe on existing WS
- Binance: `hot_swap_reconnect(new_symbols)` — connect new WS, wait for first data, close old
- Add rotation lock (asyncio.Lock) — main loop acquires read, rotation acquires write
- REST fallback for exit prices during reconnect failure
- **Estimated: 2-3h** (Binance hot-swap is the hard part)

### Phase 3: PairRotator + Integration
**File:** `app/services/arb/pair_rotator.py` + modifications to V3 trader
- Diff current vs target set, apply churn limit, protect core + open positions
- Threshold seeding from collector MongoDB with scaling
- Dormant inventory tracking
- Telegram notifications for rotations
- **Estimated: 2h**

### Phase 4: arb_h2_live_v3.py
- Copy V2, replace static PAIRS with screener init
- Background task: rotation_cycle every ROTATION_INTERVAL
- Wire up PairRotator to PriceFeed + SignalEngine + InventoryLedger
- **Estimated: 1-2h**

### Phase 5: Validation
1. **Calibrate REST-to-WS scaling**: run WS + REST simultaneously on LSK, ACT, RPL for 1h. Measure P90 ratio.
2. **Shadow mode**: run V3 alongside V2, log signals only (no trading), compare.
3. **Single pair live test**: V3 with 1 pair, verify hot-swap reconnect works.
4. **Small set**: V3 with top-5, verify rotation over 24h.
5. **Full deployment**: top-20 pairs, replace V2.
- **Estimated: 8h wall clock**

### Total: ~7h code + 8h validation

## Risk Analysis (post-adversarial fixes)

| Risk | Mitigation | Status |
|------|-----------|--------|
| Binance reconnect exit blackout | Hot-swap + REST fallback | Fixed (F1) |
| Inventory loss on demotion | Dormant inventory, cap at $50 | Fixed (F2) |
| REST vs WS P90 mismatch | 0.85x scaling + warm-up block | Fixed (F3), needs calibration |
| Race rotation vs entry flow | asyncio.Lock (read/write) | Fixed (F4) |
| MongoDB query load | Aggregation pipeline, 1/hour | Fixed (F5) |
| Churn budget math | Single shared budget, total <= max_churn | Fixed (F6) |
| Core pair goes non-viable | Protected set only blocks demotion, not entries. If excess drops, P90 threshold naturally rises, entries stop. No risk. | N/A |
| New pair has no inventory | Buy on first promotion. Position sizing handles this. | N/A |

## Adversarial Review V2 Findings & Fixes

### F7: 20 trades/day target is unrealistic -- ACCEPTED
The 1900 raw P90 spikes get decimated by excess check, direction filter, verify-at-execution, max concurrent, cooldown. Only 5-7 pairs are viable at any given moment (not all 12). Realistic target: **3-8 trades/day** (6-16x improvement over current 0.5/day). Still a major win. Updated success criteria.

### F8: 0.85x P90 scaling is backwards -- FIXED
WS P90 > REST P90 (WS catches instantaneous spikes that REST averages away). Correct scaling should be **1.15x** (inflate REST P90 to approximate WS P90). Seeded thresholds start CONSERVATIVE (higher bar for entry), then warm up to true WS levels. This is fail-safe: too-high thresholds = missed entries (safe), too-low = premature entries (unsafe).

### F9: Binance hot-swap drops all pairs during transition -- FIXED
During overlap: OLD connection continues serving ALL pairs (including kept ones). NEW connection starts. Once NEW has received at least one tick per kept pair, switch the data source atomically. Old connection closes after switch. If NEW fails to deliver within 30s, abort swap and keep old connection. No freshness gap.

### F10: Core protected set should include KERNEL and ENJ -- FIXED
Updated core set to 7: **LSK, ACT, 1000CAT, DYM, RPL, KERNEL, ENJ**. KERNEL and ENJ are top-2 by excess when active. When quiet, `viable=false` blocks entries automatically. Zero cost to keeping WS connected. Reduces reconnect churn on best pairs.

### F11: Dormant inventory cap is wrong unit -- FIXED
Replace dollar cap with pair count cap: **max 5 dormant pairs**. If a 6th pair would go dormant, liquidate the one with smallest balance first (minimal spread impact). Track residual amounts explicitly. Don't liquidate if residual < $2 (not worth the spread cost).

### F12: cleanup_pair + cooldown = cold re-promotion -- FIXED
**Don't clear threshold history on demotion.** Keep the deque data in memory (it's tiny: 720 floats = 5.6KB per pair, 50 pairs = 280KB total). When re-promoted, thresholds are immediately warm. Remove the warm-up gate for re-promoted pairs (only apply to genuinely new pairs).

### F13: Screener REST ranks vs signal engine WS thresholds -- ACCEPTED
These are intentionally different distributions serving different purposes. Screener ranks structural opportunity (which pairs have wide spreads over hours). Signal engine decides moment-to-moment entry timing. No reconciliation needed -- they answer different questions. The screener picks the universe; the engine trades within it.

## Success Criteria (revised)

- Signal frequency: **3-8 entries/day** (vs current 0.5/day, 6-16x improvement)
- Active pairs: 15-20, refreshed hourly
- Core 7 pairs always active (LSK, ACT, 1000CAT, DYM, RPL, KERNEL, ENJ)
- Zero exit blackouts during Binance reconnect
- Inventory accounting consistent after rotation
- No premature entries on warming pairs (first-time only, not re-promotions)
- Total PnL/day improves vs V2 (measured over 1 week)
