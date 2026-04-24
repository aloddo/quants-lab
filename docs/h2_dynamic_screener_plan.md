# H2 Dynamic Pair Screener — Implementation Plan

## Problem Statement

H2 spike-fade arb (Binance USDC spot vs Bybit perp) has a fixed 7-pair watchlist. When spreads tighten on those pairs — as happened Apr 21-23 (zero signals in 24h+) — the bot goes completely idle despite 198 monitored pairs having tradeable spreads elsewhere.

The old USDT paper trader averaged 70 trades/day because HIGH alone had 83bp avg spread. Current USDC pairs (ENJ 20bp, KERNEL 17bp) are mid-table. Meanwhile LSK (41bp), ACT (41bp), RPL (37bp) sit untouched.

## Goal

Replace the static pair list with a dynamic screener that:
1. Continuously ranks all 198 USDC pairs by tradeable spread opportunity
2. Rotates the top-K pairs into the active trading set
3. Gracefully handles pair transitions (positions, WS feeds, thresholds)

## Architecture

### Current System (what we have)

```
arb_dual_collector (REST, 5s poll)
  ├── 198 USDC pairs → arb_bn_usdc_bb_perp_snapshots (MongoDB)
  └── Bybit spot-perp pairs → arb_bb_spot_perp_snapshots

arb_h2_live_v2.py (independent process)
  ├── PriceFeed(7 pairs) → WS connections (Bybit + Binance)
  ├── SignalEngine → AdaptiveThresholds (rolling window=720)
  ├── EntryFlow / ExitFlow → OrderGateway
  └── InventoryLedger / RiskManager
```

### Proposed System

```
arb_dual_collector (unchanged — REST, 5s poll, 198 pairs)
  └── arb_bn_usdc_bb_perp_snapshots (MongoDB)

arb_h2_live_v3.py (enhanced trader)
  ├── PairScreener (NEW — reads collector MongoDB, ranks pairs)
  │     ├── Reads latest snapshots from MongoDB every 60s
  │     ├── Computes rolling metrics per pair (avg spread, spike freq, P90, excess)
  │     ├── Filters: min avg spread, min excess, min liquidity
  │     ├── Ranks by composite score
  │     └── Emits top-K pair list (default K=15)
  │
  ├── PairRotator (NEW — manages transitions)
  │     ├── Compares current active set vs screener output
  │     ├── PROMOTE: add new pairs → start WS, seed thresholds from MongoDB
  │     ├── DEMOTE: remove stale pairs → close positions first, then stop WS
  │     ├── PROTECT: never demote a pair with an open position (wait for exit)
  │     ├── Cooldown: don't re-add a pair within 10min of demotion
  │     └── Max churn: rotate at most 3 pairs per cycle (stability)
  │
  ├── PriceFeed (MODIFIED — dynamic add/remove)
  │     ├── add_pair(symbol, bn_symbol) → subscribe WS topic
  │     ├── remove_pair(symbol) → unsubscribe WS topic
  │     └── Max pairs: 50 (well within WS limits)
  │
  ├── SignalEngine (MINOR CHANGES)
  │     ├── AdaptiveThresholds.seed_from_collector(symbol) → bootstrap from MongoDB
  │     └── Remove pair cleanup when demoted
  │
  └── Everything else unchanged (EntryFlow, ExitFlow, Inventory, Risk)
```

### Key Design Decisions

**1. Screener reads MongoDB, not WS**
The dual collector already writes 198 pairs every 5s. The screener reads this — no additional exchange connections, no rate limit impact, 60s ranking cycle is fast enough for hourly pair rotation.

**2. WS only for active pairs (latency)**
We need sub-second execution for the active set. REST polling (5s) is too slow for entry timing — a spread spike can appear and disappear in 1-2 seconds. Active pairs get WS; screener uses REST data for ranking only.

**3. Pair rotation is slow and conservative**
Rotation every 5 minutes, max 3 changes per cycle, never demote with open position. This prevents thrashing and ensures the AdaptiveThresholds have time to warm up (window=720, needs ~360 ticks = ~6 min at WS rate).

**4. Inventory management for rotated pairs**
When a pair is demoted, we must handle the Binance spot inventory. Options:
- (a) Sell inventory on demotion — simplest but takes a loss if token is down
- (b) Hold inventory, re-buy on next promotion — saves spread but ties up capital
- (c) Mark as "inactive inventory" — don't trade but keep for potential future use
Recommend (a) for V1 — clean state, simple accounting.

## Implementation Plan

### Phase 1: PairScreener (read-only, no trading changes)

**File:** `app/services/arb/pair_screener.py`

```python
class PairScreener:
    def __init__(self, mongo_uri, min_avg_spread=20.0, min_excess=25.0, top_k=15):
        ...
    
    def rank_pairs(self) -> list[ScoredPair]:
        """Query last 720 collector snapshots per pair, compute score, return ranked list."""
        # For each of 198 pairs:
        #   1. Pull last 720 snapshots from arb_bn_usdc_bb_perp_snapshots
        #   2. Compute: avg_spread, p90, p25, excess, spike_freq (% above p90)
        #   3. Filter: avg_spread >= min_avg_spread, excess >= min_excess
        #   4. Score = excess * spike_freq (trade opportunity density)
        #   5. Return sorted top-K
```

**Validation:** Run screener standalone, compare output vs manual analysis from today. Should surface LSK, ACT, RPL as top pairs.

**Deliverable:** Screener ranking matches intuition from collector data analysis.

### Phase 2: Dynamic PriceFeed

**File:** Modify `app/services/arb/price_feed.py`

Add to `BybitPriceFeed`:
```python
async def add_symbol(self, symbol: str):
    """Subscribe to new symbol on existing WS connection."""
    
async def remove_symbol(self, symbol: str):
    """Unsubscribe from symbol."""
```

Add to `BinancePriceFeed`:
```python
# Binance combined stream doesn't support dynamic subscribe.
# Must reconnect with new stream list.
# Use a reconnect-on-change approach with debouncing.
async def update_symbols(self, symbols: list[str]):
    """Reconnect with updated symbol list (debounced, max 1 reconnect per 30s)."""
```

Add to `PriceFeed`:
```python
async def add_pair(self, symbol: str, bn_symbol: str):
    """Add a pair to both feeds."""
    
async def remove_pair(self, symbol: str):
    """Remove a pair from both feeds."""
```

**Bybit WS supports dynamic subscribe** — send `{"op": "subscribe", "args": [...]}` on existing connection. No reconnect needed.

**Binance combined stream does NOT support dynamic subscribe** — must close and reopen with new stream URL. This is the trickiest part. Debounce with 30s cooldown, batch all pending changes into one reconnect.

### Phase 3: PairRotator

**File:** `app/services/arb/pair_rotator.py`

```python
class PairRotator:
    def __init__(self, screener, price_feed, signal_engine, inventory, 
                 rotation_interval=300, max_churn=3, cooldown=600):
        ...
    
    async def rotation_cycle(self):
        """Called every rotation_interval seconds."""
        ranked = self.screener.rank_pairs()
        target_set = {p.symbol for p in ranked[:self.top_k]}
        current_set = set(self.price_feed.symbols)
        
        to_add = target_set - current_set
        to_remove = current_set - target_set
        
        # Never remove pairs with open positions
        to_remove -= {s for s in to_remove if s in self.signal_engine.open_positions}
        
        # Respect cooldown
        to_add -= {s for s in to_add if self._in_cooldown(s)}
        
        # Respect max churn
        changes = list(to_remove)[:self.max_churn] + list(to_add)[:self.max_churn]
        
        for sym in to_remove[:self.max_churn]:
            await self._demote_pair(sym)
        for sym in to_add[:self.max_churn]:
            await self._promote_pair(sym)
```

### Phase 4: Integration into arb_h2_live_v3.py

- Copy `arb_h2_live_v2.py` → `arb_h2_live_v3.py`
- Replace static `PAIRS` list with screener initialization
- Add rotation loop as background task in `run()`
- Modify `InventoryLedger` to handle dynamic pair set
- Add Telegram notifications for pair rotations

### Phase 5: Validation

1. **Shadow mode first** — run V3 alongside V2, log what it WOULD trade, compare signal quality
2. **Single-pair test** — deploy V3 with just 1 pair, verify WS add/remove works
3. **Small set** — run with top-5 pairs, verify rotation works over 24h
4. **Full deployment** — top-15 pairs, replace V2

## Rate Limit Budget

| Component | Calls | Limit | Utilization |
|-----------|-------|-------|-------------|
| Dual collector (REST) | 3/5s = 36/min | Bybit: 1440/min | 2.5% |
| Screener (MongoDB read) | 1/60s | No limit | N/A |
| Trader WS (Bybit) | 15 topics/conn | 200 topics/conn | 7.5% |
| Trader WS (Binance) | 15 streams/conn | 200 streams/conn | 7.5% |
| Trading orders | ~2/trade, ~5 trades/h | 600/min | <1% |

Total exchange API utilization: ~10%. Plenty of headroom.

## Risk Analysis

| Risk | Mitigation |
|------|-----------|
| Binance WS reconnect drops fills | Queue pending orders, retry after reconnect. Max 1 reconnect per 30s. |
| Threshold warm-up on new pair | Seed from MongoDB collector data (720 snapshots available immediately) |
| Pair churn destroys spread history | Max 3 rotations per cycle, 10min cooldown prevents thrashing |
| Demoted pair has open inventory | Never demote with open position. If position exits, pair goes to cooldown. |
| Screener picks illiquid pairs | Add min volume filter (from Bybit/Binance ticker data) |
| Regression on known pairs | Shadow mode (Phase 5) catches this before live cutover |

## What This Does NOT Change

- Entry/exit logic (P90/P25 adaptive thresholds) — unchanged
- Order execution (LegCoordinator, OrderGateway) — unchanged  
- Risk management (RiskManager, InventoryGuard) — unchanged
- Position tracking (PositionStore, MongoDB) — unchanged
- Collector infrastructure — unchanged

## Estimated Effort

| Phase | Effort | Dependencies |
|-------|--------|-------------|
| 1. PairScreener | 1h | None |
| 2. Dynamic PriceFeed | 2-3h | Binance reconnect is the hard part |
| 3. PairRotator | 1h | Phase 1, 2 |
| 4. V3 Integration | 1-2h | Phase 1, 2, 3 |
| 5. Validation | 4-8h (wall clock) | Phase 4 |
| **Total** | **~6h code + 8h validation** | |

## Adversarial Review Findings & Fixes

### F1. Binance WS reconnect creates exit blackout (MONEY RISK) -- FIXED
During reconnect, all Binance prices go stale, blocking exits including stop-losses.
**Fix:** Hot-swap architecture. Start NEW Binance WS connection before closing OLD one. Only close old after new is streaming. During overlap, use new connection's data. Fallback: if hot-swap fails, fall back to REST polling for exit prices during reconnect (5s latency, acceptable for exits).

### F2. Selling inventory on demotion locks in losses (MONEY RISK) -- FIXED
**Fix:** Change to option (b) with limits. Hold inventory on demotion, track as "dormant inventory". Re-use when pair is re-promoted. Add hard cap: if dormant inventory total exceeds $50, sell the oldest/worst-performing dormant pair. This avoids unnecessary spread costs while capping capital lock-up.

### F3. REST collector P90 != WS P90 distribution mismatch (EXECUTION BUG) -- FIXED
5s REST snapshots miss sub-second spikes. WS P90 will be higher than collector P90.
**Fix:** Two-phase threshold seeding: (1) seed from collector data with a 0.85x scaling factor (conservative), (2) mark pair as "warming up" for first 360 WS ticks, (3) only allow entries after warm-up complete. The scaling factor is empirically calibrated: run both REST and WS for 1h on 3 pairs and measure the ratio. This is a Phase 5 validation step.

### F4. Race between rotation and entry flow (EXECUTION BUG) -- FIXED
**Fix:** All rotation mutations happen inside a rotation lock. The main trading loop acquires a read-lock on the pair set before iterating. Rotation cycle acquires write-lock. Additionally: never remove a pair from PriceFeed if any signal for that pair is in-flight (check EntryFlow/ExitFlow pending queue).

### F5. MongoDB screener query is O(198 x 720) = 142K reads/min -- FIXED
**Fix:** Use a single aggregation pipeline with `$group` and `$percentile` (MongoDB 7.0+). If on older MongoDB, pre-compute per-pair stats in the collector (add `$avg`, `$max` fields to a `arb_pair_stats` collection updated every 60s). The collector already iterates all pairs -- adding a rolling stats update is trivial.

### F6. Max churn allows 6 changes (3 remove + 3 add), not 3 -- FIXED
**Fix:** Single budget: `total_changes = min(len(to_remove) + len(to_add), max_churn)`. Prioritize removals (free up capacity before adding). If 2 removals, allow at most 1 add. Total always <= max_churn.

## Success Criteria

- Signal frequency: >= 20 entries/day (vs current 0.5/day)
- Active pairs: top-15 by spread opportunity, rotated dynamically
- No manual pair management needed
- Zero missed rotations due to rate limits
- Inventory accounting always consistent after rotation
- No exit blackouts during Binance WS reconnect (hot-swap verified)
- Threshold warm-up period enforced (no premature entries on new pairs)
