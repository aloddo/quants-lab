# H2 Dynamic Pair Screener — Plan V4 (Minimal Viable)

## Problem

H2 spike-fade arb has a static 7-pair watchlist. Zero trades for 24h+ when spreads tighten. Only 12 of 198 USDC pairs cover the 31bp fee threshold at any time, and the viable set shifts every 6 hours. We have 3.8 days of collector data — too thin for fancy modeling.

## Philosophy

Ship the simplest thing that captures 80% of the value. Collect 30 days of live data. Then build V5 with event throttling, regime guards, and expected-edge entry.

## What V4 Does

1. **Reads collector MongoDB every 5 minutes** to rank all 198 USDC pairs by excess (P90 - P25)
2. **Assigns tiers** based on rolling excess:
   - Tier A (excess > 35bp): full position size, quantile gate q=0.87
   - Tier B (excess 31-35bp): half position size, quantile gate q=0.90
   - Tier C (excess 25-31bp): WS connected but entries blocked
3. **Manages WS connections** for Tier A+B+C pairs (~20-30 total), refreshed every 4h
4. **Trades** Tier A and B pairs using existing P90/P25 signal engine with per-tier quantile

## What V4 Does NOT Do

- Expected edge calculation (dead code at these margins — C2 finding)
- Event-aware throttling (needs WS-rate data to calibrate — M1 finding)
- Regime kill-switches (needs 30+ days of data for baselines)
- Per-pair slippage estimation (needs 10+ trades per pair)

These are V5 features, gated on 30 days of live V4 data.

## Architecture

```
arb_dual_collector (unchanged — REST, 5s, 198 pairs)
  └── arb_bn_usdc_bb_perp_snapshots (MongoDB)

arb_h2_live_v3.py
  ├── TierEngine (NEW)
  │     ├── Every 5 min: queries collector MongoDB for last 6h of snapshots
  │     ├── Aggregation pipeline: per symbol_bn, compute P25/P90/excess of |best_spread|
  │     ├── Assigns Tier A/B/C based on excess thresholds
  │     ├── Returns: dict[symbol] -> TierInfo(tier, excess, p90, p25, quantile_gate)
  │     ├── Logs tier transitions to MongoDB (arb_h2_tier_history) for V5 calibration
  │     └── Telegram notification on tier changes
  │
  ├── UniverseManager (NEW)
  │     ├── Every 4h: diffs current WS set vs TierEngine top-30
  │     ├── Hysteresis: pair must be below Tier C for 2 consecutive cycles to disconnect
  │     ├── Never disconnect a pair with open position (wait for exit)
  │     ├── Bybit WS: dynamic subscribe/unsubscribe (send op on existing connection)
  │     ├── Binance WS: hot-swap reconnect
  │     │   1. Build new URL with updated symbol set
  │     │   2. Connect new WS, keep old running
  │     │   3. Wait until new WS delivers first tick for every KEPT pair (max 30s)
  │     │   4. Atomic swap: route all reads to new connection
  │     │   5. Close old connection
  │     │   6. If step 3 times out: abort, keep old connection, retry next cycle
  │     │   REST fallback: if both WS fail, poll REST for exit prices (5s latency, safe for exits)
  │     └── Max 30 symbols (15% of WS limit)
  │
  ├── SignalEngine (MODIFIED — per-tier quantile)
  │     ├── check_entry(): look up pair's tier → use tier-specific quantile gate
  │     │   Tier A: spread >= P87 (numpy percentile 87)
  │     │   Tier B: spread >= P90 (current behavior)
  │     │   Tier C: entry blocked (return None)
  │     │   No tier: entry blocked
  │     ├── AdaptiveThresholds: unchanged (rolling window=720 from WS data)
  │     ├── Viable check: unchanged (excess > MIN_EXCESS_BPS=30)
  │     ├── Seed new pairs: load last 720 collector snapshots from MongoDB
  │     │   No scaling factor (removed — scaling was contentious, warm-up gate handles it)
  │     ├── Warm-up gate: new pair blocked until 180 WS ticks received (~36s at 5/s)
  │     │   Re-promoted pairs (history still in memory): no warm-up needed
  │     └── Keep threshold history in memory on WS disconnect (720 floats = 5.6KB per pair)
  │
  ├── InventoryLedger (MODIFIED)
  │     ├── Position size: Tier A = POSITION_USD ($10), Tier B = POSITION_USD * 0.5 ($5)
  │     ├── Dormant inventory on disconnect: hold up to 5 pairs, liquidate smallest first if exceeded
  │     ├── Re-use dormant inventory on reconnection
  │     └── Skip liquidation if balance < $2
  │
  └── Everything else unchanged (PriceFeed base, OrderGateway, RiskManager, PositionStore, ExitFlow)
```

## Thresholds

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Tier A excess | > 35bp | Separates consistently wide pairs from marginal. Based on 3.8d data: 5-7 pairs typically qualify. |
| Tier B excess | 31-35bp | Covers fees but marginal. Half position size limits risk. |
| Tier C excess | 25-31bp | Near-viable. WS connected for opportunity capture if they strengthen. |
| Tier A quantile | P87 | ~30% more entries than P90 on proven pairs. Conservative enough to still require spikes. |
| Tier B quantile | P90 | Current standard. No change for conditional pairs. |
| Universe refresh | 4h | 6h turnover shows 2-5 pair changes. 4h catches shifts with margin. |
| Tier recomputation | 5min | Cheap MongoDB aggregation. Fast enough for intra-hour regime shifts. |
| Warm-up ticks | 180 | 50% shorter than V2's 360. 36s at WS rate. Fast enough to catch second spike in a cluster. |
| WS hysteresis | 2 cycles (8h) | Prevents thrashing. Pair must stay below Tier C for 8h before WS disconnect. |
| Max WS symbols | 30 | 15% of limit. Plenty of headroom. |
| Max dormant inventory | 5 pairs | ~$50 max locked capital at $10/pair. |
| Cooldown per pair | 600s (10min) | Existing V2 value. Unchanged for now. V5 replaces with event throttler. |

## Data Flow

```
Every 5s:  Collector → MongoDB (198 pairs, REST)
Every 5min: TierEngine reads MongoDB → computes tiers → updates SignalEngine tier map
Every 4h:  UniverseManager diffs tiers → adjusts WS connections (add/remove pairs)
Every WS tick: PriceFeed → SignalEngine.check_entry(tier) → EntryFlow (if tier A or B)
Every WS tick: PriceFeed → SignalEngine.check_exit() → ExitFlow (unchanged)
```

## Implementation

### Phase 1: TierEngine (standalone, read-only)
**File:** `app/services/arb/tier_engine.py` (~150 lines)

```python
@dataclass
class TierInfo:
    symbol: str
    tier: str  # "A", "B", "C", or None
    excess: float
    p90: float
    p25: float
    quantile_gate: float  # 0.87 for A, 0.90 for B
    position_size_mult: float  # 1.0 for A, 0.5 for B

class TierEngine:
    def __init__(self, mongo_uri: str, excess_a=35.0, excess_b=31.0, excess_c=25.0):
        ...

    def recompute(self) -> dict[str, TierInfo]:
        """Query collector MongoDB, compute tiers for all 198 pairs."""
        pipeline = [
            {"$match": {"timestamp": {"$gte": cutoff_6h}}},
            {"$group": {
                "_id": "$symbol_bn",
                "spreads": {"$push": {"$abs": "$best_spread"}},
            }},
        ]
        # compute P25, P90, excess from spreads array per pair
        # assign tier based on excess thresholds
        ...
```

**Validation:** Run standalone, compare tier assignments against our manual analysis.
**Estimate: 2h**

### Phase 2: UniverseManager + PriceFeed modifications
**File:** `app/services/arb/universe_manager.py` (~200 lines), modify `price_feed.py`

Bybit changes (simple — native WS support):
```python
# BybitPriceFeed
async def subscribe(self, symbol: str):
    await self._ws.send_json({"op": "subscribe", "args": [f"tickers.{symbol}"]})
    self.symbols.append(symbol)

async def unsubscribe(self, symbol: str):
    await self._ws.send_json({"op": "unsubscribe", "args": [f"tickers.{symbol}"]})
    self.symbols.remove(symbol)
```

Binance changes (hard — requires reconnect):
```python
# BinancePriceFeed
async def hot_swap(self, new_symbols: list[str], session: aiohttp.ClientSession):
    old_ws = self._ws
    new_url = self._build_url(new_symbols)
    new_ws = await session.ws_connect(new_url, heartbeat=self.PING_INTERVAL)
    # Wait for first tick on each kept pair
    kept = set(self.symbols) & set(new_symbols)
    seen = set()
    deadline = time.time() + 30
    while seen != kept and time.time() < deadline:
        msg = await asyncio.wait_for(new_ws.receive(), timeout=1.0)
        # parse symbol from msg, add to seen
        ...
    if seen == kept:
        self._ws = new_ws
        self.symbols = new_symbols
        await old_ws.close()
    else:
        await new_ws.close()  # abort
        logger.warning("Hot-swap aborted, keeping old connection")
```

**Estimate: 3h** (Binance hot-swap + testing)

### Phase 3: SignalEngine modifications + Integration
**Modify:** `signal_engine.py`, `arb_h2_live_v3.py`

SignalEngine changes (~30 lines):
- Add `tier_map: dict[str, TierInfo]` attribute, updated by TierEngine
- `check_entry()`: look up tier, use tier-specific quantile, block if Tier C or None
- `seed_from_collector()`: load 720 snapshots from MongoDB for new pairs
- Keep history on disconnect (don't clear deque)

Integration (~100 lines in V3 trader):
- Replace `PAIRS` constant with `tier_engine.tradable_pairs()`
- Background tasks: tier recompute (5min), universe refresh (4h)
- Position size from `tier_info.position_size_mult * POSITION_USD`
- Dormant inventory tracking
- Telegram: tier breakdown in status report

**Estimate: 2h**

### Phase 4: Validation
1. **Offline replay:** Feed 3.8 days of collector data to TierEngine, verify tier assignments
2. **Shadow mode:** V3 alongside V2, log signals only
3. **Single pair:** V3 with 1 Tier A pair live
4. **Full deployment:** All tiers, replace V2
5. **2-week data collection** for V5 calibration

**Estimate: 8h wall clock**

### Total: ~7h code + 8h validation

## MongoDB Changes

1. **New index on collector:** `(symbol_bn, timestamp)` compound index for TierEngine queries
2. **Fix spread rounding:** increase from 2 to 4 decimal places in collector
3. **New collection:** `arb_h2_tier_history` — logs tier transitions for V5 analysis

## Adversarial Review V4 — CONDITIONAL PASS (3 fixes applied)

### Fix 1: MongoDB aggregation — use $percentile, fix index
Use native `$percentile` operator instead of `$push` + in-memory compute.
Create `(symbol_bn, timestamp)` compound index on collector collection.

### Fix 2: Binance hot-swap — 80% threshold, not 100%
Require 80% of kept pairs to deliver first tick within 30s (not 100%).
Low-volume USDC pairs (ONT, ILV) may not tick in 30s — 100% causes abort every cycle.

### Fix 3: Never disconnect pair with recent position
Add to hysteresis: a pair that had an open position in the last 2h cannot be
disconnected from WS, regardless of tier. Prevents exit signal loss on demotion.
REST fallback is 5s but spike-fade reversion windows are 2-10s — too slow.

### Additional fixes
- Dormant inventory liquidation: use limit orders with 60s timeout, not market orders
- Drop Telegram tier-change notifications. Log to MongoDB only. Save 30min.

## What We'll Learn for V5

After 30 days of V4 data:
- Per-pair actual slippage distribution → calibrate expected edge calculation
- WS-rate spike clustering patterns → calibrate event throttler gap and re-expansion
- Regime transition frequency → calibrate kill-switch thresholds and recovery windows
- Tier stability over time → validate or adjust excess thresholds
- P87 vs P90 entry quality comparison → optimize per-tier quantile gates
