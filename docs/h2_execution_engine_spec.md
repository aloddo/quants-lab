# H2 Execution Engine — 1-Week Build Spec

## Goal
Build the minimum execution engine needed for the $20 micro-test.
Paper trader keeps running untouched for comparison. This is a NEW system.

## Scope: Must-Have vs Defer

### Must-Have (Week 1)
- [ ] Real-time price feeds via WebSocket (replace 5s REST polling)
- [ ] WebSocket for order/fill status (both exchanges, real-time fill confirmation)
- [ ] Execution state machine (order lifecycle: submit -> ack -> partial -> fill/reject)
- [ ] Leg failure detection and atomic unwind
- [ ] Stale price defense (timestamp freshness, cross-venue skew detection)
- [ ] Inventory ledger (persistent, per-pair, with reconciliation + auto-rebalance)
- [ ] Proper order types (aggressive limit for entry, market for SL unwind)
- [ ] Depth-aware sizing (check L2 book depth before placing orders, skip if thin)
- [ ] Funding rate tracking on Bybit leg (8h settlements affect PnL)
- [ ] Crash recovery (reload state from MongoDB + query exchange for open orders)
- [ ] Hard kill criteria pre-committed before first trade

### Defer (Phase 2+ only)
- Multi-process architecture (single process OK at $20 size, revisit at $500+)

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                    H2 Live Trader                             │
│                                                               │
│  ┌─────────────┐    ┌──────────────┐    ┌─────────────────┐  │
│  │ PriceFeed   │    │ SignalEngine  │    │ ExecutionEngine  │  │
│  │ (WebSocket) │───>│ (thresholds) │───>│ (state machine)  │  │
│  │             │    │              │    │                  │  │
│  │ BB WS ──┐  │    │ Same P90/P25 │    │ OrderState per   │  │
│  │ BN WS ──┘  │    │ logic as     │    │ leg: IDLE →      │  │
│  │             │    │ paper trader │    │ SUBMITTED →      │  │
│  │ Freshness   │    │              │    │ ACKED →          │  │
│  │ checks per  │    │ Skew guard:  │    │ PARTIAL →        │  │
│  │ venue       │    │ reject if    │    │ FILLED |         │  │
│  │             │    │ venue ages   │    │ REJECTED |       │  │
│  │             │    │ differ >2s   │    │ CANCELLED        │  │
│  └─────────────┘    └──────────────┘    └────────┬────────┘  │
│                                                   │           │
│  ┌──────────────┐    ┌──────────────┐    ┌───────▼────────┐  │
│  │ Inventory    │    │ Risk Manager │    │ LegCoordinator │  │
│  │ Ledger       │    │              │    │                │  │
│  │              │    │ Max daily    │    │ Entry: both    │  │
│  │ Per-pair:    │    │ loss gate    │    │ legs must fill │  │
│  │ expected qty │    │              │    │ within 500ms   │  │
│  │ actual qty   │    │ Max          │    │ or unwind      │  │
│  │ locked qty   │    │ concurrent   │    │                │  │
│  │ dust         │    │              │    │ Exit: both     │  │
│  │              │    │ Circuit      │    │ legs, market   │  │
│  │ Reconcile    │    │ breaker (3   │    │ on SL          │  │
│  │ vs exchange  │    │ leg fails)   │    │                │  │
│  │ every trade  │    │              │    │ Unwind: market │  │
│  └──────────────┘    └──────────────┘    │ the filled leg │  │
│                                          └────────────────┘  │
│  ┌──────────────┐    ┌──────────────┐                        │
│  │ MongoDB      │    │ Telegram     │                        │
│  │ Persistence  │    │ Alerts       │                        │
│  │              │    │              │                        │
│  │ Orders,      │    │ Every trade, │                        │
│  │ positions,   │    │ leg failures,│                        │
│  │ inventory,   │    │ daily P&L,   │                        │
│  │ state        │    │ kill alerts  │                        │
│  └──────────────┘    └──────────────┘                        │
└───────────────────────────────────────────────────────────────┘
        │                        │
   ┌────▼────┐              ┌───▼────┐
   │ Bybit   │              │Binance │
   │ Mainnet │              │  Spot  │
   │ WS+REST │              │ WS+REST│
   └─────────┘              └────────┘
```

## Component Specs

### 1. PriceFeed (WebSocket)

```python
class PriceFeed:
    """Real-time best bid/ask from both exchanges via WebSocket."""
    
    # Bybit: wss://stream.bybit.com/v5/public/linear
    #   Subscribe: {"op": "subscribe", "args": ["tickers.HIGHUSDT"]}
    #   Updates: ~100ms for best bid/ask changes
    
    # Binance: wss://stream.binance.com:9443/ws
    #   Subscribe: individual bookTicker streams per pair
    #   Updates: real-time on every BBO change
    
    # State per pair per venue:
    #   best_bid, best_ask, last_update_ts, venue_latency_ms
    
    # Freshness rules:
    #   - Price older than 2s = STALE, do not trade
    #   - Cross-venue age difference > 1s = SKEWED, do not trade  
    #   - Both fresh and aligned = TRADEABLE
    
    def get_spread(self, symbol: str) -> SpreadSnapshot | None:
        """Returns spread only if both venues are fresh and aligned."""
        bb = self.bybit_prices[symbol]
        bn = self.binance_prices[symbol]
        
        now = time.time()
        bb_age = now - bb.last_update_ts
        bn_age = now - bn.last_update_ts
        
        if bb_age > 2.0 or bn_age > 2.0:
            return None  # STALE
        if abs(bb_age - bn_age) > 1.0:
            return None  # SKEWED
        
        # Compute spread from actual bid/ask (not mid!)
        # BUY_BB_SELL_BN: buy on Bybit ask, sell on Binance bid
        spread_bps = (bn.best_bid - bb.best_ask) / bb.best_ask * 10000
        
        return SpreadSnapshot(
            symbol=symbol,
            spread_bps=spread_bps,
            bb_ask=bb.best_ask,  # what we'd actually PAY on Bybit
            bn_bid=bn.best_bid,  # what we'd actually GET on Binance
            bb_age_ms=bb_age * 1000,
            bn_age_ms=bn_age * 1000,
            timestamp=now,
        )
```

**Key change from paper:** Paper uses `(bb_bid - bn_ask)` which is the theoretical max spread. Live uses `(bn_bid - bb_ask)` which is the actual executable spread after crossing both books. This will show LOWER spreads than paper — that's correct, paper was optimistic.

### 1b. OrderFeed (WebSocket — real-time fill confirmation)

```python
class OrderFeed:
    """Real-time order/fill updates from both exchanges via private WebSocket."""
    
    # Bybit: wss://stream.bybit.com/v5/private
    #   Auth: HMAC signature on connection
    #   Subscribe: {"op": "subscribe", "args": ["order", "execution"]}
    #   Provides: order status changes, fills with exec_id, fees, fill price
    
    # Binance: wss://stream.binance.com:9443/ws/<listenKey>
    #   Auth: listenKey from POST /api/v3/userDataStream (refresh every 30min)
    #   Provides: executionReport events (order updates, fills, cancels)
    
    # ** WS IS THE PRIMARY FILL SOURCE **
    # LegCoordinator listens to WS events, NOT REST polling.
    # REST is used ONLY for reconciliation (on reconnect, startup, timeout).
    
    # Benefits over REST polling:
    #   - Fill confirmation in ~50ms vs 200-500ms REST round-trip
    #   - No polling overhead during idle periods
    #   - Accurate fill timestamps from exchange (not our poll time)
    
    # ** RECONNECT + GAP RECOVERY (critical) **
    # On ANY disconnect (network, auth expiry, exchange maintenance):
    #   1. Set state = WS_DISCONNECTED (block new entries)
    #   2. Immediately fetch via REST:
    #      a. All open orders on both exchanges
    #      b. Recent fills/trades since last seen exec_id
    #      c. Account balances / positions
    #   3. Rebuild order state idempotently from REST data
    #   4. Reconnect WS (re-auth for private feeds)
    #   5. Verify state matches between WS and REST
    #   6. Resume trading only when WS is confirmed live
    #
    # Binance listenKey: expires every 60min without keepalive.
    #   - PUT keepalive every 30min (50% safety margin)
    #   - On expiry: create new listenKey, reconnect, run gap recovery
    # Bybit: HMAC on connect, ping/pong every 20s.
    #   - On disconnect: re-auth, resubscribe, run gap recovery
    
    async def on_bybit_fill(self, msg):
        """Update LegState, notify LegCoordinator."""
    
    async def on_binance_execution_report(self, msg):
        """Update LegState, notify LegCoordinator."""
    
    async def keep_alive(self):
        """Bybit: ping/pong 20s. Binance: PUT listenKey every 30min."""
    
    async def on_disconnect(self, venue: str):
        """Gap recovery: REST reconciliation before resuming WS."""
    
    async def reconcile_from_rest(self, venue: str, since_exec_id: str):
        """Fetch missed fills/orders via REST, rebuild state idempotently."""
```

### 1c. LiquidityVeto (coarse pair-level book check)

```python
class LiquidityVeto:
    """Coarse liquidity filter — NOT a precise pre-trade depth guarantee.
    
    REST L2 snapshots are stale by the time we submit. This is a VETO,
    not a guarantee: it catches structurally thin books (pair delisted,
    exchange maintenance, zero liquidity) but does NOT predict fill quality.
    
    Actual fill protection comes from IOC order type with max slippage bounds.
    """
    
    # Bybit: GET /v5/market/orderbook?category=linear&symbol=HIGHUSDT&limit=5
    # Binance: GET /api/v3/depth?symbol=HIGHUSDT&limit=5
    # Run once per pair on startup, then refresh every 5 minutes (not per-trade)
    
    async def is_pair_liquid(self, symbol: str) -> bool:
        """
        Returns False if the book is structurally empty or dangerously thin.
        
        Veto if:
          - Best bid or ask is zero/missing on either venue
          - Top-of-book qty < $5 on either side (ghost liquidity)
          - Bid-ask spread on either venue > 50bp (market maker gone)
        
        This is a pair-level gate, not a per-trade gate.
        """
```

### 1d. Funding Settlement Guard (lightweight, Week 1)

```python
# Median hold is 5min, funding settles every 8h = ~1% chance of crossing.
# Full funding tracker is Phase 2. Week 1 gets two rules:

FUNDING_SETTLEMENT_TIMES_UTC = [0, 8, 16]  # hours
FUNDING_BLACKOUT_MINUTES = 5  # refuse entries within 5min of settlement

def is_in_funding_blackout() -> bool:
    """Refuse new entries if within 5min of Bybit funding settlement."""
    now = datetime.utcnow()
    for hour in FUNDING_SETTLEMENT_TIMES_UTC:
        settlement = now.replace(hour=hour, minute=0, second=0)
        if abs((now - settlement).total_seconds()) < FUNDING_BLACKOUT_MINUTES * 60:
            return True
    return False

# If a position IS held through settlement (rare), log the funding payment
# from Bybit's execution WS feed and adjust position PnL. No dedicated tracker needed.
```

### 2. ExecutionEngine (State Machine)

```python
class OrderState(Enum):
    IDLE = "idle"
    SUBMITTED = "submitted"      # REST request sent
    ACKED = "acked"              # Exchange confirmed receipt
    PARTIAL = "partial"          # Partially filled
    FILLED = "filled"            # Fully filled
    REJECTED = "rejected"        # Exchange rejected
    CANCELLED = "cancelled"      # We cancelled
    CANCEL_PENDING = "cancel_pending"  # Cancel sent, not confirmed

class LegState:
    order_id: str
    venue: str  # "bybit" | "binance"
    state: OrderState
    submitted_at: float
    filled_qty: float
    filled_price: float
    target_qty: float
    target_price: float
```

### 3. LegCoordinator (the critical part)

```python
class LegCoordinator:
    """Ensures both legs fill atomically, or neither does."""
    
    FILL_TIMEOUT_MS = 500  # Codex recommended 500ms, not 3s
    
    async def execute_entry(self, signal: SignalEvent) -> EntryResult:
        """
        1. Submit both legs concurrently via REST
        2. Wait for fill events from OrderFeed WS (primary source, ~50ms)
        3. If both fill within 500ms: SUCCESS
        4. If one fills, other doesn't after 500ms:
           a. Cancel the unfilled leg
           b. Wait for cancel confirmation (max 200ms)
           c. If still filled after cancel (fill-after-cancel): BOTH legs are open, proceed
           d. If cancelled: close the filled leg at AGGRESSIVE LIMIT (not market)
              - Aggressive limit = bid-1tick for sell, ask+1tick for buy
              - If still unfilled after 500ms more: escalate to MARKET
           e. Log LEG_FAILURE with all details
        5. Return EntryResult with actual fill prices and slippage
        """
    
    async def execute_exit(self, position: LivePosition) -> ExitResult:
        """Same logic but for closing both legs."""
    
    async def emergency_unwind(self, leg: LegState) -> UnwindResult:
        """
        Close a single filled leg.
        First try: aggressive limit (200ms timeout)
        Second try: market order
        Always succeeds — we NEVER leave a naked leg.
        """
```

### 4. InventoryLedger

```python
class InventoryLedger:
    """Persistent per-pair inventory tracking on Binance spot."""
    
    # MongoDB collection: arb_h2_inventory
    # Document per pair:
    # {
    #   symbol: "HIGHUSDT",
    #   base_asset: "HIGH",
    #   expected_qty: 50.0,      # what we think we have
    #   locked_qty: 0.0,         # in open orders
    #   last_reconciled: <ts>,   # when we last checked exchange
    #   last_exchange_qty: 50.0, # what exchange reported
    #   discrepancy: 0.0,        # expected - actual
    #   dust_usdt: 0.0,          # accumulated fee dust in USDT
    # }
    
    async def reconcile(self, symbol: str) -> ReconcileResult:
        """
        Query actual Binance balance via REST.
        Compare to expected_qty.
        Alert if discrepancy > 1% of position size.
        """
    
    def can_sell(self, symbol: str, qty: float) -> bool:
        """Check if we have enough unlocked inventory to sell."""
    
    def lock(self, symbol: str, qty: float):
        """Lock inventory for a pending sell order."""
    
    def release(self, symbol: str, qty: float):
        """Release locked inventory (order cancelled or failed)."""
    
    def sold(self, symbol: str, qty: float, fee_qty: float):
        """Record a completed sell. Adjust expected_qty and dust."""
    
    def bought(self, symbol: str, qty: float, fee_qty: float):
        """Record a completed buy (exit restoring inventory)."""
    
    async def auto_rebalance(self, symbol: str):
        """
        Auto-rebalance inventory when it drifts from target.
        
        Triggers:
          - After failed exit buyback (inventory below target)
          - After dust accumulation exceeds threshold
          - After manual top-up detected (inventory above target)
        
        Actions:
          - If below target by > min_order_size: place limit buy at best ask
          - If above target by > min_order_size: log excess, don't sell (conservative)
          - If dust > $1: convert via Binance dust conversion API
        
        Always reconcile with exchange balance before and after.
        """
    
    async def periodic_reconcile(self):
        """
        Run every 5 minutes:
          1. Query actual balances on Binance for all tracked pairs
          2. Compare to expected_qty in ledger
          3. If discrepancy: auto_rebalance if small, alert if large
          4. Log all discrepancies to MongoDB
        """
```

### 5. Risk Manager

```python
class RiskManager:
    # Pre-committed kill criteria (written BEFORE Phase 1):
    
    KILL_CRITERIA = {
        "max_daily_loss_usd": 20.0,        # Hard shutdown
        "max_avg_slippage_bps": 15.0,       # Pause + review after 10 trades
        "max_consecutive_leg_failures": 3,   # Circuit breaker
        "max_leg_failure_rate": 0.3,         # >30% leg failures = pause
        "min_trades_for_eval": 10,           # Need 10 trades before any conclusion
        "max_loss_per_trade_usd": 5.0,       # Single trade loss cap
    }
    
    # Graceful shutdown sequence (not blunt kill):
    # 1. Set state = NO_NEW_ENTRIES
    # 2. Cancel all working orders
    # 3. Close all open positions (market)
    # 4. Confirm all fills
    # 5. Reconcile inventory
    # 6. Log final state
    # 7. Send Telegram alert
    # 8. Stop
```

### 6. Crash Recovery

```python
class CrashRecovery:
    """On startup, check for orphaned state."""
    
    async def recover(self):
        # BYBIT (perps — positions are directional exposure):
        # 1. Load positions from MongoDB where status = OPEN
        # 2. Query Bybit for open perp positions
        # 3. Compare:
        #    - In MongoDB but not on Bybit = already closed, mark as such
        #    - On Bybit but not in MongoDB = orphan, close immediately
        #    - In both = resume tracking
        # 4. Cancel any stranded orders on Bybit
        
        # BINANCE (spot — balances are NOT positions):
        # Binance spot has no "positions" — only balances.
        # Pre-bought inventory looks identical to an active trade's inventory.
        # Recovery must use the INVENTORY LEDGER as the source of truth:
        # 5. Load inventory ledger targets from MongoDB
        # 6. Query actual Binance balances
        # 7. Compare actual vs (expected_qty - locked_qty):
        #    - Matches = healthy, resume
        #    - Actual < expected = inventory lost (failed buyback), trigger auto-rebalance
        #    - Actual > expected = unexpected surplus (manual top-up?), log + update ledger
        # 8. Cancel any stranded orders on Binance
        # 9. Release any stale locks (locked_qty for orders that no longer exist)
```

## Order Type Strategy

| Scenario | Bybit (perp) | Binance (spot) | Why |
|----------|-------------|----------------|-----|
| Entry (normal) | Limit at ask1 (GTC) | Limit at bid (GTC) | Aggressive limit = taker fill without market order uncertainty |
| Entry (wide book) | Limit at ask1 (IOC, 500ms) | Limit at bid (IOC, 500ms) | IOC = fill what you can immediately, cancel rest |
| Exit (reversion) | Limit at bid1 (GTC) | Limit at ask (GTC) | Not urgent, can wait for fill |
| Exit (stop loss) | Market | Market | Urgent, accept slippage |
| Unwind (leg failure) | Aggressive limit, escalate to market | Aggressive limit, escalate to market | Minimize loss but guarantee exit |

**NOT post-only.** Post-only would get rejected when crossing. We want taker fills.

## Fee Model (corrected)

| Phase | Open | Close | Total RT |
|-------|------|-------|----------|
| Open legs | BB taker 5.5bp + BN taker 10bp = 15.5bp | — | — |
| Close legs | — | BB taker 5.5bp + BN taker 10bp = 15.5bp | — |
| **Total** | | | **31bp** |
| With BNB discount (25% off BN) | 5.5 + 7.5 = 13bp | 5.5 + 7.5 = 13bp | **26bp** |

Paper uses 24bp. Real worst case 31bp, with BNB discount 26bp. Close enough to paper.

**Note:** Entry/exit thresholds are adaptive (P90/P25 per pair), not fixed.
The 30bp MIN_EXCESS_BPS is only a pair viability filter (is P90-P25 wide enough to trade?).
Keep at 30bp — execution skew is handled by the FreshnessGuard (stale/skewed price rejection)
and by recalculating the actual executable spread at order submission time using live WS prices.

## Stale Data Defense

```python
class FreshnessGuard:
    MAX_AGE_S = 2.0          # Max age for any single price
    MAX_SKEW_S = 1.0         # Max age difference between venues
    MAX_UNCHANGED_POLLS = 3  # If price identical for 3 WS updates, suspicious
    MAX_SPREAD_MULTIPLE = 5  # Reject spreads > 5x rolling median
    
    def is_tradeable(self, bb_price, bn_price) -> bool:
        # All 4 checks must pass
```

## Daily Build Plan

| Day | Build | Test |
|-----|-------|------|
| Mon | PriceFeed WS (public, both) + OrderFeed WS (private, both) + reconnect/gap recovery | Verify WS prices match paper REST; verify fill events arrive; test auth + listenKey renewal |
| Tue | ExecutionEngine state machine + LegCoordinator + LiquidityVeto + **real test orders** | Place + cancel tiny test orders on Bybit mainnet + Binance spot. Validate state machine against real exchange acks/rejects. |
| Wed | InventoryLedger (with auto-rebalance + periodic reconcile) + full order flow | End-to-end: signal → depth check → submit both legs → fill via WS → track → close. Test on Bybit mainnet with $1 orders. |
| Thu | RiskManager (3-tier: alert/pause/kill) + CrashRecovery + funding blackout guard | Kill process mid-trade, verify recovery. Force WS disconnect, verify gap recovery. Force leg failure, verify unwind. |
| Fri | Integration: all components wired, shadow mode (real signals, no real orders) | Shadow logs what it WOULD do. Compare signals to paper trader. |
| Sat-Sun | Shadow validation + adversarial fault injection | Force: stale prices, WS drops, leg failures, inventory discrepancy, phantom spikes. >80% signal agreement with paper. |
| Mon (wk2) | Phase 1 launch: $20 micro-test, 2 pairs (HIGH + NOM) | Monitor every trade live. Both pairs from day 1 for faster signal accumulation. |

**Note:** Build integrates early (real exchange orders on Tue, not Wed). Components are validated
against real exchanges as they're built, not after. This avoids the "works in mock, breaks live" trap.

## Pre-Phase-1 Kill Criteria (written now, before any live trade)

### Tiered Response System

**ALERT (auto-handle + notify, keep trading):**
- Single ghost position detected → auto-close attempt + Telegram alert
- Inventory discrepancy < 5% → auto-reconcile + log
- Single leg failure → unwind + log, continue trading
- Funding payment on Bybit leg → log, adjust PnL tracking

**PAUSE (stop new entries, manage existing positions, notify for review):**
- 3 consecutive leg failures → circuit breaker, 15min pause
- Leg failure rate > 30% after 10 trades → likely API/exchange issue
- Inventory discrepancy > 5% and auto-reconcile fails → manual review needed
- Multiple ghost positions (2+) that resist auto-close → structural problem

**KILL (graceful shutdown, close everything):**
- Average per-leg slippage > 15bp after 10 trades → edge is gone
- Daily loss exceeds $20 → hard cap
- Paper trader diverges from live by >50% on executable spread measurement → execution model is wrong

**EVALUATE (after 50+ trades, not before):**
- Edge after real fees and slippage → need 50+ trades for statistical meaning at $20 size
- Week 1 kill criteria are OPERATIONAL (leg failures, inventory, slippage), not profitability
- Profitability evaluation at $20 is noise — wait for sufficient sample

These are PRE-COMMITTED. We don't change them after seeing results.

## Files

```
scripts/
  arb_h2_paper.py          # UNCHANGED — keeps running for comparison
  arb_h2_live.py            # NEW — main entry point for live trader

app/services/arb/
  price_feed.py             # WebSocket price feeds
  signal_engine.py          # Threshold logic (extracted from paper trader)
  execution_engine.py       # Order state machine
  leg_coordinator.py        # Dual-leg atomic execution
  inventory_ledger.py       # Persistent inventory tracking
  risk_manager.py           # Kill criteria + circuit breakers
  crash_recovery.py         # Startup state reconciliation
  freshness_guard.py        # Stale price detection

config/
  h2_live_config.yaml       # Pairs, sizing, thresholds, kill criteria
```

## MongoDB Collections (new)

| Collection | Purpose |
|-----------|---------|
| arb_h2_live_orders | Per-order state transitions (full lifecycle) |
| arb_h2_live_positions | Open/closed positions with actual fill prices |
| arb_h2_inventory | Per-pair inventory ledger |
| arb_h2_leg_failures | Every leg failure with full context |
| arb_h2_paper_comparison | Shadow mode: paper vs live signal comparison |
