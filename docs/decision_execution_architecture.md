# Decision: Execution Architecture
**Status:** OPEN (Apr 17, 2026)
**Question:** Should we replace HB for execution of slow strategies (X5, X8)?

## The Three Options

### Option A: Fix HB, Keep HB
Patch WS connections, optimize containers, live with HB's execution model for everything.

### Option B: Full Execution Service (what the draft plan proposed)
Build a complete execution layer: signal loop, order management, fill monitor, TP/SL/TL state machine. Migrate X5/X8 off HB entirely.

### Option C: Thin Execution Wrapper
Don't rebuild PositionExecutor. Build a thin wrapper (~300 lines) that:
1. Runs signal loop (calls controller.update_processed_data on a timer)
2. Places orders via BybitExchangeClient REST
3. Delegates TP/SL to Bybit's native conditional orders (exchange-managed)
4. Polls for fills via REST every 30s (no private WS needed)
5. Time limit close via simple cron check
6. Runs as a QL pipeline task, not a separate service

---

## Deep Analysis

### Option A: Fix HB, Keep HB

**What "fix WS waste" actually requires:**

The claim was "1 sed command to disable orderbook WS." That's wrong. Here's what HB's connector actually does:

1. `bybit_perpetual_derivative.py` line 306: `_create_order_book_data_source()` -- creates the orderbook WS connection. This is called from the connector's `start()` method deep in HB core.
2. The PositionExecutor uses mark price from this orderbook for TP/SL evaluation.
3. Disabling orderbook WS would break TP/SL monitoring -- the executor can't determine when SL/TP is hit.
4. The alternative (REST mark price polling) would require patching PositionExecutor internals -- deep HB core surgery.
5. HB also opens candle WS per pair via CandlesFeed. This is how `market_data_provider.get_candles_df()` works. Disabling it breaks the controller's data source.

**Verdict: "Fix WS waste" is NOT a simple patch.** It requires modifying HB core behavior (orderbook data source + candle feed), which violates our "never modify core" principle and breaks on every HB update.

**What we CAN do with simple patches:**
- Reduce pairs per bot (12 to 6) -- halves WS connections
- That's it. Everything else is deep HB surgery.

**Pros:**
- No new code for execution
- HB's PositionExecutor handles order lifecycle
- Backtest-to-prod path unchanged

**Cons:**
- HB API crash-looping 266x/day (18,605 errors in pipeline log)
- "Battle-tested" PositionExecutor has produced: ghost bots, stacked positions, orphaned positions, INSUFFICIENT_BALANCE false positives
- Cannot fix WS waste without HB core surgery
- 330MB per container, ~25 WS connections minimum per bot
- Every HB update risks breaking our sed patches
- X5's 50% TIME_LIMIT exits may be partly from infra instability (WS drops disrupting TP/SL monitoring)
- HB API is single point of failure for ALL bots
- Still need 4 containers for 24 pairs (at 6 pairs each) = 1.3GB + 100 WS connections

**The uncomfortable truth:** We've been assuming HB execution is reliable. The evidence says otherwise. We've had:
- Ghost bots creating 2x stacked positions ($600 instead of $300)
- Orphaned positions after crashes (9 had to be manually closed)
- 266 API restarts/day
- Reconciliation detecting mismatches every cycle
- 50% of X5 closes are TIME_LIMIT (are some of these infra failures misclassified?)

HB's PositionExecutor may be "battle-tested" in general, but it's NOT battle-tested on Bybit demo with our patched connector, Colima Docker, and crash-looping API.

---

### Option B: Full Execution Service

**What we'd need to build:**
1. Signal loop (trivial -- call update_processed_data on timer)
2. Order placement (BybitExchangeClient already has REST, but needs `place_order`, `cancel_order`, `amend_order` -- NOT yet built)
3. Fill detection (private WS or REST polling)
4. TP/SL management state machine (non-trivial)
5. Trailing stop logic (non-trivial)
6. Partial fill handling
7. Retry logic with exponential backoff
8. Position reconciliation
9. Error recovery after process restart

**Effort estimate:** 1,500-2,000 lines of code. 2 weeks to build, 2 more weeks to stabilize.

**Pros:**
- Zero dependency on HB for slow strategies
- Full control over execution path
- Simpler debugging (our code, our logs)
- 200MB shared vs 1.3GB for 4 containers
- 1-2 WS connections vs 100
- Can evolve independently of HB release cycle

**Cons:**
- We're rebuilding what HB already does
- TP/SL trailing stop state machine is where the bugs live
- 4 weeks of work before it's production-ready
- Introduces a SECOND execution path -- backtest, HB live, AND our service
- More code = more maintenance
- We're paper trading and losing money -- this is plumbing, not alpha

**The uncomfortable truth:** Building a custom execution engine while we haven't found profitable strategies is premature optimization. We're solving a scaling problem we don't have (we have 1 strategy running, not 20).

---

### Option C: Thin Execution Wrapper

**Key insight: Bybit V5 API has native TP/SL conditional orders.**

When you place a position on Bybit, you can set:
- `takeProfit`: price where exchange auto-closes for profit
- `stopLoss`: price where exchange auto-closes for loss
- These execute ON BYBIT'S SERVER, not in our code
- They survive our process crashes, WS disconnections, API restarts -- everything

This means we DON'T need to:
- Monitor mark price continuously
- Manage a TP/SL state machine
- Handle the "what if our process crashes while in a position" problem
- Maintain WS connections for price monitoring

**What the thin wrapper actually does:**

```python
class ThinExecutor:
    """~300 lines. Runs as a QL pipeline task."""

    async def signal_check(self):
        """Called every 60s by TaskOrchestrator."""
        for strategy in self.strategies:
            # 1. Load latest data (parquet + MongoDB)
            df = load_candles_and_derivatives(strategy)

            # 2. Run controller signal logic (same code as backtest)
            controller.update_processed_data(df)
            signal = controller.processed_data["signal"]

            if signal == 0:
                continue

            # 3. Check risk limits (read portfolio_risk_state from MongoDB)
            if not risk_manager.can_open(strategy, signal):
                continue

            # 4. Place order with TP/SL on exchange
            order = await exchange.place_order(
                pair=strategy.pair,
                side=signal,
                qty=risk_manager.compute_size(strategy, signal),
                take_profit=controller.get_tp_price(),
                stop_loss=controller.get_sl_price(),
            )

            # 5. Record in MongoDB
            await db.executor_state.insert_one({...})

    async def lifecycle_check(self):
        """Called every 5min. Handles time limits + trailing stops."""
        for position in active_positions:
            # Time limit check
            if position.age > position.time_limit:
                await exchange.close_position(position)

            # Trailing stop: amend SL order on exchange
            if position.should_trail(current_price):
                await exchange.amend_sl(position, new_sl_price)
```

**What needs to be added to BybitExchangeClient:**
- `place_order(pair, side, qty, order_type, price, take_profit, stop_loss)` -- 1 REST call
- `cancel_order(order_id)` -- 1 REST call
- `amend_order(order_id, new_sl)` -- 1 REST call
- `close_position(pair, side)` -- 1 REST call (market order)

~100 lines of new code in the exchange client. Bybit V5 docs are clear.

**Pros:**
- Very little new code (~400 lines total: 300 wrapper + 100 exchange client)
- Exchange-managed TP/SL -- survives our crashes
- Zero WS connections needed (pure REST)
- No state machine for TP/SL (exchange handles it)
- 0 MB per strategy (runs in pipeline process)
- Backtest bridge is clean: same controller.update_processed_data(), same signal logic
- Can build + test in 3-4 days, not weeks
- Simpler than HB's PositionExecutor (less code = fewer bugs)
- If it breaks, we can fall back to HB containers

**Cons:**
- REST polling every 30-60s for fill detection (vs WS push in HB)
- Trailing stop requires periodic amend calls (extra REST load)
- No partial fill handling (Bybit perp orders are all-or-nothing at our sizes, so this is theoretical)
- Introduces a second execution path (but a very simple one)
- Exchange-managed TP/SL means we can't do complex exit logic (multiple take profit levels, dynamic adjustment). For X5/X8 this is fine -- they use simple TP/SL/TL.
- We haven't built order placement in BybitExchangeClient yet (need to add ~100 lines)

**The nuanced truth:** This isn't "replacing HB" -- it's using Bybit's exchange as the execution engine instead of HB's PositionExecutor. We're moving the TP/SL responsibility from our infrastructure (which crashes) to Bybit's infrastructure (which doesn't).

---

## Backtest-to-Production Bridge Analysis

**The signal path (identical in all options):**
```
Backtest:  BacktestingEngine loads DataFrame -> controller.update_processed_data(df) -> reads signal
HB Live:   HB event loop fetches candles -> controller.update_processed_data(df) -> reads signal
Option C:  Pipeline task loads candles -> controller.update_processed_data(df) -> reads signal
```

Signal generation is the SAME controller code. No bridge gap here.

**The execution path (where gaps live):**

| Dimension | Backtest | HB Live | Option C |
|---|---|---|---|
| Entry | Bar close price, instant fill | LIMIT order, WS fill confirm | LIMIT order, REST fill poll |
| TP/SL | Checked on bar high/low | Monitored via orderbook WS mark price | Exchange-managed conditional order |
| Trailing stop | Checked on bar close | Monitored continuously via WS | Amended every 30-60s via REST |
| Time limit | Checked on bar timestamp | Timer in PositionExecutor | Checked every 5min in lifecycle loop |
| Fill latency | 0 | ~100ms (WS) | ~30s (REST poll) |
| Slippage | Modeled (trade_cost) | Real | Real |

**Key gap: trailing stop granularity.** Backtest checks trailing stop on bar close (1m). HB checks continuously (sub-second). Option C checks every 30-60s. For a 1h strategy with 8h time limit, 30-60s trailing stop granularity is MORE than adequate. The price move that triggers a trail adjustment won't reverse in 30s on a position held for hours.

**Key gap: fill detection latency.** HB detects fills in ~100ms via WS. Option C detects in ~30s via REST. For a 1h strategy, 30s fill latency is irrelevant -- the signal fires once per hour.

**Verdict: Option C has NO meaningful bridge gap vs HB for slow strategies (1h+ signals).** The granularity differences (30s vs 100ms) are noise on a strategy that signals hourly.

---

## Resource Impact

| | Option A (4 bots x 6 pairs) | Option B (Execution Service) | Option C (Thin Wrapper) |
|---|---|---|---|
| Memory | +1.3GB | +200MB | +0MB (runs in pipeline) |
| WS connections | ~100 | 1-2 | 0 |
| REST calls/min | ~120 (polling per container) | ~30 (shared) | ~25 (fill poll + trail amend) |
| New code | 0 | ~2,000 lines | ~400 lines |
| Time to build | 1 day (patches) | 4 weeks | 3-4 days |
| Time to stabilize | Already unstable | 2-4 weeks | 1 week |
| Colima memory needed | 5-6 GB | 4-5 GB | 4 GB (current) |

---

## Machine Constraints (actual, measured)

| Resource | Available | Option A needs | Option C needs |
|---|---|---|---|
| RAM | 16 GB (10.8 free) | +1.3 GB (Colima 5GB) | +0 (Colima stays 4GB) |
| Disk | 300 GB free | No change | +5-10 GB (microstructure data) |
| WS connections | ~100 limit (demo) | ~100 (pushing it) | ~3 (collector only) |
| CPU | 2 cores (Colima) | Shared across containers | N/A (runs on host) |

---

## Recommendation Matrix

| Criterion | Weight | Option A | Option B | Option C |
|---|---|---|---|---|
| Time to implement | HIGH | Best (1d) | Worst (4w) | Good (3-4d) |
| Execution reliability | HIGH | Poor (crash loop) | Unknown (new code) | Good (exchange-managed) |
| Backtest bridge fidelity | HIGH | Good | Good | Good |
| Resource efficiency | MEDIUM | Poor (1.3GB, 100 WS) | Good (200MB, 2 WS) | Best (0MB, 0 WS) |
| Maintenance burden | MEDIUM | High (sed patches) | High (custom engine) | Low (~400 lines) |
| Risk if it fails | HIGH | Known (current state) | High (money at risk) | Low (fallback to A) |
| Scales to 20 strategies | LOW (premature) | No | Yes | Yes |
| Focus on alpha vs plumbing | HIGH | Best | Worst | Good |

---

## Open Questions for Alberto

1. **Are we OK with 30-60s fill detection latency for X5/X8?** These are 1h strategies with 8h time limits. 30s seems fine, but you should confirm.

2. **Are we OK with exchange-managed TP/SL?** This means we can't do complex exit logic (e.g., scale out at multiple TP levels). X5/X8 use simple TP/SL/TL, so this works today. If future strategies need complex exits, those go through HB (Tier 1).

3. **Should we keep HB containers as a fallback?** Option C could run X5 in both the thin wrapper AND an HB container for 1 week (shadow mode). Compare fills. If wrapper matches or beats HB, kill the containers.

4. **Is the 50% TIME_LIMIT exit rate on X5 an alpha problem or an infra problem?** If it's infra (WS drops causing missed TP/SL), Option C fixes it by moving TP/SL to the exchange. If it's alpha (ATR exits too wide), Option C doesn't help.

5. **Cloud vs local:** With 300GB free disk and only needing 5-10GB for microstructure data, cloud is NOT needed for storage. The only cloud argument is uptime reliability (Bybit needs 24/7 connectivity). But we're paper trading -- a few hours of downtime costs nothing real. Defer cloud until we have a profitable strategy on real capital.
