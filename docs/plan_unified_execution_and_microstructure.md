# Unified Plan: Risk Management + Execution Fix + Microstructure Data (gated)
**Status:** APPROVED v4 (Apr 17, 2026) -- post CEO + Eng dual-voice review
**Supersedes:** `plan_infra_hardening.md` (P1-P2), `plan_hf_microstructure.md` (all), `plan_portfolio_risk_manager.md` (absorbed into Phase 1)
**Decision log:** `decision_execution_architecture.md` (Option A/B/C analysis)
**Review log:** CEO + Eng dual-voice (Claude subagent + Codex). Both recommended resequencing: alpha validation before infrastructure. Both flagged controller adapter gap as critical.

## Executive Summary

Alpha before infrastructure. Defensive risk controls before offensive data pipelines. Kill gates between phases.

Build order:
1. **Phase 0 (2h):** Stop the bleeding -- Colima, restart policy, kill 5-min loop
2. **Phase 1 (3-4 days):** Portfolio Risk Manager -- the "adult in the room" for 24 live pairs with zero aggregate controls
3. **Phase 2 (3-4 days):** Thin Execution Wrapper for X5 -- answers whether 50% TIME_LIMIT exits are alpha or infra
4. **Phase 3a (4h):** CVD EDA in Jupyter -- validates microstructure thesis with zero infrastructure
5. **KILL GATE:** If TIME_LIMIT unchanged AND CVD shows no edge, stop and rethink
6. **Phase 3b (1 week, GATED):** Microstructure data collection -- only if Phase 3a passes
7. **Phase 4 (1 week, GATED):** First microstructure strategy through governance

Total wall-clock if all gates pass: ~4 weeks. If gates fail: 2 weeks invested, capital protected by risk manager, informed decision on next direction.

## Key Design Decisions

### Exchange-Managed TP/SL
Bybit V5 API supports native `takeProfit` and `stopLoss` on order placement. These execute on Bybit's server, surviving our crashes, WS disconnections, and API restarts. This is MORE reliable than HB's WS-dependent PositionExecutor for slow strategies.

### MarketDataShim (eng review finding)
The wrapper can't call `controller.update_processed_data(df)` directly -- X5's method takes no arguments and reads from `self.market_data_provider.get_candles_df()` (HB runtime object). Solution: inject a `MarketDataShim` that implements the same interface, backed by parquet + MongoDB. ~50 lines, preserves controller compatibility.

### Atomic Risk Reservation (eng review finding)
Risk check and order placement are not atomic. Two signals can both pass a 60s-stale snapshot and oversubscribe. Solution: MongoDB `findAndModify` to reserve heat before order, release on cancel/failure, confirm on fill.

### Conviction in Sizing (eng review finding)
The formula `risk_budget = equity * risk_per_trade_pct * conviction_mult` directly scales the risk budget, contradicting the stated principle "conviction scales SIZE, not risk envelope." Resolution: rename the principle. Conviction DOES scale risk per trade within the portfolio heat cap. The portfolio heat cap (6%) is the hard envelope. Heat accounting uses conservative estimate: `qty * (stop_distance + fees + slippage)`.

## Dependency Graph

```
Day 1:    Phase 0 (infra P0, 2h) -- config fixes, stop the bleeding
  |
Day 1-4:  Phase 1 (portfolio risk manager, 3-4 days) -- DEFENSIVE
  |        X5 has 24 live pairs with zero portfolio-level risk controls
  |        Protects capital regardless of which strategies survive
  |
Day 3-6:  Phase 2 (thin wrapper for X5, 3-4 days) -- overlaps with Phase 1
  |        Answers: is 50% TIME_LIMIT rate an alpha or infra problem?
  |
Day 7-14: Observation period. X5 runs with risk controls + wrapper.
  |        Measure: TIME_LIMIT rate, fill quality, TP/SL reliability
  |
Day 7-8:  Phase 3a (CVD EDA, 4h, parallel during observation)
  |        Jupyter notebook on Binance aggTrades CSVs. Zero infra.
  |
  ===== KILL GATE (Day 14) =====
  TIME_LIMIT rate unchanged AND CVD no edge → STOP. Rethink.
  TIME_LIMIT drops → wrapper works. Apply to X8.
  CVD shows edge → proceed to Phase 3b.
  ===============================
  |
Day 15-21: Phase 3b (microstructure data collection, 1wk) -- GATED
  |
Day 22-28: Phase 4 (first microstructure strategy, 1wk) -- GATED
  |
Phase 5:   Multi-key rotation -- DEFERRED until 30+ strategies
```

**Serial, not parallel.** Solo developer. Kill gates prevent wasted work.

---

## Phase 0: Stop the Bleeding (2h)

- [ ] Increase Colima VM: 4GB to 5GB
- [ ] Add `restart: unless-stopped` to hummingbot-api in docker-compose.yml
- [ ] Kill 5-min restart loop: remove `StartInterval` from docker-stack plist, keep `RunAtLoad`
- [ ] Add healthcheck to hummingbot-api in docker-compose.yml
- [ ] Verify HB API stays up >1h without external restarts

---

## Phase 1: Portfolio Risk Manager (3-4 days)

**Goal: Portfolio-level risk controls for all live trading. The "adult in the room."**

### 1a. RiskStateTask (~4h)
New file: `app/tasks/risk_state_task.py`

Pipeline task (cron, every 60s). Fetches equity + all open positions from Bybit via `BybitExchangeClient`. Computes portfolio/strategy/pair heat. Writes single document to `portfolio_risk_state` in MongoDB.

```python
class RiskStateTask(BaseTask):
    """
    Reads exchange state, computes risk metrics, writes to MongoDB.
    Controllers and wrapper read this to make sizing/admission decisions.
    """
    async def execute(self):
        async with aiohttp.ClientSession() as session:
            positions = await self.exchange.fetch_positions(session)
            wallet = await self.exchange.fetch_wallet_balance(session)

        equity = wallet["equity"]
        risk_state = self._compute_risk_state(equity, positions)
        await self.db.portfolio_risk_state.replace_one(
            {"_id": "current"}, risk_state, upsert=True
        )
```

**Staleness handling (eng review fix):** If RiskStateTask can't reach Bybit for >5 minutes, set `circuit_breaker.triggered = True` with reason "stale_equity". All signal admission is blocked until fresh data arrives. Default to SAFE, not STALE.

### 1b. Portfolio Risk Manager (~4h)
New file: `app/services/portfolio_risk_manager.py`

Pure sizing functions (no I/O). Used by both wrapper and controllers.

```python
def compute_position_size(equity, sl_distance_pct, conviction_mult,
                          risk_per_trade_pct, price, max_leverage,
                          fee_rate=0.000375, slippage_bps=5) -> dict:
    """Risk-based position sizing with conservative heat accounting.

    Heat uses worst-case estimate: qty * (sl_distance + 2*fee_rate + slippage)
    This is MORE conservative than the idealized sl_distance_pct alone.
    """
    risk_budget = equity * risk_per_trade_pct * conviction_mult
    # Conservative: include round-trip fees + slippage in loss estimate
    effective_sl = sl_distance_pct + 2 * fee_rate + (slippage_bps / 10000)
    notional = risk_budget / effective_sl
    amount = notional / price
    leverage = min(max_leverage, max(1, math.ceil(notional / (equity * 0.9))))
    margin_needed = notional / leverage
    heat = amount * price * effective_sl  # conservative heat for portfolio tracking
    return {
        "notional": notional, "amount": amount, "leverage": leverage,
        "risk_amount": risk_budget, "heat": heat,
        "margin_needed": margin_needed, "effective_sl": effective_sl
    }

def check_admission(risk_state: dict, engine: str, pair: str,
                     proposed_heat: float) -> tuple[bool, str]:
    """Check if a new position is allowed under portfolio limits.
    Returns (allowed, reason).
    """
    cfg = risk_state["risk_config"]

    # Circuit breaker
    if risk_state.get("circuit_breaker", {}).get("triggered"):
        return False, f"circuit breaker: {risk_state['circuit_breaker']['reason']}"

    # Portfolio heat cap
    current = risk_state["portfolio_heat"]["total_risk_pct"]
    if current + (proposed_heat / risk_state["equity"]) > cfg["max_portfolio_heat_pct"]:
        return False, f"portfolio heat {current:.1%} + proposed would exceed {cfg['max_portfolio_heat_pct']:.0%}"

    # Strategy heat cap
    strat_heat = risk_state["portfolio_heat"]["by_strategy"].get(engine, {}).get("risk_pct", 0)
    if strat_heat + (proposed_heat / risk_state["equity"]) > cfg["max_strategy_heat_pct"]:
        return False, f"strategy {engine} heat would exceed {cfg['max_strategy_heat_pct']:.0%}"

    # Pair heat cap
    pair_heat = risk_state["portfolio_heat"]["by_pair"].get(pair, {}).get("risk_pct", 0)
    if pair_heat + (proposed_heat / risk_state["equity"]) > cfg["max_pair_heat_pct"]:
        return False, f"pair {pair} heat would exceed {cfg['max_pair_heat_pct']:.0%}"

    return True, "ok"
```

### 1c. Atomic Risk Reservation (~2h)
Add to `portfolio_risk_state` document:

```json
{
  "reservations": {
    "X5_BTC-USDT_SHORT_20260417": {
      "heat": 487.50,
      "reserved_at": "2026-04-17T12:00:00Z",
      "expires_at": "2026-04-17T12:05:00Z"
    }
  }
}
```

Before placing an order, the wrapper does:
```python
result = await db.portfolio_risk_state.find_one_and_update(
    {"_id": "current",
     f"reservations.{signal_id}": {"$exists": False}},
    {"$set": {f"reservations.{signal_id}": {
        "heat": proposed_heat,
        "reserved_at": datetime.utcnow(),
        "expires_at": datetime.utcnow() + timedelta(minutes=5)
    }}},
    return_document=True
)
if result is None:
    # Either duplicate signal or concurrent reservation conflict
    return
```

On fill confirmation: RiskStateTask incorporates into real heat, removes reservation.
On cancel/failure: wrapper removes reservation.
Expired reservations: RiskStateTask cleans up reservations older than 5 minutes.

### 1d. Risk Config in MongoDB (~1h)
```python
# Written once at deploy time, editable via CLI
risk_config = {
    "risk_per_trade_pct": 0.01,        # 1% equity at risk per trade
    "max_portfolio_heat_pct": 0.06,     # 6% max total risk
    "max_strategy_heat_pct": 0.03,      # 3% max per strategy
    "max_pair_heat_pct": 0.02,          # 2% max per pair across strategies
    "max_leverage": 5,
    "min_equity_pct": 0.80,             # circuit breaker at 80% of initial
    "enabled": True
}
```

### 1e. Pipeline Integration (~1h)
Add `risk_state_update` task to `hermes_pipeline.yml`:
```yaml
risk_state_update:
    enabled: true
    task_class: app.tasks.risk_state_task.RiskStateTask
    schedule:
        type: cron
        cron: "* * * * *"   # every minute
    config:
        fallback_capital: 100000
    tags: [infra, risk]
```

**Phase 1 exit criteria:**
- [ ] RiskStateTask running every 60s, writing fresh `portfolio_risk_state`
- [ ] Staleness >5min triggers circuit breaker (test by stopping Bybit requests)
- [ ] `check_admission()` correctly blocks when heat exceeds caps (unit test)
- [ ] Atomic reservation prevents double-booking (concurrent test)
- [ ] Conservative heat accounting includes fees + slippage

### Files

| File | Action | Lines (est.) |
|---|---|---|
| `app/tasks/risk_state_task.py` | NEW | ~120 |
| `app/services/portfolio_risk_manager.py` | NEW | ~100 |
| `config/hermes_pipeline.yml` | MODIFY | +15 |

---

## Phase 2: Thin Execution Wrapper + X5 Migration (3-4 days)

**Goal: Move X5 off HB containers. Answer the TIME_LIMIT question. Eliminate 50+ WS connections.**

### 2a. BybitExchangeClient Extensions (~4h)
Add to existing `app/services/bybit_exchange_client.py`:

```python
async def _post(self, session, path, body) -> dict:
    """Authenticated POST request. Same signing as _get."""
    ts = str(int(time.time() * 1000))
    recv_window = "5000"
    payload = json.dumps(body)
    signature = self._sign(ts, recv_window, payload)
    headers = self._headers(ts, recv_window, signature)
    headers["Content-Type"] = "application/json"

    url = f"{self.base_url}{path}"
    async with session.post(url, headers=headers, data=payload,
                            timeout=aiohttp.ClientTimeout(total=10)) as resp:
        data = await resp.json()

    ret_code = data.get("retCode", -1)
    if ret_code != 0:
        # Classified error handling (eng review fix)
        if ret_code == 110043:  # leverage not modified -- safe to ignore
            return data.get("result", {})
        raise BybitAPIError(ret_code, data.get("retMsg", ""), path)
    return data.get("result", {})

class BybitAPIError(Exception):
    """Typed error with retCode for caller classification."""
    def __init__(self, code, msg, path):
        self.code = code
        self.msg = msg
        self.path = path
        super().__init__(f"Bybit {path}: [{code}] {msg}")

async def place_order(self, session, pair, side, qty, order_type="Limit",
                      price=None, take_profit=None, stop_loss=None,
                      time_in_force="GTC", reduce_only=False) -> dict:
    """Place order with optional TP/SL (exchange-managed).
    Returns full order response including orderId and TP/SL confirmation.
    """
    body = {
        "category": CATEGORY,
        "symbol": _to_symbol(pair),
        "side": side,          # "Buy" or "Sell"
        "orderType": order_type,
        "qty": str(qty),
        "timeInForce": time_in_force,
        "reduceOnly": reduce_only,
    }
    if price: body["price"] = str(price)
    if take_profit: body["takeProfit"] = str(take_profit)
    if stop_loss: body["stopLoss"] = str(stop_loss)
    return await self._post(session, "/v5/order/create", body)

async def amend_order(self, session, pair, order_id=None,
                      stop_loss=None, take_profit=None) -> dict:
    body = {"category": CATEGORY, "symbol": _to_symbol(pair)}
    if order_id: body["orderId"] = order_id
    if stop_loss: body["stopLoss"] = str(stop_loss)
    if take_profit: body["takeProfit"] = str(take_profit)
    return await self._post(session, "/v5/order/amend", body)

async def cancel_order(self, session, pair, order_id) -> dict:
    return await self._post(session, "/v5/order/cancel", {
        "category": CATEGORY, "symbol": _to_symbol(pair), "orderId": order_id
    })

async def close_position(self, session, pair, side) -> dict:
    """Market close entire position."""
    return await self.place_order(session, pair, side="Sell" if side == "Buy" else "Buy",
                                  qty=0, order_type="Market", reduce_only=True)

async def fetch_open_orders(self, session) -> list[dict]:
    """Fetch ALL open orders in one call."""
    result = await self._get(session, "/v5/order/realtime", {"category": CATEGORY})
    return result.get("list", [])

async def set_leverage(self, session, pair, leverage) -> dict:
    """Set leverage. Ignores 110043 (already set)."""
    return await self._post(session, "/v5/position/set-leverage", {
        "category": CATEGORY, "symbol": _to_symbol(pair),
        "buyLeverage": str(leverage), "sellLeverage": str(leverage)
    })

async def set_tp_sl(self, session, pair, take_profit=None, stop_loss=None) -> dict:
    """Set/update TP/SL on existing position (standalone conditional orders)."""
    body = {"category": CATEGORY, "symbol": _to_symbol(pair)}
    if take_profit: body["takeProfit"] = str(take_profit)
    if stop_loss: body["stopLoss"] = str(stop_loss)
    return await self._post(session, "/v5/position/set-tpsl", body)
```

Note: `set_tp_sl` is the safety net. If `place_order` with TP/SL params gets accepted but TP/SL are silently dropped, the wrapper calls `set_tp_sl` as fallback.

### 2b. MarketDataShim (~3h)
New file: `app/services/market_data_shim.py` (~80 lines)

```python
class MarketDataShim:
    """
    Drop-in replacement for HB's market_data_provider.
    Provides get_candles_df() backed by parquet files + MongoDB derivatives.
    Injected into controllers so they run identically outside HB.
    """
    def __init__(self, candle_dir: str, mongo_db):
        self.candle_dir = candle_dir
        self.db = mongo_db
        self._cache = {}

    def get_candles_df(self, connector_name: str, trading_pair: str,
                       interval: str = "1h", max_records: int = 100) -> pd.DataFrame:
        """Load candles from parquet, merge derivatives from MongoDB.
        Returns DataFrame with columns matching HB's candle format:
        timestamp, open, high, low, close, volume, + derivatives columns.
        """
        # Load from parquet (same files CandlesDownloaderTask writes)
        pair_symbol = trading_pair.replace("-", "")
        path = Path(self.candle_dir) / f"{connector_name}_{pair_symbol}_{interval}.parquet"
        df = pd.read_parquet(path)
        df = df.tail(max_records).reset_index(drop=True)

        # Merge derivatives if available (same pattern as BulkBacktestTask)
        df = self._merge_derivatives(df, trading_pair)
        return df

    def time(self) -> float:
        """Current time in seconds (HB convention)."""
        return time.time()

    def get_price(self, connector_name: str, trading_pair: str) -> float:
        """Latest close price from candles."""
        df = self.get_candles_df(connector_name, trading_pair, max_records=1)
        return float(df["close"].iloc[-1]) if len(df) > 0 else 0.0

    def _merge_derivatives(self, df, trading_pair):
        """Merge funding_rate, oi_value, buy_ratio from MongoDB."""
        # Same logic as BulkBacktestTask._merge_derivatives_into_candles()
        # Reindexed to candle timestamps with ffill, neutral fill for NaN
        ...
```

**Key detail:** The shim loads parquet written by `CandlesDownloaderTask` (runs hourly). For X5 (1h signals), data is at most 1h stale. Acceptable.

### 2c. Thin Execution Wrapper (~8h)
New file: `app/services/thin_executor.py` (~450 lines)

```python
class ThinExecutor:
    """
    REST-only execution. Exchange-managed TP/SL. Zero WS connections.
    ~10 REST calls/min total at 1% rate limit utilization.
    """
    def __init__(self, strategies, exchange, risk_manager, db, shim):
        self.strategies = strategies
        self.exchange = exchange
        self.risk_mgr = risk_manager
        self.db = db
        self.shim = shim

    # ── Signal Loop ──────────────────────────────────────────

    async def signal_check(self):
        """Called every signal_interval by pipeline task."""
        async with aiohttp.ClientSession() as session:
            for strat in self.strategies:
                await self._check_strategy(session, strat)

    async def _check_strategy(self, session, strat):
        # 1. Inject shim into controller
        controller = strat.controller
        controller.market_data_provider = self.shim

        # 2. Run signal computation (same code as backtest/HB)
        await controller.update_processed_data()
        signal = controller.processed_data.get("signal", 0)
        if signal == 0:
            return

        # 3. Read risk state + check admission
        risk_state = await self.db.portfolio_risk_state.find_one({"_id": "current"})
        if not risk_state:
            logger.warning("No risk state available, suppressing signal")
            return

        exec_config = controller.get_executor_config(
            TradeType.BUY if signal == 1 else TradeType.SELL,
            Decimal(str(self.shim.get_price(strat.connector, strat.pair))),
            Decimal("0")  # amount computed by risk manager
        )

        sizing = compute_position_size(
            equity=risk_state["equity"],
            sl_distance_pct=float(exec_config.triple_barrier_config.stop_loss),
            conviction_mult=strat.conviction_mult,
            risk_per_trade_pct=risk_state["risk_config"]["risk_per_trade_pct"],
            price=float(exec_config.entry_price),
            max_leverage=risk_state["risk_config"]["max_leverage"],
        )

        allowed, reason = check_admission(
            risk_state, strat.engine, strat.pair, sizing["heat"]
        )
        if not allowed:
            logger.info(f"Signal suppressed: {strat.engine}/{strat.pair} — {reason}")
            return

        # 4. Atomic reservation (eng review fix)
        signal_id = f"{strat.engine}_{strat.pair}_{signal}_{int(time.time() // 3600)}"
        reserved = await self._reserve_heat(signal_id, sizing["heat"])
        if not reserved:
            return

        # 5. Set leverage + place order with TP/SL
        try:
            await self.exchange.set_leverage(session, strat.pair, sizing["leverage"])

            entry_price = float(exec_config.entry_price)
            tp_pct = float(exec_config.triple_barrier_config.take_profit)
            sl_pct = float(exec_config.triple_barrier_config.stop_loss)

            if signal == 1:  # LONG
                tp_price = round(entry_price * (1 + tp_pct), 2)
                sl_price = round(entry_price * (1 - sl_pct), 2)
                side = "Buy"
            else:  # SHORT
                tp_price = round(entry_price * (1 - tp_pct), 2)
                sl_price = round(entry_price * (1 + sl_pct), 2)
                side = "Sell"

            order = await self.exchange.place_order(
                session, pair=strat.pair, side=side,
                qty=sizing["amount"], order_type="Limit",
                price=entry_price,
                take_profit=tp_price, stop_loss=sl_price,
            )

            # 6. Verify TP/SL attached (eng review: naked position protection)
            order_id = order.get("orderId", "")
            await self._verify_tp_sl_or_protect(
                session, strat.pair, side, order_id, tp_price, sl_price
            )

            # 7. Record executor state
            await self._record_state(strat, signal_id, order, sizing,
                                     tp_price, sl_price, entry_price)

        except Exception as e:
            logger.error(f"Order failed {strat.engine}/{strat.pair}: {e}")
            await self._release_reservation(signal_id)

    async def _verify_tp_sl_or_protect(self, session, pair, side, order_id,
                                        tp_price, sl_price):
        """Eng review fix: verify TP/SL attached. If not, set standalone."""
        # Check order details
        orders = await self.exchange.fetch_open_orders(session)
        order = next((o for o in orders if o.get("orderId") == order_id), None)

        if order and (not order.get("takeProfit") or not order.get("stopLoss")):
            logger.warning(f"TP/SL not attached to order {order_id}, setting standalone")
            try:
                await self.exchange.set_tp_sl(session, pair, tp_price, sl_price)
            except Exception as e:
                logger.error(f"Failed to set standalone TP/SL: {e}. Closing position.")
                await self.exchange.close_position(session, pair, side)
                raise

    # ── Lifecycle Loop ───────────────────────────────────────

    async def lifecycle_check(self):
        """Called every 30s. Manages active positions."""
        async with aiohttp.ClientSession() as session:
            positions = await self.exchange.fetch_positions(session)
            tickers = await self._fetch_tickers(session)

            active = await self.db.executor_state.find(
                {"state": {"$in": ["PENDING", "ACTIVE"]}}
            ).to_list(100)

            for executor in active:
                await self._manage_executor(session, executor, positions, tickers)

    async def _manage_executor(self, session, executor, positions, tickers):
        pair = executor["pair"]
        state = executor["state"]

        has_position = any(p["pair"] == pair for p in positions)

        if state == "PENDING":
            if has_position:
                # Fill detected
                await self._transition(executor, "ACTIVE", "fill detected via position")
            elif time.time() - executor["signal_at"].timestamp() > 300:
                # 5min fill timeout
                try:
                    await self.exchange.cancel_order(session, pair, executor["entry_order_id"])
                except Exception:
                    pass
                await self._transition(executor, "CANCELLED", "fill timeout 5min")
                await self._release_reservation(executor["signal_id"])

        elif state == "ACTIVE":
            if not has_position:
                # Exchange closed it (TP or SL hit)
                close_type = await self._determine_close_type(executor)
                await self._transition(executor, close_type, "exchange closed")

            elif time.time() - executor["signal_at"].timestamp() > executor.get("time_limit_s", 28800):
                # Time limit
                await self.exchange.close_position(session, pair, executor["side"])
                await self._transition(executor, "TIME_LIMIT_CLOSE", "time limit reached")

            elif executor.get("trailing_enabled") and pair in tickers:
                # Trailing stop amend
                current_price = tickers[pair]
                new_sl = self._compute_trailing_sl(executor, current_price)
                if new_sl and new_sl != executor.get("current_sl"):
                    try:
                        await self.exchange.set_tp_sl(session, pair, stop_loss=new_sl)
                        await self.db.executor_state.update_one(
                            {"_id": executor["_id"]},
                            {"$set": {"current_sl": new_sl}}
                        )
                    except Exception as e:
                        logger.warning(f"Trail amend failed {pair}: {e}")
                        # Original SL still active on exchange — position protected

    # ── Restart Reconciliation (eng review fix) ──────────────

    async def reconcile_on_startup(self):
        """Bootstrap: exchange state is source of truth. Derive local state."""
        async with aiohttp.ClientSession() as session:
            positions = await self.exchange.fetch_positions(session)

        exchange_pairs = {p["pair"] for p in positions}
        active_executors = await self.db.executor_state.find(
            {"state": {"$in": ["PENDING", "ACTIVE"]}}
        ).to_list(100)
        executor_pairs = {e["pair"] for e in active_executors}

        # Case 1: Exchange has position, we don't track it
        for pair in exchange_pairs - executor_pairs:
            logger.warning(f"Reconciliation: untracked position {pair}. Creating executor.")
            pos = next(p for p in positions if p["pair"] == pair)
            await self._create_orphan_executor(pos)

        # Case 2: We track position, exchange doesn't have it (closed during downtime)
        for executor in active_executors:
            if executor["pair"] not in exchange_pairs and executor["state"] == "ACTIVE":
                close_type = await self._determine_close_type(executor)
                await self._transition(executor, close_type, "reconciled on restart")

        # Case 3: PENDING executor but position filled
        for executor in active_executors:
            if executor["state"] == "PENDING" and executor["pair"] in exchange_pairs:
                await self._transition(executor, "ACTIVE", "reconciled: fill during downtime")
```

### 2d. Executor State Collection
```
executor_state: {
    _id: ObjectId,
    signal_id: "X5_BTC-USDT_SHORT_20260417_12",
    engine: "X5",
    pair: "BTC-USDT",
    side: "Sell",
    state: "ACTIVE",       # PENDING|ACTIVE|TP_CLOSE|SL_CLOSE|TIME_LIMIT_CLOSE|CANCELLED|FAILED
    signal_at: ISODate,
    entry_order_id: "...",
    entry_price: 84521.5,
    qty: 0.0035,
    take_profit: 86212.0,
    stop_loss: 83399.0,
    current_sl: 83650.0,    # updated by trailing stop
    trailing_enabled: true,
    time_limit_s: 28800,
    leverage: 2,
    heat: 487.50,            # conservative heat (incl fees+slippage)
    conviction_mult: 1.5,
    state_history: [
        {state: "PENDING", at: ISODate, detail: "order placed"},
        {state: "ACTIVE", at: ISODate, detail: "fill detected"},
    ],
    pnl: null,
    close_type: null,
    closed_at: null
}
Indexes: (signal_id) unique. (engine, state). (pair, state).
```

### 2e. Pipeline Integration (~2h)
```yaml
thin_executor:
    enabled: true
    task_class: app.tasks.execution.thin_executor_task.ThinExecutorTask
    schedule:
        type: cron
        cron: "* * * * *"    # signal check every minute
    config:
        strategies: ["X5"]
        lifecycle_interval_s: 30
    tags: [execution]
```

### 2f. X5 Migration (~4h)
1. Deploy wrapper with X5 in **shadow mode** (log signals, don't execute) for 48h
2. Compare shadow signals vs HB container signals (timing, direction, frequency)
3. If match: enable wrapper execution, stop HB containers
4. Keep containers as rollback for 1 week
5. Measure TIME_LIMIT rate over 1 week (this is the critical metric)

**Phase 2 exit criteria:**
- [ ] `_post` method works, error codes classified (unit test against demo)
- [ ] MarketDataShim returns same DataFrame as HB's market_data_provider (integration test)
- [ ] Shadow mode signals match HB container signals for 48h
- [ ] TP/SL verified on every order (naked position protection working)
- [ ] Wrapper live: X5 trading for 48h+, fills detected, TP/SL working
- [ ] Reconciliation on restart handles all 3 cases (unit test)
- [ ] TIME_LIMIT rate measured (THE KEY METRIC)

### Files

| File | Action | Lines (est.) |
|---|---|---|
| `app/services/bybit_exchange_client.py` | MODIFY | +150 (_post, place/amend/cancel/set_tp_sl) |
| `app/services/market_data_shim.py` | NEW | ~80 |
| `app/services/thin_executor.py` | NEW | ~450 |
| `app/tasks/execution/thin_executor_task.py` | NEW | ~50 |
| `config/hermes_pipeline.yml` | MODIFY | +15 |

---

## Phase 3a: CVD EDA -- Validate Microstructure Thesis (4h, zero infra)

**Goal: Prove or kill the microstructure thesis before building any collection infrastructure.**

### Method
1. Download Binance aggTrades CSVs for top 5 pairs (BTC, ETH, SOL, XRP, DOGE), 30 days
2. Aggregate into 1-second trade bars (buy_volume, sell_volume, count)
3. Compute CVD (5min rolling), CVD slope, depth proxy (buy/sell ratio)
4. Forward-return analysis: does CVD slope predict 15/30/60min returns?
5. Regime breakdown: trend, range, shock periods
6. Cross-exchange proxy validation: compare Binance CVD with Bybit candle structure

### Kill Gate
- **No monotonic relationship** between CVD slope quintiles and forward returns → KILL. Don't build Phase 3b.
- **Signal-to-noise ratio** too low for fees + slippage → KILL.
- **Cross-exchange correlation** < 0.7 between Binance and Bybit → flag (backtest-to-prod gap concern, may still proceed with Bybit-only live data).

### Pass Gate
- **Monotonic forward return** relationship across quintiles
- **Top quintile return** > 2x trading costs (fees + slippage)
- Consistent across at least 3 of 5 pairs
- Not regime-specific (works in trend AND range)

---

## Phase 3b: Microstructure Data Collection (1 week, GATED on Phase 3a)

**Only build if Phase 3a EDA passes.** Full spec in prior version of this document (v2). Summary:

- Microstructure Collector: 3 WS connections (Bybit trades, Bybit orderbook, Binance aggTrades)
- 1-second trade bars + 5-second orderbook snapshots (TTL 7 days, ~8 GB steady state)
- MicrostructureFeatureTask: 1m aggregates for backtesting (CVD, imbalance, sweep count)
- Redis for real-time feature distribution
- LaunchDaemon for collector process
- Binance aggTrades historical backfill for backtesting

---

## Phase 4: First Microstructure Strategy (1 week, GATED on Phase 3b data accumulation)

**Only start after 7+ days of Phase 3b data.** Full governance pipeline per quant-governance skill.

Strategy candidate: X14 CVD Momentum (simplest signal, validated by Phase 3a EDA).

---

## Risk Assessment (updated post-review)

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| TP/SL silently dropped by Bybit demo | MEDIUM | Naked position | `_verify_tp_sl_or_protect()`: verify, set standalone, or close |
| set_leverage fails (already set) | HIGH | Signal loop crash | Classified error handling: ignore 110043 |
| Risk check race condition | LOW | Oversubscribed heat | Atomic MongoDB reservation with 5min TTL |
| Stale risk_state (Bybit unreachable) | MEDIUM | Signals fire with stale equity | Auto circuit breaker at >5min staleness |
| Controller adapter breaks HB compat | LOW | Can't run in both | MarketDataShim is additive, doesn't modify controller |
| Exchange closes position during lifecycle gap | MEDIUM | Stale heat tracking | 30s lifecycle loop + reconciliation on startup |
| Microstructure thesis has no edge | HIGH | Wasted infra work | Phase 3a EDA BEFORE Phase 3b infra (kill gate) |
| TIME_LIMIT rate unchanged by wrapper | MEDIUM | Alpha problem confirmed | Rethink strategy thesis, not execution |

## Success Metrics

| Metric | Current | After Phase 1+2 | Key Question Answered |
|---|---|---|---|
| Portfolio risk controls | NONE | Heat caps, circuit breaker, sizing | Capital protected |
| X5 TIME_LIMIT rate | ~50% | Measured | Infra or alpha? |
| WS connections (execution) | 50+ | 0 (pure REST) | — |
| Rate limit utilization | ~98% during storms | ~1% | — |
| Position orphan risk | HIGH | LOW (exchange TP/SL) | — |
| Naked position risk | Unknown | Verified + fallback | — |

## Decision Audit Trail

| # | Phase | Decision | Classification | Principle | Rationale |
|---|---|---|---|---|---|
| 1 | CEO | Resequence: risk mgr before data infra | USER CHALLENGE | P1 (completeness) | Both models: defensive before offensive |
| 2 | CEO | Add kill gate before microstructure infra | MECHANICAL | P3 (pragmatic) | 4h EDA validates thesis before 1wk infra |
| 3 | CEO | Serial not parallel for solo dev | MECHANICAL | P5 (explicit) | "Parallel" was misleading |
| 4 | Eng | Add MarketDataShim | MECHANICAL | P5 (explicit) | Controller adapter gap is critical |
| 5 | Eng | Atomic risk reservation | MECHANICAL | P1 (completeness) | Race condition between check and order |
| 6 | Eng | Classified error handling for _post | MECHANICAL | P1 (completeness) | set_leverage 110043 would crash signal loop |
| 7 | Eng | TP/SL verification + fallback | MECHANICAL | P1 (completeness) | Naked position is unacceptable risk |
| 8 | Eng | Reconciliation: exchange-first bootstrap | MECHANICAL | P5 (explicit) | Exchange is source of truth on restart |
| 9 | Eng | Conservative heat: fees + slippage | MECHANICAL | P3 (pragmatic) | Idealized SL underestimates real risk |
| 10 | CEO | Note VPS as alternative to explore | TASTE | P3 (pragmatic) | $12/mo solves stability; defer decision |
