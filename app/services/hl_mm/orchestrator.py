"""
Orchestrator — Main asyncio event loop (Spec Section 7).

Event flow:
  HL book WS -> trade WS -> Bybit WS -> feature cache
  -> pair state machine -> OMS -> fill/markout logger -> risk manager

Architecture:
  - One asyncio event loop
  - HL WS for L2 book + trades + order submission
  - Bybit WS for price anchor (public feed)
  - Per-pair actor pattern (via state machine)
  - OMS: one batch in flight per pair, cancel on disconnect, reconcile every 30s
  - Fill/quote logging to MongoDB

This is the top-level module. It creates and wires all components.
"""
import asyncio
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Optional

import eth_account
import websockets
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants

from pymongo import MongoClient

from .fair_value import FairValueEngine, AnchorTier
from .signal_engine import SignalEngine
from .quote_engine import QuoteEngine, QuoteConfig
from .inventory_manager import InventoryManager
from .fill_tracker import FillTracker, QuoteLog
from .risk_manager import RiskManager, RiskConfig, RiskAction
from .state_machine import StateMachine, PairContext, PairState
from .pair_screener import PairScreener, ScreenerConfig, BYBIT_PERPS

logger = logging.getLogger(__name__)

# Bybit public WS endpoint (linear perps)
BYBIT_WS_URL = "wss://stream.bybit.com/v5/public/linear"


class HLMarketMaker:
    """Shitcoin market making engine on Hyperliquid.

    Wires together: PairScreener, FairValueEngine, SignalEngine,
    QuoteEngine, InventoryManager, FillTracker, RiskManager, StateMachine.
    """

    def __init__(
        self,
        private_key: Optional[str] = None,
        address: Optional[str] = None,
        initial_coins: Optional[list[str]] = None,
        leverage: int = 5,
        mongo_uri: str = "mongodb://localhost:27017/quants_lab",
        dry_run: bool = False,
    ):
        # Credentials
        self.private_key = private_key or os.environ.get("HL_PRIVATE_KEY", "")
        self.address = address or os.environ.get("HL_ADDRESS", "")
        if not self.private_key or not self.address:
            raise ValueError("HL_PRIVATE_KEY and HL_ADDRESS required")

        self.leverage = leverage
        self.dry_run = dry_run
        self.mongo_uri = mongo_uri
        self._initial_coins = initial_coins or ["ORDI"]

        # SDK init (deferred to run() for async context)
        self.info: Optional[Info] = None
        self.exchange: Optional[Exchange] = None
        self._sz_decimals: dict[str, int] = {}

        # MongoDB
        client = MongoClient(mongo_uri)
        db_name = mongo_uri.split("/")[-1]
        self._db = client[db_name]
        self._fills_col = self._db["hl_mm_fills"]
        self._quotes_col = self._db["hl_mm_quote_logs"]

        # Components (initialized in _init_components)
        self.screener: Optional[PairScreener] = None
        self.fv_engine: Optional[FairValueEngine] = None
        self.signal_engine: Optional[SignalEngine] = None
        self.quote_engine: Optional[QuoteEngine] = None
        self.inventory: Optional[InventoryManager] = None
        self.fill_tracker: Optional[FillTracker] = None
        self.risk_manager: Optional[RiskManager] = None
        self.state_machine: Optional[StateMachine] = None

        # Runtime state
        self._running = False
        self._start_time = 0.0
        self._tick_count = 0
        self._active_coins: set[str] = set()
        self._bybit_ws_task: Optional[asyncio.Task] = None
        self._hl_ws_tasks: dict[str, asyncio.Task] = {}
        self._last_mongo_flush: float = 0.0
        self._prev_bybit_mids: dict[str, float] = {}

    # ==================================================================
    # Initialization
    # ==================================================================

    def _init_sdk(self) -> None:
        """Initialize HL SDK (makes REST calls, so not in __init__)."""
        wallet = eth_account.Account.from_key(self.private_key)
        api_url = constants.MAINNET_API_URL

        # skip_ws=False to enable WS subscriptions on Info
        self.info = Info(api_url, skip_ws=False)
        account_addr = self.address if wallet.address.lower() != self.address.lower() else None
        self.exchange = Exchange(wallet, api_url, account_address=account_addr)

        # Fetch metadata
        meta = self.info.meta()
        self._sz_decimals = {p["name"]: p["szDecimals"] for p in meta["universe"]}

    def _init_components(self) -> None:
        """Create all engine components."""
        # Bybit pairs map: HL coin -> Bybit symbol
        bybit_pairs = {coin: f"{coin}USDT" for coin in BYBIT_PERPS}

        self.screener = PairScreener(
            info=self.info,
            mongo_uri=self.mongo_uri,
            config=ScreenerConfig(max_live_pairs=2),
        )

        self.fv_engine = FairValueEngine(bybit_pairs=bybit_pairs)
        self.signal_engine = SignalEngine()
        self.quote_engine = QuoteEngine(
            exchange=self.exchange,
            info=self.info,
            address=self.address,
            sz_decimals=self._sz_decimals,
            dry_run=self.dry_run,
        )
        self.inventory = InventoryManager(info=self.info, address=self.address)
        self.fill_tracker = FillTracker()
        self.risk_manager = RiskManager()
        self.state_machine = StateMachine()

    # ==================================================================
    # Main event loop
    # ==================================================================

    async def run(self) -> None:
        """Main entry point. Blocks until shutdown."""
        logger.info(
            f"HL MM starting: coins={self._initial_coins} leverage={self.leverage} "
            f"dry_run={self.dry_run}"
        )

        # Init SDK (REST calls)
        try:
            await asyncio.to_thread(self._init_sdk)
        except Exception as e:
            logger.error(f"SDK init failed: {e}")
            return

        self._init_components()
        self._running = True
        self._start_time = time.time()

        # Set leverage for initial coins
        for coin in self._initial_coins:
            await self._set_leverage(coin)

        # Force initial coins to active (bypass screener shadow period)
        for coin in self._initial_coins:
            self.screener.force_active(coin)
            self._activate_coin(coin)

        # Start background tasks
        tasks = [
            asyncio.create_task(self._main_loop(), name="main_loop"),
            asyncio.create_task(self._screener_loop(), name="screener_loop"),
            asyncio.create_task(self._bybit_ws_loop(), name="bybit_ws"),
            asyncio.create_task(self._mongo_flush_loop(), name="mongo_flush"),
        ]

        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            logger.info("Shutdown requested")
        except Exception as e:
            logger.error(f"Fatal: {e}", exc_info=True)
        finally:
            await self._shutdown()

    async def _main_loop(self) -> None:
        """Core tick loop. Runs every ~500ms."""
        while self._running:
            try:
                await self._tick()
            except Exception as e:
                logger.error(f"Tick error: {e}", exc_info=True)
            await asyncio.sleep(0.5)

    async def _tick(self) -> None:
        """Single iteration of the main loop.

        Order: sync positions -> risk check -> per-pair (signal -> FV -> state
        machine -> quote -> execute) -> fill detection -> logging.
        """
        self._tick_count += 1
        now = time.time()

        # === STEP 1: Sync positions from exchange ===
        try:
            snapshot = await asyncio.wait_for(
                asyncio.to_thread(self.inventory.sync_positions), timeout=3.0
            )
        except (asyncio.TimeoutError, Exception) as e:
            logger.warning(f"Position sync failed: {e}")
            snapshot = None

        # === STEP 2: Portfolio risk check ===
        hl_book_ages = {}
        has_inventory = {}
        for coin in self._active_coins:
            sig = self.signal_engine.get_signal(coin)
            hl_book_ages[coin] = sig.book_age_ms if sig else 999999.0
            pos = self.inventory.get_position(coin)
            has_inventory[coin] = abs(pos.notional_usd) > 1.0

        risk_state = self.risk_manager.evaluate(
            daily_pnl=snapshot.daily_pnl if snapshot else 0,
            gross_notional=snapshot.total_gross_notional if snapshot else 0,
            net_exposure=snapshot.total_net_exposure if snapshot else 0,
            live_pair_count=len(self._active_coins),
            hl_book_ages_ms=hl_book_ages,
            has_inventory=has_inventory,
        )

        # Handle risk actions
        if RiskAction.HARD_STOP in risk_state.actions:
            logger.critical(f"HARD STOP: {risk_state.reason}")
            await self._emergency_shutdown(risk_state.reason)
            return

        if RiskAction.DAILY_STOP in risk_state.actions:
            logger.warning(f"DAILY STOP: {risk_state.reason}")
            self.quote_engine.cancel_all()
            return

        if RiskAction.CANCEL_ALL_QUOTES in risk_state.actions:
            self.quote_engine.cancel_all()

        if RiskAction.PAUSE_ALL in risk_state.actions:
            for coin in self._active_coins:
                self.state_machine.force_pause(coin, 900, "correlation stop")

        # Funding avoidance
        funding_window = self.risk_manager.is_funding_avoidance_window()

        # === STEP 3: Per-pair processing ===
        current_mids: dict[str, float] = {}

        for coin in list(self._active_coins):
            # Skip if circuit breaker disabled this pair
            if self.fill_tracker.is_pair_disabled(coin):
                self.quote_engine.cancel_coin(coin)
                continue

            # Get signal
            signal = self.signal_engine.get_signal(coin)
            if not signal or not signal.book:
                continue

            book = signal.book
            current_mids[coin] = book.mid

            # Compute fair value
            self.fv_engine.update_ofi(coin, signal.ofi_bps)
            fv_est = self.fv_engine.compute(
                coin=coin,
                hl_bid=book.best_bid,
                hl_ask=book.best_ask,
                bid_qty_top1=book.bid_qty_top1,
                ask_qty_top1=book.ask_qty_top1,
            )
            if not fv_est:
                continue

            fair_value = fv_est.fair_value

            # Compute reservation price
            pos = self.inventory.get_position(coin)
            reservation = self.inventory.compute_reservation_price(
                coin=coin,
                fair_value=fair_value,
                sigma_1s=signal.sigma_1s,
                rv_30s=signal.rv_30s,
            )

            # Get inventory limits
            limits = self.inventory.get_limits(coin)
            inv_age = self.inventory.get_inventory_age_s(coin)

            # Build state machine context
            edge_room = (book.spread_bps / 2.0) - 1.44  # maker fee
            from .quote_engine import DEFAULT_TOX_BUFFERS
            tox = DEFAULT_TOX_BUFFERS.get(coin, 1.0)
            bid_ev = edge_room - tox > 0
            ask_ev = edge_room - tox > 0

            ctx = PairContext(
                hl_book_fresh=not signal.is_stale,
                bybit_anchor_healthy=fv_est.anchor_weight > 0,
                hl_book_age_ms=signal.book_age_ms,
                spread_threshold_met=book.spread_bps > 2 * (1.44 + tox + 1.0),
                bid_side_ev_positive=bid_ev and not (funding_window and pos.size > 0),
                ask_side_ev_positive=ask_ev and not (funding_window and pos.size < 0),
                inventory_usd=pos.size * fair_value if fair_value > 0 else 0,
                q_soft=limits.q_soft,
                q_hard=limits.q_hard,
                q_emergency=limits.q_emergency,
                inventory_age_s=inv_age,
                adverse_move_bps=pos.adverse_move_bps,
                circuit_breaker_active=signal.any_toxic_flag and signal.anchor_jump_detected,
                oms_mismatch=False,
                regime_shock=RiskAction.PAUSE_ALL in risk_state.actions,
                strong_imbalance=signal.strong_imbalance,
                imbalance_side=signal.imbalance_side,
                hedge_in_progress=False,
                bybit_hedge_available=coin in BYBIT_PERPS,
            )

            # Run state machine
            state_info = self.state_machine.transition(coin, ctx)

            # Compute and execute quotes
            if state_info.state in (PairState.QUOTING_BOTH, PairState.QUOTING_ONE_SIDE,
                                     PairState.INVENTORY_EXIT):
                if self.quote_engine.should_requote(coin, fair_value):
                    vol_scale = 0.5 if risk_state.halve_sizes else 1.0
                    anchor_scale = 1.0 if fv_est.anchor_weight > 0.3 else 0.7

                    quotes = self.quote_engine.compute_quotes(
                        coin=coin,
                        fair_value=fair_value,
                        reservation_price=reservation,
                        hl_bid=book.best_bid,
                        hl_ask=book.best_ask,
                        depth20_bid_usd=book.bid_usd_top20,
                        depth20_ask_usd=book.ask_usd_top20,
                        free_equity_usd=self.inventory.get_free_equity(),
                        q_soft=limits.q_soft,
                        inventory_usd=ctx.inventory_usd,
                        sigma_1s=signal.sigma_1s,
                        vol_scale=vol_scale,
                        anchor_scale=anchor_scale,
                        quote_bid=state_info.quote_bid,
                        quote_ask=state_info.quote_ask,
                        exit_mode=state_info.exit_mode,
                    )

                    if quotes:
                        if not self.dry_run:
                            await asyncio.to_thread(self.quote_engine.execute_quotes, quotes)
                        else:
                            self.quote_engine.execute_quotes(quotes)

                        # Log quote decision
                        bid_o, ask_o = self.quote_engine.get_active_orders(coin)
                        self.fill_tracker.log_quote(QuoteLog(
                            coin=coin,
                            timestamp=now,
                            state=state_info.state.value,
                            fair_value=fair_value,
                            reservation_price=reservation,
                            hl_bid=book.best_bid,
                            hl_ask=book.best_ask,
                            spread_bps=book.spread_bps,
                            depth_bid_usd=book.bid_usd_top20,
                            depth_ask_usd=book.ask_usd_top20,
                            microprice=book.microprice,
                            anchor_mid=fv_est.anchored_mid,
                            imbalance_z=signal.imbalance_z,
                            bid_price=quotes.bid.price if quotes.bid else None,
                            ask_price=quotes.ask.price if quotes.ask else None,
                            bid_oid=bid_o.oid if bid_o else None,
                            ask_oid=ask_o.oid if ask_o else None,
                        ))
            else:
                # Not quoting: cancel any existing orders
                self.quote_engine.cancel_coin(coin)

            # Detect fills
            if not self.dry_run:
                try:
                    fills = await asyncio.to_thread(self.quote_engine.detect_fills, coin)
                    for fill in fills:
                        self._handle_fill(coin, fill)
                except Exception as e:
                    logger.warning(f"Fill detection error for {coin}: {e}")

        # === STEP 4: Update markouts ===
        self.fill_tracker.update_markouts(current_mids)

        # === STEP 5: Orphan cleanup (every 30s) ===
        if self._tick_count % 60 == 0:
            if not self.dry_run:
                await asyncio.to_thread(self.quote_engine.cleanup_orphans)

        # === STEP 6: Status log (every 60s) ===
        if self._tick_count % 120 == 0:
            self._log_status()

    # ==================================================================
    # Fill handling
    # ==================================================================

    def _handle_fill(self, coin: str, fill: dict) -> None:
        """Process a detected fill."""
        side = fill["side"]
        price = fill["price"]
        size = fill["size"]
        fee = fill.get("fee", 0)
        oid = fill.get("oid", 0)

        self.inventory.record_fill(coin, side, price, size)
        self.fill_tracker.record_fill(
            coin=coin, side=side, price=price, size=size,
            size_usd=size * price, fee=fee, oid=oid,
        )

        # Decay widen ticks on non-toxic recent history
        tox = self.fill_tracker.get_toxicity(coin)
        if tox.total_fills > 0 and tox.toxic_fills / tox.total_fills < 0.3:
            self.fill_tracker.decay_widen(coin)

        logger.info(
            f"FILL: {coin} {side} {size:.6f} @ ${price:.6f} "
            f"(fee=${fee:.4f}, total_fills={tox.total_fills})"
        )

    # ==================================================================
    # Pair management
    # ==================================================================

    def _activate_coin(self, coin: str) -> None:
        """Add a coin to active quoting."""
        if coin in self._active_coins:
            return

        self._active_coins.add(coin)
        self.state_machine.register_pair(coin)

        # Set anchor tier
        if coin in BYBIT_PERPS:
            self.fv_engine.set_tier(coin, AnchorTier.DIRECT)
        else:
            self.fv_engine.set_tier(coin, AnchorTier.SYNTHETIC)

        # Subscribe to HL WS for this coin
        self._subscribe_hl_ws(coin)

        logger.info(f"Activated coin: {coin}")

    def _deactivate_coin(self, coin: str) -> None:
        """Remove a coin from active quoting."""
        self.quote_engine.cancel_coin(coin)
        self.state_machine.unregister_pair(coin)
        self._active_coins.discard(coin)
        self._unsubscribe_hl_ws(coin)
        logger.info(f"Deactivated coin: {coin}")

    # ==================================================================
    # HL WebSocket subscriptions
    # ==================================================================

    def _subscribe_hl_ws(self, coin: str) -> None:
        """Subscribe to HL WS L2 book + trades for a coin."""
        if not self.info:
            return

        try:
            self.info.subscribe(
                {"type": "l2Book", "coin": coin},
                lambda data: self._on_hl_book(coin, data),
            )
            self.info.subscribe(
                {"type": "trades", "coin": coin},
                lambda data: self._on_hl_trades(coin, data),
            )
            logger.debug(f"Subscribed HL WS: {coin} L2 + trades")
        except Exception as e:
            logger.error(f"HL WS subscribe failed for {coin}: {e}")

    def _unsubscribe_hl_ws(self, coin: str) -> None:
        """Unsubscribe from HL WS for a coin."""
        if not self.info:
            return
        try:
            self.info.unsubscribe({"type": "l2Book", "coin": coin}, None)
            self.info.unsubscribe({"type": "trades", "coin": coin}, None)
        except Exception:
            pass

    def _on_hl_book(self, coin: str, data: dict) -> None:
        """Callback for HL WS L2 book updates."""
        try:
            # HL WS sends the full book in data["levels"]
            if isinstance(data, dict) and "levels" in data:
                self.signal_engine.update_book(coin, data)
            elif isinstance(data, dict) and "data" in data:
                self.signal_engine.update_book(coin, data["data"])
        except Exception as e:
            logger.debug(f"HL book parse error for {coin}: {e}")

    def _on_hl_trades(self, coin: str, data: dict) -> None:
        """Callback for HL WS trade updates."""
        try:
            trades = []
            if isinstance(data, list):
                trades = data
            elif isinstance(data, dict) and "data" in data:
                trades = data["data"]
            elif isinstance(data, dict):
                trades = [data]

            if trades:
                self.signal_engine.update_trades(coin, trades)
        except Exception as e:
            logger.debug(f"HL trade parse error for {coin}: {e}")

    # ==================================================================
    # Bybit WebSocket
    # ==================================================================

    async def _bybit_ws_loop(self) -> None:
        """Maintain Bybit WS connection for price anchoring.

        Subscribes to tickers for all coins in BYBIT_PERPS that we might trade.
        """
        # Build subscription list: all potentially active pairs
        coins_to_sub = list(BYBIT_PERPS & (self._active_coins | set(self._initial_coins)))
        # Always include BTC/ETH for correlation stop
        for c in ["BTC", "ETH", "SOL"]:
            if c not in coins_to_sub:
                coins_to_sub.append(c)

        symbols = [f"{c}USDT" for c in coins_to_sub]
        coin_map = {f"{c}USDT": c for c in coins_to_sub}

        subscribe_msg = {
            "op": "subscribe",
            "args": [f"tickers.{s}" for s in symbols],
        }

        while self._running:
            try:
                async with websockets.connect(
                    BYBIT_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info(f"Bybit WS connected, subscribed to {len(symbols)} tickers")

                    async for raw_msg in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw_msg)
                            topic = msg.get("topic", "")
                            data = msg.get("data", {})

                            if topic.startswith("tickers.") and data:
                                symbol = data.get("symbol", "")
                                coin = coin_map.get(symbol)
                                if not coin:
                                    continue

                                bid1 = float(data.get("bid1Price", 0) or 0)
                                ask1 = float(data.get("ask1Price", 0) or 0)

                                if bid1 > 0 and ask1 > 0:
                                    prev_mid = self._prev_bybit_mids.get(coin, 0)
                                    new_mid = (bid1 + ask1) / 2.0

                                    self.fv_engine.update_bybit_ticker(coin, bid1, ask1)
                                    self.risk_manager.update_reference_prices(
                                        btc_mid=new_mid if coin == "BTC" else 0,
                                        eth_mid=new_mid if coin == "ETH" else 0,
                                    )

                                    # Check anchor jump
                                    if prev_mid > 0:
                                        self.signal_engine.check_anchor_jump(
                                            coin, new_mid, prev_mid
                                        )
                                    self._prev_bybit_mids[coin] = new_mid

                        except Exception as e:
                            logger.debug(f"Bybit WS parse error: {e}")

            except websockets.exceptions.ConnectionClosed:
                logger.warning("Bybit WS disconnected, reconnecting in 5s")
            except Exception as e:
                logger.warning(f"Bybit WS error: {e}, reconnecting in 5s")

            await asyncio.sleep(5.0)

    # ==================================================================
    # Screener loop
    # ==================================================================

    async def _screener_loop(self) -> None:
        """Run pair screener every 15 minutes."""
        # Wait for initial data collection
        await asyncio.sleep(30)

        while self._running:
            try:
                if self.screener.should_rescan():
                    rankings = await self.screener.scan()

                    # Sync active coins with screener
                    screener_active = self.screener.active_pairs
                    for coin in screener_active - self._active_coins:
                        if self.risk_manager.can_add_pair(len(self._active_coins)):
                            await self._set_leverage(coin)
                            self._activate_coin(coin)

                    # Note: we don't auto-deactivate here -- deactivation requires
                    # inventory exit first, handled by state machine
            except Exception as e:
                logger.error(f"Screener error: {e}", exc_info=True)

            await asyncio.sleep(60)  # check every minute if rescan needed

    # ==================================================================
    # MongoDB persistence
    # ==================================================================

    async def _mongo_flush_loop(self) -> None:
        """Periodically flush fill and quote logs to MongoDB."""
        while self._running:
            await asyncio.sleep(30)
            try:
                # Flush fills
                fill_docs = self.fill_tracker.fills_to_mongo()
                if fill_docs:
                    self._fills_col.insert_many(fill_docs, ordered=False)

                # Flush recent quote logs
                log_docs = self.fill_tracker.quote_logs_to_mongo(
                    since=self._last_mongo_flush
                )
                if log_docs:
                    self._quotes_col.insert_many(log_docs, ordered=False)

                self._last_mongo_flush = time.time()
            except Exception as e:
                logger.warning(f"MongoDB flush error: {e}")

    # ==================================================================
    # Leverage management
    # ==================================================================

    async def _set_leverage(self, coin: str) -> None:
        """Set leverage for a coin on HL."""
        if self.dry_run:
            logger.info(f"[DRY] Would set {coin} leverage to {self.leverage}x")
            return
        try:
            result = await asyncio.to_thread(
                self.exchange.update_leverage, self.leverage, coin, True
            )
            logger.info(f"Set {coin} leverage to {self.leverage}x: {result}")
        except Exception as e:
            logger.warning(f"Failed to set leverage for {coin}: {e}")

    # ==================================================================
    # Shutdown
    # ==================================================================

    async def _emergency_shutdown(self, reason: str) -> None:
        """Emergency: cancel all orders, flatten positions, stop."""
        logger.critical(f"EMERGENCY SHUTDOWN: {reason}")
        self.quote_engine.cancel_all()

        if not self.dry_run:
            for coin in list(self._active_coins):
                pos = self.inventory.get_position(coin)
                if abs(pos.size) > 0:
                    try:
                        result = await asyncio.to_thread(
                            self.exchange.market_close, coin
                        )
                        logger.info(f"Market close {coin}: {result}")
                    except Exception as e:
                        logger.error(f"Market close failed for {coin}: {e}")

        self._running = False

    async def _shutdown(self) -> None:
        """Graceful shutdown."""
        logger.info("Shutting down...")
        self._running = False

        # Cancel all quotes
        if self.quote_engine:
            self.quote_engine.cancel_all()

        # Disconnect WS
        if self.info:
            try:
                self.info.disconnect_websocket()
            except Exception:
                pass

        # Final MongoDB flush
        try:
            fill_docs = self.fill_tracker.fills_to_mongo()
            if fill_docs:
                self._fills_col.insert_many(fill_docs, ordered=False)
        except Exception:
            pass

        logger.info("Shutdown complete")

    def stop(self) -> None:
        """Request graceful stop from outside the event loop."""
        self._running = False

    # ==================================================================
    # Status logging
    # ==================================================================

    def _log_status(self) -> None:
        """Periodic status log."""
        uptime = time.time() - self._start_time
        snapshot = self.inventory._get_snapshot()

        logger.info(
            f"STATUS: uptime={uptime/60:.0f}m ticks={self._tick_count} "
            f"coins={list(self._active_coins)} "
            f"gross=${snapshot.total_gross_notional:.2f} "
            f"net=${snapshot.total_net_exposure:.2f} "
            f"pnl=${snapshot.daily_pnl:.2f} "
            f"equity=${self.inventory._equity:.2f}"
        )

        for coin in self._active_coins:
            pos = self.inventory.get_position(coin)
            state = self.state_machine.get_state(coin)
            tox = self.fill_tracker.get_toxicity(coin)
            sig = self.signal_engine.get_signal(coin)

            state_str = state.state.value if state else "?"
            age_s = self.inventory.get_inventory_age_s(coin)

            logger.info(
                f"  {coin}: state={state_str} "
                f"pos={pos.size:.6f} (${pos.notional_usd:.2f}) "
                f"age={age_s:.0f}s adverse={pos.adverse_move_bps:.1f}bps "
                f"fills={tox.total_fills} toxic={tox.toxic_fills} "
                f"spread={sig.book.spread_bps:.1f}bps"
                if sig and sig.book else f"  {coin}: state={state_str} NO DATA"
            )
