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
import hashlib
import hmac
import json
import logging
import math
import os
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Optional

import eth_account
import requests as sync_requests
import websockets
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants

from pymongo import MongoClient, UpdateOne

from .config import HLMMConfig, load_config
from .fair_value import FairValueEngine, AnchorTier
from .signal_engine import SignalEngine
from .quote_engine import QuoteEngine, QuoteConfig
from .inventory_manager import InventoryManager
from .fill_tracker import FillTracker, QuoteLog
from .risk_manager import RiskManager, RiskConfig, RiskAction
from .state_machine import StateMachine, PairContext, PairState
from .pair_screener import PairScreener, ScreenerConfig, BYBIT_PERPS
from .notifier import TelegramNotifier

logger = logging.getLogger(__name__)

# HL REST API for order/position queries
HL_INFO_API = "https://api.hyperliquid.xyz/info"
HL_ADDRESS = "0x11ca20aeb7cd014cf8406560ae405b12601994b4"

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
        config: Optional[HLMMConfig] = None,
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
        self.config = config or load_config()

        # Bybit hedge credentials
        self._bybit_api_key = os.environ.get("BYBIT_API_KEY", "")
        self._bybit_api_secret = os.environ.get("BYBIT_API_SECRET", "")

        # SDK init (deferred to run() for async context)
        self.info: Optional[Info] = None
        self.exchange: Optional[Exchange] = None
        self._sz_decimals: dict[str, int] = {}

        # MongoDB
        client = MongoClient(mongo_uri)
        db_name = mongo_uri.split("/")[-1]
        self._db = client[db_name]
        self._fills_col = self._db["hl_mm_fills"]
        self._quotes_col = self._db["hl_mm_quote_log"]

        # Components (initialized in _init_components)
        self.screener: Optional[PairScreener] = None
        self.fv_engine: Optional[FairValueEngine] = None
        self.signal_engine: Optional[SignalEngine] = None
        self.quote_engine: Optional[QuoteEngine] = None
        self.inventory: Optional[InventoryManager] = None
        self.fill_tracker: Optional[FillTracker] = None
        self.risk_manager: Optional[RiskManager] = None
        self.state_machine: Optional[StateMachine] = None
        self.notifier: Optional[TelegramNotifier] = None

        # Runtime state
        self._running = False
        self._shutting_down = False
        self._start_time = 0.0
        self._tick_count = 0
        self._active_coins: set[str] = set()
        self._bybit_ws_task: Optional[asyncio.Task] = None
        self._hl_ws_tasks: dict[str, asyncio.Task] = {}
        self._last_mongo_flush: float = 0.0
        self._last_fill_poll: float = 0.0
        self._known_fill_hashes: set[str] = set()
        self._prev_bybit_mids: dict[str, float] = {}
        self._bybit_anchor_stale: bool = False
        self._bybit_ws_backoff: float = 1.0
        self._last_daily_summary: float = 0.0

        # Bug #3: Bybit hedge position tracking {coin: delta_size}
        self._hedge_positions: dict[str, float] = {}
        self._hedge_entry_prices: dict[str, float] = {}
        self._hedge_in_progress: dict[str, float] = {}  # coin -> timestamp when hedge placed

        # Bug #8: Shared rate limiter (token bucket: max 4/sec, burst 6)
        self._rate_tokens: float = 6.0
        self._rate_max: float = 6.0
        self._rate_refill_per_sec: float = 4.0
        self._rate_last_refill: float = 0.0

        # Bug #12: asyncio.Lock for signal state access
        self._signal_lock = asyncio.Lock()

        # Bug #13: Pair fill counts for lifecycle gating
        self._pair_fill_counts: dict[str, int] = {}

        # Bug #14: Sticky daily stop — tracks the UTC date when stopped
        self._daily_stop_sticky: bool = False
        self._daily_stop_date: Optional[object] = None  # date object

    # ==================================================================
    # Bug #8: Rate limiter
    # ==================================================================

    def _consume_rate_token(self, priority: bool = False) -> bool:
        """Try to consume a rate limit token. Returns True if allowed.

        Args:
            priority: If True (cancel/hedge), always allow but still deduct.
        """
        now = time.time()
        elapsed = now - self._rate_last_refill if self._rate_last_refill > 0 else 0
        self._rate_last_refill = now

        # Refill tokens
        self._rate_tokens = min(
            self._rate_max,
            self._rate_tokens + elapsed * self._rate_refill_per_sec,
        )

        if priority:
            self._rate_tokens = max(0, self._rate_tokens - 1)
            return True

        if self._rate_tokens >= 1.0:
            self._rate_tokens -= 1.0
            return True

        return False

    # ==================================================================
    # Bug #14: Sticky daily stop check
    # ==================================================================

    def _check_daily_stop_sticky(self) -> bool:
        """Check if daily stop is active. Sticky until next UTC day."""
        from datetime import date
        today = datetime.now(timezone.utc).date()
        if self._daily_stop_sticky and self._daily_stop_date == today:
            return True
        if self._daily_stop_sticky and self._daily_stop_date != today:
            self._daily_stop_sticky = False
            self._daily_stop_date = None
            logger.info("New UTC day: sticky daily stop cleared")
        return False

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
            config=ScreenerConfig(max_live_pairs=self.config.risk.max_live_pairs),
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
        self.notifier = TelegramNotifier(
            min_interval_s=self.config.telegram.min_message_interval_s,
            enabled=self.config.telegram.enabled,
        )

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

        # === STARTUP RECONCILIATION (Gap 2 + 3) ===
        # Cancel any stale open orders from prior sessions
        await self._reconcile_open_orders_on_startup()
        # Load existing positions into inventory manager
        await self._sync_positions_on_startup()

        # Set leverage for initial coins
        for coin in self._initial_coins:
            await self._set_leverage(coin)

        # Force initial coins to active (bypass screener shadow period)
        for coin in self._initial_coins:
            self.screener.force_active(coin)
            self._activate_coin(coin)

        # Bug #2: If startup reconciliation couldn't verify clean state, pause all coins
        if getattr(self, '_startup_pause_required', False):
            for coin in self._initial_coins:
                self.state_machine.force_pause(coin, 60, "startup: unverified order state")
            logger.warning("All coins starting in PAUSE due to unverified startup state")

        # Notify engine start
        self.notifier.notify_engine_event(
            "STARTED",
            f"Coins: {self._initial_coins}, Leverage: {self.leverage}x, "
            f"Dry run: {self.dry_run}",
        )

        # Start background tasks
        tasks = [
            asyncio.create_task(self._main_loop(), name="main_loop"),
            asyncio.create_task(self._screener_loop(), name="screener_loop"),
            asyncio.create_task(self._bybit_ws_loop(), name="bybit_ws"),
            asyncio.create_task(self._mongo_flush_loop(), name="mongo_flush"),
            asyncio.create_task(self._fill_poll_loop(), name="fill_poll"),
            asyncio.create_task(self._daily_summary_loop(), name="daily_summary"),
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

        Order: sync positions -> daily stop check -> risk check ->
        HEDGE_IMMEDIATELY check -> per-pair (signal -> FV -> state machine ->
        portfolio check -> quote -> execute) -> fill detection -> logging.
        """
        self._tick_count += 1
        now = time.time()

        # Bug #14: Sticky daily stop — check at TOP of every tick
        if self._check_daily_stop_sticky():
            self.quote_engine.cancel_all()
            return

        # === STEP 1: Sync positions from exchange ===
        if self._consume_rate_token():
            try:
                snapshot = await asyncio.wait_for(
                    asyncio.to_thread(self.inventory.sync_positions), timeout=3.0
                )
            except (asyncio.TimeoutError, Exception) as e:
                logger.warning(f"Position sync failed: {e}")
                snapshot = None
        else:
            snapshot = None

        # Bug #14: Include Bybit hedge PnL in daily PnL
        bybit_hedge_pnl = self._compute_bybit_hedge_pnl()
        effective_daily_pnl = (snapshot.daily_pnl if snapshot else 0) + bybit_hedge_pnl

        # === STEP 2: Portfolio risk check ===
        hl_book_ages = {}
        has_inventory = {}
        async with self._signal_lock:  # Bug #12: lock for signal reads
            for coin in self._active_coins:
                sig = self.signal_engine.get_signal(coin)
                hl_book_ages[coin] = sig.book_age_ms if sig else 999999.0
                pos = self.inventory.get_position(coin)
                has_inventory[coin] = abs(pos.notional_usd) > 1.0

        risk_state = self.risk_manager.evaluate(
            daily_pnl=effective_daily_pnl,
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
            # Bug #14: Make daily stop sticky
            self._daily_stop_sticky = True
            self._daily_stop_date = datetime.now(timezone.utc).date()
            self.quote_engine.cancel_all()
            return

        # Bug #4: Handle HEDGE_IMMEDIATELY BEFORE state machine pause.
        # If risk manager says hedge immediately and we have inventory, do it now
        # regardless of state machine state.
        if RiskAction.HEDGE_IMMEDIATELY in risk_state.actions and not self.dry_run:
            for coin in list(self._active_coins):
                if has_inventory.get(coin, False) and coin in BYBIT_PERPS:
                    pos = self.inventory.get_position(coin)
                    sig = self.signal_engine.get_signal(coin)
                    fv = sig.book.mid if sig and sig.book else 0
                    if abs(pos.size) > 0 and fv > 0:
                        logger.warning(
                            f"HEDGE_IMMEDIATELY: {coin} stale HL data with inventory, "
                            f"hedging before state machine runs"
                        )
                        await self._execute_bybit_hedge(coin, pos, fv)

        if RiskAction.CANCEL_ALL_QUOTES in risk_state.actions:
            self.quote_engine.cancel_all()

        if RiskAction.PAUSE_ALL in risk_state.actions:
            for coin in self._active_coins:
                self.state_machine.force_pause(coin, 900, "correlation stop")

        # Funding avoidance
        funding_window = self.risk_manager.is_funding_avoidance_window()

        # === STEP 3: Per-pair processing ===
        current_mids: dict[str, float] = {}

        # Bug #3: Expire hedge_in_progress after 60s
        for hcoin in list(self._hedge_in_progress.keys()):
            if now - self._hedge_in_progress[hcoin] > 60.0:
                logger.info(f"{hcoin}: hedge_in_progress expired after 60s")
                del self._hedge_in_progress[hcoin]

        for coin in list(self._active_coins):
            # Skip if circuit breaker disabled this pair
            if self.fill_tracker.is_pair_disabled(coin):
                self.quote_engine.cancel_coin(coin)
                continue

            # Get signal (Bug #12: under lock)
            async with self._signal_lock:
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

            # Bug #3: Track hedge_in_progress properly
            hedge_active = coin in self._hedge_in_progress

            # Bug #11: circuit_breaker_active triggers on ANY toxic flag, not just anchor jump
            cb_active = signal.any_toxic_flag

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
                circuit_breaker_active=cb_active,
                oms_mismatch=False,
                regime_shock=RiskAction.PAUSE_ALL in risk_state.actions,
                strong_imbalance=signal.strong_imbalance,
                imbalance_side=signal.imbalance_side,
                hedge_in_progress=hedge_active,
                bybit_hedge_available=coin in BYBIT_PERPS,
            )

            # Run state machine
            state_info = self.state_machine.transition(coin, ctx)

            # Bug #9: Handle EMERGENCY_FLATTEN state
            if state_info.state == PairState.EMERGENCY_FLATTEN:
                if not self.dry_run and abs(pos.size) > 0:
                    logger.critical(
                        f"EMERGENCY_FLATTEN: {coin} pos={pos.size:.6f}, "
                        f"placing taker close on HL"
                    )
                    try:
                        result = await asyncio.to_thread(
                            self.exchange.market_close, coin
                        )
                        logger.info(f"Emergency flatten {coin}: {result}")
                    except Exception as e:
                        logger.error(f"Emergency flatten failed for {coin}: {e}")
                    # Force pause 10 minutes after flatten
                    self.state_machine.force_pause(coin, 600, "post-emergency-flatten cooldown")
                self.quote_engine.cancel_coin(coin)
                continue

            # Compute and execute quotes
            if state_info.state in (PairState.QUOTING_BOTH, PairState.QUOTING_ONE_SIDE,
                                     PairState.INVENTORY_EXIT):
                # Bug #5: Portfolio-level check before quoting
                proposed_notional = self.inventory.get_limits(coin).q_soft * 0.5
                gross = snapshot.total_gross_notional if snapshot else 0
                net = snapshot.total_net_exposure if snapshot else 0
                if not self.risk_manager.check_notional_limit(gross, proposed_notional):
                    logger.debug(f"{coin}: gross notional limit hit, skipping quotes")
                    self.quote_engine.cancel_coin(coin)
                    continue

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

            # Execute Bybit hedge if requested (Gap 1)
            if state_info.hedge_requested and not self.dry_run:
                await self._execute_bybit_hedge(coin, pos, fair_value)
                # Bug #3: Mark hedge as in progress
                self._hedge_in_progress[coin] = now

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

        # Bug #3: Clear hedge_in_progress if HL inventory is now flat
        pos = self.inventory.get_position(coin)
        post_fill_flat = abs(pos.size) < 1e-10  # will be updated by record_fill
        if post_fill_flat and coin in self._hedge_in_progress:
            del self._hedge_in_progress[coin]

        # Bug #13: Track fill counts for pair lifecycle gating
        self._pair_fill_counts[coin] = self._pair_fill_counts.get(coin, 0) + 1

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

        # Telegram notification
        self.notifier.notify_fill(
            coin=coin, side=side, size=size, price=price,
            size_usd=size * price, fee=fee,
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
        """Callback for HL WS L2 book updates.

        Bug #12: WS callbacks run on the event loop. We use the signal lock
        to prevent torn reads during yield points in _tick().
        Since HL SDK callbacks are synchronous, we schedule the locked update.
        """
        try:
            # HL WS sends the full book in data["levels"]
            book_data = None
            if isinstance(data, dict) and "levels" in data:
                book_data = data
            elif isinstance(data, dict) and "data" in data:
                book_data = data["data"]

            if book_data:
                # Schedule locked update on event loop
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._locked_book_update(coin, book_data))
                else:
                    self.signal_engine.update_book(coin, book_data)
        except Exception as e:
            logger.debug(f"HL book parse error for {coin}: {e}")

    async def _locked_book_update(self, coin: str, data: dict) -> None:
        """Update book under signal lock (Bug #12)."""
        async with self._signal_lock:
            self.signal_engine.update_book(coin, data)

    async def _locked_trade_update(self, coin: str, trades: list) -> None:
        """Update trades under signal lock (Bug #12)."""
        async with self._signal_lock:
            self.signal_engine.update_trades(coin, trades)

    def _on_hl_trades(self, coin: str, data: dict) -> None:
        """Callback for HL WS trade updates.

        Bug #12: Schedule locked update to prevent torn reads.
        """
        try:
            trades = []
            if isinstance(data, list):
                trades = data
            elif isinstance(data, dict) and "data" in data:
                trades = data["data"]
            elif isinstance(data, dict):
                trades = [data]

            if trades:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(self._locked_trade_update(coin, trades))
                else:
                    self.signal_engine.update_trades(coin, trades)
        except Exception as e:
            logger.debug(f"HL trade parse error for {coin}: {e}")

    # ==================================================================
    # Bybit WebSocket
    # ==================================================================

    async def _bybit_ws_loop(self) -> None:
        """Maintain Bybit WS connection for price anchoring.

        Subscribes to tickers for all coins in BYBIT_PERPS that we might trade.
        Features (Gap 5):
          - Automatic reconnection with exponential backoff (1s, 2s, 4s, max 30s)
          - Ping every 20s (Bybit requires heartbeat)
          - On disconnect: set anchor_stale = True
          - On reconnect: resubscribe to all tickers
        """
        backoff = self.config.timing.bybit_ws_reconnect_base_s
        max_backoff = self.config.timing.bybit_ws_reconnect_max_s

        while self._running:
            # Build subscription list fresh each reconnect (coins may change)
            coins_to_sub = list(BYBIT_PERPS & (self._active_coins | set(self._initial_coins)))
            for c in ["BTC", "ETH", "SOL"]:
                if c not in coins_to_sub:
                    coins_to_sub.append(c)

            symbols = [f"{c}USDT" for c in coins_to_sub]
            coin_map = {f"{c}USDT": c for c in coins_to_sub}

            subscribe_msg = {
                "op": "subscribe",
                "args": [f"tickers.{s}" for s in symbols],
            }

            try:
                async with websockets.connect(
                    BYBIT_WS_URL,
                    ping_interval=None,  # we handle pings manually
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    await ws.send(json.dumps(subscribe_msg))
                    logger.info(f"Bybit WS connected, subscribed to {len(symbols)} tickers")
                    self._bybit_anchor_stale = False
                    backoff = self.config.timing.bybit_ws_reconnect_base_s  # reset on success

                    # Start heartbeat task
                    ping_task = asyncio.create_task(
                        self._bybit_ws_ping(ws), name="bybit_ping"
                    )

                    try:
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

                                        if prev_mid > 0:
                                            self.signal_engine.check_anchor_jump(
                                                coin, new_mid, prev_mid
                                            )
                                        self._prev_bybit_mids[coin] = new_mid

                            except Exception as e:
                                logger.debug(f"Bybit WS parse error: {e}")
                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass

            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"Bybit WS disconnected, reconnecting in {backoff:.1f}s")
                self._bybit_anchor_stale = True
            except Exception as e:
                logger.warning(f"Bybit WS error: {e}, reconnecting in {backoff:.1f}s")
                self._bybit_anchor_stale = True

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

    async def _bybit_ws_ping(self, ws) -> None:
        """Send periodic pings to keep Bybit WS alive (Gap 5)."""
        interval = self.config.timing.bybit_ws_ping_interval_s
        while True:
            try:
                await asyncio.sleep(interval)
                await ws.send(json.dumps({"op": "ping"}))
            except asyncio.CancelledError:
                break
            except Exception:
                break

    # ==================================================================
    # Screener loop
    # ==================================================================

    async def _screener_loop(self) -> None:
        """Run pair screener every 15 minutes.

        Bug #13: Second-tier coins start in SHADOW and promote to ACTIVE
        only after first-tier reaches fill count gate. Demoted pairs get
        deactivated after inventory exit + WS unsubscribe.
        """
        # Wait for initial data collection
        await asyncio.sleep(30)

        while self._running:
            try:
                if self.screener.should_rescan():
                    rankings = await self.screener.scan()

                    # Sync active coins with screener
                    screener_active = self.screener.active_pairs
                    for coin in screener_active - self._active_coins:
                        if not self.risk_manager.can_add_pair(len(self._active_coins)):
                            continue

                        # Bug #13: Second-tier coins need first-tier fill gate
                        # If any initial coin hasn't reached 100 fills, new coins stay shadow
                        first_tier_ready = all(
                            self._pair_fill_counts.get(c, 0) >= 100
                            for c in self._initial_coins
                            if c in self._active_coins
                        )
                        if not first_tier_ready and coin not in self._initial_coins:
                            logger.debug(
                                f"Screener: {coin} waiting for first-tier fill gate "
                                f"(fills: {self._pair_fill_counts})"
                            )
                            continue

                        await self._set_leverage(coin)
                        self._activate_coin(coin)

                    # Bug #13: Deactivate demoted pairs (not in screener active set)
                    # Only if they have no inventory (inventory exit handled by state machine)
                    for coin in list(self._active_coins):
                        if coin in screener_active or coin in self._initial_coins:
                            continue
                        pos = self.inventory.get_position(coin)
                        if abs(pos.size) < 1e-10:
                            logger.info(f"Screener: deactivating demoted pair {coin}")
                            self._deactivate_coin(coin)

            except Exception as e:
                logger.error(f"Screener error: {e}", exc_info=True)

            await asyncio.sleep(60)  # check every minute if rescan needed

    # ==================================================================
    # MongoDB persistence
    # ==================================================================

    async def _mongo_flush_loop(self) -> None:
        """Periodically flush fill and quote logs to MongoDB (Gap 6).

        Uses bulk_write with upsert for idempotency on fills (keyed by oid+timestamp).
        Quote logs use insert_many (append-only, no dedup needed).
        """
        interval = self.config.timing.mongo_flush_interval_s
        while self._running:
            await asyncio.sleep(interval)
            await self._flush_to_mongo()

    async def _flush_to_mongo(self) -> None:
        """Flush pending fills and quote logs to MongoDB."""
        try:
            # Flush fills with upsert (idempotent by oid + timestamp)
            fill_docs = self.fill_tracker.fills_to_mongo()
            if fill_docs:
                ops = []
                for doc in fill_docs:
                    filt = {"oid": doc["oid"], "timestamp": doc["timestamp"]}
                    ops.append(UpdateOne(filt, {"$set": doc}, upsert=True))
                if ops:
                    result = self._fills_col.bulk_write(ops, ordered=False)
                    logger.debug(
                        f"Mongo fills flush: {result.upserted_count} new, "
                        f"{result.modified_count} updated"
                    )

            # Flush recent quote logs (append-only)
            log_docs = self.fill_tracker.quote_logs_to_mongo(
                since=self._last_mongo_flush
            )
            if log_docs:
                self._quotes_col.insert_many(log_docs, ordered=False)

            self._last_mongo_flush = time.time()

            # Also flush notifier queue
            self.notifier.flush_queue()

        except Exception as e:
            logger.warning(f"MongoDB flush error: {e}")

    # ==================================================================
    # Fill polling fallback (Gap 4)
    # ==================================================================

    async def _fill_poll_loop(self) -> None:
        """REST poll fallback for fill detection every 30s (Gap 4).

        Cross-references with known fills. Any new fills not yet tracked
        are processed as if they came from the WS subscription.
        """
        # Wait for initial tick cycle
        await asyncio.sleep(10)
        interval = self.config.timing.fill_poll_interval_s

        while self._running:
            await asyncio.sleep(interval)
            if self.dry_run:
                continue
            try:
                await self._poll_fills_rest()
            except Exception as e:
                logger.warning(f"Fill poll error: {e}")

    async def _poll_fills_rest(self) -> None:
        """Query HL REST for recent user fills and process any missed ones."""
        try:
            resp = await asyncio.to_thread(
                sync_requests.post,
                HL_INFO_API,
                json={"type": "userFills", "user": self.address},
                timeout=5,
            )
            if resp.status_code != 200:
                logger.debug(f"Fill poll HTTP {resp.status_code}")
                return

            fills = resp.json()
            if not isinstance(fills, list):
                return

            # Process recent fills (last 50)
            for fill_data in fills[-50:]:
                # Build a unique hash for dedup
                fill_hash = self._fill_hash(fill_data)
                if fill_hash in self._known_fill_hashes:
                    continue

                self._known_fill_hashes.add(fill_hash)

                # Only process if it is a coin we are actively tracking
                coin = fill_data.get("coin", "")
                if coin not in self._active_coins:
                    continue

                side = "bid" if fill_data.get("side", "").upper() == "B" else "ask"
                price = float(fill_data.get("px", 0) or 0)
                size = float(fill_data.get("sz", 0) or 0)
                fee = float(fill_data.get("fee", 0) or 0)
                oid = fill_data.get("oid", 0)

                if price > 0 and size > 0:
                    logger.info(
                        f"FILL (REST fallback): {coin} {side} {size:.6f} @ "
                        f"${price:.6f} oid={oid}"
                    )
                    self._handle_fill(coin, {
                        "side": side,
                        "price": price,
                        "size": size,
                        "fee": fee,
                        "oid": oid,
                    })

            # Cap the known fill set to prevent unbounded growth
            if len(self._known_fill_hashes) > 5000:
                # Keep most recent half
                excess = len(self._known_fill_hashes) - 2500
                for _ in range(excess):
                    self._known_fill_hashes.pop()

        except Exception as e:
            logger.debug(f"Fill poll REST error: {e}")

    @staticmethod
    def _fill_hash(fill_data: dict) -> str:
        """Create unique hash for a fill record."""
        key = f"{fill_data.get('oid', '')}-{fill_data.get('time', '')}-{fill_data.get('hash', '')}"
        return hashlib.md5(key.encode()).hexdigest()

    # ==================================================================
    # Daily summary loop (Gap 8)
    # ==================================================================

    async def _daily_summary_loop(self) -> None:
        """Send daily PnL summary at 00:00 UTC."""
        while self._running:
            await asyncio.sleep(60)  # check every minute
            now = datetime.now(timezone.utc)
            if now.hour == self.config.telegram.daily_summary_hour_utc and now.minute == 0:
                if time.time() - self._last_daily_summary > 3500:  # avoid double-send
                    self._send_daily_summary()
                    self._last_daily_summary = time.time()

    def _send_daily_summary(self) -> None:
        """Build and send daily PnL summary via Telegram."""
        snapshot = self.inventory._get_snapshot()
        total_fills = sum(
            self.fill_tracker.get_toxicity(c).total_fills
            for c in self._active_coins
        )
        uptime_hours = (time.time() - self._start_time) / 3600.0
        self.notifier.notify_daily_summary(
            daily_pnl=snapshot.daily_pnl,
            total_fills=total_fills,
            gross_notional=snapshot.total_gross_notional,
            active_pairs=list(self._active_coins),
            uptime_hours=uptime_hours,
        )

    # ==================================================================
    # Startup reconciliation (Gap 2 + 3)
    # ==================================================================

    async def _reconcile_open_orders_on_startup(self) -> None:
        """Cancel ALL existing resting orders from prior sessions (Gap 2).

        Bug #2 fix: Retry cancel loop up to 30s with backoff, then VERIFY
        with REST query that zero orders remain. If can't verify, refuse to
        start quoting (stay in PAUSE). Also checks for unclean shutdown breadcrumb.
        """
        if self.dry_run:
            logger.info("[DRY] Would reconcile open orders on startup")
            return

        # Check for unclean shutdown from prior session
        UNCLEAN_FILE = "/tmp/hl_mm_unclean_shutdown"
        if os.path.exists(UNCLEAN_FILE):
            logger.critical(
                "Startup: found /tmp/hl_mm_unclean_shutdown from prior session. "
                "Performing aggressive reconciliation."
            )

        logger.info("Startup: reconciling open orders (retry loop up to 30s)...")
        deadline = time.time() + 30.0
        backoff = 1.0
        verified_clean = False

        while time.time() < deadline:
            try:
                resp = await asyncio.to_thread(
                    sync_requests.post,
                    HL_INFO_API,
                    json={"type": "openOrders", "user": self.address},
                    timeout=5,
                )
                if resp.status_code != 200:
                    logger.warning(f"Startup order query failed: HTTP {resp.status_code}, retrying in {backoff:.0f}s")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 10.0)
                    continue

                open_orders = resp.json()
                if not isinstance(open_orders, list) or not open_orders:
                    logger.info("Startup: verified zero open orders")
                    verified_clean = True
                    break

                logger.warning(f"Startup: found {len(open_orders)} stale orders, cancelling all")
                for order in open_orders:
                    coin = order.get("coin", "")
                    oid = order.get("oid", 0)
                    side = order.get("side", "?")
                    px = order.get("px", "?")
                    sz = order.get("sz", "?")
                    try:
                        await asyncio.to_thread(self.exchange.cancel, coin, oid)
                        logger.info(f"  Cancelled stale order: {coin} {side} {sz}@{px} oid={oid}")
                    except Exception as e:
                        logger.warning(f"  Failed to cancel {coin} oid={oid}: {e}")

                # Wait then verify
                await asyncio.sleep(1.0)

            except Exception as e:
                logger.warning(f"Startup reconciliation error: {e}, retrying in {backoff:.0f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)

        if not verified_clean:
            logger.critical(
                "Startup: could NOT verify zero open orders after 30s. "
                "Engine will start in PAUSE mode for all coins."
            )
            # Force all initial coins to PAUSE on activation
            self._startup_pause_required = True
        else:
            self._startup_pause_required = False

        # Clean up breadcrumb file if it exists
        if os.path.exists(UNCLEAN_FILE):
            try:
                os.remove(UNCLEAN_FILE)
            except OSError:
                pass

    async def _sync_positions_on_startup(self) -> None:
        """Load existing positions into inventory manager on startup (Gap 3).

        If any positions exist from a prior session, the inventory manager
        is pre-loaded so the state machine can enter INVENTORY_EXIT for
        those coins.
        """
        logger.info("Startup: syncing existing positions...")
        try:
            state = await asyncio.to_thread(self.info.user_state, self.address)
            if not state:
                logger.info("Startup: no user state returned")
                return

            positions_found = []
            for pos_data in state.get("assetPositions", []):
                p = pos_data.get("position", {})
                coin = p.get("coin", "")
                size = float(p.get("szi", 0))
                entry = float(p.get("entryPx", 0) or 0)
                unrealized = float(p.get("unrealizedPnl", 0) or 0)

                if abs(size) > 0 and coin:
                    positions_found.append((coin, size, entry))
                    logger.warning(
                        f"Startup: existing position {coin} size={size:.6f} "
                        f"entry=${entry:.6f} upnl=${unrealized:.4f}"
                    )

            if not positions_found:
                logger.info("Startup: no existing positions")
                return

            # Force a full inventory sync to load these into the manager
            await asyncio.to_thread(self.inventory.sync_positions)

            # For coins with existing positions, activate them and set to
            # INVENTORY_EXIT so the engine works to close them
            for coin, size, entry in positions_found:
                if coin not in self._active_coins:
                    self.screener.force_active(coin)
                    self._activate_coin(coin)
                    await self._set_leverage(coin)

                # Force into INVENTORY_EXIT state
                self.state_machine.register_pair(coin)
                state_info = self.state_machine.get_state(coin)
                if state_info:
                    from .state_machine import PairState
                    state_info.prev_state = state_info.state
                    state_info.state = PairState.INVENTORY_EXIT
                    state_info.entered_at = time.time()
                    state_info.reason = f"startup: existing position size={size:.6f}"
                    state_info.exit_mode = True

            logger.info(
                f"Startup: loaded {len(positions_found)} existing positions, "
                f"entering INVENTORY_EXIT"
            )

        except Exception as e:
            logger.error(f"Startup position sync failed: {e}")

    # ==================================================================
    # Bug #14: Bybit hedge PnL tracking
    # ==================================================================

    def _compute_bybit_hedge_pnl(self) -> float:
        """Compute unrealized PnL from Bybit hedge positions."""
        total_pnl = 0.0
        for coin, delta in self._hedge_positions.items():
            if abs(delta) < 1e-10:
                continue
            entry = self._hedge_entry_prices.get(coin, 0)
            current = self._prev_bybit_mids.get(coin, 0)
            if entry > 0 and current > 0:
                # delta > 0 means we are long on Bybit (short on HL)
                total_pnl += delta * (current - entry)
        return total_pnl

    # ==================================================================
    # Bybit hedge execution (Gap 1)
    # ==================================================================

    async def _execute_bybit_hedge(
        self,
        coin: str,
        pos,
        fair_value: float,
    ) -> None:
        """Execute an emergency delta hedge on Bybit mainnet via REST (Gap 1).

        Places an IOC limit order on Bybit at mid +/- max(1 tick, 1bp).
        Hedges 80-100% of the HL delta.
        Uses raw REST (pybit not available in this env).
        """
        if not self._bybit_api_key or not self._bybit_api_secret:
            logger.error(f"HEDGE BLOCKED: {coin} - no Bybit API credentials")
            return

        if abs(pos.size) < 1e-10 or fair_value <= 0:
            return

        # Determine hedge direction: if we are long on HL, sell on Bybit
        is_long_hl = pos.size > 0
        bybit_side = "Sell" if is_long_hl else "Buy"

        # Hedge size: 80-100% of delta
        hedge_cfg = self.config.hedge
        hedge_pct = (hedge_cfg.hedge_pct_min + hedge_cfg.hedge_pct_max) / 2.0
        hedge_size = abs(pos.size) * hedge_pct

        # Determine slippage budget
        is_direct = coin in BYBIT_PERPS
        slippage_bps = (
            hedge_cfg.direct_slippage_budget_bps if is_direct
            else hedge_cfg.proxy_slippage_budget_bps
        )

        # Compute limit price: mid +/- max(1 tick, slippage_budget)
        slip_offset = fair_value * (slippage_bps / 10000.0)
        tick_size = fair_value * 0.0001  # 1bp as minimum tick
        offset = max(tick_size, slip_offset)

        if bybit_side == "Buy":
            limit_price = fair_value + offset
        else:
            limit_price = fair_value - offset

        # Round for Bybit (perps use tick sizes, approximate with 2 decimals)
        sz_str = f"{hedge_size:.4f}"
        px_str = f"{limit_price:.2f}"
        symbol = f"{coin}USDT"

        logger.warning(
            f"HEDGE: {coin} {bybit_side} {sz_str} @ {px_str} on Bybit "
            f"(HL delta={pos.size:.6f}, hedge_pct={hedge_pct:.0%})"
        )

        try:
            result = await asyncio.to_thread(
                self._bybit_place_order,
                symbol=symbol,
                side=bybit_side,
                qty=sz_str,
                price=px_str,
                order_type="Limit",
                time_in_force="IOC",
            )

            if result and result.get("retCode") == 0:
                order_result = result.get("result", {})
                order_id = order_result.get("orderId", "?")
                logger.info(f"HEDGE SUCCESS: {coin} orderId={order_id}")

                # Bug #3: Track Bybit hedge position
                hedge_delta = -hedge_size if bybit_side == "Sell" else hedge_size
                self._hedge_positions[coin] = self._hedge_positions.get(coin, 0) + hedge_delta
                self._hedge_entry_prices[coin] = limit_price

                # Calculate actual slippage
                actual_slippage_bps = abs(limit_price - fair_value) / fair_value * 10000
                self.notifier.notify_hedge(
                    coin=coin, side=bybit_side, size=hedge_size,
                    price=limit_price, slippage_bps=actual_slippage_bps,
                )
            else:
                ret_code = result.get("retCode", "?") if result else "no response"
                ret_msg = result.get("retMsg", "") if result else ""
                logger.error(f"HEDGE FAILED: {coin} retCode={ret_code} msg={ret_msg}")
                self.notifier.notify_engine_event(
                    "HEDGE FAILED",
                    f"{coin} {bybit_side} {sz_str}: {ret_msg}",
                )

        except Exception as e:
            logger.error(f"HEDGE ERROR: {coin}: {e}", exc_info=True)
            self.notifier.notify_engine_event("HEDGE ERROR", f"{coin}: {e}")

    def _bybit_place_order(
        self,
        symbol: str,
        side: str,
        qty: str,
        price: str,
        order_type: str = "Limit",
        time_in_force: str = "IOC",
    ) -> Optional[dict]:
        """Place an order on Bybit mainnet via raw REST API.

        No pybit dependency. Uses HMAC-SHA256 signing per Bybit V5 API spec.
        """
        api_url = "https://api.bybit.com"
        endpoint = "/v5/order/create"
        timestamp = str(int(time.time() * 1000))
        recv_window = "5000"

        payload = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": order_type,
            "qty": qty,
            "price": price,
            "timeInForce": time_in_force,
        }

        payload_str = json.dumps(payload, separators=(",", ":"))

        # Bybit V5 signature: timestamp + api_key + recv_window + payload
        sign_str = f"{timestamp}{self._bybit_api_key}{recv_window}{payload_str}"
        signature = hmac.new(
            self._bybit_api_secret.encode("utf-8"),
            sign_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self._bybit_api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }

        resp = sync_requests.post(
            f"{api_url}{endpoint}",
            headers=headers,
            data=payload_str,
            timeout=5,
        )

        if resp.status_code == 200:
            return resp.json()
        else:
            logger.error(f"Bybit REST error: HTTP {resp.status_code}: {resp.text[:300]}")
            return {"retCode": resp.status_code, "retMsg": resp.text[:200]}

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
        """Graceful shutdown (Gap 9).

        Bug #2 fix: Retry cancel+verify loop up to 60s. If still can't confirm
        clean, leave a breadcrumb file for startup to check. Also close Bybit
        hedges (Bug #3).
        """
        if self._shutting_down:
            # Bug #2: Second SIGINT — still cancel orders, just skip verify
            logger.warning("Second shutdown request — cancelling orders without verify")
            if self.quote_engine and not self.dry_run:
                self.quote_engine.cancel_all()
            self._running = False
            return
        self._shutting_down = True
        logger.info("Graceful shutdown initiated...")
        self._running = False

        # Step 2: Cancel ALL resting orders with retry loop (Bug #2: up to 60s)
        verified_clean = False
        if self.quote_engine and not self.dry_run:
            logger.info("Shutdown: cancelling all resting orders...")
            self.quote_engine.cancel_all()

            deadline = time.time() + 60.0
            backoff = 1.0
            while time.time() < deadline:
                try:
                    resp = sync_requests.post(
                        HL_INFO_API,
                        json={"type": "openOrders", "user": self.address},
                        timeout=5,
                    )
                    if resp.status_code == 200:
                        remaining = resp.json()
                        if not remaining:
                            logger.info("Shutdown: verified zero open orders")
                            verified_clean = True
                            break
                        logger.warning(
                            f"Shutdown: {len(remaining)} orders still open, cancelling..."
                        )
                        for order in remaining:
                            try:
                                self.exchange.cancel(order["coin"], order["oid"])
                            except Exception:
                                pass
                    else:
                        logger.warning(f"Shutdown order query HTTP {resp.status_code}")
                except Exception as e:
                    logger.warning(f"Shutdown order verify failed: {e}")

                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 10.0)

            if not verified_clean:
                # Bug #2: Leave breadcrumb for startup
                logger.critical(
                    "Shutdown: could NOT confirm zero open orders after 60s. "
                    "Leaving /tmp/hl_mm_unclean_shutdown breadcrumb."
                )
                try:
                    with open("/tmp/hl_mm_unclean_shutdown", "w") as f:
                        f.write(f"unclean shutdown at {datetime.now(timezone.utc).isoformat()}\n")
                except OSError:
                    pass
        elif self.quote_engine:
            self.quote_engine.cancel_all()

        # Bug #3: Close Bybit hedge positions
        if self._hedge_positions and not self.dry_run:
            for coin, delta in self._hedge_positions.items():
                if abs(delta) > 1e-10:
                    logger.warning(
                        f"Shutdown: closing Bybit hedge {coin} delta={delta:.6f}"
                    )
                    bybit_side = "Sell" if delta > 0 else "Buy"
                    try:
                        mid = self._prev_bybit_mids.get(coin, 0)
                        if mid > 0:
                            # Use market-crossing price for IOC
                            offset = mid * 0.001  # 10bps
                            px = mid + offset if bybit_side == "Buy" else mid - offset
                            await asyncio.to_thread(
                                self._bybit_place_order,
                                symbol=f"{coin}USDT",
                                side=bybit_side,
                                qty=f"{abs(delta):.4f}",
                                price=f"{px:.2f}",
                                order_type="Limit",
                                time_in_force="IOC",
                            )
                    except Exception as e:
                        logger.error(f"Shutdown hedge close failed for {coin}: {e}")

        # Step 3: Warn about remaining inventory
        if self.inventory:
            for coin in list(self._active_coins):
                pos = self.inventory.get_position(coin)
                if abs(pos.size) > 1e-10:
                    logger.warning(
                        f"SHUTDOWN WARNING: {coin} has remaining inventory "
                        f"size={pos.size:.6f} (${pos.notional_usd:.2f}). "
                        f"NOT auto-hedging -- manual intervention required."
                    )

        # Step 4: Close WS connections
        if self.info:
            try:
                self.info.disconnect_websocket()
            except Exception:
                pass

        # Step 5: Flush all pending logs to MongoDB
        try:
            await self._flush_to_mongo()
        except Exception as e:
            logger.warning(f"Shutdown MongoDB flush failed: {e}")

        # Step 7: Log final PnL summary
        try:
            snapshot = self.inventory._get_snapshot() if self.inventory else None
            uptime = (time.time() - self._start_time) / 3600.0 if self._start_time > 0 else 0
            pnl = (snapshot.daily_pnl if snapshot else 0) + self._compute_bybit_hedge_pnl()
            logger.info(
                f"FINAL SUMMARY: uptime={uptime:.1f}h pnl=${pnl:.2f} "
                f"coins={list(self._active_coins)}"
            )
            self.notifier.notify_engine_event(
                "STOPPED",
                f"Uptime: {uptime:.1f}h, PnL: ${pnl:.2f}",
            )
            # Flush the stop notification
            self.notifier.flush_queue()
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
