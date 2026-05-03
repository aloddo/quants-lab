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
import json
import logging
import math
import os
import threading
import time
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
from .wallet_scorer import WalletScorer
from .mm_tracker import MMTracker
from .ws_order_client import WSOrderClient
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
        query_address: Optional[str] = None,
    ):
        # Credentials
        # address = signing address (agent wallet from env)
        # query_address = address for info queries (parent wallet where funds/positions live)
        # On HL, an agent key signs orders that execute on the parent wallet.
        # But info queries (user_state, open_orders, userFills) must target the PARENT wallet.
        self.private_key = private_key or os.environ.get("HL_PRIVATE_KEY", "")
        self.address = query_address or os.environ.get("HL_QUERY_ADDRESS", HL_ADDRESS)
        self._signing_address = address or os.environ.get("HL_ADDRESS", "")
        if not self.private_key:
            raise ValueError("HL_PRIVATE_KEY required")
        if not self.address:
            raise ValueError("HL_ADDRESS or HL_QUERY_ADDRESS required")
        if self.address != self._signing_address:
            logger.info(
                f"Agent mode: signing as {self._signing_address[:10]}..., "
                f"querying parent {self.address[:10]}..."
            )

        self.leverage = leverage
        self.dry_run = dry_run
        self.mongo_uri = mongo_uri
        self._initial_coins = initial_coins or []  # empty = screener auto-selects
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
        self._attempts_col = self._db["hl_mm_quote_attempts"]  # V2: lifecycle telemetry

        # V2: Quote attempt buffer for lifecycle tracking
        self._pending_attempts: list[dict] = []

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
        # Bug #4 fix: Use OrderedDict for bounded FIFO dedup instead of set with arbitrary eviction
        from collections import OrderedDict
        self._known_fill_hashes: OrderedDict = OrderedDict()
        self._known_fill_hashes_maxlen: int = 5000
        # Codex #1: threading.Lock for fill path — protects _known_fill_hashes,
        # _pending_cancel_coins, _pair_fill_counts, _fill_timestamps from
        # concurrent access by WS callback thread and async REST fallback.
        self._fill_lock = threading.Lock()
        self._prev_bybit_mids: dict[str, float] = {}
        self._bybit_anchor_stale: bool = False
        self._bybit_ws_backoff: float = 1.0
        self._last_daily_summary: float = 0.0

        # Hedge tracking — DISABLED (V2: costs ~23bps, destroys PnL)
        # Kept as empty dicts so references don't crash during cleanup
        self._hedge_positions: dict[str, float] = {}
        self._hedge_in_progress: dict[str, float] = {}

        # Bug #8: Shared rate limiter (token bucket)
        # HL /exchange rate limit is ~12 calls/8s = 1.5/sec (undocumented, volume-gated)
        # Screener no longer uses REST calls (MongoDB), so more budget for orders.
        # Increased: 1.8/sec refill, burst 4 — allows faster tick response.
        # HL rate limit: ~12 calls/8s = 1.5/sec. Previous 1.8/sec was too aggressive
        # and caused persistent 429 storms. Set to 1.2/sec with burst 3 for safety margin.
        self._rate_tokens: float = 3.0
        self._rate_max: float = 3.0
        self._rate_refill_per_sec: float = 1.2
        self._rate_last_refill: float = 0.0

        # Bug #12: asyncio.Lock for signal state access
        self._signal_lock = asyncio.Lock()

        # Bug #8 fix: Coalescing buffers for WS messages — store latest, process in _tick()
        self._latest_book: dict[str, dict] = {}       # coin -> latest book data
        self._latest_trades: dict[str, list] = {}     # coin -> accumulated trades (V2: ring buffer)
        # V2 fix: threading.Lock for WS buffers (WS callbacks run on SDK thread,
        # tick loop runs on asyncio thread -- asyncio.Lock doesn't protect cross-thread)
        self._ws_buffer_lock = threading.Lock()

        # Bug #9 fix: WS watchdog timestamps
        self._last_hl_ws_time: dict[str, float] = {}  # coin -> last HL WS message time
        self._last_bybit_ws_time: float = 0.0          # last Bybit WS message time
        self._bybit_ws_ref: Optional[object] = None    # reference to active Bybit WS for reconnect

        # Bug #5 (Codex R4): asyncio.Lock for inventory + quote state mutations
        self._oms_lock = asyncio.Lock()

        # Bug #13: Pair fill counts for lifecycle gating
        self._pair_fill_counts: dict[str, int] = {}

        # Bug #4 (adversarial): Pending cancel coins after fill — processed in tick loop
        self._pending_cancel_coins: set[str] = set()

        # Bug #6 (adversarial): Cached last valid snapshot for risk eval when sync skipped
        self._cached_snapshot: Optional[object] = None

        # Bug #8 (adversarial): Pending hedge close — HL flat but Bybit hedge remains
        self._pending_hedge_close: set[str] = set()

        # Codex #3: Cooling coins — recently deactivated but still monitored for
        # late fills. Keeps processing fills for 30s after deactivation to prevent
        # orphaned positions from cancel-race with late fills.
        self._cooling_coins: dict[str, float] = {}  # coin -> deactivation timestamp

        # Bug #14: Sticky daily stop — tracks the UTC date when stopped
        self._daily_stop_sticky: bool = False
        self._daily_stop_date: Optional[object] = None  # date object

        # === FILL DETECTION HARDENING (P0 — May 2 incident) ===
        # WS userFills subscription ID (for unsubscribe)
        self._ws_user_fills_sub_id: Optional[int] = None
        self._ws_order_updates_sub_id: Optional[int] = None
        self._ws_fills_received: int = 0  # count of fills via WS (vs REST)
        self._ws_fills_last_time: float = 0.0  # last WS fill timestamp

        # Fill sync health: consecutive REST poll failures trigger quoting pause
        self._fill_poll_consecutive_failures: int = 0
        self._fill_poll_max_failures: int = 3  # pause quoting after 3 consecutive failures
        self._fill_sync_healthy: bool = True  # False = blind, pause quoting

        # Max fills/min circuit breaker: if we get too many fills too fast,
        # we're likely getting adversely selected or something is wrong
        self._fill_timestamps: list[float] = []  # rolling window of fill times
        self._max_fills_per_minute: int = 20  # circuit breaker threshold
        self._fill_rate_breaker_until: float = 0.0  # pause quoting until this time

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
    # Bug #3 (Codex R4): Shared rate limiter for ALL HL REST calls
    # ==================================================================

    async def _hl_rest_call(self, fn, *args, priority: bool = False, **kwargs):
        """Gate any HL REST call through the shared token bucket.

        Args:
            fn: callable (sync) to execute via to_thread
            *args: positional args for fn
            priority: if True (cancel/hedge), always allow
            **kwargs: keyword args for fn

        Returns:
            Result of fn(*args, **kwargs)

        Raises:
            RuntimeError if rate limited and not priority
        """
        if not self._consume_rate_token(priority=priority):
            raise RuntimeError("HL REST rate limited")
        return await asyncio.to_thread(fn, *args, **kwargs)

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
        """Initialize HL SDK (makes REST calls, so not in __init__).

        Agent mode: the private key is from the agent wallet, but positions
        and fills live on the parent wallet (self.address). The Exchange SDK
        needs account_address=parent_wallet to route orders correctly.
        """
        wallet = eth_account.Account.from_key(self.private_key)
        api_url = constants.MAINNET_API_URL

        # skip_ws=False to enable WS subscriptions on Info
        self.info = Info(api_url, skip_ws=False)
        # Exchange needs the parent (query) address so orders route there
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
            rate_limit_fn=self._consume_rate_token,  # Bug #3 (Codex R4): shared limiter
        )

        self.fv_engine = FairValueEngine(bybit_pairs=bybit_pairs)
        self.signal_engine = SignalEngine()
        # Bug #6 (Codex R4): Compute per-coin tick sizes from HL meta
        # HL uses 5 significant figures; tick = 10^(-(5 - digits_before_decimal))
        tick_sizes = {}
        for coin, sz_dec in self._sz_decimals.items():
            # We don't have the price yet at init time — tick_sizes will be
            # computed dynamically by QuoteEngine._get_tick_size() using mid price.
            pass

        self.quote_engine = QuoteEngine(
            exchange=self.exchange,
            info=self.info,
            address=self.address,
            sz_decimals=self._sz_decimals,
            dry_run=self.dry_run,
            rate_limit_fn=self._consume_rate_token,  # Bug #3 (Codex R4): shared limiter
        )
        self.inventory = InventoryManager(info=self.info, address=self.address)
        self.fill_tracker = FillTracker()
        # Wire config to risk manager
        risk_cfg = RiskConfig(
            max_gross_notional=self.config.risk.max_gross_notional,
            max_net_exposure=self.config.risk.max_net_exposure,
            max_gross_resting=self.config.risk.max_resting_notional,
            daily_stop_usd=self.config.risk.daily_stop_usd,
            hard_stop_usd=self.config.risk.hard_stop_usd,
        )
        self.risk_manager = RiskManager(risk_cfg)
        logger.info(f"Risk limits: gross=${risk_cfg.max_gross_notional}, net=${risk_cfg.max_net_exposure}, resting=${risk_cfg.max_gross_resting}")
        self.state_machine = StateMachine()
        self.wallet_scorer = WalletScorer()
        self.mm_tracker = MMTracker(
            target_coins=set(self._initial_coins),
            poll_interval_s=60.0,
        )
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

        # Init SDK (REST calls) — retry on 429 with backoff
        for attempt in range(5):
            try:
                await asyncio.to_thread(self._init_sdk)
                break
            except Exception as e:
                if "429" in str(e) and attempt < 4:
                    wait = 5 * (2 ** attempt)
                    logger.warning(f"SDK init 429, retrying in {wait}s (attempt {attempt+1}/5)")
                    await asyncio.sleep(wait)
                else:
                    logger.error(f"SDK init failed: {e}")
                    return

        self._init_components()
        self._running = True
        self._start_time = time.time()
        # Codex R2 #3: Store event loop reference for WS thread -> async bridge
        self._event_loop = asyncio.get_running_loop()

        # V2: Initialize WS order client for low-latency order placement
        self._ws_order_client: Optional[WSOrderClient] = None
        if not self.dry_run:
            try:
                wallet = eth_account.Account.from_key(self.private_key)
                self._ws_order_client = WSOrderClient(
                    wallet=wallet,
                    exchange=self.exchange,
                    is_mainnet=True,
                )
                ws_ok = self._ws_order_client.start()
                if ws_ok:
                    # Inject into QuoteEngine so it uses WS for place/cancel
                    self.quote_engine.set_ws_client(
                        self._ws_order_client,
                        event_loop=self._event_loop,
                    )
                    logger.info(f"WS order client started, injected into QuoteEngine")
                else:
                    logger.warning("WS order client failed to start, using REST")
                    self._ws_order_client = None
            except Exception as e:
                logger.warning(f"WS order client init failed: {e}, using REST")
                self._ws_order_client = None

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

        # === FILL DETECTION HARDENING: Subscribe to WS userFills + orderUpdates ===
        # These are the PRIMARY fill detection path. REST poll is fallback only.
        self._subscribe_user_fills_ws()

        # V2: Initialize MM tracker (background, non-blocking)
        asyncio.create_task(self._init_mm_tracker(), name="mm_tracker_init")

        # Start background tasks
        tasks = [
            asyncio.create_task(self._main_loop(), name="main_loop"),
            asyncio.create_task(self._screener_loop(), name="screener_loop"),
            asyncio.create_task(self._bybit_ws_loop(), name="bybit_ws"),
            asyncio.create_task(self._mongo_flush_loop(), name="mongo_flush"),
            asyncio.create_task(self._fill_poll_loop(), name="fill_poll"),
            asyncio.create_task(self._daily_summary_loop(), name="daily_summary"),
            asyncio.create_task(self._mm_tracker_loop(), name="mm_tracker"),
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
        """Core tick loop. Runs every ~1.5s (matched to HL exchange rate limit)."""
        while self._running:
            try:
                await self._tick()
            except Exception as e:
                logger.error(f"Tick error: {e}", exc_info=True)
            await asyncio.sleep(1.0)  # tightened from 1.5s — faster quote adjustment

    async def _tick(self) -> None:
        """Single iteration of the main loop.

        Order: sync positions -> daily stop check -> risk check ->
        HEDGE_IMMEDIATELY check -> per-pair (signal -> FV -> state machine ->
        portfolio check -> quote -> execute) -> fill detection -> logging.
        """
        self._tick_count += 1
        now = time.time()

        # Bug #14: Sticky daily stop — check at TOP of every tick
        # Bug #7 fix: wrap cancel in to_thread since it makes sync HL REST calls
        if self._check_daily_stop_sticky():
            await asyncio.to_thread(self.quote_engine.cancel_all)
            return

        # Bug #4 fix: Process pending cancel coins (flagged by _handle_fill from WS thread)
        # Codex #1: Acquire fill_lock to safely copy+clear the set
        with self._fill_lock:
            coins_to_cancel = list(self._pending_cancel_coins)
            self._pending_cancel_coins.clear()
        if coins_to_cancel:
            for cancel_coin in coins_to_cancel:
                try:
                    await asyncio.to_thread(self.quote_engine.cancel_coin, cancel_coin)
                    logger.info(f"Post-fill cancel: {cancel_coin} orders cancelled")
                except Exception as e:
                    logger.warning(f"Post-fill cancel failed for {cancel_coin}: {e}")

        # Hedge close processing — DISABLED (V2: no hedging)
        if False and self._pending_hedge_close:
            coins_to_close = list(self._pending_hedge_close)
            self._pending_hedge_close.clear()
            for hc_coin in coins_to_close:
                hedge_delta = self._hedge_positions.get(hc_coin, 0)
                if abs(hedge_delta) > 1e-10:
                    logger.warning(
                        f"Hedge cleanup: {hc_coin} HL flat but Bybit hedge "
                        f"delta={hedge_delta:.6f} — closing Bybit position"
                    )
                    bybit_side = "Sell" if hedge_delta > 0 else "Buy"
                    mid = self._prev_bybit_mids.get(hc_coin, 0)
                    if mid > 0:
                        try:
                            offset = mid * 0.001  # 10bps crossing
                            px = mid + offset if bybit_side == "Buy" else mid - offset
                            result = await asyncio.to_thread(
                                self._bybit_place_order,
                                symbol=f"{hc_coin}USDT",
                                side=bybit_side,
                                qty=f"{abs(hedge_delta):.4f}",
                                price=f"{px:.2f}",
                                order_type="Limit",
                                time_in_force="IOC",
                                reduce_only=True,
                            )
                            # Codex #7: Verify fill before zeroing hedge position.
                            # A partially filled or rejected close leaves Bybit exposure
                            # that the engine must not forget about.
                            if result and result.get("retCode") == 0:
                                order_id = result.get("result", {}).get("orderId", "")
                                filled_qty = 0.0
                                if order_id:
                                    try:
                                        fill_result = await asyncio.to_thread(
                                            self._bybit_query_order,
                                            f"{hc_coin}USDT", order_id,
                                        )
                                        if fill_result:
                                            filled_qty = float(fill_result.get("cumExecQty", 0) or 0)
                                    except Exception:
                                        pass
                                remaining = abs(hedge_delta) - filled_qty
                                if remaining < 1e-10:
                                    self._hedge_positions[hc_coin] = 0
                                    logger.info(f"Hedge cleanup: {hc_coin} Bybit close VERIFIED flat")
                                else:
                                    # Partial fill — update remaining hedge delta
                                    sign = 1 if hedge_delta > 0 else -1
                                    self._hedge_positions[hc_coin] = sign * remaining
                                    logger.warning(
                                        f"Hedge cleanup: {hc_coin} partial close, "
                                        f"remaining={remaining:.4f} — will retry"
                                    )
                                    self._pending_hedge_close.add(hc_coin)
                            else:
                                logger.error(
                                    f"Hedge cleanup: {hc_coin} close rejected — "
                                    f"keeping hedge position, will retry"
                                )
                                self._pending_hedge_close.add(hc_coin)
                        except Exception as e:
                            logger.error(f"Hedge cleanup failed for {hc_coin}: {e}")
                            self._pending_hedge_close.add(hc_coin)

        # V2 fix: Drain WS buffers under BOTH locks:
        # _ws_buffer_lock (threading.Lock) protects against WS callback writes
        # _signal_lock (asyncio.Lock) protects signal engine reads
        with self._ws_buffer_lock:
            book_snapshot = dict(self._latest_book)
            trade_snapshot = dict(self._latest_trades)
            self._latest_book.clear()
            self._latest_trades.clear()
        async with self._signal_lock:
            for ws_coin, book_data in book_snapshot.items():
                self.signal_engine.update_book(ws_coin, book_data)
            for ws_coin, trade_data in trade_snapshot.items():
                self.signal_engine.update_trades(ws_coin, trade_data)

        # === STEP 1: Sync positions from exchange (every 30s, not every tick) ===
        now = time.time()
        snapshot = None
        if now - getattr(self, '_last_position_sync', 0) >= 30.0:
            async with self._oms_lock:
                if self._consume_rate_token():
                    try:
                        snapshot = await asyncio.to_thread(
                            self.inventory.sync_positions_safe, 2.0
                        )
                        self._last_position_sync = now
                        # Bug #6 fix: cache last valid snapshot
                        self._cached_snapshot = snapshot
                    except Exception as e:
                        logger.warning(f"Position sync failed: {e}")
                        snapshot = None
                else:
                    snapshot = None
        # Bug #6 fix: use cached snapshot when sync is skipped
        # Codex #8: Reject stale snapshots — if cached snapshot is >120s old,
        # treat as None so risk checks use zero (conservative) rather than
        # allowing quoting under a possibly-breached portfolio state.
        if snapshot is None:
            if (self._cached_snapshot is not None
                    and hasattr(self, '_last_position_sync')
                    and now - self._last_position_sync < 120.0):
                snapshot = self._cached_snapshot
            elif self._cached_snapshot is not None:
                logger.warning(
                    f"Cached snapshot too old ({now - getattr(self, '_last_position_sync', 0):.0f}s) "
                    f"— using zero values for risk checks"
                )
                snapshot = None

        effective_daily_pnl = snapshot.daily_pnl if snapshot else 0

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
            # Bug #7 fix: wrap cancel in to_thread
            await asyncio.to_thread(self.quote_engine.cancel_all)
            return

        if RiskAction.CANCEL_ALL_QUOTES in risk_state.actions:
            # Bug #7 fix: wrap cancel in to_thread
            await asyncio.to_thread(self.quote_engine.cancel_all)

        if RiskAction.PAUSE_ALL in risk_state.actions:
            for coin in self._active_coins:
                self.state_machine.force_pause(coin, 900, "correlation stop")

        # Funding avoidance
        funding_window = self.risk_manager.is_funding_avoidance_window()

        # === STEP 3: Per-pair processing ===
        current_mids: dict[str, float] = {}

        for coin in list(self._active_coins):
            # Skip if circuit breaker disabled this pair
            # Bug #7 fix: wrap cancel in to_thread
            if self.fill_tracker.is_pair_disabled(coin):
                await asyncio.to_thread(self.quote_engine.cancel_coin, coin)
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
                logger.debug(f"[{coin}] FV compute returned None — skipping")
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

            # Update fill tracker with current spread for calibrated CB
            self.fill_tracker.update_pair_spread(coin, book.spread_bps)

            # Build state machine context
            from .quote_engine import DEFAULT_TOX_BUFFERS
            tox = DEFAULT_TOX_BUFFERS.get(coin, 1.0)  # used for spread_threshold_met

            # Bug #3: Track hedge_in_progress properly
            hedge_active = coin in self._hedge_in_progress

            # === V2: SIDE-SPECIFIC EV GATING ===
            # Replace symmetric bid_ev = ask_ev = (edge_room - tox > 0)
            # with per-side markout-based expected value.
            bid_markout_ewma, bid_fill_count = self.fill_tracker.get_side_markout_ewma(coin, "bid")
            ask_markout_ewma, ask_fill_count = self.fill_tracker.get_side_markout_ewma(coin, "ask")

            half_spread = book.spread_bps / 2.0
            maker_fee = 1.44  # bps

            # Convert markout to cost: adverse (negative) markout = positive cost
            # Favorable (positive) markout = zero cost (don't reward, just don't penalize)
            bid_markout_cost = max(0.0, -bid_markout_ewma) if bid_fill_count >= 3 else 1.0  # prior: assume 1bps adverse
            ask_markout_cost = max(0.0, -ask_markout_ewma) if ask_fill_count >= 3 else 1.0

            # Exit penalty: probability of taker exit * taker fee
            # Simple model: if we have inventory on this side, exit is harder
            inv_usd = pos.size * fair_value if fair_value > 0 else 0
            bid_exit_penalty = 0.5 * 3.5 if inv_usd > limits.q_soft * 0.3 else 0.2 * 3.5  # bps
            ask_exit_penalty = 0.5 * 3.5 if inv_usd < -limits.q_soft * 0.3 else 0.2 * 3.5

            # === V2: KILL CRITERIA — auto-disable side/coin on bad markout ===
            # 8-fill EWMA < -4bps: disable that side for 30min
            if bid_fill_count >= 8 and bid_markout_ewma < -4.0:
                bid_ev = False
                if self._tick_count % 60 == 0:
                    logger.warning(f"[{coin}] KILL: bid EWMA={bid_markout_ewma:.1f}bps < -4bps, disabled")
            if ask_fill_count >= 8 and ask_markout_ewma < -4.0:
                ask_ev = False
                if self._tick_count % 60 == 0:
                    logger.warning(f"[{coin}] KILL: ask EWMA={ask_markout_ewma:.1f}bps < -4bps, disabled")

            # 50-fill coin markout < -3bps: demote coin entirely
            total_fills = bid_fill_count + ask_fill_count
            if total_fills >= 50:
                combined_markout = (
                    (bid_markout_ewma * bid_fill_count + ask_markout_ewma * ask_fill_count)
                    / total_fills if total_fills > 0 else 0
                )
                if combined_markout < -3.0:
                    bid_ev = False
                    ask_ev = False
                    if self._tick_count % 60 == 0:
                        logger.warning(
                            f"[{coin}] KILL: coin markout={combined_markout:.1f}bps < -3bps, "
                            f"both sides disabled"
                        )

            # === V2: MODEL DECAY GUARD ===
            # If markout data is stale (last fill > 5min ago), use conservative prior
            last_bid_fill = max(
                (f.timestamp for f in self.fill_tracker.get_recent_fills(coin, 20) if f.side == "bid"),
                default=0,
            )
            last_ask_fill = max(
                (f.timestamp for f in self.fill_tracker.get_recent_fills(coin, 20) if f.side == "ask"),
                default=0,
            )
            if bid_fill_count >= 3 and now - last_bid_fill > 300:
                bid_markout_cost = max(bid_markout_cost, 1.5)  # stale: use conservative 1.5bps
            if ask_fill_count >= 3 and now - last_ask_fill > 300:
                ask_markout_cost = max(ask_markout_cost, 1.5)

            # === V2: WALLET GATING — full spec actions ===
            wallet_toxic, wallet_toxic_count = self.wallet_scorer.is_toxic_active(coin)
            if wallet_toxic:
                # Spec: cancel same-side for 2-5s, widen opposite, halve size
                # Implementation: strong EV penalty + flag for size halving
                wallet_penalty = min(5.0, wallet_toxic_count * 1.5)  # up to 5bps
                # Also force cancel via immediate state-drop if already quoting
                # (the EV penalty will suppress on next tick, but we want immediate)
            else:
                wallet_penalty = 0.0

            # V2: MM crowding penalty — if competing MMs are reducing same side
            mm_reducing = self.mm_tracker.mm_reducing_side(coin)
            if mm_reducing == "bid":
                bid_markout_cost += 1.5  # MMs selling → bidding is riskier
            elif mm_reducing == "ask":
                ask_markout_cost += 1.5  # MMs buying → asking is riskier

            # V2: Crowding score penalty — crowded pairs are harder to exit
            if self.mm_tracker.is_crowded(coin, threshold=0.6):
                wallet_penalty += 1.0  # additional 1bps for crowded pairs

            bid_ev_bps = half_spread - maker_fee - bid_markout_cost - bid_exit_penalty - wallet_penalty
            ask_ev_bps = half_spread - maker_fee - ask_markout_cost - ask_exit_penalty - wallet_penalty

            ev_threshold = 0.5  # minimum bps to quote a side
            # Don't override kill criteria decisions
            if bid_fill_count < 8 or bid_markout_ewma >= -4.0:
                bid_ev = bid_ev_bps > ev_threshold
            if ask_fill_count < 8 or ask_markout_ewma >= -4.0:
                ask_ev = ask_ev_bps > ev_threshold

            if self._tick_count % 60 == 0:  # log every ~30s
                wallet_stats = self.wallet_scorer.get_stats_summary()
                logger.info(
                    f"[{coin}] EV: bid={bid_ev_bps:.1f}bps (mk={bid_markout_cost:.1f} "
                    f"ex={bid_exit_penalty:.1f} wl={wallet_penalty:.1f} n={bid_fill_count}) "
                    f"ask={ask_ev_bps:.1f}bps (mk={ask_markout_cost:.1f} "
                    f"ex={ask_exit_penalty:.1f} n={ask_fill_count}) "
                    f"spr={half_spread:.1f} w={wallet_stats['tracked_wallets']}/"
                    f"{wallet_stats['toxic_wallets']}t "
                    f"quote={'B' if bid_ev else '_'}{'A' if ask_ev else '_'}"
                )

            # === V2: SIGNAL-BASED CIRCUIT BREAKER ===
            # Wire ALL computed toxic flags as quoting gates, not just anchor_jump.
            past_warmup = (now - self._start_time > 30.0)
            if not past_warmup:
                cb_active = False
            else:
                # Hard gates: anchor_jump, depth_drop, spread_spike, VPIN > 0.8
                cb_active = (
                    signal.anchor_jump_detected
                    or signal.depth_drop_detected
                    or signal.spread_spike_detected
                    or signal.vpin > 0.8
                )
                if signal.vpin > 0.8 and self._tick_count % 30 == 0:
                    logger.warning(f"[{coin}] VPIN={signal.vpin:.2f} > 0.8 — quotes pulled")
                # Soft gates: trade imbalance -> suppress adverse side
                # V2 fix: use trade_imbalance_side from TRADE flow, not
                # imbalance_side from BOOK. They can disagree and the old
                # code suppressed the safe side when they diverged.
                if signal.trade_imbalance_toxic and not cb_active:
                    if signal.trade_imbalance_side > 0:  # buy-heavy trade flow
                        bid_ev = False  # suppress bid (toxic to buy into buying pressure)
                    elif signal.trade_imbalance_side < 0:  # sell-heavy trade flow
                        ask_ev = False  # suppress ask (toxic to sell into selling pressure)

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
                native_spread_bps=book.spread_bps,  # Codex R2 #7
            )

            # Codex #6: If pair is demoted (pending idle close), override to exit-only.
            # The screener flags demoted pairs via _pending_idle_close but the state
            # machine never consulted it — demoted pairs kept adding fresh inventory.
            coin_demoted = coin in self.screener.get_pending_idle_close()
            if coin_demoted and abs(ctx.inventory_usd) >= 1.0:
                # Force exit-only: suppress entry side in EV flags
                if ctx.inventory_usd > 0:
                    ctx.bid_side_ev_positive = False  # don't add to long
                else:
                    ctx.ask_side_ev_positive = False  # don't add to short

            # Run state machine
            # Capture previous quoting sides to detect changes
            prev_state_info = self.state_machine.get_state(coin)
            prev_quote_bid = prev_state_info.quote_bid if prev_state_info else False
            prev_quote_ask = prev_state_info.quote_ask if prev_state_info else False

            state_info = self.state_machine.transition(coin, ctx)

            # IMMEDIATE CANCEL on state transition: if a side was quoting and now
            # isn't, cancel that side's order RIGHT NOW instead of waiting for
            # next tick's execute_quotes. Without this, stale orders get filled
            # in the 0.5-1s window between state change and next requote cycle.
            #
            # GUARD: Only fire if the previous state was held for >= 1.5s.
            # Without this, rapid state oscillation (BIO flapping between
            # QUOTING_BOTH and ONE_SIDE every tick) burns the entire rate
            # budget on cancel calls, 429-ing everything else.
            bid_dropped = prev_quote_bid and not state_info.quote_bid
            ask_dropped = prev_quote_ask and not state_info.quote_ask
            prev_held_long_enough = (
                prev_state_info and (now - prev_state_info.entered_at) >= 1.5
            )
            if (bid_dropped or ask_dropped) and not self.dry_run and prev_held_long_enough:
                bid_o, ask_o = self.quote_engine.get_active_orders(coin)
                if bid_dropped and bid_o:
                    try:
                        await asyncio.to_thread(
                            self.exchange.cancel, coin, bid_o.oid
                        )
                        self.quote_engine.clear_order_by_oid(coin, bid_o.oid)
                        logger.info(f"[{coin}] Immediate cancel BID oid={bid_o.oid} (state: {state_info.state.value})")
                    except Exception as e:
                        logger.warning(f"[{coin}] Immediate cancel BID failed: {e}")
                if ask_dropped and ask_o:
                    try:
                        await asyncio.to_thread(
                            self.exchange.cancel, coin, ask_o.oid
                        )
                        self.quote_engine.clear_order_by_oid(coin, ask_o.oid)
                        logger.info(f"[{coin}] Immediate cancel ASK oid={ask_o.oid} (state: {state_info.state.value})")
                    except Exception as e:
                        logger.warning(f"[{coin}] Immediate cancel ASK failed: {e}")

            # Codex #6: If demoted and inventory cleared, finalize idle close
            if coin_demoted and abs(ctx.inventory_usd) < 1.0:
                self.screener.clear_idle_close(coin)

            # Freeze inventory age during CB pause to prevent automatic
            # emergency flatten (180s age limit < 300s CB pause = always taker exit)
            # BUT: don't freeze if hedge failed (no Bybit hedge) — let age accumulate
            # so emergency flatten can trigger and close the stuck position.
            bybit_hedged_coin = abs(self._hedge_positions.get(coin, 0)) > 1e-10
            hedge_pending = coin in self._hedge_in_progress
            if state_info.state == PairState.PAUSE and abs(pos.size) > 1e-10:
                if not (hedge_pending and not bybit_hedged_coin):
                    # Normal pause: freeze age
                    # Bug #10 fix: match tick interval (1.0s, not 1.5s)
                    self.inventory.pause_inventory_age(coin, 1.0)
                # else: failed hedge — let age accumulate toward emergency flatten

            # Bug #9: Handle EMERGENCY_FLATTEN state
            # Bug #2 (Codex R4): Verify close, retry up to 3x, fallback to Bybit hedge
            if state_info.state == PairState.EMERGENCY_FLATTEN:
                if not self.dry_run and abs(pos.size) > 0:
                    logger.critical(
                        f"EMERGENCY_FLATTEN: {coin} pos={pos.size:.6f}, "
                        f"placing taker close on HL"
                    )
                    flatten_success = False
                    for attempt in range(3):
                        try:
                            result = await asyncio.to_thread(
                                self.exchange.market_close, coin
                            )
                            logger.info(f"Emergency flatten {coin} attempt {attempt+1}: {result}")
                        except Exception as e:
                            logger.error(f"Emergency flatten {coin} attempt {attempt+1} failed: {e}")

                        # Verify position is actually flat
                        await asyncio.sleep(2.0)
                        try:
                            await asyncio.to_thread(self.inventory.sync_positions)
                        except Exception:
                            pass
                        verify_pos = self.inventory.get_position(coin)
                        if abs(verify_pos.size) < 1e-10:
                            logger.info(f"Emergency flatten {coin}: verified flat after attempt {attempt+1}")
                            flatten_success = True
                            break
                        else:
                            logger.warning(
                                f"Emergency flatten {coin}: still has position "
                                f"{verify_pos.size:.6f} after attempt {attempt+1}"
                            )

                    if not flatten_success:
                        logger.critical(
                            f"EMERGENCY_FLATTEN: {coin} failed 3 close attempts "
                            f"— MANUAL INTERVENTION REQUIRED"
                        )
                        self.notifier.notify_engine_event(
                            "EMERGENCY FLATTEN FAILED",
                            f"{coin}: 3 close attempts failed. Manual intervention required.",
                        )

                    # Only pause 10min if actually flat
                    verify_pos = self.inventory.get_position(coin)
                    if abs(verify_pos.size) < 1e-10:
                        self.state_machine.force_pause(coin, 600, "post-emergency-flatten cooldown")
                    else:
                        # Don't pause — keep retrying next tick
                        logger.critical(f"EMERGENCY_FLATTEN: {coin} NOT pausing — still exposed")
                await asyncio.to_thread(self.quote_engine.cancel_coin, coin)
                continue

            # Compute and execute quotes
            if state_info.state in (PairState.QUOTING_BOTH, PairState.QUOTING_ONE_SIDE,
                                     PairState.INVENTORY_EXIT):
                # FILL DETECTION HARDENING: Block new quotes if fill detection is blind
                if not self._fill_sync_healthy:
                    if self._tick_count % 20 == 0:  # log every ~10s
                        logger.warning(
                            f"{coin}: quoting BLOCKED — fill sync unhealthy "
                            f"({self._fill_poll_consecutive_failures} consecutive REST failures, "
                            f"WS fills received: {self._ws_fills_received})"
                        )
                    # Cancel existing quotes but don't place new ones
                    await asyncio.to_thread(self.quote_engine.cancel_coin, coin)
                    continue

                # FILL DETECTION HARDENING: Block new quotes if fill rate breaker active
                if self._check_fill_rate_breaker():
                    if self._tick_count % 20 == 0:
                        logger.warning(f"{coin}: quoting BLOCKED — fill rate breaker active")
                    await asyncio.to_thread(self.quote_engine.cancel_coin, coin)
                    continue

                # Bug #5: Portfolio-level check before quoting
                # Bug #4 (Codex R4): Also check net exposure and resting notional
                proposed_notional = self.inventory.get_limits(coin).q_soft * 0.5
                gross = snapshot.total_gross_notional if snapshot else 0
                net = snapshot.total_net_exposure if snapshot else 0
                if not self.risk_manager.check_notional_limit(gross, proposed_notional):
                    logger.debug(f"{coin}: gross notional limit hit, skipping quotes")
                    await asyncio.to_thread(self.quote_engine.cancel_coin, coin)
                    continue
                # Bug #2 fix: Pass side context so exit-side orders are always allowed.
                # If we have inventory, determine whether the quote sides would reduce exposure.
                coin_has_inv = has_inventory.get(coin, False)
                bid_blocked = not self.risk_manager.check_net_exposure(
                    net, proposed_notional, is_buy=True, has_inventory=coin_has_inv,
                )
                ask_blocked = not self.risk_manager.check_net_exposure(
                    net, proposed_notional, is_buy=False, has_inventory=coin_has_inv,
                )
                if bid_blocked and ask_blocked:
                    logger.debug(f"{coin}: net exposure limit hit on both sides, skipping quotes")
                    await asyncio.to_thread(self.quote_engine.cancel_coin, coin)
                    continue
                # Check resting order notional (sum of all active quote notionals)
                total_resting = self._compute_total_resting_notional()
                if not self.risk_manager.check_resting_notional(total_resting + proposed_notional):
                    logger.debug(f"{coin}: resting notional limit hit (${total_resting:.1f}), skipping")
                    await asyncio.to_thread(self.quote_engine.cancel_coin, coin)
                    continue

                should_rq = self.quote_engine.should_requote(coin, fair_value)
                if not should_rq:
                    pass  # normal: requote interval not reached
                else:
                    vol_scale = 0.5 if risk_state.halve_sizes else 1.0
                    anchor_scale = 1.0 if fv_est.anchor_weight > 0.3 else 0.7
                    logger.info(
                        f"[{coin}] Computing quotes: fv={fair_value:.6f} "
                        f"bid={book.best_bid:.6f} ask={book.best_ask:.6f} "
                        f"spread={book.spread_bps:.1f}bps"
                    )

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
                        logger.info(f"[{coin}] Placing quotes: {quotes}")
                        # V2: QuoteEngine handles WS vs REST internally
                        async with self._oms_lock:
                            if not self.dry_run:
                                await asyncio.to_thread(self.quote_engine.execute_quotes, quotes)
                            else:
                                self.quote_engine.execute_quotes(quotes)
                    else:
                        logger.debug(f"[{coin}] compute_quotes returned empty")

                    # Log quote decision (regardless of whether quotes were placed)
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
                        bid_price=quotes.bid.price if quotes and quotes.bid else None,
                        ask_price=quotes.ask.price if quotes and quotes.ask else None,
                        bid_oid=bid_o.oid if bid_o else None,
                        ask_oid=ask_o.oid if ask_o else None,
                    ))
            else:
                # Not quoting: cancel any existing orders
                # Bug #5 (Codex R4): Acquire OMS lock
                # Bug #7 fix: wrap cancel in to_thread
                async with self._oms_lock:
                    await asyncio.to_thread(self.quote_engine.cancel_coin, coin)

            # Execute Bybit hedge if requested (Gap 1)
            # Bug #1 (Codex R4): Check hedge_in_progress + hedge_positions before stacking

            # Detect fills — uses shared open_orders snapshot (Codex R2 #5)
            pass  # Moved to batch fill detection below

        # === STEP 3b: Batch fill detection (Codex R2 #5) ===
        # Query open_orders ONCE for all coins instead of per-coin.
        # Skip when we have WS fills working and no active quotes — saves rate budget.
        any_quoting = any(
            self.state_machine.get_state(c) and
            self.state_machine.get_state(c).state.value in ("QUOTING_BOTH", "QUOTING_ONE_SIDE", "INVENTORY_EXIT")
            for c in self._active_coins
        )
        if not self.dry_run and self._active_coins and any_quoting:
            async with self._oms_lock:
                try:
                    if self._consume_rate_token():
                        all_open = await asyncio.to_thread(
                            self.info.open_orders, self.address
                        )
                    else:
                        all_open = None
                except Exception as e:
                    logger.warning(f"Batch open_orders query failed: {e}")
                    all_open = None

                if all_open is not None:
                    for coin in list(self._active_coins):
                        try:
                            fills = self.quote_engine.detect_fills_from_snapshot(
                                coin, all_open
                            )
                            for fill in fills:
                                self._handle_fill(coin, fill)
                        except Exception as e:
                            logger.warning(f"Fill detection error for {coin}: {e}")

        # === STEP 4: Update markouts + wallet attribution ===
        self.fill_tracker.update_markouts(current_mids)

        # V2: Attribute completed markouts to counterparty wallets
        for fill in self.fill_tracker.get_recent_fills(last_n=20):
            if fill.markout_5s is not None and not getattr(fill, '_wallet_attributed', False):
                self.wallet_scorer.attribute_markout(
                    coin=fill.coin, fill_side=fill.side,
                    fill_price=fill.price, fill_time=fill.timestamp,
                    markout_5s=fill.markout_5s,
                )
                fill._wallet_attributed = True  # don't re-attribute

        # === STEP 5: Orphan cleanup (every 30s) ===
        # Bug #5 (Codex R4): Acquire OMS lock for orphan cleanup
        if self._tick_count % 60 == 0:
            if not self.dry_run:
                async with self._oms_lock:
                    await asyncio.to_thread(self.quote_engine.cleanup_orphans)

        # === STEP 5b: WS watchdog (Bug #9 fix) ===
        # Check for silent HL WS stalls (>10s without message per coin)
        for ws_coin in list(self._active_coins):
            last_msg = self._last_hl_ws_time.get(ws_coin, 0)
            if last_msg > 0 and now - last_msg > 10.0:
                logger.warning(
                    f"WS WATCHDOG: {ws_coin} HL WS silent for "
                    f"{now - last_msg:.1f}s — resubscribing"
                )
                self._unsubscribe_hl_ws(ws_coin)
                self._subscribe_hl_ws(ws_coin)
                self._last_hl_ws_time[ws_coin] = now  # reset to avoid rapid re-trigger

        # Check for silent Bybit WS stall (>30s without any message)
        if self._last_bybit_ws_time > 0 and now - self._last_bybit_ws_time > 30.0:
            logger.warning(
                f"WS WATCHDOG: Bybit WS silent for "
                f"{now - self._last_bybit_ws_time:.1f}s — flagging stale"
            )
            self._bybit_anchor_stale = True
            # The _bybit_ws_loop will detect disconnection and reconnect

        # === STEP 5c: HYPERCARE position reconciliation (every 5min) ===
        # Compare inventory manager's position state vs actual exchange positions.
        # Alert on any mismatch > threshold — catches blind fill accumulation.
        if self._tick_count % 600 == 0 and not self.dry_run:
            await self._hypercare_position_check()

        # === STEP 5d: Cooling coin cleanup (Codex #3) ===
        # Coins in cooling state for >30s with no inventory — fully deactivate
        expired_cooling = [
            c for c, t in self._cooling_coins.items()
            if now - t > 30.0
        ]
        for cool_coin in expired_cooling:
            pos = self.inventory.get_position(cool_coin)
            if abs(pos.size) > 1e-10:
                # Still has inventory after cooling — re-activate for emergency close
                logger.warning(
                    f"Cooling {cool_coin}: still has position {pos.size:.6f} after 30s "
                    f"— re-activating for emergency close"
                )
                self._activate_coin(cool_coin)
                self.state_machine.force_state(
                    cool_coin, PairState.EMERGENCY_FLATTEN,
                    "late fill during deactivation cooling"
                )
            else:
                self._unsubscribe_hl_ws(cool_coin)
                logger.info(f"Cooling {cool_coin}: expired, fully deactivated")
            del self._cooling_coins[cool_coin]

        # === STEP 6: Status log (every 60s) ===
        if self._tick_count % 120 == 0:
            self._log_status()

    # ==================================================================
    # Fill handling
    # ==================================================================

    def _handle_fill(self, coin: str, fill: dict) -> None:
        """Process a detected fill.

        Codex #1 fix: All mutations to shared state (_known_fill_hashes,
        _pending_cancel_coins, _pair_fill_counts, _fill_timestamps) are
        protected by _fill_lock. This method is called from both the WS
        callback thread and the async REST fallback — without the lock,
        concurrent calls can double-count fills or lose cancel signals.
        """
        side = fill["side"]
        price = fill["price"]
        size = fill["size"]
        fee = fill.get("fee", 0)
        oid = fill.get("oid", 0)

        # Codex #1: Acquire fill lock for all shared state mutations
        with self._fill_lock:
            # Bug #4 fix: Generate the same hash used by _poll_fills_rest and add
            # to _known_fill_hashes so fills detected via detect_fills() are deduped
            # against the REST poll fallback.
            fill_hash_data = {
                "oid": oid, "time": fill.get("time", ""), "hash": fill.get("hash", ""),
                "coin": coin, "side": side, "sz": size, "px": price,
            }
            fh = self._fill_hash(fill_hash_data)
            if fh in self._known_fill_hashes:
                return  # already processed
            self._known_fill_hashes[fh] = True
            # Evict oldest entries if over max length
            while len(self._known_fill_hashes) > self._known_fill_hashes_maxlen:
                self._known_fill_hashes.popitem(last=False)

            # Bug #13: Track fill counts for pair lifecycle gating
            self._pair_fill_counts[coin] = self._pair_fill_counts.get(coin, 0) + 1

            # FILL DETECTION HARDENING: Record fill timestamp for rate breaker
            self._fill_timestamps.append(time.time())

            # Bug #4 fix: flag coin for immediate cancel in next tick to prevent
            # stale orders from filling after this fill changes position
            self._pending_cancel_coins.add(coin)

        # Codex R2 #3: Fire cancel immediately from WS thread via event loop,
        # don't wait for next tick (~1s delay = stale exposure after fill)
        if hasattr(self, '_event_loop') and self._event_loop and self._event_loop.is_running():
            async def _cancel_now():
                try:
                    await asyncio.to_thread(self.quote_engine.cancel_coin, coin)
                except Exception:
                    pass  # _pending_cancel_coins is backup
            asyncio.run_coroutine_threadsafe(_cancel_now(), self._event_loop)

        # Bug #3: Clear hedge_in_progress if HL inventory is now flat
        # (outside fill_lock — reads inventory which has its own lock)
        pos = self.inventory.get_position(coin)
        post_fill_flat = abs(pos.size) < 1e-10  # will be updated by record_fill
        if post_fill_flat and coin in self._hedge_in_progress:
            del self._hedge_in_progress[coin]

        self.inventory.record_fill(coin, side, price, size, fee=fee)
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

        # V2: Log quote attempt with fill outcome
        bid_o, ask_o = self.quote_engine.get_active_orders(coin)
        matched_order = bid_o if side == "bid" and bid_o else ask_o if side == "ask" and ask_o else None
        self._pending_attempts.append({
            "coin": coin,
            "side": side,
            "placed_at": matched_order.placed_at if matched_order else now,
            "ended_at": now,
            "price": price,
            "ticks_from_touch": matched_order.ticks_from_touch if matched_order else 0,
            "spread_bps": matched_order.spread_bps_at_place if matched_order else 0,
            "filled_qty": size,
            "outcome": "full_fill",
            "cancel_reason": None,
        })

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

    async def _deactivate_coin(self, coin: str) -> None:
        """Remove a coin from active quoting.
        Bug #7 fix: made async, cancel wrapped in to_thread.

        Codex #3 fix: Don't immediately drop the coin — move it to cooling
        set for 30s so late fills from cancel-race are still processed.
        WS subscriptions stay alive during cooling.
        """
        await asyncio.to_thread(self.quote_engine.cancel_coin, coin)
        self.state_machine.unregister_pair(coin)
        self._active_coins.discard(coin)
        # Don't unsubscribe WS yet — move to cooling for late fill detection
        self._cooling_coins[coin] = time.time()
        logger.info(f"Deactivated coin: {coin} (cooling for 30s)")

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

        Bug #8 fix: Instead of create_task per message (which backlogs at
        bursty rates), store latest into coalescing buffer. _tick() drains
        the buffer at the start of each cycle.
        """
        try:
            book_data = None
            if isinstance(data, dict) and "levels" in data:
                book_data = data
            elif isinstance(data, dict) and "data" in data:
                book_data = data["data"]

            if book_data:
                with self._ws_buffer_lock:
                    self._latest_book[coin] = book_data
                # Bug #9 fix: track last WS message time for watchdog
                self._last_hl_ws_time[coin] = time.time()
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

        V2: Extracts wallet addresses from `users` field and feeds to
        WalletScorer. Also appends trades to ring buffer for signal engine.
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
                # V2: APPEND trades to buffer for signal engine
                with self._ws_buffer_lock:
                    if coin not in self._latest_trades:
                        self._latest_trades[coin] = []
                    self._latest_trades[coin].extend(trades)

                # V2: Feed wallet addresses to WalletScorer
                # HL trade format: {side, px, sz, hash, time, users: [buyer, seller]}
                for trade in trades:
                    users = trade.get("users", [])
                    if users and len(users) >= 2:
                        buyer = users[0] if isinstance(users[0], str) else ""
                        seller = users[1] if isinstance(users[1], str) else ""
                        side = trade.get("side", "")
                        price = float(trade.get("px", 0) or 0)
                        size = float(trade.get("sz", 0) or 0)
                        if price > 0 and size > 0 and (buyer or seller):
                            self.wallet_scorer.record_trade(
                                coin=coin, side=side, price=price,
                                size=size, buyer=buyer, seller=seller,
                            )

                # Bug #9 fix: track last WS message time for watchdog
                self._last_hl_ws_time[coin] = time.time()
        except Exception as e:
            logger.debug(f"HL trade parse error for {coin}: {e}")

    # ==================================================================
    # FILL DETECTION HARDENING: WS userFills + orderUpdates
    # ==================================================================

    def _subscribe_user_fills_ws(self) -> None:
        """Subscribe to HL WS userFills + orderUpdates for this address.

        PRIMARY fill detection path. Fires on every fill instantly via WS,
        no REST rate limit concern. REST poll (_fill_poll_loop) is fallback.
        """
        if not self.info or self.dry_run:
            return

        try:
            self._ws_user_fills_sub_id = self.info.subscribe(
                {"type": "userFills", "user": self.address},
                self._on_ws_user_fills,
            )
            logger.info(f"Subscribed to WS userFills for {self.address[:10]}...")
        except Exception as e:
            logger.error(f"Failed to subscribe WS userFills: {e}")

        try:
            self._ws_order_updates_sub_id = self.info.subscribe(
                {"type": "orderUpdates", "user": self.address},
                self._on_ws_order_updates,
            )
            logger.info(f"Subscribed to WS orderUpdates for {self.address[:10]}...")
        except Exception as e:
            logger.error(f"Failed to subscribe WS orderUpdates: {e}")

    def _unsubscribe_user_fills_ws(self) -> None:
        """Unsubscribe from userFills + orderUpdates."""
        if not self.info:
            return
        try:
            if self._ws_user_fills_sub_id is not None:
                self.info.unsubscribe(
                    {"type": "userFills", "user": self.address},
                    self._ws_user_fills_sub_id,
                )
            if self._ws_order_updates_sub_id is not None:
                self.info.unsubscribe(
                    {"type": "orderUpdates", "user": self.address},
                    self._ws_order_updates_sub_id,
                )
        except Exception:
            pass

    def _on_ws_user_fills(self, data: dict) -> None:
        """Callback for WS userFills messages.

        Format: {"user": "0x...", "isSnapshot": bool, "fills": [
            {"coin": "BIO", "px": "0.123", "sz": "100", "side": "B",
             "time": 1714..., "hash": "0x...", "oid": 123, "fee": "0.01",
             "tid": 456, "crossed": true, ...}
        ]}

        This is the PRIMARY fill detection path. Each fill is processed
        immediately and deduped against the REST poll fallback.
        """
        try:
            fills_data = None
            if isinstance(data, dict):
                if "fills" in data:
                    fills_data = data["fills"]
                elif "data" in data and isinstance(data["data"], dict):
                    fills_data = data["data"].get("fills", [])

            if not fills_data:
                return

            is_snapshot = False
            if isinstance(data, dict):
                is_snapshot = data.get("isSnapshot", False)
                if "data" in data:
                    is_snapshot = data["data"].get("isSnapshot", False)

            for fill_data in fills_data:
                coin = fill_data.get("coin", "")
                # Codex #3: Also process fills for cooling coins (recently deactivated)
                if coin not in self._active_coins and coin not in self._cooling_coins:
                    continue

                # Build fill dict matching _handle_fill format
                raw_side = fill_data.get("side", "")
                side = "bid" if raw_side == "B" else "ask"
                price = float(fill_data.get("px", 0) or 0)
                size = float(fill_data.get("sz", 0) or 0)
                fee = float(fill_data.get("fee", 0) or 0)
                oid = fill_data.get("oid", 0)
                fill_time = fill_data.get("time", "")
                fill_hash = fill_data.get("hash", "")

                if price <= 0 or size <= 0:
                    continue

                # Skip snapshot fills (historical) — only process live fills
                if is_snapshot:
                    # Still add to known hashes to avoid REST re-processing
                    fh = self._fill_hash({
                        "oid": oid, "time": fill_time, "hash": fill_hash,
                        "coin": coin, "side": side, "sz": size, "px": price,
                    })
                    self._known_fill_hashes[fh] = True
                    while len(self._known_fill_hashes) > self._known_fill_hashes_maxlen:
                        self._known_fill_hashes.popitem(last=False)
                    continue

                logger.info(
                    f"FILL (WS): {coin} {side} {size:.6f} @ ${price:.6f} "
                    f"oid={oid} fee=${fee:.4f}"
                )

                self._ws_fills_received += 1
                self._ws_fills_last_time = time.time()

                # Process fill (same path as REST fallback)
                self._handle_fill(coin, {
                    "side": side,
                    "price": price,
                    "size": size,
                    "fee": fee,
                    "oid": oid,
                    "time": fill_time,
                    "hash": fill_hash,
                    "source": "ws",
                })

        except Exception as e:
            logger.warning(f"WS userFills callback error: {e}")

    def _on_ws_order_updates(self, data: dict) -> None:
        """Callback for WS orderUpdates messages.

        Provides real-time order status changes (filled, cancelled, etc.)
        which helps the quote engine clear stale order state faster.
        """
        try:
            orders = None
            if isinstance(data, list):
                orders = data
            elif isinstance(data, dict) and "data" in data:
                orders = data["data"] if isinstance(data["data"], list) else [data["data"]]
            elif isinstance(data, dict):
                orders = [data]

            if not orders:
                return

            for order_data in orders:
                coin = order_data.get("coin", "")
                status = order_data.get("status", "")
                oid = order_data.get("oid", 0)

                if status in ("filled", "canceled", "cancelled", "marginCanceled"):
                    logger.debug(
                        f"ORDER UPDATE (WS): {coin} oid={oid} status={status}"
                    )
                    # Clear from quote engine tracking if still present
                    if self.quote_engine and coin in self._active_coins:
                        self.quote_engine.clear_order_by_oid(coin, oid)

        except Exception as e:
            logger.debug(f"WS orderUpdates callback error: {e}")

    def _check_fill_rate_breaker(self) -> bool:
        """Check if fill rate circuit breaker is active.

        Returns True if quoting should be paused due to excessive fill rate.
        """
        now = time.time()

        # Check if in breaker cooldown
        if now < self._fill_rate_breaker_until:
            return True

        # Clean old timestamps (keep last 60s)
        self._fill_timestamps = [
            t for t in self._fill_timestamps if now - t < 60.0
        ]

        if len(self._fill_timestamps) >= self._max_fills_per_minute:
            # Trip the breaker: pause for 60s
            self._fill_rate_breaker_until = now + 60.0
            logger.warning(
                f"FILL RATE BREAKER: {len(self._fill_timestamps)} fills in last 60s "
                f"(max={self._max_fills_per_minute}). Pausing quoting for 60s."
            )
            self.notifier.notify_engine_event(
                "FILL_RATE_BREAKER",
                f"{len(self._fill_timestamps)} fills/min exceeded threshold "
                f"of {self._max_fills_per_minute}. Quoting paused 60s.",
            )
            return True

        return False

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
                                        # Bug #9 fix: track Bybit WS message time
                                        self._last_bybit_ws_time = time.time()
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

    async def _init_mm_tracker(self) -> None:
        """V2: Initialize MM tracker in background (non-blocking)."""
        await asyncio.sleep(10)  # let engine start first
        try:
            self.mm_tracker.update_target_coins(self._active_coins | set(self._initial_coins))
            await self.mm_tracker.initialize()
        except Exception as e:
            logger.warning(f"MM Tracker init failed: {e}")

    async def _mm_tracker_loop(self) -> None:
        """V2: Poll competitor MM positions every 60s."""
        await asyncio.sleep(30)  # initial delay
        while self._running:
            try:
                self.mm_tracker.update_target_coins(self._active_coins)
                await self.mm_tracker.poll_positions()

                # Log crowding for active coins
                for coin in self._active_coins:
                    crowding = self.mm_tracker.get_crowding(coin)
                    if crowding.mm_count > 0:
                        logger.info(
                            f"[{coin}] MM Crowding: {crowding.mm_count} MMs, "
                            f"net={crowding.net_mm_direction:+.0f}, "
                            f"score={crowding.crowding_score:.2f}"
                        )
            except Exception as e:
                logger.warning(f"MM Tracker poll error: {e}")
            await asyncio.sleep(60)

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

                    # H2-style instant rotation: sync active coins with screener
                    screener_active = self.screener.active_pairs
                    for coin in screener_active - self._active_coins:
                        if not self.risk_manager.can_add_pair(len(self._active_coins)):
                            continue
                        await self._set_leverage(coin)
                        self._activate_coin(coin)
                        logger.info(f"Screener: activated {coin} (instant rotation)")

                    # Deactivate demoted pairs — instant, no grace period.
                    # Initial coins are NOT exempt: if screener says IDLE, they go IDLE.
                    # If pair has inventory, state machine handles maker-only close.
                    for coin in list(self._active_coins):
                        if coin in screener_active:
                            continue
                        pos = self.inventory.get_position(coin)
                        if abs(pos.size) < 1e-10:
                            logger.info(f"Screener: deactivating {coin} (no inventory)")
                            await self._deactivate_coin(coin)
                            self.screener.clear_idle_close(coin)
                        else:
                            # Has inventory — keep active for maker-only close
                            logger.info(
                                f"Screener: {coin} demoted but has inventory "
                                f"({pos.size:.0f}), keeping for maker close"
                            )

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
        """Flush pending fills and quote logs to MongoDB.

        Bug #6 fix: All MongoDB writes and Telegram flush are wrapped in
        asyncio.to_thread() to avoid blocking the event loop. Each is
        independently try/excepted so a Mongo/Telegram failure doesn't
        crash the engine.
        """
        # Flush fills with upsert (idempotent by oid + timestamp)
        try:
            fill_docs = self.fill_tracker.fills_to_mongo()
            if fill_docs:
                ops = []
                for doc in fill_docs:
                    filt = {"oid": doc["oid"], "timestamp": doc["timestamp"]}
                    ops.append(UpdateOne(filt, {"$set": doc}, upsert=True))
                if ops:
                    result = await asyncio.to_thread(
                        self._fills_col.bulk_write, ops, ordered=False
                    )
                    logger.debug(
                        f"Mongo fills flush: {result.upserted_count} new, "
                        f"{result.modified_count} updated"
                    )
        except Exception as e:
            logger.warning(f"MongoDB fills flush error: {e}")

        # Flush recent quote logs (append-only)
        try:
            log_docs = self.fill_tracker.quote_logs_to_mongo(
                since=self._last_mongo_flush
            )
            if log_docs:
                await asyncio.to_thread(
                    self._quotes_col.insert_many, log_docs, ordered=False
                )
        except Exception as e:
            logger.warning(f"MongoDB quote log flush error: {e}")

        # V2: Flush quote attempts to MongoDB
        try:
            if self._pending_attempts:
                await asyncio.to_thread(
                    self._attempts_col.insert_many,
                    list(self._pending_attempts),
                    ordered=False,
                )
                self._pending_attempts.clear()
        except Exception as e:
            logger.warning(f"MongoDB quote attempts flush error: {e}")

        # V2: Flush wallet scores to MongoDB
        try:
            wallet_docs = self.wallet_scorer.to_mongo_docs()
            if wallet_docs:
                wallet_col = self._db["hl_mm_wallet_scores"]
                ops = []
                for doc in wallet_docs:
                    ops.append(UpdateOne(
                        {"address": doc["address"]},
                        {"$set": doc},
                        upsert=True,
                    ))
                if ops:
                    await asyncio.to_thread(
                        wallet_col.bulk_write, ops, ordered=False
                    )
        except Exception as e:
            logger.warning(f"MongoDB wallet scores flush error: {e}")

        self._last_mongo_flush = time.time()

        # Also flush notifier queue (Telegram HTTP calls)
        try:
            await asyncio.to_thread(self.notifier.flush_queue)
        except Exception as e:
            logger.warning(f"Telegram flush error: {e}")

    # ==================================================================
    # Fill polling fallback (Gap 4)
    # ==================================================================

    async def _fill_poll_loop(self) -> None:
        """REST poll fallback for fill detection every 30s (Gap 4).

        Cross-references with known fills. Any new fills not yet tracked
        are processed as if they came from the WS subscription.
        """
        # FILL DETECTION HARDENING: Seed dedup hash set from initial REST query.
        # This prevents old fills from prior sessions being re-processed.
        # Wait 5s for WS snapshot to arrive first, then seed remaining.
        await asyncio.sleep(5)
        try:
            await self._seed_fill_hashes()
        except Exception as e:
            logger.warning(f"Fill hash seeding failed: {e}")

        # Wait remaining time before first real poll
        await asyncio.sleep(5)
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
        """Query HL REST for recent user fills and process any missed ones.

        FILL DETECTION HARDENING: Tracks consecutive failures.
        If REST fails 3x in a row AND no WS fills have been received recently,
        mark fill sync as unhealthy → quoting pauses until sync recovers.
        """
        try:
            # Bug #3 (Codex R4): Gate through shared rate limiter
            resp = await self._hl_rest_call(
                sync_requests.post,
                HL_INFO_API,
                json={"type": "userFills", "user": self.address},
                timeout=5,
            )
            if resp.status_code != 200:
                self._fill_poll_consecutive_failures += 1
                logger.warning(
                    f"Fill poll HTTP {resp.status_code} "
                    f"(consecutive failures: {self._fill_poll_consecutive_failures})"
                )
                self._update_fill_sync_health()
                return

            fills = resp.json()
            if not isinstance(fills, list):
                self._fill_poll_consecutive_failures += 1
                self._update_fill_sync_health()
                return

            # SUCCESS: Reset consecutive failure counter
            self._fill_poll_consecutive_failures = 0
            self._update_fill_sync_health()

            # Process recent fills — ONLY those with timestamp after engine start.
            # Without this filter, the REST poll re-processes old fills from prior
            # sessions when their hashes get evicted from the bounded dedup set.
            # This caused 50 ghost fills on 2026-05-03 that corrupted inventory.
            for fill_data in fills[-50:]:
                # TIMESTAMP GATE: reject any fill from before this engine started
                fill_time = fill_data.get("time", 0)
                if isinstance(fill_time, (int, float)) and fill_time > 0:
                    # HL fill timestamps are in milliseconds
                    fill_ts = fill_time / 1000.0 if fill_time > 1e12 else fill_time
                    if fill_ts < self._start_time:
                        continue

                # Build a unique hash for dedup
                fill_hash = self._fill_hash(fill_data)
                if fill_hash in self._known_fill_hashes:
                    continue

                self._known_fill_hashes[fill_hash] = True

                # Only process if it is a coin we are actively tracking
                # Codex #3: Also process fills for cooling coins
                coin = fill_data.get("coin", "")
                if coin not in self._active_coins and coin not in self._cooling_coins:
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
                        "source": "rest",
                    })

            # Bug #4 fix: Evict oldest entries via OrderedDict FIFO
            while len(self._known_fill_hashes) > self._known_fill_hashes_maxlen:
                self._known_fill_hashes.popitem(last=False)

        except Exception as e:
            self._fill_poll_consecutive_failures += 1
            logger.warning(
                f"Fill poll REST error: {e} "
                f"(consecutive failures: {self._fill_poll_consecutive_failures})"
            )
            self._update_fill_sync_health()

    def _update_fill_sync_health(self) -> None:
        """Update fill sync health status based on REST poll failures + WS state.

        Fill sync is HEALTHY if EITHER:
        - REST poll succeeded recently (consecutive_failures == 0), OR
        - WS userFills received a fill in the last 120s (WS is alive)

        Fill sync is UNHEALTHY (blind) if BOTH:
        - REST poll failed >= 3 consecutive times, AND
        - No WS fills received in the last 120s
        """
        now = time.time()
        rest_ok = self._fill_poll_consecutive_failures == 0
        ws_alive = (now - self._ws_fills_last_time) < 120.0 if self._ws_fills_last_time > 0 else False
        # WS subscription is healthy if it was set up (even without fills yet)
        ws_subscribed = self._ws_user_fills_sub_id is not None

        was_healthy = self._fill_sync_healthy

        # Codex #5 fix: "WS subscribed but no fills ever" is only healthy for
        # the first 30s after subscription. After that, if REST is also dead,
        # we have no proof WS is actually delivering fills.
        ws_grace_period = (now - self._start_time) < 30.0

        if rest_ok:
            self._fill_sync_healthy = True
        elif ws_alive:
            # WS delivered a fill recently — it's working
            self._fill_sync_healthy = True
        elif ws_subscribed and self._ws_fills_received == 0 and ws_grace_period:
            # WS just started, no fills yet but within grace period — OK
            self._fill_sync_healthy = True
        elif self._fill_poll_consecutive_failures >= self._fill_poll_max_failures:
            self._fill_sync_healthy = False
        else:
            self._fill_sync_healthy = True

        # Log state transitions
        if was_healthy and not self._fill_sync_healthy:
            logger.error(
                f"FILL SYNC UNHEALTHY: REST failed {self._fill_poll_consecutive_failures}x, "
                f"WS fills last seen {now - self._ws_fills_last_time:.0f}s ago. "
                f"QUOTING PAUSED until fill sync recovers."
            )
            self.notifier.notify_engine_event(
                "FILL_SYNC_BLIND",
                f"REST poll failed {self._fill_poll_consecutive_failures}x consecutively. "
                f"WS fills not received recently. Quoting paused — fills cannot be detected.",
            )
        elif not was_healthy and self._fill_sync_healthy:
            logger.info("FILL SYNC RECOVERED: fill detection operational, resuming quoting")
            self.notifier.notify_engine_event(
                "FILL_SYNC_RECOVERED",
                "Fill detection recovered. Quoting resumed.",
            )

    async def _seed_fill_hashes(self) -> None:
        """Seed the dedup hash set with existing fills from REST.

        Called once on startup BEFORE the first real poll. This prevents
        old fills from prior sessions being re-processed as new fills.
        Only adds hashes — does NOT process any fills.
        """
        try:
            resp = await self._hl_rest_call(
                sync_requests.post,
                HL_INFO_API,
                json={"type": "userFills", "user": self.address},
                timeout=5,
            )
            if resp.status_code != 200:
                logger.warning(f"Fill hash seeding HTTP {resp.status_code}")
                return

            fills = resp.json()
            if not isinstance(fills, list):
                return

            seeded = 0
            for fill_data in fills:
                fh = self._fill_hash(fill_data)
                if fh not in self._known_fill_hashes:
                    self._known_fill_hashes[fh] = True
                    seeded += 1

            while len(self._known_fill_hashes) > self._known_fill_hashes_maxlen:
                self._known_fill_hashes.popitem(last=False)

            logger.info(
                f"Seeded {seeded} fill hashes from REST "
                f"(total known: {len(self._known_fill_hashes)})"
            )

        except Exception as e:
            logger.warning(f"Fill hash seeding error: {e}")

    @staticmethod
    def _fill_hash(fill_data: dict) -> str:
        """Create unique hash for a fill record.

        Includes coin, side, size, price alongside oid/time/hash to prevent
        collisions when any field is missing or empty (bug #11 fix).
        """
        parts = [
            str(fill_data.get("oid", "")),
            str(fill_data.get("time", "")),
            str(fill_data.get("hash", "")),
            str(fill_data.get("coin", "")),
            str(fill_data.get("side", "")),
            str(fill_data.get("sz", fill_data.get("size", ""))),
            str(fill_data.get("px", fill_data.get("price", ""))),
        ]
        key = "|".join(parts)
        return hashlib.md5(key.encode()).hexdigest()

    # ==================================================================
    # HYPERCARE: Position reconciliation
    # ==================================================================

    async def _hypercare_position_check(self) -> None:
        """Compare inventory manager's position state vs actual exchange state.

        Runs every 5 minutes. If there's a position on-exchange that the
        inventory manager doesn't know about (or vice versa), raise alarm
        and potentially pause quoting.

        This catches the exact failure mode from the BIO incident: fills
        accumulating without inventory awareness.
        """
        try:
            # Fresh REST query for actual exchange positions
            state = await asyncio.to_thread(
                self.info.user_state, self.address
            )
            if not state:
                logger.warning("HYPERCARE: user_state returned None")
                return

            exchange_positions: dict[str, float] = {}
            for pos_data in state.get("assetPositions", []):
                p = pos_data.get("position", {})
                coin = p.get("coin", "")
                size = float(p.get("szi", 0))
                if abs(size) > 1e-10:
                    exchange_positions[coin] = size

            # Compare with inventory manager
            discrepancies = []
            all_coins = set(exchange_positions.keys()) | self._active_coins

            for coin in all_coins:
                exchange_size = exchange_positions.get(coin, 0)
                inv_pos = self.inventory.get_position(coin)
                inv_size = inv_pos.size

                diff = abs(exchange_size - inv_size)
                if diff > 1e-10:
                    # Check if this is a meaningful discrepancy
                    # (ignore dust from rounding)
                    mid = self.fv_engine.get_mid(coin) if self.fv_engine else 0
                    diff_usd = diff * mid if mid > 0 else diff
                    if diff_usd > 1.0:  # $1 threshold
                        discrepancies.append({
                            "coin": coin,
                            "exchange": exchange_size,
                            "inventory": inv_size,
                            "diff": diff,
                            "diff_usd": diff_usd,
                        })

            if discrepancies:
                msg_parts = ["HYPERCARE POSITION MISMATCH:"]
                for d in discrepancies:
                    msg_parts.append(
                        f"  {d['coin']}: exchange={d['exchange']:.6f} "
                        f"inv={d['inventory']:.6f} diff=${d['diff_usd']:.2f}"
                    )
                msg = "\n".join(msg_parts)
                logger.error(msg)

                # Telegram alert
                self.notifier.notify_engine_event(
                    "POSITION_MISMATCH",
                    "\n".join(
                        f"{d['coin']}: exch={d['exchange']:.4f} inv={d['inventory']:.4f} "
                        f"(${d['diff_usd']:.2f})"
                        for d in discrepancies
                    ),
                )

                # If any discrepancy > $10, pause all quoting
                max_disc = max(d["diff_usd"] for d in discrepancies)
                if max_disc > 10.0:
                    logger.critical(
                        f"HYPERCARE: ${max_disc:.2f} position discrepancy — "
                        f"pausing all quoting for 300s"
                    )
                    for coin in self._active_coins:
                        self.state_machine.force_pause(
                            coin, 300, f"HYPERCARE: position mismatch ${max_disc:.2f}"
                        )
            else:
                logger.info("HYPERCARE: positions reconciled OK")

        except Exception as e:
            logger.warning(f"HYPERCARE position check error: {e}")

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
                # Bug #3 (Codex R4): Gate through shared rate limiter (priority for startup)
                resp = await self._hl_rest_call(
                    sync_requests.post,
                    HL_INFO_API,
                    json={"type": "openOrders", "user": self.address},
                    timeout=5,
                    priority=True,
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
                        # Priority cancel
                        await self._hl_rest_call(self.exchange.cancel, coin, oid, priority=True)
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
            # Bug #3 (Codex R4): Gate through shared rate limiter
            state = await self._hl_rest_call(self.info.user_state, self.address, priority=True)
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

            # Reset PnL baseline AFTER loading inherited positions so their
            # unrealized PnL doesn't trigger daily stop on restart.
            self.inventory.reset_pnl_baseline()

        except Exception as e:
            logger.error(f"Startup position sync failed: {e}")

    # ==================================================================
    # Bug #4 (Codex R4): Resting notional computation
    # ==================================================================

    def _compute_total_resting_notional(self) -> float:
        """Sum notional of all currently resting quotes across all coins."""
        total = 0.0
        for coin in self._active_coins:
            bid_o, ask_o = self.quote_engine.get_active_orders(coin)
            if bid_o:
                total += bid_o.size * bid_o.price
            if ask_o:
                total += ask_o.size * ask_o.price
        return total

    # ==================================================================
    # Bybit hedge — DELETED (V2: hedge costs ~23bps, destroys PnL)
    # Preserved in git history: commit 630e58e
    # ==================================================================


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
        # Bug #9 fix: wrap cancel_all in to_thread (sync HL REST call)
        await asyncio.to_thread(self.quote_engine.cancel_all)

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
                # Bug #9 fix: wrap in to_thread
                await asyncio.to_thread(self.quote_engine.cancel_all)
            self._running = False
            return
        self._shutting_down = True
        logger.info("Graceful shutdown initiated...")
        self._running = False

        # Unsubscribe from WS userFills + orderUpdates
        self._unsubscribe_user_fills_ws()

        # Step 2: Cancel ALL resting orders with retry loop (Bug #2: up to 60s)
        verified_clean = False
        if self.quote_engine and not self.dry_run:
            logger.info("Shutdown: cancelling all resting orders...")
            # Bug #9 fix: wrap in to_thread
            await asyncio.to_thread(self.quote_engine.cancel_all)

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
            # Bug #9 fix: wrap in to_thread
            await asyncio.to_thread(self.quote_engine.cancel_all)

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
                                reduce_only=True,  # Bug #1 (Codex R4): close-hedge must be reduceOnly
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
            pnl = snapshot.daily_pnl if snapshot else 0
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

        fill_sync_str = "HEALTHY" if self._fill_sync_healthy else "BLIND"
        ws_fills_str = f"ws_fills={self._ws_fills_received}"
        rest_fail_str = f"rest_fail={self._fill_poll_consecutive_failures}"
        rate_breaker = "ACTIVE" if time.time() < self._fill_rate_breaker_until else "off"

        rpnl = self.inventory._realized_pnl
        fees = self.inventory._total_fees
        upnl = snapshot.daily_pnl - rpnl  # unrealized = total mark-to-market minus realized

        logger.info(
            f"STATUS: uptime={uptime/60:.0f}m ticks={self._tick_count} "
            f"coins={list(self._active_coins)} "
            f"gross=${snapshot.total_gross_notional:.2f} "
            f"net=${snapshot.total_net_exposure:.2f} "
            f"rpnl=${rpnl:.2f} upnl=${upnl:.2f} fees=${fees:.4f} "
            f"equity=${self.inventory._equity:.2f} "
            f"fill_sync={fill_sync_str} {ws_fills_str} {rest_fail_str} "
            f"rate_breaker={rate_breaker}"
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
