"""
HLBB Arb Orchestrator — main execution loop.

Ties together: WS price feeds → signal engine → dual-venue execution.
Handles position lifecycle: entry → monitoring → exit.
Crash recovery on startup.

Three modes:
- DRY_RUN: signal detection + logging only (no orders)
- PAPER: HL dry + Bybit demo (validate execution flow)
- LIVE: real orders on both venues
"""
import asyncio
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import requests as req_lib
from pymongo import MongoClient

from app.services.arb_hlbb.config import ArbConfig
from app.services.arb_hlbb.price_feed import DualPriceFeed, SpreadSnapshot
from app.services.arb_hlbb.signal_engine import SignalEngine, SignalEvent, TrackedPosition
from app.services.arb_hlbb.order_api import HLOrderAPI, BybitOrderAPI, OrderResult
from app.services.arb_hlbb.instrument_rules import InstrumentManager, PairRules

logger = logging.getLogger(__name__)


class RunMode(str, Enum):
    DRY_RUN = "dry_run"
    PAPER = "paper"
    LIVE = "live"


class RiskAction(str, Enum):
    CONTINUE = "continue"
    PAUSE = "pause"
    KILL = "kill"


@dataclass
class PositionRecord:
    """Full position record (persisted to MongoDB)."""
    position_id: str
    pair: str
    coin: str
    bb_symbol: str
    direction: str           # "SHORT_HL_LONG_BB" or "SHORT_BB_LONG_HL"
    state: str               # ENTERING, OPEN, EXITING, CLOSED, FAILED
    signal_spread_bps: float
    threshold_p90: float
    threshold_p25: float
    target_qty: float
    target_usd: float

    # Entry fills
    hl_entry: Optional[OrderResult] = None
    bb_entry: Optional[OrderResult] = None

    # Exit fills
    hl_exit: Optional[OrderResult] = None
    bb_exit: Optional[OrderResult] = None

    # Timing
    created_at: float = 0.0
    entry_time: float = 0.0
    exit_time: float = 0.0
    hold_seconds: float = 0.0

    # PnL
    gross_pnl_bps: float = 0.0
    fee_bps: float = 0.0
    net_pnl_bps: float = 0.0
    net_pnl_usd: float = 0.0

    exit_reason: str = ""
    entry_arb_direction: str = ""  # "HL_PREMIUM" or "BB_PREMIUM"


class Orchestrator:
    """Main arb execution loop."""

    def __init__(self, config: ArbConfig, mode: RunMode = RunMode.DRY_RUN):
        self.config = config
        self.mode = mode

        # Components
        self.price_feed: Optional[DualPriceFeed] = None
        self.signal_engine: Optional[SignalEngine] = None
        self.hl_api: Optional[HLOrderAPI] = None
        self.bb_api: Optional[BybitOrderAPI] = None
        self.instrument_mgr = InstrumentManager()

        # MongoDB
        self._mongo: Optional[MongoClient] = None
        self._positions_col = None
        self._trades_col = None

        # Active positions (in-memory mirror of MongoDB)
        self._positions: dict[str, PositionRecord] = {}
        self._entering_pairs: set[str] = set()  # FIX #2: pairs with entry in flight
        self._configured_pairs: set[str] = set()
        self._pair_cooldowns: dict[str, float] = {}  # pair → earliest re-entry time

        # Risk state
        self._daily_pnl_usd: float = 0.0
        self._daily_start: float = time.time()
        self._leg_failures: int = 0
        self._consecutive_leg_failures: int = 0
        self._risk_action: RiskAction = RiskAction.CONTINUE
        self._hl_query_address = os.getenv(
            "HL_QUERY_ADDRESS", "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
        )
        self._start_equity_usd: Optional[float] = None
        self._last_exchange_equity_usd: Optional[float] = None

        # Stats
        self.total_entries = 0
        self.total_exits = 0
        self.total_pnl_bps = 0.0
        self.total_pnl_usd = 0.0
        self._start_time = 0.0
        self._signal_count = 0

        # Control
        self._running = False
        self._signal_queue: asyncio.Queue = asyncio.Queue()
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None

    async def initialize(self, pairs: Optional[list[str]] = None):
        """Initialize all components."""
        pairs = pairs or self.config.default_pairs
        self._configured_pairs = set(pairs)
        logger.info(f"HLBB Arb Orchestrator — mode={self.mode.value}")
        logger.info(f"Pairs: {len(pairs)}")
        logger.info(f"Config: fee_rt={self.config.fee_rt_bps:.1f}bp, "
                     f"entry_floor={self.config.entry_min_spread_bps}bp, "
                     f"pos_size=${self.config.position_usd}")

        # MongoDB
        self._mongo = MongoClient(self.config.mongo_uri, serverSelectionTimeoutMS=5000)
        db_name = self.config.mongo_uri.rsplit("/", 1)[-1]
        db = self._mongo[db_name]
        self._positions_col = db[self.config.positions_collection]
        self._trades_col = db[self.config.trades_collection]

        # Ensure indexes
        self._positions_col.create_index("position_id", unique=True)
        self._positions_col.create_index("state")
        self._positions_col.create_index("pair")
        self._trades_col.create_index("timestamp")

        # Instrument rules
        logger.info("Fetching instrument rules...")
        self.instrument_mgr.fetch_rules(pairs)

        # Signal engine
        self.signal_engine = SignalEngine(self.config)
        logger.info("Seeding thresholds from MongoDB...")
        self.signal_engine.seed(pairs)

        # Price feeds (WS) with VWAP target for depth-aware spread
        self.price_feed = DualPriceFeed(
            pairs=pairs,
            hl_ws_url=self.config.hl_ws_url,
            bb_ws_url=self.config.bb_ws_url,
        )
        # Set VWAP target qty — rough estimate, will be pair-specific at order time
        # Use a mid-range price estimate ($1) for generic sizing
        self.price_feed._vwap_target_qty = self.config.position_usd  # $20 worth at $1/unit

        # Order APIs (only for LIVE mode — paper uses simulated fills like dry-run)
        # FIX #11: Paper mode must NOT initialize real APIs
        if self.mode == RunMode.LIVE:
            self._init_order_apis()
            if not self.hl_api or not self.bb_api:
                self._risk_action = RiskAction.KILL
                raise RuntimeError("LIVE mode requires both HL and Bybit order APIs")

            # Preflight: validate Bybit account mode + set leverage
            logger.info("Running Bybit preflight (account mode + leverage)...")
            ok, errors = self.bb_api.preflight_check(
                pairs=pairs, leverage=self.config.leverage
            )
            if not ok:
                for err in errors:
                    logger.error(f"PREFLIGHT FAIL: {err}")
                self._risk_action = RiskAction.KILL
                raise RuntimeError(
                    f"Bybit preflight failed: {'; '.join(errors)}"
                )
            logger.info("Bybit preflight passed (one-way mode, leverage set)")

            self._start_equity_usd = self._exchange_equity_usd()
            self._last_exchange_equity_usd = self._start_equity_usd
            logger.info(f"Live exchange equity baseline: ${self._start_equity_usd:.2f}")

        # Crash recovery
        await self._recover_positions()

        logger.info("Initialization complete")

    def _init_order_apis(self):
        """Initialize order APIs for live/paper modes. Retries HL on 429."""
        for attempt in range(3):
            try:
                self._init_hl_api()
                break
            except Exception as e:
                if "429" in str(e) and attempt < 2:
                    wait = 10 * (attempt + 1)
                    logger.warning(f"HL API rate limited, retrying in {wait}s...")
                    import time as _time
                    _time.sleep(wait)
                else:
                    logger.error(f"HL API init failed: {e}")

        self.bb_api = BybitOrderAPI(
            fill_poll_attempts=self.config.bybit_fill_poll_attempts,
            fill_poll_delay_s=self.config.bybit_fill_poll_delay_s,
        )
        if self.bb_api.api_key:
            logger.info("Bybit order API ready")
        else:
            logger.warning("BYBIT_API_KEY not set — Bybit orders disabled")

    def _init_hl_api(self):
        """Initialize HL order API (extracted for retry logic)."""
        from eth_account import Account
        from hyperliquid.exchange import Exchange
        from hyperliquid.info import Info

        hl_key = os.getenv("HL_PRIVATE_KEY", "")
        hl_query = self._hl_query_address

        if hl_key:
            wallet = Account.from_key(hl_key)
            info = Info("https://api.hyperliquid.xyz", skip_ws=True)
            exchange = Exchange(wallet, "https://api.hyperliquid.xyz",
                                account_address=hl_query)

            # Use REST exchange.order() for reliability.
            # WS order client saves ~400ms but returns "Empty response" failures
            # in production, causing naked-leg exposure. REST is proven reliable.
            self.hl_api = HLOrderAPI(
                ws_client=None, exchange=exchange, info=info
            )
            logger.info(f"HL order API ready (address: {hl_query[:10]}...)")
        else:
            logger.warning("HL_PRIVATE_KEY not set — HL orders disabled")

    def _exchange_equity_usd(self) -> float:
        """Return current combined venue equity for live risk limits.
        NOTE: Blocking call — use _exchange_equity_usd_async in async context.
        """
        if not self.hl_api or not self.bb_api:
            return 0.0
        hl_equity = self.hl_api.get_balance(self._hl_query_address)
        bb_equity = self.bb_api.get_balance()
        return hl_equity + bb_equity

    async def _exchange_equity_usd_async(self) -> float:
        """Non-blocking version of equity query."""
        if not self.hl_api or not self.bb_api:
            return 0.0
        loop = asyncio.get_event_loop()
        hl_equity, bb_equity = await asyncio.gather(
            loop.run_in_executor(
                None, self.hl_api.get_balance, self._hl_query_address
            ),
            loop.run_in_executor(None, self.bb_api.get_balance),
        )
        return hl_equity + bb_equity

    async def _refresh_live_risk_state(self) -> bool:
        """Update risk state from exchange balances, not modeled PnL."""
        if self.mode != RunMode.LIVE or not self.config.use_exchange_equity_risk:
            return True

        equity = await self._exchange_equity_usd_async()
        if equity <= 0:
            logger.error("Live risk check failed: could not query exchange equity")
            self._risk_action = RiskAction.PAUSE
            return False

        if self._start_equity_usd is None:
            self._start_equity_usd = equity

        self._last_exchange_equity_usd = equity
        realized_like_pnl = equity - self._start_equity_usd
        self._daily_pnl_usd = realized_like_pnl
        if realized_like_pnl < -self.config.max_daily_loss_usd:
            logger.error(
                f"KILL: live exchange equity drawdown ${realized_like_pnl:.2f} "
                f"< -${self.config.max_daily_loss_usd:.2f}"
            )
            self._risk_action = RiskAction.KILL
            self._notify_telegram(
                f"HLBB KILL: exchange equity drawdown ${realized_like_pnl:.2f}"
            )
            return False
        return True

    def _position_record_from_doc(self, doc: dict) -> PositionRecord:
        return PositionRecord(
            position_id=doc.get("position_id", ""),
            pair=doc.get("pair", ""),
            coin=doc.get("coin", ""),
            bb_symbol=doc.get("bb_symbol", ""),
            direction=doc.get("direction", ""),
            state=doc.get("state", ""),
            signal_spread_bps=doc.get("signal_spread_bps", 0),
            threshold_p90=doc.get("threshold_p90", 0),
            threshold_p25=doc.get("threshold_p25", 0),
            target_qty=doc.get("target_qty", 0),
            target_usd=doc.get("target_usd", 0),
            entry_time=doc.get("entry_time", time.time()),
            created_at=doc.get("created_at", 0),
            entry_arb_direction=doc.get("entry_arb_direction", ""),
        )

    def _expected_sides(self, pos: PositionRecord) -> tuple[int, str]:
        """Return expected HL sign and Bybit side for an open HLBB position."""
        if pos.direction == "SHORT_HL_LONG_BB":
            return -1, "Buy"
        return 1, "Sell"

    def _position_matches_exchange(
        self,
        pos: PositionRecord,
        hl_positions: dict[str, dict],
        bb_positions: dict[str, dict],
    ) -> tuple[bool, bool]:
        hl_sign, bb_side = self._expected_sides(pos)
        target = max(float(pos.target_qty or 0), 0.0)
        tol = max(target * self.config.recovery_qty_tolerance_pct, 1e-12)

        hl_pos = hl_positions.get(pos.coin)
        bb_pos = bb_positions.get(pos.bb_symbol)

        hl_ok = False
        if hl_pos:
            hl_size = float(hl_pos.get("size", 0.0))
            hl_ok = hl_size * hl_sign > 0 and abs(abs(hl_size) - target) <= tol

        bb_ok = False
        if bb_pos:
            bb_size = float(bb_pos.get("size", 0.0))
            bb_ok = bb_pos.get("side") == bb_side and abs(bb_size - target) <= tol

        return hl_ok, bb_ok

    def _exchange_position_maps(self) -> tuple[dict[str, dict], dict[str, dict]]:
        """Blocking version — use only in sync init paths (e.g., preflight)."""
        if self.mode != RunMode.LIVE or not self.hl_api or not self.bb_api:
            return {}, {}
        hl_positions = {
            p.get("coin", ""): p
            for p in self.hl_api.get_positions(self._hl_query_address)
        }
        bb_positions = {
            p.get("symbol", ""): p
            for p in self.bb_api.get_positions()
        }
        return hl_positions, bb_positions

    async def _exchange_position_maps_async(self) -> tuple[dict[str, dict], dict[str, dict]]:
        """Non-blocking version for use in async event loop."""
        if self.mode != RunMode.LIVE or not self.hl_api or not self.bb_api:
            return {}, {}
        loop = asyncio.get_event_loop()
        hl_raw, bb_raw = await asyncio.gather(
            loop.run_in_executor(
                None, self.hl_api.get_positions, self._hl_query_address
            ),
            loop.run_in_executor(None, self.bb_api.get_positions),
        )
        hl_positions = {p.get("coin", ""): p for p in hl_raw}
        bb_positions = {p.get("symbol", ""): p for p in bb_raw}
        return hl_positions, bb_positions

    async def _recover_positions(self):
        """Recover non-terminal positions from MongoDB on startup."""
        if self._positions_col is None:
            return

        non_terminal = list(self._positions_col.find({
            "state": {"$in": ["ENTERING", "OPEN", "EXITING"]}
        }))
        hl_positions, bb_positions = await self._exchange_position_maps_async()
        recovered_pairs: set[str] = set()

        if not non_terminal:
            logger.info("No positions to recover")
        else:
            logger.warning(f"Recovering {len(non_terminal)} positions...")

        for doc in non_terminal:
            pid = doc.get("position_id", "")
            state = doc.get("state", "")
            pair = doc.get("pair", "")
            pos = self._position_record_from_doc(doc)

            if self.mode == RunMode.LIVE:
                hl_ok, bb_ok = self._position_matches_exchange(
                    pos, hl_positions, bb_positions
                )

                if hl_ok and bb_ok:
                    pos.state = "OPEN"
                    self._positions[pair] = pos
                    recovered_pairs.add(pair)
                    self.signal_engine.register_position(TrackedPosition(
                        pair=pair,
                        direction=pos.direction,
                        entry_spread_bps=pos.signal_spread_bps,
                        entry_time=pos.entry_time,
                        entry_p90=pos.threshold_p90,
                        exit_p25=pos.threshold_p25,
                        position_id=pid,
                        entry_arb_direction=pos.entry_arb_direction or (
                            "HL_PREMIUM" if pos.direction == "SHORT_HL_LONG_BB" else "BB_PREMIUM"
                        ),
                    ))
                    self._positions_col.update_one(
                        {"position_id": pid},
                        {"$set": {"state": "OPEN", "updated_at": time.time()}},
                    )
                    logger.warning(f"  {pid} ({pair}): exchange legs matched — OPEN")
                    continue

                if state == "ENTERING":
                    unwind_ok = True
                    if hl_ok and not bb_ok:
                        hl_actual = hl_positions.get(pos.coin, {})
                        unwind_ok = await self._close_actual_hl_position(
                            pos.coin, float(hl_actual.get("size", 0.0))
                        )
                    elif bb_ok and not hl_ok:
                        bb_actual = bb_positions.get(pos.bb_symbol, {})
                        unwind_ok = await self._close_actual_bb_position(
                            pos.bb_symbol,
                            bb_actual.get("side", ""),
                            float(bb_actual.get("size", 0.0)),
                        )
                    self._positions_col.update_one(
                        {"position_id": pid},
                        {"$set": {
                            "state": "FAILED",
                            "exit_reason": f"CRASH_RECOVERY_ENTERING(unwind={'OK' if unwind_ok else 'FAILED'})",
                            "updated_at": time.time(),
                        }},
                    )
                    if not unwind_ok:
                        self._risk_action = RiskAction.KILL
                    logger.warning(f"  {pid} ({pair}): ENTERING → FAILED")
                    continue

                self._risk_action = RiskAction.KILL
                self._positions_col.update_one(
                    {"position_id": pid},
                    {"$set": {
                        "state": "RECONCILE_REQUIRED",
                        "exit_reason": f"CRASH_RECOVERY_MISMATCH(HL={hl_ok},BB={bb_ok})",
                        "updated_at": time.time(),
                    }},
                )
                self._notify_telegram(
                    f"HLBB KILL: recovery mismatch for {pair}. "
                    f"HL={hl_ok} BB={bb_ok}; manual reconciliation required."
                )
                logger.error(f"  {pid} ({pair}): exchange mismatch — KILL")
                continue

            if state == "ENTERING":
                self._positions_col.update_one(
                    {"position_id": pid},
                    {"$set": {"state": "FAILED", "exit_reason": "CRASH_RECOVERY"}}
                )
                logger.warning(f"  {pid} ({pair}): ENTERING → FAILED")

            elif state == "OPEN":
                pos.state = "OPEN"
                self._positions[pair] = pos
                recovered_pairs.add(pair)
                self.signal_engine.register_position(TrackedPosition(
                    pair=pair,
                    direction=pos.direction,
                    entry_spread_bps=pos.signal_spread_bps,
                    entry_time=pos.entry_time,
                    entry_p90=pos.threshold_p90,
                    exit_p25=pos.threshold_p25,
                    position_id=pid,
                    entry_arb_direction=pos.entry_arb_direction or (
                        "HL_PREMIUM" if pos.direction == "SHORT_HL_LONG_BB" else "BB_PREMIUM"
                    ),
                ))
                logger.warning(f"  {pid} ({pair}): OPEN — resuming exit monitoring")

            elif state == "EXITING":
                # Try to close any residual exchange positions
                if self.mode == RunMode.LIVE:
                    rules = self.instrument_mgr.get_rules(pair)
                    if rules:
                        closed = await self._close_pair_residuals(pos, rules)
                        if closed:
                            self._positions_col.update_one(
                                {"position_id": pid},
                                {"$set": {
                                    "state": "CLOSED",
                                    "exit_reason": "CRASH_RECOVERY_EXIT_CLOSED",
                                    "updated_at": time.time(),
                                }},
                            )
                            logger.warning(f"  {pid} ({pair}): EXITING → CLOSED (residuals cleared)")
                            continue
                    # If close failed, KILL
                    self._risk_action = RiskAction.KILL
                    self._positions_col.update_one(
                        {"position_id": pid},
                        {"$set": {
                            "state": "RECONCILE_REQUIRED",
                            "exit_reason": "CRASH_RECOVERY_EXIT_CLOSE_FAILED",
                            "updated_at": time.time(),
                        }},
                    )
                    self._notify_telegram(
                        f"HLBB KILL: EXITING recovery failed for {pair}. Manual close needed."
                    )
                    logger.error(f"  {pid} ({pair}): EXITING → RECONCILE_REQUIRED")
                else:
                    self._positions_col.update_one(
                        {"position_id": pid},
                        {"$set": {"state": "FAILED", "exit_reason": "CRASH_RECOVERY_EXIT"}}
                    )
                    logger.warning(f"  {pid} ({pair}): EXITING → FAILED (manual check needed)")

        if self.mode == RunMode.LIVE:
            self._check_live_orphans(hl_positions, bb_positions, recovered_pairs)

    def _check_live_orphans(
        self,
        hl_positions: dict[str, dict],
        bb_positions: dict[str, dict],
        recovered_pairs: set[str],
    ):
        """Pause live trading if configured HLBB pairs have untracked positions."""
        tracked = recovered_pairs | set(self._positions.keys())
        orphan_pairs = []
        for pair in self._configured_pairs:
            if pair in tracked:
                continue
            coin = pair.replace("-USDT", "")
            symbol = pair.replace("-", "")
            if coin in hl_positions or symbol in bb_positions:
                orphan_pairs.append(pair)

        if orphan_pairs:
            self._risk_action = RiskAction.KILL
            msg = f"HLBB KILL: untracked exchange positions on configured pairs: {orphan_pairs}"
            logger.error(msg)
            self._notify_telegram(msg)

    async def run(self):
        """Main loop: start feeds, process signals."""
        self._running = True
        self._start_time = time.time()
        self._event_loop = asyncio.get_event_loop()

        # Start price feeds
        logger.info("Starting WebSocket price feeds...")
        ok = self.price_feed.start(on_spread=self._on_spread_update)

        if not ok:
            logger.error("Failed to start price feeds — aborting")
            return

        # Wait for feeds to warm up
        logger.info("Waiting for price feed warmup (5s)...")
        await asyncio.sleep(5)

        metrics = self.price_feed.get_metrics()
        logger.info(f"Feed status: HL={metrics['hl_updates']} updates, "
                     f"BB={metrics['bb_updates']} updates, "
                     f"{metrics['pairs_with_data']} pairs with data")

        # Process signal queue
        logger.info("Processing signals...")
        try:
            while self._running:
                try:
                    signal = await asyncio.wait_for(
                        self._signal_queue.get(), timeout=1.0
                    )
                    await self._handle_signal(signal)
                except asyncio.TimeoutError:
                    pass

                # Periodic tasks
                await self._periodic_tasks()

        except KeyboardInterrupt:
            logger.info("Keyboard interrupt — shutting down")
        except Exception as e:
            logger.error(f"Fatal error in main loop: {e}", exc_info=True)
        finally:
            await self.shutdown()

    def _on_spread_update(self, snap: SpreadSnapshot):
        """Called from WS thread on every price update. Must be fast.
        FIX #3: Use call_soon_threadsafe for asyncio queue from WS threads.
        """
        if not self._running or not self.signal_engine:
            return

        signal = self.signal_engine.process_snapshot(snap)
        if signal:
            self._signal_count += 1
            # Thread-safe put into asyncio queue
            try:
                loop = self._event_loop
                if loop and loop.is_running():
                    loop.call_soon_threadsafe(self._signal_queue.put_nowait, signal)
            except (RuntimeError, asyncio.QueueFull):
                logger.warning("Signal queue full or loop closed — dropping signal")

    async def _handle_signal(self, signal: SignalEvent):
        """Handle a signal event."""
        if signal.signal_type == "ENTRY":
            await self._handle_entry(signal)
        elif signal.signal_type.startswith("EXIT"):
            await self._handle_exit(signal)

    async def _handle_entry(self, signal: SignalEvent):
        """Execute a new arb entry."""
        pair = signal.pair
        snap = signal.spread_snapshot

        # FIX #2: Guard against duplicate entries for same pair
        if pair in self._positions:
            return  # Already have a position (entering or open)
        if pair in self._entering_pairs:
            return  # Entry in flight

        # Re-entry cooldown check
        cooldown_until = self._pair_cooldowns.get(pair, 0)
        if time.time() < cooldown_until:
            remaining = cooldown_until - time.time()
            logger.debug(f"SKIP {pair}: re-entry cooldown ({remaining:.0f}s remaining)")
            return

        self._entering_pairs.add(pair)

        try:
            await self._do_entry(signal, pair, snap)
        finally:
            self._entering_pairs.discard(pair)

    def _get_fresh_spread(self, pair: str, max_age_s: Optional[float] = None) -> Optional[SpreadSnapshot]:
        """Return latest in-memory spread if both venue quotes are fresh enough."""
        if not self.price_feed:
            return None
        snap = self.price_feed.get_spread(pair)
        if not snap:
            return None
        max_age = self.config.entry_requote_max_age_s if max_age_s is None else max_age_s
        if time.time() - snap.ts > max_age:
            return None
        return snap

    def _requote_entry_snapshot(
        self,
        pair: str,
        signal: SignalEvent,
        original: SpreadSnapshot,
    ) -> Optional[SpreadSnapshot]:
        """Reprice immediately before live entry and abort if the edge collapsed."""
        if self.mode != RunMode.LIVE:
            return original

        fresh = self._get_fresh_spread(pair)
        if not fresh:
            logger.info(f"SKIP {pair}: no fresh quote for live entry")
            return None
        if fresh.direction != original.direction:
            logger.info(
                f"SKIP {pair}: direction flipped before entry "
                f"({original.direction} → {fresh.direction})"
            )
            return None

        min_live_entry = max(
            signal.threshold_p90,
            self.config.entry_min_spread_bps,
            self.config.fee_rt_bps + self.config.min_requote_edge_bps,
        )
        if fresh.best_spread_bps < min_live_entry:
            logger.info(
                f"SKIP {pair}: requote spread collapsed "
                f"({fresh.best_spread_bps:.1f}bp < {min_live_entry:.1f}bp)"
            )
            return None
        return fresh

    def _qty_mismatch_exceeds_tolerance(self, a: float, b: float) -> bool:
        larger = max(abs(a), abs(b), 1e-12)
        return abs(a - b) / larger > self.config.qty_mismatch_tolerance_pct

    async def _do_entry(self, signal: SignalEvent, pair: str, snap: SpreadSnapshot):
        """Actual entry logic (separated for entering guard)."""
        # Re-check capacity at execution time (signal engine checks at detection time,
        # but queued signals can exceed max_concurrent)
        total_open = len(self._positions) + len(self._entering_pairs)
        if total_open > self.config.max_concurrent:
            logger.info(f"SKIP {pair}: max_concurrent ({total_open}/{self.config.max_concurrent})")
            return

        # Risk check
        if self._risk_action != RiskAction.CONTINUE:
            logger.info(f"SKIP {pair}: risk={self._risk_action.value}")
            return
        # Equity-based risk check uses cached state (refreshed every 5min in periodic tasks)
        # Don't query exchange on every entry — burns HL rate limit

        # Funding blackout
        from app.services.arb.risk_manager import is_in_funding_blackout
        if is_in_funding_blackout():
            logger.info(f"SKIP {pair}: funding blackout")
            return

        # Daily loss check
        if self._daily_pnl_usd < -self.config.max_daily_loss_usd:
            logger.warning(f"SKIP {pair}: daily loss limit "
                          f"({self._daily_pnl_usd:.2f} < -{self.config.max_daily_loss_usd})")
            self._risk_action = RiskAction.PAUSE
            return

        # Get instrument rules
        rules = self.instrument_mgr.get_rules(pair)
        if not rules:
            logger.warning(f"SKIP {pair}: no instrument rules")
            return

        requoted = self._requote_entry_snapshot(pair, signal, snap)
        if not requoted:
            return
        snap = requoted

        # Compute qty
        mid_price = (snap.hl_bid + snap.bb_bid) / 2
        hl_qty, bb_qty = rules.common_qty(self.config.position_usd, mid_price)

        if not rules.is_tradeable(min(hl_qty, bb_qty), mid_price):
            logger.warning(f"SKIP {pair}: qty too small "
                          f"(hl={hl_qty}, bb={bb_qty}, price={mid_price:.4f})")
            return

        target_qty = min(hl_qty, bb_qty)

        # VWAP check: compute actual executable spread at our target qty
        # Uses full L2 depth instead of TOB-only spread
        if self.price_feed:
            hl_q = self.price_feed.hl_feed.get_quote(rules.coin)
            bb_q = self.price_feed.bb_feed.get_quote(rules.bb_symbol)

            if snap.direction == "HL_PREMIUM":
                vwap_sell = hl_q.vwap_sell(target_qty)  # sell into HL bids
                vwap_buy = bb_q.vwap_buy(target_qty)    # buy from BB asks
                vwap_spread = (vwap_sell - vwap_buy) / vwap_buy * 10000 if vwap_buy > 0 else 0
            else:
                vwap_sell = bb_q.vwap_sell(target_qty)   # sell into BB bids
                vwap_buy = hl_q.vwap_buy(target_qty)     # buy from HL asks
                vwap_spread = (vwap_sell - vwap_buy) / vwap_buy * 10000 if vwap_buy > 0 else 0

            if vwap_spread < self.config.entry_min_spread_bps:
                logger.info(
                    f"SKIP {pair}: VWAP spread {vwap_spread:.1f}bp < "
                    f"{self.config.entry_min_spread_bps:.0f}bp floor "
                    f"(TOB={snap.best_spread_bps:.1f}bp, qty={target_qty:.0f})"
                )
                return

        # Determine direction + apply aggression for better fill rates
        aggression_mult = self.config.order_aggression_bps / 10000
        if snap.direction == "HL_PREMIUM":
            # SHORT HL (sell), LONG BB (buy)
            direction = "SHORT_HL_LONG_BB"
            hl_side_buy = False
            bb_side = "Buy"
            hl_price = snap.hl_bid * (1 - aggression_mult)   # sell slightly below bid
            bb_price = snap.bb_ask * (1 + aggression_mult)   # buy slightly above ask
        else:
            # SHORT BB (sell), LONG HL (buy)
            direction = "SHORT_BB_LONG_HL"
            hl_side_buy = True
            bb_side = "Sell"
            hl_price = snap.hl_ask * (1 + aggression_mult)   # buy slightly above ask
            bb_price = snap.bb_bid * (1 - aggression_mult)   # sell slightly below bid

        # Create position record
        position_id = f"hlbb_{uuid.uuid4().hex[:12]}"
        pos = PositionRecord(
            position_id=position_id,
            pair=pair,
            coin=rules.coin,
            bb_symbol=rules.bb_symbol,
            direction=direction,
            state="ENTERING",
            signal_spread_bps=snap.best_spread_bps,
            threshold_p90=signal.threshold_p90,
            threshold_p25=signal.threshold_p25,
            target_qty=min(hl_qty, bb_qty),
            target_usd=self.config.position_usd,
            created_at=time.time(),
        )

        # Log signal (all modes)
        entry_msg = (
            f"ENTRY SIGNAL {pair} {direction} | "
            f"spread={snap.best_spread_bps:.1f}bp > P90={signal.threshold_p90:.1f}bp | "
            f"excess={signal.excess_bps:.1f}bp | "
            f"qty={pos.target_qty:.4f} (${self.config.position_usd})"
        )
        logger.info(entry_msg)
        self._notify_telegram(f"[HLBB {self.mode.value}] {entry_msg}")

        if self.mode in {RunMode.DRY_RUN, RunMode.PAPER}:
            # Simulate position for exit monitoring
            pos.entry_arb_direction = snap.direction
            self._positions[pair] = pos
            self.signal_engine.register_position(TrackedPosition(
                pair=pair, direction=direction,
                entry_spread_bps=snap.best_spread_bps,
                entry_time=time.time(),
                entry_p90=signal.threshold_p90,
                exit_p25=signal.threshold_p25,
                position_id=position_id,
                entry_arb_direction=snap.direction,
            ))
            pos.state = "OPEN"
            pos.entry_time = time.time()
            self.total_entries += 1

            # Log to MongoDB
            self._log_trade("ENTRY", pos, snap)
            return

        # LIVE: Persist to MongoDB
        self._persist_position(pos)

        # CONCURRENT entry: fire BOTH legs simultaneously.
        # BB fills in ~180ms (captures spread while fresh), HL fills in ~1000ms.
        # With 20bp aggression on HL, fill rate is near 100% on thick books.
        # Naked leg risk is handled below (unwind handlers for both directions).
        hl_price = rules.round_hl_price(hl_price, is_buy=hl_side_buy)
        hl_cloid = f"0x{uuid.uuid4().hex}"  # HL Cloid: 0x + 32 hex chars
        bb_link_id = f"hlbb_{position_id[-8:]}_bb_e"

        # Use HL-precision qty for BOTH legs to minimize mismatch.
        # HL rounds internally to szDecimals — send BB the same qty
        # so both legs target the same amount.
        common_qty = rules.round_hl_qty(min(hl_qty, bb_qty))

        hl_result, bb_result = await asyncio.gather(
            self.hl_api.place_ioc(
                coin=rules.coin, is_buy=hl_side_buy,
                sz=common_qty, price=hl_price,
                cloid=hl_cloid,
            ) if self.hl_api else self._dummy_order("HL"),
            self.bb_api.place_ioc(
                symbol=rules.bb_symbol, side=bb_side,
                qty=rules.format_bb_qty(rules.round_bb_qty(common_qty)),
                price=rules.format_bb_price(
                    rules.round_bb_price_buy(bb_price) if bb_side == "Buy"
                    else rules.round_bb_price_sell(bb_price)
                ),
                order_link_id=bb_link_id,
            ) if self.bb_api else self._dummy_order("BB"),
        )

        pos.hl_entry = hl_result
        pos.bb_entry = bb_result

        logger.info(
            f"ENTRY FILLS: HL={hl_result.status}(qty={hl_result.filled_qty}, "
            f"{hl_result.latency_ms:.0f}ms"
            f"{', err=' + hl_result.error if hl_result.error else ''}) "
            f"BB={bb_result.status}(qty={bb_result.filled_qty}, "
            f"{bb_result.latency_ms:.0f}ms"
            f"{', err=' + bb_result.error if bb_result.error else ''})"
        )

        if hl_result.success and bb_result.success:
            # FIX #1: Use ACTUAL filled qty (min of both) for delta-neutral sizing
            actual_hl = hl_result.filled_qty
            actual_bb = bb_result.filled_qty
            actual_qty = min(actual_hl, actual_bb)

            if actual_qty <= 0:
                logger.error(f"ENTRY: both 'success' but zero fills — marking FAILED")
                pos.state = "FAILED"
                pos.exit_reason = "ZERO_FILL"
                self._persist_position(pos)
                return

            if self._qty_mismatch_exceeds_tolerance(actual_hl, actual_bb):
                logger.warning(
                    f"SIZE MISMATCH: HL={actual_hl} BB={actual_bb}. "
                    f"Trimming excess to keep delta-neutral."
                )
                trim_ok = await self._trim_entry_mismatch(
                    rules=rules,
                    hl_entry_is_buy=hl_side_buy,
                    bb_entry_side=bb_side,
                    actual_hl=actual_hl,
                    actual_bb=actual_bb,
                )
                if not trim_ok:
                    pos.state = "RECONCILE_REQUIRED"
                    pos.exit_reason = "ENTRY_SIZE_MISMATCH_TRIM_FAILED"
                    self._risk_action = RiskAction.KILL
                    self._persist_position(pos)
                    self._notify_telegram(
                        f"HLBB KILL: failed to trim entry size mismatch for {pair}. "
                        f"HL={actual_hl} BB={actual_bb}"
                    )
                    return

            pos.target_qty = actual_qty  # Track actual, not target
            pos.state = "OPEN"
            pos.entry_time = time.time()
            pos.entry_arb_direction = snap.direction
            self._positions[pair] = pos
            self.total_entries += 1
            self._consecutive_leg_failures = 0

            # Register for exit monitoring
            self.signal_engine.register_position(TrackedPosition(
                pair=pair, direction=direction,
                entry_spread_bps=snap.best_spread_bps,
                entry_time=pos.entry_time,
                entry_p90=signal.threshold_p90,
                exit_p25=signal.threshold_p25,
                position_id=position_id,
                entry_arb_direction=snap.direction,
            ))

            self._persist_position(pos)
            self._log_trade("ENTRY", pos, snap)
            return  # Success — don't fall through to failure cooldown

        elif hl_result.success and not bb_result.success:
            # HL filled, BB failed — UNWIND HL using ACTUAL filled qty
            actual_hl = hl_result.filled_qty or hl_qty
            logger.error(f"NAKED LEG: HL filled ({actual_hl}), BB failed — unwinding HL")
            unwind_result = await self._unwind_hl(rules.coin, actual_hl, not hl_side_buy)
            pos.state = "FAILED"
            pos.exit_reason = f"BB_LEG_FAILURE(unwind={'OK' if unwind_result else 'FAILED'})"
            self._leg_failures += 1
            self._consecutive_leg_failures += 1
            self._persist_position(pos)
            if not unwind_result:
                self._notify_telegram(
                    f"ALERT: NAKED LEG UNWIND FAILED {pair} HL side. "
                    f"Manual intervention needed!"
                )

        elif not hl_result.success and bb_result.success:
            # BB filled, HL failed — UNWIND BB using ACTUAL filled qty
            actual_bb = bb_result.filled_qty or float(bb_qty)
            logger.error(f"NAKED LEG: BB filled ({actual_bb}), HL failed — unwinding BB")
            opposite_side = "Sell" if bb_side == "Buy" else "Buy"
            unwind_result = await self._unwind_bb(
                rules.bb_symbol, str(actual_bb), opposite_side
            )
            pos.state = "FAILED"
            pos.exit_reason = f"HL_LEG_FAILURE(unwind={'OK' if unwind_result else 'FAILED'})"
            self._leg_failures += 1
            self._consecutive_leg_failures += 1
            self._persist_position(pos)
            if not unwind_result:
                self._notify_telegram(
                    f"ALERT: NAKED LEG UNWIND FAILED {pair} BB side. "
                    f"Manual intervention needed!"
                )

        else:
            # Neither filled
            pos.state = "FAILED"
            pos.exit_reason = "BOTH_LEGS_FAILED"
            self._persist_position(pos)

        # Set cooldown after any entry failure (leg failure or both failed)
        self._pair_cooldowns[pair] = time.time() + self.config.reentry_cooldown_after_fail_s

        # Check consecutive failures
        if self._consecutive_leg_failures >= self.config.max_leg_failures:
            logger.error(f"PAUSE: {self._consecutive_leg_failures} consecutive leg failures")
            self._risk_action = RiskAction.PAUSE

    async def _trim_entry_mismatch(
        self,
        rules: PairRules,
        hl_entry_is_buy: bool,
        bb_entry_side: str,
        actual_hl: float,
        actual_bb: float,
    ) -> bool:
        """Immediately reduce the larger filled leg after a partial mismatch."""
        diff = abs(actual_hl - actual_bb)
        if diff <= 0:
            return True

        if actual_hl > actual_bb:
            # Close excess HL using the opposite side of the entry leg.
            return await self._unwind_hl(rules.coin, diff, not hl_entry_is_buy)

        opposite_bb_side = "Sell" if bb_entry_side == "Buy" else "Buy"
        formatted_diff = rules.format_bb_qty(rules.round_bb_qty(diff))
        return await self._unwind_bb(rules.bb_symbol, formatted_diff, opposite_bb_side)

    async def _handle_exit(self, signal: SignalEvent):
        """Execute exit for an open position."""
        pair = signal.pair
        snap = signal.spread_snapshot
        pos = self._positions.get(pair)

        if not pos:
            self.signal_engine.unregister_position(pair)
            return

        if self.mode == RunMode.LIVE:
            fresh_exit = self._get_fresh_spread(pair, max_age_s=2.0)
            if fresh_exit:
                snap = fresh_exit

        hold_s = time.time() - pos.entry_time

        # MODELED PnL (from spread snapshots — for dry-run/paper only)
        if pos.entry_arb_direction == "HL_PREMIUM":
            exit_dir_spread = snap.spread_hl_over_bb_bps
        elif pos.entry_arb_direction == "BB_PREMIUM":
            exit_dir_spread = snap.spread_bb_over_hl_bps
        else:
            exit_dir_spread = snap.best_spread_bps
        modeled_pnl_bps = pos.signal_spread_bps - exit_dir_spread - self.config.fee_rt_bps
        modeled_pnl_usd = modeled_pnl_bps / 10000 * pos.target_usd

        # Use modeled PnL as default (overridden by actual fill PnL for live)
        pnl_bps = modeled_pnl_bps
        pnl_usd = modeled_pnl_usd

        win = "WIN" if modeled_pnl_bps > 0 else "LOSS"
        exit_msg = (
            f"{win} {signal.signal_type} {pair} | "
            f"entry={pos.signal_spread_bps:.1f}bp exit={exit_dir_spread:.1f}bp | "
            f"modeled={modeled_pnl_bps:.1f}bp hold={hold_s:.0f}s"
        )
        logger.info(exit_msg)

        if self.mode in {RunMode.DRY_RUN, RunMode.PAPER}:
            # Just record
            pos.state = "CLOSED"
            pos.exit_time = time.time()
            pos.hold_seconds = hold_s
            pos.net_pnl_bps = pnl_bps
            pos.net_pnl_usd = pnl_usd
            pos.exit_reason = signal.signal_type

            self.total_exits += 1
            self.total_pnl_bps += pnl_bps
            self.total_pnl_usd += pnl_usd
            self._daily_pnl_usd += pnl_usd

            self.signal_engine.unregister_position(pair)
            del self._positions[pair]

            # Set re-entry cooldown (longer after stop-loss)
            cooldown = (self.config.reentry_cooldown_after_fail_s
                        if "STOP_LOSS" in signal.signal_type
                        else self.config.reentry_cooldown_s)
            self._pair_cooldowns[pair] = time.time() + cooldown

            self._log_trade("EXIT", pos, snap)
            return

        # LIVE: Submit exit orders
        rules = self.instrument_mgr.get_rules(pair)
        if not rules:
            logger.error(f"EXIT {pair}: no instrument rules!")
            return

        pos.state = "EXITING"
        self._persist_position(pos)

        # Determine exit sides (reverse of entry)
        if pos.direction == "SHORT_HL_LONG_BB":
            hl_buy = True   # close HL short → buy
            bb_side = "Sell"  # close BB long → sell
        else:
            hl_buy = False   # close HL long → sell
            bb_side = "Buy"   # close BB short → buy

        # Apply aggression on exit too — same HL book movement issue as entry.
        aggression_mult = self.config.order_aggression_bps / 10000
        hl_price = snap.hl_ask * (1 + aggression_mult) if hl_buy else snap.hl_bid * (1 - aggression_mult)
        hl_price = rules.round_hl_price(hl_price, is_buy=hl_buy)
        bb_price = snap.bb_bid if bb_side == "Sell" else snap.bb_ask

        hl_cloid = f"0x{uuid.uuid4().hex}"  # HL Cloid: 0x + 32 hex chars
        bb_link_id = f"hlbb_{pos.position_id[-8:]}_bb_x"
        hl_result, bb_result = await asyncio.gather(
            self.hl_api.place_ioc(
                coin=rules.coin, is_buy=hl_buy,
                sz=pos.target_qty, price=hl_price, reduce_only=True,
                cloid=hl_cloid,
            ) if self.hl_api else self._dummy_order("HL"),
            self.bb_api.place_ioc(
                symbol=rules.bb_symbol, side=bb_side,
                qty=rules.format_bb_qty(pos.target_qty),
                price=rules.format_bb_price(
                    rules.round_bb_price_buy(bb_price) if bb_side == "Buy"
                    else rules.round_bb_price_sell(bb_price)
                ),
                reduce_only=True,
                order_link_id=bb_link_id,
            ) if self.bb_api else self._dummy_order("BB"),
        )

        pos.hl_exit = hl_result
        pos.bb_exit = bb_result
        pos.exit_time = time.time()
        pos.hold_seconds = hold_s
        pos.exit_reason = signal.signal_type

        # Compute ACTUAL PnL from fill prices (not modeled from spread snapshots)
        if (pos.hl_entry and pos.bb_entry and
                hl_result.avg_price > 0 and bb_result.avg_price > 0 and
                pos.hl_entry.avg_price > 0 and pos.bb_entry.avg_price > 0):
            qty = pos.target_qty
            if pos.direction == "SHORT_HL_LONG_BB":
                # HL: sell entry, buy exit. BB: buy entry, sell exit.
                hl_leg = (pos.hl_entry.avg_price - hl_result.avg_price) * qty
                bb_leg = (bb_result.avg_price - pos.bb_entry.avg_price) * qty
            else:
                # HL: buy entry, sell exit. BB: sell entry, buy exit.
                hl_leg = (hl_result.avg_price - pos.hl_entry.avg_price) * qty
                bb_leg = (pos.bb_entry.avg_price - bb_result.avg_price) * qty

            # Fees: 4 taker fills
            fee_usd = qty * (
                pos.hl_entry.avg_price * self.config.hl_taker_bps / 10000 +
                pos.bb_entry.avg_price * self.config.bb_taker_bps / 10000 +
                hl_result.avg_price * self.config.hl_taker_bps / 10000 +
                bb_result.avg_price * self.config.bb_taker_bps / 10000
            )

            actual_pnl_usd = hl_leg + bb_leg - fee_usd
            mid_price = (pos.hl_entry.avg_price + pos.bb_entry.avg_price) / 2
            actual_pnl_bps = actual_pnl_usd / (qty * mid_price) * 10000 if qty * mid_price > 0 else 0

            pnl_bps = actual_pnl_bps
            pnl_usd = actual_pnl_usd

            logger.info(
                f"ACTUAL PnL {pair}: HL_leg=${hl_leg:.6f} BB_leg=${bb_leg:.6f} "
                f"fees=${fee_usd:.6f} net=${actual_pnl_usd:.6f} ({actual_pnl_bps:.1f}bp) "
                f"[modeled was {modeled_pnl_bps:.1f}bp]"
            )
        else:
            logger.warning(f"EXIT {pair}: missing fill prices, using modeled PnL")

        pos.net_pnl_bps = pnl_bps
        pos.net_pnl_usd = pnl_usd

        actual_win = "WIN" if pnl_bps > 0 else "LOSS"
        self._notify_telegram(
            f"[HLBB {self.mode.value}] {actual_win} {pair} | "
            f"ACTUAL={pnl_bps:.1f}bp (${pnl_usd:.4f}) "
            f"modeled={modeled_pnl_bps:.1f}bp | hold={hold_s:.0f}s"
        )

        # Check for FULL fills, not just any fill.
        hl_full = hl_result.success and hl_result.status == "FILLED"
        bb_full = bb_result.success and bb_result.status == "FILLED"

        if hl_full and bb_full:
            pos.state = "CLOSED"
            self.total_exits += 1
            self.total_pnl_bps += pnl_bps
            self.total_pnl_usd += pnl_usd
            self._daily_pnl_usd += pnl_usd
        else:
            # Partial or failed exit — query exchange state and close residual legs.
            logger.error(
                f"EXIT INCOMPLETE: HL={hl_result.status}(qty={hl_result.filled_qty}) "
                f"BB={bb_result.status}(qty={bb_result.filled_qty})"
            )
            residual_closed = await self._close_pair_residuals(pos, rules)
            if residual_closed:
                pos.state = "CLOSED"
                pos.exit_reason += f"_RESIDUAL_CLOSED(HL={hl_result.status},BB={bb_result.status})"
                # PnL is estimated (modeled from spread, not actual fill prices
                # from aggressive unwind). Flag for post-hoc analysis.
                pos.exit_reason += "_PNL_ESTIMATED"
                self.total_exits += 1
                self.total_pnl_bps += pnl_bps
                self.total_pnl_usd += pnl_usd
                self._daily_pnl_usd += pnl_usd
            else:
                pos.state = "RECONCILE_REQUIRED"
                pos.exit_reason += f"_PARTIAL(HL={hl_result.status},BB={bb_result.status})"
                self._risk_action = RiskAction.KILL
                self._notify_telegram(
                    f"HLBB KILL: partial exit unresolved for {pair}. "
                    f"HL={hl_result.status} BB={bb_result.status}"
                )

        self.signal_engine.unregister_position(pair)
        self._positions.pop(pair, None)
        self._persist_position(pos)
        self._log_trade("EXIT", pos, snap)

        # Set re-entry cooldown (longer after failures)
        is_bad_exit = "STOP_LOSS" in signal.signal_type or "RECONCILE" in pos.state
        cooldown = (self.config.reentry_cooldown_after_fail_s if is_bad_exit
                    else self.config.reentry_cooldown_s)
        self._pair_cooldowns[pair] = time.time() + cooldown

    async def _close_actual_hl_position(self, coin: str, signed_size: float) -> bool:
        """Close an actual HL position by size sign."""
        qty = abs(float(signed_size or 0.0))
        if qty <= 0:
            return True
        return await self._unwind_hl(coin, qty, is_buy=signed_size < 0)

    async def _close_actual_bb_position(self, symbol: str, side: str, size: float) -> bool:
        """Close an actual Bybit position by side."""
        qty = abs(float(size or 0.0))
        if qty <= 0:
            return True
        if side not in {"Buy", "Sell"}:
            logger.error(f"BB close: unknown side for {symbol}: {side}")
            return False
        close_side = "Sell" if side == "Buy" else "Buy"
        return await self._unwind_bb(symbol, str(qty), close_side)

    async def _close_pair_residuals(self, pos: PositionRecord, rules: PairRules) -> bool:
        """Close any actual residual exchange position for this HLBB pair."""
        if self.mode != RunMode.LIVE:
            return False

        for attempt in range(2):
            hl_positions, bb_positions = await self._exchange_position_maps_async()
            hl_actual = hl_positions.get(rules.coin)
            bb_actual = bb_positions.get(rules.bb_symbol)

            tasks = []
            if hl_actual:
                tasks.append(self._close_actual_hl_position(
                    rules.coin, float(hl_actual.get("size", 0.0))
                ))
            if bb_actual:
                tasks.append(self._close_actual_bb_position(
                    rules.bb_symbol,
                    bb_actual.get("side", ""),
                    float(bb_actual.get("size", 0.0)),
                ))

            if not tasks:
                return True

            results = await asyncio.gather(*tasks, return_exceptions=True)
            if all(r is True for r in results):
                await asyncio.sleep(0.5)
                hl_positions, bb_positions = await self._exchange_position_maps_async()
                if rules.coin not in hl_positions and rules.bb_symbol not in bb_positions:
                    return True

            logger.warning(
                f"Residual close attempt {attempt + 1} failed for {pos.pair}: {results}"
            )
            await asyncio.sleep(0.5)

        return False

    async def _unwind_hl(self, coin: str, qty: float, is_buy: bool) -> bool:
        """Emergency unwind: close an HL position. Returns True if successful.
        FIX #5: Return success, use actual qty, retry once.
        """
        if not self.hl_api:
            return False
        # Find pair rules for price formatting
        pair_key = f"{coin}-USDT"
        rules = self.instrument_mgr.get_rules(pair_key)
        for attempt in range(2):
            try:
                # Reuse existing info instance for speed; only create new if needed
                info = getattr(self.hl_api, 'info', None)
                if not info:
                    from hyperliquid.info import Info
                    info = Info("https://api.hyperliquid.xyz", skip_ws=True)
                loop = asyncio.get_event_loop()
                mids = await loop.run_in_executor(None, info.all_mids)
                mid = float(mids.get(coin, 0))
                if mid <= 0:
                    logger.error(f"HL unwind: no mid price for {coin}")
                    return False
                # 2% aggressive to ensure fill, with proper precision
                price = mid * (1.02 if is_buy else 0.98)
                if rules:
                    price = rules.round_hl_price(price, is_buy=is_buy)
                result = await self.hl_api.place_ioc(
                    coin, is_buy, qty, price, reduce_only=True
                )
                if result and result.success:
                    logger.info(f"HL unwind OK: {coin} qty={result.filled_qty}")
                    return True
                logger.warning(f"HL unwind attempt {attempt+1} failed: {result}")
            except Exception as e:
                logger.error(f"HL unwind attempt {attempt+1} error: {e}")
            if attempt == 0:
                await asyncio.sleep(0.5)
        return False

    async def _unwind_bb(self, symbol: str, qty: str, side: str) -> bool:
        """Emergency unwind: close a Bybit position. Returns True if successful.
        FIX #5: Return success, retry once.
        """
        if not self.bb_api:
            return False
        # Find pair rules for price formatting
        pair_key = symbol.replace("USDT", "-USDT")
        rules = self.instrument_mgr.get_rules(pair_key)
        for attempt in range(2):
            try:
                resp = req_lib.get(
                    "https://api.bybit.com/v5/market/tickers",
                    params={"category": "linear", "symbol": symbol},
                    timeout=5,
                ).json()
                tickers = resp.get("result", {}).get("list", [])
                if not tickers:
                    logger.error(f"BB unwind: no ticker for {symbol}")
                    return False
                price = float(tickers[0].get("lastPrice", 0))
                # 2% aggressive with proper tick rounding
                aggressive = price * (1.02 if side == "Buy" else 0.98)
                if rules:
                    aggressive = (rules.round_bb_price_buy(aggressive) if side == "Buy"
                                  else rules.round_bb_price_sell(aggressive))
                    price_str = rules.format_bb_price(aggressive)
                else:
                    price_str = f"{aggressive:.6f}"
                result = await self.bb_api.place_ioc(
                    symbol, side, qty, price_str, reduce_only=True
                )
                if result and result.success:
                    logger.info(f"BB unwind OK: {symbol} qty={result.filled_qty}")
                    return True
                logger.warning(f"BB unwind attempt {attempt+1} failed: {result}")
            except Exception as e:
                logger.error(f"BB unwind attempt {attempt+1} error: {e}")
            if attempt == 0:
                await asyncio.sleep(0.5)
        return False

    async def _dummy_order(self, venue: str) -> OrderResult:
        """Dummy order for when API is not initialized."""
        return OrderResult(success=False, venue=venue, status="NO_API",
                          error="API not initialized")

    def _persist_position(self, pos: PositionRecord):
        """Upsert position to MongoDB."""
        if self._positions_col is None:
            return
        doc = {
            "position_id": pos.position_id,
            "pair": pos.pair,
            "coin": pos.coin,
            "bb_symbol": pos.bb_symbol,
            "direction": pos.direction,
            "state": pos.state,
            "signal_spread_bps": pos.signal_spread_bps,
            "threshold_p90": pos.threshold_p90,
            "threshold_p25": pos.threshold_p25,
            "target_qty": pos.target_qty,
            "target_usd": pos.target_usd,
            "entry_time": pos.entry_time,
            "exit_time": pos.exit_time,
            "hold_seconds": pos.hold_seconds,
            "net_pnl_bps": pos.net_pnl_bps,
            "net_pnl_usd": pos.net_pnl_usd,
            "exit_reason": pos.exit_reason,
            "entry_arb_direction": pos.entry_arb_direction,
            "created_at": pos.created_at,
            "updated_at": time.time(),
        }
        # Add entry/exit fill details if available
        for prefix, result in [("hl_entry", pos.hl_entry), ("bb_entry", pos.bb_entry),
                               ("hl_exit", pos.hl_exit), ("bb_exit", pos.bb_exit)]:
            if result:
                doc[prefix] = {
                    "success": result.success,
                    "order_id": result.order_id,
                    "filled_qty": result.filled_qty,
                    "avg_price": result.avg_price,
                    "status": result.status,
                    "latency_ms": result.latency_ms,
                }

        self._positions_col.update_one(
            {"position_id": pos.position_id},
            {"$set": doc},
            upsert=True,
        )

    def _log_trade(self, event: str, pos: PositionRecord, snap: SpreadSnapshot):
        """Log trade to MongoDB trades collection."""
        if self._trades_col is None:
            return
        self._trades_col.insert_one({
            "event": event,
            "position_id": pos.position_id,
            "pair": pos.pair,
            "direction": pos.direction,
            "signal_spread_bps": pos.signal_spread_bps,
            "exit_spread_bps": snap.best_spread_bps if event == "EXIT" else None,
            "net_pnl_bps": pos.net_pnl_bps if event == "EXIT" else None,
            "net_pnl_usd": pos.net_pnl_usd if event == "EXIT" else None,
            "hold_seconds": pos.hold_seconds if event == "EXIT" else None,
            "exit_reason": pos.exit_reason if event == "EXIT" else None,
            "threshold_p90": pos.threshold_p90,
            "threshold_p25": pos.threshold_p25,
            "mode": self.mode.value,
            "timestamp": datetime.now(timezone.utc),
        })

    def _notify_telegram(self, text: str):
        """Send Telegram notification (fire-and-forget, non-blocking).

        Uses run_in_executor to avoid blocking the asyncio event loop.
        Falls back to synchronous if no event loop is running.
        """
        if not self.config.telegram_enabled:
            return
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.run_in_executor(None, self._send_telegram_sync, text)
            else:
                self._send_telegram_sync(text)
        except RuntimeError:
            self._send_telegram_sync(text)

    def _send_telegram_sync(self, text: str):
        """Synchronous Telegram send (runs in thread pool)."""
        try:
            token = os.getenv("TELEGRAM_BOT_TOKEN", "")
            if not token:
                logger.warning("Telegram: no bot token")
                return
            chat_id = self.config.telegram_chat_id
            if not chat_id:
                logger.warning("Telegram: no chat_id configured")
                return
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            resp = req_lib.post(url, json={
                "chat_id": chat_id,
                "text": text,
            }, timeout=5)
            if not resp.json().get("ok"):
                logger.warning(f"Telegram send failed: {resp.json().get('description', '')}")
        except Exception as e:
            logger.warning(f"Telegram notify failed: {e}")

    _last_periodic = 0.0  # Will fire immediately on first check, then every 60s

    async def _periodic_tasks(self):
        """Run periodic health checks and status logging.
        FIX #6: Independent max-hold watchdog (fires even if feeds are stale).
        """
        now = time.time()
        if now - self._last_periodic < 10:  # check every 10s for max-hold
            return
        self._last_periodic = now

        # KILL/PAUSE watchdog: flatten all open positions immediately
        if self._risk_action in {RiskAction.KILL, RiskAction.PAUSE} and self._positions:
            if self.mode == RunMode.LIVE:
                logger.error(
                    f"RISK {self._risk_action.value}: flattening {len(self._positions)} "
                    f"open positions"
                )
                for pair, pos in list(self._positions.items()):
                    rules = self.instrument_mgr.get_rules(pair)
                    if rules:
                        closed = await self._close_pair_residuals(pos, rules)
                        if closed:
                            pos.state = "CLOSED"
                            pos.exit_reason = f"RISK_{self._risk_action.value}_FLATTEN"
                        else:
                            pos.state = "RECONCILE_REQUIRED"
                            pos.exit_reason = f"RISK_{self._risk_action.value}_FLATTEN_FAILED"
                        pos.exit_time = time.time()
                        pos.hold_seconds = now - pos.entry_time
                        self.signal_engine.unregister_position(pair)
                        self._positions.pop(pair, None)
                        self._persist_position(pos)
                        self._notify_telegram(
                            f"HLBB {self._risk_action.value} flatten {pair}: {pos.state}"
                        )
            else:
                # Dry-run: just close simulated positions
                for pair, pos in list(self._positions.items()):
                    pos.state = "CLOSED"
                    pos.exit_reason = f"RISK_{self._risk_action.value}_FLATTEN"
                    self.signal_engine.unregister_position(pair)
                    self._positions.pop(pair, None)

        # Max-hold watchdog: force-exit positions held too long (independent of feeds)
        for pair, pos in list(self._positions.items()):
            if pos.state == "OPEN" and pos.entry_time > 0:
                hold_s = now - pos.entry_time
                if hold_s > self.config.max_hold_s:
                    logger.warning(
                        f"MAX_HOLD WATCHDOG: {pair} held {hold_s:.0f}s > "
                        f"{self.config.max_hold_s}s — forcing exit"
                    )
                    # Create a synthetic exit signal
                    snap = self.price_feed.get_spread(pair) if self.price_feed else None
                    if snap:
                        signal = SignalEvent(
                            pair=pair, signal_type="EXIT_MAX_HOLD",
                            spread_snapshot=snap,
                            threshold_p90=pos.threshold_p90,
                            threshold_p25=pos.threshold_p25,
                            excess_bps=0, timestamp=now,
                        )
                        await self._handle_exit(signal)
                    elif self.mode == RunMode.LIVE:
                        rules = self.instrument_mgr.get_rules(pair)
                        if rules and await self._close_pair_residuals(pos, rules):
                            pos.state = "CLOSED"
                            pos.exit_time = time.time()
                            pos.hold_seconds = hold_s
                            pos.exit_reason = "EXIT_MAX_HOLD_NO_FEED"
                            self.signal_engine.unregister_position(pair)
                            self._positions.pop(pair, None)
                            self._persist_position(pos)
                            self._notify_telegram(
                                f"HLBB forced max-hold close without feed for {pair}"
                            )
                        else:
                            self._risk_action = RiskAction.KILL
                            self._notify_telegram(
                                f"HLBB KILL: {pair} max-hold exceeded and force close failed. "
                                f"Manual close needed!"
                            )
                    else:
                        self._notify_telegram(
                            f"ALERT: {pair} max-hold exceeded but no price data. "
                            f"Manual close needed!"
                        )

        # Feed health check
        if self.price_feed and self._positions:
            for pair in list(self._positions.keys()):
                snap = self.price_feed.get_spread(pair)
                if snap and now - snap.ts > 30:
                    logger.warning(f"STALE FEED: {pair} last update {now - snap.ts:.0f}s ago")

        # Status log (every 60s)
        if now - getattr(self, '_last_status_log', 0) < 60:
            return
        self._last_status_log = now
        # Refresh equity every 5 min (not every 60s) to avoid HL 429 storms
        if now - getattr(self, '_last_equity_check', 0) > 300:
            self._last_equity_check = now
            await self._refresh_live_risk_state()

        uptime_min = (now - self._start_time) / 60
        metrics = self.price_feed.get_metrics() if self.price_feed else {}

        # Reset daily PnL at midnight UTC
        from datetime import datetime, timezone
        utc_now = datetime.now(timezone.utc)
        if utc_now.hour == 0 and utc_now.minute < 2:
            if now - self._daily_start > 3600:
                self._daily_pnl_usd = 0.0
                self._daily_start = now
                # Reset equity baseline so daily loss limit is truly daily, not lifetime
                if self.mode == RunMode.LIVE:
                    self._start_equity_usd = self._exchange_equity_usd()
                    logger.info(f"Daily reset: equity baseline → ${self._start_equity_usd:.2f}")

        # Status line
        open_count = len(self._positions)
        pair_status = self.signal_engine.get_pair_status() if self.signal_engine else []
        ready = sum(1 for p in pair_status if p["ready"])
        viable = sum(1 for p in pair_status if p["viable"])

        status_line = (
            f"[{uptime_min:.0f}m] ready={ready} viable={viable} "
            f"open={open_count} entries={self.total_entries} exits={self.total_exits} "
            f"cumPnL={self.total_pnl_bps:.1f}bp (${self.total_pnl_usd:.3f}) "
            f"dailyPnL=${self._daily_pnl_usd:.3f} "
            f"signals={self._signal_count} "
            f"risk={self._risk_action.value} | "
            f"HL={metrics.get('hl_updates', 0)} BB={metrics.get('bb_updates', 0)}"
        )
        logger.info(status_line)

        # Telegram performance report every 15 minutes
        if now - getattr(self, '_last_tg_report', 0) > 900:
            self._last_tg_report = now
            wr = (self.total_exits and
                  sum(1 for _ in [] ) or 0)  # placeholder — use trade log
            equity_str = ""
            if self._last_exchange_equity_usd:
                equity_str = f"\nEquity: ${self._last_exchange_equity_usd:.2f}"
            self._notify_telegram(
                f"[HLBB {self.mode.value}] {uptime_min:.0f}m uptime\n"
                f"Trades: {self.total_entries}E/{self.total_exits}X | "
                f"Open: {open_count}\n"
                f"PnL: {self.total_pnl_bps:.1f}bp (${self.total_pnl_usd:.3f})\n"
                f"Daily: ${self._daily_pnl_usd:.3f} | "
                f"Signals: {self._signal_count}\n"
                f"Risk: {self._risk_action.value} | "
                f"Pairs: {ready}rdy/{viable}viable"
                f"{equity_str}"
            )

    async def shutdown(self):
        """Graceful shutdown."""
        self._running = False
        logger.info("Shutting down...")

        if self.price_feed:
            self.price_feed.stop()

        # Log final stats
        logger.info(
            f"FINAL: entries={self.total_entries} exits={self.total_exits} "
            f"totalPnL={self.total_pnl_bps:.1f}bp (${self.total_pnl_usd:.3f}) "
            f"legFailures={self._leg_failures}"
        )

        if self._mongo:
            self._mongo.close()

    def get_status(self) -> dict:
        """Return current status for external queries."""
        return {
            "mode": self.mode.value,
            "running": self._running,
            "uptime_min": (time.time() - self._start_time) / 60 if self._start_time else 0,
            "open_positions": len(self._positions),
            "total_entries": self.total_entries,
            "total_exits": self.total_exits,
            "total_pnl_bps": round(self.total_pnl_bps, 1),
            "total_pnl_usd": round(self.total_pnl_usd, 4),
            "daily_pnl_usd": round(self._daily_pnl_usd, 4),
            "risk_action": self._risk_action.value,
            "leg_failures": self._leg_failures,
            "signal_count": self._signal_count,
            "feed_healthy": self.price_feed.is_healthy if self.price_feed else False,
        }
