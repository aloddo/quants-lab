"""
H2 ExecutionEngine — Order state machine + LegCoordinator for dual-leg execution.

Core invariant: NEVER leave a naked leg open.
If one leg fills and the other doesn't within 500ms, unwind the filled leg.

Fill source: WS events from OrderFeed (primary). REST only for reconciliation.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from app.services.arb.order_feed import FillEvent, OrderUpdate, FillSource
from app.services.arb.price_feed import SpreadSnapshot

logger = logging.getLogger(__name__)


# ── Order State Machine ─────────────────────────────────────────

class OrderState(str, Enum):
    IDLE = "idle"
    SUBMITTED = "submitted"
    ACKED = "acked"
    PARTIAL = "partial"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCELLED = "cancelled"
    CANCEL_PENDING = "cancel_pending"


@dataclass
class LegState:
    """Tracks the state of a single order leg (one side of the arb)."""
    venue: str                  # "bybit" | "binance"
    symbol: str
    side: str                   # "Buy" | "Sell"
    order_id: str = ""
    state: OrderState = OrderState.IDLE
    submitted_at: float = 0.0
    acked_at: float = 0.0
    filled_at: float = 0.0
    target_qty: float = 0.0
    target_price: float = 0.0
    filled_qty: float = 0.0
    filled_price: float = 0.0
    avg_fill_price: float = 0.0
    leaves_qty: float = 0.0
    fee: float = 0.0
    fee_asset: str = ""
    exec_ids: list = field(default_factory=list)
    cancel_sent_at: float = 0.0

    @property
    def is_terminal(self) -> bool:
        return self.state in (OrderState.FILLED, OrderState.REJECTED, OrderState.CANCELLED)

    @property
    def is_filled(self) -> bool:
        return self.state == OrderState.FILLED

    @property
    def has_any_fill(self) -> bool:
        """True if ANY quantity was filled (even partial)."""
        return self.filled_qty > 0

    @property
    def fill_pct(self) -> float:
        if self.target_qty <= 0:
            return 0
        return self.filled_qty / self.target_qty

    def on_fill(self, fill: FillEvent):
        """Update state from a fill event. Idempotent — deduplicates by exec_id."""
        if fill.exec_id in self.exec_ids:
            logger.debug(f"Duplicate fill ignored: {fill.exec_id}")
            return
        self.filled_qty += fill.qty
        self.fee += fill.fee
        self.fee_asset = fill.fee_asset
        self.filled_at = fill.local_ts
        self.exec_ids.append(fill.exec_id)

        # Weighted average fill price
        if self.filled_qty > 0:
            prev_value = self.avg_fill_price * (self.filled_qty - fill.qty)
            self.avg_fill_price = (prev_value + fill.price * fill.qty) / self.filled_qty

        self.leaves_qty = max(0, self.target_qty - self.filled_qty)

        if self.leaves_qty <= 0 or fill.order_status == "Filled":
            self.state = OrderState.FILLED
        else:
            self.state = OrderState.PARTIAL

    def on_order_update(self, update: OrderUpdate):
        """Update state from an order status event. Also copies fill qty/price if present."""
        if update.status == "New" and self.state == OrderState.SUBMITTED:
            self.state = OrderState.ACKED
            self.acked_at = update.local_ts
        elif update.status == "Cancelled":
            if self.state != OrderState.FILLED:
                self.state = OrderState.CANCELLED
        elif update.status == "Rejected":
            self.state = OrderState.REJECTED
        elif update.status == "Filled":
            self.state = OrderState.FILLED
            # Copy fill data if cumulative is greater than what we have
            # (covers missed partial fills — same logic as PartiallyFilled)
            if update.filled_qty > self.filled_qty:
                self.filled_qty = update.filled_qty
                self.avg_fill_price = update.avg_price
                self.leaves_qty = update.leaves_qty
        elif update.status == "PartiallyFilled":
            self.state = OrderState.PARTIAL
            if update.filled_qty > self.filled_qty:
                self.filled_qty = update.filled_qty
                self.avg_fill_price = update.avg_price
                self.leaves_qty = update.leaves_qty


# ── Entry/Exit Results ──────────────────────────────────────────

class EntryOutcome(str, Enum):
    SUCCESS = "success"             # both legs filled
    LEG_FAILURE = "leg_failure"     # one filled, other didn't, unwound
    BOTH_MISSED = "both_missed"     # neither filled
    BOTH_REJECTED = "both_rejected"


@dataclass
class EntryResult:
    outcome: EntryOutcome
    bybit_leg: LegState
    binance_leg: LegState
    signal_spread_bps: float = 0.0    # spread at signal time
    actual_spread_bps: float = 0.0    # spread from actual fill prices
    slippage_bps: float = 0.0         # signal spread - actual spread
    total_fees_bps: float = 0.0
    entry_latency_ms: float = 0.0     # time from signal to both fills
    leg_failure_venue: str = ""       # which leg failed (if any)
    unwind_pnl_usd: float = 0.0      # loss from unwinding failed leg


@dataclass
class ExitResult:
    outcome: str                      # "success", "partial", "leg_failure"
    bybit_leg: LegState
    binance_leg: LegState
    actual_exit_spread_bps: float = 0.0
    exit_latency_ms: float = 0.0


# ── Leg Coordinator ─────────────────────────────────────────────

class LegCoordinator:
    """
    Ensures both legs fill atomically, or neither does.

    Uses WS fill events (via asyncio.Event notifications from OrderFeed callbacks)
    to detect fills. NO REST polling during execution.
    """

    FILL_TIMEOUT_S = 0.5        # 500ms max wait for both fills
    CANCEL_TIMEOUT_S = 0.2      # 200ms to confirm cancel
    UNWIND_LIMIT_TIMEOUT_S = 0.2  # aggressive limit unwind
    UNWIND_MARKET_TIMEOUT_S = 1.0  # market order last resort

    def __init__(self, submit_order_fn, cancel_order_fn, check_order_fn=None,
                 check_position_fn=None):
        """
        submit_order_fn(venue, symbol, side, qty, price, order_type, client_order_id) -> order_id
        cancel_order_fn(venue, symbol, order_id) -> bool
        check_order_fn(venue, symbol, order_id) -> dict with status/filled_qty/avg_price (REST fallback)
        check_position_fn(venue, symbol) -> float (net position size, for post-trade verification)
        """
        self._submit = submit_order_fn
        self._cancel = cancel_order_fn
        self._check_order = check_order_fn  # REST fallback for fill confirmation
        self._check_position = check_position_fn  # Position verification after failures

        # Fill events are signaled here by OrderFeed callbacks
        self._fill_events: dict[str, asyncio.Event] = {}  # order_id -> event
        self._leg_states: dict[str, LegState] = {}  # order_id -> LegState
        self._unmatched_fills: list = []  # Buffer for fills that arrive before legs are registered
        self._UNMATCHED_BUFFER_MAX = 50  # Cap to prevent unbounded growth
        self._UNMATCHED_TTL_S = 60.0  # Drop fills older than 60s

    def register_leg(self, leg: LegState):
        """Register a leg for fill tracking."""
        self._fill_events[leg.order_id] = asyncio.Event()
        self._leg_states[leg.order_id] = leg

    def on_fill(self, fill: FillEvent):
        """Called by OrderFeed when a fill arrives. Updates leg state and signals.

        Matches by exchange order_id first, then falls back to client_order_id
        (orderLinkId). This handles the race condition where WS fills arrive
        before execute_entry() maps exchange IDs to legs.
        """
        leg = self._leg_states.get(fill.order_id)
        matched_key = fill.order_id

        # Fallback: try client order ID (handles WS fill during submit race)
        if not leg:
            client_id = getattr(fill, 'client_order_id', '')
            if client_id:
                leg = self._leg_states.get(client_id)
                matched_key = client_id
                if leg:
                    # Also register the exchange order_id for future lookups
                    self._leg_states[fill.order_id] = leg
                    if client_id in self._fill_events:
                        self._fill_events[fill.order_id] = self._fill_events[client_id]
                    logger.debug(f"Fill matched via client_order_id: {client_id} -> {fill.order_id}")

        if not leg:
            # Buffer unmatched fills for later replay (covers submit latency)
            # TTL: drop stale fills to prevent unbounded growth
            now = time.time()
            self._unmatched_fills = [
                f for f in self._unmatched_fills
                if now - f.local_ts < self._UNMATCHED_TTL_S
            ]
            if len(self._unmatched_fills) < self._UNMATCHED_BUFFER_MAX:
                self._unmatched_fills.append(fill)
                logger.warning(f"Unmatched fill buffered: {fill.venue} {fill.order_id} qty={fill.qty}")
            else:
                logger.error(f"Unmatched fill buffer FULL ({self._UNMATCHED_BUFFER_MAX}), dropping: {fill.venue} {fill.order_id}")
            return

        leg.on_fill(fill)
        if leg.is_filled:
            event = self._fill_events.get(matched_key) or self._fill_events.get(fill.order_id)
            if event:
                event.set()

    def on_order_update(self, update: OrderUpdate):
        """Called by OrderFeed when an order status changes."""
        leg = self._leg_states.get(update.order_id)
        if leg:
            leg.on_order_update(update)
            # Signal on terminal states too (rejected, cancelled)
            if leg.is_terminal:
                event = self._fill_events.get(update.order_id)
                if event:
                    event.set()

    async def execute_entry(
        self,
        signal: SpreadSnapshot,
        bb_side: str,
        bn_side: str,
        qty_bb: float,
        qty_bn: float,
        price_bb: float,
        price_bn: float,
    ) -> EntryResult:
        """
        Submit both legs concurrently, wait for WS fills, handle failures.

        Critical invariants:
        - Register legs BEFORE submitting (so WS fills are never lost)
        - Handle asymmetric submit failure (one succeeds, other fails)
        - Handle partial fills (any fill residual must be unwound)
        - Wait for actual cancel confirmation, not arbitrary sleep
        """
        t0 = time.time()

        # BUG FIX #1: Generate client order IDs and PRE-REGISTER before submitting
        # This ensures WS fill events can be matched even if they arrive before
        # the REST response returns the exchange order ID
        import uuid
        bb_client_id = f"h2_bb_{uuid.uuid4().hex[:12]}"
        bn_client_id = f"h2_bn_{uuid.uuid4().hex[:12]}"

        bb_leg = LegState(
            venue="bybit", symbol=signal.symbol, side=bb_side,
            target_qty=qty_bb, target_price=price_bb,
            order_id=bb_client_id,  # use client ID as initial tracking key
        )
        bn_leg = LegState(
            venue="binance", symbol=signal.symbol, side=bn_side,
            target_qty=qty_bn, target_price=price_bn,
            order_id=bn_client_id,
        )

        # Pre-register BEFORE submitting so WS fills are never lost
        self.register_leg(bb_leg)
        self.register_leg(bn_leg)

        # Submit both legs with client order IDs
        bb_oid = None
        bn_oid = None
        bb_err = None
        bn_err = None

        async def submit_bb():
            nonlocal bb_oid, bb_err
            try:
                bb_oid = await self._submit("bybit", signal.symbol, bb_side, qty_bb, price_bb, "limit", bb_client_id)
            except Exception as e:
                bb_err = e

        async def submit_bn():
            nonlocal bn_oid, bn_err
            try:
                bn_oid = await self._submit("binance", signal.symbol, bn_side, qty_bn, price_bn, "limit", bn_client_id)
            except Exception as e:
                bn_err = e

        await asyncio.gather(submit_bb(), submit_bn())

        # Handle asymmetric submit failure
        if bb_err and bn_err:
            logger.error(f"Both submits failed: bb={bb_err}, bn={bn_err}")
            return EntryResult(
                outcome=EntryOutcome.BOTH_REJECTED,
                bybit_leg=bb_leg, binance_leg=bn_leg,
                entry_latency_ms=(time.time() - t0) * 1000,
            )

        if bb_err and bn_oid:
            # Bybit failed, Binance succeeded — cancel Binance immediately
            logger.warning(f"Bybit submit failed ({bb_err}), unwinding Binance {bn_oid}")
            bn_leg.order_id = bn_oid
            bn_leg.state = OrderState.SUBMITTED
            self.register_leg(bn_leg)
            await self._safe_cancel("binance", signal.symbol, bn_oid)
            # Wait for cancel confirmation
            event = self._fill_events.get(bn_oid)
            if event:
                try:
                    await asyncio.wait_for(event.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
            if bn_leg.has_any_fill:
                await self._emergency_unwind(bn_leg, signal.symbol)
            self._cleanup_leg(bn_oid)
            return EntryResult(
                outcome=EntryOutcome.LEG_FAILURE, bybit_leg=bb_leg, binance_leg=bn_leg,
                leg_failure_venue="bybit", entry_latency_ms=(time.time() - t0) * 1000,
            )

        if bn_err and bb_oid:
            # Binance failed, Bybit succeeded — cancel Bybit immediately
            logger.warning(f"Binance submit failed ({bn_err}), unwinding Bybit {bb_oid}")
            bb_leg.order_id = bb_oid
            bb_leg.state = OrderState.SUBMITTED
            self.register_leg(bb_leg)
            await self._safe_cancel("bybit", signal.symbol, bb_oid)
            event = self._fill_events.get(bb_oid)
            if event:
                try:
                    await asyncio.wait_for(event.wait(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
            if bb_leg.has_any_fill:
                await self._emergency_unwind(bb_leg, signal.symbol)
            self._cleanup_leg(bb_oid)
            return EntryResult(
                outcome=EntryOutcome.LEG_FAILURE, bybit_leg=bb_leg, binance_leg=bn_leg,
                leg_failure_venue="binance", entry_latency_ms=(time.time() - t0) * 1000,
            )

        # Both submits succeeded — update legs with exchange order IDs
        # Also register the exchange IDs so WS events using either ID are matched
        # IMPORTANT: Do NOT overwrite state if a WS fill already matched via client ID
        if bb_oid and bb_oid != bb_client_id:
            self._fill_events[bb_oid] = self._fill_events[bb_client_id]
            self._leg_states[bb_oid] = bb_leg
        bb_leg.order_id = bb_oid or bb_client_id
        if bb_leg.state == OrderState.IDLE:
            bb_leg.state = OrderState.SUBMITTED
        bb_leg.submitted_at = bb_leg.submitted_at or time.time()
        if bn_oid and bn_oid != bn_client_id:
            self._fill_events[bn_oid] = self._fill_events[bn_client_id]
            self._leg_states[bn_oid] = bn_leg
        bn_leg.order_id = bn_oid or bn_client_id
        if bn_leg.state == OrderState.IDLE:
            bn_leg.state = OrderState.SUBMITTED
        bn_leg.submitted_at = bn_leg.submitted_at or time.time()

        # Replay any fills that arrived during the submit race window
        if self._unmatched_fills:
            buffered = self._unmatched_fills[:]
            self._unmatched_fills.clear()
            for fill in buffered:
                logger.info(f"Replaying buffered fill: {fill.venue} {fill.order_id} qty={fill.qty}")
                self.on_fill(fill)

        # Wait for both fills via WS events (check both client and exchange IDs)
        bb_event = self._fill_events.get(bb_oid) or self._fill_events.get(bb_client_id)
        bn_event = self._fill_events.get(bn_oid) or self._fill_events.get(bn_client_id)

        try:
            await asyncio.wait_for(
                asyncio.gather(bb_event.wait(), bn_event.wait()),
                timeout=self.FILL_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            # WS timeout — try REST fallback for any unfilled legs
            if self._check_order:
                for leg, oid in [(bb_leg, bb_oid), (bn_leg, bn_oid)]:
                    if oid and not leg.is_filled:
                        try:
                            order_info = await self._check_order(leg.venue, signal.symbol, oid)
                            if order_info and float(order_info.get("filled_qty", 0)) > 0:
                                leg.filled_qty = float(order_info["filled_qty"])
                                leg.avg_fill_price = float(order_info.get("avg_price", 0))
                                leg.state = OrderState.FILLED if leg.filled_qty >= leg.target_qty * 0.99 else OrderState.PARTIAL
                                logger.info(f"REST fill confirmed: {leg.venue} {oid} qty={leg.filled_qty}")
                        except Exception as e:
                            logger.warning(f"REST fill check failed for {leg.venue}: {e}")

        # Evaluate outcome — check has_any_fill (not just is_filled) for partial handling
        bb_fully = bb_leg.is_filled
        bn_fully = bn_leg.is_filled
        bb_any = bb_leg.has_any_fill
        bn_any = bn_leg.has_any_fill

        if bb_fully and bn_fully:
            # SUCCESS: both legs fully filled
            actual_spread = self._compute_actual_spread(bb_leg, bn_leg, bb_side)
            result = EntryResult(
                outcome=EntryOutcome.SUCCESS,
                bybit_leg=bb_leg, binance_leg=bn_leg,
                signal_spread_bps=signal.spread_bps,
                actual_spread_bps=actual_spread,
                slippage_bps=signal.spread_bps - actual_spread,
                entry_latency_ms=(time.time() - t0) * 1000,
            )
            self._cleanup_leg(bb_oid)
            self._cleanup_leg(bn_oid)
            return result

        # Cancel both unfilled/partial legs and wait for confirmation
        await asyncio.gather(
            self._safe_cancel("bybit", signal.symbol, bb_oid) if not bb_fully else asyncio.sleep(0),
            self._safe_cancel("binance", signal.symbol, bn_oid) if not bn_fully else asyncio.sleep(0),
        )

        # Wait for cancel confirmations + possible fill-after-cancel
        await asyncio.sleep(0.2)
        # Re-check terminal states
        bb_fully = bb_leg.is_filled
        bn_fully = bn_leg.is_filled
        bb_any = bb_leg.has_any_fill
        bn_any = bn_leg.has_any_fill

        if bb_fully and bn_fully:
            # Fill-after-cancel: both are now filled
            actual_spread = self._compute_actual_spread(bb_leg, bn_leg, bb_side)
            result = EntryResult(
                outcome=EntryOutcome.SUCCESS,
                bybit_leg=bb_leg, binance_leg=bn_leg,
                signal_spread_bps=signal.spread_bps,
                actual_spread_bps=actual_spread,
                slippage_bps=signal.spread_bps - actual_spread,
                entry_latency_ms=(time.time() - t0) * 1000,
            )
            self._cleanup_leg(bb_oid)
            self._cleanup_leg(bn_oid)
            return result

        # SAFETY CHECK: Before unwinding, verify actual exchange positions.
        # This catches the race condition where a leg filled but we missed it.
        if self._check_position:
            for leg, venue_label in [(bb_leg, "bybit"), (bn_leg, "binance")]:
                if not leg.has_any_fill:
                    try:
                        actual_pos = await self._check_position(venue_label, signal.symbol)
                        if actual_pos and abs(actual_pos) >= leg.target_qty * 0.9:
                            logger.warning(
                                f"POSITION VERIFICATION: {venue_label} has position {actual_pos} "
                                f"but coordinator thinks unfilled! Correcting."
                            )
                            leg.filled_qty = abs(actual_pos)
                            leg.avg_fill_price = leg.target_price  # approximate
                            leg.state = OrderState.FILLED
                    except Exception as e:
                        logger.warning(f"Position verification failed for {venue_label}: {e}")

            # Re-evaluate after position verification
            bb_fully = bb_leg.is_filled
            bn_fully = bn_leg.is_filled
            bb_any = bb_leg.has_any_fill
            bn_any = bn_leg.has_any_fill

            if bb_fully and bn_fully:
                actual_spread = self._compute_actual_spread(bb_leg, bn_leg, bb_side)
                result = EntryResult(
                    outcome=EntryOutcome.SUCCESS,
                    bybit_leg=bb_leg, binance_leg=bn_leg,
                    signal_spread_bps=signal.spread_bps,
                    actual_spread_bps=actual_spread,
                    slippage_bps=signal.spread_bps - actual_spread,
                    entry_latency_ms=(time.time() - t0) * 1000,
                )
                self._cleanup_leg(bb_oid)
                self._cleanup_leg(bn_oid)
                logger.info("Position verification saved trade from false leg failure!")
                return result

        # Handle any residual fills (partial or one-sided)
        if bb_any or bn_any:
            # At least one leg has some fill — unwind everything
            unwind_pnl = 0.0
            failure_venue = ""
            if bb_any and not bn_any:
                failure_venue = "binance"
                unwind_pnl = await self._emergency_unwind(bb_leg, signal.symbol)
            elif bn_any and not bb_any:
                failure_venue = "bybit"
                unwind_pnl = await self._emergency_unwind(bn_leg, signal.symbol)
            else:
                # Both have partial fills — unwind both
                failure_venue = "both_partial"
                unwind_pnl += await self._emergency_unwind(bb_leg, signal.symbol)
                unwind_pnl += await self._emergency_unwind(bn_leg, signal.symbol)

            self._cleanup_leg(bb_oid)
            self._cleanup_leg(bn_oid)
            return EntryResult(
                outcome=EntryOutcome.LEG_FAILURE,
                bybit_leg=bb_leg, binance_leg=bn_leg,
                leg_failure_venue=failure_venue,
                unwind_pnl_usd=unwind_pnl,
                entry_latency_ms=(time.time() - t0) * 1000,
            )

        # Neither leg has any fill — clean exit
        self._cleanup_leg(bb_oid)
        self._cleanup_leg(bn_oid)
        return EntryResult(
            outcome=EntryOutcome.BOTH_MISSED,
            bybit_leg=bb_leg, binance_leg=bn_leg,
            entry_latency_ms=(time.time() - t0) * 1000,
        )

    async def _emergency_unwind(self, leg: LegState, symbol: str) -> float:
        """
        Close a filled leg that has no counterpart.
        Goes straight to market order — NEVER leave a naked leg open.

        M7 FIX: Bybit limit orders use IOC (exchange-level), so an aggressive
        limit at our fill price will miss and cancel instantly. The 200ms limit
        attempt is wasted time that delays the market safety net. Skip the limit
        step and go straight to market.
        """
        logger.warning(f"EMERGENCY UNWIND {leg.venue} {symbol} {leg.side} qty={leg.filled_qty}")

        # Reverse side
        unwind_side = "Sell" if leg.side == "Buy" else "Buy"

        # Market order — guaranteed fill (only path for emergency unwind)
        try:
            oid = await self._submit(
                leg.venue, symbol, unwind_side,
                leg.filled_qty, 0,  # price=0 for market
                "market",
            )
            unwind_leg = LegState(
                venue=leg.venue, symbol=symbol, side=unwind_side,
                order_id=oid, target_qty=leg.filled_qty,
                state=OrderState.SUBMITTED, submitted_at=time.time(),
            )
            self.register_leg(unwind_leg)

            event = self._fill_events[oid]
            try:
                await asyncio.wait_for(event.wait(), timeout=self.UNWIND_MARKET_TIMEOUT_S)
            except asyncio.TimeoutError:
                # Final REST check
                if self._check_order:
                    try:
                        info = await self._check_order(leg.venue, symbol, oid)
                        if float(info.get("filled_qty", 0)) > 0:
                            unwind_leg.filled_qty = float(info["filled_qty"])
                            unwind_leg.avg_fill_price = float(info.get("avg_price", 0))
                            unwind_leg.state = OrderState.FILLED
                    except Exception:
                        pass
                if not unwind_leg.is_filled:
                    logger.critical(f"MARKET UNWIND TIMEOUT {leg.venue} {symbol} — MANUAL INTERVENTION NEEDED")

            if unwind_leg.is_filled:
                loss = (unwind_leg.avg_fill_price - leg.avg_fill_price) * leg.filled_qty
                if leg.side == "Sell":
                    loss = -loss
                logger.warning(f"Unwind via market: loss=${loss:.4f}")
                self._cleanup_leg(oid)
                return loss
            self._cleanup_leg(oid)

        except Exception as e:
            logger.critical(f"MARKET UNWIND FAILED {leg.venue} {symbol}: {e} — MANUAL INTERVENTION NEEDED")

        return 0.0

    async def _safe_cancel(self, venue: str, symbol: str, order_id: str):
        """Cancel an order, swallow errors."""
        try:
            await self._cancel(venue, symbol, order_id)
        except Exception as e:
            logger.warning(f"Cancel failed {venue} {order_id}: {e}")

    def _compute_actual_spread(self, bb_leg: LegState, bn_leg: LegState, bb_side: str) -> float:
        """Compute actual spread from fill prices."""
        if bb_leg.avg_fill_price <= 0 or bn_leg.avg_fill_price <= 0:
            logger.warning(f"Zero fill price in spread calc: bb={bb_leg.avg_fill_price} bn={bn_leg.avg_fill_price}")
            return 0.0
        if bb_side == "Buy":
            # BUY_BB_SELL_BN: bought on Bybit, sold on Binance
            return (bn_leg.avg_fill_price - bb_leg.avg_fill_price) / bb_leg.avg_fill_price * 10000
        else:
            # BUY_BN_SELL_BB: bought on Binance, sold on Bybit
            return (bb_leg.avg_fill_price - bn_leg.avg_fill_price) / bn_leg.avg_fill_price * 10000

    def _cleanup_leg(self, order_id: str):
        """Remove a leg from tracking (both exchange order_id and client_order_id keys)."""
        leg = self._leg_states.get(order_id)
        self._fill_events.pop(order_id, None)
        self._leg_states.pop(order_id, None)
        # M1 FIX: also remove client_order_id entries to prevent memory leak.
        # The leg may have been pre-registered under a client ID (h2_bb_* / h2_bn_* / h2x_bb_*
        # / h2x_bn_*) and then also keyed under the exchange order_id. If we received the
        # order_id key, find and purge the matching client_id key too.
        if leg:
            for key in list(self._leg_states.keys()):
                if self._leg_states.get(key) is leg:
                    self._fill_events.pop(key, None)
                    self._leg_states.pop(key, None)

    async def execute_exit(
        self,
        symbol: str,
        bb_side: str,
        bn_side: str,
        qty_bb: float,
        qty_bn: float,
        price_bb: float,
        price_bn: float,
        is_stop_loss: bool = False,
    ) -> ExitResult:
        """
        Close both legs of an open position.

        For stop loss: uses market orders (accept slippage, prevent further loss).
        For reversion exits: uses IOC limit orders.

        Uses same pre-registration pattern as execute_entry to prevent WS
        fill race condition (C1 fix).
        """
        t0 = time.time()
        order_type = "market" if is_stop_loss else "limit"

        logger.info(f"EXIT {'SL' if is_stop_loss else 'REVERT'}: {symbol} bb={bb_side} bn={bn_side} type={order_type}")

        # Pre-register with client order IDs (same pattern as execute_entry)
        import uuid
        bb_client_id = f"h2x_bb_{uuid.uuid4().hex[:12]}"
        bn_client_id = f"h2x_bn_{uuid.uuid4().hex[:12]}"

        bb_leg = LegState(
            venue="bybit", symbol=symbol, side=bb_side,
            target_qty=qty_bb, target_price=price_bb,
            order_id=bb_client_id,
        )
        bn_leg = LegState(
            venue="binance", symbol=symbol, side=bn_side,
            target_qty=qty_bn, target_price=price_bn,
            order_id=bn_client_id,
        )

        # Pre-register BEFORE submitting so WS fills are never lost
        self.register_leg(bb_leg)
        self.register_leg(bn_leg)

        # Submit both close orders with client order IDs
        bb_oid = None
        bn_oid = None

        async def close_bb():
            nonlocal bb_oid
            try:
                bb_oid = await self._submit("bybit", symbol, bb_side, qty_bb, price_bb, order_type, bb_client_id)
            except Exception as e:
                logger.error(f"Exit Bybit submit failed: {e}")

        async def close_bn():
            nonlocal bn_oid
            try:
                bn_oid = await self._submit("binance", symbol, bn_side, qty_bn, price_bn, order_type, bn_client_id)
            except Exception as e:
                logger.error(f"Exit Binance submit failed: {e}")

        await asyncio.gather(close_bb(), close_bn())

        # Map exchange IDs (same pattern as execute_entry — don't overwrite filled state)
        if bb_oid and bb_oid != bb_client_id:
            self._fill_events[bb_oid] = self._fill_events[bb_client_id]
            self._leg_states[bb_oid] = bb_leg
        bb_leg.order_id = bb_oid or bb_client_id
        if bb_leg.state == OrderState.IDLE:
            bb_leg.state = OrderState.SUBMITTED
        bb_leg.submitted_at = bb_leg.submitted_at or time.time()

        if bn_oid and bn_oid != bn_client_id:
            self._fill_events[bn_oid] = self._fill_events[bn_client_id]
            self._leg_states[bn_oid] = bn_leg
        bn_leg.order_id = bn_oid or bn_client_id
        if bn_leg.state == OrderState.IDLE:
            bn_leg.state = OrderState.SUBMITTED
        bn_leg.submitted_at = bn_leg.submitted_at or time.time()

        # Replay any fills that arrived during the submit race window
        if self._unmatched_fills:
            buffered = self._unmatched_fills[:]
            self._unmatched_fills.clear()
            for fill in buffered:
                logger.info(f"Replaying buffered exit fill: {fill.venue} {fill.order_id} qty={fill.qty}")
                self.on_fill(fill)

        # Wait for fills (longer timeout for exits, especially stop loss)
        timeout = 2.0 if is_stop_loss else self.FILL_TIMEOUT_S
        bb_event = self._fill_events.get(bb_oid) or self._fill_events.get(bb_client_id)
        bn_event = self._fill_events.get(bn_oid) or self._fill_events.get(bn_client_id)
        events_to_wait = []
        if bb_event:
            events_to_wait.append(bb_event.wait())
        if bn_event:
            events_to_wait.append(bn_event.wait())

        if events_to_wait:
            try:
                await asyncio.wait_for(asyncio.gather(*events_to_wait), timeout=timeout)
            except asyncio.TimeoutError:
                logger.warning(f"Exit timeout after {timeout}s — checking fill status")

        # BUG FIX #9 + #11: Before escalating to market, check if original order
        # already filled via REST (the WS fill may have been missed).
        # This prevents the double-fill bug on thin Binance USDC books.
        for leg, oid in [(bb_leg, bb_oid), (bn_leg, bn_oid)]:
            remaining = leg.target_qty - leg.filled_qty if leg.target_qty > 0 else 0
            if oid and remaining > 0:
                # REST CHECK FIRST: did the original order actually fill?
                if self._check_order:
                    try:
                        order_info = await self._check_order(leg.venue, symbol, oid)
                        filled = float(order_info.get("filled_qty", 0))
                        if filled >= leg.target_qty * 0.99:
                            logger.info(
                                f"Exit leg {leg.venue} already filled via REST "
                                f"(qty={filled}) — skipping market escalation"
                            )
                            leg.filled_qty = filled
                            leg.avg_fill_price = float(order_info.get("avg_price", leg.target_price))
                            leg.state = OrderState.FILLED
                            continue  # Don't submit market order!
                    except Exception as e:
                        logger.warning(f"Exit REST fill check failed for {leg.venue}: {e}")

                logger.warning(f"Exit leg {leg.venue} has {remaining:.4f} unfilled — escalating to market")
                await self._safe_cancel(leg.venue, symbol, oid)

                # Wait 200ms for cancel to propagate and check again
                await asyncio.sleep(0.2)
                if leg.is_filled:
                    logger.info(f"Exit leg {leg.venue} filled during cancel wait — skipping market")
                    continue

                try:
                    market_oid = await self._submit(leg.venue, symbol, leg.side, remaining, 0, "market")
                    mkt_leg = LegState(venue=leg.venue, symbol=symbol, side=leg.side,
                                       order_id=market_oid, target_qty=leg.target_qty,
                                       state=OrderState.SUBMITTED, submitted_at=time.time())
                    self.register_leg(mkt_leg)
                    event = self._fill_events[market_oid]
                    try:
                        await asyncio.wait_for(event.wait(), timeout=2.0)
                    except asyncio.TimeoutError:
                        # Final REST check before declaring failure
                        if self._check_order:
                            try:
                                info = await self._check_order(leg.venue, symbol, market_oid)
                                if float(info.get("filled_qty", 0)) > 0:
                                    mkt_leg.filled_qty = float(info["filled_qty"])
                                    mkt_leg.avg_fill_price = float(info.get("avg_price", 0))
                                    mkt_leg.state = OrderState.FILLED
                                    logger.info(f"Exit market filled via REST: {leg.venue} qty={mkt_leg.filled_qty}")
                            except Exception:
                                pass
                        if not mkt_leg.is_filled:
                            logger.critical(f"EXIT MARKET TIMEOUT {leg.venue} {symbol} — MANUAL INTERVENTION")
                    if mkt_leg.is_filled:
                        # ADD to existing fill, don't overwrite (handles partial original + market remainder)
                        orig_qty = leg.filled_qty
                        orig_value = leg.avg_fill_price * orig_qty if orig_qty > 0 else 0
                        mkt_value = mkt_leg.avg_fill_price * mkt_leg.filled_qty
                        leg.filled_qty = orig_qty + mkt_leg.filled_qty
                        leg.avg_fill_price = (orig_value + mkt_value) / leg.filled_qty if leg.filled_qty > 0 else 0
                        leg.fee += mkt_leg.fee
                        leg.state = OrderState.FILLED
                    self._cleanup_leg(market_oid)
                except Exception as e:
                    logger.critical(f"EXIT MARKET FAILED {leg.venue} {symbol}: {e}")

        # Compute result
        actual_spread = 0.0
        if bb_leg.is_filled and bn_leg.is_filled:
            actual_spread = self._compute_actual_spread(bb_leg, bn_leg, bb_side)

        outcome = "success"
        if not bb_leg.is_filled or not bn_leg.is_filled:
            outcome = "partial"
            if not bb_leg.is_filled and not bn_leg.is_filled:
                outcome = "failed"

        # Cleanup all tracked IDs (exchange + client)
        for oid in [bb_oid, bn_oid, bb_client_id, bn_client_id]:
            if oid:
                self._cleanup_leg(oid)

        logger.info(f"EXIT {outcome}: {symbol} spread={actual_spread:.1f}bp latency={((time.time()-t0)*1000):.0f}ms")

        return ExitResult(
            outcome=outcome,
            bybit_leg=bb_leg,
            binance_leg=bn_leg,
            actual_exit_spread_bps=actual_spread,
            exit_latency_ms=(time.time() - t0) * 1000,
        )
