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
            # Copy fill data if present (in case fill events were missed)
            if update.filled_qty > 0 and self.filled_qty == 0:
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

    def __init__(self, submit_order_fn, cancel_order_fn):
        """
        submit_order_fn(venue, symbol, side, qty, price, order_type) -> order_id
        cancel_order_fn(venue, symbol, order_id) -> bool
        """
        self._submit = submit_order_fn
        self._cancel = cancel_order_fn

        # Fill events are signaled here by OrderFeed callbacks
        self._fill_events: dict[str, asyncio.Event] = {}  # order_id -> event
        self._leg_states: dict[str, LegState] = {}  # order_id -> LegState

    def register_leg(self, leg: LegState):
        """Register a leg for fill tracking."""
        self._fill_events[leg.order_id] = asyncio.Event()
        self._leg_states[leg.order_id] = leg

    def on_fill(self, fill: FillEvent):
        """Called by OrderFeed when a fill arrives. Updates leg state and signals."""
        leg = self._leg_states.get(fill.order_id)
        if leg:
            leg.on_fill(fill)
            if leg.is_filled:
                event = self._fill_events.get(fill.order_id)
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

        # Create leg states with placeholder order IDs
        # We use placeholder IDs for pre-registration, then update after submit
        bb_placeholder = f"pending_bb_{int(time.time()*1000)}"
        bn_placeholder = f"pending_bn_{int(time.time()*1000)}"

        bb_leg = LegState(
            venue="bybit", symbol=signal.symbol, side=bb_side,
            target_qty=qty_bb, target_price=price_bb,
            order_id=bb_placeholder,
        )
        bn_leg = LegState(
            venue="binance", symbol=signal.symbol, side=bn_side,
            target_qty=qty_bn, target_price=price_bn,
            order_id=bn_placeholder,
        )

        # Submit both legs with individual error handling (NOT asyncio.gather with raise)
        bb_oid = None
        bn_oid = None
        bb_err = None
        bn_err = None

        async def submit_bb():
            nonlocal bb_oid, bb_err
            try:
                bb_oid = await self._submit("bybit", signal.symbol, bb_side, qty_bb, price_bb, "limit")
            except Exception as e:
                bb_err = e

        async def submit_bn():
            nonlocal bn_oid, bn_err
            try:
                bn_oid = await self._submit("binance", signal.symbol, bn_side, qty_bn, price_bn, "limit")
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

        # Both submits succeeded — register with real order IDs
        bb_leg.order_id = bb_oid
        bb_leg.state = OrderState.SUBMITTED
        bb_leg.submitted_at = time.time()
        bn_leg.order_id = bn_oid
        bn_leg.state = OrderState.SUBMITTED
        bn_leg.submitted_at = time.time()

        self.register_leg(bb_leg)
        self.register_leg(bn_leg)

        # Wait for both fills via WS events
        bb_event = self._fill_events[bb_oid]
        bn_event = self._fill_events[bn_oid]

        try:
            await asyncio.wait_for(
                asyncio.gather(bb_event.wait(), bn_event.wait()),
                timeout=self.FILL_TIMEOUT_S,
            )
        except asyncio.TimeoutError:
            pass  # Handle below

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
        First: aggressive limit (200ms). If miss: market order.
        NEVER leave a naked leg open.
        """
        logger.warning(f"EMERGENCY UNWIND {leg.venue} {symbol} {leg.side} qty={leg.filled_qty}")

        # Reverse side
        unwind_side = "Sell" if leg.side == "Buy" else "Buy"

        # Try aggressive limit first
        try:
            oid = await self._submit(
                leg.venue, symbol, unwind_side,
                leg.filled_qty, leg.avg_fill_price,  # at our fill price
                "limit",
            )
            unwind_leg = LegState(
                venue=leg.venue, symbol=symbol, side=unwind_side,
                order_id=oid, target_qty=leg.filled_qty,
                target_price=leg.avg_fill_price,
                state=OrderState.SUBMITTED, submitted_at=time.time(),
            )
            self.register_leg(unwind_leg)

            event = self._fill_events[oid]
            try:
                await asyncio.wait_for(event.wait(), timeout=self.UNWIND_LIMIT_TIMEOUT_S)
            except asyncio.TimeoutError:
                pass

            if unwind_leg.is_filled:
                loss = (unwind_leg.avg_fill_price - leg.avg_fill_price) * leg.filled_qty
                if leg.side == "Sell":
                    loss = -loss
                logger.info(f"Unwind via limit: loss=${loss:.4f}")
                return loss

            # Cancel limit, escalate to market
            await self._safe_cancel(leg.venue, symbol, oid)

        except Exception as e:
            logger.error(f"Limit unwind failed: {e}")

        # Market order — guaranteed fill
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
                logger.critical(f"MARKET UNWIND TIMEOUT {leg.venue} {symbol} — MANUAL INTERVENTION NEEDED")

            if unwind_leg.is_filled:
                loss = (unwind_leg.avg_fill_price - leg.avg_fill_price) * leg.filled_qty
                if leg.side == "Sell":
                    loss = -loss
                logger.warning(f"Unwind via market: loss=${loss:.4f}")
                return loss

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
        if bb_side == "Buy":
            # BUY_BB_SELL_BN: bought on Bybit, sold on Binance
            return (bn_leg.avg_fill_price - bb_leg.avg_fill_price) / bb_leg.avg_fill_price * 10000
        else:
            # BUY_BN_SELL_BB: bought on Binance, sold on Bybit
            return (bb_leg.avg_fill_price - bn_leg.avg_fill_price) / bn_leg.avg_fill_price * 10000

    def _cleanup_leg(self, order_id: str):
        """Remove a leg from tracking."""
        self._fill_events.pop(order_id, None)
        self._leg_states.pop(order_id, None)
