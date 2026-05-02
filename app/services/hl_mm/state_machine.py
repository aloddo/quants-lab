"""
Per-Pair State Machine (Spec Section 3).

States:
  IDLE             - no quoting, waiting for data/spread to be ready
  QUOTING_BOTH     - two-sided maker quotes
  QUOTING_ONE_SIDE - one side only (contrarian, EV, or inventory)
  INVENTORY_EXIT   - actively reducing inventory (passive then aggressive)
  HEDGE            - hedging on Bybit
  PAUSE            - circuit breaker, stale data, or regime shock

Transitions are deterministic based on:
  - spread/EV conditions
  - inventory level and age
  - data freshness
  - risk events
"""
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


class PairState(Enum):
    IDLE = "IDLE"
    QUOTING_BOTH = "QUOTING_BOTH"
    QUOTING_ONE_SIDE = "QUOTING_ONE_SIDE"
    INVENTORY_EXIT = "INVENTORY_EXIT"
    HEDGE = "HEDGE"
    EMERGENCY_FLATTEN = "EMERGENCY_FLATTEN"  # Bug #9: taker flatten + pause
    PAUSE = "PAUSE"


@dataclass
class PairContext:
    """All the inputs the state machine needs to make a transition decision."""
    # Data health
    hl_book_fresh: bool = False         # HL L2 data received within 1.5s
    bybit_anchor_healthy: bool = False  # Bybit data within staleness limits
    hl_book_age_ms: float = 999999.0

    # Spread
    spread_threshold_met: bool = False  # native spread > 2*(fee + tox + 1bps)
    bid_side_ev_positive: bool = False  # expected edge > 0 on bid side
    ask_side_ev_positive: bool = False  # expected edge > 0 on ask side

    # Inventory
    inventory_usd: float = 0.0         # signed: positive = long
    q_soft: float = 60.0               # soft limit (notional USD)
    q_hard: float = 80.0
    q_emergency: float = 100.0
    inventory_age_s: float = 0.0       # seconds since position was opened
    adverse_move_bps: float = 0.0      # how far price moved against us

    # Risk events
    circuit_breaker_active: bool = False
    oms_mismatch: bool = False
    regime_shock: bool = False          # BTC/ETH large move

    # Contrarian imbalance
    strong_imbalance: bool = False      # |z_imb| >= 1.5 AND OFI confirms
    imbalance_side: int = 0             # +1 = bid heavy, -1 = ask heavy

    # Hedge state
    hedge_in_progress: bool = False
    bybit_hedge_available: bool = False  # direct Bybit pair exists


@dataclass
class PairStateInfo:
    """State machine output for one pair."""
    coin: str
    state: PairState
    entered_at: float                   # timestamp when current state was entered
    prev_state: PairState
    quote_bid: bool = False             # should we place a bid?
    quote_ask: bool = False             # should we place an ask?
    exit_mode: bool = False             # are we in exit/reduce-only mode?
    hedge_requested: bool = False       # should orchestrator trigger a hedge?
    pause_until: float = 0.0           # if PAUSE, when can we resume?
    reason: str = ""                    # human-readable reason for state


class StateMachine:
    """Per-pair state machine. One instance manages all pairs."""

    def __init__(self, pause_cooldown_s: float = 60.0):
        self._states: dict[str, PairStateInfo] = {}
        self._pause_cooldown_s = pause_cooldown_s

    def register_pair(self, coin: str) -> None:
        """Register a new pair, starting in IDLE."""
        if coin not in self._states:
            now = time.time()
            self._states[coin] = PairStateInfo(
                coin=coin,
                state=PairState.IDLE,
                entered_at=now,
                prev_state=PairState.IDLE,
            )

    def unregister_pair(self, coin: str) -> None:
        """Remove a pair from the state machine."""
        self._states.pop(coin, None)

    def get_state(self, coin: str) -> Optional[PairStateInfo]:
        """Get current state info for a pair."""
        return self._states.get(coin)

    def transition(self, coin: str, ctx: PairContext) -> PairStateInfo:
        """Evaluate transition rules and return updated state.

        This is the core decision function. It is called every tick for each
        active pair. All transitions are logged.
        """
        info = self._states.get(coin)
        if not info:
            self.register_pair(coin)
            info = self._states[coin]

        old_state = info.state
        now = time.time()

        # --- ANY -> PAUSE (highest priority) ---
        if self._should_pause(ctx):
            if old_state != PairState.PAUSE:
                self._enter_state(info, PairState.PAUSE, now,
                                  self._pause_reason(ctx))
            info.quote_bid = False
            info.quote_ask = False
            info.exit_mode = False
            info.hedge_requested = ctx.circuit_breaker_active and abs(ctx.inventory_usd) > 0
            return info

        # --- PAUSE -> IDLE (cooldown expired + fresh data) ---
        if old_state == PairState.PAUSE:
            if now >= info.pause_until and ctx.hl_book_fresh and ctx.bybit_anchor_healthy:
                self._enter_state(info, PairState.IDLE, now, "pause cooldown expired, data healthy")
            else:
                info.quote_bid = False
                info.quote_ask = False
                return info

        # --- HEDGE state handling ---
        # Bug #3: Stay in HEDGE while hedge_in_progress is True
        if old_state == PairState.HEDGE:
            if ctx.hedge_in_progress:
                # Hedge still active — stay in HEDGE, don't transition
                info.quote_bid = False
                info.quote_ask = False
                info.hedge_requested = True
                return info
            elif abs(ctx.inventory_usd) < ctx.q_soft * 0.3:
                self._enter_state(info, PairState.IDLE, now, "hedge complete, inventory reduced")
            else:
                self._enter_state(info, PairState.INVENTORY_EXIT, now,
                                  "hedge done but inventory remains")

        # --- INVENTORY_EXIT -> EMERGENCY_FLATTEN (Bug #9) ---
        if old_state == PairState.INVENTORY_EXIT:
            if self._should_emergency_flatten(ctx):
                self._enter_state(info, PairState.EMERGENCY_FLATTEN, now,
                                  self._emergency_flatten_reason(ctx))
                info.quote_bid = False
                info.quote_ask = False
                info.exit_mode = True
                info.hedge_requested = False
                return info

        # --- INVENTORY_EXIT -> HEDGE (escalation) ---
        if old_state == PairState.INVENTORY_EXIT:
            if self._should_hedge(ctx):
                self._enter_state(info, PairState.HEDGE, now,
                                  self._hedge_reason(ctx))
                info.quote_bid = False
                info.quote_ask = False
                info.hedge_requested = True
                return info

        # --- QUOTING_* -> INVENTORY_EXIT ---
        if old_state in (PairState.QUOTING_BOTH, PairState.QUOTING_ONE_SIDE):
            if abs(ctx.inventory_usd) >= ctx.q_soft or ctx.inventory_age_s > 30:
                self._enter_state(info, PairState.INVENTORY_EXIT, now,
                                  f"|q|={abs(ctx.inventory_usd):.0f} >= Q_soft={ctx.q_soft:.0f} "
                                  f"or age={ctx.inventory_age_s:.0f}s > 30s")

        # --- IDLE -> QUOTING_BOTH or QUOTING_ONE_SIDE ---
        if old_state == PairState.IDLE:
            if (ctx.hl_book_fresh and ctx.spread_threshold_met
                    and abs(ctx.inventory_usd) < ctx.q_soft * 0.5):
                if ctx.bid_side_ev_positive and ctx.ask_side_ev_positive:
                    self._enter_state(info, PairState.QUOTING_BOTH, now,
                                      "data healthy, both sides EV+")
                elif ctx.bid_side_ev_positive or ctx.ask_side_ev_positive:
                    self._enter_state(info, PairState.QUOTING_ONE_SIDE, now,
                                      "only one side EV+")

        # --- QUOTING_BOTH -> QUOTING_ONE_SIDE ---
        if info.state == PairState.QUOTING_BOTH:
            if not ctx.bid_side_ev_positive or not ctx.ask_side_ev_positive:
                self._enter_state(info, PairState.QUOTING_ONE_SIDE, now,
                                  "one side EV dropped")
            elif abs(ctx.inventory_usd) >= ctx.q_soft * 0.5:
                self._enter_state(info, PairState.QUOTING_ONE_SIDE, now,
                                  "inventory approaching soft limit")
            elif ctx.strong_imbalance:
                self._enter_state(info, PairState.QUOTING_ONE_SIDE, now,
                                  "strong contrarian imbalance")

        # --- QUOTING_ONE_SIDE -> QUOTING_BOTH (recovery) ---
        if info.state == PairState.QUOTING_ONE_SIDE:
            if (ctx.bid_side_ev_positive and ctx.ask_side_ev_positive
                    and abs(ctx.inventory_usd) < ctx.q_soft * 0.3
                    and not ctx.strong_imbalance):
                self._enter_state(info, PairState.QUOTING_BOTH, now,
                                  "both sides EV+ again, inventory low")

        # --- INVENTORY_EXIT -> IDLE (inventory cleared) ---
        if info.state == PairState.INVENTORY_EXIT:
            if abs(ctx.inventory_usd) < 5.0:  # effectively flat
                self._enter_state(info, PairState.IDLE, now, "inventory cleared")

        # --- Set output flags based on current state ---
        self._set_output_flags(info, ctx)

        return info

    def force_pause(self, coin: str, duration_s: float, reason: str) -> None:
        """Force a pair into PAUSE for a specific duration."""
        info = self._states.get(coin)
        if not info:
            return
        now = time.time()
        self._enter_state(info, PairState.PAUSE, now, reason)
        info.pause_until = now + duration_s

    def _should_pause(self, ctx: PairContext) -> bool:
        """Check if ANY pause trigger is active."""
        return (
            ctx.circuit_breaker_active
            or ctx.oms_mismatch
            or ctx.regime_shock
            or (not ctx.hl_book_fresh and ctx.hl_book_age_ms > 1500)
        )

    def _pause_reason(self, ctx: PairContext) -> str:
        if ctx.circuit_breaker_active:
            return "circuit breaker"
        if ctx.oms_mismatch:
            return "OMS mismatch"
        if ctx.regime_shock:
            return "regime shock"
        return f"stale HL data ({ctx.hl_book_age_ms:.0f}ms)"

    def _should_hedge(self, ctx: PairContext) -> bool:
        """Check if hedge is needed (Spec Section 4)."""
        if not ctx.bybit_hedge_available:
            return False
        return (
            abs(ctx.inventory_usd) >= ctx.q_hard
            or (ctx.inventory_age_s > 60 and ctx.adverse_move_bps > 4.0)
            or (not ctx.hl_book_fresh and abs(ctx.inventory_usd) > 0)
        )

    def _hedge_reason(self, ctx: PairContext) -> str:
        if abs(ctx.inventory_usd) >= ctx.q_hard:
            return f"|q|={abs(ctx.inventory_usd):.0f} >= Q_hard={ctx.q_hard:.0f}"
        if ctx.inventory_age_s > 60:
            return f"age={ctx.inventory_age_s:.0f}s > 60s, adverse={ctx.adverse_move_bps:.1f}bps"
        return "stale HL data with inventory"

    def _should_emergency_flatten(self, ctx: PairContext) -> bool:
        """Bug #9: Check if emergency flatten is needed.

        Triggers when: inventory age > 180s, OR loss > 12bps and no hedge available.
        """
        if abs(ctx.inventory_usd) < 5.0:
            return False
        if ctx.inventory_age_s > 180:
            return True
        if ctx.adverse_move_bps > 12.0 and not ctx.bybit_hedge_available:
            return True
        return False

    def _emergency_flatten_reason(self, ctx: PairContext) -> str:
        if ctx.inventory_age_s > 180:
            return f"age={ctx.inventory_age_s:.0f}s > 180s emergency limit"
        return f"adverse={ctx.adverse_move_bps:.1f}bps > 12bps, no hedge available"

    def _set_output_flags(self, info: PairStateInfo, ctx: PairContext) -> None:
        """Set quote_bid/quote_ask/exit_mode based on state + context."""
        state = info.state

        if state == PairState.IDLE:
            info.quote_bid = False
            info.quote_ask = False
            info.exit_mode = False
            info.hedge_requested = False

        elif state == PairState.QUOTING_BOTH:
            info.quote_bid = True
            info.quote_ask = True
            info.exit_mode = False
            info.hedge_requested = False

        elif state == PairState.QUOTING_ONE_SIDE:
            # Contrarian: bid-heavy -> improve ASK, widen BID
            if ctx.strong_imbalance and ctx.imbalance_side == 1:
                # Bid heavy: quote ask (fade), suppress bid
                info.quote_bid = False
                info.quote_ask = True
            elif ctx.strong_imbalance and ctx.imbalance_side == -1:
                # Ask heavy: quote bid (fade), suppress ask
                info.quote_bid = True
                info.quote_ask = False
            else:
                # EV-driven one-side
                info.quote_bid = ctx.bid_side_ev_positive
                info.quote_ask = ctx.ask_side_ev_positive

            # Inventory-driven suppression
            if ctx.inventory_usd > ctx.q_soft * 0.5:
                info.quote_bid = False  # don't add to long
            elif ctx.inventory_usd < -ctx.q_soft * 0.5:
                info.quote_ask = False  # don't add to short

            info.exit_mode = False
            info.hedge_requested = False

        elif state == PairState.INVENTORY_EXIT:
            # Only quote the side that reduces inventory
            if ctx.inventory_usd > 0:
                info.quote_bid = False
                info.quote_ask = True   # sell to reduce long
            else:
                info.quote_bid = True   # buy to reduce short
                info.quote_ask = False
            info.exit_mode = True
            info.hedge_requested = False

        elif state == PairState.HEDGE:
            info.quote_bid = False
            info.quote_ask = False
            info.exit_mode = True
            info.hedge_requested = True

        elif state == PairState.EMERGENCY_FLATTEN:
            # Bug #9: taker flatten handled by orchestrator
            info.quote_bid = False
            info.quote_ask = False
            info.exit_mode = True
            info.hedge_requested = False

        elif state == PairState.PAUSE:
            info.quote_bid = False
            info.quote_ask = False
            info.exit_mode = False
            info.hedge_requested = False

    def _enter_state(
        self, info: PairStateInfo, new_state: PairState, now: float, reason: str
    ) -> None:
        """Transition to a new state with logging."""
        old = info.state
        if old == new_state:
            return
        info.prev_state = old
        info.state = new_state
        info.entered_at = now
        info.reason = reason

        if new_state == PairState.PAUSE:
            info.pause_until = now + self._pause_cooldown_s

        logger.info(f"[{info.coin}] {old.value} -> {new_state.value}: {reason}")
