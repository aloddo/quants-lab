"""
Inventory Manager — AS reservation price + age-based exits (Spec Section 4).

AS Reservation Price:
  q_norm = q_usd / Q_soft
  reservation = fv - q_norm * gamma * sigma_1s^2 * tau
  tau = 8s

Inventory limits (notional USD at 5x leverage):
  Q_soft=60, Q_hard=80, Q_emergency=100 (ORDI/BIO)
  Q_soft=50, Q_hard=65, Q_emergency=80  (DASH/AXS/PNUT/APE)
  Q_soft=40, Q_hard=50, Q_emergency=65  (PENDLE)

Age limits:
  soft=30s, hard=60s, emergency=180s

Exit decision tree:
  <30s + adverse <4bps:  passive exit (improve exit side)
  30-60s OR adverse 4-8bps: exit-only, suppress re-entry
  >60s: Bybit hedge
  >180s OR loss >12bps + no hedge: flatten taker, pause 10min
"""
import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

from hyperliquid.info import Info

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-pair inventory limits from spec
# ---------------------------------------------------------------------------

@dataclass
class PairLimits:
    """Inventory limits for a specific pair (notional USD, leveraged)."""
    q_soft: float = 60.0
    q_hard: float = 80.0
    q_emergency: float = 100.0
    gamma_target_shift_bps: float = 1.5  # target AS shift at q_norm=1


PAIR_LIMITS: dict[str, PairLimits] = {
    "ORDI": PairLimits(60, 80, 100, 1.5),
    "BIO": PairLimits(60, 80, 100, 1.5),
    "DASH": PairLimits(50, 65, 80, 2.0),
    "AXS": PairLimits(50, 65, 80, 2.0),
    "PNUT": PairLimits(50, 65, 80, 2.0),
    "APE": PairLimits(50, 65, 80, 1.0),
    "PENDLE": PairLimits(40, 50, 65, 1.0),
}

DEFAULT_LIMITS = PairLimits(50, 65, 80, 1.5)


# ---------------------------------------------------------------------------
# Exit mode enum
# ---------------------------------------------------------------------------

class ExitMode:
    """Exit mode for inventory management."""
    NONE = "none"
    PASSIVE = "passive"           # improve exit side, still quoting
    EXIT_ONLY = "exit_only"       # only quote exit side, suppress entry
    HEDGE = "hedge"               # hedge on Bybit
    EMERGENCY_FLATTEN = "flatten" # taker flatten + pause


# ---------------------------------------------------------------------------
# Position tracking
# ---------------------------------------------------------------------------

@dataclass
class PositionState:
    """Current position for one coin."""
    coin: str
    size: float = 0.0              # signed: positive = long, negative = short
    entry_price: float = 0.0
    mark_price: float = 0.0
    notional_usd: float = 0.0     # abs(size * mark_price)
    unrealized_pnl: float = 0.0
    opened_at: float = 0.0        # timestamp when position was first opened
    last_fill_at: float = 0.0     # timestamp of last fill
    adverse_move_bps: float = 0.0  # how far price moved against entry


@dataclass
class InventorySnapshot:
    """Portfolio-level inventory state."""
    total_gross_notional: float = 0.0
    total_net_exposure: float = 0.0
    free_equity: float = 0.0
    daily_pnl: float = 0.0
    session_peak_equity: float = 0.0


class InventoryManager:
    """Manage inventory with AS reservation pricing and age-based exits."""

    def __init__(
        self,
        info: Info,
        address: str,
        tau_s: float = 8.0,             # AS tau parameter (seconds)
        rv_baseline_multiplier: float = 1.75,  # gamma multiplier threshold
        rv_high_multiplier: float = 2.5,
    ):
        self.info = info
        self.address = address
        self.tau_s = tau_s
        self.rv_baseline_mult = rv_baseline_multiplier
        self.rv_high_mult = rv_high_multiplier

        # Per-coin state
        self._positions: dict[str, PositionState] = {}
        self._rv_baseline: dict[str, float] = {}  # baseline 30s realized vol per coin

        # Portfolio state
        self._equity: float = 0.0
        self._session_start_equity: Optional[float] = None
        self._peak_equity: float = 0.0
        self._daily_pnl: float = 0.0
        self._last_position_sync: float = 0.0

    def get_limits(self, coin: str) -> PairLimits:
        """Get inventory limits for a coin."""
        return PAIR_LIMITS.get(coin, DEFAULT_LIMITS)

    # ------------------------------------------------------------------
    # AS Reservation Price
    # ------------------------------------------------------------------

    def compute_reservation_price(
        self,
        coin: str,
        fair_value: float,
        sigma_1s: float,
        rv_30s: float = 0.0,
    ) -> float:
        """Compute AS reservation price (Spec Section 4).

        reservation = fv - q_norm * gamma * sigma_1s^2 * tau

        Gamma is calibrated so that at q_norm=1, the shift equals
        gamma_target_shift_bps for that pair.
        """
        pos = self._positions.get(coin)
        limits = self.get_limits(coin)

        if not pos or pos.size == 0 or fair_value <= 0 or sigma_1s <= 0:
            return fair_value

        q_usd = pos.size * fair_value
        q_norm = q_usd / limits.q_soft if limits.q_soft > 0 else 0

        # Gamma calibration: gamma = target_shift / (sigma_1s^2 * tau)
        sigma_sq_tau = sigma_1s ** 2 * self.tau_s
        if sigma_sq_tau > 0:
            gamma = (limits.gamma_target_shift_bps / 10000.0) / sigma_sq_tau
        else:
            gamma = 0.0

        # Volatility regime scaling
        if rv_30s > 0 and self._rv_baseline.get(coin, 0) > 0:
            rv_ratio = rv_30s / self._rv_baseline[coin]
            if rv_ratio > self.rv_high_mult:
                gamma *= 2.0
            elif rv_ratio > self.rv_baseline_mult:
                gamma *= 1.5

        # AS reservation
        shift = q_norm * gamma * sigma_sq_tau
        reservation = fair_value * (1.0 - shift)

        return reservation

    def update_rv_baseline(self, coin: str, baseline: float) -> None:
        """Set the baseline 30s realized vol for gamma scaling."""
        if baseline > 0:
            self._rv_baseline[coin] = baseline

    # ------------------------------------------------------------------
    # Exit decision
    # ------------------------------------------------------------------

    def get_exit_mode(self, coin: str) -> str:
        """Determine exit mode based on inventory age and adverse move.

        Returns one of ExitMode constants.
        """
        pos = self._positions.get(coin)
        if not pos or abs(pos.notional_usd) < 5.0:
            return ExitMode.NONE

        limits = self.get_limits(coin)
        age_s = time.time() - pos.opened_at if pos.opened_at > 0 else 0.0

        # Emergency: age > 180s OR loss > 12bps and no hedge possible
        if age_s > 180 or pos.adverse_move_bps > 12.0:
            return ExitMode.EMERGENCY_FLATTEN

        # Hedge: age > 60s
        if age_s > 60:
            return ExitMode.HEDGE

        # Exit-only: age 30-60s OR adverse move 4-8bps
        if age_s > 30 or pos.adverse_move_bps > 4.0:
            return ExitMode.EXIT_ONLY

        # Passive: age < 30s AND adverse < 4bps
        if pos.adverse_move_bps < 4.0:
            return ExitMode.PASSIVE

        return ExitMode.NONE

    def get_inventory_age_s(self, coin: str) -> float:
        """Get how long we have held inventory for this coin."""
        pos = self._positions.get(coin)
        if not pos or pos.size == 0 or pos.opened_at <= 0:
            return 0.0
        return time.time() - pos.opened_at

    # ------------------------------------------------------------------
    # Position sync from exchange
    # ------------------------------------------------------------------

    def sync_positions(self) -> InventorySnapshot:
        """Refresh positions from HL. Call every tick.

        Returns portfolio-level snapshot.
        """
        now = time.time()
        if now - self._last_position_sync < 1.0:
            return self._get_snapshot()

        try:
            state = self.info.user_state(self.address)
            if not state:
                return self._get_snapshot()

            margin = state.get("marginSummary", {})
            account_value = float(margin.get("accountValue", 0) or 0)
            total_margin = float(margin.get("totalMarginUsed", 0) or 0)

            self._equity = account_value
            if self._session_start_equity is None:
                self._session_start_equity = account_value
                self._peak_equity = account_value
            else:
                self._peak_equity = max(self._peak_equity, account_value)

            self._daily_pnl = account_value - self._session_start_equity

            # Update per-coin positions
            returned_coins = set()
            for pos_data in state.get("assetPositions", []):
                p = pos_data.get("position", {})
                coin = p.get("coin", "")
                size = float(p.get("szi", 0))
                entry = float(p.get("entryPx", 0) or 0)
                unrealized = float(p.get("unrealizedPnl", 0) or 0)
                returned_coins.add(coin)

                existing = self._positions.get(coin)
                if size != 0:
                    mark = entry + unrealized / size if size != 0 else entry
                    # Track when position was opened
                    opened_at = existing.opened_at if existing and existing.size != 0 else now

                    # Compute adverse move
                    if entry > 0:
                        if size > 0:
                            adverse = max(0, (entry - mark) / entry * 10000)
                        else:
                            adverse = max(0, (mark - entry) / entry * 10000)
                    else:
                        adverse = 0.0

                    self._positions[coin] = PositionState(
                        coin=coin, size=size, entry_price=entry,
                        mark_price=mark, notional_usd=abs(size * mark),
                        unrealized_pnl=unrealized, opened_at=opened_at,
                        last_fill_at=existing.last_fill_at if existing else 0.0,
                        adverse_move_bps=adverse,
                    )
                else:
                    self._positions[coin] = PositionState(coin=coin)

            # Clear coins not returned
            for coin in list(self._positions.keys()):
                if coin not in returned_coins and coin in self._positions:
                    self._positions[coin] = PositionState(coin=coin)

            self._last_position_sync = now

        except Exception as e:
            logger.warning(f"Position sync failed: {e}")

        return self._get_snapshot()

    def sync_positions_safe(self, timeout_s: float = 2.0) -> InventorySnapshot:
        """Bug #10 fix: sync_positions with internal timeout.

        Instead of wrapping sync_positions in asyncio.wait_for(to_thread()),
        which doesn't kill the thread on timeout, this method uses a requests
        timeout internally. If the REST call times out, it returns the current
        snapshot without mutating state.
        """
        import requests as _requests

        now = time.time()
        if now - self._last_position_sync < 1.0:
            return self._get_snapshot()

        try:
            # Use info.user_state with internal timeout via the session
            # HL SDK doesn't expose timeout param, so we patch the session
            original_timeout = getattr(self.info.session, 'timeout', None)
            self.info.session.timeout = timeout_s
            try:
                state = self.info.user_state(self.address)
            finally:
                # Restore original timeout
                if original_timeout is not None:
                    self.info.session.timeout = original_timeout
                else:
                    try:
                        del self.info.session.timeout
                    except AttributeError:
                        pass

            if not state:
                return self._get_snapshot()

            margin = state.get("marginSummary", {})
            account_value = float(margin.get("accountValue", 0) or 0)

            self._equity = account_value
            if self._session_start_equity is None:
                self._session_start_equity = account_value
                self._peak_equity = account_value
            else:
                self._peak_equity = max(self._peak_equity, account_value)

            self._daily_pnl = account_value - self._session_start_equity

            # Update per-coin positions (same logic as sync_positions)
            returned_coins = set()
            for pos_data in state.get("assetPositions", []):
                p = pos_data.get("position", {})
                coin = p.get("coin", "")
                size = float(p.get("szi", 0))
                entry = float(p.get("entryPx", 0) or 0)
                unrealized = float(p.get("unrealizedPnl", 0) or 0)
                returned_coins.add(coin)

                existing = self._positions.get(coin)
                if size != 0:
                    mark = entry + unrealized / size if size != 0 else entry
                    opened_at = existing.opened_at if existing and existing.size != 0 else now

                    if entry > 0:
                        if size > 0:
                            adverse = max(0, (entry - mark) / entry * 10000)
                        else:
                            adverse = max(0, (mark - entry) / entry * 10000)
                    else:
                        adverse = 0.0

                    self._positions[coin] = PositionState(
                        coin=coin, size=size, entry_price=entry,
                        mark_price=mark, notional_usd=abs(size * mark),
                        unrealized_pnl=unrealized, opened_at=opened_at,
                        last_fill_at=existing.last_fill_at if existing else 0.0,
                        adverse_move_bps=adverse,
                    )
                else:
                    self._positions[coin] = PositionState(coin=coin)

            for coin in list(self._positions.keys()):
                if coin not in returned_coins and coin in self._positions:
                    self._positions[coin] = PositionState(coin=coin)

            self._last_position_sync = now

        except Exception as e:
            logger.warning(f"Position sync (safe) failed: {e}")

        return self._get_snapshot()

    def record_fill(self, coin: str, side: str, price: float, size: float) -> None:
        """Record a fill. Update position tracking."""
        pos = self._positions.get(coin)
        if not pos:
            pos = PositionState(coin=coin)
            self._positions[coin] = pos

        now = time.time()
        pos.last_fill_at = now

        # If this opens a new position, track the open time
        was_flat = abs(pos.size) < 1e-10
        if was_flat:
            pos.opened_at = now

    def get_position(self, coin: str) -> PositionState:
        """Get current position for a coin."""
        return self._positions.get(coin, PositionState(coin=coin))

    def get_free_equity(self) -> float:
        """Get available equity for new positions."""
        total_margin = sum(
            pos.notional_usd / 5.0  # assuming 5x leverage
            for pos in self._positions.values()
            if pos.notional_usd > 0
        )
        return max(0.0, self._equity - total_margin)

    def _get_snapshot(self) -> InventorySnapshot:
        """Build portfolio-level snapshot."""
        gross = sum(pos.notional_usd for pos in self._positions.values())
        net = sum(
            pos.size * pos.mark_price
            for pos in self._positions.values()
            if pos.mark_price > 0
        )
        return InventorySnapshot(
            total_gross_notional=gross,
            total_net_exposure=net,
            free_equity=self.get_free_equity(),
            daily_pnl=self._daily_pnl,
            session_peak_equity=self._peak_equity,
        )
