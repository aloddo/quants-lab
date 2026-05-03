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
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
    realized_pnl: float = 0.0       # sum of closed round-trip PnL
    unrealized_pnl: float = 0.0     # mark-to-market on open positions
    total_fees: float = 0.0         # cumulative fees paid
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

        # Bug #1 fix: threading.RLock to protect against WS callback thread
        # calling record_fill() while tick loop reads/writes positions.
        # Codex R2 #1 fix: Must be RLock (reentrant) because internal methods
        # like _get_snapshot_unlocked() call get_free_equity() which also
        # acquires the lock. Plain Lock would deadlock every 30s.
        self._lock = threading.RLock()

        # Per-coin state
        self._positions: dict[str, PositionState] = {}
        self._rv_baseline: dict[str, float] = {}  # baseline 30s realized vol per coin

        # Portfolio state
        # HL unified accounts report $0 accountValue in clearinghouse when all
        # funds are in spot USDC. Use a flag to track if we've ever gotten a
        # real equity reading. If not, disable PnL-based stops.
        self._equity: float = 51.73  # default to known HL spot USDC (updated 2026-05-03)
        self._equity_ever_confirmed: bool = False
        self._session_start_equity: Optional[float] = None
        self._peak_equity: float = 0.0
        self._daily_pnl: float = 0.0
        self._realized_pnl: float = 0.0       # cumulative realized PnL from closed fills
        self._total_fees: float = 0.0          # cumulative fees
        self._last_position_sync: float = 0.0
        self._last_daily_reset_date: Optional[object] = None  # tracks last UTC date we reset PnL

        # FILL DETECTION HARDENING: Track whether position sync has ever succeeded.
        # Never trust inventory=0 if sync hasn't completed at least once.
        self._sync_ever_succeeded: bool = False
        self._sync_consecutive_failures: int = 0

    def reset_pnl_baseline(self) -> None:
        """Reset PnL baseline to current equity.

        Called after startup position sync so inherited positions' unrealized
        PnL doesn't trigger daily stop. Only tracks PnL from new fills.
        """
        if self._equity > 0:
            old = self._session_start_equity
            self._session_start_equity = self._equity
            self._daily_pnl = 0.0
            self._peak_equity = self._equity
            logger.info(
                f"PnL baseline reset: {old} -> {self._equity:.2f} "
                f"(inherited positions baked into baseline)"
            )

    def _maybe_reset_daily_pnl(self) -> None:
        """Reset daily PnL baseline at UTC midnight.

        Snapshots current equity as the new session_start_equity so that
        daily_pnl = equity - session_start_equity starts fresh each UTC day.
        """
        today = datetime.now(timezone.utc).date()
        if self._last_daily_reset_date is not None and self._last_daily_reset_date == today:
            return  # already reset today
        if self._last_daily_reset_date is None:
            # First call — just record the date, don't reset (session just started)
            self._last_daily_reset_date = today
            return
        # New UTC day — reset PnL baseline
        if self._equity_ever_confirmed and self._equity > 0:
            old_start = self._session_start_equity
            self._session_start_equity = self._equity
            self._daily_pnl = 0.0
            self._peak_equity = self._equity
            self._last_daily_reset_date = today
            logger.info(
                f"UTC midnight PnL reset: start_equity {old_start:.2f} -> {self._equity:.2f}"
            )
        else:
            # No confirmed equity yet — just update the date so we don't keep retrying
            self._last_daily_reset_date = today

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

        Codex #9: Acquires lock to read position state atomically,
        preventing half-updated reads during concurrent record_fill().
        """
        with self._lock:
            pos = self._positions.get(coin)
            if not pos or pos.size == 0:
                return fair_value
            # Copy fields we need under lock
            q_usd = pos.size * fair_value
            pos_size = pos.size
        limits = self.get_limits(coin)

        if fair_value <= 0 or sigma_1s <= 0:
            return fair_value

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
        """Get how long we have held inventory for this coin.
        Codex #9: Lock protects against half-updated position reads.
        """
        with self._lock:
            pos = self._positions.get(coin)
            if not pos or pos.size == 0 or pos.opened_at <= 0:
                return 0.0
            return time.time() - pos.opened_at

    def pause_inventory_age(self, coin: str, pause_duration_s: float) -> None:
        """Shift opened_at forward to freeze inventory age during CB pause.

        Without this, a 5min CB pause always exceeds the 180s emergency
        flatten limit, causing every paused position to be market-closed.
        Codex #9: Lock protects against concurrent modification.
        """
        with self._lock:
            pos = self._positions.get(coin)
            if pos and pos.size != 0 and pos.opened_at > 0:
                pos.opened_at += pause_duration_s
                logger.debug(
                    f"{coin}: inventory age paused {pause_duration_s:.0f}s "
                    f"(effective age now {time.time() - pos.opened_at:.0f}s)"
                )

    # ------------------------------------------------------------------
    # Spot equity fallback (HL unified account fix)
    # ------------------------------------------------------------------

    def _query_spot_equity(self) -> float:
        """Query spot USDC balance as equity fallback.

        On HL unified accounts, all funds may be in spot USDC (still usable
        as perps margin) but accountValue reports $0. This queries spot
        balances and sums USDC holdings as the true equity.

        Rate-limited: caches result for 5s to balance accuracy vs API load.
        """
        now = time.time()
        if hasattr(self, '_last_spot_query') and now - self._last_spot_query < 5.0:
            return getattr(self, '_cached_spot_equity', 0.0)

        try:
            spot_state = self.info.spot_user_state(self.address)
            if not spot_state:
                return 0.0

            total_usdc = 0.0
            for bal in spot_state.get("balances", []):
                coin = bal.get("coin", "")
                if coin == "USDC":
                    total_usdc += float(bal.get("total", 0) or 0)

            self._last_spot_query = now
            self._cached_spot_equity = total_usdc

            if total_usdc > 0:
                logger.debug(f"Spot equity fallback: ${total_usdc:.2f} USDC")

            return total_usdc
        except Exception as e:
            logger.debug(f"Spot equity query failed: {e}")
            return getattr(self, '_cached_spot_equity', 0.0)

    # ------------------------------------------------------------------
    # Position sync from exchange
    # ------------------------------------------------------------------

    def sync_positions(self) -> InventorySnapshot:
        """Refresh positions from HL. Call every tick.

        Returns portfolio-level snapshot.
        """
        with self._lock:
            return self._sync_positions_unlocked()

    def _sync_positions_unlocked(self) -> InventorySnapshot:
        """Internal sync_positions without lock (caller must hold self._lock)."""
        now = time.time()
        if now - self._last_position_sync < 1.0:
            return self._get_snapshot_unlocked()

        try:
            state = self.info.user_state(self.address)
            if not state:
                return self._get_snapshot_unlocked()

            margin = state.get("marginSummary", {})
            account_value = float(margin.get("accountValue", 0) or 0)
            total_margin = float(margin.get("totalMarginUsed", 0) or 0)

            # HL UNIFIED ACCOUNT EQUITY MODEL (verified 2026-05-03):
            #
            # When FLAT:  accountValue=0, spot USDC = full balance.
            # When HOLDING: accountValue = marginUsed + uPnL (perps sub-account equity)
            #               spot USDC = UNCHANGED (HL does NOT deduct margin from spot)
            #
            # Therefore: equity = spot_USDC + uPnL (NOT spot + accountValue)
            #
            # The old formula (spot + accountValue) double-counted the margin:
            #   spot=51.73, accountValue=3.67 → reported 55.40
            #   real equity was 51.73 + (-0.01 uPnL) = 51.72
            #
            # accountValue = totalRawUsd + totalNtlPos = (margin - borrowed) + notional
            # Since margin comes from spot USDC but spot balance isn't reduced,
            # adding both double-counts the margin amount.
            spot_equity = self._query_spot_equity()
            total_upnl = sum(
                float(p.get("position", {}).get("unrealizedPnl", 0) or 0)
                for p in state.get("assetPositions", [])
            )
            combined_equity = spot_equity + total_upnl

            if combined_equity > 0:
                self._equity = combined_equity
                self._equity_ever_confirmed = True
            # else: keep existing _equity (default 54.0 or last known good)

            # Reset PnL baseline at UTC midnight
            self._maybe_reset_daily_pnl()

            if self._session_start_equity is None and self._equity > 0:
                self._session_start_equity = self._equity
                self._peak_equity = self._equity
            else:
                # Bug #2 fix: track _equity not account_value for peak
                self._peak_equity = max(self._peak_equity, self._equity)

            # Only compute daily PnL if we have a real equity reading
            if self._equity_ever_confirmed and self._session_start_equity:
                self._daily_pnl = self._equity - self._session_start_equity
            # else: keep daily_pnl at 0 (no confirmed readings yet)

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

            # Clear coins not returned — but ONLY if sync has succeeded before.
            # If this is the first successful sync, we trust it. If sync has
            # never succeeded, we don't zero out positions (they might be real
            # positions the sync hasn't seen yet).
            for coin in list(self._positions.keys()):
                if coin not in returned_coins and coin in self._positions:
                    if self._sync_ever_succeeded:
                        self._positions[coin] = PositionState(coin=coin)
                    else:
                        logger.warning(
                            f"Position sync: {coin} not returned but sync never "
                            f"confirmed — keeping existing position state"
                        )

            self._last_position_sync = now
            self._sync_ever_succeeded = True
            self._sync_consecutive_failures = 0

        except Exception as e:
            self._sync_consecutive_failures += 1
            logger.warning(
                f"Position sync failed ({self._sync_consecutive_failures}x): {e}"
            )

        return self._get_snapshot_unlocked()

    def sync_positions_safe(self, timeout_s: float = 2.0) -> InventorySnapshot:
        """Bug #10 fix: sync_positions with internal timeout.

        Instead of wrapping sync_positions in asyncio.wait_for(to_thread()),
        which doesn't kill the thread on timeout, this method uses a requests
        timeout internally. If the REST call times out, it returns the current
        snapshot without mutating state.
        """
        import requests as _requests

        with self._lock:
            now = time.time()
            if now - self._last_position_sync < 1.0:
                return self._get_snapshot_unlocked()

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
                    return self._get_snapshot_unlocked()

                margin = state.get("marginSummary", {})
                account_value = float(margin.get("accountValue", 0) or 0)

                # HL unified account: equity = spot USDC + unrealized PnL
                # (spot balance is NOT reduced by margin — see sync_positions for full explanation)
                spot_equity = self._query_spot_equity()
                total_upnl = sum(
                    float(p.get("position", {}).get("unrealizedPnl", 0) or 0)
                    for p in state.get("assetPositions", [])
                )
                combined_equity = spot_equity + total_upnl

                if combined_equity > 0:
                    self._equity = combined_equity
                    self._equity_ever_confirmed = True

                # Reset PnL baseline at UTC midnight
                self._maybe_reset_daily_pnl()

                if self._session_start_equity is None and self._equity > 0:
                    self._session_start_equity = self._equity
                    self._peak_equity = self._equity
                elif self._equity > 0:
                    self._peak_equity = max(self._peak_equity, self._equity)

                # Only compute daily PnL with confirmed equity
                if self._equity_ever_confirmed and self._session_start_equity:
                    self._daily_pnl = self._equity - self._session_start_equity

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
                        if self._sync_ever_succeeded:
                            self._positions[coin] = PositionState(coin=coin)

                self._last_position_sync = now
                self._sync_ever_succeeded = True
                self._sync_consecutive_failures = 0

            except Exception as e:
                self._sync_consecutive_failures += 1
                logger.warning(f"Position sync (safe) failed ({self._sync_consecutive_failures}x): {e}")

            return self._get_snapshot_unlocked()

    def record_fill(self, coin: str, side: str, price: float, size: float,
                    fee: float = 0.0) -> None:
        """Record a fill. Update position tracking INCLUDING size and realized PnL.

        This is critical for preventing the overbuy bug: when REST position
        sync is 429'd, the engine must still know its actual position from
        WS fills. Without this, the engine places new orders based on stale
        position data and overshoots.

        Also computes realized PnL when a fill reduces an existing position.
        """
        with self._lock:
            self._record_fill_unlocked(coin, side, price, size, fee)

    def _record_fill_unlocked(self, coin: str, side: str, price: float, size: float,
                              fee: float = 0.0) -> None:
        """Internal record_fill without lock (caller must hold self._lock)."""
        pos = self._positions.get(coin)
        if not pos:
            pos = PositionState(coin=coin)
            self._positions[coin] = pos

        now = time.time()
        pos.last_fill_at = now
        self._total_fees += fee

        # If this opens a new position, track the open time
        was_flat = abs(pos.size) < 1e-10
        if was_flat:
            pos.opened_at = now

        # Compute realized PnL BEFORE updating position size
        old_size = pos.size
        realized_this_fill = 0.0

        # Check if this fill REDUCES the position (closing trade)
        is_reducing = (
            (old_size > 0 and side == "ask") or   # long position, selling
            (old_size < 0 and side == "bid")       # short position, buying
        )
        if is_reducing and abs(old_size) > 1e-10:
            # How many units are being closed (min of fill size and position size)
            closed_qty = min(size, abs(old_size))
            if old_size > 0:
                # Closing long: PnL = (sell_price - entry_price) * qty
                realized_this_fill = (price - pos.entry_price) * closed_qty
            else:
                # Closing short: PnL = (entry_price - buy_price) * qty
                realized_this_fill = (pos.entry_price - price) * closed_qty
            self._realized_pnl += realized_this_fill

        # Update position size from fill (bid = buy = +size, ask = sell = -size)
        if side == "bid":
            pos.size += size
        elif side == "ask":
            pos.size -= size

        # Update entry price: weighted average for adds, fill price on flip, unchanged on reduce
        new_size = pos.size
        if abs(new_size) < 1e-10:
            # Position is flat — entry price is irrelevant, reset to 0
            pos.entry_price = 0.0
        elif (old_size > 0 and new_size < 0) or (old_size < 0 and new_size > 0):
            # Position FLIPPED direction — new entry is the fill price
            # (the portion that opened the new side entered at this price)
            pos.entry_price = price
        elif abs(new_size) > abs(old_size) and abs(old_size) > 1e-10:
            # Position INCREASED (same direction) — weighted average entry
            old_cost = abs(old_size) * pos.entry_price
            new_cost = size * price
            pos.entry_price = (old_cost + new_cost) / abs(new_size)
        elif abs(old_size) < 1e-10:
            # Opened from flat — entry is fill price
            pos.entry_price = price
        # else: position REDUCED but not flipped — entry price unchanged

        # If position crossed zero, reset open time
        if (old_size > 0 and pos.size <= 0) or (old_size < 0 and pos.size >= 0):
            if abs(pos.size) > 1e-10:
                pos.opened_at = now

        # Bug #7 fix: update notional_usd and mark_price after fill
        pos.notional_usd = abs(pos.size * price)
        pos.mark_price = price

        logger.debug(
            f"Fill-adjusted {coin}: {old_size:.0f} -> {pos.size:.0f} "
            f"(side={side} size={size:.0f} rpnl=${realized_this_fill:.4f})"
        )

    def get_position(self, coin: str) -> PositionState:
        """Get current position for a coin.
        Codex #9: Returns a COPY to prevent callers from reading
        half-updated fields while record_fill() runs concurrently.
        """
        with self._lock:
            pos = self._positions.get(coin)
            if not pos:
                return PositionState(coin=coin)
            # Return a shallow copy so callers can't mutate the original
            from dataclasses import asdict
            return PositionState(**asdict(pos))

    def get_free_equity(self) -> float:
        """Get available equity for new positions.
        Codex #9: Lock protects against concurrent position mutations.
        """
        with self._lock:
            total_margin = sum(
                pos.notional_usd / 5.0  # assuming 5x leverage
                for pos in self._positions.values()
                if pos.notional_usd > 0
            )
            return max(0.0, self._equity - total_margin)

    def _get_snapshot(self) -> InventorySnapshot:
        """Build portfolio-level snapshot (thread-safe)."""
        with self._lock:
            return self._get_snapshot_unlocked()

    def _get_snapshot_unlocked(self) -> InventorySnapshot:
        """Build portfolio-level snapshot (caller must hold self._lock)."""
        gross = sum(pos.notional_usd for pos in self._positions.values())
        net = sum(
            pos.size * pos.mark_price
            for pos in self._positions.values()
            if pos.mark_price > 0
        )
        # Bug #3 fix: populate realized_pnl, unrealized_pnl, total_fees
        unrealized = sum(pos.unrealized_pnl for pos in self._positions.values())
        return InventorySnapshot(
            total_gross_notional=gross,
            total_net_exposure=net,
            free_equity=self.get_free_equity(),
            daily_pnl=self._daily_pnl,
            realized_pnl=self._realized_pnl,
            unrealized_pnl=unrealized,
            total_fees=self._total_fees,
            session_peak_equity=self._peak_equity,
        )
