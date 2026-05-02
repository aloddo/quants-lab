"""
Inventory Manager V2 — Correct limits, mark-to-market, state clearing.

Fixes from adversarial review:
- F3: Limits sized to $50 capital (not $500-$2000)
- F8: Check inventory BEFORE quotes (not after)
- F15: Clear position state when venue returns empty
- Mark-to-market exposure (not entry price)
"""
import logging
import time
from dataclasses import dataclass
from typing import Optional

from hyperliquid.info import Info

logger = logging.getLogger(__name__)


@dataclass
class PositionState:
    """Current position for a coin, mark-to-market."""
    coin: str
    size: float = 0.0            # positive=long, negative=short
    entry_price: float = 0.0
    mark_price: float = 0.0      # current price for MTM
    notional_usd: float = 0.0    # abs(size * mark_price) — F8: use mark, not entry
    unrealized_pnl: float = 0.0
    last_updated: float = 0.0


@dataclass
class InventoryConfig:
    """Inventory limits SIZED FOR $50 CAPITAL."""
    # Per-pair limits (USD notional at mark)
    soft_limit_usd: float = 7.50     # 15% of capital — start aggressive skew
    hard_limit_usd: float = 10.0     # 20% — one-sided quoting only
    emergency_limit_usd: float = 15.0 # 30% — market flatten NOW

    # Total portfolio limits
    max_total_exposure_usd: float = 25.0  # 50% of capital across all pairs

    # Daily P&L limits
    max_daily_loss_usd: float = 2.50  # 5% of capital — stop trading
    max_drawdown_usd: float = 5.00    # 10% — full stop

    # Consecutive fill protection (F7 related)
    max_consecutive_same_side: int = 4  # likely toxic flow

    # Refresh interval
    position_check_interval: float = 2.0  # seconds (F8: check more frequently)


class InventoryManagerV2:
    """Track and manage MM inventory with proper limits."""

    def __init__(
        self,
        info: Info,
        address: str,
        coins: list[str],
        config: Optional[InventoryConfig] = None,
    ):
        self.info = info
        self.address = address
        self.coins = coins
        self.config = config or InventoryConfig()

        self._positions: dict[str, PositionState] = {
            coin: PositionState(coin=coin) for coin in coins
        }
        self._last_check: float = 0.0
        self._daily_pnl: float = 0.0
        self._session_start_equity: Optional[float] = None  # set on first update
        self._peak_equity: float = 0.0
        self._current_equity: float = 0.0
        self._consecutive_fills: dict[str, list[str]] = {coin: [] for coin in coins}

    def update(self) -> dict[str, PositionState]:
        """Refresh positions from HL. Call BEFORE quoting (F8)."""
        now = time.time()
        if now - self._last_check < self.config.position_check_interval:
            return self._positions

        try:
            state = self.info.user_state(self.address)
            if not state:
                return self._positions

            # FIX 1: Track REAL account equity for PnL limits
            margin = state.get("marginSummary", {})
            account_value = float(margin.get("accountValue", 0) or 0)
            self._current_equity = account_value
            if self._session_start_equity is None:
                self._session_start_equity = account_value
                self._peak_equity = account_value
            else:
                self._peak_equity = max(self._peak_equity, account_value)

            # Real daily PnL = current equity - session start equity
            self._daily_pnl = account_value - self._session_start_equity

            # F15: Start with zero positions, only set what venue returns
            returned_coins = set()

            positions = state.get("assetPositions", [])
            for pos in positions:
                p = pos.get("position", {})
                coin = p.get("coin", "")
                if coin not in self._positions:
                    continue

                size = float(p.get("szi", 0))
                entry = float(p.get("entryPx", 0) or 0)
                unrealized = float(p.get("unrealizedPnl", 0) or 0)

                returned_coins.add(coin)

                if size != 0:
                    # F8: Use mark price for notional (not entry)
                    # Mark ≈ entry + unrealizedPnl/size
                    mark = entry + unrealized / size if size != 0 else entry
                    self._positions[coin] = PositionState(
                        coin=coin,
                        size=size,
                        entry_price=entry,
                        mark_price=mark,
                        notional_usd=abs(size * mark),
                        unrealized_pnl=unrealized,
                        last_updated=now,
                    )
                else:
                    # F15: Position closed — clear state
                    self._positions[coin] = PositionState(coin=coin, last_updated=now)

            # F15: Clear any coin NOT returned by venue
            for coin in self.coins:
                if coin not in returned_coins:
                    self._positions[coin] = PositionState(coin=coin, last_updated=now)

            self._last_check = now

        except Exception as e:
            logger.warning(f"Position update failed: {e}")

        return self._positions

    def record_fill(self, coin: str, side: str):
        """Track consecutive fills for toxic flow detection."""
        self._consecutive_fills[coin].append(side)
        # Keep last N
        max_n = self.config.max_consecutive_same_side + 2
        self._consecutive_fills[coin] = self._consecutive_fills[coin][-max_n:]

    def should_stop_quoting(self, coin: str) -> tuple[bool, str]:
        """Check if quoting should stop entirely for this coin.

        Returns (should_stop, reason). Check BEFORE placing quotes (F8).
        """
        pos = self._positions.get(coin)
        cfg = self.config

        # Emergency inventory
        if pos and pos.notional_usd >= cfg.emergency_limit_usd:
            return True, f"EMERGENCY: {coin} notional ${pos.notional_usd:.2f} >= ${cfg.emergency_limit_usd}"

        # Total portfolio exposure
        total = self.total_exposure_usd()
        if total >= cfg.max_total_exposure_usd:
            return True, f"TOTAL_EXPOSURE: ${total:.2f} >= ${cfg.max_total_exposure_usd}"

        # Daily loss (from REAL exchange equity)
        if self._daily_pnl <= -cfg.max_daily_loss_usd:
            return True, f"DAILY_LOSS: ${self._daily_pnl:.2f} <= -${cfg.max_daily_loss_usd}"

        # Drawdown from peak (REAL)
        if self._peak_equity > 0:
            drawdown = self._peak_equity - self._current_equity
            if drawdown >= cfg.max_drawdown_usd:
                return True, f"DRAWDOWN: ${drawdown:.2f} from peak ${self._peak_equity:.2f}"

        # Consecutive same-side fills (likely toxic flow)
        fills = self._consecutive_fills.get(coin, [])
        if len(fills) >= cfg.max_consecutive_same_side:
            last_n = fills[-cfg.max_consecutive_same_side:]
            if len(set(last_n)) == 1:  # all same side
                return True, f"TOXIC_FLOW: {cfg.max_consecutive_same_side} consecutive {last_n[0]} fills"

        return False, ""

    def get_quote_mode(self, coin: str) -> str:
        """Determine quoting mode based on inventory level.

        Returns: "normal", "skew", "one_sided_reduce", or "stop"
        """
        pos = self._positions.get(coin)
        cfg = self.config

        if not pos or pos.notional_usd == 0:
            return "normal"

        if pos.notional_usd >= cfg.hard_limit_usd:
            return "one_sided_reduce"
        elif pos.notional_usd >= cfg.soft_limit_usd:
            return "skew"
        else:
            return "normal"

    def get_position(self, coin: str) -> PositionState:
        return self._positions.get(coin, PositionState(coin=coin))

    def total_exposure_usd(self) -> float:
        return sum(pos.notional_usd for pos in self._positions.values())

    def net_pnl(self) -> float:
        return sum(pos.unrealized_pnl for pos in self._positions.values())

    def update_daily_pnl(self, realized_pnl: float):
        """Track daily realized P&L."""
        self._daily_pnl += realized_pnl

    def reset_daily(self):
        """Reset daily counters (call at start of each day)."""
        self._daily_pnl = 0.0
        for coin in self._consecutive_fills:
            self._consecutive_fills[coin] = []
