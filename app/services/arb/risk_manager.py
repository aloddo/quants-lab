"""
H2 RiskManager — Three-tier response system: ALERT / PAUSE / KILL.

Pre-committed criteria. Do not change after seeing results.

Week 1 kill criteria are OPERATIONAL (leg failures, inventory, slippage).
Profitability evaluation waits for 50+ trades.
"""
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

logger = logging.getLogger(__name__)


class RiskAction(str, Enum):
    CONTINUE = "continue"
    ALERT = "alert"       # auto-handle + notify, keep trading
    PAUSE = "pause"       # stop new entries, manage existing
    KILL = "kill"         # graceful shutdown


# ── Funding Settlement Guard ────────────────────────────────────

FUNDING_SETTLEMENT_HOURS_UTC = [0, 8, 16]
FUNDING_BLACKOUT_MINUTES = 5


def is_in_funding_blackout() -> bool:
    """Refuse new entries within 5min of Bybit funding settlement."""
    now = datetime.now(timezone.utc)
    for hour in FUNDING_SETTLEMENT_HOURS_UTC:
        settlement = now.replace(hour=hour, minute=0, second=0, microsecond=0)
        diff = abs((now - settlement).total_seconds())
        if diff < FUNDING_BLACKOUT_MINUTES * 60:
            return True
    return False


# ── Trade Stats Tracker ─────────────────────────────────────────

@dataclass
class TradeStats:
    """Running statistics for risk evaluation."""
    total_trades: int = 0
    total_pnl_usd: float = 0.0
    total_fees_usd: float = 0.0
    leg_failures: int = 0
    consecutive_leg_failures: int = 0
    max_consecutive_leg_failures: int = 0
    slippage_sum_bps: float = 0.0
    daily_pnl_usd: float = 0.0
    daily_start: float = 0.0
    ghost_positions: int = 0
    ghost_auto_resolved: int = 0
    inventory_discrepancies: int = 0
    inventory_auto_resolved: int = 0
    ws_disconnects: int = 0

    @property
    def avg_slippage_bps(self) -> float:
        if self.total_trades == 0:
            return 0
        return self.slippage_sum_bps / self.total_trades

    @property
    def leg_failure_rate(self) -> float:
        if self.total_trades == 0:
            return 0
        return self.leg_failures / self.total_trades

    def new_day(self):
        """Reset daily counters."""
        self.daily_pnl_usd = 0.0
        self.daily_start = time.time()


# ── Risk Manager ────────────────────────────────────────────────

class RiskManager:
    """
    Three-tier response system with pre-committed criteria.

    ALERT: auto-handle + notify via Telegram, keep trading
    PAUSE: stop new entries, manage existing positions, notify for review
    KILL: graceful shutdown (close everything, reconcile, stop)
    """

    # Pre-committed operational criteria (Week 1)
    MAX_DAILY_LOSS_USD = 20.0
    MAX_AVG_SLIPPAGE_BPS = 15.0
    MAX_CONSECUTIVE_LEG_FAILURES = 3
    MAX_LEG_FAILURE_RATE = 0.30
    MIN_TRADES_FOR_SLIPPAGE_EVAL = 10
    MAX_LOSS_PER_TRADE_USD = 5.0
    MAX_CONCURRENT_POSITIONS = 3
    PAPER_LIVE_DIVERGENCE_PCT = 0.50

    # Profitability evaluation waits for 50+ trades
    MIN_TRADES_FOR_PROFIT_EVAL = 50

    def __init__(self):
        self.stats = TradeStats()
        self.stats.daily_start = time.time()
        self._paused = False
        self._killed = False
        self._pause_reason = ""
        self._kill_reason = ""

    @property
    def is_paused(self) -> bool:
        return self._paused

    @property
    def is_killed(self) -> bool:
        return self._killed

    @property
    def can_trade(self) -> bool:
        """Can we open new positions?"""
        if self._killed:
            return False
        if self._paused:
            return False
        if is_in_funding_blackout():
            return False
        return True

    def record_trade(self, pnl_usd: float, fees_usd: float, slippage_bps: float, leg_failure: bool):
        """Record a completed trade and check all criteria."""
        self.stats.total_trades += 1
        self.stats.total_pnl_usd += pnl_usd
        self.stats.total_fees_usd += fees_usd
        self.stats.slippage_sum_bps += abs(slippage_bps)
        self.stats.daily_pnl_usd += pnl_usd

        logger.info(
            f"TRADE #{self.stats.total_trades}: pnl=${pnl_usd:.4f} fees=${fees_usd:.4f} "
            f"slippage={slippage_bps:.1f}bp leg_fail={leg_failure} "
            f"total_pnl=${self.stats.total_pnl_usd:.2f} daily=${self.stats.daily_pnl_usd:.2f}"
        )

        if leg_failure:
            self.stats.leg_failures += 1
            self.stats.consecutive_leg_failures += 1
            self.stats.max_consecutive_leg_failures = max(
                self.stats.max_consecutive_leg_failures,
                self.stats.consecutive_leg_failures,
            )
            logger.warning(f"LEG FAILURE #{self.stats.leg_failures} (consecutive: {self.stats.consecutive_leg_failures})")
        else:
            self.stats.consecutive_leg_failures = 0

    def record_ghost(self, auto_resolved: bool):
        """Record a ghost position detection."""
        self.stats.ghost_positions += 1
        if auto_resolved:
            self.stats.ghost_auto_resolved += 1
        logger.warning(f"GHOST POSITION detected (auto_resolved={auto_resolved}, total={self.stats.ghost_positions})")

    def record_inventory_issue(self, auto_resolved: bool):
        """Record an inventory discrepancy."""
        self.stats.inventory_discrepancies += 1
        if auto_resolved:
            self.stats.inventory_auto_resolved += 1
        logger.warning(f"INVENTORY ISSUE (auto_resolved={auto_resolved}, total={self.stats.inventory_discrepancies})")

    def record_disconnect(self):
        """Record a WS disconnect event."""
        self.stats.ws_disconnects += 1
        logger.warning(f"WS DISCONNECT #{self.stats.ws_disconnects}")

    def evaluate(self) -> tuple[RiskAction, str]:
        """
        Evaluate current state against all criteria.
        Returns (action, reason).
        """
        # KILL criteria (immediate shutdown)
        if self.stats.daily_pnl_usd < -self.MAX_DAILY_LOSS_USD:
            self._killed = True
            self._kill_reason = f"daily_loss_{self.stats.daily_pnl_usd:.2f}"
            return RiskAction.KILL, f"Daily loss ${self.stats.daily_pnl_usd:.2f} exceeds -${self.MAX_DAILY_LOSS_USD}"

        if (self.stats.total_trades >= self.MIN_TRADES_FOR_SLIPPAGE_EVAL
                and self.stats.avg_slippage_bps > self.MAX_AVG_SLIPPAGE_BPS):
            self._killed = True
            self._kill_reason = f"avg_slippage_{self.stats.avg_slippage_bps:.1f}bp"
            return RiskAction.KILL, f"Avg slippage {self.stats.avg_slippage_bps:.1f}bp exceeds {self.MAX_AVG_SLIPPAGE_BPS}bp"

        # PAUSE criteria (stop new entries)
        if self.stats.consecutive_leg_failures >= self.MAX_CONSECUTIVE_LEG_FAILURES:
            self._paused = True
            self._pause_reason = f"consecutive_leg_failures_{self.stats.consecutive_leg_failures}"
            return RiskAction.PAUSE, f"{self.stats.consecutive_leg_failures} consecutive leg failures (circuit breaker)"

        if (self.stats.total_trades >= self.MIN_TRADES_FOR_SLIPPAGE_EVAL
                and self.stats.leg_failure_rate > self.MAX_LEG_FAILURE_RATE):
            self._paused = True
            self._pause_reason = f"leg_failure_rate_{self.stats.leg_failure_rate:.0%}"
            return RiskAction.PAUSE, f"Leg failure rate {self.stats.leg_failure_rate:.0%} exceeds {self.MAX_LEG_FAILURE_RATE:.0%}"

        unresolved_ghosts = self.stats.ghost_positions - self.stats.ghost_auto_resolved
        if unresolved_ghosts >= 2:
            self._paused = True
            self._pause_reason = f"unresolved_ghosts_{unresolved_ghosts}"
            return RiskAction.PAUSE, f"{unresolved_ghosts} unresolved ghost positions"

        # ALERT criteria (keep trading, notify)
        if unresolved_ghosts == 1:
            return RiskAction.ALERT, "1 ghost position detected, auto-close attempted"

        unresolved_inv = self.stats.inventory_discrepancies - self.stats.inventory_auto_resolved
        if unresolved_inv > 0:
            return RiskAction.ALERT, f"{unresolved_inv} inventory discrepancies pending"

        return RiskAction.CONTINUE, "all_clear"

    def unpause(self):
        """Resume trading after manual review."""
        self._paused = False
        self._pause_reason = ""
        self.stats.consecutive_leg_failures = 0
        logger.info("Risk manager unpaused")

    def check_daily_reset(self):
        """Reset daily counters at midnight UTC."""
        now = datetime.now(timezone.utc)
        start = datetime.fromtimestamp(self.stats.daily_start, tz=timezone.utc)
        if now.date() != start.date():
            logger.info(f"Daily reset: yesterday PnL=${self.stats.daily_pnl_usd:.2f}")
            self.stats.new_day()

    def status(self) -> dict:
        """Return current risk state for monitoring."""
        return {
            "can_trade": self.can_trade,
            "paused": self._paused,
            "killed": self._killed,
            "pause_reason": self._pause_reason,
            "kill_reason": self._kill_reason,
            "funding_blackout": is_in_funding_blackout(),
            "total_trades": self.stats.total_trades,
            "total_pnl_usd": round(self.stats.total_pnl_usd, 2),
            "daily_pnl_usd": round(self.stats.daily_pnl_usd, 2),
            "avg_slippage_bps": round(self.stats.avg_slippage_bps, 1),
            "leg_failures": self.stats.leg_failures,
            "leg_failure_rate": round(self.stats.leg_failure_rate, 2),
            "consecutive_leg_failures": self.stats.consecutive_leg_failures,
            "ghost_positions": self.stats.ghost_positions,
            "inventory_issues": self.stats.inventory_discrepancies,
            "ws_disconnects": self.stats.ws_disconnects,
        }
