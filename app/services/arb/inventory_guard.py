"""
V2 InventoryImpairmentGuard — Tracks inventory mark-to-market vs trade capture.

Auto-pauses trading when inventory drag exceeds alpha.
Both CEO review voices flagged inventory impairment (-2.1% in 14h on $69)
as potentially exceeding per-trade capture (+27-58bp on $10/side).

This guard provides the data to answer: "Is the strategy structurally profitable?"
"""
import logging
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class PairMetrics:
    """Per-pair inventory and trade metrics."""
    symbol: str
    qty: float = 0.0
    cost_basis_usd: float = 0.0
    current_value_usd: float = 0.0
    cumulative_impairment_usd: float = 0.0  # negative = loss
    cumulative_capture_usd: float = 0.0     # positive = gain from trades
    trade_count: int = 0
    last_mark_time: float = 0.0


class InventoryImpairmentGuard:
    """
    Monitors inventory drag vs trade alpha. Auto-pauses if impairment dominates.

    Check every 5 minutes:
    1. Mark all inventory to market using PriceFeed mid prices
    2. Compare cumulative impairment to cumulative trade capture
    3. If impairment > threshold * capture, trigger PAUSE

    Also provides break-even position size for reports.
    """

    def __init__(self, impairment_threshold: float = 3.0, absolute_impairment_limit: float = 5.0):
        """
        impairment_threshold: pause if impairment > N * cumulative capture.
        Default 3.0 means: if inventory lost 3x what trades made, something is wrong.
        absolute_impairment_limit: pause if total impairment exceeds this USD amount
        regardless of trade count. Fires before the 5-trade minimum.
        """
        self._threshold = impairment_threshold
        self._absolute_limit = absolute_impairment_limit
        self._pairs: dict[str, PairMetrics] = {}
        self._total_capture_usd: float = 0.0
        self._total_impairment_usd: float = 0.0
        self._last_check: float = 0.0
        self._start_time: float = time.time()

    def register_pair(self, symbol: str, qty: float, cost_basis_usd: float):
        """Register or update inventory for a pair. Merges into existing entry
        to preserve capture/impairment history."""
        existing = self._pairs.get(symbol)
        if existing:
            # Merge: update qty and cost_basis, preserve history
            existing.qty = qty
            existing.cost_basis_usd = cost_basis_usd
            existing.current_value_usd = cost_basis_usd
            existing.last_mark_time = time.time()
        else:
            self._pairs[symbol] = PairMetrics(
                symbol=symbol,
                qty=qty,
                cost_basis_usd=cost_basis_usd,
                current_value_usd=cost_basis_usd,  # initial mark = cost
                last_mark_time=time.time(),
            )

    def record_trade_capture(self, symbol: str, capture_usd: float):
        """Record PnL from a completed round-trip."""
        pm = self._pairs.get(symbol)
        if pm:
            pm.cumulative_capture_usd += capture_usd
            pm.trade_count += 1
        self._total_capture_usd += capture_usd

    def mark_to_market(self, prices: dict[str, float]):
        """
        Update all inventory valuations. Call every 5 minutes.

        prices: {symbol: mid_price} from PriceFeed
        """
        now = time.time()
        self._total_impairment_usd = 0.0

        for symbol, pm in self._pairs.items():
            mid = prices.get(symbol)
            if mid and mid > 0 and pm.qty > 0:
                new_value = pm.qty * mid
                change = new_value - pm.current_value_usd
                pm.current_value_usd = new_value
                pm.cumulative_impairment_usd = new_value - pm.cost_basis_usd
                pm.last_mark_time = now

            self._total_impairment_usd += pm.cumulative_impairment_usd

        self._last_check = now

    def should_pause(self) -> tuple[bool, str]:
        """
        Check if inventory impairment exceeds alpha threshold.

        Returns (should_pause, reason).
        Only triggers after at least 5 trades (need data to decide).
        """
        # Absolute impairment guardrail: triggers before 5 trades if loss is large
        if self._total_impairment_usd < 0 and abs(self._total_impairment_usd) > self._absolute_limit:
            return True, (
                f"Absolute impairment ${self._total_impairment_usd:.2f} exceeds "
                f"${self._absolute_limit:.2f} limit — pausing regardless of trade count"
            )

        total_trades = sum(pm.trade_count for pm in self._pairs.values())
        if total_trades < 5:
            return False, ""

        if self._total_capture_usd <= 0:
            # All trades net negative -- no capture to compare against
            if abs(self._total_impairment_usd) > 1.0:  # More than $1 impairment
                return True, (
                    f"Zero positive capture after {total_trades} trades, "
                    f"inventory impairment ${self._total_impairment_usd:.2f}"
                )
            return False, ""

        ratio = abs(self._total_impairment_usd) / self._total_capture_usd
        if self._total_impairment_usd < 0 and ratio > self._threshold:
            return True, (
                f"Inventory impairment (${self._total_impairment_usd:.2f}) "
                f"exceeds {self._threshold}x trade capture (${self._total_capture_usd:.2f}). "
                f"Ratio: {ratio:.1f}x. Strategy may be structurally unprofitable at current size."
            )

        return False, ""

    def break_even_position_size(self, avg_capture_bps: float, avg_hold_hours: float) -> float:
        """
        Estimate the position size needed for trade capture to exceed inventory drag.

        Returns USD per side. If the number is unreachable, the strategy is not viable.
        """
        if avg_capture_bps <= 0 or avg_hold_hours <= 0:
            return float('inf')

        total_trades = sum(pm.trade_count for pm in self._pairs.values())
        uptime_h = (time.time() - self._start_time) / 3600
        if uptime_h <= 0 or total_trades == 0:
            return float('inf')

        # Hourly impairment rate
        hourly_impairment = abs(self._total_impairment_usd) / uptime_h if self._total_impairment_usd < 0 else 0
        # Trades per hour
        trades_per_hour = total_trades / uptime_h
        # Capture per trade at current size (assume $10/side if not enough data)
        current_capture_per_trade = self._total_capture_usd / total_trades if total_trades > 0 else 0

        if hourly_impairment <= 0 or trades_per_hour <= 0:
            return 10.0  # Current size is fine

        # Break-even: capture_per_trade * trades_per_hour >= hourly_impairment
        # capture_per_trade = avg_capture_bps/10000 * position_usd * 2
        # position_usd = hourly_impairment / (trades_per_hour * avg_capture_bps/10000 * 2)
        needed = hourly_impairment / (trades_per_hour * avg_capture_bps / 10000 * 2)
        return round(needed, 2)

    def status(self) -> dict:
        """Return current status for Telegram reports."""
        total_trades = sum(pm.trade_count for pm in self._pairs.values())
        uptime_h = (time.time() - self._start_time) / 3600

        return {
            "total_impairment_usd": round(self._total_impairment_usd, 4),
            "total_capture_usd": round(self._total_capture_usd, 4),
            "impairment_ratio": (
                abs(self._total_impairment_usd) / self._total_capture_usd
                if self._total_capture_usd > 0 else float('inf')
            ),
            "total_trades": total_trades,
            "uptime_hours": round(uptime_h, 1),
            "pairs": {
                sym: {
                    "qty": pm.qty,
                    "cost_basis": round(pm.cost_basis_usd, 2),
                    "current_value": round(pm.current_value_usd, 2),
                    "impairment": round(pm.cumulative_impairment_usd, 4),
                    "capture": round(pm.cumulative_capture_usd, 4),
                    "trades": pm.trade_count,
                }
                for sym, pm in self._pairs.items()
            },
        }
