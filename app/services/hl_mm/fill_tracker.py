"""
Fill Tracker — Markout analysis and toxicity scoring.

Codex Round 3: "The most valuable signal is short-horizon markout after each fill."

Records every fill and computes:
- Post-fill markout at +1s, +5s, +30s, +60s
- Toxicity score per fill (negative markout = we got picked off)
- Running adverse selection rate
- Optimal cooldown estimation

Stores to MongoDB for offline analysis and live calibration.
"""
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Fill:
    """A single fill event."""
    coin: str
    side: str  # "bid" (we bought) or "ask" (we sold)
    price: float
    size_usd: float
    timestamp: float
    # Post-fill markouts (populated asynchronously)
    markout_1s: Optional[float] = None   # bps
    markout_5s: Optional[float] = None
    markout_30s: Optional[float] = None
    markout_60s: Optional[float] = None
    is_toxic: Optional[bool] = None  # True if 5s markout is negative


@dataclass
class ToxicityStats:
    """Running toxicity statistics."""
    total_fills: int = 0
    toxic_fills: int = 0  # negative 5s markout
    avg_markout_1s_bps: float = 0.0
    avg_markout_5s_bps: float = 0.0
    avg_markout_30s_bps: float = 0.0
    adverse_selection_rate: float = 0.0  # fraction of toxic fills


class FillTracker:
    """Track fills and compute markout metrics."""

    def __init__(
        self,
        max_history: int = 200,
        toxicity_threshold_bps: float = -2.0,  # markout worse than this = toxic
    ):
        self.max_history = max_history
        self.toxicity_threshold_bps = toxicity_threshold_bps

        self._fills: deque[Fill] = deque(maxlen=max_history)
        self._pending_markouts: list[Fill] = []  # fills waiting for markout computation
        self._stats: dict[str, ToxicityStats] = {}

    def record_fill(self, coin: str, side: str, price: float, size_usd: float):
        """Record a new fill. Markouts computed later via update_markouts()."""
        fill = Fill(
            coin=coin,
            side=side,
            price=price,
            size_usd=size_usd,
            timestamp=time.time(),
        )
        self._fills.append(fill)
        self._pending_markouts.append(fill)

        # Initialize stats for coin if needed
        if coin not in self._stats:
            self._stats[coin] = ToxicityStats()

        logger.info(f"Fill recorded: {coin} {side} @ ${price:.5f} (${size_usd:.2f})")

    def update_markouts(self, current_mids: dict[str, float]):
        """Update markouts for pending fills based on current mid prices.

        Call every tick. Fills get their markout computed once enough time has passed.
        """
        now = time.time()
        still_pending = []

        for fill in self._pending_markouts:
            age = now - fill.timestamp
            mid = current_mids.get(fill.coin)
            if not mid or mid <= 0:
                still_pending.append(fill)
                continue

            # Compute markout: how much did price move in our favor after fill?
            # If we bought (bid fill) and price went UP → positive markout (good)
            # If we sold (ask fill) and price went DOWN → positive markout (good)
            direction = 1.0 if fill.side == "bid" else -1.0
            move_bps = (mid - fill.price) / fill.price * 10000 * direction

            # Assign to time buckets
            if age >= 1.0 and fill.markout_1s is None:
                fill.markout_1s = move_bps
            if age >= 5.0 and fill.markout_5s is None:
                fill.markout_5s = move_bps
                # Determine toxicity at 5s
                fill.is_toxic = move_bps < self.toxicity_threshold_bps
                self._update_stats(fill)
            if age >= 30.0 and fill.markout_30s is None:
                fill.markout_30s = move_bps
            if age >= 60.0 and fill.markout_60s is None:
                fill.markout_60s = move_bps

            # Keep pending until 60s markout is computed
            if fill.markout_60s is None:
                still_pending.append(fill)

        self._pending_markouts = still_pending

    def _update_stats(self, fill: Fill):
        """Update running toxicity statistics after 5s markout is known."""
        stats = self._stats.get(fill.coin)
        if not stats:
            return

        stats.total_fills += 1
        if fill.is_toxic:
            stats.toxic_fills += 1

        # Running average of markouts
        n = stats.total_fills
        if fill.markout_1s is not None:
            stats.avg_markout_1s_bps += (fill.markout_1s - stats.avg_markout_1s_bps) / n
        if fill.markout_5s is not None:
            stats.avg_markout_5s_bps += (fill.markout_5s - stats.avg_markout_5s_bps) / n

        stats.adverse_selection_rate = stats.toxic_fills / stats.total_fills if stats.total_fills > 0 else 0

    def get_stats(self, coin: str) -> ToxicityStats:
        """Get current toxicity stats for a coin."""
        return self._stats.get(coin, ToxicityStats())

    def get_optimal_spread_adjustment(self, coin: str) -> float:
        """Suggest spread widening based on observed adverse selection.

        Returns additional bps to add to min_spread.
        If markouts are positive → can tighten.
        If markouts are negative → must widen.
        """
        stats = self._stats.get(coin)
        if not stats or stats.total_fills < 5:
            return 0.0  # not enough data

        # Target: average 5s markout should be >= 0
        # If negative, widen by that amount
        if stats.avg_markout_5s_bps < 0:
            return abs(stats.avg_markout_5s_bps) * 1.5  # widen by 1.5x the adverse selection
        elif stats.avg_markout_5s_bps > 5.0:
            return -2.0  # can tighten slightly (max 2bps tighter)
        return 0.0

    def should_pause(self, coin: str) -> bool:
        """Check if RECENT fills are too toxic to continue.

        FIX 5: Uses a sliding window (last 10 fills), not all-time stats.
        Naturally decays as good fills come in. Auto-resumes after toxic cluster passes.
        """
        recent = self.get_recent_fills(coin, last_n=10)
        if len(recent) < 4:
            return False  # not enough data

        # Only look at fills with computed toxicity
        scored = [f for f in recent if f.is_toxic is not None]
        if len(scored) < 3:
            return False

        toxic_count = sum(1 for f in scored if f.is_toxic)
        toxic_rate = toxic_count / len(scored)

        # Pause if >70% of last 10 scored fills are toxic
        return toxic_rate > 0.70

    def get_recent_fills(self, coin: str, last_n: int = 10) -> list[Fill]:
        """Get last N fills for a coin."""
        return [f for f in list(self._fills)[-last_n:] if f.coin == coin]

    def to_mongo_docs(self, coin: Optional[str] = None) -> list[dict]:
        """Export fills as MongoDB documents for persistence."""
        fills = [f for f in self._fills if (coin is None or f.coin == coin)]
        return [
            {
                "coin": f.coin,
                "side": f.side,
                "price": f.price,
                "size_usd": f.size_usd,
                "timestamp": f.timestamp,
                "markout_1s_bps": f.markout_1s,
                "markout_5s_bps": f.markout_5s,
                "markout_30s_bps": f.markout_30s,
                "markout_60s_bps": f.markout_60s,
                "is_toxic": f.is_toxic,
            }
            for f in fills
            if f.markout_5s is not None  # only export fills with computed markout
        ]
