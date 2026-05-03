"""
Fill Tracker — Markout analysis + toxicity scoring (Spec Section 5 + 7).

Logs every quote decision and computes 1/5/15/60s markouts per fill.
Used for:
  - Live toxicity detection and circuit breakers
  - Spread adjustment calibration
  - Offline P&L analysis
  - MongoDB persistence for reporting

Circuit breaker rules (Spec Section 5):
  - 1 toxic fill: widen next quotes +1 tick
  - 2 toxic fills in 10m: PAUSE 5m
  - 3 toxic fills in 30m: disable pair for rest of UTC day
"""
import logging
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class Fill:
    """A single fill event with markout tracking."""
    coin: str
    side: str                     # "bid" (we bought) or "ask" (we sold)
    price: float
    size: float                   # base units
    size_usd: float
    fee: float
    oid: int
    timestamp: float

    # Codex #10: Snapshot spread at fill time for consistent toxicity evaluation
    spread_at_fill_bps: float = 0.0

    # Post-fill markouts (bps, populated asynchronously)
    markout_1s: Optional[float] = None
    markout_5s: Optional[float] = None
    markout_15s: Optional[float] = None
    markout_60s: Optional[float] = None
    is_toxic: Optional[bool] = None  # True if 5s markout < threshold


@dataclass
class QuoteLog:
    """A single quote decision log entry (every tick, not just fills)."""
    coin: str
    timestamp: float
    state: str                    # state machine state
    fair_value: float
    reservation_price: float
    hl_bid: float
    hl_ask: float
    spread_bps: float
    depth_bid_usd: float
    depth_ask_usd: float
    microprice: float
    anchor_mid: float
    imbalance_z: float
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    bid_oid: Optional[int] = None
    ask_oid: Optional[int] = None


@dataclass
class PairToxicity:
    """Running toxicity state for one pair."""
    total_fills: int = 0
    toxic_fills: int = 0
    recent_toxic_timestamps: list = field(default_factory=list)  # for circuit breaker windows
    avg_markout_1s_bps: float = 0.0
    avg_markout_5s_bps: float = 0.0
    avg_markout_15s_bps: float = 0.0
    avg_markout_60s_bps: float = 0.0
    widen_ticks: int = 0              # extra ticks to widen (from circuit breaker)
    disabled_until: float = 0.0       # pair disabled until this time


class FillTracker:
    """Track fills, compute markouts, manage circuit breakers."""

    def __init__(
        self,
        max_history: int = 500,
        toxicity_threshold_bps: float = -2.0,  # default 5s markout threshold (fallback)
        circuit_breaker_2_window_s: float = 600.0,    # 10 min window
        circuit_breaker_3_window_s: float = 1800.0,   # 30 min window
    ):
        self.max_history = max_history
        self.toxicity_threshold_bps = toxicity_threshold_bps
        self.cb2_window = circuit_breaker_2_window_s
        self.cb3_window = circuit_breaker_3_window_s

        self._fills: deque[Fill] = deque(maxlen=max_history)
        self._pending_markouts: list[Fill] = []
        self._markout_lock = threading.Lock()  # V2 fix: protects _pending_markouts from WS thread race
        # Bug #9 fix: single lock for all mutable state accessed from multiple threads
        # (_fills, _toxicity, _quote_logs are accessed from WS callbacks and tick loop)
        self._state_lock = threading.Lock()
        self._toxicity: dict[str, PairToxicity] = {}

        # V2: Per-side EWMA markout for side-specific EV gating.
        # Key: (coin, side) -> EWMA of markout_5s in bps.
        # Convention: favorable markout = positive, adverse = negative.
        # EV formula uses: E[markout_cost] = max(0, -ewma) to convert to cost.
        self._side_markout_ewma: dict[tuple[str, str], float] = {}
        self._side_fill_count: dict[tuple[str, str], int] = {}
        self._ewma_alpha: float = 0.15  # EWMA decay: ~7-fill half-life

        # Per-pair spread for calibrated CB thresholds.
        # Updated by orchestrator from signal engine data.
        self._pair_spread_bps: dict[str, float] = {}

        # Quote log (ring buffer, separate from fills)
        self._quote_logs: deque[QuoteLog] = deque(maxlen=2000)

    def update_pair_spread(self, coin: str, spread_bps: float) -> None:
        """Update the current spread for a pair (for calibrated CB thresholds)."""
        self._pair_spread_bps[coin] = spread_bps

    def get_toxicity_threshold(self, coin: str) -> float:
        """Get spread-calibrated toxicity threshold for a pair (current spread).

        Toxic = 5s markout worse than -1x half-spread.
        E.g., BIO at 8bps spread → threshold = -4bps (half-spread).
        Minimum threshold capped at -1.5bps to avoid being too permissive on tight pairs.
        """
        spread = self._pair_spread_bps.get(coin, 0)
        return self._get_toxicity_threshold_for_spread(spread)

    def _get_toxicity_threshold_for_spread(self, spread_bps: float) -> float:
        """Codex #10: Compute toxicity threshold from a specific spread value.

        Used by update_markouts() with the spread snapshotted at fill time,
        ensuring a toxic fill can't be reclassified as non-toxic when the
        spread widens after the fill.
        """
        if spread_bps > 0:
            return max(-spread_bps / 2.0, -10.0)  # half-spread, cap at -10bps
        return self.toxicity_threshold_bps  # fallback to default

    # ------------------------------------------------------------------
    # Fill recording
    # ------------------------------------------------------------------

    def record_fill(
        self,
        coin: str,
        side: str,
        price: float,
        size: float,
        size_usd: float,
        fee: float,
        oid: int,
    ) -> None:
        """Record a new fill. Markouts computed later via update_markouts().

        Codex #10: Snapshots current spread at fill time so toxicity
        threshold uses the spread when the fill occurred, not the spread
        at markout time (which can be very different).
        """
        # Codex #10: Snapshot spread at fill time
        fill_spread = self._pair_spread_bps.get(coin, 0)
        fill = Fill(
            coin=coin, side=side, price=price, size=size,
            size_usd=size_usd, fee=fee, oid=oid,
            timestamp=time.time(),
            spread_at_fill_bps=fill_spread,
        )
        # Bug #9 fix: protect _fills under state_lock
        with self._state_lock:
            self._fills.append(fill)
        with self._markout_lock:
            self._pending_markouts.append(fill)

        if coin not in self._toxicity:
            self._toxicity[coin] = PairToxicity()

        logger.info(
            f"FILL: {coin} {side} {size:.6f} @ ${price:.6f} "
            f"(${size_usd:.2f}, fee=${fee:.4f})"
        )

    # ------------------------------------------------------------------
    # Quote logging
    # ------------------------------------------------------------------

    def log_quote(self, log: QuoteLog) -> None:
        """Log a quote decision. Called every tick regardless of fills.

        Bug #9 fix: protect _quote_logs under state_lock.
        """
        with self._state_lock:
            self._quote_logs.append(log)

    # ------------------------------------------------------------------
    # Markout computation
    # ------------------------------------------------------------------

    def update_markouts(self, current_mids: dict[str, float]) -> None:
        """Update markouts for pending fills. Call every tick.

        Positive markout = price moved in our favor after fill.
        V2 fix: Lock protects against WS thread appending to _pending_markouts
        while we iterate and replace it.
        """
        now = time.time()
        still_pending = []

        with self._markout_lock:
            pending_snapshot = list(self._pending_markouts)

        for fill in pending_snapshot:
            mid = current_mids.get(fill.coin)
            if not mid or mid <= 0:
                still_pending.append(fill)
                continue

            age = now - fill.timestamp
            direction = 1.0 if fill.side == "bid" else -1.0
            move_bps = (mid - fill.price) / fill.price * 10000 * direction

            if age >= 1.0 and fill.markout_1s is None:
                fill.markout_1s = move_bps
            if age >= 5.0 and fill.markout_5s is None:
                fill.markout_5s = move_bps
                # Codex #10: Use spread at fill time for toxicity threshold,
                # not current spread (which may have widened/compressed since fill)
                threshold = self._get_toxicity_threshold_for_spread(fill.spread_at_fill_bps)
                fill.is_toxic = move_bps < threshold
                self._update_toxicity(fill)
            if age >= 15.0 and fill.markout_15s is None:
                fill.markout_15s = move_bps
            if age >= 60.0 and fill.markout_60s is None:
                fill.markout_60s = move_bps

            # Keep pending until 60s markout done
            if fill.markout_60s is None:
                still_pending.append(fill)

        with self._markout_lock:
            # Keep fills that were appended DURING iteration (not in our snapshot)
            # plus fills from our snapshot that still need processing
            new_fills_during_iteration = [
                f for f in self._pending_markouts if f not in pending_snapshot
            ]
            self._pending_markouts = still_pending + new_fills_during_iteration

    def _update_toxicity(self, fill: Fill) -> None:
        """Update toxicity stats + circuit breakers after 5s markout computed.

        V2: Also updates per-side EWMA markout for side-specific EV gating.
        """
        tox = self._toxicity.get(fill.coin)
        if not tox:
            return

        tox.total_fills += 1

        # Running averages (legacy, per-coin)
        n = tox.total_fills
        if fill.markout_1s is not None:
            tox.avg_markout_1s_bps += (fill.markout_1s - tox.avg_markout_1s_bps) / n
        if fill.markout_5s is not None:
            tox.avg_markout_5s_bps += (fill.markout_5s - tox.avg_markout_5s_bps) / n

        # V2: Per-side EWMA markout
        if fill.markout_5s is not None:
            key = (fill.coin, fill.side)
            count = self._side_fill_count.get(key, 0) + 1
            self._side_fill_count[key] = count
            old_ewma = self._side_markout_ewma.get(key, 0.0)
            if count == 1:
                # First fill for this side: use raw value
                self._side_markout_ewma[key] = fill.markout_5s
            else:
                # EWMA update
                self._side_markout_ewma[key] = (
                    self._ewma_alpha * fill.markout_5s
                    + (1 - self._ewma_alpha) * old_ewma
                )

        if not fill.is_toxic:
            return

        tox.toxic_fills += 1
        tox.recent_toxic_timestamps.append(fill.timestamp)

        # Prune old timestamps
        cutoff = time.time() - self.cb3_window
        tox.recent_toxic_timestamps = [
            t for t in tox.recent_toxic_timestamps if t > cutoff
        ]

        # Circuit breaker escalation
        now = time.time()
        recent_30m = [t for t in tox.recent_toxic_timestamps if t > now - self.cb3_window]
        recent_10m = [t for t in tox.recent_toxic_timestamps if t > now - self.cb2_window]

        if len(recent_30m) >= 3:
            # Disable pair for rest of UTC day
            next_utc_midnight = self._next_utc_midnight()
            tox.disabled_until = next_utc_midnight
            logger.warning(
                f"CIRCUIT BREAKER 3: {fill.coin} disabled until "
                f"{datetime.fromtimestamp(next_utc_midnight, tz=timezone.utc).isoformat()} "
                f"({len(recent_30m)} toxic fills in 30m)"
            )
        elif len(recent_10m) >= 2:
            # Pause 5 minutes
            tox.disabled_until = now + 300
            logger.warning(
                f"CIRCUIT BREAKER 2: {fill.coin} paused 5min "
                f"({len(recent_10m)} toxic fills in 10m)"
            )
        else:
            # Widen next quotes +1 tick
            tox.widen_ticks += 1
            logger.info(f"CIRCUIT BREAKER 1: {fill.coin} widen +{tox.widen_ticks} ticks")

    # ------------------------------------------------------------------
    # Queries for orchestrator
    # ------------------------------------------------------------------

    def is_pair_disabled(self, coin: str) -> bool:
        """Check if a pair is disabled by circuit breaker."""
        tox = self._toxicity.get(coin)
        if not tox:
            return False
        return time.time() < tox.disabled_until

    def get_side_markout_ewma(self, coin: str, side: str) -> tuple[float, int]:
        """V2: Get per-side EWMA markout for EV gating.

        Returns (ewma_bps, fill_count) for this coin+side.
        Convention: favorable = positive, adverse = negative.
        EV formula converts: markout_cost = max(0, -ewma)

        Returns (0.0, 0) if no fills yet for this side.
        """
        key = (coin, side)
        ewma = self._side_markout_ewma.get(key, 0.0)
        count = self._side_fill_count.get(key, 0)
        return ewma, count

    def get_widen_ticks(self, coin: str) -> int:
        """Get number of extra ticks to widen quotes (decays after clean fills)."""
        tox = self._toxicity.get(coin)
        if not tox:
            return 0
        return tox.widen_ticks

    def decay_widen(self, coin: str) -> None:
        """Reduce widen_ticks after a non-toxic fill."""
        tox = self._toxicity.get(coin)
        if tox and tox.widen_ticks > 0:
            tox.widen_ticks -= 1

    def get_spread_adjustment_bps(self, coin: str) -> float:
        """Suggest spread widening based on observed adverse selection.

        Returns additional bps to add. Positive = widen.
        """
        tox = self._toxicity.get(coin)
        if not tox or tox.total_fills < 5:
            return 0.0

        if tox.avg_markout_5s_bps < 0:
            return abs(tox.avg_markout_5s_bps) * 1.5
        elif tox.avg_markout_5s_bps > 5.0:
            return -2.0  # can tighten (max 2bps)
        return 0.0

    def get_toxicity(self, coin: str) -> PairToxicity:
        """Get toxicity stats for a coin."""
        with self._state_lock:
            return self._toxicity.get(coin, PairToxicity())

    def get_recent_fills(self, coin: Optional[str] = None, last_n: int = 20) -> list[Fill]:
        """Get recent fills, optionally filtered by coin."""
        with self._state_lock:
            fills = list(self._fills)
        if coin:
            fills = [f for f in fills if f.coin == coin]
        return fills[-last_n:]

    # ------------------------------------------------------------------
    # MongoDB export
    # ------------------------------------------------------------------

    def fills_to_mongo(self, coin: Optional[str] = None) -> list[dict]:
        """Export fills as MongoDB documents."""
        fills = list(self._fills)
        if coin:
            fills = [f for f in fills if f.coin == coin]

        return [
            {
                "coin": f.coin,
                "side": f.side,
                "price": f.price,
                "size": f.size,
                "size_usd": f.size_usd,
                "fee": f.fee,
                "oid": f.oid,
                "timestamp": f.timestamp,
                "markout_1s_bps": f.markout_1s,
                "markout_5s_bps": f.markout_5s,
                "markout_15s_bps": f.markout_15s,
                "markout_60s_bps": f.markout_60s,
                "is_toxic": f.is_toxic,
                "spread_at_fill_bps": f.spread_at_fill_bps,
            }
            for f in fills
            if f.markout_5s is not None
        ]

    def quote_logs_to_mongo(self, since: float = 0.0) -> list[dict]:
        """Export quote logs as MongoDB documents."""
        return [
            {
                "coin": q.coin,
                "timestamp": q.timestamp,
                "state": q.state,
                "fair_value": q.fair_value,
                "reservation_price": q.reservation_price,
                "hl_bid": q.hl_bid,
                "hl_ask": q.hl_ask,
                "spread_bps": q.spread_bps,
                "depth_bid_usd": q.depth_bid_usd,
                "depth_ask_usd": q.depth_ask_usd,
                "microprice": q.microprice,
                "anchor_mid": q.anchor_mid,
                "imbalance_z": q.imbalance_z,
                "bid_price": q.bid_price,
                "ask_price": q.ask_price,
                "bid_oid": q.bid_oid,
                "ask_oid": q.ask_oid,
            }
            for q in self._quote_logs
            if q.timestamp > since
        ]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _next_utc_midnight() -> float:
        """Return timestamp of next UTC midnight."""
        now = datetime.now(timezone.utc)
        tomorrow = now.replace(hour=0, minute=0, second=0, microsecond=0)
        if tomorrow <= now:
            from datetime import timedelta
            tomorrow += timedelta(days=1)
        return tomorrow.timestamp()
