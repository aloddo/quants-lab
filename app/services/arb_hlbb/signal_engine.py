"""
HLBB Signal Engine — Adaptive threshold spread detection.

Reuses H2's P90/P25 logic with HLBB-specific parameters:
- Fee RT: 19.64bp (not 31bp)
- Entry floor: 30bp (validated from backtest)
- Faster mean reversion: max hold 5min (median 10s)

Signal flow:
1. DualPriceFeed emits SpreadSnapshot on every WS update
2. SignalEngine checks adaptive thresholds → emit SignalEvent
3. Orchestrator executes the trade
"""
import logging
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional

import numpy as np
from pymongo import MongoClient

from app.services.arb_hlbb.config import ArbConfig
from app.services.arb_hlbb.price_feed import SpreadSnapshot

logger = logging.getLogger(__name__)


@dataclass
class SignalEvent:
    """Entry or exit signal."""
    pair: str
    signal_type: str           # "ENTRY" | "EXIT_REVERT" | "EXIT_STOP_LOSS" | "EXIT_MAX_HOLD"
    spread_snapshot: SpreadSnapshot
    threshold_p90: float
    threshold_p25: float
    excess_bps: float
    timestamp: float


@dataclass
class TrackedPosition:
    """Minimal info needed for exit signal detection."""
    pair: str
    direction: str             # "SHORT_HL_LONG_BB" or "SHORT_BB_LONG_HL"
    entry_spread_bps: float
    entry_time: float
    entry_p90: float
    exit_p25: float
    position_id: str = ""
    entry_arb_direction: str = ""  # "HL_PREMIUM" or "BB_PREMIUM" — the spread direction at entry


class AdaptiveThresholds:
    """Per-pair P90/P25 from rolling spread history."""

    def __init__(self, config: ArbConfig):
        self.config = config
        self.history: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=config.threshold_window)
        )

    def update(self, pair: str, abs_spread: float):
        self.history[pair].append(abs_spread)

    def ready(self, pair: str) -> bool:
        return len(self.history[pair]) >= self.config.min_warmup

    def get_thresholds(self, pair: str) -> Optional[dict]:
        h = self.history[pair]
        if len(h) < self.config.min_warmup:
            return None
        arr = np.array(h)
        p25 = float(np.percentile(arr, self.config.exit_percentile * 100))
        p50 = float(np.percentile(arr, 50))
        p90 = float(np.percentile(arr, self.config.entry_percentile * 100))

        # Effective entry = max(P90, floor). Floor dominates when spreads are tight.
        effective_entry = max(p90, self.config.entry_min_spread_bps)
        excess = effective_entry - p25

        return {
            "p25": p25, "median": p50, "p90": p90,
            "effective_entry": effective_entry,
            "excess": excess,
            "viable": excess > self.config.fee_rt_bps,  # must cover fees
        }

    def seed_from_mongodb(self, pairs: list[str], config: ArbConfig):
        """Bootstrap thresholds from historical snapshots."""
        try:
            client = MongoClient(config.mongo_uri, serverSelectionTimeoutMS=5000)
            db_name = config.mongo_uri.rsplit("/", 1)[-1]
            db = client[db_name]
            col = db[config.snapshots_collection]

            for pair in pairs:
                docs = list(col.find(
                    {"pair": pair},
                    {"best_spread": 1, "_id": 0}
                ).sort("timestamp", -1).limit(self.config.threshold_window))

                if docs:
                    for d in reversed(docs):
                        self.history[pair].append(abs(d.get("best_spread", 0)))
                    logger.info(f"  {pair}: seeded {len(docs)} from MongoDB")
                else:
                    logger.warning(f"  {pair}: no historical data")
            client.close()
        except Exception as e:
            logger.error(f"Failed to seed from MongoDB: {e}")


class SignalEngine:
    """Detect entry/exit signals from spread snapshots."""

    def __init__(self, config: ArbConfig):
        self.config = config
        self.thresholds = AdaptiveThresholds(config)
        self._tracked: dict[str, TrackedPosition] = {}
        self._entry_streak: dict[str, int] = defaultdict(int)
        self._tick_count: dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()  # FIX #3: thread safety

        # Stats
        self.total_entry_signals = 0
        self.total_exit_signals = 0

    def seed(self, pairs: list[str]):
        """Bootstrap from MongoDB."""
        self.thresholds.seed_from_mongodb(pairs, self.config)

    def register_position(self, pos: TrackedPosition):
        """Register a position for exit monitoring."""
        with self._lock:
            self._tracked[pos.pair] = pos

    def unregister_position(self, pair: str):
        """Remove a position from exit monitoring."""
        with self._lock:
            self._tracked.pop(pair, None)
            self._entry_streak.pop(pair, None)

    def has_position(self, pair: str) -> bool:
        with self._lock:
            return pair in self._tracked

    @property
    def open_count(self) -> int:
        with self._lock:
            return len(self._tracked)

    def process_snapshot(self, snap: SpreadSnapshot) -> Optional[SignalEvent]:
        """Process a spread snapshot. Returns SignalEvent or None.
        FIX #3: Thread-safe — called from WS threads.
        """
        with self._lock:
            pair = snap.pair
            self._tick_count[pair] += 1

            # Update thresholds (subsampled to reduce CPU).
            # NOTE: Uses best_spread_bps (direction-agnostic absolute magnitude)
            # intentionally. Thresholds represent "how unusual is ANY large spread",
            # not direction-specific. The 30bp entry floor is the real protection.
            if self._tick_count[pair] % self.config.subsample_rate == 0:
                self.thresholds.update(pair, snap.best_spread_bps)

            # Check exit first
            if pair in self._tracked:
                return self._check_exit(snap)

            # Check entry
            return self._check_entry(snap)

    def _check_entry(self, snap: SpreadSnapshot) -> Optional[SignalEvent]:
        pair = snap.pair

        # Capacity check
        if len(self._tracked) >= self.config.max_concurrent:
            return None

        # Hard floor: never enter below 30bp
        if snap.best_spread_bps < self.config.entry_min_spread_bps:
            self._entry_streak[pair] = 0
            return None

        thresh = self.thresholds.get_thresholds(pair)
        if not thresh or not thresh["viable"]:
            self._entry_streak[pair] = 0
            return None

        # Spread must exceed effective entry = max(P90, floor)
        effective_entry = thresh["effective_entry"]
        if snap.best_spread_bps >= effective_entry:
            self._entry_streak[pair] += 1
            if self._entry_streak[pair] >= self.config.min_entry_ticks:
                # Fee gate: excess must cover round-trip fees
                if thresh["excess"] < self.config.fee_rt_bps:
                    return None

                self._entry_streak[pair] = 0
                self.total_entry_signals += 1

                return SignalEvent(
                    pair=pair,
                    signal_type="ENTRY",
                    spread_snapshot=snap,
                    threshold_p90=effective_entry,
                    threshold_p25=thresh["p25"],
                    excess_bps=thresh["excess"],
                    timestamp=time.time(),
                )
        else:
            self._entry_streak[pair] = 0

        return None

    def _direction_spread(self, snap: SpreadSnapshot, pos: TrackedPosition) -> float:
        """Return the spread in the ENTRY direction (can go negative if direction flipped).

        If we entered on HL_PREMIUM, the relevant spread is spread_hl_over_bb_bps.
        If we entered on BB_PREMIUM, the relevant spread is spread_bb_over_hl_bps.
        When this value drops to/below P25, the arb has reverted — time to exit.
        """
        if pos.entry_arb_direction == "HL_PREMIUM":
            return snap.spread_hl_over_bb_bps
        elif pos.entry_arb_direction == "BB_PREMIUM":
            return snap.spread_bb_over_hl_bps
        # Fallback for legacy positions without entry_arb_direction
        return snap.best_spread_bps

    def _check_exit(self, snap: SpreadSnapshot) -> Optional[SignalEvent]:
        pair = snap.pair
        pos = self._tracked[pair]
        now = time.time()
        hold_s = now - pos.entry_time

        exit_type = None

        # Use direction-specific spread (not best_spread_bps which can flip)
        dir_spread = self._direction_spread(snap, pos)

        # Reversion exit (entry-direction spread collapsed)
        if dir_spread <= pos.exit_p25:
            exit_type = "EXIT_REVERT"

        # Stop loss (entry-direction spread widened beyond threshold)
        elif dir_spread > pos.entry_spread_bps * self.config.stop_loss_multiple:
            exit_type = "EXIT_STOP_LOSS"

        # Max hold time
        elif hold_s > self.config.max_hold_s:
            exit_type = "EXIT_MAX_HOLD"

        if exit_type:
            self.total_exit_signals += 1
            thresh = self.thresholds.get_thresholds(pair) or {}
            return SignalEvent(
                pair=pair,
                signal_type=exit_type,
                spread_snapshot=snap,
                threshold_p90=thresh.get("p90", 0),
                threshold_p25=thresh.get("p25", 0),
                excess_bps=thresh.get("excess", 0),
                timestamp=now,
            )

        return None

    def get_pair_status(self) -> list[dict]:
        """Get status for all monitored pairs."""
        result = []
        for pair in sorted(self.thresholds.history.keys()):
            thresh = self.thresholds.get_thresholds(pair)
            tracked = self._tracked.get(pair)
            result.append({
                "pair": pair,
                "ready": self.thresholds.ready(pair),
                "observations": len(self.thresholds.history[pair]),
                "p90": thresh["p90"] if thresh else None,
                "p25": thresh["p25"] if thresh else None,
                "excess": thresh["excess"] if thresh else None,
                "viable": thresh["viable"] if thresh else False,
                "has_position": tracked is not None,
                "streak": self._entry_streak.get(pair, 0),
            })
        return result
