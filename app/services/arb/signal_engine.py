"""
H2 SignalEngine — Adaptive threshold logic extracted from the paper trader.

Same P90/P25 entry/exit thresholds as arb_h2_paper.py.
Uses WS prices instead of REST polling.

Signal flow:
1. PriceFeed provides fresh SpreadSnapshot
2. SignalEngine checks thresholds -> emit SignalEvent
3. LegCoordinator executes the trade

The spread is RECALCULATED at order submission time using live WS prices.
If spread has narrowed below threshold by then, we skip the trade.
"""
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Optional

import numpy as np
from pymongo import MongoClient

from app.services.arb.price_feed import SpreadSnapshot

logger = logging.getLogger(__name__)


# ── Config (same as paper trader) ───────────────────────────────

ENTRY_PERCENTILE = 0.90
EXIT_PERCENTILE = 0.25
MIN_EXCESS_BPS = 30.0
FEE_RT_BPS = 31.0  # corrected: 4 fills at taker rates
STOP_LOSS_MULTIPLE = 2.0  # spread > 2x entry = stop loss
MAX_HOLD_S = 86400  # 24h max hold


@dataclass
class SignalEvent:
    """Entry or exit signal from the adaptive threshold engine."""
    symbol: str
    signal_type: str           # "ENTRY" | "EXIT_REVERT" | "EXIT_STOP_LOSS" | "EXIT_MAX_HOLD"
    spread_snapshot: SpreadSnapshot
    threshold_p90: float
    threshold_p25: float
    threshold_median: float
    excess_bps: float          # P90 - P25
    timestamp: float


@dataclass
class OpenPosition:
    """Tracks an open position for exit signal detection."""
    symbol: str
    direction: str
    entry_spread: float
    entry_time: float
    entry_threshold_p90: float
    exit_threshold_p25: float
    position_id: str = ""


class AdaptiveThresholds:
    """
    Per-pair P90/P25 thresholds from rolling spread history.
    Same logic as paper trader's AdaptiveThresholds class.
    """

    def __init__(self, window: int = 720):  # 1h at 5s polling -> keep same for WS
        self.window = window
        self.history: dict[str, deque] = defaultdict(lambda: deque(maxlen=window))

    def update(self, symbol: str, abs_spread: float):
        """Add a new spread observation."""
        self.history[symbol].append(abs_spread)

    def ready(self, symbol: str) -> bool:
        """Have enough data to compute thresholds?"""
        return len(self.history[symbol]) >= self.window // 2

    def get_thresholds(self, symbol: str) -> Optional[dict]:
        """Compute current thresholds for a pair."""
        h = self.history[symbol]
        if len(h) < self.window // 2:
            return None
        arr = np.array(h)
        p25 = float(np.percentile(arr, 25))
        p50 = float(np.percentile(arr, 50))
        p90 = float(np.percentile(arr, 90))
        excess = p90 - p25
        return {
            "p25": p25,
            "median": p50,
            "p90": p90,
            "excess": excess,
            "viable": excess > MIN_EXCESS_BPS,
        }

    def load_from_mongodb(self, db_uri: str = "mongodb://localhost:27017/quants_lab"):
        """Bootstrap thresholds from historical quote snapshots.

        Tries multiple collections in order:
        1. arb_usdc_quote_snapshots (USDC paper trader — most relevant for USDC mode)
        2. arb_bn_usdc_bb_perp_snapshots (dual collector — USDC cross-venue)
        3. arb_quote_snapshots (original USDT paper trader — fallback, similar dynamics)
        """
        client = MongoClient(db_uri)
        db_name = db_uri.rsplit("/", 1)[-1]
        db = client[db_name]

        for sym in list(self.history.keys()) or []:
            docs = []

            # Try USDC paper trader first
            docs = list(db.arb_usdc_quote_snapshots.find(
                {"symbol": sym}, {"best_spread": 1, "_id": 0},
            ).sort("timestamp", -1).limit(self.window))

            # Try dual collector (Binance USDC vs Bybit perp)
            if len(docs) < self.window // 2:
                dual_docs = list(db.arb_bn_usdc_bb_perp_snapshots.find(
                    {"symbol_bb": sym}, {"best_spread": 1, "_id": 0},
                ).sort("timestamp", -1).limit(self.window - len(docs)))
                docs.extend(dual_docs)

            # Fallback to original USDT collector
            if len(docs) < self.window // 2:
                orig_docs = list(db.arb_quote_snapshots.find(
                    {"symbol": sym}, {"best_spread": 1, "_id": 0},
                ).sort("timestamp", -1).limit(self.window - len(docs)))
                docs.extend(orig_docs)

            for d in reversed(docs):
                self.history[sym].append(abs(d["best_spread"]))
            if docs:
                logger.info(f"  {sym}: loaded {len(docs)} quotes from multiple sources")

    def seed_symbols(self, symbols: list[str]):
        """Initialize history deques for symbols."""
        for sym in symbols:
            if sym not in self.history:
                self.history[sym]  # triggers defaultdict creation


class SignalEngine:
    """
    Generates entry and exit signals from price feed data.

    Feed it spread snapshots, it emits signals based on adaptive thresholds.
    """

    def __init__(self, symbols: list[str], db_uri: str = "mongodb://localhost:27017/quants_lab"):
        self.symbols = symbols
        self.thresholds = AdaptiveThresholds()
        self.thresholds.seed_symbols(symbols)
        self.open_positions: dict[str, OpenPosition] = {}  # symbol -> position
        self._update_counts: dict[str, int] = defaultdict(int)  # per-symbol counter
        self._subsample_rate = 12  # update thresholds every 12th spread (~1/min at WS rate)

    def load_history(self, db_uri: str):
        """Bootstrap thresholds from MongoDB quote snapshots."""
        self.thresholds.load_from_mongodb(db_uri)

    def update_thresholds(self, snap: 'SpreadSnapshot'):
        """Update adaptive thresholds from a spread snapshot.

        Call from the main loop on every tick regardless of entry/exit state,
        so thresholds stay current even while holding positions.
        """
        sym = snap.symbol
        if not snap.fresh:
            return
        self._update_counts[sym] += 1
        if self._update_counts[sym] % self._subsample_rate == 0:
            self.thresholds.update(sym, abs(snap.spread_bps))

    def check_entry(self, snap: SpreadSnapshot) -> Optional[SignalEvent]:
        """
        Check if current spread triggers an entry signal.
        Returns SignalEvent or None.

        CRITICAL: spread_bps must be POSITIVE for the chosen direction.
        A positive spread means the arb is profitable (buy low, sell high).
        A negative spread means you'd PAY to enter -- never trade this.
        """
        if not snap.fresh:
            return None

        sym = snap.symbol
        if sym in self.open_positions:
            return None  # already have a position

        # SAFETY: spread must be positive (profitable direction)
        if snap.spread_bps <= 0:
            return None

        thresh = self.thresholds.get_thresholds(sym)
        if not thresh or not thresh["viable"]:
            return None

        if snap.spread_bps >= thresh["p90"]:
            logger.info(
                f"ENTRY SIGNAL: {sym} spread={snap.spread_bps:.1f}bp >= P90={thresh['p90']:.1f}bp "
                f"(P25={thresh['p25']:.1f} excess={thresh['excess']:.1f} direction={snap.direction})"
            )
            return SignalEvent(
                symbol=sym,
                signal_type="ENTRY",
                spread_snapshot=snap,
                threshold_p90=thresh["p90"],
                threshold_p25=thresh["p25"],
                threshold_median=thresh["median"],
                excess_bps=thresh["excess"],
                timestamp=time.time(),
            )

        return None

    def check_exit(self, snap: SpreadSnapshot) -> Optional[SignalEvent]:
        """
        Check if current spread triggers an exit signal for an open position.
        Returns SignalEvent or None.

        Note: snap.fresh may be False if using get_spread_for_exit() with
        relaxed freshness. We still allow exits with slightly stale data
        to prevent positions from being trapped.
        """
        sym = snap.symbol
        pos = self.open_positions.get(sym)
        if not pos:
            return None

        # Don't require snap.fresh for exits -- stale exit is better than trapped position

        abs_spread = abs(snap.spread_bps)
        now = time.time()
        hold_s = now - pos.entry_time

        # Reversion exit: spread reverted below P25
        if abs_spread <= pos.exit_threshold_p25:
            logger.info(f"EXIT REVERT: {sym} spread={abs_spread:.1f}bp <= P25={pos.exit_threshold_p25:.1f}bp (hold={hold_s:.0f}s)")
            return SignalEvent(
                symbol=sym,
                signal_type="EXIT_REVERT",
                spread_snapshot=snap,
                threshold_p90=pos.entry_threshold_p90,
                threshold_p25=pos.exit_threshold_p25,
                threshold_median=0,
                excess_bps=0,
                timestamp=now,
            )

        # Stop loss: spread widened FURTHER in same direction (reversion hasn't happened)
        # For spike fade: entry at high spread, exit at low spread. If spread doubles
        # from entry, the spike is getting worse, not reverting. That's a stop loss.
        # If spread goes negative (crossed zero), that means FULL REVERSION = profitable.
        # Use actual spread value (not abs) to detect same-direction widening.
        if pos.entry_spread > 0 and snap.spread_bps > pos.entry_spread * STOP_LOSS_MULTIPLE:
            logger.warning(f"EXIT STOP LOSS: {sym} spread={snap.spread_bps:.1f}bp > {STOP_LOSS_MULTIPLE}x entry={pos.entry_spread:.1f}bp (hold={hold_s:.0f}s)")
            return SignalEvent(
                symbol=sym,
                signal_type="EXIT_STOP_LOSS",
                spread_snapshot=snap,
                threshold_p90=pos.entry_threshold_p90,
                threshold_p25=pos.exit_threshold_p25,
                threshold_median=0,
                excess_bps=0,
                timestamp=now,
            )

        # Max hold
        if hold_s > MAX_HOLD_S:
            return SignalEvent(
                symbol=sym,
                signal_type="EXIT_MAX_HOLD",
                spread_snapshot=snap,
                threshold_p90=pos.entry_threshold_p90,
                threshold_p25=pos.exit_threshold_p25,
                threshold_median=0,
                excess_bps=0,
                timestamp=now,
            )

        return None

    def register_position(self, pos: OpenPosition):
        """Register a newly opened position for exit signal tracking."""
        self.open_positions[pos.symbol] = pos
        logger.info(f"Position registered: {pos.symbol} entry_spread={pos.entry_spread:.0f}bp")

    def unregister_position(self, symbol: str):
        """Remove a closed position."""
        self.open_positions.pop(symbol, None)

    def verify_spread_at_execution(self, snap: SpreadSnapshot, original_signal: SignalEvent) -> bool:
        """
        Re-check spread at the moment of order submission.
        If spread has narrowed below threshold, skip the trade.

        This is the defense against execution skew (100-800ms between signal and order).
        """
        if not snap.fresh:
            return False

        # Re-check: spread must still be positive AND exceed P90
        # Must match check_entry's sign check: positive = profitable direction
        if snap.spread_bps <= 0:
            return False

        thresh = self.thresholds.get_thresholds(snap.symbol)
        if not thresh:
            return False

        # Accept 80% of P90 at verify time — spread naturally narrows 10-30bp
        # between signal and execution on thin USDC books. If we require full P90,
        # most trades are rejected at verify despite being profitable.
        return snap.spread_bps >= thresh["p90"] * 0.8

    def status(self) -> dict:
        """Return current signal engine state for monitoring."""
        result = {"open_positions": len(self.open_positions), "pairs": {}}
        for sym in self.symbols:
            thresh = self.thresholds.get_thresholds(sym)
            result["pairs"][sym] = {
                "ready": self.thresholds.ready(sym),
                "p90": thresh["p90"] if thresh else None,
                "p25": thresh["p25"] if thresh else None,
                "viable": thresh["viable"] if thresh else False,
                "has_position": sym in self.open_positions,
            }
        return result
