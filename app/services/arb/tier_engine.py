"""
TierEngine — Dynamic pair tiering for H2 spike-fade arb.

Reads collector MongoDB (arb_bn_usdc_bb_perp_snapshots) every N minutes,
computes P90/P25/excess per pair, assigns tiers:
  Tier A: excess > 35bp → full size, quantile gate 0.87
  Tier B: excess 31-35bp → half size, quantile gate 0.90
  Tier C: excess 25-31bp → WS connected, entries blocked
  None: below 25bp → not connected

No pair gets permanent status. Every pair earns its tier from rolling metrics.
"""
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
from pymongo import MongoClient

logger = logging.getLogger(__name__)

# ── Tier thresholds ──────────────────────────────────────────

TIER_A_EXCESS = 35.0   # bp
TIER_B_EXCESS = 31.0   # bp (fee threshold)
TIER_C_EXCESS = 25.0   # bp (watch-only)
LOOKBACK_HOURS = 6     # rolling window for tier computation

TIER_A_QUANTILE = 0.87
TIER_B_QUANTILE = 0.90


@dataclass
class TierInfo:
    """Tier assignment for a single pair."""
    symbol_bn: str        # Binance USDC symbol (e.g., "LSKUSDC")
    symbol_bb: str        # Bybit perp symbol (e.g., "LSKUSDT")
    tier: Optional[str]   # "A", "B", "C", or None
    excess: float         # P90 - P25
    p90: float
    p25: float
    avg_spread: float
    quantile_gate: float  # 0.87 for A, 0.90 for B, 1.0 for C (blocks all)
    position_size_mult: float  # 1.0 for A, 0.5 for B, 0.0 for C/None
    doc_count: int        # number of snapshots in lookback window


@dataclass
class TierTransition:
    """Record of a tier change for logging."""
    symbol_bn: str
    old_tier: Optional[str]
    new_tier: Optional[str]
    excess: float
    timestamp: float


class TierEngine:
    """Computes dynamic pair tiers from collector MongoDB data."""

    def __init__(
        self,
        mongo_uri: str = "mongodb://localhost:27017/quants_lab",
        excess_a: float = TIER_A_EXCESS,
        excess_b: float = TIER_B_EXCESS,
        excess_c: float = TIER_C_EXCESS,
        lookback_hours: float = LOOKBACK_HOURS,
    ):
        self._mongo_uri = mongo_uri
        self._db_name = mongo_uri.rsplit("/", 1)[-1]
        self._excess_a = excess_a
        self._excess_b = excess_b
        self._excess_c = excess_c
        self._lookback_hours = lookback_hours

        # Current state
        self.tiers: dict[str, TierInfo] = {}  # symbol_bn -> TierInfo
        self._last_recompute: float = 0.0
        self._transition_log: list[TierTransition] = []

        # MongoDB client (lazy init)
        self._client: Optional[MongoClient] = None
        self._coll = None
        self._tier_history_coll = None

    def _ensure_mongo(self):
        """Lazy-init MongoDB connection."""
        if self._client is None:
            self._client = MongoClient(self._mongo_uri, serverSelectionTimeoutMS=5000)
            db = self._client[self._db_name]
            self._coll = db["arb_bn_usdc_bb_perp_snapshots"]
            self._tier_history_coll = db["arb_h2_tier_history"]

    def recompute(self) -> dict[str, TierInfo]:
        """
        Query collector MongoDB for last N hours of snapshots.
        Compute P25/P90/excess per pair. Assign tiers.

        Returns dict of symbol_bn -> TierInfo for all pairs above Tier C threshold.
        """
        self._ensure_mongo()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self._lookback_hours)

        # Aggregation pipeline: per-pair P25, P90, avg, count
        pipeline = [
            {"$match": {"timestamp": {"$gte": cutoff}}},
            {"$group": {
                "_id": "$symbol_bn",
                "p25": {"$percentile": {
                    "input": {"$abs": "$best_spread"},
                    "p": [0.25],
                    "method": "approximate",
                }},
                "p90": {"$percentile": {
                    "input": {"$abs": "$best_spread"},
                    "p": [0.90],
                    "method": "approximate",
                }},
                "avg_spread": {"$avg": {"$abs": "$best_spread"}},
                "count": {"$sum": 1},
            }},
            {"$match": {"count": {"$gte": 100}}},  # need enough data points
            # No sort here — we sort by excess in Python after computing it
        ]

        try:
            results = list(self._coll.aggregate(pipeline, allowDiskUse=True))
        except Exception as e:
            logger.error(f"TierEngine: MongoDB aggregation failed: {e}")
            return self.tiers  # return stale tiers on failure

        old_tiers = dict(self.tiers)
        new_tiers: dict[str, TierInfo] = {}
        transitions: list[TierTransition] = []

        for doc in results:
            sym_bn = doc["_id"]
            if not sym_bn:
                continue

            # $percentile returns arrays
            p25 = doc["p25"][0] if isinstance(doc["p25"], list) else doc["p25"]
            p90 = doc["p90"][0] if isinstance(doc["p90"], list) else doc["p90"]
            excess = p90 - p25
            avg_spread = doc["avg_spread"]
            count = doc["count"]

            # Derive Bybit symbol: replace USDC with USDT
            sym_bb = sym_bn.replace("USDC", "USDT")

            # Assign tier
            if excess >= self._excess_a:
                tier = "A"
                quantile = TIER_A_QUANTILE
                size_mult = 1.0
            elif excess >= self._excess_b:
                tier = "B"
                quantile = TIER_B_QUANTILE
                size_mult = 0.5
            elif excess >= self._excess_c:
                tier = "C"
                quantile = 1.0  # entries blocked
                size_mult = 0.0
            else:
                continue  # below Tier C, skip

            info = TierInfo(
                symbol_bn=sym_bn,
                symbol_bb=sym_bb,
                tier=tier,
                excess=excess,
                p90=p90,
                p25=p25,
                avg_spread=avg_spread,
                quantile_gate=quantile,
                position_size_mult=size_mult,
                doc_count=count,
            )
            new_tiers[sym_bn] = info

            # Check for tier transition
            old_info = old_tiers.get(sym_bn)
            old_tier = old_info.tier if old_info else None
            if old_tier != tier:
                t = TierTransition(
                    symbol_bn=sym_bn,
                    old_tier=old_tier,
                    new_tier=tier,
                    excess=excess,
                    timestamp=time.time(),
                )
                transitions.append(t)

        # Check for pairs that dropped out entirely
        for sym_bn, old_info in old_tiers.items():
            if sym_bn not in new_tiers and old_info.tier is not None:
                transitions.append(TierTransition(
                    symbol_bn=sym_bn,
                    old_tier=old_info.tier,
                    new_tier=None,
                    excess=0.0,
                    timestamp=time.time(),
                ))

        self.tiers = new_tiers
        self._last_recompute = time.time()
        self._transition_log.extend(transitions)

        # Log transitions to MongoDB
        if transitions:
            self._log_transitions(transitions)

        # Log summary
        tier_counts = {"A": 0, "B": 0, "C": 0}
        for info in new_tiers.values():
            if info.tier:
                tier_counts[info.tier] += 1

        logger.info(
            f"TierEngine: A={tier_counts['A']} B={tier_counts['B']} C={tier_counts['C']} "
            f"({len(transitions)} transitions, {len(results)} pairs analyzed)"
        )

        return new_tiers

    def _log_transitions(self, transitions: list[TierTransition]):
        """Log tier transitions to MongoDB for V5 analysis."""
        try:
            docs = [
                {
                    "symbol_bn": t.symbol_bn,
                    "old_tier": t.old_tier,
                    "new_tier": t.new_tier,
                    "excess": t.excess,
                    "timestamp": datetime.fromtimestamp(t.timestamp, tz=timezone.utc),
                }
                for t in transitions
            ]
            self._tier_history_coll.insert_many(docs)
        except Exception as e:
            logger.warning(f"TierEngine: failed to log transitions: {e}")

    def tradable_pairs(self) -> list[TierInfo]:
        """Return Tier A and B pairs (tradable), sorted by excess descending."""
        return sorted(
            [info for info in self.tiers.values() if info.tier in ("A", "B")],
            key=lambda x: -x.excess,
        )

    def connected_pairs(self) -> list[TierInfo]:
        """Return Tier A, B, and C pairs (should have WS connected), sorted by excess descending."""
        return sorted(self.tiers.values(), key=lambda x: -x.excess)

    def get_tier(self, symbol_bn: str) -> Optional[TierInfo]:
        """Look up tier for a specific pair."""
        return self.tiers.get(symbol_bn)

    def get_tier_by_bb(self, symbol_bb: str) -> Optional[TierInfo]:
        """Look up tier by Bybit symbol (e.g., LSKUSDT)."""
        for info in self.tiers.values():
            if info.symbol_bb == symbol_bb:
                return info
        return None

    def summary(self) -> str:
        """Human-readable tier summary for Telegram/logging."""
        lines = []
        for tier_name in ("A", "B", "C"):
            pairs = [
                info for info in self.tiers.values()
                if info.tier == tier_name
            ]
            if not pairs:
                continue
            pairs.sort(key=lambda x: -x.excess)
            pair_strs = [
                f"{p.symbol_bn.replace('USDC', '')}({p.excess:.0f})"
                for p in pairs
            ]
            lines.append(f"  Tier {tier_name}: {', '.join(pair_strs)}")
        return "\n".join(lines) if lines else "  No viable pairs"

    def close(self):
        """Clean up MongoDB connection."""
        if self._client:
            self._client.close()
            self._client = None
