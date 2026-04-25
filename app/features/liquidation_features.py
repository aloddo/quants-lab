"""
Liquidation-derived features from Coinalyze data.

Computes from MongoDB `coinalyze_liquidations` and `coinalyze_oi`:
- Liquidation cascade detector (acceleration of liquidation volume)
- Liquidation imbalance momentum
- Organic OI change (OI change + liquidation volume)
- Cross-exchange OI dynamics
- Liquidation heat map approximation

Also includes Fear & Greed as a sentiment feature.

Schema (coinalyze_liquidations):
  timestamp_utc (Int64 ms), pair, resolution, long_liquidations_usd,
  short_liquidations_usd, total_liquidations_usd

Schema (coinalyze_oi):
  timestamp_utc (Int64 ms), pair, resolution, oi_open, oi_high, oi_low, oi_close
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature
from core.data_structures.candles import Candles


class LiquidationConfig(FeatureConfig):
    name: str = "liquidation"
    cascade_threshold: float = 2.0  # volume must 2x previous bar for cascade
    cascade_min_bars: int = 3  # consecutive accelerating bars


class LiquidationFeature(FeatureBase[LiquidationConfig]):
    """
    Liquidation feature — reads from MongoDB coinglass_liquidations and coinglass_oi.

    For FeatureComputationTask: data is assembled by querying MongoDB and passed
    in via create_feature_from_data().
    """

    HISTORY_DEPTH = 100  # hourly bars

    def __init__(self, feature_config: Optional[LiquidationConfig] = None):
        super().__init__(feature_config or LiquidationConfig())

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        return data

    def create_feature(self, candles: Candles) -> Feature:
        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=candles.trading_pair,
            connector_name=candles.connector_name,
            value={},
        )

    @staticmethod
    def _compute_imbalance(doc: Dict) -> float:
        """Compute liquidation imbalance: (long - short) / total.

        Coinalyze does not store an imbalance field directly,
        so we compute it from long/short/total liquidation values.
        """
        total = doc.get("total_liquidations_usd", 0)
        if total == 0:
            return 0.0
        long_liq = doc.get("long_liquidations_usd", 0)
        short_liq = doc.get("short_liquidations_usd", 0)
        return (long_liq - short_liq) / total

    def create_feature_from_data(
        self,
        trading_pair: str,
        connector_name: str,
        liq_docs: List[Dict],
        oi_docs: List[Dict],
        fear_greed_docs: Optional[List[Dict]] = None,
    ) -> Feature:
        """Build liquidation features from Coinalyze data.

        liq_docs: from coinalyze_liquidations, sorted by timestamp_utc DESC
        oi_docs: from coinalyze_oi, sorted by timestamp_utc DESC
        fear_greed_docs: from fear_greed_index, sorted by timestamp_utc DESC
        """
        value: Dict[str, Any] = {}

        # ── Liquidation features ─────────────────────────────
        if len(liq_docs) >= 2:
            # Current values (coinalyze uses plural field names)
            value["liq_total_usd"] = liq_docs[0].get("total_liquidations_usd", 0)
            value["liq_imbalance"] = self._compute_imbalance(liq_docs[0])
            value["liq_long_usd"] = liq_docs[0].get("long_liquidations_usd", 0)
            value["liq_short_usd"] = liq_docs[0].get("short_liquidations_usd", 0)

            # Liquidation momentum (change from previous bar)
            prev_total = liq_docs[1].get("total_liquidations_usd", 0)
            if prev_total > 0:
                value["liq_momentum"] = (
                    liq_docs[0].get("total_liquidations_usd", 0) / prev_total - 1
                )

            # Imbalance momentum
            value["liq_imbalance_momentum"] = (
                self._compute_imbalance(liq_docs[0])
                - self._compute_imbalance(liq_docs[1])
            )

        # Cascade detection
        if len(liq_docs) >= self.config.cascade_min_bars + 1:
            cascade = self._detect_cascade(liq_docs)
            value["liq_cascade_active"] = 1.0 if cascade["active"] else 0.0
            value["liq_cascade_direction"] = cascade["direction"]
            value["liq_cascade_magnitude"] = cascade["magnitude"]

        # Rolling liquidation stats (24h)
        if len(liq_docs) >= 24:
            total_24h = sum(
                d.get("total_liquidations_usd", 0) for d in liq_docs[:24]
            )
            long_24h = sum(
                d.get("long_liquidations_usd", 0) for d in liq_docs[:24]
            )
            short_24h = sum(
                d.get("short_liquidations_usd", 0) for d in liq_docs[:24]
            )
            value["liq_total_24h_usd"] = total_24h
            value["liq_imbalance_24h"] = (
                (long_24h - short_24h) / total_24h if total_24h > 0 else 0.0
            )

        # ── OI features (cross-exchange) ─────────────────────
        if len(oi_docs) >= 2:
            value["cg_oi_close"] = oi_docs[0].get("oi_close", 0)
            prev_oi = oi_docs[1].get("oi_close", 0)
            if prev_oi > 0:
                value["cg_oi_change_pct"] = (
                    oi_docs[0].get("oi_close", 0) / prev_oi - 1
                ) * 100

            # OI range (high - low) as % of close = intra-hour OI volatility
            oi_close = oi_docs[0].get("oi_close", 0)
            oi_range = oi_docs[0].get("oi_high", 0) - oi_docs[0].get("oi_low", 0)
            if oi_close > 0:
                value["cg_oi_range_pct"] = (oi_range / oi_close) * 100

        # Organic OI change = OI_change + liquidation_volume
        if len(oi_docs) >= 2 and len(liq_docs) >= 1:
            oi_change = oi_docs[0].get("oi_close", 0) - oi_docs[1].get("oi_close", 0)
            liq_volume = liq_docs[0].get("total_liquidations_usd", 0)
            value["organic_oi_change"] = oi_change + liq_volume

        # OI trend (4-bar)
        if len(oi_docs) >= 5:
            value["cg_oi_change_4h_pct"] = (
                (oi_docs[0].get("oi_close", 0) / oi_docs[4].get("oi_close", 1) - 1)
                * 100
            )

        # ── Fear & Greed ─────────────────────────────────────
        if fear_greed_docs:
            value["fear_greed_value"] = fear_greed_docs[0].get("value", 50)
            value["fear_greed_class"] = fear_greed_docs[0].get("classification", "Neutral")

            # Extreme fear/greed flags (contrarian signals)
            fg_val = fear_greed_docs[0].get("value", 50)
            value["extreme_fear"] = 1.0 if fg_val <= 20 else 0.0
            value["extreme_greed"] = 1.0 if fg_val >= 80 else 0.0

            # Fear & Greed momentum (change over last 7 days)
            if len(fear_greed_docs) >= 7:
                value["fear_greed_momentum_7d"] = (
                    fear_greed_docs[0].get("value", 50)
                    - fear_greed_docs[6].get("value", 50)
                )

        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=trading_pair,
            connector_name=connector_name,
            value=value,
        )

    def _detect_cascade(self, liq_docs: List[Dict]) -> Dict:
        """Detect liquidation cascade: accelerating volume over N consecutive bars."""
        threshold = self.config.cascade_threshold
        min_bars = self.config.cascade_min_bars

        # Check consecutive acceleration (most recent first)
        accel_count = 0
        direction_sum = 0.0
        magnitude = 0.0

        for i in range(len(liq_docs) - 1):
            current = liq_docs[i].get("total_liquidations_usd", 0)
            previous = liq_docs[i + 1].get("total_liquidations_usd", 0)

            if previous > 0 and current >= previous * threshold:
                accel_count += 1
                magnitude += current
                direction_sum += self._compute_imbalance(liq_docs[i])
            else:
                break

            if accel_count >= min_bars:
                break

        active = accel_count >= min_bars
        # Direction: positive = long squeeze (bearish), negative = short squeeze (bullish)
        direction = direction_sum / accel_count if accel_count > 0 else 0.0

        return {
            "active": active,
            "direction": direction,
            "magnitude": magnitude,
        }
