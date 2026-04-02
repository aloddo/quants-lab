"""
Market regime classification per pair.

Risk-Off: return_4h < -3% OR ATR percentile > 90th
BTC Contagion: if BTC is Risk-Off → global hard block on E1 entries
Per-pair regime label: compressed, elevated, normal, risk_off
"""
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd

from core.data_structures.candles import Candles
from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature


class MarketRegimeConfig(FeatureConfig):
    name: str = "market_regime"
    risk_off_return_threshold: float = -0.03  # -3% in 4h
    risk_off_atr_percentile: float = 0.90
    compression_threshold: float = 0.20
    elevated_threshold: float = 0.70


class MarketRegimeFeature(FeatureBase[MarketRegimeConfig]):
    """
    Reads ATR and momentum features from previously computed data.
    Should run AFTER ATRFeature and MomentumFeature.

    For the task layer: this feature is computed from existing feature store
    data, not directly from candles. The create_feature_from_values method
    is the primary interface.
    """

    def __init__(self, feature_config: Optional[MarketRegimeConfig] = None):
        super().__init__(feature_config or MarketRegimeConfig())

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        # Requires atr_percentile_90d and return_4h columns already present
        df = data.copy()
        if "atr_percentile_90d" not in df.columns or "return_4h" not in df.columns:
            df["regime_label"] = "unknown"
            df["risk_off"] = False
            return df

        df["risk_off"] = (
            (df["return_4h"] < self.config.risk_off_return_threshold)
            | (df["atr_percentile_90d"] > self.config.risk_off_atr_percentile)
        )
        df["regime_label"] = "normal"
        df.loc[df["atr_percentile_90d"] < self.config.compression_threshold, "regime_label"] = "compressed"
        df.loc[df["atr_percentile_90d"] > self.config.elevated_threshold, "regime_label"] = "elevated"
        df.loc[df["risk_off"], "regime_label"] = "risk_off"
        return df

    def create_feature(self, candles: Candles) -> Feature:
        df = self.calculate(candles.data)
        last = df.iloc[-1]
        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=candles.trading_pair,
            connector_name=candles.connector_name,
            value={"risk_off": 1.0 if last.get("risk_off", False) else 0.0, "btc_contagion": 0.0},
            info={"regime_label": str(last.get("regime_label", "unknown"))},
        )

    def create_feature_from_values(
        self,
        trading_pair: str,
        connector_name: str,
        atr_percentile_90d: Optional[float],
        return_4h: Optional[float],
        btc_is_risk_off: bool = False,
    ) -> Feature:
        """Build regime feature from pre-computed values."""
        risk_off = False
        regime_label = "normal"

        if return_4h is not None and return_4h < self.config.risk_off_return_threshold:
            risk_off = True
        if atr_percentile_90d is not None and atr_percentile_90d > self.config.risk_off_atr_percentile:
            risk_off = True

        if risk_off:
            regime_label = "risk_off"
        elif atr_percentile_90d is not None:
            if atr_percentile_90d < self.config.compression_threshold:
                regime_label = "compressed"
            elif atr_percentile_90d > self.config.elevated_threshold:
                regime_label = "elevated"

        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=trading_pair,
            connector_name=connector_name,
            value={
                "risk_off": 1.0 if risk_off else 0.0,
                "btc_contagion": 1.0 if btc_is_risk_off else 0.0,
            },
            info={"regime_label": regime_label},
        )
