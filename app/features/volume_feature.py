"""Volume average, z-score, and floor detection."""
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from core.data_structures.candles import Candles
from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature

from app.features.helpers import zscore


class VolumeConfig(FeatureConfig):
    name: str = "volume"
    window: int = 20
    floor_multiplier: float = 1.6  # V3.2 locked, validated via stress testing (Apr 2026)


class VolumeFeature(FeatureBase[VolumeConfig]):

    def __init__(self, feature_config: Optional[VolumeConfig] = None):
        super().__init__(feature_config or VolumeConfig())

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        w = self.config.window
        df["vol_avg_20"] = df["volume"].rolling(w).mean()
        df["vol_zscore_20"] = zscore(df["volume"], w)
        df["vol_floor_passed"] = df["volume"] > df["vol_avg_20"] * self.config.floor_multiplier
        return df

    def create_feature(self, candles: Candles) -> Feature:
        df = self.calculate(candles.data)
        last = df.iloc[-1]
        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=candles.trading_pair,
            connector_name=candles.connector_name,
            value={
                "vol_avg_20": float(last["vol_avg_20"]) if pd.notna(last["vol_avg_20"]) else None,
                "vol_zscore_20": float(last["vol_zscore_20"]) if pd.notna(last["vol_zscore_20"]) else None,
                "vol_floor_passed": 1.0 if last["vol_floor_passed"] else 0.0,
            },
        )
