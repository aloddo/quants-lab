"""ATR, ATR percentile (90d rolling), and compression flag."""
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from core.data_structures.candles import Candles
from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature

from app.features.helpers import atr, percentile_rank


class ATRConfig(FeatureConfig):
    name: str = "atr"
    atr_period: int = 14
    percentile_window: int = 90 * 24  # 90 days of 1h bars
    compression_threshold: float = 0.35  # V3.2 locked, validated via stress testing (Apr 2026)


class ATRFeature(FeatureBase[ATRConfig]):

    def __init__(self, feature_config: Optional[ATRConfig] = None):
        super().__init__(feature_config or ATRConfig())

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        df["atr_14_1h"] = atr(df, self.config.atr_period)
        df["atr_percentile_90d"] = percentile_rank(df["atr_14_1h"], self.config.percentile_window)
        df["compression_flag"] = df["atr_percentile_90d"] < self.config.compression_threshold
        return df

    def create_feature(self, candles: Candles) -> Feature:
        df = self.calculate(candles.data)
        last = df.iloc[-1]
        value = {}
        if pd.notna(last["atr_14_1h"]):
            value["atr_14_1h"] = float(last["atr_14_1h"])
        if pd.notna(last["atr_percentile_90d"]):
            value["atr_percentile_90d"] = float(last["atr_percentile_90d"])
        value["compression_flag"] = 1.0 if last.get("compression_flag") else 0.0

        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=candles.trading_pair,
            connector_name=candles.connector_name,
            value=value if value else 0.0,
        )
