"""Range boundaries, width, and expansion detection."""
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from core.data_structures.candles import Candles
from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature
from app.features.decorators import screening_feature


class RangeConfig(FeatureConfig):
    name: str = "range"
    period: int = 30  # V3.2 locked, validated via stress testing (Apr 2026)
    expansion_lookback: int = 5
    expansion_threshold: float = 0.20


@screening_feature
class RangeFeature(FeatureBase[RangeConfig]):

    def __init__(self, feature_config: Optional[RangeConfig] = None):
        super().__init__(feature_config or RangeConfig())

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        p = self.config.period
        df["range_high_20"] = df["high"].shift(1).rolling(p).max()
        df["range_low_20"] = df["low"].shift(1).rolling(p).min()
        df["range_width"] = df["range_high_20"] - df["range_low_20"]
        # Range expanding = % change of range width over last N bars
        df["range_expanding"] = df["range_width"].pct_change(self.config.expansion_lookback)
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
                "range_high_20": float(last["range_high_20"]) if pd.notna(last["range_high_20"]) else None,
                "range_low_20": float(last["range_low_20"]) if pd.notna(last["range_low_20"]) else None,
                "range_width": float(last["range_width"]) if pd.notna(last["range_width"]) else None,
                "range_expanding": float(last["range_expanding"]) if pd.notna(last["range_expanding"]) else None,
            },
        )
