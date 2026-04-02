"""Candle body/wick ratios vs ATR and price position in range."""
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from core.data_structures.candles import Candles
from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature

from app.features.helpers import atr


class CandleStructureConfig(FeatureConfig):
    name: str = "candle_structure"
    atr_period: int = 14
    range_period: int = 20


class CandleStructureFeature(FeatureBase[CandleStructureConfig]):

    def __init__(self, feature_config: Optional[CandleStructureConfig] = None):
        super().__init__(feature_config or CandleStructureConfig())

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        atr_val = atr(df, self.config.atr_period)
        body = (df["close"] - df["open"]).abs()
        total_range = df["high"] - df["low"]
        wick = total_range - body

        df["body_atr_ratio"] = body / atr_val
        df["wick_atr_ratio"] = wick / atr_val

        # Price position in 20-period range (0 = at low, 1 = at high)
        rh = df["high"].rolling(self.config.range_period).max()
        rl = df["low"].rolling(self.config.range_period).min()
        rng = rh - rl
        df["price_position_in_range"] = (df["close"] - rl) / rng.replace(0, float("nan"))
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
                "body_atr_ratio": float(last["body_atr_ratio"]) if pd.notna(last["body_atr_ratio"]) else None,
                "wick_atr_ratio": float(last["wick_atr_ratio"]) if pd.notna(last["wick_atr_ratio"]) else None,
                "price_position_in_range": float(last["price_position_in_range"]) if pd.notna(last["price_position_in_range"]) else None,
            },
        )
