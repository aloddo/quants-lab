"""Returns, EMA alignment, and momentum indicators."""
from datetime import datetime, timezone
from typing import Optional

import pandas as pd

from core.data_structures.candles import Candles
from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature


class MomentumConfig(FeatureConfig):
    name: str = "momentum"
    ema_short: int = 50
    ema_long: int = 200


class MomentumFeature(FeatureBase[MomentumConfig]):

    def __init__(self, feature_config: Optional[MomentumConfig] = None):
        super().__init__(feature_config or MomentumConfig())

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        df = data.copy()
        c = df["close"]
        # Returns
        df["return_1h"] = c.pct_change(1)
        df["return_4h"] = c.pct_change(4)
        df["return_24h"] = c.pct_change(24)
        # EMAs
        df["ema_50"] = c.ewm(span=self.config.ema_short, adjust=False).mean()
        df["ema_200"] = c.ewm(span=self.config.ema_long, adjust=False).mean()
        # Alignment: price > ema50 > ema200 = bullish (+1), inverse = bearish (-1), else 0
        df["ema_alignment"] = 0
        df.loc[(c > df["ema_50"]) & (df["ema_50"] > df["ema_200"]), "ema_alignment"] = 1
        df.loc[(c < df["ema_50"]) & (df["ema_50"] < df["ema_200"]), "ema_alignment"] = -1
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
                "return_1h": float(last["return_1h"]) if pd.notna(last["return_1h"]) else None,
                "return_4h": float(last["return_4h"]) if pd.notna(last["return_4h"]) else None,
                "return_24h": float(last["return_24h"]) if pd.notna(last["return_24h"]) else None,
                "ema_50": float(last["ema_50"]) if pd.notna(last["ema_50"]) else None,
                "ema_200": float(last["ema_200"]) if pd.notna(last["ema_200"]) else None,
                "ema_alignment": int(last["ema_alignment"]),
            },
        )
