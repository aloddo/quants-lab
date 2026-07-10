"""
External Feature Base — for features sourced from external APIs rather than candle data.

Subclass this instead of FeatureBase when your feature data comes from an
external service (sentiment APIs, on-chain data, order flow, etc.) rather
than from OHLCV candles.

The FeatureComputationTask handles external features in a separate pass:
it calls `fetch()` instead of `calculate()` + `create_feature()`.

Example implementation:

    class SentimentFeature(ExternalFeatureBase[SentimentConfig]):
        async def fetch(self, pair: str, connector_name: str) -> Feature:
            data = await self._call_sentiment_api(pair)
            return Feature(
                timestamp=datetime.now(timezone.utc),
                feature_name=self.config.name,
                trading_pair=pair,
                connector_name=connector_name,
                value={"score": data["score"], "source": "lunarcrush"},
            )
"""
from abc import abstractmethod
from datetime import datetime, timezone
from typing import Optional, TypeVar

import pandas as pd

from core.data_structures.candles import Candles
from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature

T = TypeVar("T", bound=FeatureConfig)


class ExternalFeatureBase(FeatureBase[T]):
    """Base class for features sourced from external APIs.

    Subclasses must implement `fetch()` which returns a Feature object
    with data from the external source. The `calculate()` and
    `create_feature()` methods are no-ops since we don't use candle data.
    """

    # Marker for FeatureComputationTask to dispatch correctly
    source_type: str = "external"

    @abstractmethod
    async def fetch(self, pair: str, connector_name: str) -> Feature:
        """Fetch latest data from external source and return a Feature.

        This is called by FeatureComputationTask instead of
        calculate() + create_feature().
        """
        ...

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        """No-op — external features don't compute from candle data."""
        return data

    def create_feature(self, candles: Candles) -> Feature:
        """No-op — use fetch() instead."""
        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=candles.trading_pair,
            connector_name=candles.connector_name,
            value={},
        )
