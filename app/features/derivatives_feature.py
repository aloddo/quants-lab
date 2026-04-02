"""
Derivatives features: OI change, funding rate/delta, L/S ratio, relative strength vs BTC.

Reads from MongoDB collections populated by BybitDerivativesTask.
Unlike other features that read from parquet candles, this one queries MongoDB directly.
"""
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import pandas as pd

from core.data_structures.candles import Candles
from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature


class DerivativesConfig(FeatureConfig):
    name: str = "derivatives"


class DerivativesFeature(FeatureBase[DerivativesConfig]):
    """
    This feature is special: it reads from MongoDB (derivatives collections)
    rather than from candle data. The create_feature method requires
    a pre-populated `derivatives_data` dict passed via the info field
    of the candles object or externally.

    For the FeatureComputationTask, this feature's data is assembled
    by querying MongoDB and passed in as extra context.
    """

    def __init__(self, feature_config: Optional[DerivativesConfig] = None):
        super().__init__(feature_config or DerivativesConfig())

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        # No-op for candle-based calculation — derivatives come from MongoDB
        return data

    def create_feature(self, candles: Candles) -> Feature:
        # Returns an empty feature — actual data comes from create_feature_from_data
        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=candles.trading_pair,
            connector_name=candles.connector_name,
            value={},
        )

    def create_feature_from_data(
        self,
        trading_pair: str,
        connector_name: str,
        oi_docs: list[dict],
        funding_docs: list[dict],
        ls_docs: list[dict],
        btc_return_4h: Optional[float] = None,
        btc_return_24h: Optional[float] = None,
        pair_return_4h: Optional[float] = None,
        pair_return_24h: Optional[float] = None,
    ) -> Feature:
        """Build derivatives feature from MongoDB query results."""
        value: Dict[str, Any] = {}

        # OI change
        if len(oi_docs) >= 2:
            latest_oi = oi_docs[0]["oi_value"]
            prev_oi = oi_docs[1]["oi_value"]
            value["oi_change_1h_pct"] = (latest_oi / prev_oi - 1) * 100 if prev_oi else 0.0
            value["oi_increasing"] = 1.0 if value.get("oi_change_1h_pct", 0) > 0 else 0.0
        if len(oi_docs) >= 5:
            value["oi_change_4h_pct"] = (oi_docs[0]["oi_value"] / oi_docs[4]["oi_value"] - 1) * 100

        # Funding
        if funding_docs:
            value["funding_rate"] = funding_docs[0]["funding_rate"]
            value["funding_neutral"] = 1.0 if -0.0001 <= funding_docs[0]["funding_rate"] <= 0.0005 else 0.0
            if len(funding_docs) >= 4:
                value["funding_delta"] = funding_docs[0]["funding_rate"] - funding_docs[3]["funding_rate"]

        # L/S ratio
        if ls_docs:
            value["ls_buy_ratio"] = ls_docs[0]["buy_ratio"]
            if len(ls_docs) >= 2:
                value["ls_change"] = ls_docs[0]["buy_ratio"] - ls_docs[1]["buy_ratio"]

        # Relative strength vs BTC
        if (
            trading_pair != "BTC-USDT"
            and pair_return_4h is not None
            and btc_return_4h is not None
            and btc_return_4h != 0
        ):
            value["rs_vs_btc_4h"] = pair_return_4h / abs(btc_return_4h)
        if (
            trading_pair != "BTC-USDT"
            and pair_return_24h is not None
            and btc_return_24h is not None
            and btc_return_24h != 0
        ):
            value["rs_vs_btc_24h"] = pair_return_24h / abs(btc_return_24h)

        rs_4h = value.get("rs_vs_btc_4h")
        rs_24h = value.get("rs_vs_btc_24h")
        value["rs_aligned"] = 1.0 if (
            rs_4h is not None and rs_4h > 1.3 and rs_24h is not None and rs_24h > 1.3
        ) else 0.0

        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=trading_pair,
            connector_name=connector_name,
            value=value,
        )
