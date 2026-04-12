"""
Derivatives features: OI change/RSI, funding rate/streak/z-score, L/S ratio/z-score,
relative strength vs BTC.

Reads from MongoDB collections populated by BybitDerivativesTask.
Unlike other features that read from parquet candles, this one queries MongoDB directly.

New indicators (Apr 2026):
- oi_rsi_14: RSI of OI changes — detects crowded positioning
- funding_streak: consecutive positive/negative funding periods
- funding_cumulative_8: sum of last 8 funding rates (24h of carry)
- funding_zscore_30: z-score of current funding vs 30-period rolling window
- ls_zscore_30: z-score of buy_ratio vs 30-period rolling window
"""
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from core.data_structures.candles import Candles
from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature


def _rsi(series: pd.Series, length: int = 14) -> float:
    """Compute RSI of a series, return last value. NaN if insufficient data."""
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=length, min_periods=length).mean()
    avg_loss = loss.rolling(window=length, min_periods=length).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    last = rsi.iloc[-1]
    return float(last) if pd.notna(last) else np.nan


def _zscore(series: pd.Series, window: int = 30) -> float:
    """Z-score of last value vs rolling window. NaN if insufficient data."""
    if len(series) < window:
        return np.nan
    rolling_mean = series.rolling(window=window).mean()
    rolling_std = series.rolling(window=window).std()
    last_std = rolling_std.iloc[-1]
    if pd.isna(last_std) or last_std == 0:
        return 0.0
    z = (series.iloc[-1] - rolling_mean.iloc[-1]) / last_std
    return float(z)


def _streak(values: list[float]) -> int:
    """Count consecutive same-sign values from most recent. Positive = positive streak."""
    if not values:
        return 0
    sign = 1 if values[0] >= 0 else -1
    count = 0
    for v in values:
        if (v >= 0 and sign == 1) or (v < 0 and sign == -1):
            count += 1
        else:
            break
    return count * sign


class DerivativesConfig(FeatureConfig):
    name: str = "derivatives"


class DerivativesFeature(FeatureBase[DerivativesConfig]):
    """
    Derivatives feature — reads from MongoDB (OI, funding, L/S ratio collections).

    For FeatureComputationTask: data is assembled by querying MongoDB and passed
    in via create_feature_from_data(). Requires 50+ docs per collection for
    time-series indicators (RSI, z-score).
    """

    # How many docs to request from MongoDB for time-series indicators
    HISTORY_DEPTH = 50

    def __init__(self, feature_config: Optional[DerivativesConfig] = None):
        super().__init__(feature_config or DerivativesConfig())

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
        """Build derivatives feature from MongoDB query results.

        Docs should be sorted by timestamp_utc DESC (newest first).
        For time-series indicators (RSI, z-score), pass 50+ docs.
        """
        value: Dict[str, Any] = {}

        # ── OI features ──────────────────────────────────────────
        if len(oi_docs) >= 2:
            latest_oi = oi_docs[0]["oi_value"]
            prev_oi = oi_docs[1]["oi_value"]
            value["oi_change_1h_pct"] = (latest_oi / prev_oi - 1) * 100 if prev_oi else 0.0
            value["oi_increasing"] = 1.0 if value.get("oi_change_1h_pct", 0) > 0 else 0.0
        if len(oi_docs) >= 5:
            value["oi_change_4h_pct"] = (oi_docs[0]["oi_value"] / oi_docs[4]["oi_value"] - 1) * 100

        # OI RSI: RSI of OI value changes over 14 periods
        if len(oi_docs) >= 16:
            oi_series = pd.Series([d["oi_value"] for d in reversed(oi_docs)])
            value["oi_rsi_14"] = _rsi(oi_series, length=14)

        # ── Funding features ───────────────��─────────────────────
        if funding_docs:
            value["funding_rate"] = funding_docs[0]["funding_rate"]
            value["funding_neutral"] = 1.0 if -0.0001 <= funding_docs[0]["funding_rate"] <= 0.0005 else 0.0
            if len(funding_docs) >= 4:
                value["funding_delta"] = funding_docs[0]["funding_rate"] - funding_docs[3]["funding_rate"]

        # Funding streak: consecutive same-sign periods (positive = longs paying shorts)
        if len(funding_docs) >= 2:
            rates = [d["funding_rate"] for d in funding_docs]
            value["funding_streak"] = _streak(rates)

        # Funding cumulative: sum of last 8 periods (= 24h of 8h funding, or 8h of 1h funding)
        if len(funding_docs) >= 8:
            value["funding_cumulative_8"] = sum(d["funding_rate"] for d in funding_docs[:8])

        # Funding z-score: how extreme is current rate vs recent history
        if len(funding_docs) >= 30:
            funding_series = pd.Series([d["funding_rate"] for d in reversed(funding_docs)])
            value["funding_zscore_30"] = _zscore(funding_series, window=30)

        # ── L/S ratio features ───────���───────────────────────────
        if ls_docs:
            value["ls_buy_ratio"] = ls_docs[0]["buy_ratio"]
            if len(ls_docs) >= 2:
                value["ls_change"] = ls_docs[0]["buy_ratio"] - ls_docs[1]["buy_ratio"]

        # LS ratio z-score: how extreme is current positioning vs recent history
        if len(ls_docs) >= 30:
            ls_series = pd.Series([d["buy_ratio"] for d in reversed(ls_docs)])
            value["ls_zscore_30"] = _zscore(ls_series, window=30)

        # LS ratio extreme flag: buy_ratio > 0.65 or < 0.35
        if ls_docs:
            br = ls_docs[0]["buy_ratio"]
            value["ls_crowd_long"] = 1.0 if br > 0.65 else 0.0
            value["ls_crowd_short"] = 1.0 if br < 0.35 else 0.0

        # ── Relative strength vs BTC ─────────���───────────────────
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
