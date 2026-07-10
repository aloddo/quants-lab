"""
Microstructure features computed from existing 1m candle data.

Phase 0 of Exotic Signal Factory v2 — extract maximum value from data we already have.

Implements:
- VPIN (Volume-Synchronized Probability of Informed Trading)
- Kyle's Lambda (Price Impact)
- Amihud Illiquidity Ratio
- Realized Volatility Signature (multi-scale RV + ratio)
- Volume Profile features (POC, Value Area, skew)
"""
from datetime import datetime, timezone
from typing import Optional

import numpy as np
import pandas as pd

from core.data_structures.candles import Candles
from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature
from app.features.decorators import screening_feature


class MicrostructureConfig(FeatureConfig):
    name: str = "microstructure"
    # VPIN params
    vpin_volume_bucket_size: int = 50  # number of 1m bars per volume bucket
    vpin_n_buckets: int = 50  # rolling window of buckets for VPIN CDF
    # Kyle's Lambda params
    lambda_window: int = 60  # 1h of 1m bars
    # Amihud params
    amihud_window: int = 60  # 1h rolling
    # RV params
    rv_windows: list = [1, 5, 15, 60, 240]  # minutes: 1m, 5m, 15m, 1h, 4h
    # Volume Profile params
    vp_window: int = 240  # 4h of 1m bars for volume profile
    vp_value_area_pct: float = 0.70  # 70% value area


@screening_feature
class MicrostructureFeature(FeatureBase[MicrostructureConfig]):

    def __init__(self, feature_config: Optional[MicrostructureConfig] = None):
        super().__init__(feature_config or MicrostructureConfig())

    def calculate(self, data: pd.DataFrame, skip_volume_profile: bool = False) -> pd.DataFrame:
        """Compute all microstructure features on 1m candle DataFrame.

        Args:
            skip_volume_profile: If True, skip the O(N) volume profile computation.
                Useful for large datasets where vp_poc_distance/vp_vol_skew aren't needed.
        """
        df = data.copy()
        df = self._compute_vpin(df)
        df = self._compute_kyles_lambda(df)
        df = self._compute_amihud(df)
        df = self._compute_rv_signature(df)
        if not skip_volume_profile:
            df = self._compute_volume_profile(df)
        return df

    def _classify_trades(self, df: pd.DataFrame) -> pd.Series:
        """Classify each bar as buy or sell initiated using tick rule.

        Tick rule: if close > prev close → buy, if close < prev close → sell,
        if equal → same as previous classification.
        """
        price_diff = df["close"].diff()
        # 1 = buy, -1 = sell, 0 = unchanged
        classification = np.sign(price_diff)
        # Forward-fill zeros (unchanged prices keep prior classification)
        classification = classification.replace(0, np.nan).ffill().fillna(1)
        return classification

    def _compute_vpin(self, df: pd.DataFrame) -> pd.DataFrame:
        """VPIN: Volume-Synchronized Probability of Informed Trading.

        1. Classify each bar as buy/sell (tick rule)
        2. Bucket bars by cumulative volume (not time)
        3. Compute order imbalance per bucket
        4. VPIN = rolling mean of absolute imbalance
        """
        bucket_size = self.config.vpin_volume_bucket_size
        n_buckets = self.config.vpin_n_buckets

        classification = self._classify_trades(df)
        buy_volume = df["volume"].where(classification > 0, 0)
        sell_volume = df["volume"].where(classification < 0, 0)

        # Create volume buckets
        cumvol = df["volume"].cumsum()
        avg_bar_vol = df["volume"].mean()
        if avg_bar_vol <= 0:
            df["vpin"] = np.nan
            return df

        bucket_vol = avg_bar_vol * bucket_size
        bucket_id = (cumvol / bucket_vol).astype(int)

        # Group by bucket: compute buy/sell volume per bucket
        bucket_buy = buy_volume.groupby(bucket_id).sum()
        bucket_sell = sell_volume.groupby(bucket_id).sum()
        bucket_total = bucket_buy + bucket_sell
        bucket_total = bucket_total.replace(0, np.nan)

        # Order imbalance per bucket
        bucket_imbalance = (bucket_buy - bucket_sell).abs() / bucket_total

        # VPIN = rolling mean of imbalance over last N buckets
        vpin_series = bucket_imbalance.rolling(n_buckets, min_periods=n_buckets // 2).mean()

        # Map bucket-level VPIN back to bar-level by bucket_id
        vpin_by_bucket = vpin_series.to_dict()
        df["vpin"] = bucket_id.map(vpin_by_bucket)

        return df

    def _compute_kyles_lambda(self, df: pd.DataFrame) -> pd.DataFrame:
        """Kyle's Lambda: regression of |price change| on volume.

        High lambda = low liquidity = large moves for small volume.
        Computed as rolling OLS slope.
        """
        window = self.config.lambda_window
        abs_return = df["close"].pct_change().abs()
        dollar_volume = df["volume"] * df["close"]

        # Rolling covariance / variance = regression slope
        cov = abs_return.rolling(window, min_periods=window // 2).cov(dollar_volume)
        var = dollar_volume.rolling(window, min_periods=window // 2).var()
        df["kyles_lambda"] = cov / var.replace(0, np.nan)

        return df

    def _compute_amihud(self, df: pd.DataFrame) -> pd.DataFrame:
        """Amihud Illiquidity: |return| / dollar_volume, rolling average.

        High Amihud = illiquid = volatile regime.
        """
        window = self.config.amihud_window
        abs_return = df["close"].pct_change().abs()
        dollar_volume = df["volume"] * df["close"]
        illiq = abs_return / dollar_volume.replace(0, np.nan)
        df["amihud_illiq"] = illiq.rolling(window, min_periods=window // 2).mean()

        return df

    def _compute_rv_signature(self, df: pd.DataFrame) -> pd.DataFrame:
        """Realized Volatility at multiple timescales + noise ratio.

        RV(1m)/RV(1h) reveals microstructure noise vs true volatility.
        Declining ratio = directional move forming.
        """
        log_returns = np.log(df["close"] / df["close"].shift(1))

        for w in self.config.rv_windows:
            if w == 1:
                # 1m returns: just squared returns summed over 60-bar window
                rv = (log_returns ** 2).rolling(60, min_periods=30).sum()
            else:
                # Subsample returns at w-minute intervals, then sum squared
                subsampled = log_returns.rolling(w).sum()
                rv = (subsampled ** 2).rolling(60, min_periods=30).sum()
            df[f"rv_{w}m"] = rv

        # Noise ratio: RV(1m) / RV(1h) — high = noisy, low = directional
        if "rv_1m" in df.columns and "rv_60m" in df.columns:
            df["rv_noise_ratio"] = df["rv_1m"] / df["rv_60m"].replace(0, np.nan)
            # Rate of change of noise ratio (momentum of regime shift)
            df["rv_noise_ratio_roc"] = df["rv_noise_ratio"].pct_change(periods=60)

        return df

    def _compute_volume_profile(self, df: pd.DataFrame) -> pd.DataFrame:
        """Volume Profile: POC, Value Area High/Low, volume skew.

        POC = price level with most volume in window.
        Value Area = price range containing 70% of volume.
        Skew = volume concentration above vs below current price.
        """
        window = self.config.vp_window
        va_pct = self.config.vp_value_area_pct

        poc_list = []
        va_high_list = []
        va_low_list = []
        vol_skew_list = []

        for i in range(len(df)):
            if i < window:
                poc_list.append(np.nan)
                va_high_list.append(np.nan)
                va_low_list.append(np.nan)
                vol_skew_list.append(np.nan)
                continue

            window_df = df.iloc[i - window:i]
            prices = window_df["close"].values
            volumes = window_df["volume"].values
            current_price = df.iloc[i]["close"]

            if len(prices) == 0 or np.sum(volumes) == 0:
                poc_list.append(np.nan)
                va_high_list.append(np.nan)
                va_low_list.append(np.nan)
                vol_skew_list.append(np.nan)
                continue

            # Create price bins (20 bins across the window range)
            n_bins = 20
            price_min, price_max = np.min(prices), np.max(prices)
            if price_max == price_min:
                poc_list.append(price_min)
                va_high_list.append(price_max)
                va_low_list.append(price_min)
                vol_skew_list.append(0.0)
                continue

            bin_edges = np.linspace(price_min, price_max, n_bins + 1)
            bin_indices = np.digitize(prices, bin_edges) - 1
            bin_indices = np.clip(bin_indices, 0, n_bins - 1)

            # Volume at each price bin
            vol_at_price = np.zeros(n_bins)
            for bi, v in zip(bin_indices, volumes):
                vol_at_price[bi] += v

            # POC: bin center with max volume
            poc_bin = np.argmax(vol_at_price)
            poc = (bin_edges[poc_bin] + bin_edges[poc_bin + 1]) / 2
            poc_list.append(poc)

            # Value Area: expand from POC until 70% of volume
            total_vol = np.sum(vol_at_price)
            va_vol = vol_at_price[poc_bin]
            lo, hi = poc_bin, poc_bin
            while va_vol / total_vol < va_pct and (lo > 0 or hi < n_bins - 1):
                expand_lo = vol_at_price[lo - 1] if lo > 0 else 0
                expand_hi = vol_at_price[hi + 1] if hi < n_bins - 1 else 0
                if expand_lo >= expand_hi and lo > 0:
                    lo -= 1
                    va_vol += vol_at_price[lo]
                elif hi < n_bins - 1:
                    hi += 1
                    va_vol += vol_at_price[hi]
                else:
                    break
            va_low_list.append(bin_edges[lo])
            va_high_list.append(bin_edges[hi + 1])

            # Volume skew: volume above vs below current price
            above_vol = np.sum(volumes[prices > current_price])
            below_vol = np.sum(volumes[prices <= current_price])
            total = above_vol + below_vol
            vol_skew_list.append((above_vol - below_vol) / total if total > 0 else 0.0)

        df["vp_poc"] = poc_list
        df["vp_va_high"] = va_high_list
        df["vp_va_low"] = va_low_list
        df["vp_vol_skew"] = vol_skew_list

        # Distance from POC (normalized)
        df["vp_poc_distance"] = (df["close"] - df["vp_poc"]) / df["close"]

        return df

    def create_feature(self, candles: Candles) -> Feature:
        """Create feature from the last row of computed microstructure data."""
        df = self.calculate(candles.data)
        last = df.iloc[-1]

        value = {}
        for col in ["vpin", "kyles_lambda", "amihud_illiq",
                     "rv_1m", "rv_5m", "rv_15m", "rv_60m", "rv_240m",
                     "rv_noise_ratio", "rv_noise_ratio_roc",
                     "vp_poc", "vp_va_high", "vp_va_low",
                     "vp_vol_skew", "vp_poc_distance"]:
            if col in last.index and pd.notna(last[col]):
                value[col] = float(last[col])

        return Feature(
            timestamp=datetime.now(timezone.utc),
            feature_name=self.config.name,
            trading_pair=candles.trading_pair,
            connector_name=candles.connector_name,
            value=value if value else 0.0,
        )
