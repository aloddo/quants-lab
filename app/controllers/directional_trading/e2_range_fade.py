"""
E2 — Range Fade Controller (Quants Lab / Hummingbot V2)
V1 P3 — Chop Regime Mean Reversion

Architecture (single layer, 1h only):
──────────────────────────────────────
Trigger (all must be true on candle N):
  1. ATR percentile < 0.30 (compression)
  2. Range NOT expanding (5-bar width growth ≤ 20%)
  3. Low boundary touch: candle low ≤ range_low_20
  4. Rejection: close > range_low_20
  5. No breakout confirmation:
     a. Volume z-score ≤ 1.5
     b. OI change 1h ≤ 2.0% (if available)
     c. Candle body ≤ 0.8 × ATR(14)

Exit:
  - TP: range midpoint
  - SL: range_low - 0.75 × ATR
  - Time limit: 12h

Direction: LONG ONLY (P3 locked)
"""
from typing import List

import numpy as np
import pandas as pd
import pandas_ta as ta
from pydantic import Field

from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)


class E2RangeFadeConfig(DirectionalTradingControllerConfigBase):
    controller_name: str = "e2_range_fade"

    # ATR compression
    atr_period: int = Field(default=14)
    atr_compression_window: int = Field(default=90)  # days
    atr_compression_threshold: float = Field(default=0.30)

    # Range
    range_period: int = Field(default=20)
    range_expansion_lookback: int = Field(default=5)
    range_expansion_max: float = Field(default=0.20)

    # No-breakout confirmation
    volume_period: int = Field(default=20)
    volume_zscore_ceiling: float = Field(default=1.5)
    body_atr_ratio_max: float = Field(default=0.8)

    # Exit
    stop_atr_multiple: float = Field(default=0.75)
    min_reward_atr_multiple: float = Field(default=0.5)

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            lookback = self.atr_compression_window * 24 + self.atr_period + 50
            self.candles_config = [
                CandlesConfig(
                    connector=self.connector_name,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=max(lookback, self.range_period + 10),
                )
            ]


class E2RangeFadeController(DirectionalTradingControllerBase):
    """E2 Range Fade — V1, 1h single-layer, LONG ONLY."""

    def __init__(self, config: E2RangeFadeConfig, *args, **kwargs):
        self.config = config
        self.atr_lookback_bars = config.atr_compression_window * 24
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    async def update_processed_data(self):
        c = self.config

        df = self.market_data_provider.get_candles_df(
            connector_name=c.connector_name,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=self.atr_lookback_bars + c.atr_period + 50,
        )

        if df is None or len(df) < c.range_period + c.atr_period + 10:
            self.processed_data["signal"] = 0
            self.processed_data["features"] = pd.DataFrame()
            return

        df = self._compute_features(df)
        signal = self._evaluate_signal(df)

        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

    def _compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        c = self.config
        df = df.copy()

        # ATR
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        # ATR percentile
        window = min(self.atr_lookback_bars, len(df) - 1)
        if window >= 100:
            df["atr_pct"] = df["atr"].rolling(window).apply(
                lambda s: (s.iloc[:-1] < s.iloc[-1]).sum() / (len(s) - 1)
                if len(s) > 1 else np.nan,
                raw=False,
            )
        else:
            df["atr_pct"] = np.nan

        # Range
        df["range_high"] = df["high"].rolling(c.range_period).max()
        df["range_low"] = df["low"].rolling(c.range_period).min()
        df["range_width"] = df["range_high"] - df["range_low"]
        df["range_expanding"] = df["range_width"].pct_change(c.range_expansion_lookback)
        df["range_midpoint"] = (df["range_high"] + df["range_low"]) / 2

        # Volume z-score
        vol_mean = df["volume"].rolling(c.volume_period).mean()
        vol_std = df["volume"].rolling(c.volume_period).std()
        df["vol_zscore"] = (df["volume"] - vol_mean) / vol_std.replace(0, np.nan)

        # Candle body
        df["body"] = (df["close"] - df["open"]).abs()
        df["body_atr_ratio"] = df["body"] / df["atr"]

        # TP / SL levels
        df["tp_price"] = df["range_midpoint"]
        df["sl_price"] = df["range_low"] - c.stop_atr_multiple * df["atr"]

        # Minimum reward check
        df["reward_distance"] = (df["range_midpoint"] - df["close"]).abs()
        df["min_reward"] = c.min_reward_atr_multiple * df["atr"]

        # Signal column (populated by _evaluate_signal)
        df["signal"] = 0

        return df

    def _evaluate_signal(self, df: pd.DataFrame) -> int:
        """Evaluate last bar for E2 trigger. Returns 1 (long) or 0 (no signal)."""
        c = self.config
        last = df.iloc[-1]

        # 1. ATR compression
        if pd.isna(last["atr_pct"]) or last["atr_pct"] >= c.atr_compression_threshold:
            return 0

        # 2. Range NOT expanding
        if pd.notna(last["range_expanding"]) and last["range_expanding"] > c.range_expansion_max:
            return 0

        # 3. Low boundary touch (LONG only)
        if pd.isna(last["range_low"]) or last["low"] > last["range_low"]:
            return 0

        # 4. Rejection: close back inside range
        if last["close"] <= last["range_low"]:
            return 0

        # 5a. No volume spike
        if pd.notna(last["vol_zscore"]) and last["vol_zscore"] > c.volume_zscore_ceiling:
            return 0

        # 5c. No momentum candle (body ≤ 0.8 ATR)
        if pd.notna(last["body_atr_ratio"]) and last["body_atr_ratio"] > c.body_atr_ratio_max:
            return 0

        # Minimum reward check
        if pd.notna(last["reward_distance"]) and pd.notna(last["min_reward"]):
            if last["reward_distance"] < last["min_reward"]:
                return 0

        # All conditions met — LONG signal
        df.loc[df.index[-1], "signal"] = 1
        return 1
