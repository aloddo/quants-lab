"""
E2 — Range Fade Controller (Quants Lab / Hummingbot V2)
V2 — Chop Regime Mean Reversion with ADX regime filter.

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: In confirmed ranging regimes (ADX < 25), fade touches of the range
boundary when price rejects and closes back inside. The key insight is that
low ATR alone is NOT sufficient to confirm a range — ADX separates genuine
chop from early-trend compression.

Architecture (single layer, 1h only):
──────────────────────────────────────
Regime Gate (must pass before any trigger check):
  - ADX(14) < 25 — confirmed non-trending

Trigger (all must be true on candle N):
  1. ATR percentile < 0.30 (compression)
  2. Range NOT expanding (5-bar width growth ≤ 20%)
  3. Range boundaries stable (max shift ≤ 15% of width over 10 bars)
  4. Low boundary touch: candle low ≤ range_low_30
  5. Rejection: close > range_low_30
  6. No breakout confirmation:
     a. Volume z-score ≤ 1.5
     b. Candle body ≤ 0.8 × ATR(14)

Exit:
  - TP: range midpoint
  - SL: range_low - 1.0 × ATR (widened from 0.75 to reduce noise stops)
  - Time limit: 12h

Direction: LONG ONLY
"""
from decimal import Decimal
from typing import List

import numpy as np
import pandas as pd
import pandas_ta as ta
from pydantic import Field

from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)
from hummingbot.strategy_v2.executors.position_executor.data_types import (
    PositionExecutorConfig,
    TripleBarrierConfig,
)


class E2RangeFadeConfig(DirectionalTradingControllerConfigBase):
    controller_name: str = "e2_range_fade"

    # Regime gate — ADX must be below this to confirm non-trending
    adx_period: int = Field(default=14)
    adx_threshold: float = Field(default=25.0)

    # ATR compression
    atr_period: int = Field(default=14)
    atr_compression_window: int = Field(default=90)  # days
    atr_compression_threshold: float = Field(default=0.30)

    # Range
    range_period: int = Field(default=30)
    range_expansion_lookback: int = Field(default=5)
    range_expansion_max: float = Field(default=0.20)

    # No-breakout confirmation
    volume_period: int = Field(default=20)
    volume_zscore_ceiling: float = Field(default=1.5)
    body_atr_ratio_max: float = Field(default=0.8)

    # Range stability: max % that range boundaries have shifted
    # over the lookback. Filters staircase trends disguised as ranges.
    range_stability_max: float = Field(default=0.15)
    range_stability_lookback: int = Field(default=10)

    # Directional bias (0 = disabled)
    drift_threshold: float = Field(default=0.0)

    # Range quality: min boundary touches (0 = disabled)
    min_boundary_touches: int = Field(default=0)

    # Exit — SL widened from 0.75 to 1.0 to reduce noise stops
    stop_atr_multiple: float = Field(default=1.0)
    min_reward_atr_multiple: float = Field(default=0.5)

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            lookback = self.atr_compression_window * 24 + self.atr_period + 50
            candles_connector = self.connector_name.replace("_testnet", "").replace("_demo", "")
            self.candles_config = [
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=max(lookback, self.range_period + 10),
                )
            ]


class E2RangeFadeController(DirectionalTradingControllerBase):
    """E2 Range Fade — V2, 1h single-layer, LONG ONLY, ADX regime filter."""

    def __init__(self, config: E2RangeFadeConfig, *args, **kwargs):
        self.config = config
        self.atr_lookback_bars = config.atr_compression_window * 24
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    async def update_processed_data(self):
        c = self.config

        df = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
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

        # Extract TP/SL from last bar for get_executor_config()
        if signal != 0 and len(df) > 0:
            last = df.iloc[-1]
            tp = last.get("tp_price")
            sl = last.get("sl_price")
            if tp is not None and not pd.isna(tp):
                self.processed_data["tp_price"] = float(tp)
            if sl is not None and not pd.isna(sl):
                self.processed_data["sl_price"] = float(sl)

    def _compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        c = self.config
        df = df.copy()

        # ADX — regime gate (must compute before anything else)
        df["adx"] = ta.adx(df["high"], df["low"], df["close"], length=c.adx_period)[f"ADX_{c.adx_period}"]

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

        # Range — shift(1) excludes current bar (aligns with live RangeFeature)
        df["range_high"] = df["high"].shift(1).rolling(c.range_period).max()
        df["range_low"] = df["low"].shift(1).rolling(c.range_period).min()
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

        # ── Structural filter features (V2) ──────────────

        # 1. Directional bias: linear regression slope of close over range_period,
        #    normalized by ATR so it's comparable across pairs and time.
        #    Negative = price drifting down inside range (don't go LONG).
        if c.drift_threshold > 0:
            def _norm_slope(window):
                if len(window) < 2 or np.isnan(window).any():
                    return np.nan
                x = np.arange(len(window))
                slope = np.polyfit(x, window, 1)[0]
                return slope
            df["drift_slope"] = df["close"].rolling(c.range_period).apply(
                _norm_slope, raw=True
            )
            # Normalize by ATR: slope per bar in ATR units
            df["drift_norm"] = df["drift_slope"] / df["atr"]
        else:
            df["drift_norm"] = 0.0

        # 2. Range stability: how much have range_high and range_low shifted
        #    over the stability lookback?  Staircase trends show high drift.
        if c.range_stability_max > 0:
            lb = c.range_stability_lookback
            df["range_high_shift"] = (
                df["range_high"] - df["range_high"].shift(lb)
            ).abs() / df["range_width"]
            df["range_low_shift"] = (
                df["range_low"] - df["range_low"].shift(lb)
            ).abs() / df["range_width"]
            df["range_drift"] = df[["range_high_shift", "range_low_shift"]].max(axis=1)
        else:
            df["range_drift"] = 0.0

        # 3. Range quality: count closes within 0.5 ATR of range_low
        #    over the range_period (for LONG signals).
        if c.min_boundary_touches > 0:
            touch_zone = df["range_low"] + 0.5 * df["atr"]
            near_low = (df["low"] <= touch_zone).astype(float)
            df["low_touches"] = near_low.rolling(c.range_period).sum()
        else:
            df["low_touches"] = 999  # always passes

        # Signal column (populated by _evaluate_signal)
        df["signal"] = 0

        return df

    def _evaluate_signal(self, df: pd.DataFrame) -> int:
        """
        Vectorized signal evaluation across ALL bars (required for backtesting).
        The backtesting engine reads df["signal"] at every timestep via merge_asof.
        Also returns last bar's signal for live mode.
        """
        c = self.config

        # All conditions must be true (LONG ONLY)
        # Regime gate: ADX must confirm non-trending
        ranging = df["adx"].notna() & (df["adx"] < c.adx_threshold)
        compressed = df["atr_pct"].notna() & (df["atr_pct"] < c.atr_compression_threshold)
        not_expanding = df["range_expanding"].isna() | (df["range_expanding"] <= c.range_expansion_max)
        boundary_touch = df["range_low"].notna() & (df["low"] <= df["range_low"])
        rejection = df["close"] > df["range_low"]
        no_vol_spike = df["vol_zscore"].isna() | (df["vol_zscore"] <= c.volume_zscore_ceiling)
        no_momentum = df["body_atr_ratio"].isna() | (df["body_atr_ratio"] <= c.body_atr_ratio_max)
        min_reward_ok = (
            df["reward_distance"].isna() | df["min_reward"].isna()
            | (df["reward_distance"] >= df["min_reward"])
        )

        # ── Structural filters (V2) ──────────────────────────
        # Directional bias: block LONG when price is drifting down
        if c.drift_threshold > 0:
            no_downward_drift = df["drift_norm"].isna() | (df["drift_norm"] > -c.drift_threshold)
        else:
            no_downward_drift = True

        # Range stability: block when range boundaries are shifting
        if c.range_stability_max > 0:
            range_stable = df["range_drift"].isna() | (df["range_drift"] <= c.range_stability_max)
        else:
            range_stable = True

        # Range quality: require N boundary touches
        if c.min_boundary_touches > 0:
            enough_touches = df["low_touches"] >= c.min_boundary_touches
        else:
            enough_touches = True

        signal_mask = (
            ranging & compressed & not_expanding & boundary_touch & rejection
            & no_vol_spike & no_momentum & min_reward_ok
            & no_downward_drift & range_stable & enough_touches
        )
        df.loc[signal_mask, "signal"] = 1

        return int(df["signal"].iloc[-1]) if len(df) > 0 else 0

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        """Build executor config with dynamic TP/SL from the features DataFrame.

        TP = range midpoint, SL = range_low - stop_atr_multiple * ATR.
        Converts absolute price levels to percentages for TripleBarrierConfig.
        """
        tp_abs = self.processed_data.get("tp_price")
        sl_abs = self.processed_data.get("sl_price")

        if tp_abs is not None and sl_abs is not None and float(price) > 0:
            tp_pct = abs(Decimal(str(tp_abs)) - price) / price
            sl_pct = abs(Decimal(str(sl_abs)) - price) / price

            # Safety: clamp to reasonable bounds (0.1% – 10%)
            tp_pct = max(Decimal("0.001"), min(tp_pct, Decimal("0.10")))
            sl_pct = max(Decimal("0.001"), min(sl_pct, Decimal("0.10")))
        else:
            # Fallback to config defaults
            tp_pct = self.config.take_profit
            sl_pct = self.config.stop_loss

        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=sl_pct,
                take_profit=tp_pct,
                time_limit=self.config.time_limit,
                open_order_type=OrderType.MARKET,
                take_profit_order_type=OrderType.MARKET,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=self.config.leverage,
        )
