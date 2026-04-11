"""
S7 — Hurst Regime Adaptive (HB V2 Controller)
V1 — April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: The Hurst exponent (via R/S analysis) identifies market regime:
  - H < 0.45 → mean-reverting: fade extremes (RSI oversold/overbought)
  - H > 0.55 → trending: follow momentum (EMA crossover + close confirmation)
  - 0.45 ≤ H ≤ 0.55 → random walk, no edge, skip

By adapting strategy to the detected regime, we avoid the #1 failure mode of
both E1 (compression breakout in trending markets) and E2 (range fade in trends).

Signal (1h):
  Mean-revert regime (H < 0.45):
    - RSI(14) < 30 → LONG (oversold bounce)
    - RSI(14) > 70 → SHORT (overbought fade)
  Trending regime (H > 0.55):
    - EMA(20) > EMA(50) AND close > EMA(20) → LONG
    - EMA(20) < EMA(50) AND close < EMA(20) → SHORT

Exits (regime-dependent):
  Mean-revert: TP 1.5×ATR, SL 1.5×ATR, trailing act=0.8, 6h limit
  Trend-follow: TP 3.0×ATR, SL 2.0×ATR, trailing act=1.5, 12h limit
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
    TrailingStop,
    TripleBarrierConfig,
)


class S7HurstAdaptiveConfig(DirectionalTradingControllerConfigBase):
    """V1 config — Hurst Regime Adaptive."""
    controller_name: str = "s7_hurst_adaptive"

    # Hurst R/S computation
    hurst_window: int = Field(default=200)
    hurst_min_subwindow: int = Field(default=10)

    # Regime thresholds
    mean_revert_threshold: float = Field(default=0.45)
    trending_threshold: float = Field(default=0.55)

    # Mean-revert signals (RSI)
    rsi_period: int = Field(default=14)
    rsi_oversold: float = Field(default=30.0)
    rsi_overbought: float = Field(default=70.0)

    # Trend-follow signals (EMA crossover)
    ema_fast: int = Field(default=20)
    ema_slow: int = Field(default=50)

    # ATR for exits
    atr_period: int = Field(default=14)

    # Mean-revert exits (ATR multipliers)
    mr_tp_atr: float = Field(default=1.5)
    mr_sl_atr: float = Field(default=1.5)
    mr_trailing_act_atr: float = Field(default=0.8)
    mr_trailing_delta_atr: float = Field(default=0.4)
    mr_time_limit: int = Field(default=21600)   # 6h

    # Trend-follow exits (ATR multipliers)
    tf_tp_atr: float = Field(default=3.0)
    tf_sl_atr: float = Field(default=2.0)
    tf_trailing_act_atr: float = Field(default=1.5)
    tf_trailing_delta_atr: float = Field(default=0.8)
    tf_time_limit: int = Field(default=43200)   # 12h

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            max_records = self.hurst_window + max(self.ema_slow, self.rsi_period, self.atr_period) + 50
            candles_connector = self.connector_name.replace("_testnet", "").replace("_demo", "")
            self.candles_config = [
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=max_records,
                ),
            ]


class S7HurstAdaptiveController(DirectionalTradingControllerBase):
    """S7 Hurst Regime Adaptive — mean-revert or trend-follow based on regime."""

    def __init__(self, config: S7HurstAdaptiveConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    @staticmethod
    def _hurst_rs(returns: np.ndarray, min_k: int = 10) -> float:
        """Compute Hurst exponent via Rescaled Range (R/S) analysis. Pure numpy."""
        n = len(returns)
        if n < min_k * 4:
            return 0.5  # insufficient data, assume random walk

        max_k = min(n // 2, 100)
        if max_k <= min_k:
            return 0.5

        sizes = np.unique(np.logspace(
            np.log10(min_k), np.log10(max_k), num=20
        ).astype(int))
        sizes = sizes[(sizes >= min_k) & (sizes <= max_k)]

        if len(sizes) < 3:
            return 0.5

        rs_means = []
        for size in sizes:
            n_windows = n // size
            if n_windows < 1:
                continue
            rs_list = []
            for i in range(n_windows):
                window = returns[i * size:(i + 1) * size]
                mean = window.mean()
                cumdev = np.cumsum(window - mean)
                R = cumdev.max() - cumdev.min()
                S = window.std(ddof=1)
                if S > 1e-12:
                    rs_list.append(R / S)
            if rs_list:
                rs_means.append((size, np.mean(rs_list)))

        if len(rs_means) < 3:
            return 0.5

        log_n = np.log([x[0] for x in rs_means])
        log_rs = np.log([x[1] for x in rs_means])
        slope = np.polyfit(log_n, log_rs, 1)[0]
        return float(np.clip(slope, 0.0, 1.0))

    async def update_processed_data(self):
        c = self.config
        max_records = c.hurst_window + max(c.ema_slow, c.rsi_period, c.atr_period) + 50

        df = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=max_records,
        )

        min_viable = c.hurst_window + 10
        if df is None or len(df) < min_viable:
            self.processed_data["signal"] = 0
            self.processed_data["signal_reason"] = "insufficient_data"
            return

        df = self._compute_features(df)
        signal, regime = self._evaluate_signal(df)

        self.processed_data["signal"] = signal
        self.processed_data["regime"] = regime
        self.processed_data["features"] = df

        if signal != 0 and len(df) > 0:
            last = df.iloc[-1]
            atr = float(last.get("atr", 0) or 0)
            price = float(last["close"])
            if atr > 0 and price > 0:
                self.processed_data["entry_atr"] = atr
                self.processed_data["entry_price"] = price

    def _compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        c = self.config
        df = df.copy()

        # ATR
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        # RSI
        df["rsi"] = ta.rsi(df["close"], length=c.rsi_period)

        # EMAs
        df["ema_fast"] = ta.ema(df["close"], length=c.ema_fast)
        df["ema_slow"] = ta.ema(df["close"], length=c.ema_slow)

        # Hurst exponent — computed on log returns of the last hurst_window bars
        log_returns = np.log(df["close"] / df["close"].shift(1)).dropna().values
        if len(log_returns) >= c.hurst_window:
            hurst = self._hurst_rs(
                log_returns[-c.hurst_window:],
                min_k=c.hurst_min_subwindow,
            )
        else:
            hurst = 0.5

        df["hurst"] = hurst  # scalar broadcast to all rows (only latest matters)

        # Regime classification
        if hurst < c.mean_revert_threshold:
            regime = "mean_revert"
        elif hurst > c.trending_threshold:
            regime = "trending"
        else:
            regime = "random_walk"
        df["regime"] = regime

        # Signal computation
        df["signal"] = 0

        if regime == "mean_revert":
            # Fade extremes: RSI oversold → LONG, overbought → SHORT
            df.loc[df["rsi"] < c.rsi_oversold, "signal"] = 1
            df.loc[df["rsi"] > c.rsi_overbought, "signal"] = -1
        elif regime == "trending":
            # Follow trend: EMA crossover + close confirmation
            ema_bull = (df["ema_fast"] > df["ema_slow"]) & (df["close"] > df["ema_fast"])
            ema_bear = (df["ema_fast"] < df["ema_slow"]) & (df["close"] < df["ema_fast"])
            df.loc[ema_bull, "signal"] = 1
            df.loc[ema_bear, "signal"] = -1
        # random_walk: signal stays 0

        return df

    def _evaluate_signal(self, df: pd.DataFrame) -> tuple:
        """Return (signal, regime) from last bar."""
        if len(df) == 0 or "signal" not in df.columns:
            return 0, "random_walk"
        last = df.iloc[-1]
        return int(last.get("signal", 0)), str(last.get("regime", "random_walk"))

    def _get_regime_exits(self, regime: str, atr: float, price: float) -> dict:
        """Return regime-specific exit parameters."""
        c = self.config
        if regime == "mean_revert":
            tp_mult, sl_mult = c.mr_tp_atr, c.mr_sl_atr
            trail_act, trail_delta = c.mr_trailing_act_atr, c.mr_trailing_delta_atr
            time_limit = c.mr_time_limit
        else:  # trending or fallback
            tp_mult, sl_mult = c.tf_tp_atr, c.tf_sl_atr
            trail_act, trail_delta = c.tf_trailing_act_atr, c.tf_trailing_delta_atr
            time_limit = c.tf_time_limit

        tp_pct = Decimal(str(tp_mult * atr / price))
        sl_pct = Decimal(str(sl_mult * atr / price))
        trail_act_pct = Decimal(str(trail_act * atr / price))
        trail_delta_pct = Decimal(str(trail_delta * atr / price))

        tp_pct = max(Decimal("0.001"), min(tp_pct, Decimal("0.15")))
        sl_pct = max(Decimal("0.001"), min(sl_pct, Decimal("0.15")))
        trail_act_pct = max(Decimal("0.001"), min(trail_act_pct, Decimal("0.05")))
        trail_delta_pct = max(Decimal("0.001"), min(trail_delta_pct, Decimal("0.03")))

        return {
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "trailing": TrailingStop(
                activation_price=trail_act_pct,
                trailing_delta=trail_delta_pct,
            ),
            "time_limit": time_limit,
        }

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        regime = self.processed_data.get("regime", "trending")
        atr = self.processed_data.get("entry_atr", 0)
        entry_price = float(price) if float(price) > 0 else self.processed_data.get("entry_price", 0)

        if atr > 0 and entry_price > 0:
            exits = self._get_regime_exits(regime, atr, entry_price)
        else:
            exits = {
                "tp_pct": self.config.take_profit or Decimal("0.02"),
                "sl_pct": self.config.stop_loss or Decimal("0.015"),
                "trailing": None,
                "time_limit": 21600,
            }

        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=exits["sl_pct"],
                take_profit=exits["tp_pct"],
                time_limit=exits["time_limit"],
                trailing_stop=exits.get("trailing"),
                open_order_type=OrderType.MARKET,
                take_profit_order_type=OrderType.MARKET,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=self.config.leverage,
        )
