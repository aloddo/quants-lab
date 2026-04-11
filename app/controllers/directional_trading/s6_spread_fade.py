"""
S6 — Cross-Pair Spread Fade (HB V2 Controller)
V1 — April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: Correlated crypto pairs (e.g. ETH/SOL) mean-revert relative to each other.
When the log-price spread deviates > 2σ from its rolling mean, fade the overvalued
pair. This is a single-leg directional approximation of stat arb: we trade one pair
based on its spread signal against a reference pair.

Signal (1h):
  - Compute log spread: log(target_close) - beta × log(reference_close)
  - Beta = rolling OLS slope over 720 bars (30 days)
  - Z-score = (spread - rolling_mean) / rolling_std over 168 bars (7 days)
  - Z > +2.0 → SHORT target (overvalued vs reference)
  - Z < -2.0 → LONG target (undervalued vs reference)
  - Gate: rolling correlation > 0.7 (skip if decorrelated)

Exits:
  - TP 2.0×ATR, SL 1.5×ATR, trailing act=1.0, 12h limit
  - OR: z-score crosses back to ±0.5 (spread normalizes) — time limit handles this

Pair groups:
  - ETH-USDT → BTC-USDT (market leaders)
  - SOL-USDT → ETH-USDT (L1 alts)
  - LINK-USDT → UNI-USDT (DeFi sector)
  - DOGE-USDT → 1000PEPE-USDT (meme sector)
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


class S6SpreadFadeConfig(DirectionalTradingControllerConfigBase):
    """V1 config — Cross-Pair Spread Fade."""
    controller_name: str = "s6_spread_fade"

    # Reference pair (the one we DON'T trade, just observe)
    reference_pair: str = Field(default="BTC-USDT")

    # Spread computation
    beta_window: int = Field(default=720)      # 30 days of 1h bars for OLS beta
    zscore_window: int = Field(default=168)    # 7 days for spread mean/std
    zscore_entry: float = Field(default=2.0)   # |z| > 2.0 to enter
    zscore_exit: float = Field(default=0.5)    # used for thesis, time limit handles

    # Correlation gate
    min_correlation: float = Field(default=0.7)
    correlation_window: int = Field(default=720)  # 30 days

    # ATR for exits
    atr_period: int = Field(default=14)

    # Exit params (ATR multipliers)
    tp_atr: float = Field(default=2.0)
    sl_atr: float = Field(default=1.5)
    trailing_act_atr: float = Field(default=1.0)
    trailing_delta_atr: float = Field(default=0.5)
    time_limit_seconds: int = Field(default=43200)  # 12h

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            max_records = max(self.beta_window, self.correlation_window) + self.atr_period + 50
            candles_connector = self.connector_name.replace("_testnet", "").replace("_demo", "")
            self.candles_config = [
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=max_records,
                ),
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.reference_pair,
                    interval="1h",
                    max_records=max_records,
                ),
            ]


class S6SpreadFadeController(DirectionalTradingControllerBase):
    """S6 Spread Fade — directional trades on spread z-score."""

    def __init__(self, config: S6SpreadFadeConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    async def update_processed_data(self):
        c = self.config
        max_records = max(c.beta_window, c.correlation_window) + c.atr_period + 50

        # Target pair candles
        df_target = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=max_records,
        )

        # Reference pair candles
        df_ref = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.reference_pair,
            interval="1h",
            max_records=max_records,
        )

        min_viable = max(c.beta_window + 10, c.correlation_window + 10, 200)
        if df_target is None or df_ref is None or len(df_target) < min_viable or len(df_ref) < min_viable:
            self.processed_data["signal"] = 0
            self.processed_data["signal_reason"] = "insufficient_data"
            return

        df = self._compute_features(df_target, df_ref)
        if df is None:
            self.processed_data["signal"] = 0
            self.processed_data["signal_reason"] = "alignment_failed"
            return

        signal = self._evaluate_signal(df)
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

        if signal != 0 and len(df) > 0:
            last = df.iloc[-1]
            atr = float(last.get("atr", 0) or 0)
            price = float(last.get("close_target", 0))
            if atr > 0 and price > 0:
                self.processed_data["entry_atr"] = atr
                self.processed_data["entry_price"] = price

    def _compute_features(self, df_target: pd.DataFrame, df_ref: pd.DataFrame) -> pd.DataFrame:
        c = self.config

        # Align on timestamp
        df_t = df_target[["timestamp", "open", "high", "low", "close", "volume"]].copy()
        df_r = df_ref[["timestamp", "close"]].copy()
        df_t = df_t.rename(columns={"close": "close_target", "open": "open_target",
                                     "high": "high_target", "low": "low_target"})
        df_r = df_r.rename(columns={"close": "close_ref"})

        df = pd.merge(df_t, df_r, on="timestamp", how="inner")
        if len(df) < 200:
            return None

        df = df.sort_values("timestamp").reset_index(drop=True)

        # ATR on target pair
        df["atr"] = ta.atr(df["high_target"], df["low_target"], df["close_target"], length=c.atr_period)

        # Log prices
        df["log_target"] = np.log(df["close_target"])
        df["log_ref"] = np.log(df["close_ref"])

        # Rolling beta via polyfit (OLS slope of log_target on log_ref)
        df["beta"] = float("nan")
        df["correlation"] = float("nan")
        df["spread"] = float("nan")

        for i in range(c.beta_window, len(df)):
            window_t = df["log_target"].iloc[i - c.beta_window:i].values
            window_r = df["log_ref"].iloc[i - c.beta_window:i].values

            # Beta (OLS slope)
            if np.std(window_r) > 1e-12:
                beta = np.polyfit(window_r, window_t, 1)[0]
            else:
                beta = 1.0
            df.iloc[i, df.columns.get_loc("beta")] = beta

            # Spread
            df.iloc[i, df.columns.get_loc("spread")] = (
                df.iloc[i]["log_target"] - beta * df.iloc[i]["log_ref"]
            )

        # Rolling correlation
        for i in range(c.correlation_window, len(df)):
            ret_t = np.diff(df["log_target"].iloc[i - c.correlation_window:i].values)
            ret_r = np.diff(df["log_ref"].iloc[i - c.correlation_window:i].values)
            if np.std(ret_t) > 1e-12 and np.std(ret_r) > 1e-12:
                corr = np.corrcoef(ret_t, ret_r)[0, 1]
            else:
                corr = 0.0
            df.iloc[i, df.columns.get_loc("correlation")] = corr

        # Z-score of spread
        spread_mean = df["spread"].rolling(c.zscore_window).mean()
        spread_std = df["spread"].rolling(c.zscore_window).std()
        df["zscore"] = (df["spread"] - spread_mean) / spread_std.replace(0, float("nan"))

        # Signal: z > entry → SHORT (target overvalued), z < -entry → LONG
        df["signal"] = 0
        corr_ok = df["correlation"] > c.min_correlation
        df.loc[(df["zscore"] > c.zscore_entry) & corr_ok, "signal"] = -1
        df.loc[(df["zscore"] < -c.zscore_entry) & corr_ok, "signal"] = 1

        return df

    def _evaluate_signal(self, df: pd.DataFrame) -> int:
        if len(df) == 0 or "signal" not in df.columns:
            return 0
        return int(df["signal"].iloc[-1])

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        c = self.config
        atr = self.processed_data.get("entry_atr", 0)
        entry_price = float(price) if float(price) > 0 else self.processed_data.get("entry_price", 0)

        if atr > 0 and entry_price > 0:
            tp_pct = Decimal(str(c.tp_atr * atr / entry_price))
            sl_pct = Decimal(str(c.sl_atr * atr / entry_price))
            trail_act_pct = Decimal(str(c.trailing_act_atr * atr / entry_price))
            trail_delta_pct = Decimal(str(c.trailing_delta_atr * atr / entry_price))

            tp_pct = max(Decimal("0.001"), min(tp_pct, Decimal("0.15")))
            sl_pct = max(Decimal("0.001"), min(sl_pct, Decimal("0.15")))
            trail_act_pct = max(Decimal("0.001"), min(trail_act_pct, Decimal("0.05")))
            trail_delta_pct = max(Decimal("0.001"), min(trail_delta_pct, Decimal("0.03")))

            trailing = TrailingStop(
                activation_price=trail_act_pct,
                trailing_delta=trail_delta_pct,
            )
        else:
            tp_pct = c.take_profit or Decimal("0.02")
            sl_pct = c.stop_loss or Decimal("0.015")
            trailing = None

        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=c.connector_name,
            trading_pair=c.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=sl_pct,
                take_profit=tp_pct,
                time_limit=c.time_limit_seconds,
                trailing_stop=trailing,
                open_order_type=OrderType.MARKET,
                take_profit_order_type=OrderType.MARKET,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=c.leverage,
        )
