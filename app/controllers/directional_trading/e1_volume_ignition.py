"""
E1 — Volume Ignition Continuation (HB V2 Controller)
V1 — April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: When a single candle has extreme volume (> 3× 20-period average) AND
the candle body exceeds 2× ATR(14), institutional-size flow is entering.
This flow takes 1-4 hours to fully execute, creating predictable continuation.

Signal (1h):
  - Volume > 3× 20-bar average
  - Candle body (|close - open|) > 2× ATR(14)
  - Direction: bullish candle → LONG, bearish → SHORT

Context (1h):
  - Not in extreme drawdown (optional: 1h close not > 3× ATR below EMA50)

Exits:
  - TP: 1.0× ATR(14) from entry
  - SL: 0.5× ATR(14) from entry (R:R = 2:1)
  - Trailing: activate at 0.5×ATR, delta 0.3×ATR
  - Time limit: 4 hours (edge decays after 4h per backtest)

Backtest stats (370 days, 20 pairs):
  - 1316 signals (3.6/day), Sharpe 6.22 at 1h, 3.61 at 4h
  - Edge strongest at 1-4h, decays by 12-24h
"""
from decimal import Decimal
from typing import List, Dict, Any

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


class E1VolumeIgnitionConfig(DirectionalTradingControllerConfigBase):
    """V1 config — Volume Ignition Continuation."""
    controller_name: str = "e1_volume_ignition"

    # Volume threshold: current volume / 20-bar MA
    volume_period: int = Field(default=20)
    volume_threshold: float = Field(default=3.0)

    # Body threshold: |close - open| / ATR(14)
    atr_period: int = Field(default=14)
    body_atr_threshold: float = Field(default=2.0)

    # Exit params (ATR-based)
    tp_atr_mult: float = Field(default=99.0)      # disabled — time + trailing only
    sl_atr_mult: float = Field(default=99.0)       # disabled — time + trailing only
    trailing_activation_atr: float = Field(default=1.0)  # activate trailing at 1.0×ATR
    trailing_delta_atr: float = Field(default=0.5)       # trail by 0.5×ATR

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            candles_connector = self.connector_name.replace("_testnet", "").replace("_demo", "")
            self.candles_config = [
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=max(self.volume_period + self.atr_period + 10, 50),
                ),
            ]


class E1VolumeIgnitionController(DirectionalTradingControllerBase):
    """E1 Volume Ignition — momentum continuation after institutional flow."""

    def __init__(self, config: E1VolumeIgnitionConfig, *args, **kwargs):
        self.config = config
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
            max_records=c.volume_period + c.atr_period + 10,
        )

        if df is None or len(df) < c.volume_period + c.atr_period:
            self.processed_data["signal"] = 0
            self.processed_data["signal_reason"] = "insufficient_data"
            return

        df = self._compute_features(df)
        signal = self._evaluate_signal(df)

        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

        # Extract TP/SL for get_executor_config
        if signal != 0 and len(df) > 0:
            last = df.iloc[-1]
            atr = last.get("atr")
            price = last["close"]
            if atr and not pd.isna(atr) and atr > 0:
                if signal == 1:
                    self.processed_data["tp_price"] = float(price + c.tp_atr_mult * atr)
                    self.processed_data["sl_price"] = float(price - c.sl_atr_mult * atr)
                else:
                    self.processed_data["tp_price"] = float(price - c.tp_atr_mult * atr)
                    self.processed_data["sl_price"] = float(price + c.sl_atr_mult * atr)
                # Trailing stop levels (absolute prices for conversion)
                self.processed_data["trailing_activation"] = float(c.trailing_activation_atr * atr)
                self.processed_data["trailing_delta"] = float(c.trailing_delta_atr * atr)

    def _compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        c = self.config
        df = df.copy()

        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)
        df["vol_ma"] = df["volume"].rolling(c.volume_period).mean()
        df["vol_ratio"] = df["volume"] / df["vol_ma"]
        df["body"] = (df["close"] - df["open"]).abs()
        df["body_atr_ratio"] = df["body"] / df["atr"]
        df["candle_dir"] = np.sign(df["close"] - df["open"])

        # Ignition detection on PREVIOUS bar — signal fires on the NEXT bar
        # so we enter after the monster candle, not during it.
        df["ignition"] = (
            (df["vol_ratio"].shift(1) > c.volume_threshold) &
            (df["body_atr_ratio"].shift(1) > c.body_atr_threshold)
        )

        df["signal"] = 0
        df.loc[df["ignition"] & (df["candle_dir"].shift(1) > 0), "signal"] = 1
        df.loc[df["ignition"] & (df["candle_dir"].shift(1) < 0), "signal"] = -1

        return df

    def _evaluate_signal(self, df: pd.DataFrame) -> int:
        if len(df) == 0 or "signal" not in df.columns:
            return 0
        return int(df["signal"].iloc[-1])

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        tp_abs = self.processed_data.get("tp_price")
        sl_abs = self.processed_data.get("sl_price")

        if tp_abs is not None and sl_abs is not None and float(price) > 0:
            tp_pct = abs(Decimal(str(tp_abs)) - price) / price
            sl_pct = abs(Decimal(str(sl_abs)) - price) / price
            tp_pct = max(Decimal("0.001"), min(tp_pct, Decimal("0.15")))
            sl_pct = max(Decimal("0.001"), min(sl_pct, Decimal("0.15")))
        else:
            tp_pct = self.config.take_profit or Decimal("0.01")
            sl_pct = self.config.stop_loss or Decimal("0.005")

        # Trailing stop from ATR
        trail_act = self.processed_data.get("trailing_activation")
        trail_delta = self.processed_data.get("trailing_delta")
        trailing = None
        if trail_act and trail_delta and float(price) > 0:
            act_pct = Decimal(str(trail_act)) / price
            delta_pct = Decimal(str(trail_delta)) / price
            act_pct = max(Decimal("0.001"), min(act_pct, Decimal("0.05")))
            delta_pct = max(Decimal("0.001"), min(delta_pct, Decimal("0.03")))
            trailing = TrailingStop(activation_price=act_pct, trailing_delta=delta_pct)

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
                time_limit=self.config.time_limit or 14400,  # 4 hours default
                trailing_stop=trailing,
                open_order_type=OrderType.MARKET,
                take_profit_order_type=OrderType.MARKET,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=self.config.leverage,
        )
