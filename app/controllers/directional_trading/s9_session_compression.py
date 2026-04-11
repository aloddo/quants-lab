"""
S9 — Session-Aware Compression Breakout (HB V2 Controller)
V1 — April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: Volatility compression is session-dependent. Asian session (00-08 UTC)
compresses more naturally, so the threshold must be stricter. US session (16-24 UTC)
produces larger breakouts, so exits can be wider. By calibrating both the entry
threshold and exit parameters per-session, we improve on E1's flat compression
approach which failed with 88-91% TIME_LIMIT exits.

Signal (1h):
  - Classify session: Asia (00-08 UTC), Europe (08-16), US (16-24)
  - Compute ATR percentile using ONLY same-session bars over 90 days
  - If session-specific ATR percentile < session threshold → compression confirmed
  - Price breaks range high/low (same range logic as E1, 30-bar lookback)
  - Volume > 1.5× 20-bar average (floor filter)
  - Direction: breakout side (BOTH)

Exits (session-dependent):
  - Asia:   TP 1.5×ATR, SL 1.5×ATR, trailing act=0.8×ATR, 8h limit
  - Europe: TP 2.0×ATR, SL 1.5×ATR, trailing act=1.0×ATR, 8h limit
  - US:     TP 2.5×ATR, SL 2.0×ATR, trailing act=1.2×ATR, 8h limit
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


def _percentile_rank(series: pd.Series, window: int) -> pd.Series:
    """Rolling percentile rank (0-1) of the last value vs the window."""
    def _pct(s):
        if len(s) < 2:
            return float("nan")
        v = s.iloc[-1]
        return (s.iloc[:-1] < v).sum() / (len(s) - 1)
    return series.rolling(window, min_periods=max(100, window // 10)).apply(_pct, raw=False)


# Session definitions (UTC hours)
SESSIONS = {
    "asia":   (0, 8),
    "europe": (8, 16),
    "us":     (16, 24),
}


class S9SessionCompressionConfig(DirectionalTradingControllerConfigBase):
    """V1 config — Session-Aware Compression Breakout."""
    controller_name: str = "s9_session_compression"

    # ATR
    atr_period: int = Field(default=14)
    atr_compression_window_days: int = Field(default=90)

    # Range breakout
    range_period: int = Field(default=30)

    # Volume floor
    volume_period: int = Field(default=20)
    volume_floor_multiplier: float = Field(default=1.5)

    # Per-session compression thresholds (ATR percentile)
    asia_compression_pct: float = Field(default=0.25)
    europe_compression_pct: float = Field(default=0.35)
    us_compression_pct: float = Field(default=0.35)

    # Per-session exit params (ATR multipliers)
    asia_tp_atr: float = Field(default=1.5)
    asia_sl_atr: float = Field(default=1.5)
    europe_tp_atr: float = Field(default=2.0)
    europe_sl_atr: float = Field(default=1.5)
    us_tp_atr: float = Field(default=2.5)
    us_sl_atr: float = Field(default=2.0)

    # Per-session trailing stop (ATR multipliers)
    asia_trailing_act_atr: float = Field(default=0.8)
    asia_trailing_delta_atr: float = Field(default=0.4)
    europe_trailing_act_atr: float = Field(default=1.0)
    europe_trailing_delta_atr: float = Field(default=0.5)
    us_trailing_act_atr: float = Field(default=1.2)
    us_trailing_delta_atr: float = Field(default=0.6)

    # Per-session time limits (seconds)
    asia_time_limit: int = Field(default=28800)    # 8h
    europe_time_limit: int = Field(default=28800)  # 8h
    us_time_limit: int = Field(default=28800)       # 8h

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            lookback_bars = self.atr_compression_window_days * 24 + self.atr_period + 50
            max_1h = max(lookback_bars, self.range_period + 5)
            candles_connector = self.connector_name.replace("_testnet", "").replace("_demo", "")
            self.candles_config = [
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=max_1h,
                ),
            ]


class S9SessionCompressionController(DirectionalTradingControllerBase):
    """S9 Session-Aware Compression Breakout."""

    def __init__(self, config: S9SessionCompressionConfig, *args, **kwargs):
        self.config = config
        self.atr_lookback_bars = config.atr_compression_window_days * 24
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    @staticmethod
    def _classify_session(hour: int) -> str:
        """Classify UTC hour into trading session."""
        for name, (start, end) in SESSIONS.items():
            if start <= hour < end:
                return name
        return "us"  # fallback (shouldn't happen with 0-24 coverage)

    def _get_session_threshold(self, session: str) -> float:
        c = self.config
        return {
            "asia": c.asia_compression_pct,
            "europe": c.europe_compression_pct,
            "us": c.us_compression_pct,
        }[session]

    def _get_session_exits(self, session: str, atr: float, price: float) -> dict:
        """Return session-specific exit parameters."""
        c = self.config
        params = {
            "asia":   (c.asia_tp_atr, c.asia_sl_atr, c.asia_trailing_act_atr, c.asia_trailing_delta_atr, c.asia_time_limit),
            "europe": (c.europe_tp_atr, c.europe_sl_atr, c.europe_trailing_act_atr, c.europe_trailing_delta_atr, c.europe_time_limit),
            "us":     (c.us_tp_atr, c.us_sl_atr, c.us_trailing_act_atr, c.us_trailing_delta_atr, c.us_time_limit),
        }[session]
        tp_mult, sl_mult, trail_act_mult, trail_delta_mult, time_limit = params

        tp_pct = Decimal(str(tp_mult * atr / price))
        sl_pct = Decimal(str(sl_mult * atr / price))
        trail_act_pct = Decimal(str(trail_act_mult * atr / price))
        trail_delta_pct = Decimal(str(trail_delta_mult * atr / price))

        # Clamp to reasonable bounds
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

    async def update_processed_data(self):
        c = self.config
        max_records = self.atr_lookback_bars + c.atr_period + 50

        df = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=max_records,
        )

        min_viable = max(c.atr_period + c.range_period + 10, 50)
        if df is None or len(df) < min_viable:
            self.processed_data["signal"] = 0
            self.processed_data["signal_reason"] = "insufficient_data"
            return

        df = self._compute_features(df)
        signal, session, atr, price = self._evaluate_signal(df)

        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

        if signal != 0 and atr > 0 and price > 0:
            self.processed_data["entry_session"] = session
            self.processed_data["entry_atr"] = atr
            self.processed_data["entry_price"] = price

    def _compute_features(self, df: pd.DataFrame) -> pd.DataFrame:
        c = self.config
        df = df.copy()

        # ATR
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        # Session classification from bar timestamp
        if "timestamp" in df.columns:
            df["hour_utc"] = pd.to_datetime(df["timestamp"], unit="s").dt.hour
        else:
            df["hour_utc"] = df.index.hour
        df["session"] = df["hour_utc"].apply(self._classify_session)

        # Per-session ATR percentile: for each bar, compute percentile using
        # only bars from the same session within the lookback window.
        # 90 days × 8 hours/session = 720 same-session bars.
        session_bars = self.atr_lookback_bars // 3  # ~720 bars per session
        df["session_atr_pct"] = float("nan")

        for session_name in SESSIONS:
            mask = df["session"] == session_name
            session_atr = df.loc[mask, "atr"]
            if len(session_atr) >= 100:
                pct = _percentile_rank(session_atr, min(session_bars, len(session_atr)))
                df.loc[pct.index, "session_atr_pct"] = pct

        # Session-specific compression check
        df["compressed"] = False
        for session_name in SESSIONS:
            mask = df["session"] == session_name
            threshold = self._get_session_threshold(session_name)
            df.loc[mask, "compressed"] = df.loc[mask, "session_atr_pct"] < threshold

        # Range high/low (exclude current bar)
        df["range_high"] = df["high"].shift(1).rolling(c.range_period).max()
        df["range_low"] = df["low"].shift(1).rolling(c.range_period).min()

        # Breakout event (first crossing while compressed)
        prior_inside = (
            (df["close"].shift(1) <= df["range_high"].shift(1)) &
            (df["close"].shift(1) >= df["range_low"].shift(1))
        )
        lc = prior_inside & (df["close"] > df["range_high"]) & df["compressed"]
        sc = prior_inside & (df["close"] < df["range_low"]) & df["compressed"]
        df["breakout_event"] = 0
        df.loc[lc, "breakout_event"] = 1
        df.loc[sc, "breakout_event"] = -1

        # Volume floor
        df["vol_ma"] = df["volume"].rolling(c.volume_period).mean()
        df["vol_ratio"] = df["volume"] / df["vol_ma"]
        df["volume_ok"] = df["vol_ratio"] > c.volume_floor_multiplier

        # Final signal: breakout + volume
        df["signal"] = 0
        df.loc[(df["breakout_event"] == 1) & df["volume_ok"], "signal"] = 1
        df.loc[(df["breakout_event"] == -1) & df["volume_ok"], "signal"] = -1

        return df

    def _evaluate_signal(self, df: pd.DataFrame) -> tuple:
        """Return (signal, session, atr, price) from last bar."""
        if len(df) == 0 or "signal" not in df.columns:
            return 0, "us", 0.0, 0.0

        last = df.iloc[-1]
        signal = int(last.get("signal", 0))
        session = str(last.get("session", "us"))
        atr = float(last.get("atr", 0) or 0)
        price = float(last.get("close", 0))
        return signal, session, atr, price

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        session = self.processed_data.get("entry_session", "us")
        atr = self.processed_data.get("entry_atr", 0)
        entry_price = float(price) if float(price) > 0 else self.processed_data.get("entry_price", 0)

        if atr > 0 and entry_price > 0:
            exits = self._get_session_exits(session, atr, entry_price)
        else:
            exits = {
                "tp_pct": self.config.take_profit or Decimal("0.02"),
                "sl_pct": self.config.stop_loss or Decimal("0.015"),
                "trailing": None,
                "time_limit": 28800,
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
