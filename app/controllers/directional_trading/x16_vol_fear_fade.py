"""
X16 -- Vol Fear Fade (HB V2 Controller)
V1 -- May 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: When implied volatility spikes sharply (DVOL z-score > threshold),
the options market is pricing extreme fear. This fear systematically
overshoots -- price recovers within 24-48h as panic subsides.
Go LONG when DVOL spikes indicate extreme fear.

Evidence (May 1, 2026, 212 days, 5000+ hourly observations):
  DVOL spike z>2.0: +1.50% fwd 48h, 69% WR, p<0.001, n=263
  DVOL spike z>2.5: +1.84% fwd 48h, 73% WR, p<0.001, n=124
  NOT "buy the dip": works even without price drop (72% WR when price flat)
  Regime-stable: first half 75% WR, second half 65% WR
  Independence: price drop WITHOUT DVOL spike -> continues DOWN (-0.58%)

Signal (1h candle with dvol column or live Deribit API):
  - DVOL 24h change z-score > threshold (default: 2.0) over 7-day window
  - Direction: always LONG (fading the fear overshoot)
  - BTC-USDT only (DVOL is BTC-specific)

CRITICAL: This is LONG-ONLY. Vol crush (contango) -> SHORT does NOT work
reliably (41-49% WR). The asymmetry comes from panic overshooting
systematically, while complacency unwinds slowly.

Exits (ATR-based dynamic):
  - TP: 2.5x ATR(14) -- mean reversion target (larger because LONG into dip)
  - SL: 3.0x ATR(14) -- wider SL because buying into vol
  - Trailing stop: activate at 1.5x ATR, delta 0.8x ATR
  - Time limit: 2 days (48h optimal window)
  - Safety clamps: 0.3% floor, 12% ceiling

Data:
  - Backtest: dvol column pre-merged into candle DataFrame
  - Live: fetched from Deribit public API (DVOL index)
"""
from decimal import Decimal
from typing import List
import json
import logging
import time
import urllib.request

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

logger = logging.getLogger(__name__)


class X16VolFearFadeConfig(DirectionalTradingControllerConfigBase):
    """X16 config -- Vol Fear Fade with DVOL spike z-score and ATR exits."""
    model_config = {"extra": "ignore"}
    controller_name: str = "x16_vol_fear_fade"

    # DVOL spike detection parameters
    dvol_zscore_window: int = Field(default=168, description="Rolling window for DVOL z-score (hours, 168=7d)")
    dvol_spike_threshold: float = Field(default=2.0, description="Min DVOL 24h-delta z-score to trigger LONG")
    dvol_level_threshold: float = Field(default=2.0, description="Alt: DVOL level z-score threshold")
    dvol_delta_lookback: int = Field(default=24, description="Hours for DVOL change calculation")

    # Signal cooldown
    signal_cooldown_bars: int = Field(default=24, description="Min bars between signals (24h)")

    # ATR-based dynamic exits
    atr_period: int = Field(default=14, description="ATR lookback period")
    tp_atr_mult: float = Field(default=2.5, description="Take profit in ATR multiples")
    sl_atr_mult: float = Field(default=3.0, description="Stop loss in ATR multiples (wider for buying dip)")
    trailing_act_atr_mult: float = Field(default=1.5, description="Trailing stop activation")
    trailing_delta_atr_mult: float = Field(default=0.8, description="Trailing stop delta")
    time_limit_seconds: int = Field(default=172800, description="2 days (48h)")

    # Safety clamps
    exit_pct_floor: float = Field(default=0.003, description="Min exit pct (0.3%)")
    exit_pct_ceiling: float = Field(default=0.12, description="Max exit pct (12%)")

    # Static fallbacks
    fallback_sl_pct: float = Field(default=0.05, description="Fallback SL (5% for BTC)")
    fallback_tp_pct: float = Field(default=0.04, description="Fallback TP (4%)")

    # Deribit API for live DVOL fetch
    deribit_api_url: str = Field(default="https://www.deribit.com/api/v2", description="Deribit API base URL")

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            candles_connector = self.connector_name.replace("_testnet", "").replace("_demo", "")
            self.candles_config = [
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=200,
                ),
            ]


class X16VolFearFadeController(DirectionalTradingControllerBase):
    """X16 Vol Fear Fade -- LONG when DVOL spikes indicate extreme fear."""

    def __init__(self, config: X16VolFearFadeConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_dvol_fetch = 0.0
        self._cached_dvol: list = []
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    async def update_processed_data(self):
        c = self.config

        df = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=200,
        )

        if df is None or len(df) < c.dvol_zscore_window + 10:
            self.processed_data["signal"] = 0
            return

        # Check if dvol is pre-merged (backtest mode)
        # The merge engine outputs "dvol_close" from the deribit_dvol registry entry
        if "dvol_close" in df.columns:
            df = df.rename(columns={"dvol_close": "dvol"})
        if "dvol" in df.columns:
            df = self._compute_signals_vectorized(df)
        else:
            # Live mode: fetch DVOL from Deribit public API
            signal = self._compute_live_signal(df)
            df = df.copy()
            df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)
            df["signal"] = 0
            if len(df) > 0:
                df.iloc[-1, df.columns.get_loc("signal")] = signal

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

        # Store ATR for dynamic exits
        if signal != 0 and len(df) > 0:
            last = df.iloc[-1]
            atr_val = float(last.get("atr", 0) or 0)
            price_val = float(last["close"])
            if atr_val > 0 and price_val > 0:
                self.processed_data["entry_atr"] = atr_val
                self.processed_data["entry_price"] = price_val

    def _compute_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute signal column for every bar (required for BacktestingEngine).

        Signal: LONG (+1) when DVOL 24h-delta z-score exceeds threshold.
        No SHORT signals -- this strategy is asymmetric by design.
        """
        c = self.config
        df = df.copy()
        df["signal"] = 0

        # ATR for dynamic exits
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        dvol = df["dvol"].copy()
        # DVOL is never truly 0 — treat 0 as missing (fill_value artifact from merge)
        dvol = dvol.replace(0.0, np.nan)

        # DVOL 24h change (NaN where data is missing)
        dvol_delta = dvol.diff(c.dvol_delta_lookback)

        # Z-score of the delta over rolling window
        delta_mean = dvol_delta.rolling(c.dvol_zscore_window, min_periods=c.dvol_zscore_window // 2).mean()
        delta_std = dvol_delta.rolling(c.dvol_zscore_window, min_periods=c.dvol_zscore_window // 2).std()
        safe_std = delta_std.replace(0, np.nan)
        df["dvol_delta_z"] = (dvol_delta - delta_mean) / safe_std

        # Also compute level z-score as secondary confirmation
        dvol_mean = dvol.rolling(c.dvol_zscore_window, min_periods=c.dvol_zscore_window // 2).mean()
        dvol_std = dvol.rolling(c.dvol_zscore_window, min_periods=c.dvol_zscore_window // 2).std()
        safe_dvol_std = dvol_std.replace(0, np.nan)
        df["dvol_level_z"] = (dvol - dvol_mean) / safe_dvol_std

        # Signal: LONG when DVOL spike z-score exceeds threshold
        # Primary: delta z-score (vol is spiking)
        # Secondary confirmation: level z-score (vol is already high)
        fear_spike = (df["dvol_delta_z"] > c.dvol_spike_threshold)
        df.loc[fear_spike, "signal"] = 1  # LONG only

        # Cooldown: suppress signals within N bars of a previous signal
        if c.signal_cooldown_bars > 0:
            signal_col = df["signal"].copy()
            last_signal_idx = -c.signal_cooldown_bars - 1
            for i in range(len(signal_col)):
                if signal_col.iloc[i] != 0:
                    if (i - last_signal_idx) <= c.signal_cooldown_bars:
                        signal_col.iloc[i] = 0
                    else:
                        last_signal_idx = i
            df["signal"] = signal_col

        return df

    def _compute_live_signal(self, df: pd.DataFrame) -> int:
        """Compute signal in live mode by fetching DVOL from Deribit API."""
        c = self.config

        dvol_history = self._fetch_dvol_live()
        if len(dvol_history) < c.dvol_zscore_window // 2:
            return 0

        arr = np.array(dvol_history)  # oldest first

        # Compute 24h delta
        if len(arr) < c.dvol_delta_lookback + 1:
            return 0

        deltas = np.diff(arr, n=1)
        # We need the rolling delta (current - 24h ago)
        current_delta = arr[-1] - arr[-1 - c.dvol_delta_lookback] if len(arr) > c.dvol_delta_lookback else 0

        # Z-score of recent deltas
        recent_deltas = []
        for i in range(c.dvol_delta_lookback, len(arr)):
            recent_deltas.append(arr[i] - arr[i - c.dvol_delta_lookback])

        if len(recent_deltas) < 24:
            return 0

        delta_arr = np.array(recent_deltas)
        window = min(c.dvol_zscore_window, len(delta_arr))
        mean = np.mean(delta_arr[-window:])
        std = np.std(delta_arr[-window:])

        if std <= 0:
            return 0

        zscore = (current_delta - mean) / std

        if zscore > c.dvol_spike_threshold:
            logger.info(
                f"X16 LONG signal: BTC-USDT dvol_delta={current_delta:.2f} "
                f"z={zscore:.2f} (threshold={c.dvol_spike_threshold})"
            )
            return 1  # LONG

        return 0

    def _fetch_dvol_live(self) -> list:
        """Fetch DVOL index history from Deribit public API."""
        c = self.config
        now = time.time()

        # Cache for 5 minutes
        if now - self._last_dvol_fetch < 300 and self._cached_dvol:
            return self._cached_dvol

        # Deribit public API: get_volatility_index_data
        # Returns hourly DVOL candles
        end_ts = int(now * 1000)
        start_ts = end_ts - (c.dvol_zscore_window + c.dvol_delta_lookback + 48) * 3600 * 1000
        url = (
            f"{c.deribit_api_url}/public/get_volatility_index_data"
            f"?currency=BTC&start_timestamp={start_ts}&end_timestamp={end_ts}&resolution=3600"
        )

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "HummingbotX16"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())

            # Response: result.data = [[timestamp, open, high, low, close], ...]
            candles = data.get("result", {}).get("data", [])
            if not candles:
                return self._cached_dvol or []

            # Extract close values (index 4), sorted oldest first
            dvol_values = [float(c[4]) for c in sorted(candles, key=lambda x: x[0])]
            self._cached_dvol = dvol_values
            self._last_dvol_fetch = now
            return dvol_values
        except Exception as e:
            logger.warning(f"X16 DVOL fetch failed: {e}")
            return self._cached_dvol or []

    def _compute_dynamic_exits(self, price: Decimal) -> dict:
        """Compute ATR-based exit percentages."""
        c = self.config
        atr = self.processed_data.get("atr", 0)
        if not atr or (isinstance(atr, float) and np.isnan(atr)):
            atr = self.processed_data.get("entry_atr", 0)
        atr = float(atr) if atr else 0
        entry_price = float(price) if float(price) > 0 else self.processed_data.get("entry_price", 0)

        if atr <= 0 or entry_price <= 0:
            return {
                "tp_pct": Decimal(str(c.fallback_tp_pct)),
                "sl_pct": Decimal(str(c.fallback_sl_pct)),
                "trailing_stop": None,
            }

        tp_pct = Decimal(str(c.tp_atr_mult * atr / entry_price))
        sl_pct = Decimal(str(c.sl_atr_mult * atr / entry_price))
        trail_act_pct = Decimal(str(c.trailing_act_atr_mult * atr / entry_price))
        trail_delta_pct = Decimal(str(c.trailing_delta_atr_mult * atr / entry_price))

        # Safety clamps
        floor = Decimal(str(c.exit_pct_floor))
        ceiling = Decimal(str(c.exit_pct_ceiling))
        tp_pct = max(floor, min(tp_pct, ceiling))
        sl_pct = max(floor, min(sl_pct, ceiling))
        trail_act_pct = max(Decimal("0.001"), min(trail_act_pct, Decimal("0.20")))
        trail_delta_pct = max(Decimal("0.001"), min(trail_delta_pct, Decimal("0.10")))

        return {
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "trailing_stop": TrailingStop(
                activation_price=trail_act_pct,
                trailing_delta=trail_delta_pct,
            ),
        }

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        c = self.config
        exits = self._compute_dynamic_exits(price)

        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=c.connector_name,
            trading_pair=c.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=exits["sl_pct"],
                take_profit=exits["tp_pct"],
                time_limit=c.time_limit_seconds,
                trailing_stop=exits["trailing_stop"],
                open_order_type=OrderType.MARKET,
                take_profit_order_type=OrderType.LIMIT,
                stop_loss_order_type=OrderType.MARKET,
            ),
            leverage=1,
        )
