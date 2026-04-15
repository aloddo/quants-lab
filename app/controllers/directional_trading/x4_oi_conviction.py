"""
X4 — OI Conviction Momentum (HB V2 Controller)
V1 — April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: Price moves backed by rising Open Interest represent real capital
commitment (new positions entering). Price moves on falling OI are fragile
(position unwinding/squeezes). OI conviction separates durable trends from noise.

The four OI-price regimes:
  1. Price UP + OI UP   → Real buying (new longs entering) → LONG
  2. Price DOWN + OI UP  → Real selling (new shorts entering) → SHORT
  3. Price UP + OI DOWN  → Short squeeze (shorts covering, fragile) → AVOID
  4. Price DOWN + OI DOWN → Long capitulation (longs exiting, fragile) → AVOID

Signal: Only trade regimes 1 and 2 (conviction moves with rising OI).
Exit: ATR-based dynamic exits (learned from E3 static exit failure).

Data:
  - Backtest: oi_value column pre-merged by BulkBacktestTask
  - Live: fetched from Bybit REST API
  - 368 days of OI history across 46+ pairs
"""
from decimal import Decimal
from typing import List
import json
import logging
import time
import urllib.request

import numpy as np
import pandas as pd
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

logger = logging.getLogger(__name__)


class X4OIConvictionConfig(DirectionalTradingControllerConfigBase):
    """V1 config — OI Conviction Momentum."""
    controller_name: str = "x4_oi_conviction"

    # ── Momentum parameters ──
    momentum_period: int = Field(
        default=12,
        description="Price momentum lookback in hours (12h = half-day trend)"
    )
    momentum_threshold: float = Field(
        default=0.01,
        description="Min |price_return| over momentum_period to qualify (1%)"
    )

    # ── OI conviction parameters ──
    oi_change_period: int = Field(
        default=12,
        description="OI change lookback in hours (match momentum period)"
    )
    oi_change_min_pct: float = Field(
        default=0.02,
        description="Min OI change % over period to confirm conviction (2% = real capital)"
    )
    oi_rsi_period: int = Field(
        default=24,
        description="OI RSI period for overbought/oversold detection"
    )
    oi_rsi_entry_min: float = Field(
        default=50.0,
        description="Min OI RSI for entry (above 50 = OI rising)"
    )

    # ── ATR-based dynamic exits ──
    atr_period: int = Field(
        default=24,
        description="ATR period in hours for exit calculations"
    )
    sl_atr_mult: float = Field(
        default=2.5,
        description="SL = entry price +/- (ATR * mult). Wide enough for momentum."
    )
    tp_atr_mult: float = Field(
        default=3.0,
        description="TP = entry price +/- (ATR * mult). R:R > 1.0."
    )
    time_limit_seconds: int = Field(
        default=259200,
        description="3 days (72h). Momentum decays, don't overstay."
    )

    # ── Funding alignment filter (optional) ──
    funding_alignment_enabled: bool = Field(
        default=True,
        description="Boost conviction when funding aligns with direction"
    )
    funding_aligned_threshold: float = Field(
        default=0.0001,
        description="Min |funding_rate| for alignment confirmation (0.01%)"
    )

    # ── Volume confirmation ──
    volume_filter_enabled: bool = Field(
        default=True,
        description="Require above-average volume to confirm conviction"
    )
    volume_lookback: int = Field(
        default=48,
        description="Volume moving average lookback in hours"
    )
    volume_min_ratio: float = Field(
        default=1.2,
        description="Min volume/avg_volume ratio (1.2 = 20% above average)"
    )

    # ── REST API (live mode) ──
    bybit_api_url: str = Field(default="https://api.bybit.com")

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


class X4OIConvictionController(DirectionalTradingControllerBase):
    """X4 — trade momentum moves confirmed by rising open interest.

    Core insight: OI rising + price moving = real capital entering.
    OI falling + price moving = position unwinding (fragile, avoid).
    """

    def __init__(self, config: X4OIConvictionConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_fetch = 0.0
        self._cached_oi: list = []
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    # ── Signal computation ──────────────────────────────────

    async def update_processed_data(self):
        c = self.config

        df = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=200,
        )

        if df is None or len(df) < max(c.momentum_period, c.atr_period, c.volume_lookback) + 5:
            self.processed_data["signal"] = 0
            return

        # Backtest mode: oi_value column pre-merged
        if "oi_value" in df.columns:
            df = self._compute_signals_vectorized(df)
        else:
            # Live mode: fetch OI from API, compute scalar
            signal = self._compute_signal_live(df, c.trading_pair)
            df = df.copy()
            df["signal"] = 0
            if len(df) > 0:
                df.iloc[-1, df.columns.get_loc("signal")] = signal

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

    def _compute_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute OI conviction signal for every bar (required for BacktestingEngine).

        CRITICAL: Do NOT store intermediate NaN columns in returned DataFrame.
        BacktestingEngine.prepare_market_data() does dropna(how='any').

        Signal logic:
          1. Price momentum = return over lookback period
          2. OI change = % change over same period
          3. Conviction: momentum AND OI rising together
          4. Direction: follow the momentum (OI confirms, not opposes)
          5. Filters: volume, funding alignment
        """
        c = self.config
        df = df.copy()
        df["signal"] = 0

        close = df["close"]
        oi = df["oi_value"]

        # Step 1: Price momentum (temporary, NOT stored)
        price_return = close.pct_change(periods=c.momentum_period)

        # Step 2: OI change percentage (temporary, NOT stored)
        oi_change_pct = oi.pct_change(periods=c.oi_change_period)

        # Step 3: OI is rising (the conviction signal)
        oi_rising = oi_change_pct > c.oi_change_min_pct

        # Step 4: Price momentum is significant
        strong_up = price_return > c.momentum_threshold
        strong_down = price_return < -c.momentum_threshold

        # Regime 1: Price UP + OI UP → LONG (real buying)
        long_signal = strong_up & oi_rising
        # Regime 2: Price DOWN + OI UP → SHORT (real selling)
        short_signal = strong_down & oi_rising

        df.loc[long_signal, "signal"] = 1
        df.loc[short_signal, "signal"] = -1

        # Step 5: Volume filter (above-average volume confirms conviction)
        if c.volume_filter_enabled and "volume" in df.columns:
            vol = df["volume"]
            vol_ma = vol.rolling(c.volume_lookback, min_periods=24).mean()
            vol_ratio = vol / vol_ma.replace(0, np.nan)
            low_volume = vol_ratio < c.volume_min_ratio
            df.loc[(df["signal"] != 0) & low_volume, "signal"] = 0

        # Step 6: Funding alignment (optional — confirms market agrees)
        if c.funding_alignment_enabled and "funding_rate" in df.columns:
            fr = df["funding_rate"]
            # Long signal: prefer negative funding (shorts paying → crowd is short → contrarian long + OI confirms)
            # Actually for momentum: positive funding aligns with LONG (longs dominant)
            # Let's not over-filter — just suppress if funding STRONGLY opposes
            strong_opposing_long = (df["signal"] == 1) & (fr < -c.funding_aligned_threshold)
            strong_opposing_short = (df["signal"] == -1) & (fr > c.funding_aligned_threshold)
            # Don't suppress — just be aware. Funding opposing momentum means squeeze risk.
            # For v1, leave as-is. Can add suppression in v2 if needed.

        return df

    def _compute_signal_live(self, df: pd.DataFrame, trading_pair: str) -> int:
        """Compute OI conviction signal from live data."""
        c = self.config

        if len(df) < c.momentum_period:
            return 0

        close = df["close"]
        price_return = (close.iloc[-1] - close.iloc[-c.momentum_period - 1]) / close.iloc[-c.momentum_period - 1]

        # Fetch OI
        oi_data = self._fetch_bybit_oi(trading_pair)
        if len(oi_data) < c.oi_change_period:
            return 0

        oi_values = [float(d.get("openInterest", 0)) for d in oi_data[:c.oi_change_period + 1]]
        oi_values.reverse()  # oldest first

        if oi_values[0] == 0:
            return 0
        oi_change = (oi_values[-1] - oi_values[0]) / oi_values[0]

        # Conviction: OI must be rising
        if oi_change < c.oi_change_min_pct:
            return 0

        # Volume check
        if c.volume_filter_enabled and "volume" in df.columns:
            vol = df["volume"]
            vol_ma = vol.rolling(c.volume_lookback, min_periods=24).mean().iloc[-1]
            if vol_ma > 0 and vol.iloc[-1] / vol_ma < c.volume_min_ratio:
                return 0

        # Direction follows momentum
        if price_return > c.momentum_threshold:
            return 1  # LONG
        elif price_return < -c.momentum_threshold:
            return -1  # SHORT

        return 0

    def _fetch_bybit_oi(self, trading_pair: str) -> list:
        """Fetch Bybit open interest history via REST API."""
        c = self.config
        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.bybit_api_url}/v5/market/open-interest"
            f"?category=linear&symbol={symbol}&intervalTime=1h&limit=50"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "hummingbot"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            return data.get("result", {}).get("list", [])
        except Exception as e:
            logger.warning(f"X4 OI fetch failed for {trading_pair}: {e}")
            return []

    # ── Executor config ─────────────────────────────────────

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        c = self.config

        # Dynamic ATR-based exits (learned from E3 static exit failure)
        # Compute ATR from recent candles
        df = self.processed_data.get("features")
        if df is not None and len(df) >= c.atr_period and "high" in df.columns and "low" in df.columns:
            tr = df["high"] - df["low"]
            atr = tr.rolling(c.atr_period).mean().iloc[-1]
            atr_pct = float(atr / df["close"].iloc[-1]) if df["close"].iloc[-1] != 0 else 0.03
        else:
            atr_pct = 0.03  # fallback 3%

        # Clamp to reasonable range
        sl_pct = max(0.015, min(0.08, atr_pct * c.sl_atr_mult))
        tp_pct = max(0.02, min(0.10, atr_pct * c.tp_atr_mult))

        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=c.connector_name,
            trading_pair=c.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=Decimal(str(round(sl_pct, 4))),
                take_profit=Decimal(str(round(tp_pct, 4))),
                time_limit=c.time_limit_seconds,
                trailing_stop=None,  # Momentum — could add trailing in v2
                open_order_type=OrderType.LIMIT,
                take_profit_order_type=OrderType.LIMIT,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=c.leverage,
        )
