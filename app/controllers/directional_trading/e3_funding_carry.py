"""
E3 — Funding Rate Carry (HB V2 Controller)
V1 — April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: When funding rate is persistently positive (longs paying shorts),
enter SHORT to collect carry. Retail is long-biased; we collect their premium.
When funding is persistently negative (shorts paying longs), enter LONG.

Signal (1h candle with funding_rate column):
  - Funding same sign for N consecutive periods (default: 3)
  - |funding_rate| >= threshold (default: 0.0001 = 0.01%)
  - Direction: opposite to payers (positive funding → SHORT, negative → LONG)

Context:
  - funding_zscore_30: if |z| > 2, extra confidence (extreme vs history)
  - funding_cumulative_8: if abs > threshold, 24h carry is significant

Exits:
  - Funding inverts (sign changes) → signal flips to 0
  - SL: 5% (wide — carry is slow and steady)
  - Time limit: 5 days (rebalance weekly)
  - No trailing stop (not momentum)

Data:
  - Backtest: funding_rate column pre-merged into candle DataFrame
  - Live: fetched from Bybit REST API
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


class E3FundingCarryConfig(DirectionalTradingControllerConfigBase):
    """V1 config — Funding Rate Carry."""
    controller_name: str = "e3_funding_carry"

    # Funding parameters
    funding_streak_min: int = Field(default=3, description="Min consecutive same-sign funding periods")
    funding_rate_threshold: float = Field(default=0.00005, description="Min |funding_rate| to enter")
    funding_zscore_boost: float = Field(default=2.0, description="Z-score threshold for extra confidence")

    # Exit
    sl_pct: float = Field(default=0.05, description="Stop loss as decimal (5%)")
    tp_pct: float = Field(default=0.03, description="Take profit as decimal (3%)")
    time_limit_seconds: int = Field(default=432000, description="5 days")

    # OI filter (skip trades when OI is in bottom percentile — SHAP analysis shows
    # low-OI environments have worst WR across most pairs)
    oi_filter_enabled: bool = Field(default=False, description="Enable OI percentile filter")
    oi_filter_min_pct: float = Field(default=0.20, description="Skip signal when OI rolling pct < this")
    oi_filter_window: int = Field(default=168, description="OI percentile rolling window (hours)")

    # BTC regime filter (only enter when BTC 4h return aligns with trade direction)
    # A/B tested 2026-04-12: +0.15 PF on XRP/SUI, +0.25 on ADA at threshold=0.0
    btc_regime_enabled: bool = Field(default=True, description="Enable BTC regime filter")
    btc_regime_threshold: float = Field(default=0.0, description="Min |btc_return_4h| to suppress (0=any opposite blocks)")

    # Volatility filter (skip when Amihud illiquidity is extreme — thin markets)
    vol_filter_enabled: bool = Field(default=False, description="Enable volatility/illiquidity filter")
    vol_filter_max_pct: float = Field(default=0.95, description="Skip signal when Amihud percentile > this")
    vol_filter_window: int = Field(default=168, description="Amihud rolling percentile window (hours)")

    # Time-of-day filter (only enter during specific UTC sessions)
    tod_filter_enabled: bool = Field(default=False, description="Enable time-of-day session filter")
    tod_allowed_hours: List[int] = Field(
        default_factory=lambda: list(range(0, 24)),
        description="UTC hours when signals are allowed (default: all)"
    )

    # REST API fetch (live mode only)
    funding_api_url: str = Field(default="https://api.bybit.com", description="Bybit API base URL")
    funding_fetch_limit: int = Field(default=50, description="Number of funding records to fetch")

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            candles_connector = self.connector_name.replace("_testnet", "").replace("_demo", "")
            self.candles_config = [
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=100,
                ),
            ]


class E3FundingCarryController(DirectionalTradingControllerBase):
    """E3 Funding Carry — collect carry by being on the paid side of funding."""

    def __init__(self, config: E3FundingCarryConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_funding_fetch = 0.0
        self._cached_funding: list = []
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    async def update_processed_data(self):
        c = self.config

        df = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=100,
        )

        if df is None or len(df) < 10:
            self.processed_data["signal"] = 0
            return

        # Check if derivatives columns are pre-merged (backtest mode)
        if "funding_rate" in df.columns:
            df = self._compute_signals_vectorized(df)
        else:
            # Live mode: fetch from Bybit REST API and compute scalar signal
            funding_rates = self._fetch_funding_live(c.trading_pair)
            signal = self._signal_from_funding_list(funding_rates)
            df = df.copy()
            df["signal"] = 0
            if len(df) > 0:
                df.iloc[-1, df.columns.get_loc("signal")] = signal

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

    def _compute_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute signal column for every bar (required for BacktestingEngine)."""
        c = self.config
        df = df.copy()
        df["signal"] = 0

        fr = df["funding_rate"]

        # Streak: count consecutive same-sign periods using rolling window
        pos = (fr > 0).astype(int)
        neg = (fr < 0).astype(int)

        # Rolling sum of positive/negative — if sum == streak_min, all were same sign
        pos_streak = pos.rolling(window=c.funding_streak_min, min_periods=c.funding_streak_min).sum()
        neg_streak = neg.rolling(window=c.funding_streak_min, min_periods=c.funding_streak_min).sum()

        all_positive = pos_streak == c.funding_streak_min
        all_negative = neg_streak == c.funding_streak_min

        # Magnitude threshold
        above_threshold = fr.abs() >= c.funding_rate_threshold

        # Positive funding streak + above threshold → SHORT (-1)
        df.loc[all_positive & above_threshold, "signal"] = -1
        # Negative funding streak + above threshold → LONG (1)
        df.loc[all_negative & above_threshold, "signal"] = 1

        # OI filter: suppress signals when OI is in bottom percentile
        if c.oi_filter_enabled and "oi_value" in df.columns:
            oi = df["oi_value"]
            oi_pct = oi.rolling(c.oi_filter_window, min_periods=24).rank(pct=True)
            low_oi = oi_pct < c.oi_filter_min_pct
            suppressed = (df["signal"] != 0) & low_oi
            df.loc[suppressed, "signal"] = 0

        # BTC regime filter: suppress signals where BTC 4h move opposes trade direction
        # LONG signals suppressed when BTC is falling, SHORT when BTC is rising
        if c.btc_regime_enabled and "btc_return_4h" in df.columns:
            btc_ret = df["btc_return_4h"]
            # For LONG (signal=1): suppress if BTC falling below -threshold
            long_against_btc = (df["signal"] == 1) & (btc_ret < -c.btc_regime_threshold)
            # For SHORT (signal=-1): suppress if BTC rising above +threshold
            short_against_btc = (df["signal"] == -1) & (btc_ret > c.btc_regime_threshold)
            df.loc[long_against_btc | short_against_btc, "signal"] = 0

        # Volatility filter: suppress signals when Amihud illiquidity is extreme
        # Amihud = |return| / volume — high values = thin, dangerous markets
        if c.vol_filter_enabled:
            close = df["close"]
            volume = df["volume"]
            ret_abs = close.pct_change().abs()
            # Avoid division by zero
            safe_vol = volume.replace(0, np.nan)
            amihud = ret_abs / safe_vol
            amihud_pct = amihud.rolling(c.vol_filter_window, min_periods=24).rank(pct=True)
            illiquid = amihud_pct > c.vol_filter_max_pct
            suppressed = (df["signal"] != 0) & illiquid
            df.loc[suppressed, "signal"] = 0

        # Time-of-day filter: only allow signals during specified UTC hours
        if c.tod_filter_enabled and "timestamp" in df.columns:
            hours = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.hour
            allowed = hours.isin(c.tod_allowed_hours)
            suppressed = (df["signal"] != 0) & ~allowed
            df.loc[suppressed, "signal"] = 0

        return df

    def _signal_from_funding_list(self, funding_rates: list) -> int:
        """Compute signal from list of funding rate dicts (live mode)."""
        c = self.config
        if len(funding_rates) < c.funding_streak_min:
            return 0

        # funding_rates sorted newest first from API
        recent = funding_rates[:c.funding_streak_min]
        rates = [float(r.get("fundingRate", 0)) for r in recent]

        if all(r > 0 for r in rates):
            sign = 1
        elif all(r < 0 for r in rates):
            sign = -1
        else:
            return 0

        if abs(rates[0]) < c.funding_rate_threshold:
            return 0

        return -sign

    def _fetch_funding_live(self, trading_pair: str) -> list:
        """Fetch funding rate history from Bybit REST API. Cached for 5 min."""
        now = time.time()
        if now - self._last_funding_fetch < 300 and self._cached_funding:
            return self._cached_funding

        c = self.config
        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.funding_api_url}/v5/market/funding/history"
            f"?category=linear&symbol={symbol}&limit={c.funding_fetch_limit}"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "hummingbot"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            result = data.get("result", {}).get("list", [])
            self._cached_funding = result
            self._last_funding_fetch = now
            return result
        except Exception as e:
            logger.warning(f"E3 funding fetch failed for {trading_pair}: {e}")
            return self._cached_funding

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        c = self.config
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=c.connector_name,
            trading_pair=c.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=Decimal(str(c.sl_pct)),
                take_profit=Decimal(str(c.tp_pct)),
                time_limit=c.time_limit_seconds,
                trailing_stop=None,
                open_order_type=OrderType.LIMIT,
                take_profit_order_type=OrderType.LIMIT,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=c.leverage,
        )
