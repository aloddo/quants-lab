"""
E4 — Crowd Fade (HB V2 Controller)
V1 — April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: When the long/short ratio reaches an extreme (crowd heavily positioned
one way), fade them. Retail traders are consistently wrong at extremes.
OI must still be rising (confirms crowding, not liquidation cascade).

Signal (1h candle with buy_ratio + oi_value columns):
  - buy_ratio > 0.65 → SHORT (crowd is long, fade them)
  - buy_ratio < 0.35 → LONG (crowd is short, fade them)
  - OI must be rising over last N bars (not a liquidation unwind)
  - Optional: oi_rsi_14 > 70 + SHORT, or < 30 + LONG = extra confluence

Exits:
  - LS ratio returns to neutral [0.4, 0.6] → signal flips to 0
  - SL: 3%
  - Time limit: 48 hours
  - Trailing: activate at 1%, delta 0.5%

Data:
  - Backtest: buy_ratio, oi_value columns pre-merged into candle DataFrame
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
    TrailingStop,
    TripleBarrierConfig,
)

logger = logging.getLogger(__name__)


class E4CrowdFadeConfig(DirectionalTradingControllerConfigBase):
    """V1 config — Crowd Fade."""
    controller_name: str = "e4_crowd_fade"

    # LS ratio thresholds
    crowd_long_threshold: float = Field(default=0.65, description="buy_ratio above this → SHORT")
    crowd_short_threshold: float = Field(default=0.35, description="buy_ratio below this → LONG")
    neutral_low: float = Field(default=0.40, description="Below this = still short-crowded")
    neutral_high: float = Field(default=0.60, description="Above this = still long-crowded")

    # OI confirmation
    oi_rising_periods: int = Field(default=3, description="OI must be rising over this many bars")

    # Exit params
    sl_pct: float = Field(default=0.03, description="Stop loss 3%")
    tp_pct: float = Field(default=0.025, description="Take profit 2.5%")
    time_limit_seconds: int = Field(default=172800, description="48 hours")
    trailing_activation_pct: float = Field(default=0.01, description="Trailing activation 1%")
    trailing_delta_pct: float = Field(default=0.005, description="Trailing delta 0.5%")

    # REST API (live mode)
    api_url: str = Field(default="https://api.bybit.com", description="Bybit API base URL")
    fetch_limit: int = Field(default=50, description="Records to fetch per API call")

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


class E4CrowdFadeController(DirectionalTradingControllerBase):
    """E4 Crowd Fade — fade LS ratio extremes with OI confirmation."""

    def __init__(self, config: E4CrowdFadeConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_ls_fetch = 0.0
        self._last_oi_fetch = 0.0
        self._cached_ls: list = []
        self._cached_oi: list = []
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
        if "buy_ratio" in df.columns and "oi_value" in df.columns:
            signal = self._signal_from_df(df)
        else:
            # Live mode: fetch from Bybit REST API
            signal = self._signal_from_api(c.trading_pair)

        self.processed_data["signal"] = signal

    def _signal_from_df(self, df: pd.DataFrame) -> int:
        """Compute signal from DataFrame with buy_ratio + oi_value columns."""
        c = self.config
        br = df["buy_ratio"].dropna()
        oi = df["oi_value"].dropna()

        if len(br) < 1 or len(oi) < c.oi_rising_periods + 1:
            return 0

        current_br = float(br.iloc[-1])
        signal = 0

        # Check LS extreme
        if current_br > c.crowd_long_threshold:
            signal = -1  # crowd is long → SHORT
        elif current_br < c.crowd_short_threshold:
            signal = 1   # crowd is short → LONG
        else:
            return 0

        # OI confirmation: must be rising (crowding, not unwinding)
        recent_oi = oi.tail(c.oi_rising_periods + 1).values
        if len(recent_oi) >= 2:
            oi_increasing = all(
                recent_oi[i] <= recent_oi[i + 1]
                for i in range(len(recent_oi) - 1)
            )
            if not oi_increasing:
                return 0  # OI falling = liquidation cascade, don't fade

        return signal

    def _signal_from_api(self, trading_pair: str) -> int:
        """Compute signal from Bybit REST API data (live mode)."""
        c = self.config
        ls_data = self._fetch_ls_ratio(trading_pair)
        oi_data = self._fetch_oi(trading_pair)

        if not ls_data or not oi_data:
            return 0

        current_br = float(ls_data[0].get("buyRatio", 0.5))
        signal = 0

        if current_br > c.crowd_long_threshold:
            signal = -1
        elif current_br < c.crowd_short_threshold:
            signal = 1
        else:
            return 0

        # OI confirmation from API data (newest first)
        if len(oi_data) >= c.oi_rising_periods + 1:
            recent = oi_data[:c.oi_rising_periods + 1]
            oi_values = [float(r.get("openInterest", 0)) for r in reversed(recent)]
            oi_increasing = all(
                oi_values[i] <= oi_values[i + 1]
                for i in range(len(oi_values) - 1)
            )
            if not oi_increasing:
                return 0

        return signal

    def _fetch_ls_ratio(self, trading_pair: str) -> list:
        """Fetch LS ratio from Bybit. Cached 5 min."""
        now = time.time()
        if now - self._last_ls_fetch < 300 and self._cached_ls:
            return self._cached_ls

        c = self.config
        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.api_url}/v5/market/account-ratio"
            f"?category=linear&symbol={symbol}&period=1h&limit={c.fetch_limit}"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "hummingbot"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            result = data.get("result", {}).get("list", [])
            self._cached_ls = result
            self._last_ls_fetch = now
            return result
        except Exception as e:
            logger.warning(f"E4 LS ratio fetch failed for {trading_pair}: {e}")
            return self._cached_ls

    def _fetch_oi(self, trading_pair: str) -> list:
        """Fetch open interest from Bybit. Cached 5 min."""
        now = time.time()
        if now - self._last_oi_fetch < 300 and self._cached_oi:
            return self._cached_oi

        c = self.config
        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.api_url}/v5/market/open-interest"
            f"?category=linear&symbol={symbol}&intervalTime=1h&limit={c.fetch_limit}"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "hummingbot"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            result = data.get("result", {}).get("list", [])
            self._cached_oi = result
            self._last_oi_fetch = now
            return result
        except Exception as e:
            logger.warning(f"E4 OI fetch failed for {trading_pair}: {e}")
            return self._cached_oi

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
                trailing_stop=TrailingStop(
                    activation_price=Decimal(str(c.trailing_activation_pct)),
                    trailing_delta=Decimal(str(c.trailing_delta_pct)),
                ),
                open_order_type=OrderType.MARKET,
                take_profit_order_type=OrderType.MARKET,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=c.leverage,
        )
