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
    funding_rate_threshold: float = Field(default=0.0001, description="Min |funding_rate| to enter")
    funding_zscore_boost: float = Field(default=2.0, description="Z-score threshold for extra confidence")

    # Exit
    sl_pct: float = Field(default=0.05, description="Stop loss as decimal (5%)")
    tp_pct: float = Field(default=0.03, description="Take profit as decimal (3%)")
    time_limit_seconds: int = Field(default=432000, description="5 days")

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
            signal = self._signal_from_df(df)
        else:
            # Live mode: fetch from Bybit REST API
            funding_rates = self._fetch_funding_live(c.trading_pair)
            signal = self._signal_from_funding_list(funding_rates)

        self.processed_data["signal"] = signal

    def _signal_from_df(self, df: pd.DataFrame) -> int:
        """Compute signal from DataFrame with funding_rate column (backtest mode)."""
        c = self.config
        fr = df["funding_rate"].dropna()
        if len(fr) < c.funding_streak_min:
            return 0

        # Get recent funding rates (most recent last)
        recent = fr.tail(c.funding_streak_min).values

        # Check streak: all same sign
        if all(r > 0 for r in recent):
            sign = 1  # positive funding = longs pay shorts
        elif all(r < 0 for r in recent):
            sign = -1  # negative funding = shorts pay longs
        else:
            return 0

        # Check magnitude threshold
        current_rate = float(fr.iloc[-1])
        if abs(current_rate) < c.funding_rate_threshold:
            return 0

        # Optional z-score boost check
        if "funding_zscore_30" in df.columns:
            zscore = df["funding_zscore_30"].iloc[-1]
            if pd.notna(zscore) and abs(zscore) > c.funding_zscore_boost:
                pass  # extra confidence, could log or adjust sizing

        # Positive funding → SHORT (collect from longs)
        # Negative funding → LONG (collect from shorts)
        return -sign

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
                open_order_type=OrderType.MARKET,
                take_profit_order_type=OrderType.MARKET,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=c.leverage,
        )
