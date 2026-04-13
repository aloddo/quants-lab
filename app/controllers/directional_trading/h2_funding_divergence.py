"""
H2 — Cross-Exchange Funding Divergence (HB V2 Controller)
V1 — April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: When Bybit funding rate significantly diverges from Binance funding rate,
arbitrage flow creates directional pressure. Arb bots short the expensive exchange
and long the cheap one, pushing prices toward convergence.

Signal (1h candle with funding_rate + binance_funding_rate columns):
  - funding_spread = bybit_fr - binance_fr
  - Compute rolling z-score of funding_spread (30-period window)
  - Entry: |z-score| > 2.0
  - Direction: SHORT when Bybit premium (spread > 0, arbs will push Bybit down)
               LONG when Bybit discount (spread < 0, arbs will push Bybit up)

Exit:
  - Spread mean-reverts (z-score crosses back through 0) → signal flips to 0
  - SL: 3% (tighter than E3 — arb pressure acts faster than carry)
  - TP: 2% (capture the convergence move)
  - Time limit: 8 hours (arb flow resolves within hours, not days)
  - Trailing stop: 1% activation, 0.5% delta (capture extended moves)

Data:
  - Backtest: funding_rate + binance_funding_rate columns pre-merged into candle DF
  - Live: fetched from both Bybit and Binance REST APIs
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


class H2FundingDivergenceConfig(DirectionalTradingControllerConfigBase):
    """V1 config — Cross-Exchange Funding Divergence."""
    controller_name: str = "h2_funding_divergence"

    # Signal parameters
    zscore_window: int = Field(default=30, description="Rolling window for z-score (in funding periods = ~10 days)")
    zscore_entry: float = Field(default=2.0, description="Z-score threshold for entry")
    min_spread_abs: float = Field(default=0.00005, description="Min |spread| to filter noise")

    # Exit
    sl_pct: float = Field(default=0.03, description="Stop loss as decimal (3%)")
    tp_pct: float = Field(default=0.02, description="Take profit as decimal (2%)")
    time_limit_seconds: int = Field(default=28800, description="8 hours")

    # Trailing stop
    trailing_activation_pct: float = Field(default=0.01, description="Trailing stop activation (1%)")
    trailing_delta_pct: float = Field(default=0.005, description="Trailing stop delta (0.5%)")

    # REST API fetch (live mode only)
    bybit_api_url: str = Field(default="https://api.bybit.com", description="Bybit API base URL")
    binance_api_url: str = Field(default="https://fapi.binance.com", description="Binance API base URL")
    funding_fetch_limit: int = Field(default=50, description="Number of funding records to fetch per exchange")

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


class H2FundingDivergenceController(DirectionalTradingControllerBase):
    """H2 — trade cross-exchange funding divergence."""

    def __init__(self, config: H2FundingDivergenceConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_fetch = 0.0
        self._cached_bybit_funding: list = []
        self._cached_binance_funding: list = []
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

        if df is None or len(df) < c.zscore_window + 5:
            self.processed_data["signal"] = 0
            return

        # Check if derivatives columns are pre-merged (backtest mode)
        if "funding_rate" in df.columns and "binance_funding_rate" in df.columns:
            df = self._compute_signals_vectorized(df)
        else:
            # Live mode: fetch from both APIs and compute scalar signal
            signal = self._compute_signal_live(c.trading_pair)
            df = df.copy()
            df["signal"] = 0
            if len(df) > 0:
                df.iloc[-1, df.columns.get_loc("signal")] = signal

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

    def _compute_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute signal column for every bar (required for BacktestingEngine).

        Vectorized: processes entire DataFrame at once. No scalar assignments.
        """
        c = self.config
        df = df.copy()
        df["signal"] = 0

        # Compute funding spread (Bybit - Binance)
        df["funding_spread"] = df["funding_rate"] - df["binance_funding_rate"]

        # Rolling z-score of funding spread
        spread = df["funding_spread"]
        roll_mean = spread.rolling(window=c.zscore_window, min_periods=c.zscore_window).mean()
        roll_std = spread.rolling(window=c.zscore_window, min_periods=c.zscore_window).std()

        # Avoid division by zero
        roll_std = roll_std.replace(0, np.nan)
        df["spread_zscore"] = (spread - roll_mean) / roll_std

        # Min absolute spread filter
        above_noise = spread.abs() >= c.min_spread_abs

        # Entry signals:
        # Bybit premium (spread > 0, z > threshold) → SHORT (-1): arbs will push Bybit down
        # Bybit discount (spread < 0, z < -threshold) → LONG (1): arbs will push Bybit up
        df.loc[(df["spread_zscore"] > c.zscore_entry) & above_noise, "signal"] = -1
        df.loc[(df["spread_zscore"] < -c.zscore_entry) & above_noise, "signal"] = 1

        return df

    def _compute_signal_live(self, trading_pair: str) -> int:
        """Compute signal from live API data. Cached for 5 min."""
        c = self.config
        now = time.time()
        if now - self._last_fetch < 300 and self._cached_bybit_funding:
            bybit_rates = self._cached_bybit_funding
            binance_rates = self._cached_binance_funding
        else:
            bybit_rates = self._fetch_bybit_funding(trading_pair)
            binance_rates = self._fetch_binance_funding(trading_pair)
            self._cached_bybit_funding = bybit_rates
            self._cached_binance_funding = binance_rates
            self._last_fetch = now

        if len(bybit_rates) < c.zscore_window or len(binance_rates) < c.zscore_window:
            return 0

        # Align by timestamp and compute spread
        bybit_map = {int(r.get("fundingRateTimestamp", 0)): float(r.get("fundingRate", 0))
                     for r in bybit_rates}
        binance_map = {int(r.get("fundingTime", 0)): float(r.get("fundingRate", 0))
                       for r in binance_rates}

        # Find common timestamps (within 1h tolerance)
        spreads = []
        for ts, bybit_fr in sorted(bybit_map.items()):
            # Find closest Binance timestamp within 1h
            best_binance_ts = min(binance_map.keys(), key=lambda t: abs(t - ts), default=None)
            if best_binance_ts and abs(best_binance_ts - ts) < 3600_000:
                spreads.append(bybit_fr - binance_map[best_binance_ts])

        if len(spreads) < c.zscore_window:
            return 0

        spreads = np.array(spreads[-c.zscore_window:])
        mean = spreads.mean()
        std = spreads.std()
        if std == 0:
            return 0

        current_spread = spreads[-1]
        zscore = (current_spread - mean) / std

        if abs(current_spread) < c.min_spread_abs:
            return 0

        if zscore > c.zscore_entry:
            return -1  # SHORT: Bybit premium, arbs push down
        elif zscore < -c.zscore_entry:
            return 1   # LONG: Bybit discount, arbs push up

        return 0

    def _fetch_bybit_funding(self, trading_pair: str) -> list:
        """Fetch funding rate history from Bybit REST API."""
        c = self.config
        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.bybit_api_url}/v5/market/funding/history"
            f"?category=linear&symbol={symbol}&limit={c.funding_fetch_limit}"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "hummingbot"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            return data.get("result", {}).get("list", [])
        except Exception as e:
            logger.warning(f"H2 Bybit funding fetch failed for {trading_pair}: {e}")
            return []

    def _fetch_binance_funding(self, trading_pair: str) -> list:
        """Fetch funding rate history from Binance REST API."""
        c = self.config
        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.binance_api_url}/fapi/v1/fundingRate"
            f"?symbol={symbol}&limit={c.funding_fetch_limit}"
        )
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "hummingbot"})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())
            return data
        except Exception as e:
            logger.warning(f"H2 Binance funding fetch failed for {trading_pair}: {e}")
            return []

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
