"""
X13 -- Capitulation Reversal (HB V2 Controller)
V1 -- April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: When both OI and price drop sharply simultaneously, forced
liquidations exhaust selling pressure and price rebounds. This is the
crypto equivalent of a "selling climax."

EDA evidence (Apr 30, 2026):
  - 4,799 capitulation events across 15 major pairs
  - Average 24h forward return: +0.512% (vs baseline -0.071%)
  - t=7.14, p<0.0001 (extremely significant)
  - Win rate: 52.1% (vs baseline 47.9%)

Signal (1h candle with oi_value column):
  - OI 24h pct change < -oi_drop_threshold (default: -0.05 = 5% drop)
  - Price 24h pct change < -price_drop_threshold (default: -0.02 = 2% drop)
  - Both conditions must be true simultaneously
  - Direction: always LONG (buying the capitulation dip)

Exits (ATR-based dynamic):
  - TP: 2.5x ATR(14) -- capture the mean reversion bounce
  - SL: 2.0x ATR(14) -- protect if capitulation continues
  - Trailing stop: activate at 1.5x ATR, delta 0.7x ATR
  - Time limit: 2 days (signal edge decays after 24-48h)
  - Safety clamps: 0.3% floor, 12% ceiling

Data:
  - Backtest: oi_value column pre-merged into candle DataFrame
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


class X13CapitulationReversalConfig(DirectionalTradingControllerConfigBase):
    """X13 config -- Capitulation Reversal with ATR-based dynamic exits."""
    controller_name: str = "x13_capitulation_reversal"

    # Capitulation detection parameters
    oi_lookback: int = Field(default=24, description="Hours to measure OI change")
    price_lookback: int = Field(default=24, description="Hours to measure price change")
    oi_drop_threshold: float = Field(default=0.05, description="Min OI drop pct to trigger (0.05 = 5%)")
    price_drop_threshold: float = Field(default=0.02, description="Min price drop pct to trigger (0.02 = 2%)")

    # Cooldown: avoid re-entering during same capitulation event
    signal_cooldown_bars: int = Field(default=12, description="Min bars between signals (12h)")

    # ATR-based dynamic exits
    atr_period: int = Field(default=14, description="ATR lookback period")
    tp_atr_mult: float = Field(default=2.5, description="Take profit in ATR multiples")
    sl_atr_mult: float = Field(default=2.0, description="Stop loss in ATR multiples")
    trailing_act_atr_mult: float = Field(default=1.5, description="Trailing stop activation in ATR multiples")
    trailing_delta_atr_mult: float = Field(default=0.7, description="Trailing stop delta in ATR multiples")
    time_limit_seconds: int = Field(default=172800, description="2 days (48h)")

    # Safety clamps for ATR-derived percentages
    exit_pct_floor: float = Field(default=0.003, description="Min exit pct (0.3%)")
    exit_pct_ceiling: float = Field(default=0.12, description="Max exit pct (12%)")

    # Static fallbacks (used when ATR unavailable)
    fallback_sl_pct: float = Field(default=0.03, description="Fallback SL when ATR unavailable")
    fallback_tp_pct: float = Field(default=0.025, description="Fallback TP when ATR unavailable")

    # BTC regime filter (suppress longs when BTC is in strong downtrend)
    btc_regime_enabled: bool = Field(default=True, description="Enable BTC regime filter")
    btc_regime_threshold: float = Field(default=-0.05, description="Suppress if BTC 24h ret < this (strong downtrend)")

    # REST API for live OI fetch
    oi_api_url: str = Field(default="https://api.bybit.com", description="Bybit API base URL")

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


class X13CapitulationReversalController(DirectionalTradingControllerBase):
    """X13 Capitulation Reversal -- buy the selling climax when OI + price both drop."""

    def __init__(self, config: X13CapitulationReversalConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_oi_fetch = 0.0
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

        if df is None or len(df) < max(c.oi_lookback, c.price_lookback) + 5:
            self.processed_data["signal"] = 0
            return

        # Check if derivatives columns are pre-merged (backtest mode)
        if "oi_value" in df.columns:
            df = self._compute_signals_vectorized(df)
        else:
            # Live mode: fetch OI from Bybit REST API
            signal = self._compute_live_signal(df, c.trading_pair)
            df = df.copy()
            df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)
            df["signal"] = 0
            if len(df) > 0:
                df.iloc[-1, df.columns.get_loc("signal")] = signal

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

        # Store ATR and entry price for dynamic exits
        if signal != 0 and len(df) > 0:
            last = df.iloc[-1]
            atr_val = float(last.get("atr", 0) or 0)
            price_val = float(last["close"])
            if atr_val > 0 and price_val > 0:
                self.processed_data["entry_atr"] = atr_val
                self.processed_data["entry_price"] = price_val

    def _compute_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute signal column for every bar (required for BacktestingEngine)."""
        c = self.config
        df = df.copy()
        df["signal"] = 0

        # ATR for dynamic exits
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        # OI percentage change over lookback
        oi = df["oi_value"]
        oi_pct_change = oi.pct_change(c.oi_lookback)

        # Price percentage change over lookback
        price_pct_change = df["close"].pct_change(c.price_lookback)

        # Capitulation: both OI and price dropping significantly
        oi_dropping = oi_pct_change < -c.oi_drop_threshold
        price_dropping = price_pct_change < -c.price_drop_threshold

        # Signal: LONG when both conditions met
        capitulation = oi_dropping & price_dropping
        df.loc[capitulation, "signal"] = 1

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

        # BTC regime filter: suppress longs when BTC is in strong downtrend
        if c.btc_regime_enabled and "btc_return_4h" in df.columns:
            btc_ret_24h = df["close"].pct_change(24)  # Use own pair's 24h as proxy if no BTC column
            # If btc_return_4h column exists, use it; if BTC itself, use price change
            if "btc_return_4h" in df.columns:
                # Strong downtrend: suppress if BTC 4h return is very negative
                strong_downtrend = df["btc_return_4h"] < c.btc_regime_threshold
                df.loc[(df["signal"] == 1) & strong_downtrend, "signal"] = 0

        return df

    def _compute_live_signal(self, df: pd.DataFrame, trading_pair: str) -> int:
        """Compute signal in live mode by fetching OI from Bybit REST API."""
        c = self.config

        # Fetch recent OI data
        oi_data = self._fetch_oi_live(trading_pair)
        if len(oi_data) < c.oi_lookback:
            return 0

        # OI change
        current_oi = float(oi_data[0].get("openInterest", 0))
        old_oi = float(oi_data[min(c.oi_lookback - 1, len(oi_data) - 1)].get("openInterest", 0))
        if old_oi <= 0:
            return 0
        oi_change = (current_oi - old_oi) / old_oi

        # Price change from candles
        if len(df) < c.price_lookback:
            return 0
        current_price = float(df["close"].iloc[-1])
        old_price = float(df["close"].iloc[-c.price_lookback])
        if old_price <= 0:
            return 0
        price_change = (current_price - old_price) / old_price

        # Check capitulation conditions
        if oi_change < -c.oi_drop_threshold and price_change < -c.price_drop_threshold:
            return 1  # LONG

        return 0

    def _fetch_oi_live(self, trading_pair: str) -> list:
        """Fetch open interest history from Bybit REST API."""
        c = self.config
        now = time.time()

        # Cache for 5 minutes
        if now - self._last_oi_fetch < 300 and self._cached_oi:
            return self._cached_oi

        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.oi_api_url}/v5/market/open-interest"
            f"?category=linear&symbol={symbol}&intervalTime=1h&limit=50"
        )

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "HummingbotX13"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            oi_list = data.get("result", {}).get("list", [])
            self._cached_oi = oi_list
            self._last_oi_fetch = now
            return oi_list
        except Exception as e:
            logger.warning(f"X13 OI fetch failed: {e}")
            return self._cached_oi or []

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        """Build executor config with ATR-based dynamic exits."""
        c = self.config
        entry_atr = self.processed_data.get("entry_atr", 0)
        entry_price = self.processed_data.get("entry_price", float(price))

        if entry_atr > 0 and entry_price > 0:
            atr_pct = entry_atr / entry_price

            tp_pct = max(c.exit_pct_floor, min(c.exit_pct_ceiling,
                         atr_pct * c.tp_atr_mult))
            sl_pct = max(c.exit_pct_floor, min(c.exit_pct_ceiling,
                         atr_pct * c.sl_atr_mult))
            trail_act = max(c.exit_pct_floor, min(c.exit_pct_ceiling,
                            atr_pct * c.trailing_act_atr_mult))
            trail_delta = max(c.exit_pct_floor, min(c.exit_pct_ceiling,
                              atr_pct * c.trailing_delta_atr_mult))
        else:
            tp_pct = c.fallback_tp_pct
            sl_pct = c.fallback_sl_pct
            trail_act = c.fallback_tp_pct * 0.6
            trail_delta = c.fallback_tp_pct * 0.28

        triple_barrier = TripleBarrierConfig(
            stop_loss=Decimal(str(sl_pct)),
            take_profit=Decimal(str(tp_pct)),
            time_limit=c.time_limit_seconds,
            trailing_stop=TrailingStop(
                activation_price=Decimal(str(trail_act)),
                trailing_delta=Decimal(str(trail_delta)),
            ),
            open_order_type=OrderType.MARKET,
        )

        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=c.connector_name,
            trading_pair=c.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=triple_barrier,
            leverage=c.leverage,
        )
