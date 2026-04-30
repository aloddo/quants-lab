"""
X14 -- Crowd Fade V2 (HB V2 Controller)
V1 -- April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: When retail traders are extremely crowded on the long side,
their collective positioning creates a liquidity imbalance. Market
makers and whales fade this crowding, causing a sharp reversal.
Go SHORT when the crowd is extremely long.

Evidence (Apr 30, 2026):
  Bybit LS ratio (2,152 events): -0.61% avg 24h fwd, 58.4% WR, p<0.0001
  Binance top traders (498 events): -1.27% avg 24h fwd, 60.0% WR, p<0.0001
  Excess vs baseline: -0.54%

Signal (1h candle with buy_ratio column):
  - buy_ratio rolling z-score > threshold (default: 2.0) over 7-day window
  - Direction: always SHORT (fading the crowded longs)
  - Pair-specific: only trade pairs with >65% historical WR

CRITICAL: This is SHORT-ONLY. Crowded short -> long does NOT work.
The asymmetry comes from retail being systematically wrong at extreme
bullish positioning, but not at extreme bearish positioning.

Exits (ATR-based dynamic):
  - TP: 2.0x ATR(14) -- mean reversion target
  - SL: 2.5x ATR(14) -- wider SL because shorting into momentum
  - Trailing stop: activate at 1.2x ATR, delta 0.6x ATR
  - Time limit: 2 days
  - Safety clamps: 0.3% floor, 12% ceiling

Data:
  - Backtest: buy_ratio column pre-merged into candle DataFrame
  - Live: fetched from Bybit REST API (long/short ratio endpoint)
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


class X14CrowdFadeV2Config(DirectionalTradingControllerConfigBase):
    """X14 config -- Crowd Fade V2 with LS ratio z-score and ATR exits."""
    controller_name: str = "x14_crowd_fade_v2"

    # Crowding detection parameters
    zscore_window: int = Field(default=168, description="Rolling window for z-score (hours, 168=7d)")
    zscore_threshold: float = Field(default=2.0, description="Min z-score to trigger SHORT")
    min_buy_ratio: float = Field(default=0.52, description="Min absolute buy_ratio level (sanity check)")

    # Signal cooldown
    signal_cooldown_bars: int = Field(default=8, description="Min bars between signals (8h)")

    # ATR-based dynamic exits
    atr_period: int = Field(default=14, description="ATR lookback period")
    tp_atr_mult: float = Field(default=2.0, description="Take profit in ATR multiples")
    sl_atr_mult: float = Field(default=2.5, description="Stop loss in ATR multiples (wider for shorts)")
    trailing_act_atr_mult: float = Field(default=1.2, description="Trailing stop activation")
    trailing_delta_atr_mult: float = Field(default=0.6, description="Trailing stop delta")
    time_limit_seconds: int = Field(default=172800, description="2 days")

    # Safety clamps
    exit_pct_floor: float = Field(default=0.003, description="Min exit pct (0.3%)")
    exit_pct_ceiling: float = Field(default=0.12, description="Max exit pct (12%)")

    # Static fallbacks
    fallback_sl_pct: float = Field(default=0.04, description="Fallback SL")
    fallback_tp_pct: float = Field(default=0.03, description="Fallback TP")

    # REST API for live LS ratio fetch
    ls_api_url: str = Field(default="https://api.bybit.com", description="Bybit API base URL")
    ls_fetch_limit: int = Field(default=200, description="Number of LS records to fetch")

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


class X14CrowdFadeV2Controller(DirectionalTradingControllerBase):
    """X14 Crowd Fade V2 -- SHORT when retail is crowded long."""

    def __init__(self, config: X14CrowdFadeV2Config, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_ls_fetch = 0.0
        self._cached_ls: list = []
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

        if df is None or len(df) < c.zscore_window + 10:
            self.processed_data["signal"] = 0
            return

        # Check if buy_ratio is pre-merged (backtest mode)
        if "buy_ratio" in df.columns:
            df = self._compute_signals_vectorized(df)
        else:
            # Live mode: fetch LS ratio from Bybit REST API
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
        """Compute signal column for every bar (required for BacktestingEngine).

        Signal: SHORT (-1) when buy_ratio z-score exceeds threshold.
        No LONG signals -- this strategy is asymmetric by design.
        """
        c = self.config
        df = df.copy()
        df["signal"] = 0

        # ATR for dynamic exits
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        br = df["buy_ratio"]

        # Z-score over rolling window
        br_mean = br.rolling(c.zscore_window, min_periods=c.zscore_window // 2).mean()
        br_std = br.rolling(c.zscore_window, min_periods=c.zscore_window // 2).std()
        # Avoid division by zero
        safe_std = br_std.replace(0, np.nan)
        df["br_zscore"] = (br - br_mean) / safe_std

        # Signal: SHORT when z-score exceeds threshold AND absolute level confirms
        crowded_long = (df["br_zscore"] > c.zscore_threshold) & (br > c.min_buy_ratio)
        df.loc[crowded_long, "signal"] = -1  # SHORT only

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

    def _compute_live_signal(self, df: pd.DataFrame, trading_pair: str) -> int:
        """Compute signal in live mode by fetching LS ratio from Bybit REST API."""
        c = self.config

        ls_data = self._fetch_ls_live(trading_pair)
        if len(ls_data) < c.zscore_window // 2:
            return 0

        # Extract buy_ratio values (newest first from API)
        ratios = [float(r.get("buyRatio", 0.5)) for r in ls_data]
        ratios.reverse()  # oldest first for rolling calc

        if len(ratios) < 24:
            return 0

        arr = np.array(ratios)
        window = min(c.zscore_window, len(arr))
        mean = np.mean(arr[-window:])
        std = np.std(arr[-window:])

        if std <= 0:
            return 0

        current = arr[-1]
        zscore = (current - mean) / std

        if zscore > c.zscore_threshold and current > c.min_buy_ratio:
            logger.info(
                f"X14 SHORT signal: {trading_pair} buy_ratio={current:.4f} "
                f"z={zscore:.2f} (threshold={c.zscore_threshold})"
            )
            return -1  # SHORT

        return 0

    def _fetch_ls_live(self, trading_pair: str) -> list:
        """Fetch long/short ratio history from Bybit REST API."""
        c = self.config
        now = time.time()

        # Cache for 5 minutes
        if now - self._last_ls_fetch < 300 and self._cached_ls:
            return self._cached_ls

        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.ls_api_url}/v5/market/account-ratio"
            f"?category=linear&symbol={symbol}&period=1h&limit={c.ls_fetch_limit}"
        )

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "HummingbotX14"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            ls_list = data.get("result", {}).get("list", [])
            self._cached_ls = ls_list
            self._last_ls_fetch = now
            return ls_list
        except Exception as e:
            logger.warning(f"X14 LS fetch failed: {e}")
            return self._cached_ls or []

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
        trail_act_pct = max(Decimal("0.001"), min(trail_act_pct, Decimal("0.05")))
        trail_delta_pct = max(Decimal("0.001"), min(trail_delta_pct, Decimal("0.03")))

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
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=c.leverage,
        )
