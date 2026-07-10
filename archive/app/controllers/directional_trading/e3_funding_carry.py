"""
E3 — Funding Rate Directional (HB V2 Controller)
V2 — April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: Persistent funding rate direction reveals retail positioning bias.
When funding is persistently positive (longs paying shorts), retail is
overleveraged long — fade them by entering SHORT. When persistently negative,
retail is short-biased — enter LONG. The edge is in the directional move,
NOT in collecting funding fees (which are incidental).

Signal (1h candle with funding_rate column):
  - Funding same sign for N consecutive periods (default: 3)
  - |funding_rate| >= threshold (default: 0.00005 = 0.005%)
  - Direction: opposite to payers (positive funding -> SHORT, negative -> LONG)

Context:
  - funding_zscore_30: if |z| > 2, extra confidence (extreme vs history)
  - BTC regime filter: suppress signals opposing BTC 4h trend

Exits (V2 — ATR-based dynamic):
  - SL: 2.0x ATR(14) — adapts to per-pair volatility
  - TP: 3.0x ATR(14) — R:R of 1.5 (was 0.6 with static 3%/5%)
  - Trailing stop: activate at 1.5x ATR, delta 0.7x ATR — let funding-driven moves run
  - Time limit: 5 days (rebalance weekly)
  - Safety clamps: 0.3% floor, 12% ceiling on all exit percentages

Changes from V1:
  - Static 3% TP / 5% SL replaced with ATR-based dynamic exits
  - Trailing stop added to let winners run in persistent funding regimes
  - ATR(14) computed in vectorized signal path (backtest) and live path
  - Consistent with exit patterns in E1, E2, E4, S6, S7, S9 controllers

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


class E3FundingCarryConfig(DirectionalTradingControllerConfigBase):
    """V2 config — Funding Rate Carry with ATR-based dynamic exits."""
    controller_name: str = "e3_funding_carry"

    # Funding parameters
    funding_streak_min: int = Field(default=3, description="Min consecutive same-sign funding periods")
    funding_rate_threshold: float = Field(default=0.00005, description="Min |funding_rate| to enter")
    funding_zscore_boost: float = Field(default=2.0, description="Z-score threshold for extra confidence")

    # ATR-based dynamic exits (V2)
    atr_period: int = Field(default=14, description="ATR lookback period")
    tp_atr_mult: float = Field(default=3.0, description="Take profit in ATR multiples")
    sl_atr_mult: float = Field(default=2.0, description="Stop loss in ATR multiples")
    trailing_act_atr_mult: float = Field(default=1.5, description="Trailing stop activation in ATR multiples")
    trailing_delta_atr_mult: float = Field(default=0.7, description="Trailing stop delta in ATR multiples")
    time_limit_seconds: int = Field(default=432000, description="5 days")

    # Safety clamps for ATR-derived percentages
    exit_pct_floor: float = Field(default=0.003, description="Min exit pct (0.3%)")
    exit_pct_ceiling: float = Field(default=0.12, description="Max exit pct (12%)")

    # Static fallbacks (used when ATR unavailable)
    fallback_sl_pct: float = Field(default=0.04, description="Fallback SL when ATR unavailable")
    fallback_tp_pct: float = Field(default=0.03, description="Fallback TP when ATR unavailable")

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
    """E3 Funding Directional — fade retail positioning bias revealed by persistent funding."""

    def __init__(self, config: E3FundingCarryConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_funding_fetch = 0.0
        self._cached_funding: list = []
        super().__init__(config, *args, **kwargs)

    def can_create_executor(self, signal: int) -> bool:
        """Override to enforce cooldown after SL/TL exits, not just active executors.

        The base class only checks cooldown against the last ACTIVE executor's
        timestamp. After a SL closes the executor, active count drops to 0 and
        cooldown is bypassed — causing immediate re-entry into the same losing
        signal. This is a money pit for carry strategies where the funding signal
        persists long after price has moved against us.

        Fix: also check the most recent CLOSED executor on the same side.
        Cooldown applies to whichever timestamp is newer (last open or last close).
        """
        from hummingbot.core.data_type.common import TradeType as TT

        target_side = TT.BUY if signal > 0 else TT.SELL

        # Active executors on this side (same as base class)
        active_same_side = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: x.is_active and x.side == target_side,
        )
        if len(active_same_side) >= self.config.max_executors_per_side:
            return False

        # Find most recent timestamp across BOTH active and closed executors
        # on the same side — this prevents re-entry right after SL
        all_same_side = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: x.side == target_side,
        )
        # Use close_timestamp if available (closed executors), else timestamp (open)
        timestamps = []
        for ex in all_same_side:
            close_ts = getattr(ex, "close_timestamp", None) or 0
            open_ts = getattr(ex, "timestamp", None) or 0
            timestamps.append(max(close_ts, open_ts))

        max_ts = max(timestamps, default=0)
        elapsed = self.market_data_provider.time() - max_ts
        return elapsed > self.config.cooldown_time

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
            df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)
            df["signal"] = 0
            if len(df) > 0:
                df.iloc[-1, df.columns.get_loc("signal")] = signal

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

        # Store ATR and entry price for dynamic exits in get_executor_config()
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

        # Compute ATR for dynamic exits
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

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

    def _compute_dynamic_exits(self, price: Decimal) -> dict:
        """Compute ATR-based exit percentages from stored signal data.

        Returns dict with tp_pct, sl_pct, trailing_stop (or None), all as Decimal.
        Falls back to static values if ATR is unavailable.

        ATR sources (checked in order):
        - processed_data["atr"]: set per-bar by BacktestingEngine via row.to_dict()
        - processed_data["entry_atr"]: set in update_processed_data() for live mode
        """
        c = self.config
        # Backtest: "atr" column auto-populated per-bar. Live: "entry_atr" set on signal.
        atr = self.processed_data.get("atr", 0)
        if not atr or (isinstance(atr, float) and np.isnan(atr)):
            atr = self.processed_data.get("entry_atr", 0)
        atr = float(atr) if atr else 0
        entry_price = float(price) if float(price) > 0 else self.processed_data.get("entry_price", 0)

        if atr <= 0 or entry_price <= 0:
            logger.warning("E3: ATR unavailable, using static fallback exits")
            return {
                "tp_pct": Decimal(str(c.fallback_tp_pct)),
                "sl_pct": Decimal(str(c.fallback_sl_pct)),
                "trailing_stop": None,
            }

        # Convert ATR multiples to percentages of entry price
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

        trailing_stop = TrailingStop(
            activation_price=trail_act_pct,
            trailing_delta=trail_delta_pct,
        )

        logger.info(
            f"E3 dynamic exits: TP={float(tp_pct):.4f} SL={float(sl_pct):.4f} "
            f"trail_act={float(trail_act_pct):.4f} trail_delta={float(trail_delta_pct):.4f} "
            f"(ATR={atr:.4f}, price={entry_price:.2f})"
        )

        return {
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "trailing_stop": trailing_stop,
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
                open_order_type=OrderType.LIMIT,
                take_profit_order_type=OrderType.LIMIT,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=c.leverage,
        )
