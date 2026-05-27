"""
X17 — OI Flush Recovery (HB V2 Controller)
May 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: When OI drops significantly (positions being liquidated/closed) while
funding rate is elevated (crowded longs), the flush creates an oversold condition.
Price recovers as the market absorbs the forced selling. LONG only.

Signal (1h candle with funding_rate + oi_value columns pre-merged):
  Vectorized features at each bar:
  - oi_chg_24h: 24-bar percentage change in OI
  - funding_z: funding rate z-score (rolling 30 events)

  Entry (LONG only):
    oi_chg_24h < oi_drop_threshold (bottom 20% historically ~ -3 to -5%)
    AND funding_z > funding_z_min (confirms crowded-long prior to flush)

  Anti-signal (do NOT enter):
    OI drop + LOW funding (z < -1) = bearish continuation, NOT recovery.

Exits (ATR-based):
  - SL: 2.5x ATR(14) — sized for recovery trades (oversold → bounce)
  - TP: 2.0x ATR(14) — captures the recovery move
  - Trailing: activate at 1.0x ATR, delta 0.5x ATR (tight — recovery is fast)
  - Time limit: 24h (the flush recovery window)
  - Safety clamps: 0.3% floor, 10% ceiling

Phase 0 EDA (2026-05-05):
  - BTC: +64.3 bps/24h (t=5.16) when OI drop + high funding (n=472)
  - Multi-asset: BTC(t=5.3), ADA(t=5.2), APT(t=4.4), BCH(t=4.3), BNB(t=2.6)
  - Anti-signal validated: OI drop + low funding = -19.3 bps (t=-2.18)
  - Non-overlapping BTC (n=146): +42.5 bps/24h (t=2.08)

Data:
  - Backtest: funding_rate + oi_value columns pre-merged into candle DataFrame
    by BulkBacktestTask (required_features: ["derivatives"])
  - Live: fetched from Bybit REST API (funding + OI)
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


class X17OiFlushRecoveryConfig(DirectionalTradingControllerConfigBase):
    """Configuration for X17 OI Flush Recovery."""
    model_config = {"extra": "ignore"}
    controller_name: str = "x17_oi_flush_recovery"

    # ── Signal params ──────────────────────────────────────
    oi_lookback: int = Field(
        default=24,
        description="OI change lookback in hours (24h default)"
    )
    oi_drop_threshold: float = Field(
        default=-0.03,
        description="OI drop threshold (negative pct, e.g. -0.03 = -3%)"
    )
    oi_z_threshold: float = Field(
        default=-1.5,
        description="OI z-score threshold (adaptive, overrides fixed pct if set)"
    )
    oi_z_window: int = Field(
        default=168,
        description="Window for OI change z-score (168h = 7 days)"
    )
    use_oi_zscore: bool = Field(
        default=True,
        description="Use adaptive z-score instead of fixed pct threshold"
    )
    funding_z_min: float = Field(
        default=0.0,
        description="Min funding z-score to confirm crowded-long (0 = any positive)"
    )
    funding_z_window: int = Field(
        default=30,
        description="Rolling window for funding z-score (in events)"
    )
    funding_z_min_periods: int = Field(
        default=15,
        description="Min periods for z-score computation"
    )
    anti_signal_z: float = Field(
        default=-1.0,
        description="Suppress signal when funding z below this (bearish continuation)"
    )

    # ── ATR-based dynamic exits ────────────────────────────
    atr_period: int = Field(default=14, description="ATR lookback period")
    tp_atr_mult: float = Field(default=2.0, description="Take profit in ATR multiples")
    sl_atr_mult: float = Field(default=3.0, description="Stop loss in ATR multiples")
    trailing_act_atr_mult: float = Field(
        default=1.0, description="Trailing stop activation in ATR multiples"
    )
    trailing_delta_atr_mult: float = Field(
        default=0.5, description="Trailing stop delta in ATR multiples"
    )
    time_limit_seconds: int = Field(default=172800, description="48h hold limit")

    # ── Safety clamps ──────────────────────────────────────
    exit_pct_floor: float = Field(default=0.003, description="Min exit pct (0.3%)")
    exit_pct_ceiling: float = Field(default=0.10, description="Max exit pct (10%)")

    # ── Static fallbacks ───────────────────────────────────
    fallback_sl_pct: float = Field(default=0.04, description="Fallback SL when ATR unavailable")
    fallback_tp_pct: float = Field(default=0.03, description="Fallback TP when ATR unavailable")

    # ── REST API fetch (live mode) ─────────────────────────
    api_url: str = Field(default="https://api.bybit.com", description="Bybit API base URL")
    oi_fetch_limit: int = Field(default=50, description="Number of OI records to fetch")
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
                    max_records=500,
                ),
            ]


class X17OiFlushRecoveryController(DirectionalTradingControllerBase):
    """X17 OI Flush Recovery — buy the OI flush when longs were crowded."""

    def __init__(self, config: X17OiFlushRecoveryConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_oi_fetch = 0.0
        self._cached_oi: list = []
        self._last_funding_fetch = 0.0
        self._cached_funding: list = []
        super().__init__(config, *args, **kwargs)

    # ── Candles ────────────────────────────────────────────

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    # ── Signal computation ─────────────────────────────────

    async def update_processed_data(self):
        c = self.config

        df = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=100,
        )

        if df is None or len(df) < max(c.oi_lookback + 10, 50):
            self.processed_data["signal"] = 0
            return

        # Check if derivatives columns are pre-merged (backtest mode)
        if "oi_value" in df.columns:
            df = self._compute_signals_vectorized(df)
        else:
            # Live mode: fetch from Bybit REST API
            oi_values = self._fetch_oi_live(c.trading_pair)
            funding_rates = self._fetch_funding_live(c.trading_pair)
            signal = self._signal_from_live_data(oi_values, funding_rates)
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

        Signal logic:
        1. Compute OI 24h percentage change
        2. Compute funding rate z-score (on event boundaries)
        3. LONG when: OI drops below threshold AND funding_z > min
        4. Suppress when: funding_z < anti_signal_z (bearish continuation)
        """
        c = self.config
        df = df.copy()
        df["signal"] = 0

        # ATR for dynamic exits
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        # ── OI change (percentage, 24h lookback) ─────────
        oi = df.get("oi_value")
        if oi is None or oi.isna().all() or (oi == 0).all():
            return df

        # Replace 0 with NaN then ffill (OI should never be 0)
        oi = oi.replace(0, np.nan).ffill()
        df["oi_chg"] = oi.pct_change(c.oi_lookback)

        # Compute adaptive OI z-score (normalizes across pairs)
        if c.use_oi_zscore:
            oi_chg_mu = df["oi_chg"].rolling(c.oi_z_window, min_periods=48).mean()
            oi_chg_sd = df["oi_chg"].rolling(c.oi_z_window, min_periods=48).std().replace(0, np.nan)
            df["oi_chg_z"] = (df["oi_chg"] - oi_chg_mu) / oi_chg_sd
        else:
            df["oi_chg_z"] = 0.0

        # ── Funding z-score (event-based like X9) ────────
        fr = df.get("funding_rate")
        if fr is not None and not fr.isna().all():
            # Detect funding events (where rate changes)
            fr_changed = fr != fr.shift(1)
            fr_changed.iloc[0] = True
            event_fr = fr[fr_changed]

            if len(event_fr) >= c.funding_z_min_periods:
                fr_mu = event_fr.rolling(c.funding_z_window, min_periods=c.funding_z_min_periods).mean()
                fr_sd = event_fr.rolling(c.funding_z_window, min_periods=c.funding_z_min_periods).std().replace(0, np.nan)
                event_fr_z = (event_fr - fr_mu) / fr_sd

                df["funding_z"] = np.nan
                df.loc[event_fr_z.index, "funding_z"] = event_fr_z
                df["funding_z"] = df["funding_z"].ffill().fillna(0.0)
            else:
                df["funding_z"] = 0.0
        else:
            # No funding data: use raw OI signal only (weaker)
            df["funding_z"] = 0.0

        # ── Entry conditions (LONG only) ─────────────────
        if c.use_oi_zscore:
            oi_drop = df["oi_chg_z"] < c.oi_z_threshold
        else:
            oi_drop = df["oi_chg"] < c.oi_drop_threshold

        funding_confirm = df["funding_z"] > c.funding_z_min
        anti_signal = df["funding_z"] < c.anti_signal_z

        # LONG signal: OI flush + funding confirms crowded-long
        long_trigger = oi_drop & funding_confirm & ~anti_signal

        # Only fire on transitions: when OI first breaches threshold
        if c.use_oi_zscore:
            prev_val = df["oi_chg_z"].shift(1)
            oi_just_crossed = (df["oi_chg_z"] < c.oi_z_threshold) & (prev_val >= c.oi_z_threshold)
        else:
            oi_chg_prev = df["oi_chg"].shift(1)
            oi_just_crossed = (df["oi_chg"] < c.oi_drop_threshold) & (oi_chg_prev >= c.oi_drop_threshold)

        # Signal fires ONLY on crossing events (not while condition persists)
        df.loc[long_trigger & oi_just_crossed, "signal"] = 1

        return df

    # ── Live mode: REST API fetchers ─────────────────────

    def _fetch_oi_live(self, trading_pair: str) -> list:
        """Fetch OI history from Bybit REST API. Cached 5 min."""
        now = time.time()
        if now - self._last_oi_fetch < 300 and self._cached_oi:
            return self._cached_oi

        c = self.config
        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.api_url}/v5/market/open-interest"
            f"?category=linear&symbol={symbol}&intervalTime=1h&limit={c.oi_fetch_limit}"
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
            logger.warning(f"X17 OI fetch failed for {trading_pair}: {e}")
            return self._cached_oi

    def _fetch_funding_live(self, trading_pair: str) -> list:
        """Fetch funding rate history from Bybit REST API. Cached 5 min."""
        now = time.time()
        if now - self._last_funding_fetch < 300 and self._cached_funding:
            return self._cached_funding

        c = self.config
        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.api_url}/v5/market/funding/history"
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
            logger.warning(f"X17 funding fetch failed for {trading_pair}: {e}")
            return self._cached_funding

    def _signal_from_live_data(self, oi_values: list, funding_rates: list) -> int:
        """Compute signal from live API data (newest first → reverse)."""
        c = self.config

        if len(oi_values) < c.oi_lookback + 5:
            return 0

        # Parse OI (newest first from API → reverse to chronological)
        oi_vals = [float(o.get("openInterest", 0)) for o in reversed(oi_values)]
        oi_series = pd.Series(oi_vals)

        # Compute OI change over lookback
        if len(oi_series) < c.oi_lookback + 1:
            return 0
        oi_chg = (oi_series.iloc[-1] - oi_series.iloc[-1 - c.oi_lookback]) / oi_series.iloc[-1 - c.oi_lookback]

        if oi_chg >= c.oi_drop_threshold:
            return 0  # OI hasn't dropped enough

        # Check funding z-score
        if len(funding_rates) < c.funding_z_min_periods:
            # No funding data: allow signal based on OI alone (weaker)
            return 1

        rates = [float(r.get("fundingRate", 0)) for r in reversed(funding_rates)]
        fr_series = pd.Series(rates)

        fr_mu = fr_series.rolling(c.funding_z_window, min_periods=c.funding_z_min_periods).mean()
        fr_sd = fr_series.rolling(c.funding_z_window, min_periods=c.funding_z_min_periods).std().replace(0, np.nan)
        fr_z = ((fr_series - fr_mu) / fr_sd).iloc[-1]

        if pd.isna(fr_z):
            return 1 if oi_chg < c.oi_drop_threshold else 0

        # Anti-signal: low funding = bearish continuation
        if fr_z < c.anti_signal_z:
            return 0

        # Confirm: funding must be above threshold
        if fr_z < c.funding_z_min:
            return 0

        return 1  # LONG only

    # ── Dynamic exits ─────────────────────────────────────

    def _compute_dynamic_exits(self, price: Decimal) -> dict:
        """Compute ATR-based exit percentages."""
        c = self.config

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

        trailing_stop = TrailingStop(
            activation_price=trail_act_pct,
            trailing_delta=trail_delta_pct,
        )

        return {
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "trailing_stop": trailing_stop,
        }

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        """Build position executor with dynamic ATR-based exits."""
        c = self.config
        exits = self._compute_dynamic_exits(price)

        triple_barrier = TripleBarrierConfig(
            stop_loss=exits["sl_pct"],
            take_profit=exits["tp_pct"],
            time_limit=c.time_limit_seconds,
            trailing_stop=exits["trailing_stop"],
            open_order_type=OrderType.MARKET,
            take_profit_order_type=OrderType.LIMIT,
            stop_loss_order_type=OrderType.MARKET,
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
