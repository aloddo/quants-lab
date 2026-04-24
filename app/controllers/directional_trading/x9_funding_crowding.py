"""
X9 — Funding Crowding Reversion (HB V2 Controller)
April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: When funding rate is extreme (z-score > 1) AND persistent (streak >= 2),
the crowded side is overleveraged. Fade them: positive funding -> SHORT,
negative funding -> LONG. OI z-score >= 0 confirms speculative pressure hasn't
unwound yet.

Signal (1h candle with funding_rate + oi_value columns pre-merged):
  Vectorized features at each bar:
  - fr_z: funding rate z-score (rolling 30 bars, ~10 days at 8h funding cadence)
  - oi_z: OI log-delta z-score (rolling 30 bars)
  - streak: consecutive same-sign funding events

  Entry: |fr_z| >= threshold AND streak >= min AND oi_z >= min
  Direction: -sign(funding_rate) (fade the payers)

Exits (ATR-based):
  - SL: 2.5x ATR(14) — sized to survive 95th pctl MAE (~850 bps)
  - TP: 2.0x ATR(14) — captures median MFE (~275 bps) on most pairs
  - Trailing: activate at 1.5x ATR, delta 0.7x ATR
  - Time limit: 48h (6 funding events)
  - Safety clamps: 0.3% floor, 12% ceiling

Phase 0 validation (2026-04-21):
  - 7/8 governance tests passed (MAE addressed with ATR SL)
  - Permutation p=0.0005, walk-forward 3/3 positive
  - Both long and short profitable independently
  - No regime concentration (TREND_UP 47%, TREND_DOWN 50%)
  - DOGE dropout: only 3.5% mean drop (not concentrated)

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


class X9FundingCrowdingConfig(DirectionalTradingControllerConfigBase):
    """Config for X9 Funding Crowding Reversion strategy."""
    controller_name: str = "x9_funding_crowding"

    # ── Signal params ──────────────────────────────────────
    funding_z_threshold: float = Field(
        default=1.0,
        description="Min |funding_rate z-score| to trigger (rolling 30 events)"
    )
    streak_min: int = Field(
        default=2,
        description="Min consecutive same-sign funding events"
    )
    oi_z_min: float = Field(
        default=0.0,
        description="Min OI-delta z-score (0 = no filter, higher = more selective)"
    )
    z_window: int = Field(
        default=30,
        description="Rolling window for z-score computation (in funding events)"
    )
    z_min_periods: int = Field(
        default=15,
        description="Min periods for z-score rolling stats"
    )

    # ── ATR-based dynamic exits ────────────────────────────
    atr_period: int = Field(default=14, description="ATR lookback period")
    tp_atr_mult: float = Field(default=2.0, description="Take profit in ATR multiples")
    sl_atr_mult: float = Field(default=2.5, description="Stop loss in ATR multiples")
    trailing_act_atr_mult: float = Field(
        default=1.5, description="Trailing stop activation in ATR multiples"
    )
    trailing_delta_atr_mult: float = Field(
        default=0.7, description="Trailing stop delta in ATR multiples"
    )
    time_limit_seconds: int = Field(default=172800, description="48h hold (6 funding events)")

    # ── Safety clamps ──────────────────────────────────────
    exit_pct_floor: float = Field(default=0.003, description="Min exit pct (0.3%)")
    exit_pct_ceiling: float = Field(default=0.12, description="Max exit pct (12%)")

    # ── Static fallbacks ───────────────────────────────────
    fallback_sl_pct: float = Field(default=0.05, description="Fallback SL when ATR unavailable")
    fallback_tp_pct: float = Field(default=0.04, description="Fallback TP when ATR unavailable")

    # ── BTC regime filter ──────────────────────────────────
    # Phase 0 showed edge works in both trend directions, but SHOCK regime
    # is negative (N=8). Disable by default until Phase 1 confirms.
    btc_regime_enabled: bool = Field(default=False, description="Suppress signals opposing BTC trend")
    btc_regime_threshold: float = Field(default=0.0, description="Min |btc_return_4h| to suppress")

    # ── REST API fetch (live mode) ─────────────────────────
    funding_api_url: str = Field(default="https://api.bybit.com", description="Bybit API base URL")
    funding_fetch_limit: int = Field(default=50, description="Number of funding records to fetch")
    oi_fetch_limit: int = Field(default=50, description="Number of OI records to fetch")

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            candles_connector = self.connector_name.replace("_testnet", "").replace("_demo", "")
            # Need enough history for z-score warmup:
            # 30 funding events × 8h/event = 240h, plus margin → 500 bars
            # NOTE: max_records also controls the "candles_buffer" in
            # BacktestingDataProvider.get_candles_feed() which determines
            # whether the cached feed is reused or re-fetched fresh.
            # Too large a value causes re-fetch (losing merged derivatives).
            # 500 bars × 1h = 20 days of buffer — enough for warmup.
            self.candles_config = [
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=500,
                ),
            ]


class X9FundingCrowdingController(DirectionalTradingControllerBase):
    """X9 Funding Crowding Reversion — fade the overleveraged side."""

    def __init__(self, config: X9FundingCrowdingConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_funding_fetch = 0.0
        self._cached_funding: list = []
        self._last_oi_fetch = 0.0
        self._cached_oi: list = []
        super().__init__(config, *args, **kwargs)

    # ── Cooldown override (same pattern as E3) ─────────────

    def can_create_executor(self, signal: int) -> bool:
        """Enforce cooldown after SL/TL exits, not just active executors.

        After a SL closes, the funding signal often persists (it's a slow
        feature). Without post-close cooldown, the bot re-enters immediately
        into the same losing environment. Check both active and closed
        executor timestamps.
        """
        from hummingbot.core.data_type.common import TradeType as TT

        target_side = TT.BUY if signal > 0 else TT.SELL

        # Check active count limit
        active_same_side = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: x.is_active and x.side == target_side,
        )
        if len(active_same_side) >= self.config.max_executors_per_side:
            return False

        # Check cooldown against most recent close OR open on same side
        all_same_side = self.filter_executors(
            executors=self.executors_info,
            filter_func=lambda x: x.side == target_side,
        )
        timestamps = []
        for ex in all_same_side:
            close_ts = getattr(ex, "close_timestamp", None) or 0
            open_ts = getattr(ex, "timestamp", None) or 0
            timestamps.append(max(close_ts, open_ts))

        max_ts = max(timestamps, default=0)
        elapsed = self.market_data_provider.time() - max_ts
        return elapsed > self.config.cooldown_time

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

        if df is None or len(df) < 10:
            self.processed_data["signal"] = 0
            return

        # Check if derivatives columns are pre-merged (backtest mode)
        if "funding_rate" in df.columns and "oi_value" in df.columns:
            df = self._compute_signals_vectorized(df)
        elif "funding_rate" in df.columns:
            # Partial merge (funding only, no OI) — still try vectorized
            df = self._compute_signals_vectorized(df)
        else:
            # Live mode: fetch from Bybit REST API
            funding_rates = self._fetch_funding_live(c.trading_pair)
            oi_values = self._fetch_oi_live(c.trading_pair)
            signal = self._signal_from_live_data(funding_rates, oi_values)
            df = df.copy()
            df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)
            df["signal"] = 0
            if len(df) > 0:
                df.iloc[-1, df.columns.get_loc("signal")] = signal

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

        # Store ATR and price for dynamic exits
        if signal != 0 and len(df) > 0:
            last = df.iloc[-1]
            atr_val = float(last.get("atr", 0) or 0)
            price_val = float(last["close"])
            if atr_val > 0 and price_val > 0:
                self.processed_data["entry_atr"] = atr_val
                self.processed_data["entry_price"] = price_val

    def _compute_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute signal column for every bar (required for BacktestingEngine).

        CRITICAL: funding_rate is forward-filled from 8h events to every 1h bar.
        Computing z-score directly on 1h bars gives wrong results because most
        consecutive bars have identical values. Instead:
        1. Detect funding EVENT boundaries (where funding_rate changes)
        2. Compute z-score and streak on events only
        3. Forward-fill computed features back to all 1h bars
        4. Only trigger signals at event boundaries (first bar of new funding)

        This matches the Codex discovery script's behavior on event timestamps.
        """
        c = self.config
        df = df.copy()
        df["signal"] = 0

        # ATR for dynamic exits
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        fr = df["funding_rate"]
        if fr.isna().all() or (fr == 0).all():
            return df

        # ── Detect funding events (rate changes) ─────────
        # Funding changes every 8h. Detect by shift comparison.
        # Also detect first valid row after NaN.
        fr_changed = fr != fr.shift(1)
        # First row is always an "event"
        fr_changed.iloc[0] = True

        # Create event-only series
        event_mask = fr_changed
        event_fr = fr[event_mask]

        if len(event_fr) < c.z_min_periods:
            return df

        # ── Funding z-score (on events only) ─────────────
        fr_mu = event_fr.rolling(c.z_window, min_periods=c.z_min_periods).mean()
        fr_sd = event_fr.rolling(c.z_window, min_periods=c.z_min_periods).std().replace(0, np.nan)
        event_fr_z = (event_fr - fr_mu) / fr_sd

        # Forward-fill z-score to all bars, fill remaining NaN with 0
        # (neutral: won't trigger |fr_z| >= threshold)
        # CRITICAL: BacktestingEngineBase.prepare_market_data() calls dropna()
        # which kills rows with ANY NaN. Must fill all NaN to preserve data.
        df["fr_z"] = np.nan
        df.loc[event_fr_z.index, "fr_z"] = event_fr_z
        df["fr_z"] = df["fr_z"].ffill().fillna(0.0)

        # ── OI delta z-score (on events only) ────────────
        if "oi_value" in df.columns:
            oi = df["oi_value"].replace(0, np.nan)
            oi_log = np.log(oi).ffill()

            # OI delta at event timestamps
            event_oi_log = oi_log[event_mask]
            event_oi_delta = event_oi_log.diff(1)
            oi_mu = event_oi_delta.rolling(c.z_window, min_periods=c.z_min_periods).mean()
            oi_sd = event_oi_delta.rolling(c.z_window, min_periods=c.z_min_periods).std().replace(0, np.nan)
            event_oi_z = (event_oi_delta - oi_mu) / oi_sd

            df["oi_z"] = np.nan
            df.loc[event_oi_z.index, "oi_z"] = event_oi_z
            df["oi_z"] = df["oi_z"].ffill().fillna(0.0)
        else:
            df["oi_z"] = 0.0

        # ── Funding streak (on events only) ──────────────
        # Count consecutive same-sign funding events
        event_sign = np.sign(event_fr)
        streak_vals = []
        cur = 0
        prev_s = 0
        for s in event_sign.values:
            if s == 0:
                cur = 0
            elif s == prev_s:
                cur += 1
            else:
                cur = 1
            streak_vals.append(cur)
            if s != 0:
                prev_s = s

        event_streak = pd.Series(streak_vals, index=event_fr.index, dtype=float)
        df["streak"] = np.nan
        df.loc[event_streak.index, "streak"] = event_streak
        df["streak"] = df["streak"].ffill().fillna(0)

        # ── Entry conditions (ONLY at event boundaries) ──
        # Signal only fires on the FIRST 1h bar of a new funding event,
        # preventing 8 duplicate signals per event.
        fr_z_abs = df["fr_z"].abs()
        oi_z = df["oi_z"].fillna(-999)

        z_pass = fr_z_abs >= c.funding_z_threshold
        streak_pass = df["streak"] >= c.streak_min
        oi_pass = oi_z >= c.oi_z_min
        is_event = event_mask  # only trigger at event boundaries

        fr_positive = fr > 0
        fr_negative = fr < 0

        # Positive funding + extreme z + streak + OI → SHORT (-1)
        short_trigger = fr_positive & z_pass & streak_pass & oi_pass & is_event
        # Negative funding + extreme z + streak + OI → LONG (1)
        long_trigger = fr_negative & z_pass & streak_pass & oi_pass & is_event

        df.loc[short_trigger, "signal"] = -1
        df.loc[long_trigger, "signal"] = 1

        # ── BTC regime filter (optional) ─────────────────
        if c.btc_regime_enabled and "btc_return_4h" in df.columns:
            btc_ret = df["btc_return_4h"]
            long_against = (df["signal"] == 1) & (btc_ret < -c.btc_regime_threshold)
            short_against = (df["signal"] == -1) & (btc_ret > c.btc_regime_threshold)
            df.loc[long_against | short_against, "signal"] = 0

        return df

    # ── Live mode: REST API fetchers ─────────────────────

    def _fetch_funding_live(self, trading_pair: str) -> list:
        """Fetch funding rate history from Bybit REST API. Cached 5 min."""
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
            logger.warning(f"X9 funding fetch failed for {trading_pair}: {e}")
            return self._cached_funding

    def _fetch_oi_live(self, trading_pair: str) -> list:
        """Fetch OI history from Bybit REST API. Cached 5 min."""
        now = time.time()
        if now - self._last_oi_fetch < 300 and self._cached_oi:
            return self._cached_oi

        c = self.config
        symbol = trading_pair.replace("-", "")
        url = (
            f"{c.funding_api_url}/v5/market/open-interest"
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
            logger.warning(f"X9 OI fetch failed for {trading_pair}: {e}")
            return self._cached_oi

    def _signal_from_live_data(self, funding_rates: list, oi_values: list) -> int:
        """Compute signal from live API data (lists of dicts, newest first)."""
        c = self.config

        if len(funding_rates) < max(c.z_window, c.streak_min):
            return 0

        # Parse funding rates (newest first from API → reverse to chronological)
        rates = [float(r.get("fundingRate", 0)) for r in reversed(funding_rates)]
        fr_series = pd.Series(rates)

        # Z-score
        fr_mu = fr_series.rolling(c.z_window, min_periods=c.z_min_periods).mean()
        fr_sd = fr_series.rolling(c.z_window, min_periods=c.z_min_periods).std().replace(0, np.nan)
        fr_z = ((fr_series - fr_mu) / fr_sd).iloc[-1]

        if pd.isna(fr_z) or abs(fr_z) < c.funding_z_threshold:
            return 0

        # Streak check
        recent = rates[-c.streak_min:]
        if not all(r > 0 for r in recent) and not all(r < 0 for r in recent):
            return 0

        # OI z-score (if available)
        if oi_values and len(oi_values) >= c.z_min_periods:
            oi_vals = [float(o.get("openInterest", 0)) for o in reversed(oi_values)]
            oi_log = pd.Series(oi_vals).replace(0, np.nan).apply(np.log)
            oi_delta = oi_log.diff(1)
            oi_mu = oi_delta.rolling(c.z_window, min_periods=c.z_min_periods).mean()
            oi_sd = oi_delta.rolling(c.z_window, min_periods=c.z_min_periods).std().replace(0, np.nan)
            oi_z = ((oi_delta - oi_mu) / oi_sd).iloc[-1]
            if pd.isna(oi_z) or oi_z < c.oi_z_min:
                return 0

        # Fade crowded side
        return -int(np.sign(rates[-1]))

    # ── Dynamic exits ─────────────────────────────────────

    def _compute_dynamic_exits(self, price: Decimal) -> dict:
        """Compute ATR-based exit percentages."""
        c = self.config

        atr = self.processed_data.get("atr", 0)
        if not atr or (isinstance(atr, float) and np.isnan(atr)):
            atr = self.processed_data.get("entry_atr", 0)
        atr = float(atr) if atr else 0
        entry_price = float(price) if float(price) > 0 else self.processed_data.get("entry_price", 0)

        if atr <= 0 or entry_price <= 0:
            logger.warning("X9: ATR unavailable, using static fallback exits")
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

        logger.info(
            f"X9 dynamic exits: TP={float(tp_pct):.4f} SL={float(sl_pct):.4f} "
            f"trail_act={float(trail_act_pct):.4f} trail_delta={float(trail_delta_pct):.4f} "
            f"(ATR={atr:.4f}, price={entry_price:.2f})"
        )

        return {
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "trailing_stop": trailing_stop,
        }

    # ── Executor config ───────────────────────────────────

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
