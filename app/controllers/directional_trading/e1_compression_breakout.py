"""
E1 — Compression Breakout Controller (Quants Lab / Hummingbot V2)
V7.2 spec: Phase 1, 1h setup + 1m confirmation architecture.

Architecture (two layers, deterministic):
─────────────────────────────────────────
Layer 1 — 1h setup detection:
  - ATR percentile < threshold (compression confirmed, full 2160-bar lookback)
  - Price breaks 1h range high/low for first time (breakout event, debounced)
  - Hard filters: Risk-Off, volume floor, short EMA gate
  - Each valid breakout creates a setup row: (direction, breakout_level, start_ts, expiry_ts)

Layer 2 — 1m confirmation:
  For each 1m bar within the setup window:
    1. Is price on the correct side of breakout_level?
    2. consecutive_bars counter increments on correct side, resets on wick
  After confirmation_bars (default=2) consecutive correct-side closes → signal fires.
  This gives a ~2-minute confirmation window that filters single-wick false breakouts.
  Works identically in backtest (closed 1m bars) and live (forming last bar = current price).
  Setup expires after setup_expiry_bars × 1m = default 60 bars (1h).

Key design principle:
  Setups are precomputed as a static table from 1h data.
  1m signal assignment is deterministic — no mutable loop state.
  Reproducible in backtests, debuggable from the features DataFrame.

A1 degradation test (E1V32A1Controller):
  Skip first valid setup that fires, enter on the second.
  Implemented via _setup_skip counter: signal only assigned after _setup_skip+1 setups fire.

V3.2 Canonical params (locked):
  atr_compression_threshold = 0.35
  volume_floor_multiplier   = 1.6
  range_period              = 30
  short_trend_ema_period    = 200
"""

from decimal import Decimal
from typing import List, Dict, Any, Optional

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
    TripleBarrierConfig,
)

def _percentile_rank(series: pd.Series, window: int) -> pd.Series:
    """Rolling percentile rank (0-1) of the last value vs the window.

    Inlined from app.features.helpers to keep the controller self-contained
    (no quants-lab imports — required for HB bot deployment).
    """
    def _pct(s):
        if len(s) < 2:
            return float("nan")
        v = s.iloc[-1]
        return (s.iloc[:-1] < v).sum() / (len(s) - 1)
    return series.rolling(window, min_periods=max(100, window // 10)).apply(_pct, raw=False)


class E1CompressionBreakoutConfig(DirectionalTradingControllerConfigBase):
    """V7.2 Phase 1 config — V3.2 canonical defaults, 1m confirmation architecture."""
    controller_name: str = "e1_compression_breakout"

    # ATR compression — 90 DAYS × 24h = 2160 bars
    atr_period: int = Field(default=14)
    atr_compression_window: int = Field(default=90)          # unit: DAYS
    atr_compression_threshold: float = Field(default=0.35)  # V3.2 locked

    # Range breakout (1h)
    range_period: int = Field(default=30)                    # V3.2 locked

    # Volume
    volume_period: int = Field(default=20)
    volume_floor_multiplier: float = Field(default=1.6)      # V3.2 locked
    volume_spike_multiplier: float = Field(default=2.0)

    # Trade direction
    trade_direction: str = Field(default="BOTH")             # "BOTH" | "LONG_ONLY" | "SHORT_ONLY"

    # Short-side EMA gate
    short_trend_filter: bool = Field(default=True)
    short_trend_ema_period: int = Field(default=200)         # V3.2 locked
    short_ema_slope_filter: bool = Field(default=False)
    short_volume_floor_multiplier: float = Field(default=1.6)  # V3.2 locked

    # Risk-Off — local EMA fallback
    risk_off_ema_period: int = Field(default=20)
    risk_off_threshold: float = Field(default=0.97)

    # 1m confirmation: consecutive 1m closes on correct side of breakout level
    # 2 bars = 2-minute window — filters single-wick false breakouts without delay
    confirmation_bars: int = Field(default=2)

    # Setup expiry: number of 1m bars before a pending setup is cancelled
    # Default = 60 bars = 1h. Entry window ≠ position hold time (time_limit = 24h).
    setup_expiry_bars: int = Field(default=60)               # 60 × 1m = 1h

    # Exit mode: "range" (default) | "atr" (legacy) | "fixed" (registry pcts)
    exit_mode: str = Field(default="range")
    min_rr: float = Field(default=1.0)                       # minimum R:R for range mode

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            lookback_bars = self.atr_compression_window * 24 + self.atr_period + 50
            max_1h = max(lookback_bars, self.range_period + 5,
                         self.risk_off_ema_period + 5)
            max_1m = self.setup_expiry_bars + 14 + 10  # expiry window + ATR-14 + buffer
            # Candles use the base connector (public market data) — strip _testnet/_demo
            # suffix since the candles factory only knows the base connector name.
            candles_connector = self.connector_name.replace("_testnet", "").replace("_demo", "")
            self.candles_config = [
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=max_1h,
                ),
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1m",
                    max_records=max_1m,
                ),
            ]


class E1CompressionBreakoutController(DirectionalTradingControllerBase):
    """E1 Compression Breakout — V7.2 Phase 1, deterministic 1h+1m architecture."""

    # Number of fired setups to skip before assigning a signal (0=first, 1=A1 second)
    _setup_skip: int = 0

    def __init__(self, config: E1CompressionBreakoutConfig, *args, **kwargs):
        self.config = config
        self.atr_lookback_bars = config.atr_compression_window * 24
        self.max_records = max(
            self.atr_lookback_bars + config.atr_period + 50,
            config.range_period + 5,
            config.risk_off_ema_period + 5,
        )
        # Candles use the base connector (public market data) — strip _testnet/_demo
        # since the candles factory only knows base connector names.
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    def _get_market_state_risk_off(self) -> Optional[bool]:
        ms = self.processed_data.get("market_state", None)
        return (ms == "Risk-Off Contagion") if ms is not None else None

    # ── Main pipeline ─────────────────────────────────────────────────────────

    async def update_processed_data(self):
        c = self.config

        # ── 1h data ──
        df_1h = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=self.max_records,
        )
        min_viable = max(c.atr_period + c.range_period + c.risk_off_ema_period + 10, 50)
        if df_1h is None or len(df_1h) < min_viable:
            self._emit_empty("insufficient_1h_data")
            return

        df_1h = self._compute_1h_features(df_1h)

        # Build deterministic setup table from 1h features
        setups_df = self._build_setup_table(df_1h)

        # ── 1m data ──
        df_1m = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1m",
            max_records=c.setup_expiry_bars + 14 + 10,
        )

        if df_1m is None or len(df_1m) < 3:
            # No 1m data — fall back to 1h signal
            self._build_signal_1h_fallback(df_1h)
            return

        df_1m = self._compute_1m_features(df_1m)

        if setups_df.empty:
            self._finalize(df_1m, 0, "no_valid_1h_setups")
            return

        # Assign signals deterministically
        df_1m = self._assign_1m_signals(df_1m, setups_df)
        last_sig = int(df_1m["signal"].iloc[-1]) if len(df_1m) else 0
        last_rsn = "1m_confirmation" if last_sig != 0 else "no_1m_confirmation"
        self._finalize(df_1m, last_sig, last_rsn)

    # ── 1h feature computation ────────────────────────────────────────────────

    def _compute_1h_features(self, df: pd.DataFrame) -> pd.DataFrame:
        c = self.config
        df = df.copy()

        # ATR
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        # ATR percentile — full feed, strict
        feed_key = f"{self._candles_connector}_{c.trading_pair}_1h"
        feed_obj = self.market_data_provider.candles_feeds.get(feed_key)
        df_full = feed_obj.candles_df if feed_obj is not None and hasattr(feed_obj, 'candles_df') else feed_obj
        if df_full is not None and len(df_full) >= self.atr_lookback_bars \
                and "timestamp" in df_full.columns:
            ff = df_full.copy()
            ff["_atr"] = ta.atr(ff["high"], ff["low"], ff["close"], length=c.atr_period)
            # Align with live feature: percentile_rank excludes current bar
            ff["_atr_pct"] = _percentile_rank(ff["_atr"], self.atr_lookback_bars)
            atr_map = ff.set_index("timestamp")["_atr_pct"].to_dict()
            key_col = df["timestamp"] if "timestamp" in df.columns \
                else (df.index.astype("int64") // 1_000_000_000)
            df["atr_pct"] = key_col.map(atr_map)
        else:
            df["atr_pct"] = float("nan")

        df["compressed"] = df["atr_pct"] < c.atr_compression_threshold

        # Range high/low — full feed
        if df_full is not None and len(df_full) >= c.range_period + 5 \
                and "timestamp" in df_full.columns:
            rh = df_full["high"].shift(1).rolling(c.range_period).max()
            rl = df_full["low"].shift(1).rolling(c.range_period).min()
            rh_map = dict(zip(df_full["timestamp"], rh))
            rl_map = dict(zip(df_full["timestamp"], rl))
            key_col = df["timestamp"] if "timestamp" in df.columns \
                else (df.index.astype("int64") // 1_000_000_000)
            df["range_high"] = key_col.map(rh_map)
            df["range_low"]  = key_col.map(rl_map)
        else:
            df["range_high"] = float("nan")
            df["range_low"]  = float("nan")

        # Breakout event (first crossing only)
        prior_inside = (df["close"].shift(1) <= df["range_high"].shift(1)) & \
                       (df["close"].shift(1) >= df["range_low"].shift(1))
        lc = prior_inside & (df["close"] > df["range_high"]) & df["compressed"]
        sc = prior_inside & (df["close"] < df["range_low"])  & df["compressed"]
        df["breakout_event"] = 0
        df.loc[lc, "breakout_event"] =  1
        df.loc[sc, "breakout_event"] = -1

        # Volume
        df["vol_ma"]          = df["volume"].rolling(c.volume_period).mean()
        df["vol_ratio"]       = df["volume"] / df["vol_ma"]
        df["volume_floor_ok"] = df["vol_ratio"] > c.volume_floor_multiplier
        df["volume_spike"]    = df["vol_ratio"] > c.volume_spike_multiplier

        # Short EMA gate
        if c.short_trend_filter:
            df["ema_trend"] = ta.ema(df["close"], length=c.short_trend_ema_period)
            below = df["close"] < df["ema_trend"]
            if c.short_ema_slope_filter:
                slope = df["ema_trend"] - df["ema_trend"].shift(3)
                df["short_trend_ok"] = below & (slope <= 0)
            else:
                df["short_trend_ok"] = below
        else:
            df["ema_trend"] = 0.0
            df["short_trend_ok"] = True

        df["short_vol_floor_ok"] = df["vol_ratio"] > c.short_volume_floor_multiplier

        # Risk-Off
        ext = self._get_market_state_risk_off()
        if ext is not None:
            df["ema_risk"] = 0.0
            df["risk_off"] = ext
        else:
            df["ema_risk"] = ta.ema(df["close"], length=c.risk_off_ema_period)
            df["risk_off"] = df["close"] < (df["ema_risk"] * c.risk_off_threshold)

        return df

    # ── Setup table construction ──────────────────────────────────────────────

    def _build_setup_table(self, df_1h: pd.DataFrame) -> pd.DataFrame:
        """
        Precompute all valid 1h setups as a static table.
        Returns DataFrame with columns:
          direction, breakout_level, start_ts, expiry_ts
        """
        c = self.config
        take_longs  = c.trade_direction in ("BOTH", "LONG_ONLY")
        take_shorts = c.trade_direction in ("BOTH", "SHORT_ONLY")
        expiry_secs = c.setup_expiry_bars * 60   # 1m = 60s

        rows = []
        for _, row in df_1h.iterrows():
            be = int(row.get("breakout_event", 0))
            if be == 0:
                continue
            if be == 1 and not take_longs:
                continue
            if be == -1 and not take_shorts:
                continue

            # Hard filters
            if bool(row.get("risk_off", False)):
                continue

            if be == 1:
                vol_ok = bool(row.get("volume_floor_ok", False))
            else:
                vol_ok = bool(row.get("short_vol_floor_ok", False))
            if not vol_ok:
                continue

            if be == -1 and c.short_trend_filter:
                if not bool(row.get("short_trend_ok", True)):
                    continue

            bl = float(row["range_high"]) if be == 1 else float(row["range_low"])
            if pd.isna(bl):
                continue

            if "timestamp" in df_1h.columns:
                bar_ts = float(row["timestamp"])
            else:
                idx = row.name
                bar_ts = float(idx.timestamp() if hasattr(idx, "timestamp") else float(idx) / 1e9)

            atr_val = float(row.get("atr", float("nan")))
            rh = float(row.get("range_high", float("nan")))
            rl = float(row.get("range_low", float("nan")))
            # Candle timestamps are open times (Bybit convention). Breakout is
            # confirmed at the close, so the 1m confirmation window must start
            # AFTER the 1h candle closes (bar_ts + 3600s).
            confirm_start_ts = bar_ts + 3600

            rows.append({
                "direction":      be,
                "breakout_level": bl,
                "atr_1h":         atr_val,
                "range_high":     rh,
                "range_low":      rl,
                "start_ts":       confirm_start_ts,
                "expiry_ts":      confirm_start_ts + expiry_secs,
            })

        if not rows:
            return pd.DataFrame(columns=[
                "direction", "breakout_level", "atr_1h",
                "range_high", "range_low", "start_ts", "expiry_ts",
            ])

        return pd.DataFrame(rows)

    # ── 1m feature computation ────────────────────────────────────────────────

    def _compute_1m_features(self, df_1m: pd.DataFrame) -> pd.DataFrame:
        df_1m = df_1m.copy()
        df_1m["atr_1m"] = ta.atr(df_1m["high"], df_1m["low"], df_1m["close"], length=14)
        if "timestamp" not in df_1m.columns:
            df_1m["timestamp"] = df_1m.index.astype("int64") // 1_000_000_000
        return df_1m

    # ── Deterministic 1m signal assignment ───────────────────────────────────

    def _assign_1m_signals(self, df_1m: pd.DataFrame,
                           setups_df: pd.DataFrame) -> pd.DataFrame:
        """
        For each setup, scan 1m bars within the window.
        Signal fires after confirmation_bars consecutive closes on the correct side.
        Wick (wrong side) resets the consecutive counter.

        Fields added to df_1m:
          signal, setup_active, breakout_level, consecutive_bars,
          tp_price, sl_price
        """
        c = self.config
        ts_1m = df_1m["timestamp"].values.astype(float)

        df_1m["signal"]           = 0
        df_1m["setup_active"]     = 0
        df_1m["breakout_level"]   = float("nan")
        df_1m["consecutive_bars"] = 0
        df_1m["tp_price"]         = float("nan")
        df_1m["sl_price"]         = float("nan")

        exit_mode = getattr(c, "exit_mode", "range")
        min_rr = getattr(c, "min_rr", 1.0)
        fired_count = 0  # total setups that generated a signal (for A1 skip)

        for _, setup in setups_df.iterrows():
            direction  = int(setup["direction"])
            level      = float(setup["breakout_level"])
            atr_1h     = float(setup.get("atr_1h", float("nan")))
            range_high = float(setup.get("range_high", float("nan")))
            range_low  = float(setup.get("range_low", float("nan")))
            start_ts   = float(setup["start_ts"])
            expiry_ts  = float(setup["expiry_ts"])

            mask = (ts_1m >= start_ts) & (ts_1m < expiry_ts)
            window_idx = df_1m.index[mask]
            if len(window_idx) == 0:
                continue

            df_1m.loc[window_idx, "setup_active"]  = 1
            df_1m.loc[window_idx, "breakout_level"] = level

            range_width = (range_high - range_low
                           if not (pd.isna(range_high) or pd.isna(range_low))
                           else float("nan"))
            atr_buffer = 0.5 * atr_1h if (not pd.isna(atr_1h) and atr_1h > 0) else 0.0

            consecutive = 0
            fire_idx = None

            for idx in window_idx:
                close_1m = float(df_1m.loc[idx, "close"])
                correct_side = (
                    (direction == 1 and close_1m > level) or
                    (direction == -1 and close_1m < level)
                )

                if correct_side:
                    consecutive += 1
                    df_1m.loc[idx, "consecutive_bars"] = consecutive

                    if consecutive >= c.confirmation_bars and fire_idx is None:
                        # Compute TP/SL at entry price (current 1m close)
                        tp_p = sl_p = None
                        if exit_mode == "range" and not pd.isna(range_width) and range_width > 0:
                            if direction == 1:
                                tp_p = close_1m + range_width
                                sl_p = level - atr_buffer
                            else:
                                tp_p = close_1m - range_width
                                sl_p = level + atr_buffer
                            tp_dist = abs(tp_p - close_1m)
                            sl_dist = abs(sl_p - close_1m)
                            if sl_dist <= 0 or tp_dist / sl_dist < min_rr:
                                tp_p = sl_p = None  # R:R gate failed
                        elif exit_mode == "atr" and not pd.isna(atr_1h):
                            if direction == 1:
                                tp_p = level + 1.5 * atr_1h
                                sl_p = level - 1.0 * atr_1h
                            else:
                                tp_p = level - 1.5 * atr_1h
                                sl_p = level + 1.0 * atr_1h
                        # "fixed" mode: no tp_p/sl_p → falls back in get_executor_config

                        if tp_p is not None:
                            fire_idx = idx
                            df_1m.loc[idx, "tp_price"] = tp_p
                            df_1m.loc[idx, "sl_price"] = sl_p
                        elif exit_mode == "fixed":
                            fire_idx = idx  # signal without explicit price levels
                else:
                    consecutive = 0  # wick — reset streak

            if fire_idx is not None:
                fired_count += 1
                if fired_count > self._setup_skip:
                    df_1m.loc[fire_idx, "signal"] = direction

        return df_1m

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _build_signal_1h_fallback(self, df_1h: pd.DataFrame):
        """Fallback when 5m feed unavailable: emit signal from 1h features."""
        c = self.config
        take_longs  = c.trade_direction in ("BOTH", "LONG_ONLY")
        take_shorts = c.trade_direction in ("BOTH", "SHORT_ONLY")

        df_1h = df_1h.copy()
        df_1h["signal"] = 0

        bool_cols = df_1h.select_dtypes(include="bool").columns
        df_1h[bool_cols] = df_1h[bool_cols].astype(int)

        ro  = df_1h["risk_off"].astype(bool)
        vf  = df_1h["volume_floor_ok"].astype(bool)
        svf = df_1h["short_vol_floor_ok"].astype(bool)
        sto = df_1h["short_trend_ok"].astype(bool)

        lc = (df_1h["breakout_event"] == 1)  & vf  & ~ro
        sc = (df_1h["breakout_event"] == -1) & svf & ~ro & sto

        if take_longs:  df_1h.loc[lc, "signal"] =  1
        if take_shorts: df_1h.loc[sc, "signal"] = -1

        self.processed_data["signal"] = int(df_1h["signal"].iloc[-1])
        self.processed_data["signal_reason"] = "1h_fallback_no_5m_feed"

        numeric = df_1h.select_dtypes(include="number").copy()
        if "timestamp" not in numeric.columns:
            numeric["timestamp"] = numeric.index.astype("int64") // 1_000_000_000
        self.processed_data["features"] = numeric

    def _finalize(self, df_5m: pd.DataFrame, last_sig: int, reason: str):
        self.processed_data["signal"] = last_sig
        self.processed_data["signal_reason"] = reason

        bool_cols = df_5m.select_dtypes(include="bool").columns
        df_5m[bool_cols] = df_5m[bool_cols].astype(int)

        # Extract dynamic TP/SL from last signal bar for live executor config
        if last_sig != 0 and len(df_5m) > 0:
            last_row = df_5m.iloc[-1]
            tp_val = last_row.get("tp_price")
            sl_val = last_row.get("sl_price")
            if tp_val is not None and not pd.isna(tp_val):
                self.processed_data["tp_price"] = float(tp_val)
            if sl_val is not None and not pd.isna(sl_val):
                self.processed_data["sl_price"] = float(sl_val)

        numeric = df_5m.select_dtypes(include="number").copy()
        if "timestamp" not in numeric.columns:
            numeric["timestamp"] = numeric.index.astype("int64") // 1_000_000_000
        self.processed_data["features"] = numeric

    def _emit_empty(self, reason: str):
        self.processed_data["signal"] = 0
        self.processed_data["signal_reason"] = reason
        cols = ["timestamp", "open", "high", "low", "close", "volume",
                "atr_1m", "signal", "setup_active", "breakout_level",
                "consecutive_bars", "tp_price", "sl_price"]
        self.processed_data["features"] = pd.DataFrame(columns=cols)

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        """Build executor config with dynamic TP/SL.

        Exit modes:
        - range: TP = entry +/- range_width, SL = opposite boundary (R:R gated)
        - atr:   TP = level +/- 1.5×ATR, SL = level -/+ 1.0×ATR
        - fixed: uses config take_profit/stop_loss percentages
        Converts absolute price levels to percentages for TripleBarrierConfig.
        """
        tp_abs = self.processed_data.get("tp_price")
        sl_abs = self.processed_data.get("sl_price")

        if tp_abs is not None and sl_abs is not None and float(price) > 0:
            tp_pct = abs(Decimal(str(tp_abs)) - price) / price
            sl_pct = abs(Decimal(str(sl_abs)) - price) / price

            # Safety: clamp to reasonable bounds (0.1% – 10%)
            tp_pct = max(Decimal("0.001"), min(tp_pct, Decimal("0.10")))
            sl_pct = max(Decimal("0.001"), min(sl_pct, Decimal("0.10")))
        else:
            # Fallback to config defaults
            tp_pct = self.config.take_profit
            sl_pct = self.config.stop_loss

        trailing = getattr(self.config, "trailing_stop", None)
        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=self.config.connector_name,
            trading_pair=self.config.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=sl_pct,
                take_profit=tp_pct,
                time_limit=self.config.time_limit,
                trailing_stop=trailing,
                open_order_type=OrderType.MARKET,
                take_profit_order_type=OrderType.MARKET,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=self.config.leverage,
        )

    def _evaluate_signal(self, df: pd.DataFrame):
        """Live path: read last-bar signal from features."""
        if "signal" in df.columns and len(df) > 0:
            return int(df["signal"].iloc[-1]), self.processed_data.get("signal_reason", "")
        return 0, "no_signal_column"

    def _extract_last_features(self, df: pd.DataFrame) -> Dict[str, Any]:
        if len(df) == 0:
            return {}
        row = df.iloc[-1]
        return {
            "signal":            int(row.get("signal", 0) or 0),
            "setup_active":      int(row.get("setup_active", 0) or 0),
            "breakout_level":    float(row.get("breakout_level", 0) or 0),
            "consecutive_bars":  int(row.get("consecutive_bars", 0) or 0),
            "atr_1m":            float(row.get("atr_1m", 0) or 0),
            "tp_price":          float(row.get("tp_price", 0) or 0),
            "sl_price":          float(row.get("sl_price", 0) or 0),
        }


# ── A1 Degradation Test Controller ───────────────────────────────────────────

class E1V32A1Controller(E1CompressionBreakoutController):
    """
    A1 stress test: skip first setup that fires, enter on the second.
    Fully deterministic — _setup_skip=1 means signal assigned only after 2 setups have fired.
    """
    _setup_skip: int = 1
