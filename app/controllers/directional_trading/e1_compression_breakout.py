"""
E1 — Compression Breakout Controller (Quants Lab / Hummingbot V2)
V7.1 spec: Phase 1 BTC-USDT, 1h setup + 5m trigger architecture.

Architecture (two layers, deterministic):
─────────────────────────────────────────
Layer 1 — 1h setup detection:
  - ATR percentile < threshold (compression confirmed, full 2160-bar lookback)
  - Price breaks 1h range high/low for first time (breakout event, debounced)
  - Hard filters: Risk-Off, volume floor, short EMA gate
  - Each valid breakout creates a setup row: (direction, breakout_level, start_ts, expiry_ts)

Layer 2 — 5m execution trigger:
  For each 5m bar:
    1. Is there an active setup? (start_ts <= bar_ts < expiry_ts)
    2. Is price on the correct side of breakout_level?
    3. Do all 3 entry-quality checks pass on 5m ATR?
  First 5m bar passing all 3 → signal fires.
  Setup expires after setup_expiry_bars × 5m = default 12 bars (1h).

Key design principle:
  Setups are precomputed as a static table from 1h data.
  5m signal assignment is deterministic — no mutable loop state.
  Reproducible in backtests, debuggable from the features DataFrame.

A1 degradation test (E1V32A1Controller):
  Skip first valid 5m trigger per setup.
  Enter on the second valid 5m trigger within the same setup window.
  Computed deterministically: find all valid triggers per setup, assign to index 1.

V3.2 Canonical params (locked):
  atr_compression_threshold = 0.35
  volume_floor_multiplier   = 1.6
  range_period              = 30
  short_trend_ema_period    = 200
"""

from typing import List, Dict, Any, Optional

import numpy as np
import pandas as pd
import pandas_ta as ta
from pydantic import Field

from hummingbot.data_feed.candles_feed.data_types import CandlesConfig
from hummingbot.strategy_v2.controllers.directional_trading_controller_base import (
    DirectionalTradingControllerBase,
    DirectionalTradingControllerConfigBase,
)

# Entry quality constants — BTC-calibrated per V7.1 Section 6
_GAP_BPS_FLOOR    = 5.0    # bps
_GAP_ATR_MULT_BPS = 0.15   # × ATR_5m expressed in bps


class E1CompressionBreakoutConfig(DirectionalTradingControllerConfigBase):
    """V7.1 Phase 1 config — V3.2 canonical defaults."""
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

    # 5m entry quality (V7.1 Section 6, BTC-calibrated)
    entry_quality_filter: bool = Field(default=True)
    entry_distance_atr_mult: float = Field(default=0.3)      # distance < 0.3 × ATR_5m
    entry_body_atr_mult: float = Field(default=0.5)          # body < 0.5 × ATR_5m
    entry_gap_bps_floor: float = Field(default=5.0)          # max gap in bps
    entry_gap_atr_mult: float = Field(default=0.15)          # min(floor, mult × ATR_5m_bps)

    # Setup expiry: number of 5m bars before a pending setup is cancelled
    # Default = 12 bars = 1h. Entry window ≠ position hold time (time_limit = 24h).
    setup_expiry_bars: int = Field(default=12)               # 12 × 5m = 1h

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            lookback_bars = self.atr_compression_window * 24 + self.atr_period + 50
            max_records = max(lookback_bars, self.range_period + 5,
                              self.risk_off_ema_period + 5)
            self.candles_config = [
                CandlesConfig(
                    connector=self.connector_name,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=max_records,
                )
            ]


class E1CompressionBreakoutController(DirectionalTradingControllerBase):
    """E1 Compression Breakout — V7.1 Phase 1, deterministic 1h+5m architecture."""

    # Which trigger index to use per setup (0 = first, 1 = second for A1)
    _trigger_index: int = 0

    def __init__(self, config: E1CompressionBreakoutConfig, *args, **kwargs):
        self.config = config
        self.atr_lookback_bars = config.atr_compression_window * 24
        self.max_records = max(
            self.atr_lookback_bars + config.atr_period + 50,
            config.range_period + 5,
            config.risk_off_ema_period + 5,
        )
        self._atr5m_period = 14
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
            connector_name=c.connector_name,
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

        # ── 5m data ──
        df_5m = self.market_data_provider.get_candles_df(
            connector_name=c.connector_name,
            trading_pair=c.trading_pair,
            interval="5m",
            max_records=c.setup_expiry_bars * 3 + self._atr5m_period + 10,
        )

        if df_5m is None or len(df_5m) < self._atr5m_period + 2:
            # No 5m data — fall back to 1h signal
            self._build_signal_1h_fallback(df_1h)
            return

        df_5m = self._compute_5m_features(df_5m)

        if setups_df.empty:
            self._finalize(df_5m, 0, "no_valid_1h_setups")
            return

        # Assign signals deterministically
        df_5m = self._assign_5m_signals(df_5m, setups_df)
        last_sig = int(df_5m["signal"].iloc[-1]) if len(df_5m) else 0
        last_rsn = "5m_trigger" if last_sig != 0 else "no_valid_5m_trigger"
        self._finalize(df_5m, last_sig, last_rsn)

    # ── 1h feature computation ────────────────────────────────────────────────

    def _compute_1h_features(self, df: pd.DataFrame) -> pd.DataFrame:
        c = self.config
        df = df.copy()

        # ATR
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        # ATR percentile — full feed, strict
        feed_key = f"{c.connector_name}_{c.trading_pair}_1h"
        df_full = self.market_data_provider.candles_feeds.get(feed_key)
        if df_full is not None and len(df_full) >= self.atr_lookback_bars \
                and "timestamp" in df_full.columns:
            ff = df_full.copy()
            ff["_atr"] = ta.atr(ff["high"], ff["low"], ff["close"], length=c.atr_period)
            ff["_atr_pct"] = ff["_atr"].rolling(self.atr_lookback_bars).rank(pct=True)
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
        expiry_secs = c.setup_expiry_bars * 300  # 5m = 300s

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

            rows.append({
                "direction":      be,
                "breakout_level": bl,
                "start_ts":       bar_ts,
                "expiry_ts":      bar_ts + expiry_secs,
            })

        if not rows:
            return pd.DataFrame(columns=["direction", "breakout_level", "start_ts", "expiry_ts"])

        return pd.DataFrame(rows)

    # ── 5m feature computation ────────────────────────────────────────────────

    def _compute_5m_features(self, df_5m: pd.DataFrame) -> pd.DataFrame:
        df_5m = df_5m.copy()
        df_5m["atr_5m"] = ta.atr(df_5m["high"], df_5m["low"], df_5m["close"],
                                  length=self._atr5m_period)
        if "timestamp" not in df_5m.columns:
            df_5m["timestamp"] = df_5m.index.astype("int64") // 1_000_000_000
        return df_5m

    # ── Deterministic 5m signal assignment ───────────────────────────────────

    def _assign_5m_signals(self, df_5m: pd.DataFrame,
                           setups_df: pd.DataFrame) -> pd.DataFrame:
        """
        For each setup, find all valid 5m trigger bars (deterministic list).
        Assign signal to trigger at index self._trigger_index (0=first, 1=A1 second).

        Fields added to df_5m:
          signal, setup_active, setup_direction, breakout_level,
          setup_age_minutes, trigger_5m_pass, entry_quality_pass,
          entry_quality_fail_reason
        """
        c = self.config
        ts_5m = df_5m["timestamp"].values.astype(float)

        df_5m["signal"]                  = 0
        df_5m["setup_active"]            = 0
        df_5m["setup_direction"]         = 0
        df_5m["breakout_level"]          = float("nan")
        df_5m["setup_age_minutes"]       = float("nan")
        df_5m["trigger_5m_pass"]         = 0
        df_5m["entry_quality_pass"]      = 0
        df_5m["entry_quality_fail_reason"] = ""

        iloc_map = {ts: i for i, ts in enumerate(ts_5m)}

        for _, setup in setups_df.iterrows():
            direction = int(setup["direction"])
            level     = float(setup["breakout_level"])
            start_ts  = float(setup["start_ts"])
            expiry_ts = float(setup["expiry_ts"])

            # All 5m bars in setup window
            mask = (ts_5m >= start_ts) & (ts_5m < expiry_ts)
            window_idx = df_5m.index[mask]

            # Mark setup active
            df_5m.loc[window_idx, "setup_active"]    = 1
            df_5m.loc[window_idx, "setup_direction"] = direction
            df_5m.loc[window_idx, "breakout_level"]  = level

            # Setup age in minutes
            df_5m.loc[window_idx, "setup_age_minutes"] = \
                (ts_5m[mask] - start_ts) / 60.0

            # Find valid triggers deterministically
            valid_triggers = []

            for idx in window_idx:
                row = df_5m.loc[idx]
                atr_5m = row.get("atr_5m", None)
                if atr_5m is None or pd.isna(atr_5m) or atr_5m <= 0:
                    continue

                close_5m = float(row["close"])
                open_5m  = float(row.get("open", close_5m))

                # 5m trigger: price on correct side of breakout level
                if direction == 1 and close_5m <= level:
                    continue
                if direction == -1 and close_5m >= level:
                    continue

                df_5m.loc[idx, "trigger_5m_pass"] = 1

                if not c.entry_quality_filter:
                    df_5m.loc[idx, "entry_quality_pass"] = 1
                    valid_triggers.append(idx)
                    continue

                # Entry quality checks
                distance = abs(close_5m - level)
                body     = abs(close_5m - open_5m)
                gap_bps  = (distance / abs(level)) * 10_000 if level != 0 else 9999.0
                atr_bps  = (atr_5m   / abs(level)) * 10_000 if level != 0 else 9999.0
                gap_thr  = min(c.entry_gap_bps_floor, c.entry_gap_atr_mult * atr_bps)

                dist_ok = distance < c.entry_distance_atr_mult * atr_5m
                body_ok = body     < c.entry_body_atr_mult     * atr_5m
                gap_ok  = gap_bps  < gap_thr

                if dist_ok and body_ok and gap_ok:
                    df_5m.loc[idx, "entry_quality_pass"] = 1
                    valid_triggers.append(idx)
                else:
                    reasons = []
                    if not dist_ok: reasons.append(f"dist>{c.entry_distance_atr_mult}xATR")
                    if not body_ok: reasons.append(f"body>{c.entry_body_atr_mult}xATR")
                    if not gap_ok:  reasons.append(f"gap>{gap_thr:.1f}bps")
                    df_5m.loc[idx, "entry_quality_fail_reason"] = "|".join(reasons)

            # Assign signal to the Nth valid trigger
            if len(valid_triggers) > self._trigger_index:
                fire_idx = valid_triggers[self._trigger_index]
                df_5m.loc[fire_idx, "signal"] = direction

        return df_5m

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

        # Drop string column before numeric select
        df_5m_num = df_5m.drop(columns=["entry_quality_fail_reason"], errors="ignore")
        numeric = df_5m_num.select_dtypes(include="number").copy()
        if "timestamp" not in numeric.columns:
            numeric["timestamp"] = numeric.index.astype("int64") // 1_000_000_000
        self.processed_data["features"] = numeric

    def _emit_empty(self, reason: str):
        self.processed_data["signal"] = 0
        self.processed_data["signal_reason"] = reason
        cols = ["timestamp", "open", "high", "low", "close", "volume",
                "atr_5m", "signal", "setup_active", "setup_direction",
                "breakout_level", "setup_age_minutes",
                "trigger_5m_pass", "entry_quality_pass"]
        self.processed_data["features"] = pd.DataFrame(columns=cols)

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
            "signal":             int(row.get("signal", 0) or 0),
            "setup_active":       int(row.get("setup_active", 0) or 0),
            "setup_direction":    int(row.get("setup_direction", 0) or 0),
            "breakout_level":     float(row.get("breakout_level", 0) or 0),
            "setup_age_minutes":  float(row.get("setup_age_minutes", 0) or 0),
            "trigger_5m_pass":    int(row.get("trigger_5m_pass", 0) or 0),
            "entry_quality_pass": int(row.get("entry_quality_pass", 0) or 0),
            "atr_5m":             float(row.get("atr_5m", 0) or 0),
        }


# ── A1 Degradation Test Controller ───────────────────────────────────────────

class E1V32A1Controller(E1CompressionBreakoutController):
    """
    A1 stress test: miss first valid 5m trigger, enter on second.
    Fully deterministic — uses trigger_index=1 on the precomputed valid triggers list.
    """
    _trigger_index: int = 1
