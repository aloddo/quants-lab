"""
X10 — Liquidation Exhaustion Reversion (HB V2 Controller)
April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: When total liquidations spike AND the liquidation imbalance is extreme,
forced selling/buying has exhausted the weak side. Fade the imbalance: if longs
were liquidated more (negative imbalance z), the selling pressure is spent and
price is likely to bounce -- go LONG. Vice versa for short liquidations.

This is the OPPOSITE thesis to X5 (which follows cascades). Phase 0 EDA shows
that after 8-12h the exhaustion reversal dominates the cascade momentum.

Signal (1h candle with liquidation columns pre-merged from Coinalyze):
  - liq_total_z: rolling z-score of log(1 + total_liquidations_usd) over 72 bars
  - liq_imb: (long_liq - short_liq) / (total_liq + 1)
  - liq_imb_z: rolling z-score of liq_imb over 72 bars
  - LONG signal:  liq_total_z >= z_total AND liq_imb_z <= -z_imb
    (longs were liquidated more -> exhaustion -> fade by going LONG)
  - SHORT signal: liq_total_z >= z_total AND liq_imb_z >= z_imb
    (shorts were liquidated more -> exhaustion -> fade by going SHORT)

Exits (ATR-based, informed by Phase 0 MAE/MFE analysis):
  - SL: 2.5x ATR(14) -- covers 85th pctl winner MAE (~155 bps)
  - TP: 2.0x ATR(14) -- targets 50th pctl winner MFE (~228 bps)
  - Trailing: activate at 1.5x ATR, delta 0.7x ATR
  - Time limit: 12 hours (matches discovery sweep hold period)
  - Safety clamps: 0.3% floor, 12% ceiling

Phase 0 validation (2026-04-21):
  - 8/8 governance tests passed
  - 18/18 pairs profitable OOS (pooled PF=2.37, WR=62.4%)
  - Both long (+79 bps) and short (+54 bps) profitable independently
  - All 4 regimes profitable (TREND_UP, TREND_DOWN, RANGE, SHOCK)
  - Top-5 trade concentration: 3.4% (extremely distributed)
  - Permutation p=0.0003, walk-forward 4/4 positive

Data:
  - Backtest: liq columns pre-merged into 1h candle DataFrame by BulkBacktestTask
  - Live: fetched from Coinalyze REST API (public, needs API key)
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


# ── Inline helpers (no external imports) ────────────────────────

def _rolling_z(s: pd.Series, window: int, min_periods: int) -> pd.Series:
    """Rolling z-score with neutral fill for warm-up NaNs."""
    mu = s.rolling(window, min_periods=min_periods).mean()
    sd = s.rolling(window, min_periods=min_periods).std().replace(0, np.nan)
    z = (s - mu) / sd
    return z.replace([np.inf, -np.inf], np.nan).fillna(0.0)


# ── Config ──────────────────────────────────────────────────────

class X10LiqExhaustRevertConfig(DirectionalTradingControllerConfigBase):
    """Config for X10 Liquidation Exhaustion Reversion strategy."""
    controller_name: str = "x10_liq_exhaust_revert"

    # ── Signal params (from discovery sweep + Phase 0) ────────
    z_total_threshold: float = Field(
        default=2.0,
        description="Min total liquidation z-score to trigger signal"
    )
    z_imb_threshold: float = Field(
        default=0.5,
        description="Min |imbalance z-score| to confirm directional exhaustion"
    )
    z_window: int = Field(
        default=72,
        description="Rolling z-score window in 1h bars (3 days)"
    )
    z_min_periods: int = Field(
        default=24,
        description="Minimum periods for z-score computation"
    )

    # ── ATR exits (tuned for paper trading v1) ──────────────
    atr_period: int = Field(default=14, description="ATR lookback period")
    tp_atr_mult: float = Field(
        default=1.5,
        description="Take profit — tighter than MAE suggested 2.0x to reduce TIME_LIMIT exits"
    )
    sl_atr_mult: float = Field(
        default=2.0,
        description="Stop loss — 2.0x ATR covers ~75th pctl winner MAE"
    )
    time_limit_seconds: int = Field(
        default=28800,
        description="8 hours — shorter than sweep's 12h to force quicker resolution"
    )

    # ── Trailing stop ─────────────────────────────────────────
    trailing_enabled: bool = Field(default=True, description="Enable trailing stop")
    trailing_activation_atr: float = Field(
        default=1.0,
        description="Activate trailing at 1.0x ATR — lower to actually fire"
    )
    trailing_delta_atr: float = Field(
        default=0.5,
        description="Trailing delta 0.5x ATR — lock in gains once activated"
    )

    # ── Safety clamps ─────────────────────────────────────────
    exit_pct_floor: float = Field(default=0.003, description="Min exit pct (0.3%)")
    exit_pct_ceiling: float = Field(default=0.12, description="Max exit pct (12%)")

    # ── Fallbacks ─────────────────────────────────────────────
    fallback_sl_pct: float = Field(default=0.04, description="Fallback SL when ATR unavailable")
    fallback_tp_pct: float = Field(default=0.03, description="Fallback TP when ATR unavailable")

    # ── MongoDB (live mode — preferred data source) ─────────
    mongo_uri: str = Field(
        default="",
        description="MongoDB URI for liquidation data. Set to mongodb://host.docker.internal:27017/quants_lab in Docker."
    )
    mongo_database: str = Field(
        default="quants_lab",
        description="MongoDB database name"
    )

    # ── Coinalyze REST API (live mode fallback) ──────────────
    coinalyze_api_url: str = Field(
        default="https://api.coinalyze.net/v1",
        description="Coinalyze base URL"
    )
    coinalyze_api_key: str = Field(
        default="",
        description="Coinalyze API key (passed via config for Docker)"
    )
    coinalyze_api_key_env: str = Field(
        default="COINALYZE_API_KEY",
        description="Env var fallback for API key"
    )

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            candles_connector = self.connector_name.replace("_testnet", "").replace("_demo", "")
            self.candles_config = [
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=120,  # 5 days of 1h for z-score warm-up
                ),
            ]


# ── Controller ──────────────────────────────────────────────────

class X10LiqExhaustRevertController(DirectionalTradingControllerBase):
    """X10 Liquidation Exhaustion Reversion — fade spent liquidation cascades."""

    def __init__(self, config: X10LiqExhaustRevertConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_liq_fetch = 0.0
        self._cached_liq: list = []
        self._log_counter = 0  # Rate-limit controller logging
        super().__init__(config, *args, **kwargs)
        logger.info(f"X10 init: pair={config.trading_pair} mongo_uri={'SET' if config.mongo_uri else 'UNSET'}")

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    async def update_processed_data(self):
        c = self.config

        df = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=120,
        )

        self._log_counter += 1
        should_log = (self._log_counter % 60 == 1)  # Log every ~60 ticks

        if df is None or len(df) < 30:
            if should_log:
                logger.warning(f"X10 {c.trading_pair}: insufficient candles ({0 if df is None else len(df)}/30)")
            self.processed_data["signal"] = 0
            return

        # Check if liquidation columns are pre-merged (backtest mode)
        if "total_liquidations_usd" not in df.columns:
            df = self._merge_liq_from_mongodb(df, c.trading_pair)

        if "total_liquidations_usd" in df.columns:
            df = self._compute_signals_vectorized(df)
            if should_log:
                sig = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
                ltz = float(df["liq_total_z"].iloc[-1]) if "liq_total_z" in df.columns else 0
                liz = float(df["liq_imb_z"].iloc[-1]) if "liq_imb_z" in df.columns else 0
                logger.info(f"X10 {c.trading_pair}: MongoDB liq OK, bars={len(df)} sig={sig} liq_z={ltz:.2f} imb_z={liz:.2f}")
        else:
            # Live mode fallback: fetch from Coinalyze REST API
            if should_log:
                logger.warning(f"X10 {c.trading_pair}: no MongoDB liq data, falling back to Coinalyze REST")
            liq_data = self._fetch_liquidations_live(c.trading_pair)
            df = df.copy()
            df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)
            df["signal"] = 0
            if liq_data and len(df) > 0:
                signal = self._signal_from_liq_data(liq_data)
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

        Liquidation columns are already merged into the DataFrame by BulkBacktestTask.
        We compute z-scores of log-total-liquidations and imbalance, then generate
        exhaustion reversal signals.
        """
        c = self.config
        df = df.copy()

        # ATR for exits
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        # Liquidation features
        total_liq = df["total_liquidations_usd"].astype(float).fillna(0.0)
        long_liq = df["long_liquidations_usd"].astype(float).fillna(0.0)
        short_liq = df["short_liquidations_usd"].astype(float).fillna(0.0)

        # Z-score of log(1 + total_liquidations) — log transform for heavy-tailed data
        df["liq_total_z"] = _rolling_z(
            np.log1p(total_liq), c.z_window, c.z_min_periods
        )

        # Imbalance: positive = more longs liquidated, negative = more shorts liquidated
        df["liq_imb"] = (long_liq - short_liq) / (total_liq + 1.0)
        df["liq_imb_z"] = _rolling_z(df["liq_imb"], c.z_window, c.z_min_periods)

        # Signal generation: FADE the exhausted side
        df["signal"] = 0

        ltz = df["liq_total_z"].to_numpy()
        liz = df["liq_imb_z"].to_numpy()

        # Total liq spike AND directional imbalance
        spike = ltz >= c.z_total_threshold

        # LONG: longs were liquidated more (imb_z < -threshold) -> selling exhausted -> bounce
        long_cond = spike & (liz <= -c.z_imb_threshold)
        # SHORT: shorts were liquidated more (imb_z > threshold) -> buying exhausted -> fade
        short_cond = spike & (liz >= c.z_imb_threshold)

        sig = np.zeros(len(df), dtype=np.int8)
        sig[long_cond] = 1
        sig[short_cond] = -1
        df["signal"] = sig

        return df

    def _merge_liq_from_mongodb(self, df: pd.DataFrame, trading_pair: str) -> pd.DataFrame:
        """Merge liquidation data from MongoDB into candle DataFrame.

        BacktestingEngine reloads from parquet, wiping pre-merged columns.
        This queries MongoDB as fallback (same pattern as X5).
        Uses config.mongo_uri if set (for Docker), else falls back to env var.
        """
        try:
            from pymongo import MongoClient as _MongoClient
            import os

            mongo_uri = self.config.mongo_uri or os.environ.get(
                "MONGO_URI", "mongodb://localhost:27017/quants_lab"
            )
            client = _MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
            db_name = self.config.mongo_database or os.environ.get(
                "MONGO_DATABASE", "quants_lab"
            )
            db = client[db_name]

            pair = trading_pair
            liq_docs = list(db["coinalyze_liquidations"].find(
                {"pair": pair}
            ).sort("timestamp_utc", 1))

            if not liq_docs:
                client.close()
                return df

            df = df.copy()
            candle_idx = pd.to_datetime(df["timestamp"], unit="s", utc=True)

            liqdf = pd.DataFrame(liq_docs)
            liqdf["ts"] = pd.to_datetime(liqdf["timestamp_utc"], unit="ms", utc=True)
            liqdf = liqdf.set_index("ts").sort_index()
            liqdf = liqdf[~liqdf.index.duplicated(keep="last")]

            for col in ["long_liquidations_usd", "short_liquidations_usd", "total_liquidations_usd"]:
                if col in liqdf.columns:
                    reindexed = liqdf[[col]].reindex(candle_idx, method="ffill")
                    df[col] = reindexed[col].fillna(0.0).values
                else:
                    df[col] = 0.0

            client.close()
            return df

        except Exception as e:
            logger.warning(f"X10: MongoDB liq merge failed: {e}")
            return df

    def _fetch_liquidations_live(self, trading_pair: str) -> list:
        """Fetch recent liquidation data from Coinalyze REST API (live mode)."""
        import os

        now = time.time()
        # Cache for 5 min — liquidation data updates hourly
        if now - self._last_liq_fetch < 300 and self._cached_liq:
            return self._cached_liq

        api_key = self.config.coinalyze_api_key or os.environ.get(
            self.config.coinalyze_api_key_env, ""
        )
        if not api_key:
            logger.warning("X10: No COINALYZE_API_KEY, cannot fetch live liq data")
            return []

        base = trading_pair.replace("-", "")
        symbol = f"{base}_PERP.A"
        from_ts = int(now) - 10 * 86400  # 10 days for z-score (72 hourly bars)
        url = (
            f"{self.config.coinalyze_api_url}/liquidation-history"
            f"?api_key={api_key}&symbols={symbol}&interval=hourly"
            f"&from={from_ts}&to={int(now)}"
        )

        try:
            req = urllib.request.Request(url, headers={"User-Agent": "quants-lab"})
            resp = urllib.request.urlopen(req, timeout=10)
            data = json.loads(resp.read())
            if data and len(data) > 0:
                history = data[0].get("history", [])
                self._cached_liq = history
                self._last_liq_fetch = now
                return history
        except Exception as e:
            logger.warning(f"X10: Failed to fetch Coinalyze liq: {e}")

        return self._cached_liq

    def _signal_from_liq_data(self, liq_history: list) -> int:
        """Compute signal from live liquidation history."""
        c = self.config

        if len(liq_history) < c.z_min_periods:
            return 0

        rows = []
        for h in liq_history:
            long_l = h.get("l", 0)
            short_l = h.get("s", 0)
            total_l = long_l + short_l
            rows.append({
                "long_liq": long_l,
                "short_liq": short_l,
                "total_liq": total_l,
            })
        ldf = pd.DataFrame(rows)

        total = ldf["total_liq"].astype(float)
        long_l = ldf["long_liq"].astype(float)
        short_l = ldf["short_liq"].astype(float)

        # Z-score of log(1 + total)
        log_total = np.log1p(total)
        mu = log_total.rolling(c.z_window, min_periods=c.z_min_periods).mean()
        sd = log_total.rolling(c.z_window, min_periods=c.z_min_periods).std()
        total_z = ((log_total - mu) / (sd + 1e-10)).iloc[-1]

        # Imbalance z-score
        imb = (long_l - short_l) / (total + 1.0)
        imb_mu = imb.rolling(c.z_window, min_periods=c.z_min_periods).mean()
        imb_sd = imb.rolling(c.z_window, min_periods=c.z_min_periods).std()
        imb_z = ((imb - imb_mu) / (imb_sd + 1e-10)).iloc[-1]

        if total_z < c.z_total_threshold:
            return 0

        if imb_z <= -c.z_imb_threshold:
            return 1   # Longs exhausted -> LONG (fade)
        elif imb_z >= c.z_imb_threshold:
            return -1  # Shorts exhausted -> SHORT (fade)

        return 0

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        """Build executor with ATR-based dynamic exits."""
        c = self.config

        entry_atr = self.processed_data.get("entry_atr", 0)
        entry_price = self.processed_data.get("entry_price", float(price))

        if entry_atr > 0 and entry_price > 0:
            tp_pct = (c.tp_atr_mult * entry_atr) / entry_price
            sl_pct = (c.sl_atr_mult * entry_atr) / entry_price
            tp_pct = max(c.exit_pct_floor, min(c.exit_pct_ceiling, tp_pct))
            sl_pct = max(c.exit_pct_floor, min(c.exit_pct_ceiling, sl_pct))
        else:
            tp_pct = c.fallback_tp_pct
            sl_pct = c.fallback_sl_pct

        # Trailing stop config
        trailing = None
        if c.trailing_enabled and entry_atr > 0 and entry_price > 0:
            activation_pct = (c.trailing_activation_atr * entry_atr) / entry_price
            delta_pct = (c.trailing_delta_atr * entry_atr) / entry_price
            activation_pct = max(c.exit_pct_floor, min(c.exit_pct_ceiling, activation_pct))
            delta_pct = max(c.exit_pct_floor * 0.5, min(c.exit_pct_ceiling * 0.5, delta_pct))
            trailing = TrailingStop(
                activation_price=Decimal(str(activation_pct)),
                trailing_delta=Decimal(str(delta_pct)),
            )

        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            trading_pair=c.trading_pair,
            connector_name=c.connector_name,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=Decimal(str(sl_pct)),
                take_profit=Decimal(str(tp_pct)),
                time_limit=c.time_limit_seconds,
                trailing_stop=trailing,
                open_order_type=OrderType.LIMIT,
                take_profit_order_type=OrderType.LIMIT,
                stop_loss_order_type=OrderType.MARKET,
            ),
            leverage=3,
        )
