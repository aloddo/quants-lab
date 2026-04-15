"""
X5 — Liquidation Cascade (HB V2 Controller)
April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: One-sided liquidation spikes reveal forced selling/buying pressure that
creates short-term directional momentum. When longs get wiped (high long-liq
asymmetry + z-score spike), price cascades further down. When shorts get wiped,
price squeezes up. The edge is in RIDING the cascade, not fading it.

Signal (1h candle with liquidation columns pre-merged from Coinalyze daily):
  - Compute rolling z-score of total liquidations (30-bar window on daily data,
    forward-filled to 1h)
  - Compute liquidation asymmetry: long_liq / total_liq
  - CASCADE SHORT: z-score > z_threshold AND asymmetry > asym_long_threshold
    (longs getting wiped -> SHORT to ride the cascade)
  - CASCADE LONG: z-score > z_threshold AND asymmetry < asym_short_threshold
    (shorts getting wiped -> LONG to ride the squeeze)

Conviction-based position sizing:
  - z_multiplier = clip(z_score / 2.0, 0.5, max_z_mult)
  - Higher z-score = higher confidence = larger position
  - Vol regime (20-bar daily return std) scales leverage

Exits (ATR-based):
  - TP: tp_atr_mult x ATR(14) — default 2.0x
  - SL: sl_atr_mult x ATR(14) — default 1.5x (R:R = 1.33)
  - Time limit: 8 hours (hourly EDA shows edge dead by 24h)
  - Safety clamps: 0.3% floor, 10% ceiling

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


class X5LiqCascadeConfig(DirectionalTradingControllerConfigBase):
    """Config for X5 Liquidation Cascade strategy."""
    controller_name: str = "x5_liq_cascade"

    # Liquidation signal params
    z_threshold: float = Field(default=2.0, description="Min total liq z-score to trigger")
    z_window: int = Field(default=720, description="Z-score rolling window in 1h bars (30 days)")
    asym_long_threshold: float = Field(default=0.7, description="Asymmetry above this = long liq dominant -> SHORT")
    asym_short_threshold: float = Field(default=0.3, description="Asymmetry below this = short liq dominant -> LONG")

    # Conviction sizing
    z_base: float = Field(default=2.0, description="Z-score that maps to 1.0x position")
    max_z_mult: float = Field(default=2.0, description="Max z-score multiplier for sizing")

    # ATR exits
    atr_period: int = Field(default=14, description="ATR lookback period")
    tp_atr_mult: float = Field(default=2.0, description="Take profit in ATR multiples")
    sl_atr_mult: float = Field(default=1.5, description="Stop loss in ATR multiples")
    time_limit_seconds: int = Field(default=28800, description="8 hours")

    # Safety clamps
    exit_pct_floor: float = Field(default=0.003, description="Min exit pct (0.3%)")
    exit_pct_ceiling: float = Field(default=0.10, description="Max exit pct (10%)")

    # Fallbacks
    fallback_sl_pct: float = Field(default=0.03, description="Fallback SL when ATR unavailable")
    fallback_tp_pct: float = Field(default=0.02, description="Fallback TP when ATR unavailable")

    # BTC regime filter
    btc_regime_enabled: bool = Field(default=True, description="Suppress signals opposing BTC trend")
    btc_regime_threshold: float = Field(default=0.0, description="Min |btc_return_4h| to suppress")

    # Coinalyze REST API (live mode)
    coinalyze_api_url: str = Field(default="https://api.coinalyze.net/v1", description="Coinalyze base URL")
    coinalyze_api_key_env: str = Field(default="COINALYZE_API_KEY", description="Env var for API key")

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


class X5LiqCascadeController(DirectionalTradingControllerBase):
    """X5 Liquidation Cascade — ride forced liquidation momentum."""

    def __init__(self, config: X5LiqCascadeConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_liq_fetch = 0.0
        self._cached_liq: list = []
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

        if df is None or len(df) < 20:
            self.processed_data["signal"] = 0
            return

        # Check if liquidation columns are pre-merged (backtest mode with BulkBacktestTask)
        if "total_liquidations_usd" not in df.columns:
            # Columns not present — either BacktestingEngine reloaded from parquet
            # (wiping merged data) or we're in live mode. Try MongoDB first (backtest),
            # then Coinalyze REST API (live).
            df = self._merge_liq_from_mongodb(df, c.trading_pair)

        if "total_liquidations_usd" in df.columns:
            df = self._compute_signals_vectorized(df)
        else:
            # Fallback: live mode fetch from Coinalyze REST API
            liq_data = self._fetch_liquidations_live(c.trading_pair)
            df = df.copy()
            df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)
            df["signal"] = 0
            df["z_score"] = 0.0
            if liq_data and len(df) > 0:
                signal, z_score = self._signal_from_liq_data(liq_data)
                df.iloc[-1, df.columns.get_loc("signal")] = signal
                df.iloc[-1, df.columns.get_loc("z_score")] = z_score

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

        # Store ATR, price, and z-score for dynamic exits + sizing
        if signal != 0 and len(df) > 0:
            last = df.iloc[-1]
            atr_val = float(last.get("atr", 0) or 0)
            price_val = float(last["close"])
            z_val = float(last.get("z_score", 0) or 0)
            if atr_val > 0 and price_val > 0:
                self.processed_data["entry_atr"] = atr_val
                self.processed_data["entry_price"] = price_val
                self.processed_data["entry_z_score"] = z_val

    def _merge_liq_from_mongodb(self, df: pd.DataFrame, trading_pair: str) -> pd.DataFrame:
        """Merge liquidation data from MongoDB directly into candle DataFrame.

        BacktestingEngine reloads from parquet on run_backtesting(), wiping any
        pre-merged columns. This method queries MongoDB as a fallback, using the
        same merge pattern as BulkBacktestTask._merge_derivatives_into_candles().

        Only imports pymongo when called (not at module level) to keep the
        controller self-contained for live deployment.
        """
        try:
            from pymongo import MongoClient as _MongoClient
            import os

            mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
            client = _MongoClient(mongo_uri)
            db_name = os.environ.get("MONGO_DATABASE", "quants_lab")
            db = client[db_name]

            pair = trading_pair  # Already in "DOGE-USDT" format
            liq_docs = list(db["coinalyze_liquidations"].find(
                {"pair": pair, "resolution": "daily"}
            ).sort("timestamp_utc", 1))

            if not liq_docs:
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
            logger.warning(f"X5: MongoDB liq merge failed: {e}")
            return df

    def _compute_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute signal column for every bar (required for BacktestingEngine).

        Liquidation columns (daily resolution, forward-filled to 1h) are already
        merged into the DataFrame by BulkBacktestTask.
        """
        c = self.config
        df = df.copy()

        # ATR for exits
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        # Liquidation features
        total_liq = df["total_liquidations_usd"].astype(float)
        long_liq = df["long_liquidations_usd"].astype(float)

        # Asymmetry: fraction of liquidations that are longs
        df["liq_asymmetry"] = long_liq / (total_liq + 1e-10)

        # Z-score of total liquidations (rolling window)
        roll_mean = total_liq.rolling(c.z_window, min_periods=30).mean()
        roll_std = total_liq.rolling(c.z_window, min_periods=30).std()
        df["z_score"] = (total_liq - roll_mean) / (roll_std + 1e-10)

        # BTC regime filter
        btc_filter = pd.Series(True, index=df.index)
        if c.btc_regime_enabled and "btc_return_4h" in df.columns:
            btc_ret = df["btc_return_4h"].astype(float)
            # For SHORT signals: suppress when BTC is strongly bullish
            # For LONG signals: suppress when BTC is strongly bearish
            # Applied per-signal below
            pass  # handled in signal logic

        # Signal generation
        df["signal"] = 0

        z = df["z_score"]
        asym = df["liq_asymmetry"]

        # CASCADE SHORT: longs getting wiped -> price cascades down
        short_signal = (z > c.z_threshold) & (asym > c.asym_long_threshold)

        # CASCADE LONG: shorts getting wiped -> price squeezes up
        long_signal = (z > c.z_threshold) & (asym < c.asym_short_threshold)

        # Apply BTC regime filter if enabled
        if c.btc_regime_enabled and "btc_return_4h" in df.columns:
            btc_ret = df["btc_return_4h"].astype(float)
            # Suppress SHORT when BTC is bullish (btc_ret > threshold)
            short_signal = short_signal & ~(btc_ret > c.btc_regime_threshold)
            # Suppress LONG when BTC is bearish (btc_ret < -threshold)
            long_signal = long_signal & ~(btc_ret < -c.btc_regime_threshold)

        df.loc[short_signal, "signal"] = -1
        df.loc[long_signal, "signal"] = 1

        return df

    def _fetch_liquidations_live(self, trading_pair: str) -> list:
        """Fetch recent liquidation data from Coinalyze REST API (live mode only)."""
        import os

        now = time.time()
        # Cache for 5 min (300s) — liquidation data is daily resolution
        if now - self._last_liq_fetch < 300 and self._cached_liq:
            return self._cached_liq

        api_key = os.environ.get(self.config.coinalyze_api_key_env, "")
        if not api_key:
            logger.warning("X5: No COINALYZE_API_KEY set, cannot fetch live liq data")
            return []

        base = trading_pair.replace("-", "")  # "BTC-USDT" -> "BTCUSDT"
        symbol = f"{base}_PERP.A"
        from_ts = int(now) - 45 * 86400  # 45 days for z-score calculation
        url = (
            f"{self.config.coinalyze_api_url}/liquidation-history"
            f"?api_key={api_key}&symbols={symbol}&interval=daily"
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
            logger.warning(f"X5: Failed to fetch Coinalyze liq: {e}")

        return self._cached_liq  # return stale cache on error

    def _signal_from_liq_data(self, liq_history: list) -> tuple:
        """Compute signal from live liquidation history. Returns (signal, z_score)."""
        c = self.config

        if len(liq_history) < 10:
            return 0, 0.0

        # Build DataFrame from API response
        rows = []
        for h in liq_history:
            rows.append({
                "long_liq": h.get("l", 0),
                "short_liq": h.get("s", 0),
                "total_liq": h.get("l", 0) + h.get("s", 0),
            })
        ldf = pd.DataFrame(rows)

        total = ldf["total_liq"].astype(float)
        if total.std() < 1e-10:
            return 0, 0.0

        # Z-score of latest bar
        z_score = float((total.iloc[-1] - total.mean()) / (total.std() + 1e-10))

        # Asymmetry of latest bar
        latest_total = float(ldf["total_liq"].iloc[-1])
        latest_long = float(ldf["long_liq"].iloc[-1])
        asym = latest_long / (latest_total + 1e-10) if latest_total > 0 else 0.5

        if z_score <= c.z_threshold:
            return 0, z_score

        if asym > c.asym_long_threshold:
            return -1, z_score  # CASCADE SHORT
        elif asym < c.asym_short_threshold:
            return 1, z_score  # CASCADE LONG

        return 0, z_score

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        """Build executor with ATR-based dynamic exits and conviction sizing."""
        c = self.config

        entry_atr = self.processed_data.get("entry_atr", 0)
        entry_price = self.processed_data.get("entry_price", float(price))
        entry_z = self.processed_data.get("entry_z_score", c.z_threshold)

        if entry_atr > 0 and entry_price > 0:
            tp_pct = (c.tp_atr_mult * entry_atr) / entry_price
            sl_pct = (c.sl_atr_mult * entry_atr) / entry_price
            tp_pct = max(c.exit_pct_floor, min(c.exit_pct_ceiling, tp_pct))
            sl_pct = max(c.exit_pct_floor, min(c.exit_pct_ceiling, sl_pct))
        else:
            tp_pct = c.fallback_tp_pct
            sl_pct = c.fallback_sl_pct

        # Conviction-based amount scaling
        z_mult = max(0.5, min(c.max_z_mult, entry_z / c.z_base))
        scaled_amount = Decimal(str(float(amount) * z_mult))

        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            trading_pair=c.trading_pair,
            connector_name=c.connector_name,
            side=trade_type,
            entry_price=price,
            amount=scaled_amount,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=Decimal(str(sl_pct)),
                take_profit=Decimal(str(tp_pct)),
                time_limit=c.time_limit_seconds,
                open_order_type=OrderType.LIMIT,
                take_profit_order_type=OrderType.LIMIT,
                stop_loss_order_type=OrderType.MARKET,
            ),
            leverage=min(5, max(1, int(z_mult * 2))),
        )
