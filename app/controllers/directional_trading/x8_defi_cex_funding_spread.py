"""
X8 — DeFi-CEX Funding Spread (HB V2 Controller)
April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: Hyperliquid settles funding every 1h vs Bybit/Binance every 8h. This
settlement frequency mismatch means HL funding LEADS CEX funding by ~8h. When
the HL-CEX spread is extreme, it predicts: (a) the direction of next CEX funding
settlement, and (b) on BTC specifically, price direction.

Two signal modes (selected per-pair via config):

MODE A — Carry Timing (default, all pairs):
  When HL cumulative 8h funding diverges from Bybit rate by > z_threshold:
  - Spread z > z_threshold: next Bybit rate will be HIGH → go SHORT to EARN funding
  - Spread z < -z_threshold: next Bybit rate will be LOW/NEGATIVE → go LONG to EARN funding
  Hold through the next 1-2 funding settlements (8-16h).
  Edge: 33 pairs showed r > 0.5 between spread and next Bybit funding change.

MODE B — Directional (BTC only):
  When HL-CEX spread z > z_threshold: DeFi crowd is overleveraged long → SHORT
  When HL-CEX spread z < -z_threshold: DeFi crowd is overleveraged short → LONG
  Hold 24h.
  Edge: BTC SHORT on z>1.5 → 78.2bps avg, 68.2% WR, Sharpe 3.47.

Data:
  - Backtest: HL/Bybit/Binance funding columns pre-merged into candle DataFrame
    by BulkBacktestTask (required_features: ["funding_spread"])
  - Live: fetched from MongoDB (hyperliquid_funding_rates, bybit_funding_rates)
    which are populated by the pipeline every 15min.
"""
from decimal import Decimal
from typing import List
import logging
import time

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


class X8DefiCexFundingSpreadConfig(DirectionalTradingControllerConfigBase):
    """Configuration for X8 DeFi-CEX Funding Spread."""
    controller_name: str = "x8_defi_cex_funding_spread"

    # Signal mode: "carry" (all pairs) or "directional" (BTC only)
    signal_mode: str = Field(default="carry", description="'carry' for funding timing, 'directional' for price prediction")

    # Spread z-score params
    z_threshold: float = Field(default=1.5, description="Min spread z-score to trigger signal")
    z_window: int = Field(default=240, description="Z-score rolling window in 1h bars (30 x 8h = 240h)")

    # ATR exits
    atr_period: int = Field(default=14, description="ATR lookback period (1h bars)")
    tp_atr_mult: float = Field(default=2.5, description="Take profit in ATR multiples")
    sl_atr_mult: float = Field(default=1.5, description="Stop loss in ATR multiples")
    time_limit_seconds: int = Field(default=57600, description="16h for carry, 24h for directional")

    # Safety clamps
    exit_pct_floor: float = Field(default=0.002, description="Min exit pct (0.2%)")
    exit_pct_ceiling: float = Field(default=0.08, description="Max exit pct (8%)")

    # Fallbacks
    fallback_sl_pct: float = Field(default=0.02, description="Fallback SL when ATR unavailable")
    fallback_tp_pct: float = Field(default=0.03, description="Fallback TP when ATR unavailable")

    # Funding data (8h resampling for spread computation)
    funding_resample_hours: int = Field(default=8, description="Resample HL funding to this period")

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


class X8DefiCexFundingSpreadController(DirectionalTradingControllerBase):
    """X8 DeFi-CEX Funding Spread — carry timing + directional."""

    def __init__(self, config: X8DefiCexFundingSpreadConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._last_funding_fetch = 0.0
        self._cached_hl_funding: list = []
        self._cached_bybit_funding: list = []
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    async def update_processed_data(self):
        """Fetch candles + funding data, compute spread, emit signal."""
        c = self.config

        df = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=500,
        )

        if df is None or len(df) < 50:
            self.processed_data["signal"] = 0
            return

        # Check if funding columns are pre-merged (backtest mode)
        if "hl_funding_rate" not in df.columns:
            df = self._merge_funding_from_mongodb(df, c.trading_pair)

        if "hl_funding_rate" in df.columns and "bybit_funding_rate" in df.columns:
            df = self._compute_signals_vectorized(df)
        else:
            # Live mode: fetch from MongoDB cache
            funding_signal = self._compute_signal_live(c.trading_pair)
            df = df.copy()
            df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)
            df["signal"] = 0
            df["spread_z"] = 0.0
            if funding_signal is not None and len(df) > 0:
                df.iloc[-1, df.columns.get_loc("signal")] = funding_signal["signal"]
                df.iloc[-1, df.columns.get_loc("spread_z")] = funding_signal["spread_z"]

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

        # Store ATR and spread_z for dynamic exits
        if signal != 0 and len(df) > 0:
            last = df.iloc[-1]
            atr_val = float(last.get("atr", 0) or 0)
            price_val = float(last["close"])
            spread_z = float(last.get("spread_z", 0) or 0)
            if atr_val > 0 and price_val > 0:
                self.processed_data["entry_atr"] = atr_val
                self.processed_data["entry_price"] = price_val
                self.processed_data["entry_spread_z"] = abs(spread_z)

    def _compute_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute spread z-score and signals for full DataFrame (backtest mode).

        Expects columns: hl_funding_rate, bybit_funding_rate (and optionally
        binance_funding_rate) merged at 1h resolution.
        """
        c = self.config
        df = df.copy()

        # ATR for exits
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        # Resample HL funding to 8h cumulative windows
        # In vectorized mode, compute rolling 8h sum of HL funding
        resample_bars = c.funding_resample_hours  # 8 bars of 1h = 8h
        df["hl_cum"] = df["hl_funding_rate"].rolling(window=resample_bars, min_periods=1).sum()

        # Bybit rate is settled every 8h — forward-fill to hourly
        # In merged data, bybit_funding_rate is already ffilled
        df["bybit_rate"] = df["bybit_funding_rate"]

        # CEX average (use Binance if available)
        if "binance_funding_rate" in df.columns:
            df["cex_avg"] = (df["bybit_rate"] + df["binance_funding_rate"]) / 2
        else:
            df["cex_avg"] = df["bybit_rate"]

        # Spread: HL cumulative 8h - CEX 8h rate
        df["spread"] = df["hl_cum"] - df["cex_avg"]

        # Z-score of spread
        spread_mean = df["spread"].rolling(window=c.z_window, min_periods=30).mean()
        spread_std = df["spread"].rolling(window=c.z_window, min_periods=30).std()
        df["spread_z"] = (df["spread"] - spread_mean) / spread_std.replace(0, np.nan)
        df["spread_z"] = df["spread_z"].fillna(0)

        # Signal generation
        df["signal"] = 0

        if c.signal_mode == "carry":
            # CARRY MODE: position to earn upcoming funding
            # Positive spread (HL > CEX) → next Bybit funding will be high → SHORT to earn
            # Negative spread (HL < CEX) → next Bybit funding will be low/neg → LONG to earn
            df.loc[df["spread_z"] > c.z_threshold, "signal"] = -1  # SHORT
            df.loc[df["spread_z"] < -c.z_threshold, "signal"] = 1  # LONG

        elif c.signal_mode == "directional":
            # DIRECTIONAL MODE (BTC): fade the overleveraged DeFi crowd
            # Positive spread → DeFi longs overleveraged → SHORT
            # Negative spread → DeFi shorts overleveraged → LONG (weaker)
            df.loc[df["spread_z"] > c.z_threshold, "signal"] = -1  # SHORT

            # Only enable LONG if explicitly configured (weak signal)
            # df.loc[df["spread_z"] < -c.z_threshold, "signal"] = 1

        return df

    def _merge_funding_from_mongodb(self, df: pd.DataFrame, trading_pair: str) -> pd.DataFrame:
        """Merge funding rate data from MongoDB into candle DataFrame (live mode).

        Standard template: list of (collection, ts_field, val_field, col_name, fill, unit).
        Same pattern used across all controllers -- see docs/feature_store_handbook.md.
        """
        collections_config = [
            ("hyperliquid_funding_rates", "timestamp_utc", "funding_rate", "hl_funding_rate", 0.0, "ms"),
            ("bybit_funding_rates", "timestamp_utc", "funding_rate", "bybit_funding_rate", 0.0, "ms"),
            ("binance_funding_rates", "timestamp_utc", "funding_rate", "binance_funding_rate", 0.0, "ms"),
        ]
        return self._merge_from_mongodb(df, trading_pair, collections_config)

    def _merge_from_mongodb(self, df, pair, collections_config):
        """Standard MongoDB merge for live mode. Same template across all controllers."""
        try:
            from pymongo import MongoClient as _MongoClient
            import os
            uri = os.environ.get("MONGO_URI", "mongodb://host.docker.internal:27017/quants_lab")
            db_name = os.environ.get("MONGO_DATABASE", "quants_lab")
            client = _MongoClient(uri, serverSelectionTimeoutMS=5000)
            db = client[db_name]
            df = df.copy()
            candle_idx = pd.to_datetime(df["timestamp"], unit="s", utc=True)
            for coll, ts_field, val_field, col_name, fill, unit in collections_config:
                docs = list(db[coll].find({"pair": pair}).sort(ts_field, 1))
                if docs:
                    sdf = pd.DataFrame(docs)
                    sdf["_ts"] = pd.to_datetime(sdf[ts_field], unit=unit, utc=True)
                    sdf = sdf.set_index("_ts")[[val_field]].sort_index()
                    sdf = sdf[~sdf.index.duplicated(keep="last")]
                    df[col_name] = sdf.reindex(candle_idx, method="ffill")[val_field].fillna(fill).values
                else:
                    df[col_name] = fill
            client.close()
        except Exception as e:
            logger.warning(f"MongoDB merge failed for {pair}: {e}")
        return df

    def _compute_signal_live(self, trading_pair: str) -> dict:
        """Compute funding spread signal from MongoDB for live trading.

        Fetches recent HL and Bybit funding, computes 8h cumulative HL,
        calculates spread z-score, returns signal dict.

        Caches for 5 minutes to avoid hammering MongoDB.
        """
        now = time.time()
        if now - self._last_funding_fetch < 300:
            # Use cached values if recent
            if hasattr(self, "_cached_signal"):
                return self._cached_signal
            return None

        try:
            from pymongo import MongoClient as _MongoClient
            import os

            mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
            client = _MongoClient(mongo_uri)
            db_name = os.environ.get("MONGO_DATABASE", "quants_lab")
            db = client[db_name]

            c = self.config
            pair = trading_pair

            # Get last 30 days of HL funding (720 hourly points)
            hl_docs = list(db["hyperliquid_funding_rates"].find(
                {"pair": pair},
                {"_id": 0, "timestamp_utc": 1, "funding_rate": 1},
            ).sort("timestamp_utc", -1).limit(720))

            # Get last 30 days of Bybit funding (90 x 8h points)
            bybit_docs = list(db["bybit_funding_rates"].find(
                {"pair": pair},
                {"_id": 0, "timestamp_utc": 1, "funding_rate": 1},
            ).sort("timestamp_utc", -1).limit(90))

            client.close()

            if len(hl_docs) < 50 or len(bybit_docs) < 10:
                self._last_funding_fetch = now
                return None

            # Build DataFrames
            hl_df = pd.DataFrame(hl_docs)
            hl_df["dt"] = pd.to_datetime(hl_df["timestamp_utc"], unit="ms", utc=True)
            hl_df = hl_df.sort_values("dt").set_index("dt")

            bybit_df = pd.DataFrame(bybit_docs)
            bybit_df["dt"] = pd.to_datetime(bybit_df["timestamp_utc"], unit="ms", utc=True)
            bybit_df = bybit_df.sort_values("dt").set_index("dt")

            # Resample to 8h
            hl_8h = hl_df["funding_rate"].resample("8h").sum()
            bybit_8h = bybit_df["funding_rate"].resample("8h").last()

            merged = pd.DataFrame({"hl": hl_8h, "bybit": bybit_8h}).dropna()
            if len(merged) < 10:
                self._last_funding_fetch = now
                return None

            merged["spread"] = merged["hl"] - merged["bybit"]

            # Z-score
            window = min(30, len(merged) - 1)
            spread_mean = merged["spread"].rolling(window).mean()
            spread_std = merged["spread"].rolling(window).std()
            merged["spread_z"] = (merged["spread"] - spread_mean) / spread_std.replace(0, np.nan)

            latest_z = float(merged["spread_z"].iloc[-1]) if not pd.isna(merged["spread_z"].iloc[-1]) else 0

            # Generate signal
            signal = 0
            if c.signal_mode == "carry":
                if latest_z > c.z_threshold:
                    signal = -1  # SHORT to earn high funding
                elif latest_z < -c.z_threshold:
                    signal = 1  # LONG to earn low/negative funding
            elif c.signal_mode == "directional":
                if latest_z > c.z_threshold:
                    signal = -1  # SHORT — fade overleveraged DeFi longs

            result = {"signal": signal, "spread_z": latest_z}
            self._cached_signal = result
            self._last_funding_fetch = now
            return result

        except Exception as e:
            logger.warning(f"X8: live funding fetch failed: {e}")
            self._last_funding_fetch = now
            return None

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        """Build position executor with dynamic ATR exits."""
        c = self.config

        atr = self.processed_data.get("entry_atr", 0)
        entry_price = self.processed_data.get("entry_price", float(price))
        spread_z = self.processed_data.get("entry_spread_z", 0)

        # Dynamic exits: ATR-based with safety clamps
        if atr > 0 and entry_price > 0:
            tp_pct = min(max(
                (atr * c.tp_atr_mult) / entry_price,
                c.exit_pct_floor,
            ), c.exit_pct_ceiling)
            sl_pct = min(max(
                (atr * c.sl_atr_mult) / entry_price,
                c.exit_pct_floor,
            ), c.exit_pct_ceiling)
        else:
            tp_pct = c.fallback_tp_pct
            sl_pct = c.fallback_sl_pct

        # Time limit: carry=16h, directional=24h
        time_limit = c.time_limit_seconds

        triple_barrier = TripleBarrierConfig(
            stop_loss=Decimal(str(sl_pct)),
            take_profit=Decimal(str(tp_pct)),
            time_limit=time_limit,
            open_order_type=OrderType.LIMIT,
            take_profit_order_type=OrderType.LIMIT,
            stop_loss_order_type=OrderType.MARKET,
        )

        return PositionExecutorConfig(
            timestamp=time.time(),
            trading_pair=c.trading_pair,
            connector_name=c.connector_name,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=triple_barrier,
            leverage=c.leverage if hasattr(c, "leverage") else 1,
        )
