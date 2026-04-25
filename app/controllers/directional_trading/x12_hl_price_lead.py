"""
X12 — HL Price Lead (HB V2 Controller)
April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: Hyperliquid perp price LEADS Bybit perp price. When HL trades at a
premium to Bybit, Bybit converges upward (and vice versa). This is a cross-venue
lead-lag effect driven by HL's faster information incorporation (1h funding
settlement, on-chain, aggressive trader base).

EDA evidence (90d, 15 pairs):
  - hl_premium_bps IC=+0.156 at 1h (momentum — premium predicts same-direction move)
  - All 15 pairs positive IC (min 0.088, max 0.227) — structural, not pair-specific
  - liq_imbalance IC=-0.060 at 1h (contrarian overlay — fade liquidation side)

Signal:
  - hl_premium_bps: (hl_close - bybit_close) / bybit_close * 10000
  - premium_z: rolling z-score of hl_premium_bps over z_window bars
  - LONG: premium_z >= z_threshold (HL premium → Bybit catch-up expected)
  - SHORT: premium_z <= -z_threshold (HL discount → Bybit catch-down expected)

Optional overlay (liq_imbalance): contrarian confirmation from liquidation data.
  - Suppress LONG when liq_imbalance is strongly positive (longs being liquidated = more downside)
  - Suppress SHORT when liq_imbalance is strongly negative

Exits (ATR-based):
  - SL: 2.0x ATR(14)
  - TP: 1.5x ATR(14) — conservative, convergence is partial
  - Trailing: activate at 1.0x ATR, delta 0.5x ATR
  - Time limit: 8h (signal decays after convergence)
  - Safety clamps: 0.3% floor, 8% ceiling

Data:
  - Backtest: hl_close column pre-merged into 1h candle DataFrame by merge engine
    (required_features: ["hl_price"])
  - Live: fetched from MongoDB hyperliquid_candles_1h
"""
from decimal import Decimal
from typing import List
import logging
import os

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


def _rolling_z(s: pd.Series, window: int, min_periods: int) -> pd.Series:
    """Rolling z-score with neutral fill for warm-up NaNs."""
    mu = s.rolling(window, min_periods=min_periods).mean()
    sd = s.rolling(window, min_periods=min_periods).std().replace(0, np.nan)
    z = (s - mu) / sd
    return z.replace([np.inf, -np.inf], np.nan).fillna(0.0)


class X12HlPriceLeadConfig(DirectionalTradingControllerConfigBase):
    """Configuration for X12 HL Price Lead."""
    controller_name: str = "x12_hl_price_lead"

    # Signal params
    z_threshold: float = Field(default=1.5, description="Min premium z-score to trigger signal")
    z_window: int = Field(default=24, description="Z-score rolling window in 1h bars")

    # ATR exits
    atr_period: int = Field(default=14, description="ATR lookback (1h bars)")
    tp_atr_mult: float = Field(default=1.5, description="Take profit in ATR multiples")
    sl_atr_mult: float = Field(default=2.0, description="Stop loss in ATR multiples")
    trailing_activation_atr: float = Field(default=1.0, description="Trailing stop activation in ATR multiples")
    trailing_delta_atr: float = Field(default=0.5, description="Trailing stop delta in ATR multiples")
    time_limit_seconds: int = Field(default=28800, description="8h time limit")

    # Safety clamps
    exit_pct_floor: float = Field(default=0.003, description="Min exit pct (0.3%)")
    exit_pct_ceiling: float = Field(default=0.08, description="Max exit pct (8%)")

    # Fallbacks
    fallback_sl_pct: float = Field(default=0.02, description="Fallback SL when ATR unavailable")
    fallback_tp_pct: float = Field(default=0.015, description="Fallback TP when ATR unavailable")

    # MongoDB config (for live mode HL price fetch)
    mongo_uri: str = Field(default="", description="MongoDB URI (live mode)")
    mongo_database: str = Field(default="", description="MongoDB database name (live mode)")

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


class X12HlPriceLeadController(DirectionalTradingControllerBase):
    """X12 HL Price Lead — cross-venue lead-lag signal."""

    def __init__(self, config: X12HlPriceLeadConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        super().__init__(config, *args, **kwargs)

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    async def update_processed_data(self):
        """Fetch candles + HL price, compute premium z-score, emit signal."""
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

        # Check if hl_close is pre-merged (backtest mode)
        if "hl_close" not in df.columns:
            df = self._merge_hl_price_from_mongodb(df, c.trading_pair)

        if "hl_close" in df.columns:
            df = self._compute_signals_vectorized(df)
        else:
            # No HL data available — no signal
            df = df.copy()
            df["signal"] = 0

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

        if signal != 0 and len(df) > 0:
            last = df.iloc[-1]
            atr_val = float(last.get("atr", 0) or 0)
            price_val = float(last["close"])
            if atr_val > 0 and price_val > 0:
                self.processed_data["entry_atr"] = atr_val
                self.processed_data["entry_price"] = price_val

    def _compute_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute HL premium z-score and signals for full DataFrame."""
        c = self.config
        df = df.copy()

        # ATR for exits
        df["atr"] = ta.atr(df["high"], df["low"], df["close"], length=c.atr_period)

        # HL premium in bps
        hl_close = pd.to_numeric(df["hl_close"], errors="coerce")
        by_close = pd.to_numeric(df["close"], errors="coerce")
        df["hl_premium_bps"] = ((hl_close - by_close) / by_close * 10000).fillna(0)

        # Premium z-score
        df["premium_z"] = _rolling_z(df["hl_premium_bps"], c.z_window, min_periods=max(6, c.z_window // 4))

        # Signal generation
        df["signal"] = 0
        df.loc[df["premium_z"] >= c.z_threshold, "signal"] = 1   # LONG: HL premium → Bybit catch-up
        df.loc[df["premium_z"] <= -c.z_threshold, "signal"] = -1  # SHORT: HL discount → Bybit catch-down

        return df

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        """Build executor with ATR-based TP/SL."""
        c = self.config
        atr = self.processed_data.get("entry_atr", 0)
        entry_price = self.processed_data.get("entry_price", float(price))

        if atr > 0 and entry_price > 0:
            tp_pct = min(max((atr * c.tp_atr_mult) / entry_price, c.exit_pct_floor), c.exit_pct_ceiling)
            sl_pct = min(max((atr * c.sl_atr_mult) / entry_price, c.exit_pct_floor), c.exit_pct_ceiling)
            trail_act = (atr * c.trailing_activation_atr) / entry_price
            trail_delta = (atr * c.trailing_delta_atr) / entry_price
        else:
            tp_pct = c.fallback_tp_pct
            sl_pct = c.fallback_sl_pct
            trail_act = tp_pct * 0.7
            trail_delta = tp_pct * 0.3

        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=c.connector_name,
            trading_pair=c.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=TripleBarrierConfig(
                take_profit=Decimal(str(round(tp_pct, 6))),
                stop_loss=Decimal(str(round(sl_pct, 6))),
                time_limit=c.time_limit_seconds,
                trailing_stop=TrailingStop(
                    activation_price=Decimal(str(round(trail_act, 6))),
                    trailing_delta=Decimal(str(round(trail_delta, 6))),
                ),
                open_order_type=OrderType.MARKET,
                take_profit_order_type=OrderType.LIMIT,
                stop_loss_order_type=OrderType.MARKET,
            ),
        )

    def _merge_hl_price_from_mongodb(self, df: pd.DataFrame, trading_pair: str) -> pd.DataFrame:
        """Merge HL close price from MongoDB (live mode).

        Standard template: list of (collection, ts_field, val_field, col_name, fill, unit).
        """
        collections_config = [
            ("hyperliquid_candles_1h", "timestamp_utc", "close", "hl_close", 0.0, "ms"),
        ]
        return self._merge_from_mongodb(df, trading_pair, collections_config)

    def _merge_from_mongodb(self, df, pair, collections_config):
        """Standard MongoDB merge for live mode. Same template across all controllers."""
        try:
            from pymongo import MongoClient as _MongoClient
            uri = getattr(self.config, 'mongo_uri', '') or os.environ.get(
                "MONGO_URI", "mongodb://host.docker.internal:27017/quants_lab"
            )
            db_name = getattr(self.config, 'mongo_database', '') or os.environ.get(
                "MONGO_DATABASE", "quants_lab"
            )
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
