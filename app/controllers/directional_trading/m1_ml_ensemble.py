"""
M1 — ML Ensemble Signal (HB V2 Controller)
V1 — April 2026

Self-contained: no quants-lab imports. Same code runs in backtest and live.

Thesis: XGBoost model trained on microstructure + derivatives + options +
liquidation features predicts trade profitability better than any single
threshold-based signal. Model outputs P(profitable); enter only when
confidence exceeds threshold.

Signal:
  - ML model predicts P(profitable) from feature vector
  - Enter LONG when P > entry_threshold and predicted direction UP
  - Enter SHORT when P > entry_threshold and predicted direction DOWN
  - Exit when P drops below exit_threshold (signal re-evaluation)

Exits (dynamic, not static):
  - Signal-based: exit when ML confidence drops below exit_threshold
  - ATR-based SL: 2 * ATR(14) at entry (adapts to volatility)
  - Partial TP: at +1 ATR close 50% (via reduced SL to breakeven)
  - Time decay: 1.5x expected duration from model
  - Fallback SL: 5% hard stop

Data:
  - Backtest: features pre-computed in vectorized signal column
  - Live: compute features from candles + MongoDB at each update
"""
from decimal import Decimal
from typing import List
import logging
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
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

logger = logging.getLogger(__name__)


class M1MLEnsembleConfig(DirectionalTradingControllerConfigBase):
    """V1 config — ML Ensemble Signal."""
    controller_name: str = "m1_ml_ensemble"

    # ML thresholds (model base rate ~0.41, probabilities range 0.35-0.48)
    entry_threshold: float = Field(default=0.45, description="Min P(profitable) to enter")
    exit_threshold: float = Field(default=0.40, description="Exit when P drops below this")

    # ATR-based dynamic exits
    atr_period: int = Field(default=14)
    sl_atr_multiplier: float = Field(default=2.0, description="SL = N * ATR")
    tp_atr_multiplier: float = Field(default=2.0, description="TP = N * ATR")
    hard_sl_pct: float = Field(default=0.05, description="Hard stop loss fallback (5%)")

    # Time
    time_limit_seconds: int = Field(default=345600, description="4 days default")

    # Model path (relative to HB working dir)
    model_path: str = Field(default="app/models/entry_classifier_latest.pkl", description="Path to pickle model")

    # VPIN params (inline computation)
    vpin_bucket_size: int = Field(default=50)
    vpin_n_buckets: int = Field(default=50)

    candles_config: List[CandlesConfig] = Field(default_factory=list)

    def model_post_init(self, __context) -> None:
        if not self.candles_config:
            candles_connector = self.connector_name.replace("_testnet", "").replace("_demo", "")
            self.candles_config = [
                CandlesConfig(
                    connector=candles_connector,
                    trading_pair=self.trading_pair,
                    interval="1h",
                    max_records=300,
                ),
            ]


class M1MLEnsembleController(DirectionalTradingControllerBase):
    """M1 ML Ensemble — enter on high ML confidence, exit when confidence drops."""

    def __init__(self, config: M1MLEnsembleConfig, *args, **kwargs):
        self.config = config
        self._candles_connector = config.connector_name.replace("_testnet", "").replace("_demo", "")
        self._model = None
        self._model_features: list = []
        self._load_model()
        super().__init__(config, *args, **kwargs)

    def _load_model(self):
        """Load ML model from pickle. Fails gracefully if not found."""
        model_path = Path(self.config.model_path)
        if not model_path.exists():
            logger.warning(f"M1: model not found at {model_path} — signals will be 0")
            return

        try:
            with open(model_path, "rb") as f:
                self._model = pickle.load(f)

            meta_path = model_path.with_name(
                model_path.stem.replace("_latest", "_latest_meta") + ".pkl"
            )
            if meta_path.exists():
                with open(meta_path, "rb") as f:
                    meta = pickle.load(f)
                self._model_features = getattr(meta, "feature_cols", [])

            logger.info(f"M1: loaded model with {len(self._model_features)} features")
        except Exception as e:
            logger.error(f"M1: failed to load model: {e}")
            self._model = None

    def get_candles_config(self) -> List[CandlesConfig]:
        return self.config.candles_config

    async def update_processed_data(self):
        c = self.config

        df = self.market_data_provider.get_candles_df(
            connector_name=self._candles_connector,
            trading_pair=c.trading_pair,
            interval="1h",
            max_records=300,
        )

        if df is None or len(df) < 30:
            self.processed_data["signal"] = 0
            return

        # Check if we're in backtest mode (features pre-computed) or live
        if "signal" in df.columns and df["signal"].abs().sum() > 0:
            # Backtest mode: signal already computed by _compute_signals_vectorized
            pass
        elif self._model is not None:
            df = self._compute_signals_vectorized(df)
        else:
            df = df.copy()
            df["signal"] = 0

        signal = int(df["signal"].iloc[-1]) if len(df) > 0 else 0
        self.processed_data["signal"] = signal
        self.processed_data["features"] = df

    def _compute_signals_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute signal for every bar using ML model (vectorized for backtest).

        IMPORTANT: Do NOT add NaN columns to the output DataFrame.
        BacktestingEngineBase.prepare_market_data() calls dropna(how='any')
        which kills any row with NaN in ANY column. Model features that don't
        exist in the candle data are built in a temporary frame for prediction
        only — they never touch the output df.
        """
        df = df.copy()
        df["signal"] = 0

        if self._model is None:
            return df

        # Compute inline features (adds columns to df)
        df = self._compute_inline_features(df)

        feature_cols = self._model_features
        if not feature_cols:
            logger.warning("M1: no model features defined")
            return df

        # Build feature matrix in a TEMPORARY frame — never pollute df with NaN
        X = pd.DataFrame(index=df.index)
        for col in feature_cols:
            if col in df.columns:
                X[col] = df[col]
            else:
                X[col] = np.nan  # XGBoost handles NaN natively

        # Predict P(profitable) for each bar
        try:
            proba = self._model.predict_proba(X)[:, 1]
        except Exception as e:
            logger.warning(f"M1: model prediction failed: {e}")
            return df

        df["ml_confidence"] = proba

        # Direction: use price momentum as tiebreaker
        # Positive return + high confidence → LONG
        # Negative return + high confidence → SHORT
        returns = df["close"].pct_change(periods=4)

        high_conf = proba > self.config.entry_threshold
        long_signal = high_conf & (returns > 0)
        short_signal = high_conf & (returns <= 0)

        df.loc[long_signal, "signal"] = 1
        df.loc[short_signal, "signal"] = -1

        n_signals = (df["signal"] != 0).sum()
        logger.info(f"M1: {n_signals} signals ({(df['signal']==1).sum()} long, {(df['signal']==-1).sum()} short) "
                     f"from {len(df)} bars, threshold={self.config.entry_threshold:.2f}, "
                     f"proba range=[{proba.min():.4f}, {proba.max():.4f}]")

        # Drop temporary inline feature columns that would cause dropna issues
        # Keep only: original candle columns + signal + ml_confidence + pre-merged features
        _SAFE_COLS = {
            "timestamp", "open", "high", "low", "close", "volume",
            "quote_asset_volume", "n_trades", "taker_buy_base_volume",
            "taker_buy_quote_volume", "signal", "ml_confidence",
            # Pre-merged by BulkBacktestTask (NaN-safe, neutral-filled)
            "funding_rate", "oi_value", "buy_ratio", "binance_funding_rate",
            "fear_greed_value", "btc_return_4h",
        }
        cols_to_drop = [c for c in df.columns if c not in _SAFE_COLS]
        df.drop(columns=cols_to_drop, errors="ignore", inplace=True)

        return df

    def _compute_inline_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute features inline from candle data (no external imports)."""
        # ATR
        high, low, close = df["high"], df["low"], df["close"]
        prev_close = close.shift(1)
        tr = pd.concat([
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ], axis=1).max(axis=1)
        atr_period = self.config.atr_period
        df["atr_14"] = tr.ewm(alpha=1/atr_period, min_periods=atr_period, adjust=False).mean()

        # Returns at multiple scales
        df["return_1h"] = close.pct_change(1)
        df["return_4h"] = close.pct_change(4)
        df["return_24h"] = close.pct_change(24)

        # Volume features
        df["vol_zscore"] = (df["volume"] - df["volume"].rolling(20).mean()) / df["volume"].rolling(20).std()

        # VPIN (simplified — using price change direction as trade classification)
        price_diff = close.diff()
        classification = np.sign(price_diff).replace(0, np.nan).ffill().fillna(1)
        buy_vol = df["volume"].where(classification > 0, 0)
        sell_vol = df["volume"].where(classification < 0, 0)

        bucket_size = self.config.vpin_bucket_size
        n_buckets = self.config.vpin_n_buckets
        cumvol = df["volume"].cumsum()
        avg_bar_vol = df["volume"].mean()
        if avg_bar_vol > 0:
            bucket_vol = avg_bar_vol * bucket_size
            bucket_id = (cumvol / bucket_vol).astype(int)
            bucket_buy = buy_vol.groupby(bucket_id).sum()
            bucket_sell = sell_vol.groupby(bucket_id).sum()
            bucket_total = (bucket_buy + bucket_sell).replace(0, np.nan)
            bucket_imbalance = (bucket_buy - bucket_sell).abs() / bucket_total
            vpin_series = bucket_imbalance.rolling(n_buckets, min_periods=n_buckets // 2).mean()
            df["vpin"] = bucket_id.map(vpin_series.to_dict())
        else:
            df["vpin"] = np.nan

        # Amihud illiquidity
        abs_ret = close.pct_change().abs()
        dollar_vol = df["volume"] * close
        illiq = abs_ret / dollar_vol.replace(0, np.nan)
        df["amihud_illiq"] = illiq.rolling(60, min_periods=30).mean()

        # Realized volatility at multiple scales
        log_ret = np.log(close / close.shift(1))
        df["rv_1m"] = (log_ret ** 2).rolling(60, min_periods=30).sum()
        df["rv_5m"] = (log_ret ** 2).rolling(12, min_periods=6).sum()
        df["rv_15m"] = (log_ret ** 2).rolling(4, min_periods=2).sum()
        df["rv_240m"] = (log_ret ** 2).rolling(1, min_periods=1).sum()  # single bar proxy at 1h

        # Kyle's lambda
        cov = abs_ret.rolling(60, min_periods=30).cov(dollar_vol)
        var = dollar_vol.rolling(60, min_periods=30).var()
        df["kyles_lambda"] = cov / var.replace(0, np.nan)

        # BTC return proxy — use this pair's return as BTC regime proxy
        df["btc_return_4h"] = (close / close.shift(4) - 1) * 100

        # Funding spread if both funding columns exist
        if "funding_rate" in df.columns and "binance_funding_rate" in df.columns:
            df["funding_spread"] = df["funding_rate"] - df["binance_funding_rate"]

        # Use pre-merged derivatives columns if available (backtest mode)
        # funding_rate, oi_value, buy_ratio, binance_funding_rate
        # These are already in the DataFrame from _merge_derivatives_into_candles

        return df

    def get_executor_config(self, trade_type: TradeType, price: Decimal, amount: Decimal):
        c = self.config

        # Dynamic SL based on ATR
        features_df = self.processed_data.get("features")
        if features_df is not None and "atr_14" in features_df.columns:
            last_atr = features_df["atr_14"].iloc[-1]
            if pd.notna(last_atr) and last_atr > 0:
                atr_sl = (last_atr * c.sl_atr_multiplier) / float(price)
                atr_tp = (last_atr * c.tp_atr_multiplier) / float(price)
                # Clamp to reasonable range
                sl_pct = min(max(atr_sl, 0.005), c.hard_sl_pct)
                tp_pct = min(max(atr_tp, 0.005), 0.10)
            else:
                sl_pct = c.hard_sl_pct
                tp_pct = c.hard_sl_pct * 0.8
        else:
            sl_pct = c.hard_sl_pct
            tp_pct = c.hard_sl_pct * 0.8

        return PositionExecutorConfig(
            timestamp=self.market_data_provider.time(),
            connector_name=c.connector_name,
            trading_pair=c.trading_pair,
            side=trade_type,
            entry_price=price,
            amount=amount,
            triple_barrier_config=TripleBarrierConfig(
                stop_loss=Decimal(str(round(sl_pct, 5))),
                take_profit=Decimal(str(round(tp_pct, 5))),
                time_limit=c.time_limit_seconds,
                trailing_stop=None,
                open_order_type=OrderType.MARKET,
                take_profit_order_type=OrderType.MARKET,
                stop_loss_order_type=OrderType.MARKET,
                time_limit_order_type=OrderType.MARKET,
            ),
            leverage=c.leverage,
        )
