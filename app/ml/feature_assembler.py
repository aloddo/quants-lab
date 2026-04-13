"""
Feature assembler: joins backtest_trades with feature data to build ML training datasets.

Reconstructs the full feature vector at each trade entry timestamp by:
1. Loading 1m candle parquet data → microstructure features
2. Querying MongoDB for derivatives at trade time
3. Querying MongoDB for options/liquidation/sentiment at trade time
4. Computing regime features from BTC candles
5. Labeling: net_pnl > 0 → profitable, close_type, net_pnl_pct

Output: a single DataFrame with all features + labels, one row per trade.
"""
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.features.microstructure_features import MicrostructureFeature, MicrostructureConfig
from app.features.helpers import atr, zscore

logger = logging.getLogger(__name__)

# Feature columns produced by this assembler
MICROSTRUCTURE_COLS = [
    "vpin", "kyles_lambda", "amihud_illiq",
    "rv_1m", "rv_5m", "rv_15m", "rv_60m", "rv_240m",
    "rv_noise_ratio", "rv_noise_ratio_roc",
    "vp_poc_distance", "vp_vol_skew",
]

DERIVATIVES_COLS = [
    "funding_rate", "oi_value", "buy_ratio", "binance_funding_rate",
    "funding_spread",
]

OPTIONS_COLS = [
    "skew_25d", "term_structure_slope", "net_gex",
    "max_pain_distance", "put_call_oi_ratio", "dvol_proxy",
]

LIQUIDATION_COLS = [
    "liq_total_usd", "liq_imbalance", "liq_cascade_active",
    "organic_oi_change", "cg_oi_change_pct",
]

SENTIMENT_COLS = [
    "fear_greed_value",
]

REGIME_COLS = [
    "btc_sma50_above_sma200", "btc_atr_percentile",
    "btc_return_20bar", "btc_return_4h",
]

LABEL_COLS = [
    "profitable", "net_pnl_pct", "close_type", "side",
    "engine", "pair",
]

ALL_FEATURE_COLS = (
    MICROSTRUCTURE_COLS + DERIVATIVES_COLS + OPTIONS_COLS
    + LIQUIDATION_COLS + SENTIMENT_COLS + REGIME_COLS
)


class FeatureAssembler:
    """Assembles feature vectors for ML training from backtest trades."""

    def __init__(
        self,
        db: AsyncIOMotorDatabase,
        candle_cache_dir: str = "app/data/cache/candles",
        connector: str = "bybit_perpetual",
    ):
        self.db = db
        self.candle_cache_dir = Path(candle_cache_dir)
        self.connector = connector
        self._micro_feature = MicrostructureFeature()
        self._candle_cache: Dict[str, pd.DataFrame] = {}

    async def build_training_dataset(
        self,
        engines: Optional[List[str]] = None,
        min_trades: int = 30,
    ) -> pd.DataFrame:
        """Build complete training dataset from backtest_trades + feature stores.

        Returns DataFrame with ALL_FEATURE_COLS + LABEL_COLS, one row per trade.
        """
        # 1. Load backtest trades
        trades = await self._load_trades(engines)
        logger.info(f"Loaded {len(trades)} backtest trades")

        if len(trades) < min_trades:
            logger.warning(f"Only {len(trades)} trades, need {min_trades}")
            return pd.DataFrame()

        # 2. Load BTC candles for regime features (pre-compute as time series)
        btc_candles = self._load_candles("BTC-USDT", "1h")
        btc_regime_df = self._precompute_regime(btc_candles)
        logger.info(f"BTC regime: {len(btc_regime_df)} bars pre-computed")

        # 3. Pre-load all derivatives data per pair into indexed DataFrames
        unique_pairs = trades["pair"].unique().tolist()
        logger.info(f"Pre-loading derivatives for {len(unique_pairs)} pairs...")
        pair_funding = await self._bulk_load_collection("bybit_funding_rates", unique_pairs, "funding_rate")
        pair_oi = await self._bulk_load_collection("bybit_open_interest", unique_pairs, "oi_value")
        pair_ls = await self._bulk_load_collection("bybit_ls_ratio", unique_pairs, "buy_ratio")
        pair_binance = await self._bulk_load_collection("binance_funding_rates", unique_pairs, "funding_rate")
        logger.info("Derivatives data pre-loaded into memory")

        # Pre-load sentiment (single time series, not per pair)
        sentiment_df = await self._bulk_load_sentiment()
        logger.info(f"Sentiment data: {len(sentiment_df)} entries")

        # 4. Check if options/liquidation collections have data (skip per-trade queries if empty)
        has_options = await self.db["deribit_options_surface"].count_documents({}, limit=1) > 0
        has_liquidations = await self.db["coinglass_liquidations"].count_documents({}, limit=1) > 0
        has_cg_oi = await self.db["coinglass_oi"].count_documents({}, limit=1) > 0
        logger.info(f"Exotic data: options={has_options}, liquidations={has_liquidations}, cg_oi={has_cg_oi}")

        # 5. Pre-compute microstructure features for each pair (heavy, do before loop)
        pair_micro: Dict[str, pd.DataFrame] = {}
        for pi, pair in enumerate(unique_pairs):
            logger.info(f"  Computing microstructure for {pair} ({pi+1}/{len(unique_pairs)})...")
            candles_1m = self._load_candles(pair, "1m")
            if not candles_1m.empty:
                pair_micro[pair] = self._micro_feature.calculate(candles_1m, skip_volume_profile=True)
            else:
                pair_micro[pair] = pd.DataFrame()
        logger.info(f"Microstructure pre-computed for {len(pair_micro)} pairs")

        # 6. Assemble features for each trade (all lookups are now in-memory)
        rows = []
        n_trades = len(trades)
        for i, (_, trade) in enumerate(trades.iterrows()):
            if i % 10000 == 0:
                logger.info(f"  Assembling trade {i:,}/{n_trades:,} ({i/n_trades*100:.0f}%)")

            pair = trade["pair"]
            entry_ts = trade["entry_timestamp"]

            row = {}

            # Labels
            row["profitable"] = 1.0 if trade["net_pnl_quote"] > 0 else 0.0
            row["net_pnl_pct"] = trade.get("net_pnl_pct", 0)
            row["close_type"] = trade.get("close_type", "UNKNOWN")
            row["side"] = trade.get("side", "BUY")
            row["engine"] = trade.get("engine", "")
            row["pair"] = pair
            row["entry_timestamp"] = entry_ts

            # Microstructure features from pre-computed DataFrames
            micro = self._lookup_micro_at_time(pair, entry_ts, pair_micro)
            row.update(micro)

            # Derivatives from pre-loaded DataFrames (in-memory lookup)
            derivs = self._lookup_derivatives_at_time(
                pair, entry_ts, pair_funding, pair_oi, pair_ls, pair_binance
            )
            row.update(derivs)

            # Options features (skip if collection empty)
            if has_options:
                options = await self._get_options_at_time(pair, entry_ts)
            else:
                options = {col: np.nan for col in OPTIONS_COLS}
            row.update(options)

            # Liquidation features (skip if collections empty)
            if has_liquidations or has_cg_oi:
                liqs = await self._get_liquidation_at_time(pair, entry_ts)
            else:
                liqs = {col: np.nan for col in LIQUIDATION_COLS}
            row.update(liqs)

            # Sentiment from pre-loaded DataFrame
            sentiment = self._lookup_sentiment_at_time(entry_ts, sentiment_df)
            row.update(sentiment)

            # Regime features from pre-computed BTC series
            regime = self._lookup_regime_at_time(entry_ts, btc_regime_df)
            row.update(regime)

            rows.append(row)

        df = pd.DataFrame(rows)
        logger.info(
            f"Assembled {len(df)} feature vectors, "
            f"{df['profitable'].sum():.0f} profitable ({df['profitable'].mean()*100:.1f}%)"
        )
        return df

    async def _load_trades(
        self, engines: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Load backtest trades from MongoDB."""
        query = {}
        if engines:
            query["engine"] = {"$in": engines}

        cursor = self.db["backtest_trades"].find(query)
        docs = await cursor.to_list(length=500000)

        if not docs:
            return pd.DataFrame()

        df = pd.DataFrame(docs)

        # Normalize field names (backtest_trades uses 'timestamp' not 'entry_timestamp')
        if "timestamp" in df.columns and "entry_timestamp" not in df.columns:
            df["entry_timestamp"] = df["timestamp"]
        if "close_timestamp" in df.columns and "exit_timestamp" not in df.columns:
            df["exit_timestamp"] = df["close_timestamp"]

        # Normalize timestamp columns to datetime
        for col in ["entry_timestamp", "exit_timestamp"]:
            if col in df.columns:
                if df[col].dtype == "float64" or df[col].dtype == "int64":
                    if df[col].max() > 1e12:
                        df[col] = pd.to_datetime(df[col], unit="ms", utc=True)
                    else:
                        df[col] = pd.to_datetime(df[col], unit="s", utc=True)

        return df

    def _load_candles(self, pair: str, interval: str) -> pd.DataFrame:
        """Load candle data from parquet cache."""
        cache_key = f"{pair}|{interval}"
        if cache_key in self._candle_cache:
            return self._candle_cache[cache_key]

        pattern = f"{self.connector}|{pair}|{interval}.parquet"
        files = list(self.candle_cache_dir.glob(pattern))
        if not files:
            # Try alternative separator
            pattern2 = f"{self.connector}_{pair}_{interval}.parquet"
            files = list(self.candle_cache_dir.glob(pattern2))

        if not files:
            logger.warning(f"No parquet file for {pair} {interval}")
            return pd.DataFrame()

        df = pd.read_parquet(files[0])
        if "timestamp" in df.columns:
            df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
            df = df.set_index("datetime").sort_index()

        self._candle_cache[cache_key] = df
        return df

    def _lookup_micro_at_time(
        self,
        pair: str,
        timestamp: pd.Timestamp,
        pair_micro: Dict[str, pd.DataFrame],
    ) -> Dict[str, float]:
        """In-memory lookup of pre-computed microstructure features."""
        result = {col: np.nan for col in MICROSTRUCTURE_COLS}

        micro_df = pair_micro.get(pair)
        if micro_df is None or micro_df.empty:
            return result

        idx = micro_df.index.get_indexer([timestamp], method="ffill")
        if idx[0] < 0 or idx[0] >= len(micro_df):
            return result

        row = micro_df.iloc[idx[0]]
        for col in MICROSTRUCTURE_COLS:
            if col in row.index and pd.notna(row[col]):
                result[col] = float(row[col])

        return result

    def _precompute_regime(self, btc_candles: pd.DataFrame) -> pd.DataFrame:
        """Pre-compute BTC regime features as a time series for fast lookup."""
        if btc_candles.empty:
            return pd.DataFrame()

        df = btc_candles.copy()
        close = df["close"]

        sma50 = close.rolling(50).mean()
        sma200 = close.rolling(200).mean()
        df["btc_sma50_above_sma200"] = (sma50 > sma200).astype(float)

        atr_series = atr(df, 14)
        # Rolling percentile rank (vectorized via rank)
        window = min(len(atr_series), 90 * 24)
        df["btc_atr_percentile"] = atr_series.rolling(window, min_periods=200).rank(pct=True)

        df["btc_return_20bar"] = (close / close.shift(20) - 1) * 100
        df["btc_return_4h"] = (close / close.shift(4) - 1) * 100

        return df[["btc_sma50_above_sma200", "btc_atr_percentile",
                    "btc_return_20bar", "btc_return_4h"]].dropna(subset=["btc_sma50_above_sma200"])

    def _lookup_regime_at_time(
        self,
        timestamp: pd.Timestamp,
        btc_regime_df: pd.DataFrame,
    ) -> Dict[str, float]:
        """In-memory ffill lookup of pre-computed BTC regime features."""
        result = {col: np.nan for col in REGIME_COLS}

        if btc_regime_df.empty:
            return result

        mask = btc_regime_df.index <= timestamp
        if not mask.any():
            return result

        row = btc_regime_df.loc[mask].iloc[-1]
        for col in REGIME_COLS:
            if col in row.index and pd.notna(row[col]):
                result[col] = float(row[col])

        return result

    def _get_microstructure_at_time(
        self,
        pair: str,
        timestamp: pd.Timestamp,
        cache: Dict[str, pd.DataFrame],
    ) -> Dict[str, float]:
        """Get microstructure features at a specific time (legacy, use _lookup_micro_at_time)."""
        result = {col: np.nan for col in MICROSTRUCTURE_COLS}

        if pair not in cache:
            candles_1m = self._load_candles(pair, "1m")
            if candles_1m.empty:
                return result
            micro_df = self._micro_feature.calculate(candles_1m)
            cache[pair] = micro_df

        micro_df = cache[pair]
        if micro_df.empty:
            return result

        idx = micro_df.index.get_indexer([timestamp], method="ffill")
        if idx[0] < 0 or idx[0] >= len(micro_df):
            return result

        row = micro_df.iloc[idx[0]]
        for col in MICROSTRUCTURE_COLS:
            if col in row.index and pd.notna(row[col]):
                result[col] = float(row[col])

        return result

    async def _get_derivatives_at_time(
        self, pair: str, timestamp: pd.Timestamp
    ) -> Dict[str, float]:
        """Get derivatives features at trade entry time from MongoDB."""
        result = {col: np.nan for col in DERIVATIVES_COLS}
        ts_ms = int(timestamp.timestamp() * 1000)

        # Funding rate
        doc = await self.db["bybit_funding_rates"].find_one(
            {"pair": pair, "timestamp_utc": {"$lte": ts_ms}},
            sort=[("timestamp_utc", -1)],
        )
        if doc:
            result["funding_rate"] = doc.get("funding_rate", 0)

        # OI
        doc = await self.db["bybit_open_interest"].find_one(
            {"pair": pair, "timestamp_utc": {"$lte": ts_ms}},
            sort=[("timestamp_utc", -1)],
        )
        if doc:
            result["oi_value"] = doc.get("oi_value", 0)

        # LS ratio
        doc = await self.db["bybit_ls_ratio"].find_one(
            {"pair": pair, "timestamp_utc": {"$lte": ts_ms}},
            sort=[("timestamp_utc", -1)],
        )
        if doc:
            result["buy_ratio"] = doc.get("buy_ratio", 0.5)

        # Binance funding
        doc = await self.db["binance_funding_rates"].find_one(
            {"pair": pair, "timestamp_utc": {"$lte": ts_ms}},
            sort=[("timestamp_utc", -1)],
        )
        if doc:
            result["binance_funding_rate"] = doc.get("funding_rate", 0)

        return result

    async def _get_options_at_time(
        self, pair: str, timestamp: pd.Timestamp
    ) -> Dict[str, float]:
        """Get options features at trade time. Uses BTC options as portfolio-level signal."""
        result = {col: np.nan for col in OPTIONS_COLS}
        ts_ms = int(timestamp.timestamp() * 1000)

        # Use BTC options for all pairs (portfolio-level feature)
        # Find the nearest options snapshot before trade time
        cursor = self.db["deribit_options_surface"].find(
            {"currency": "BTC", "timestamp_utc": {"$lte": ts_ms}},
        ).sort("timestamp_utc", -1).limit(500)

        docs = await cursor.to_list(length=500)
        if not docs:
            return result

        # Group by latest timestamp
        latest_ts = docs[0]["timestamp_utc"]
        snapshot = [d for d in docs if d["timestamp_utc"] == latest_ts]

        if not snapshot:
            return result

        from app.features.options_features import OptionsFeature
        feat = OptionsFeature()
        feature = feat.create_feature_from_data("BTC", snapshot)

        for col in OPTIONS_COLS:
            val = feature.value.get(col)
            if val is not None:
                result[col] = float(val)

        return result

    async def _get_liquidation_at_time(
        self, pair: str, timestamp: pd.Timestamp
    ) -> Dict[str, float]:
        """Get liquidation features at trade time."""
        result = {col: np.nan for col in LIQUIDATION_COLS}
        ts_ms = int(timestamp.timestamp() * 1000)

        # Liquidations
        cursor = self.db["coinglass_liquidations"].find(
            {"pair": pair, "timestamp_utc": {"$lte": ts_ms}},
        ).sort("timestamp_utc", -1).limit(24)
        liq_docs = await cursor.to_list(length=24)

        # OI
        cursor = self.db["coinglass_oi"].find(
            {"pair": pair, "timestamp_utc": {"$lte": ts_ms}},
        ).sort("timestamp_utc", -1).limit(24)
        oi_docs = await cursor.to_list(length=24)

        if liq_docs:
            result["liq_total_usd"] = liq_docs[0].get("total_liquidation_usd", 0)
            result["liq_imbalance"] = liq_docs[0].get("liquidation_imbalance", 0)

        if len(liq_docs) >= 3:
            # Simple cascade check
            current = liq_docs[0].get("total_liquidation_usd", 0)
            prev = liq_docs[1].get("total_liquidation_usd", 0)
            result["liq_cascade_active"] = 1.0 if (prev > 0 and current > prev * 2) else 0.0

        if oi_docs and liq_docs and len(oi_docs) >= 2:
            oi_change = oi_docs[0].get("oi_close", 0) - oi_docs[1].get("oi_close", 0)
            liq_vol = liq_docs[0].get("total_liquidation_usd", 0)
            result["organic_oi_change"] = oi_change + liq_vol
            prev_oi = oi_docs[1].get("oi_close", 0)
            if prev_oi > 0:
                result["cg_oi_change_pct"] = (
                    oi_docs[0].get("oi_close", 0) / prev_oi - 1
                ) * 100

        return result

    async def _get_sentiment_at_time(
        self, timestamp: pd.Timestamp
    ) -> Dict[str, float]:
        """Get sentiment features at trade time."""
        result = {col: np.nan for col in SENTIMENT_COLS}
        ts_ms = int(timestamp.timestamp() * 1000)

        doc = await self.db["fear_greed_index"].find_one(
            {"timestamp_utc": {"$lte": ts_ms}},
            sort=[("timestamp_utc", -1)],
        )
        if doc:
            result["fear_greed_value"] = doc.get("value", 50)

        return result

    def _get_regime_at_time(
        self, btc_candles: pd.DataFrame, timestamp: pd.Timestamp
    ) -> Dict[str, float]:
        """Get BTC regime features at trade time."""
        result = {col: np.nan for col in REGIME_COLS}

        if btc_candles.empty:
            return result

        # Find BTC data up to trade time
        mask = btc_candles.index <= timestamp
        if not mask.any():
            return result

        df = btc_candles.loc[mask]
        if len(df) < 200:  # Need enough data for SMA200
            return result

        close = df["close"]
        sma50 = close.rolling(50).mean()
        sma200 = close.rolling(200).mean()

        result["btc_sma50_above_sma200"] = (
            1.0 if sma50.iloc[-1] > sma200.iloc[-1] else 0.0
        )

        # ATR percentile
        atr_series = atr(df, 14)
        if pd.notna(atr_series.iloc[-1]):
            window = min(len(atr_series), 90 * 24)
            rank = (atr_series.iloc[-window:] < atr_series.iloc[-1]).sum() / window
            result["btc_atr_percentile"] = float(rank)

        # Returns
        if len(close) >= 21:
            result["btc_return_20bar"] = float(
                (close.iloc[-1] / close.iloc[-21] - 1) * 100
            )
        if len(close) >= 5:
            result["btc_return_4h"] = float(
                (close.iloc[-1] / close.iloc[-5] - 1) * 100
            )

        return result

    async def _bulk_load_collection(
        self,
        collection_name: str,
        pairs: List[str],
        value_field: str,
    ) -> Dict[str, pd.DataFrame]:
        """Bulk-load a MongoDB collection for all pairs into indexed DataFrames.

        Returns dict of pair → DataFrame with datetime index and value_field column.
        """
        result: Dict[str, pd.DataFrame] = {}

        for pair in pairs:
            cursor = self.db[collection_name].find(
                {"pair": pair},
                {"timestamp_utc": 1, value_field: 1, "_id": 0},
            ).sort("timestamp_utc", 1)
            docs = await cursor.to_list(length=1_000_000)

            if not docs:
                result[pair] = pd.DataFrame(columns=["timestamp_utc", value_field])
                continue

            df = pd.DataFrame(docs)
            df["datetime"] = pd.to_datetime(df["timestamp_utc"], unit="ms", utc=True)
            df = df.set_index("datetime").sort_index()
            result[pair] = df

        return result

    def _lookup_derivatives_at_time(
        self,
        pair: str,
        timestamp: pd.Timestamp,
        pair_funding: Dict[str, pd.DataFrame],
        pair_oi: Dict[str, pd.DataFrame],
        pair_ls: Dict[str, pd.DataFrame],
        pair_binance: Dict[str, pd.DataFrame],
    ) -> Dict[str, float]:
        """In-memory ffill lookup of derivatives features from pre-loaded DataFrames."""
        result = {col: np.nan for col in DERIVATIVES_COLS}

        lookups = [
            (pair_funding, "funding_rate", "funding_rate"),
            (pair_oi, "oi_value", "oi_value"),
            (pair_ls, "buy_ratio", "buy_ratio"),
            (pair_binance, "funding_rate", "binance_funding_rate"),
        ]

        for source, src_col, dst_col in lookups:
            df = source.get(pair)
            if df is None or df.empty or src_col not in df.columns:
                continue
            mask = df.index <= timestamp
            if not mask.any():
                continue
            val = df.loc[mask, src_col].iloc[-1]
            if pd.notna(val):
                result[dst_col] = float(val)

        # Derived: Bybit - Binance funding spread
        if pd.notna(result["funding_rate"]) and pd.notna(result["binance_funding_rate"]):
            result["funding_spread"] = result["funding_rate"] - result["binance_funding_rate"]

        return result

    async def _bulk_load_sentiment(self) -> pd.DataFrame:
        """Load entire fear_greed_index collection into an indexed DataFrame."""
        cursor = self.db["fear_greed_index"].find(
            {}, {"timestamp_utc": 1, "value": 1, "_id": 0}
        ).sort("timestamp_utc", 1)
        docs = await cursor.to_list(length=100_000)

        if not docs:
            return pd.DataFrame(columns=["datetime", "value"])

        df = pd.DataFrame(docs)
        df["datetime"] = pd.to_datetime(df["timestamp_utc"], unit="ms", utc=True)
        df = df.set_index("datetime").sort_index()
        return df

    def _lookup_sentiment_at_time(
        self,
        timestamp: pd.Timestamp,
        sentiment_df: pd.DataFrame,
    ) -> Dict[str, float]:
        """In-memory ffill lookup of sentiment from pre-loaded DataFrame."""
        result = {col: np.nan for col in SENTIMENT_COLS}

        if sentiment_df.empty or "value" not in sentiment_df.columns:
            return result

        mask = sentiment_df.index <= timestamp
        if not mask.any():
            return result

        val = sentiment_df.loc[mask, "value"].iloc[-1]
        if pd.notna(val):
            result["fear_greed_value"] = float(val)

        return result
