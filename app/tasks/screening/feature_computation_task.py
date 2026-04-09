"""
Feature computation task — runs all FeatureBase subclasses for all pairs in parquet cache.

Reads 1h candles from parquet, computes features, writes to MongoDB via FeatureStorage.
Also queries MongoDB for derivatives data (OI, funding, L/S) for each pair.
"""
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from core.data_paths import data_paths
from core.data_structures.candles import Candles
from core.features.models import Feature
from core.tasks import BaseTask, TaskContext

from app.features.atr_feature import ATRFeature
from app.features.candle_structure_feature import CandleStructureFeature
from app.features.derivatives_feature import DerivativesFeature
from app.features.market_regime_feature import MarketRegimeFeature
from app.features.momentum_feature import MomentumFeature
from app.features.range_feature import RangeFeature
from app.features.volume_feature import VolumeFeature

logger = logging.getLogger(__name__)


from app.tasks.notifying_task import NotifyingTaskMixin


class FeatureComputationTask(NotifyingTaskMixin, BaseTask):
    """Compute features for all pairs in parquet cache and store to MongoDB."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.connector_name = task_config.get("connector_name", "bybit_perpetual")
        self.interval = task_config.get("interval", "1h")
        self.min_bars = task_config.get("min_bars", 30)

        # Initialize features
        self.candle_features = [
            ATRFeature(),
            RangeFeature(),
            VolumeFeature(),
            MomentumFeature(),
            CandleStructureFeature(),
        ]
        self.derivatives_feature = DerivativesFeature()
        self.regime_feature = MarketRegimeFeature()

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for FeatureComputationTask")

    def _discover_pairs(self) -> List[str]:
        """Find all pairs with parquet data for the configured connector+interval."""
        candles_dir = data_paths.candles_dir
        pattern = f"{self.connector_name}|*|{self.interval}.parquet"
        pairs = []
        for f in candles_dir.glob(pattern):
            parts = f.stem.split("|")
            if len(parts) == 3:
                pairs.append(parts[1])
        return sorted(pairs)

    def _load_candles(self, pair: str) -> Optional[Candles]:
        """Load parquet candles for a pair."""
        path = data_paths.candles_dir / f"{self.connector_name}|{pair}|{self.interval}.parquet"
        if not path.exists():
            return None
        df = pd.read_parquet(path)
        if len(df) < self.min_bars:
            return None
        return Candles(
            candles_df=df,
            connector_name=self.connector_name,
            trading_pair=pair,
            interval=self.interval,
        )

    async def _get_derivatives_data(self, pair: str) -> Dict[str, list]:
        """Fetch latest derivatives data from MongoDB for a pair."""
        result = {"oi": [], "funding": [], "ls": []}
        try:
            result["oi"] = await self.mongodb_client.get_documents(
                "bybit_open_interest", {"pair": pair}, limit=10
            )
            result["funding"] = await self.mongodb_client.get_documents(
                "bybit_funding_rates", {"pair": pair}, limit=10
            )
            result["ls"] = await self.mongodb_client.get_documents(
                "bybit_ls_ratio", {"pair": pair}, limit=10
            )
        except Exception as e:
            logger.warning(f"Failed to fetch derivatives for {pair}: {e}")
        return result

    async def _upsert_features(
        self, features: List[Feature], data_timestamp_utc: Optional[datetime] = None
    ) -> int:
        """Upsert features by (feature_name, trading_pair) — no duplicates.

        Stores both the compute time (``timestamp``) and the source data time
        (``data_timestamp_utc``) so downstream tasks can check freshness.
        """
        db = self.mongodb_client.get_database()
        collection = db["features"]
        # Ensure compound index exists
        await collection.create_index(
            [("feature_name", 1), ("trading_pair", 1)],
            unique=False,
        )
        count = 0
        for f in features:
            doc = f.to_mongo()
            if data_timestamp_utc is not None:
                doc["data_timestamp_utc"] = data_timestamp_utc
            doc["computed_at"] = datetime.now(timezone.utc)
            await collection.update_one(
                {"feature_name": f.feature_name, "trading_pair": f.trading_pair},
                {"$set": doc},
                upsert=True,
            )
            count += 1
        return count

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)

        pairs = self._discover_pairs()
        logger.info(f"Found {len(pairs)} pairs with {self.interval} parquet data")

        stats = {"pairs": 0, "features_saved": 0, "errors": 0}
        btc_regime_risk_off = False

        # First pass: compute BTC regime for contagion flag
        btc_candles = self._load_candles("BTC-USDT")
        if btc_candles is not None:
            atr_f = ATRFeature()
            mom_f = MomentumFeature()
            atr_df = atr_f.calculate(btc_candles.data)
            mom_df = mom_f.calculate(btc_candles.data)
            last_atr_pct = atr_df["atr_percentile_90d"].iloc[-1] if pd.notna(atr_df["atr_percentile_90d"].iloc[-1]) else None
            last_ret_4h = mom_df["return_4h"].iloc[-1] if pd.notna(mom_df["return_4h"].iloc[-1]) else None
            if last_ret_4h is not None and last_ret_4h < -0.03:
                btc_regime_risk_off = True
            if last_atr_pct is not None and last_atr_pct > 0.90:
                btc_regime_risk_off = True

        # Get BTC returns for relative strength
        btc_return_4h = None
        btc_return_24h = None
        if btc_candles is not None:
            mom_df = MomentumFeature().calculate(btc_candles.data)
            btc_return_4h = float(mom_df["return_4h"].iloc[-1]) if pd.notna(mom_df["return_4h"].iloc[-1]) else None
            btc_return_24h = float(mom_df["return_24h"].iloc[-1]) if pd.notna(mom_df["return_24h"].iloc[-1]) else None

        # Process all pairs
        for pair in pairs:
            try:
                candles = self._load_candles(pair)
                if candles is None:
                    continue

                features_to_save: List[Feature] = []

                # Candle-based features
                for feat in self.candle_features:
                    try:
                        feature = feat.create_feature(candles)
                        features_to_save.append(feature)
                    except Exception as e:
                        logger.warning(f"Feature {feat.config.name} failed for {pair}: {e}")

                # Derivatives feature (from MongoDB)
                deriv_data = await self._get_derivatives_data(pair)
                pair_mom = MomentumFeature().calculate(candles.data)
                pair_ret_4h = float(pair_mom["return_4h"].iloc[-1]) if pd.notna(pair_mom["return_4h"].iloc[-1]) else None
                pair_ret_24h = float(pair_mom["return_24h"].iloc[-1]) if pd.notna(pair_mom["return_24h"].iloc[-1]) else None

                deriv_feature = self.derivatives_feature.create_feature_from_data(
                    trading_pair=pair,
                    connector_name=self.connector_name,
                    oi_docs=deriv_data["oi"],
                    funding_docs=deriv_data["funding"],
                    ls_docs=deriv_data["ls"],
                    btc_return_4h=btc_return_4h,
                    btc_return_24h=btc_return_24h,
                    pair_return_4h=pair_ret_4h,
                    pair_return_24h=pair_ret_24h,
                )
                features_to_save.append(deriv_feature)

                # Market regime feature
                atr_df = ATRFeature().calculate(candles.data)
                atr_pct = float(atr_df["atr_percentile_90d"].iloc[-1]) if pd.notna(atr_df["atr_percentile_90d"].iloc[-1]) else None
                regime_feature = self.regime_feature.create_feature_from_values(
                    trading_pair=pair,
                    connector_name=self.connector_name,
                    atr_percentile_90d=atr_pct,
                    return_4h=pair_ret_4h,
                    btc_is_risk_off=btc_regime_risk_off,
                )
                features_to_save.append(regime_feature)

                # Determine the data timestamp (latest candle close time)
                data_ts = None
                if "timestamp" in candles.data.columns and not candles.data.empty:
                    data_ts = pd.Timestamp(candles.data["timestamp"].iloc[-1], unit="s", tz="UTC").to_pydatetime()

                # Upsert all features (no duplicates)
                if features_to_save:
                    n = await self._upsert_features(features_to_save, data_timestamp_utc=data_ts)
                    stats["features_saved"] += n

                stats["pairs"] += 1

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Error computing features for {pair}: {e}")

        # ── External features pass ──────────────────────────────
        from app.features import ALL_EXTERNAL_FEATURES
        if ALL_EXTERNAL_FEATURES:
            ext_count = 0
            for ExtFeatureClass in ALL_EXTERNAL_FEATURES:
                try:
                    ext_feature = ExtFeatureClass()
                    for pair in pairs:
                        try:
                            feature = await ext_feature.fetch(pair, self.connector_name)
                            if feature and feature.value:
                                await self._upsert_features([feature])
                                ext_count += 1
                        except Exception as e:
                            logger.warning(f"External feature {ext_feature.config.name}/{pair}: {e}")
                except Exception as e:
                    logger.error(f"External feature class {ExtFeatureClass}: {e}")
            if ext_count:
                stats["features_saved"] += ext_count
                logger.info(f"External features: {ext_count} saved from {len(ALL_EXTERNAL_FEATURES)} sources")

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(
            f"FeatureComputationTask: {stats['pairs']} pairs, "
            f"{stats['features_saved']} features, {stats['errors']} errors in {duration:.1f}s"
        )

        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "stats": stats,
            "btc_risk_off": btc_regime_risk_off,
            "duration_seconds": duration,
        }

    async def on_success(self, context: TaskContext, result) -> None:
        stats = result.result_data.get("stats", {})
        logger.info(f"FeatureComputationTask: {stats['pairs']} pairs, {stats['features_saved']} features")

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"FeatureComputationTask failed: {result.error_message}")
