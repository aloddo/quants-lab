"""
Data Retention Task — scheduled cleanup replacing broken TTL indexes.

MongoDB TTL indexes only work on datetime fields. Our collections use Int64 millis
timestamps, so TTL indexes were silently broken and never expired documents. This task
replaces them with explicit deleteMany calls per collection.

Runs daily via pipeline. For each collection: deletes docs older than the configured
retention period. Idempotent, tunable per collection, no schema changes needed.

After first successful run, manually drop the broken TTL indexes:
  db.bybit_funding_rates.dropIndex("timestamp_utc_1")  # etc.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any

from pymongo import MongoClient

from core.tasks import BaseTask, TaskContext

logger = logging.getLogger(__name__)


# Retention config: collection → {field, unit, days}
# field: the timestamp field name in the collection
# unit: "ms" (Int64 millis), "s" (float seconds), "datetime" (Python datetime)
# days: how many days of data to keep
RETENTION_CONFIG: Dict[str, Dict[str, Any]] = {
    # Bybit derivatives — 730 days (2 years). Backtests run up to 365d and need
    # buffer for walk-forward + ffill anchor. DO NOT reduce below 400d.
    "bybit_funding_rates":          {"field": "timestamp_utc", "unit": "ms", "days": 730},
    "bybit_open_interest":          {"field": "timestamp_utc", "unit": "ms", "days": 730},
    "bybit_ls_ratio":               {"field": "timestamp_utc", "unit": "ms", "days": 730},
    # Binance funding — same reasoning as Bybit
    "binance_funding_rates":        {"field": "timestamp_utc", "unit": "ms", "days": 730},
    # Hyperliquid funding (730 days — matches Bybit for cross-venue strategies)
    "hyperliquid_funding_rates":    {"field": "timestamp_utc", "unit": "ms", "days": 730},
    # Hyperliquid candles — 730 days (X12 needs max history, HL API only keeps ~6mo rolling)
    "hyperliquid_candles_1h":       {"field": "timestamp_utc", "unit": "ms", "days": 730},
    # Hyperliquid microstructure (7 days — high volume, low shelf life)
    "hyperliquid_l2_snapshots_1s":  {"field": "timestamp_utc", "unit": "ms", "days": 7},
    "hyperliquid_recent_trades_1s": {"field": "time",          "unit": "ms", "days": 7},
    # Deribit (180 days options, 365 days DVol)
    "deribit_options_surface":      {"field": "timestamp_utc", "unit": "ms", "days": 180},
    "deribit_dvol":                 {"field": "timestamp_utc", "unit": "ms", "days": 365},
    # Coinalyze — 730 days (X10 does 365d backtests, need buffer)
    "coinalyze_liquidations":       {"field": "timestamp_utc", "unit": "ms", "days": 730},
    "coinalyze_oi":                 {"field": "timestamp_utc", "unit": "ms", "days": 730},
    # Signal candidates (365 days)
    "candidates":                   {"field": "timestamp_utc", "unit": "ms", "days": 365},
    # Arb tick-level data (90 days — enough for spread analysis + strategy dev)
    "arb_hl_bybit_perp_snapshots":  {"field": "timestamp",     "unit": "datetime", "days": 90},
    # Arb operational data (shorter retention)
    "arb_opportunities":            {"field": "timestamp",     "unit": "s",  "days": 30},
    "arb_h2_inventory_drift":       {"field": "timestamp",     "unit": "s",  "days": 30},
    "arb_h2_tier_history":          {"field": "timestamp",     "unit": "s",  "days": 90},
    # Task executions (90 days)
    "task_executions":              {"field": "started_at",    "unit": "datetime", "days": 90},
    # Fear & Greed — keep forever (small, daily)
    # fear_greed_index: intentionally excluded
}


class DataRetentionTask(BaseTask):
    """Delete documents older than configured retention per collection.

    Replaces broken MongoDB TTL indexes that silently failed because
    they were created on Int64 millis fields instead of datetime fields.
    """

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config if hasattr(self.config, "config") else config
        self.mongo_uri = task_config.get("mongo_uri", "mongodb://localhost:27017/quants_lab")
        self.mongo_db = task_config.get("mongo_database", "quants_lab")

    async def execute(self, context: TaskContext):
        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]
        now = datetime.now(timezone.utc)

        total_deleted = 0
        results = {}

        for coll_name, cfg in RETENTION_CONFIG.items():
            if coll_name not in db.list_collection_names():
                continue

            field = cfg["field"]
            unit = cfg["unit"]
            days = cfg["days"]
            cutoff_dt = now - timedelta(days=days)

            # Convert cutoff to the collection's timestamp format
            if unit == "ms":
                cutoff_val = int(cutoff_dt.timestamp() * 1000)
            elif unit == "s":
                cutoff_val = cutoff_dt.timestamp()
            elif unit == "datetime":
                cutoff_val = cutoff_dt
            else:
                logger.warning(f"Unknown unit '{unit}' for {coll_name}, skipping")
                continue

            try:
                result = db[coll_name].delete_many({field: {"$lt": cutoff_val}})
                deleted = result.deleted_count
                total_deleted += deleted
                results[coll_name] = {"deleted": deleted, "retention_days": days}
                if deleted > 0:
                    logger.info(f"DataRetention: {coll_name} — deleted {deleted:,} docs older than {days}d")
            except Exception as e:
                logger.error(f"DataRetention: {coll_name} — error: {e}")
                results[coll_name] = {"error": str(e)}

        client.close()

        # Log summary
        logger.info(f"DataRetention complete: {total_deleted:,} total docs deleted across {len(results)} collections")

        return {
            "total_deleted": total_deleted,
            "collections_processed": len(results),
            "details": results,
        }
