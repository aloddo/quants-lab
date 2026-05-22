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
    # ─────────────────────────────────────────────────────────────────────
    # POLICY (Alberto directive 2026-05-22): RAW DATA NEVER DELETES.
    # Painfully acquired over days — must never auto-purge.
    # Only DERIVED / OPERATIONAL data has retention rules.
    # ─────────────────────────────────────────────────────────────────────
    #
    # REMOVED ENTRIES (raw, no retention — keep forever):
    #   - bybit_funding_rates, bybit_open_interest, bybit_ls_ratio
    #   - binance_funding_rates
    #   - hyperliquid_funding_rates, hyperliquid_candles_1h
    #   - hyperliquid_l2_snapshots_1s, hyperliquid_recent_trades_1s
    #     (L2/trades: 7d rule was the bug that purged previously collected data)
    #   - deribit_options_surface, deribit_dvol
    #   - coinalyze_liquidations, coinalyze_oi
    #   - arb_hl_bybit_perp_snapshots (HL/Bybit top-of-book — RAW)
    #
    # If disk pressure ever requires retention on raw collections,
    # archive to remote DB / cold storage FIRST, then prune.

    # Derived / operational data only (safe to purge):
    "candidates":                   {"field": "timestamp_utc", "unit": "ms", "days": 365},
    "arb_opportunities":            {"field": "timestamp",     "unit": "s",  "days": 30},
    "arb_h2_inventory_drift":       {"field": "timestamp",     "unit": "s",  "days": 30},
    "arb_h2_tier_history":          {"field": "timestamp",     "unit": "s",  "days": 90},
    "task_executions":              {"field": "started_at",    "unit": "datetime", "days": 90},
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
