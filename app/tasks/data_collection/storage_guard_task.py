"""
Storage Guard Task — monitors disk and MongoDB usage, logs to MongoDB + stdout.

Runs daily in the pipeline. Alerts when thresholds are breached.
"""

import shutil
import logging
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

from core.tasks import BaseTask, TaskContext

logger = logging.getLogger(__name__)


class StorageGuardTask(BaseTask):
    """Monitor storage usage across disk, MongoDB, and parquet cache."""

    # Thresholds (fraction of total disk)
    WARN_DISK_PCT = 0.70
    CRIT_DISK_PCT = 0.85

    # MongoDB size thresholds (bytes)
    WARN_MONGO_GB = 10
    CRIT_MONGO_GB = 25

    # Parquet cache threshold (bytes)
    WARN_CACHE_GB = 20
    CRIT_CACHE_GB = 50

    CACHE_DIR = Path("/Users/hermes/quants-lab/app/data/cache")

    def __init__(self, config):
        super().__init__(config)
        # Task runner v2 passes TaskConfig; keep backward compatibility with dict.
        task_config = self.config.config if hasattr(self.config, "config") else config
        self.mongo_uri = task_config.get("mongo_uri", "mongodb://localhost:27017/quants_lab")
        self.mongo_db = task_config.get("mongo_database", "quants_lab")

    async def execute(self, context: TaskContext):
        ts = datetime.now(timezone.utc)
        report = {"timestamp_utc": ts, "alerts": []}

        # ── Disk usage ──
        total, used, free = shutil.disk_usage("/")
        disk_pct = used / total
        report["disk"] = {
            "total_gb": round(total / 1e9, 1),
            "used_gb": round(used / 1e9, 1),
            "free_gb": round(free / 1e9, 1),
            "used_pct": round(disk_pct * 100, 1),
        }
        if disk_pct >= self.CRIT_DISK_PCT:
            report["alerts"].append(f"CRITICAL: Disk {disk_pct*100:.0f}% used (>={self.CRIT_DISK_PCT*100:.0f}%)")
        elif disk_pct >= self.WARN_DISK_PCT:
            report["alerts"].append(f"WARNING: Disk {disk_pct*100:.0f}% used (>={self.WARN_DISK_PCT*100:.0f}%)")

        # ── MongoDB usage ──
        client = MongoClient(self.mongo_uri)
        db_name = self.mongo_uri.rsplit("/", 1)[-1] if "/" in self.mongo_uri else self.mongo_db
        db = client[db_name]

        colls = sorted(db.list_collection_names())
        mongo_total_bytes = 0
        coll_stats = []
        for c in colls:
            try:
                stats = db.command("collStats", c)
                storage = stats.get("storageSize", 0)
                mongo_total_bytes += storage
                count = stats.get("count", 0)

                # Check for TTL
                indexes = list(db[c].list_indexes())
                has_ttl = any("expireAfterSeconds" in idx for idx in indexes)
                ttl_days = None
                for idx in indexes:
                    if "expireAfterSeconds" in idx:
                        ttl_days = idx["expireAfterSeconds"] / 86400

                coll_stats.append({
                    "collection": c,
                    "docs": count,
                    "storage_mb": round(storage / 1e6, 1),
                    "has_ttl": has_ttl,
                    "ttl_days": ttl_days,
                })
            except Exception:
                continue

        mongo_gb = mongo_total_bytes / 1e9
        report["mongodb"] = {
            "total_gb": round(mongo_gb, 2),
            "collections": len(colls),
            "top_10": sorted(coll_stats, key=lambda x: -x["storage_mb"])[:10],
            "no_ttl_large": [
                c for c in coll_stats
                if not c["has_ttl"] and c["storage_mb"] > 10
            ],
        }
        if mongo_gb >= self.CRIT_MONGO_GB:
            report["alerts"].append(f"CRITICAL: MongoDB {mongo_gb:.1f}GB (>={self.CRIT_MONGO_GB}GB)")
        elif mongo_gb >= self.WARN_MONGO_GB:
            report["alerts"].append(f"WARNING: MongoDB {mongo_gb:.1f}GB (>={self.WARN_MONGO_GB}GB)")

        # Flag large collections without TTL
        for c in report["mongodb"]["no_ttl_large"]:
            report["alerts"].append(
                f"WARNING: {c['collection']} has {c['storage_mb']:.0f}MB and no TTL index"
            )

        # ── Parquet cache usage ──
        cache_bytes = 0
        cache_dirs = {}
        if self.CACHE_DIR.exists():
            for item in self.CACHE_DIR.iterdir():
                if item.is_dir():
                    dir_size = sum(f.stat().st_size for f in item.rglob("*") if f.is_file())
                    cache_bytes += dir_size
                    cache_dirs[item.name] = round(dir_size / 1e6, 1)

        cache_gb = cache_bytes / 1e9
        report["parquet_cache"] = {
            "total_gb": round(cache_gb, 2),
            "top_dirs": dict(sorted(cache_dirs.items(), key=lambda x: -x[1])[:10]),
        }
        if cache_gb >= self.CRIT_CACHE_GB:
            report["alerts"].append(f"CRITICAL: Parquet cache {cache_gb:.1f}GB (>={self.CRIT_CACHE_GB}GB)")
        elif cache_gb >= self.WARN_CACHE_GB:
            report["alerts"].append(f"WARNING: Parquet cache {cache_gb:.1f}GB (>={self.WARN_CACHE_GB}GB)")

        # ── Summary ──
        total_data_gb = mongo_gb + cache_gb
        report["summary"] = {
            "total_data_gb": round(total_data_gb, 2),
            "disk_free_gb": round(free / 1e9, 1),
            "runway_days": round((free / 1e9 - 50) / max(total_data_gb / 30, 0.01), 0),  # rough estimate
            "status": "CRITICAL" if any("CRITICAL" in a for a in report["alerts"])
                      else "WARNING" if report["alerts"]
                      else "HEALTHY",
        }

        # ── Log ──
        status = report["summary"]["status"]
        logger.info(
            f"Storage Guard: {status} | "
            f"Disk: {report['disk']['used_pct']}% | "
            f"MongoDB: {mongo_gb:.2f}GB | "
            f"Cache: {cache_gb:.2f}GB | "
            f"Total: {total_data_gb:.2f}GB | "
            f"Free: {report['disk']['free_gb']}GB"
        )
        for alert in report["alerts"]:
            logger.warning(f"  {alert}")

        # ── Store in MongoDB ──
        db.storage_guard_reports.insert_one(report)
        client.close()

        return {
            "status": "completed",
            "timestamp": ts.isoformat(),
            "execution_id": context.execution_id,
            "report": report,
        }
