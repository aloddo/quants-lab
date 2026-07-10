"""
Feature Catalog — queryable inventory of all data sources, their health, and consumers.

Cross-references DATA_SOURCE_REGISTRY with STRATEGY_REGISTRY to produce a
complete picture of what data exists, who consumes it, and whether it's healthy.

Usage:
    python cli.py feature-catalog          # Print health dashboard
    python cli.py feature-catalog --json   # JSON output for monitoring
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import MongoClient

from app.data_sources import DataSourceDescriptor, CompositeDataSourceDescriptor
from app.data_sources.registry import DATA_SOURCE_REGISTRY, get_all_tags

logger = logging.getLogger(__name__)


@dataclass
class CatalogEntry:
    """One row in the feature catalog."""
    name: str
    feature_tag: str
    collection: str
    descriptor_type: str  # "flat" or "composite"
    columns_produced: List[str]
    consumers: List[str]  # strategy names
    doc_count: int = 0
    last_doc_timestamp: Optional[datetime] = None
    freshness_seconds: Optional[float] = None
    health: str = "unknown"  # "healthy", "stale", "empty", "error"
    quality_issues: List[str] = field(default_factory=list)


# Expected update frequency per collection (seconds)
FRESHNESS_THRESHOLDS = {
    "bybit_funding_rates": 1800,       # 15min task → stale after 30min
    "bybit_open_interest": 1800,
    "bybit_ls_ratio": 1800,
    "binance_funding_rates": 1800,
    "hyperliquid_funding_rates": 1800,
    "hyperliquid_l2_snapshots_1s": 120,  # 1min task → stale after 2min
    "hyperliquid_recent_trades_1s": 120,
    "deribit_options_surface": 1800,
    "deribit_dvol": 1800,
    "coinalyze_liquidations": 1800,
    "coinalyze_oi": 1800,
    "fear_greed_index": 172800,        # daily → stale after 2 days
    "hl_whale_consensus": 3600,        # 15min → stale after 1h
}


def build_catalog(mongo_uri: str = "mongodb://localhost:27017/quants_lab") -> List[CatalogEntry]:
    """Build the feature catalog by cross-referencing data sources and strategy registry."""
    from app.engines.strategy_registry import STRATEGY_REGISTRY

    db_name = mongo_uri.rsplit("/", 1)[-1] or "quants_lab"
    client = MongoClient(mongo_uri)
    db = client[db_name]

    catalog = []

    for ds_name, ds in DATA_SOURCE_REGISTRY.items():
        # Find consuming strategies
        consumers = [
            name for name, meta in STRATEGY_REGISTRY.items()
            if ds.feature_tag in (meta.required_features or [])
        ]

        # Determine columns produced
        if isinstance(ds, DataSourceDescriptor):
            columns = [cs.df_column for cs in ds.columns.values()]
            dtype = "flat"
        else:
            columns = ["(custom merge)"]
            dtype = "composite"

        # Query collection stats
        entry = CatalogEntry(
            name=ds_name,
            feature_tag=ds.feature_tag,
            collection=ds.collection,
            descriptor_type=dtype,
            columns_produced=columns,
            consumers=consumers,
        )

        try:
            if ds.collection not in db.list_collection_names():
                entry.health = "missing"
                catalog.append(entry)
                continue

            entry.doc_count = db[ds.collection].estimated_document_count()
            if entry.doc_count == 0:
                entry.health = "empty"
                catalog.append(entry)
                continue

            # Find latest document
            sort_field = ds.sort_field if isinstance(ds, DataSourceDescriptor) else "timestamp_utc"
            latest = db[ds.collection].find_one(sort=[(sort_field, -1)])

            if latest:
                sort_unit = ds.sort_unit if isinstance(ds, DataSourceDescriptor) else "ms"
                raw_ts = latest.get(sort_field)
                if raw_ts is not None:
                    if sort_unit == "ms" and isinstance(raw_ts, (int, float)):
                        entry.last_doc_timestamp = datetime.fromtimestamp(
                            raw_ts / 1000, tz=timezone.utc
                        )
                    elif sort_unit == "s" and isinstance(raw_ts, (int, float)):
                        entry.last_doc_timestamp = datetime.fromtimestamp(
                            raw_ts, tz=timezone.utc
                        )
                    elif sort_unit == "datetime" or isinstance(raw_ts, datetime):
                        if raw_ts.tzinfo is None:
                            entry.last_doc_timestamp = raw_ts.replace(tzinfo=timezone.utc)
                        else:
                            entry.last_doc_timestamp = raw_ts

            # Compute freshness
            if entry.last_doc_timestamp:
                now = datetime.now(timezone.utc)
                entry.freshness_seconds = (now - entry.last_doc_timestamp).total_seconds()

                threshold = FRESHNESS_THRESHOLDS.get(ds.collection, 7200)
                if entry.freshness_seconds <= threshold:
                    entry.health = "healthy"
                else:
                    entry.health = "stale"

            # Data quality checks (if expected_ranges defined)
            if isinstance(ds, DataSourceDescriptor) and ds.expected_ranges:
                sample = list(db[ds.collection].find().sort(sort_field, -1).limit(10))
                for mongo_field, (min_val, max_val) in ds.expected_ranges.items():
                    for doc in sample:
                        val = doc.get(mongo_field)
                        if val is not None and (val < min_val or val > max_val):
                            entry.quality_issues.append(
                                f"{mongo_field}={val} outside [{min_val}, {max_val}]"
                            )
                            break

        except Exception as e:
            entry.health = "error"
            entry.quality_issues.append(str(e))

        catalog.append(entry)

    client.close()
    return catalog


def print_catalog(catalog: List[CatalogEntry]) -> None:
    """Print the catalog as a formatted table."""
    print(f"\n{'='*100}")
    print(f"  FEATURE STORE CATALOG — {len(catalog)} data sources, {len(set(e.feature_tag for e in catalog))} tags")
    print(f"{'='*100}\n")

    # Group by tag
    tags = sorted(set(e.feature_tag for e in catalog))
    for tag in tags:
        entries = [e for e in catalog if e.feature_tag == tag]
        print(f"  [{tag}]")
        for e in entries:
            health_icon = {"healthy": "✓", "stale": "⚠", "empty": "○", "missing": "✗", "error": "✗"}.get(e.health, "?")
            freshness = ""
            if e.freshness_seconds is not None:
                if e.freshness_seconds < 3600:
                    freshness = f"{e.freshness_seconds/60:.0f}m ago"
                elif e.freshness_seconds < 86400:
                    freshness = f"{e.freshness_seconds/3600:.1f}h ago"
                else:
                    freshness = f"{e.freshness_seconds/86400:.1f}d ago"

            consumers_str = ", ".join(e.consumers) if e.consumers else "(no consumers)"
            cols_str = ", ".join(e.columns_produced)
            quality_str = f" ⚠ {'; '.join(e.quality_issues)}" if e.quality_issues else ""

            print(f"    {health_icon} {e.name:<30} {e.doc_count:>10,} docs  {freshness:>10}  → {cols_str}")
            print(f"      {e.collection:<30} consumers: {consumers_str}{quality_str}")
        print()

    # Summary
    healthy = sum(1 for e in catalog if e.health == "healthy")
    stale = sum(1 for e in catalog if e.health == "stale")
    empty = sum(1 for e in catalog if e.health in ("empty", "missing"))
    no_consumer = sum(1 for e in catalog if not e.consumers)
    print(f"  Summary: {healthy} healthy, {stale} stale, {empty} empty/missing, {no_consumer} without consumers")
    print()
