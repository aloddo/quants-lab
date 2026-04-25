"""
Tick Aggregation Task — compresses raw 5s spread snapshots to 1m distributional stats.

Raw tick data (5s quotes from arb collectors) is too large to retain long-term.
But arb strategies need spread DISTRIBUTION information for screening and research.
This task aggregates raw ticks into 1m statistical bars that preserve:
- Percentile behavior (p25, p50, p75, p90) — what TierEngine uses
- Exceedance rates (time above fee thresholds) — for execution feasibility
- Direction-specific spreads — entry/exit on arb strategies is directional

IMPORTANT: These stats are for SCREENING and RESEARCH only. P90 of 1m P90s != P90
of the raw 6h population (quantile aggregation problem). For faithful arb REPLAY,
use the raw 14-day tick data directly.

Schema per 1m bar (per pair):
  timestamp (datetime, left edge of minute)
  pair (str)
  count (int) — ticks in the minute
  # HL premium direction (buy BB, sell HL)
  hl_prem_mean, hl_prem_std, hl_prem_min, hl_prem_max
  hl_prem_p25, hl_prem_p50, hl_prem_p75, hl_prem_p90
  # BB premium direction (buy HL, sell BB)
  bb_prem_mean, bb_prem_std, bb_prem_min, bb_prem_max
  bb_prem_p25, bb_prem_p50, bb_prem_p75, bb_prem_p90
  # Best spread (either direction)
  best_mean, best_max, best_p90
  # Exceedance (fraction of ticks above threshold)
  time_above_5bps_pct (float, 0-1)
  time_above_10bps_pct (float, 0-1)
  time_above_15bps_pct (float, 0-1)

Runs daily. Source → stats collection mapping:
  arb_hl_bybit_perp_snapshots → arb_hl_bybit_spread_stats_1m
  arb_bb_spot_perp_snapshots → arb_bb_spread_stats_1m
  arb_bn_usdc_bb_perp_snapshots → arb_bn_spread_stats_1m
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pymongo import MongoClient, UpdateOne

from core.tasks import BaseTask, TaskContext

logger = logging.getLogger(__name__)


# Source collection → (stats collection, spread field configs)
# Each config: (src_field_a, src_field_b, best_field)
# field_a = "hl premium" direction, field_b = "bb premium" direction
AGGREGATION_SOURCES = {
    "arb_hl_bybit_perp_snapshots": {
        "stats_collection": "arb_hl_bybit_spread_stats_1m",
        "ts_field": "timestamp",
        "ts_unit": "datetime",
        "pair_field": "pair",
        "spread_a_field": "spread_hl_over_bb",  # HL premium (buy BB, sell HL)
        "spread_b_field": "spread_bb_over_hl",  # BB premium (buy HL, sell BB)
        "best_field": "best_spread",
    },
    "arb_bb_spot_perp_snapshots": {
        "stats_collection": "arb_bb_spread_stats_1m",
        "ts_field": "timestamp",
        "ts_unit": "datetime",
        "pair_field": "symbol",
        "spread_a_field": "spread_buy_perp_sell_spot",
        "spread_b_field": "spread_buy_spot_sell_perp",
        "best_field": "best_spread",
    },
    "arb_bn_usdc_bb_perp_snapshots": {
        "stats_collection": "arb_bn_spread_stats_1m",
        "ts_field": "timestamp",
        "ts_unit": "datetime",
        "pair_field": "symbol_bb",
        "spread_a_field": "spread_buy_perp_sell_bn",
        "spread_b_field": "spread_buy_bn_sell_perp",
        "best_field": "best_spread",
    },
}


def _compute_minute_stats(group: pd.DataFrame, cfg: dict) -> dict:
    """Compute distributional stats for one minute of tick data."""
    n = len(group)
    if n == 0:
        return None

    # Clean NaN/inf from raw tick data before computing stats
    a = pd.to_numeric(group[cfg["spread_a_field"]], errors="coerce").values
    b = pd.to_numeric(group[cfg["spread_b_field"]], errors="coerce").values
    best = pd.to_numeric(group[cfg["best_field"]], errors="coerce").values
    a = a[np.isfinite(a)]
    b = b[np.isfinite(b)]
    best = best[np.isfinite(best)]
    if len(a) == 0 or len(b) == 0 or len(best) == 0:
        return None
    n = len(best)  # Update count to reflect cleaned data

    stats = {"count": n}

    # Direction A stats (e.g., HL premium)
    stats["a_mean"] = float(np.mean(a))
    stats["a_std"] = float(np.std(a)) if n > 1 else 0.0
    stats["a_min"] = float(np.min(a))
    stats["a_max"] = float(np.max(a))
    stats["a_p25"] = float(np.percentile(a, 25))
    stats["a_p50"] = float(np.percentile(a, 50))
    stats["a_p75"] = float(np.percentile(a, 75))
    stats["a_p90"] = float(np.percentile(a, 90))

    # Direction B stats (e.g., BB premium)
    stats["b_mean"] = float(np.mean(b))
    stats["b_std"] = float(np.std(b)) if n > 1 else 0.0
    stats["b_min"] = float(np.min(b))
    stats["b_max"] = float(np.max(b))
    stats["b_p25"] = float(np.percentile(b, 25))
    stats["b_p50"] = float(np.percentile(b, 50))
    stats["b_p75"] = float(np.percentile(b, 75))
    stats["b_p90"] = float(np.percentile(b, 90))

    # Best spread stats
    stats["best_mean"] = float(np.mean(best))
    stats["best_max"] = float(np.max(best))
    stats["best_p90"] = float(np.percentile(best, 90))

    # Exceedance rates (fraction of ticks above threshold)
    stats["time_above_5bps"] = float(np.mean(best > 5.0))
    stats["time_above_10bps"] = float(np.mean(best > 10.0))
    stats["time_above_15bps"] = float(np.mean(best > 15.0))

    return stats


class TickAggregationTask(BaseTask):
    """Aggregate raw tick spread snapshots to 1m distributional stats.

    Processes each source collection, computes per-minute per-pair distributional
    statistics, and stores to a companion stats collection. Designed to run daily
    after the raw data has accumulated.
    """

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config if hasattr(self.config, "config") else config
        self.mongo_uri = task_config.get("mongo_uri", "mongodb://localhost:27017/quants_lab")
        self.mongo_db = task_config.get("mongo_database", "quants_lab")
        # Only process ticks from the last N days (avoid re-processing old data)
        self.lookback_days = task_config.get("lookback_days", 2)

    async def execute(self, context: TaskContext):
        client = MongoClient(self.mongo_uri)
        db = client[self.mongo_db]
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=self.lookback_days)

        total_stats = 0
        results = {}

        for src_coll_name, cfg in AGGREGATION_SOURCES.items():
            stats_coll_name = cfg["stats_collection"]

            if src_coll_name not in db.list_collection_names():
                continue

            src_coll = db[src_coll_name]
            stats_coll = db[stats_coll_name]

            # Ensure indexes on stats collection
            stats_coll.create_index([("pair", 1), ("timestamp", 1)], unique=True)
            stats_coll.create_index("timestamp", expireAfterSeconds=90 * 86400)  # 90d TTL

            # Find the latest aggregated timestamp to avoid re-processing
            latest_agg = stats_coll.find_one(sort=[("timestamp", -1)])
            if latest_agg and latest_agg.get("timestamp"):
                agg_cutoff = latest_agg["timestamp"] - timedelta(minutes=5)  # Small overlap
            else:
                agg_cutoff = cutoff

            # Use the later of the two cutoffs
            effective_cutoff = max(agg_cutoff, cutoff) if latest_agg else cutoff

            # Process in pair chunks to avoid memory blowup on large collections
            # (30 pairs × 14 days × 5s = ~7M docs would be ~2GB in RAM)
            pair_field = cfg["pair_field"]
            pairs = src_coll.distinct(pair_field)
            total_processed = 0
            stats_docs = []

            for pair_val in pairs:
                query = {
                    cfg["ts_field"]: {"$gte": effective_cutoff},
                    pair_field: pair_val,
                }
                docs = list(src_coll.find(query).sort(cfg["ts_field"], 1))
                if not docs:
                    continue

                total_processed += len(docs)
                df = pd.DataFrame(docs)
                df["_ts"] = pd.to_datetime(df[cfg["ts_field"]])
                if df["_ts"].dt.tz is None:
                    df["_ts"] = df["_ts"].dt.tz_localize("UTC")
                df["_minute"] = df["_ts"].dt.floor("min")

                for minute, group in df.groupby("_minute"):
                    stats = _compute_minute_stats(group, cfg)
                    if stats is None:
                        continue
                    stats_docs.append({
                        "timestamp": minute.to_pydatetime(),
                        "pair": pair_val,
                        **stats,
                    })

            # Upsert stats
            if stats_docs:
                ops = [
                    UpdateOne(
                        {"pair": d["pair"], "timestamp": d["timestamp"]},
                        {"$set": d},
                        upsert=True,
                    )
                    for d in stats_docs
                ]
                result = stats_coll.bulk_write(ops, ordered=False)
                n_written = result.upserted_count + result.modified_count
                total_stats += n_written
                results[src_coll_name] = {
                    "processed": total_processed,
                    "stats_written": n_written,
                    "minutes": len(stats_docs),
                }
                logger.info(
                    f"TickAggregation: {src_coll_name} → {stats_coll_name}: "
                    f"{total_processed:,} ticks → {n_written:,} minute-stats"
                )
            else:
                results[src_coll_name] = {"processed": total_processed, "stats_written": 0}

        client.close()
        logger.info(f"TickAggregation complete: {total_stats:,} stats written")
        return {"total_stats": total_stats, "details": results}
