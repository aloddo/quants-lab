"""
Generic Merge Engine — replaces ALL hardcoded merge functions.

Merges MongoDB collections into candle DataFrames using DataSourceDescriptor
configuration. Used by _backtest_worker.py, bulk_backtest_task.py, and
walk_forward_task.py.

Key design decisions:
- db (MongoClient database) is passed in, never created here (avoids connection leaks)
- start_ts/end_ts enable time-range filtering (prevents walk-forward lookahead)
- ffill+bfill semantics: ffill FIRST, then bfill (order matters for OI)
"""

import importlib
import logging
from typing import Optional, Union

import numpy as np
import pandas as pd

from app.data_sources import (
    ColumnSpec,
    CompositeDataSourceDescriptor,
    DataSourceDescriptor,
)

logger = logging.getLogger(__name__)

# Buffer before start_ts for ffill anchor (24h in each unit)
_BUFFER_MS = 24 * 3600 * 1000
_BUFFER_S = 24 * 3600.0


def merge_source_sync(
    db,
    descriptor: DataSourceDescriptor,
    candle_df: pd.DataFrame,
    pair: str,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
    candle_ts_col: str = "timestamp",
    candle_ts_unit: str = "s",
) -> pd.DataFrame:
    """Merge one flat data source into a candle DataFrame (sync/pymongo).

    Args:
        db: pymongo Database instance (caller manages lifecycle).
        descriptor: DataSourceDescriptor defining the merge.
        candle_df: DataFrame with OHLCV candle data.
        pair: Trading pair (e.g., "BTC-USDT").
        start_ts: Optional start timestamp in millis. If provided, only query
            docs >= (start_ts - 24h buffer) to prevent lookahead in walk-forward.
        end_ts: Optional end timestamp in millis. If provided, only query
            docs <= end_ts.
        candle_ts_col: Column name for timestamps in candle_df (default: "timestamp").
        candle_ts_unit: Unit for candle timestamps (default: "s" for seconds).

    Returns:
        candle_df with new columns added per descriptor.columns.
    """
    candle_idx = pd.to_datetime(candle_df[candle_ts_col], unit=candle_ts_unit, utc=True)

    # Build query — normalize pair value for non-standard pair_fields
    query = {}
    if descriptor.pair_scoped:
        pair_value = pair
        if descriptor.pair_field == "currency":
            # deribit_dvol uses "BTC"/"ETH", not "BTC-USDT"
            pair_value = pair.split("-")[0]
        elif descriptor.pair_field == "coin":
            # whale_consensus uses "BTC", not "BTC-USDT"
            pair_value = pair.split("-")[0]
        elif descriptor.pair_field == "symbol":
            # arb collections use "BTCUSDT"
            pair_value = pair.replace("-", "")
        query[descriptor.pair_field] = pair_value
    if descriptor.extra_filter:
        query.update(descriptor.extra_filter)

    # Time-range filter (prevents walk-forward lookahead)
    if start_ts is not None or end_ts is not None:
        ts_filter = {}
        if start_ts is not None:
            if descriptor.sort_unit == "ms":
                ts_filter["$gte"] = start_ts - _BUFFER_MS
            elif descriptor.sort_unit == "s":
                ts_filter["$gte"] = start_ts / 1000 - _BUFFER_S
            elif descriptor.sort_unit == "datetime":
                from datetime import datetime, timezone, timedelta
                ts_filter["$gte"] = datetime.fromtimestamp(
                    start_ts / 1000 - _BUFFER_S, tz=timezone.utc
                )
        if end_ts is not None:
            if descriptor.sort_unit == "ms":
                ts_filter["$lte"] = end_ts
            elif descriptor.sort_unit == "s":
                ts_filter["$lte"] = end_ts / 1000
            elif descriptor.sort_unit == "datetime":
                from datetime import datetime, timezone
                ts_filter["$lte"] = datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc)
        if ts_filter:
            query[descriptor.sort_field] = ts_filter

    # Query MongoDB
    docs = list(db[descriptor.collection].find(query).sort(descriptor.sort_field, 1))

    # Fallback to alternate filter ONLY if primary returns ZERO docs.
    # Codex review: short backtest windows naturally have < fallback_min_docs
    # hourly data — falling back to daily on "too few" silently corrupts features.
    if len(docs) == 0 and descriptor.fallback_filter:
        query_fb = {k: v for k, v in query.items()}
        for key in descriptor.extra_filter:
            query_fb.pop(key, None)
        query_fb.update(descriptor.fallback_filter)
        docs = list(db[descriptor.collection].find(query_fb).sort(descriptor.sort_field, 1))

    # If no docs, fill all columns with neutral values
    if not docs:
        for col_spec in descriptor.columns.values():
            candle_df[col_spec.df_column] = col_spec.fill_value
        return candle_df

    # Build source DataFrame
    sdf = pd.DataFrame(docs)

    # Parse timestamps
    if descriptor.sort_unit == "ms":
        sdf["_ts"] = pd.to_datetime(sdf[descriptor.sort_field], unit="ms", utc=True)
    elif descriptor.sort_unit == "s":
        sdf["_ts"] = pd.to_datetime(sdf[descriptor.sort_field], unit="s", utc=True)
    elif descriptor.sort_unit == "datetime":
        sdf["_ts"] = pd.to_datetime(sdf[descriptor.sort_field], utc=True)
    else:
        raise ValueError(f"Unknown sort_unit: {descriptor.sort_unit}")

    sdf = sdf.set_index("_ts").sort_index()
    sdf = sdf[~sdf.index.duplicated(keep="last")]

    # Merge each column
    for mongo_field, col_spec in descriptor.columns.items():
        if mongo_field not in sdf.columns:
            candle_df[col_spec.df_column] = col_spec.fill_value
            continue

        series = sdf[[mongo_field]]

        # Reindex to candle timestamps
        if col_spec.reindex_method == "ffill":
            reindexed = series.reindex(candle_idx, method="ffill")
        elif col_spec.reindex_method == "bfill":
            reindexed = series.reindex(candle_idx, method="bfill")
        elif col_spec.reindex_method == "ffill+bfill":
            # ffill FIRST (propagate last known forward), then bfill (fill leading NaNs)
            reindexed = series.reindex(candle_idx, method="ffill")
            reindexed = reindexed.bfill()
        else:
            reindexed = series.reindex(candle_idx, method="ffill")

        candle_df[col_spec.df_column] = reindexed[mongo_field].fillna(col_spec.fill_value).values

    return candle_df


def merge_composite_sync(
    db,
    descriptor: CompositeDataSourceDescriptor,
    candle_df: pd.DataFrame,
    pair: str,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
) -> pd.DataFrame:
    """Merge a composite data source via its custom merge function (sync)."""
    module_path, fn_name = descriptor.merge_fn.rsplit(".", 1)
    mod = importlib.import_module(module_path)
    merge_fn = getattr(mod, fn_name)
    return merge_fn(db, candle_df, pair, start_ts, end_ts)


def merge_all_for_engine_sync(
    db,
    engine_name: str,
    candle_df: pd.DataFrame,
    pair: str,
    start_ts: Optional[int] = None,
    end_ts: Optional[int] = None,
) -> pd.DataFrame:
    """Merge ALL data sources required by an engine's required_features.

    This is the single entry point for backtest workers. Reads the strategy's
    required_features tags, finds all matching descriptors, applies them
    sequentially.

    Args:
        db: pymongo Database instance.
        engine_name: Strategy name (e.g., "E3", "X10").
        candle_df: DataFrame with OHLCV candle data.
        pair: Trading pair.
        start_ts: Optional start timestamp in millis (for walk-forward).
        end_ts: Optional end timestamp in millis.

    Returns:
        candle_df enriched with all required data source columns.
    """
    from app.engines.strategy_registry import get_strategy
    from app.data_sources.registry import get_sources_for_tag

    meta = get_strategy(engine_name)
    required = meta.required_features or []

    for tag in required:
        sources = get_sources_for_tag(tag)
        if not sources:
            logger.warning(f"No data sources registered for feature tag '{tag}' (engine={engine_name})")
            continue

        for source in sources:
            try:
                if isinstance(source, DataSourceDescriptor):
                    candle_df = merge_source_sync(
                        db, source, candle_df, pair,
                        start_ts=start_ts, end_ts=end_ts,
                    )
                elif isinstance(source, CompositeDataSourceDescriptor):
                    candle_df = merge_composite_sync(
                        db, source, candle_df, pair,
                        start_ts=start_ts, end_ts=end_ts,
                    )
            except Exception as e:
                logger.error(
                    f"Failed to merge source '{source.name}' for {pair}: {e}",
                    exc_info=True,
                )
                # Fill columns with neutral values on error
                if isinstance(source, DataSourceDescriptor):
                    for col_spec in source.columns.values():
                        if col_spec.df_column not in candle_df.columns:
                            candle_df[col_spec.df_column] = col_spec.fill_value

    return candle_df
