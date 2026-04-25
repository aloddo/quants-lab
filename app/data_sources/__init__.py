"""
Data Source Registry — declarative descriptors for MongoDB → candle DataFrame merging.

Each MongoDB collection that feeds into strategy backtests or feature computation
gets a DataSourceDescriptor. The generic merge engine reads these descriptors
to produce enriched candle DataFrames without hardcoded per-collection logic.

Two descriptor types:
- DataSourceDescriptor: flat sources (one pair_field, scalar columns, ffill/bfill)
- CompositeDataSourceDescriptor: multi-dimensional sources (options surface, L2 book)
  that need custom merge logic via an escape hatch function.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union


@dataclass(frozen=True)
class ColumnSpec:
    """How one MongoDB field maps to a candle DataFrame column.

    Attributes:
        df_column: Target column name in the candle DataFrame.
        fill_value: Neutral fill for NaN after reindexing. Must be chosen carefully:
            - 0.0 for additive quantities (funding_rate, liquidations)
            - 0.5 for ratios centered at 0.5 (buy_ratio)
            - 50.0 for indices centered at 50 (Fear & Greed)
        reindex_method: How to align to candle timestamps.
            - "ffill": forward-fill (most common — propagate last known value)
            - "bfill": back-fill (rare)
            - "ffill+bfill": ffill FIRST, then bfill leading NaNs with earliest
              known value. ORDER MATTERS. Used for OI where we want to fill both
              ends of the series.
    """
    df_column: str
    fill_value: float
    reindex_method: str = "ffill"


@dataclass(frozen=True)
class DataSourceDescriptor:
    """Flat data source: one collection, pair-scoped, scalar columns.

    Covers ~90% of sources: funding rates, OI, LS ratio, liquidations,
    Fear & Greed, DVol, whale consensus, etc.

    Attributes:
        name: Unique identifier (e.g., "bybit_funding").
        collection: MongoDB collection name.
        feature_tag: Matches strategy's required_features entry.
            All descriptors with the same tag are merged when a strategy
            declares that tag in its required_features list.
        columns: {mongo_field_name: ColumnSpec} mapping.
        pair_field: Field name for pair filter ("pair", "coin", "symbol", etc.).
        sort_field: Field to sort by (usually "timestamp_utc").
        sort_unit: Timestamp unit: "ms" (Int64 millis), "s" (float seconds),
            "datetime" (Python datetime objects).
        pair_scoped: False for global data (Fear & Greed, BTC regime).
        extra_filter: Additional MongoDB query filter (e.g., {"resolution": "1hour"}).
        fallback_filter: Fallback filter if < fallback_min_docs with extra_filter
            (e.g., {"resolution": "daily"} when hourly data is sparse).
        fallback_min_docs: Threshold to trigger fallback.
        expected_ranges: Optional value-range sanity checks for data quality monitoring.
            {mongo_field: (min_val, max_val)} — StorageGuard checks latest docs.
    """
    name: str
    collection: str
    feature_tag: str
    columns: Dict[str, ColumnSpec]
    pair_field: str = "pair"
    sort_field: str = "timestamp_utc"
    sort_unit: str = "ms"
    pair_scoped: bool = True
    extra_filter: Dict[str, Any] = field(default_factory=dict)
    fallback_filter: Optional[Dict[str, Any]] = None
    fallback_min_docs: int = 100
    expected_ranges: Dict[str, tuple] = field(default_factory=dict)


@dataclass(frozen=True)
class CompositeDataSourceDescriptor:
    """Multi-dimensional data source requiring custom merge logic.

    Escape hatch for sources that don't fit the flat pattern:
    - Options surface (grouped by strike/expiry, cross-sectional aggregation)
    - L2 order book (multi-level, needs depth aggregation)

    The merge_fn is a dotted path to a function with signature:
        (db, candle_df, pair, start_ts, end_ts) -> pd.DataFrame

    Validated at import time by validate_registry().

    Attributes:
        name: Unique identifier.
        collection: MongoDB collection name (for catalog/monitoring).
        feature_tag: Matches strategy's required_features entry.
        merge_fn: Dotted path to sync merge function.
        merge_fn_async: Dotted path to async merge function (optional).
        expected_ranges: Optional value-range sanity checks.
    """
    name: str
    collection: str
    feature_tag: str
    merge_fn: str
    merge_fn_async: Optional[str] = None
    expected_ranges: Dict[str, tuple] = field(default_factory=dict)
