"""
Data Source Registry — THE single file to edit when adding a new data source.

Every MongoDB collection that feeds into candle DataFrames for backtesting or
feature computation is registered here. The generic merge engine reads this
registry to know what to merge and how.

To add a new data source:
1. Add a DataSourceDescriptor entry below
2. Add the feature_tag to your strategy's required_features in strategy_registry.py
That's it. The merge engine picks it up automatically.

For multi-dimensional sources (options surface, L2 book), use
CompositeDataSourceDescriptor and implement a custom merge function
in app/data_sources/mergers.py.
"""

import inspect
import importlib
import logging
from typing import Dict, List, Optional, Set, Union

from app.data_sources import (
    ColumnSpec,
    CompositeDataSourceDescriptor,
    DataSourceDescriptor,
)

logger = logging.getLogger(__name__)


DATA_SOURCE_REGISTRY: Dict[str, Union[DataSourceDescriptor, CompositeDataSourceDescriptor]] = {

    # ═══════════════════════════════════════════════════════════
    # Bybit derivatives (tag: "derivatives")
    # Used by: E3, E4, X1, X2, X5, X9, X10
    # ═══════════════════════════════════════════════════════════

    "bybit_funding": DataSourceDescriptor(
        name="bybit_funding",
        collection="bybit_funding_rates",
        feature_tag="derivatives",
        columns={
            "funding_rate": ColumnSpec("funding_rate", fill_value=0.0),
        },
        expected_ranges={"funding_rate": (-0.01, 0.01)},
    ),

    "bybit_oi": DataSourceDescriptor(
        name="bybit_oi",
        collection="bybit_open_interest",
        feature_tag="derivatives",
        columns={
            # ffill+bfill: ffill FIRST, then bfill leading NaNs with earliest value.
            # This is the only column that uses this method — OI needs both ends filled.
            "oi_value": ColumnSpec("oi_value", fill_value=0.0, reindex_method="ffill+bfill"),
        },
    ),

    "bybit_ls_ratio": DataSourceDescriptor(
        name="bybit_ls_ratio",
        collection="bybit_ls_ratio",
        feature_tag="derivatives",
        columns={
            "buy_ratio": ColumnSpec("buy_ratio", fill_value=0.5),
        },
        expected_ranges={"buy_ratio": (0.0, 1.0)},
    ),

    "binance_funding": DataSourceDescriptor(
        name="binance_funding",
        collection="binance_funding_rates",
        feature_tag="derivatives",
        columns={
            "funding_rate": ColumnSpec("binance_funding_rate", fill_value=0.0),
        },
        expected_ranges={"funding_rate": (-0.01, 0.01)},
    ),

    # ═══════════════════════════════════════════════════════════
    # Coinalyze (tag: "derivatives" for liquidations, "cross_exchange_oi" for OI)
    # Liquidations: hourly preferred, daily fallback
    # ═══════════════════════════════════════════════════════════

    "coinalyze_liq": DataSourceDescriptor(
        name="coinalyze_liq",
        collection="coinalyze_liquidations",
        feature_tag="derivatives",
        extra_filter={"resolution": "1hour"},
        fallback_filter={"resolution": "daily"},
        fallback_min_docs=100,
        columns={
            "long_liquidations_usd": ColumnSpec("long_liquidations_usd", fill_value=0.0),
            "short_liquidations_usd": ColumnSpec("short_liquidations_usd", fill_value=0.0),
            "total_liquidations_usd": ColumnSpec("total_liquidations_usd", fill_value=0.0),
        },
    ),

    "coinalyze_oi": DataSourceDescriptor(
        name="coinalyze_oi",
        collection="coinalyze_oi",
        feature_tag="cross_exchange_oi",
        extra_filter={"resolution": "1hour"},
        fallback_filter={"resolution": "daily"},
        fallback_min_docs=100,
        columns={
            "oi_close": ColumnSpec("coinalyze_oi_close", fill_value=0.0),
        },
    ),

    # ═══════════════════════════════════════════════════════════
    # Hyperliquid + cross-venue funding (tag: "funding_spread")
    # Used by: X8
    # ═══════════════════════════════════════════════════════════

    "hl_funding": DataSourceDescriptor(
        name="hl_funding",
        collection="hyperliquid_funding_rates",
        feature_tag="funding_spread",
        columns={
            "funding_rate": ColumnSpec("hl_funding_rate", fill_value=0.0),
        },
        expected_ranges={"funding_rate": (-0.05, 0.05)},
    ),

    "bybit_funding_for_spread": DataSourceDescriptor(
        name="bybit_funding_for_spread",
        collection="bybit_funding_rates",
        feature_tag="funding_spread",
        columns={
            "funding_rate": ColumnSpec("bybit_funding_rate", fill_value=0.0),
        },
    ),

    "binance_funding_for_spread": DataSourceDescriptor(
        name="binance_funding_for_spread",
        collection="binance_funding_rates",
        feature_tag="funding_spread",
        columns={
            "funding_rate": ColumnSpec("binance_funding_rate", fill_value=0.0),
        },
    ),

    # ═══════════════════════════════════════════════════════════
    # Sentiment (tag: "sentiment")
    # ═══════════════════════════════════════════════════════════

    "fear_greed": DataSourceDescriptor(
        name="fear_greed",
        collection="fear_greed_index",
        feature_tag="sentiment",
        pair_scoped=False,  # Global data, not per-pair
        columns={
            "value": ColumnSpec("fear_greed_value", fill_value=50.0),
        },
        expected_ranges={"value": (0, 100)},
    ),

    # ═══════════════════════════════════════════════════════════
    # Deribit volatility (tag: "volatility" for DVol, "options" for surface)
    # ═══════════════════════════════════════════════════════════

    "deribit_dvol": DataSourceDescriptor(
        name="deribit_dvol",
        collection="deribit_dvol",
        feature_tag="volatility",
        pair_field="currency",  # Keyed by "BTC"/"ETH", not "BTC-USDT"
        columns={
            "dvol_close": ColumnSpec("dvol_close", fill_value=0.0),
        },
    ),

    "deribit_options": CompositeDataSourceDescriptor(
        name="deribit_options",
        collection="deribit_options_surface",
        feature_tag="options",
        merge_fn="app.data_sources.mergers.merge_options_surface_sync",
    ),

    # ═══════════════════════════════════════════════════════════
    # HL price data (for cross-venue premium signal)
    "hl_candles_1h": DataSourceDescriptor(
        name="hl_candles_1h",
        collection="hyperliquid_candles_1h",
        feature_tag="hl_price",
        sort_field="timestamp_utc",
        sort_unit="ms",
        columns={
            "close": ColumnSpec("hl_close", fill_value=0.0),
        },
    ),

    # HL whale data (tag: "whale")
    # Used by: D1 research, future whale-signal strategies
    # ═══════════════════════════════════════════════════════════

    "whale_consensus": DataSourceDescriptor(
        name="whale_consensus",
        collection="hl_whale_consensus",
        feature_tag="whale",
        pair_field="coin",  # Uses "coin" not "pair"
        sort_field="timestamp_utc",
        sort_unit="datetime",  # This collection uses Python datetime objects
        columns={
            "net_bias": ColumnSpec("whale_net_bias", fill_value=0.0),
            "total_notional": ColumnSpec("whale_notional", fill_value=0.0),
        },
    ),
}


def get_sources_for_tag(tag: str) -> List[Union[DataSourceDescriptor, CompositeDataSourceDescriptor]]:
    """Return all data sources matching a feature tag."""
    return [ds for ds in DATA_SOURCE_REGISTRY.values() if ds.feature_tag == tag]


def get_all_tags() -> Set[str]:
    """Return all unique feature tags across all registered data sources."""
    return {ds.feature_tag for ds in DATA_SOURCE_REGISTRY.values()}


def validate_registry() -> List[str]:
    """Validate all registry entries at import time.

    Checks:
    - CompositeDataSourceDescriptor merge_fn paths resolve to importable functions
    - merge_fn signatures match expected (db, candle_df, pair, start_ts, end_ts)
    - No duplicate descriptor names

    Returns list of error messages (empty = valid).
    """
    errors = []
    keys_seen = set()
    names_seen = set()

    for key, ds in DATA_SOURCE_REGISTRY.items():
        # Check for duplicate dict keys (can't happen in literal, but guard for runtime adds)
        if key in keys_seen:
            errors.append(f"Duplicate registry key: {key}")
        keys_seen.add(key)

        # Check for duplicate ds.name across values (the actual identity)
        if ds.name in names_seen:
            errors.append(f"Duplicate descriptor name '{ds.name}' (key='{key}')")
        names_seen.add(ds.name)

        # Validate CompositeDataSourceDescriptor merge functions
        if isinstance(ds, CompositeDataSourceDescriptor):
            for fn_attr in ("merge_fn", "merge_fn_async"):
                fn_path = getattr(ds, fn_attr, None)
                if fn_path is None:
                    continue
                try:
                    module_path, fn_name = fn_path.rsplit(".", 1)
                    mod = importlib.import_module(module_path)
                    fn = getattr(mod, fn_name)
                    # Check signature has 5 params: db, candle_df, pair, start_ts, end_ts
                    sig = inspect.signature(fn)
                    params = list(sig.parameters.keys())
                    expected = ["db", "candle_df", "pair", "start_ts", "end_ts"]
                    if params != expected:
                        errors.append(
                            f"{name}.{fn_attr}: signature mismatch. "
                            f"Expected {expected}, got {params}"
                        )
                except (ImportError, AttributeError) as e:
                    errors.append(f"{name}.{fn_attr}: cannot resolve '{fn_path}': {e}")

    if errors:
        for err in errors:
            logger.error(f"Registry validation: {err}")

    return errors
