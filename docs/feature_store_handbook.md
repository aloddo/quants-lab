# Feature Store & Signal Factory Handbook

Last updated: 2026-04-25

## Architecture

```
DATA COLLECTION (pipeline tasks + standalone collectors)
  |  writes to MongoDB collections + parquet files
  v
DATA SOURCE REGISTRY (app/data_sources/registry.py)
  |  declares: collection -> columns -> fill values -> feature tag
  |  supports: flat (DataSourceDescriptor) + composite (CompositeDataSourceDescriptor)
  v
GENERIC MERGE ENGINE (app/data_sources/merge.py)
  |  merges any registered source into 1h candle DataFrames
  |  used by: _backtest_worker, bulk_backtest_task, walk_forward_task
  v
FEATURE COMPUTATION (app/features/ + FeatureComputationTask)
  |  computes derived features (ATR, momentum, regime, etc.)
  |  stores to MongoDB "features" collection
  v
STRATEGIES (app/engines/strategy_registry.py)
  |  declare required_features tags
  |  controllers read enriched candle DataFrames
  v
MONITORING (feature-catalog CLI + DataRetentionTask)
```

## Data Source Inventory

Run `python cli.py feature-catalog` for live status. Current sources:

| Source | Collection | Tag | Columns | Update | Retention |
|--------|-----------|-----|---------|--------|-----------|
| bybit_funding | bybit_funding_rates | derivatives | funding_rate | 15min | 90d |
| bybit_oi | bybit_open_interest | derivatives | oi_value | 15min | 90d |
| bybit_ls_ratio | bybit_ls_ratio | derivatives | buy_ratio | 15min | 90d |
| binance_funding | binance_funding_rates | derivatives | binance_funding_rate | 15min | 90d |
| coinalyze_liq | coinalyze_liquidations | derivatives | long/short/total_liquidations_usd | 15min | 365d |
| coinalyze_oi | coinalyze_oi | cross_exchange_oi | coinalyze_oi_close | 15min | 365d |
| hl_funding | hyperliquid_funding_rates | funding_spread | hl_funding_rate | 15min | 365d |
| fear_greed | fear_greed_index | sentiment | fear_greed_value | daily | forever |
| deribit_dvol | deribit_dvol | volatility | dvol_close | 15min | 365d |
| deribit_options | deribit_options_surface | options | (custom merge) | 15min | 180d |
| whale_consensus | hl_whale_consensus | whale | whale_net_bias, whale_notional | 15min | n/a |

### Standalone tick-level sources (arb tier, NOT in feature store)

The feature store operates at 1h resolution for directional strategies. Arb
strategies need tick-level data (1-5s) that flows through completely separate
collectors. Do NOT try to merge these into 1h candle DataFrames.

| Collection | Consumer | Frequency | Collector |
|-----------|----------|-----------|-----------|
| arb_bb_spot_perp_snapshots | TierEngine (H2) | 5s | `scripts/arb_dual_collector.py` |
| arb_bn_usdc_bb_perp_snapshots | TierEngine (H2) | 5s | `scripts/arb_dual_collector.py` |
| **arb_hl_bybit_perp_snapshots** | **HL-Bybit arb** | **5s** | **`scripts/arb_hl_bybit_collector.py`** |
| hyperliquid_l2_snapshots_1s | Research | 1s | pipeline microstructure task |
| hyperliquid_recent_trades_1s | Research | 1s | pipeline microstructure task |

## How to Add a New Data Source

### Step 1: Write the collector task

Create `app/tasks/data_collection/your_task.py` extending `BaseTask`:
```python
from core.tasks import BaseTask, TaskContext

class YourDataTask(BaseTask):
    async def execute(self, context: TaskContext):
        # Query external API
        # Write to MongoDB with upsert (dedup)
        pass
```

### Step 2: Add to pipeline

In `config/hermes_pipeline.yml`:
```yaml
your_data:
  enabled: true
  task_class: app.tasks.data_collection.your_task.YourDataTask
  schedule:
    type: cron
    cron: "*/15 * * * *"
    timezone: UTC
  config:
    collection_name: "your_collection"
```

### Step 3: Register in the Data Source Registry

In `app/data_sources/registry.py`, add a `DataSourceDescriptor`:
```python
"your_source": DataSourceDescriptor(
    name="your_source",
    collection="your_collection",
    feature_tag="your_tag",      # strategies use this to opt in
    columns={
        "your_field": ColumnSpec("your_column_name", fill_value=0.0),
    },
    expected_ranges={"your_field": (-1.0, 1.0)},  # for quality monitoring
),
```

For multi-dimensional sources (options surface, L2 book), use `CompositeDataSourceDescriptor` and add a custom merge function in `app/data_sources/mergers.py`.

### Step 4: Tell strategies about it

In `app/engines/strategy_registry.py`, add the tag to your strategy's `required_features`:
```python
required_features=["derivatives", "your_tag"],
```

### Step 5: Add retention config

In `app/tasks/data_collection/data_retention_task.py`, add to `RETENTION_CONFIG`:
```python
"your_collection": {"field": "timestamp_utc", "unit": "ms", "days": 90},
```

That's it. The generic merge engine picks it up for backtests. The catalog reports it. The retention task cleans it up.

### Step 6 (if needed in live mode): Add to controller

If the controller needs this data in live mode (not just backtest), add to its `collections_config`:
```python
collections_config = [
    ("your_collection", "timestamp_utc", "your_field", "your_column_name", 0.0, "ms"),
]
return self._merge_from_mongodb(df, pair, collections_config)
```

## How to Add a New Feature

### Step 1: Create the feature class

Create `app/features/your_feature.py`:
```python
from app.features.decorators import screening_feature
from core.features.feature_base import FeatureBase, FeatureConfig
from core.features.models import Feature

class YourConfig(FeatureConfig):
    name: str = "your_feature"

@screening_feature
class YourFeature(FeatureBase[YourConfig]):
    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        # Add columns to DataFrame
        return data

    def create_feature(self, candles) -> Feature:
        # Extract latest values
        return Feature(...)
```

### Step 2: Register the import

In `app/features/__init__.py`, add a fault-isolated import:
```python
try:
    from app.features.your_feature import YourFeature, YourConfig
except ImportError as e:
    logger.warning(f"Failed to import YourFeature: {e}")
```

The `@screening_feature` decorator handles registration. No need to edit `ALL_FEATURES` manually.

### Step 3 (if MongoDB-sourced): Add data pass

If the feature reads from MongoDB (like DerivativesFeature), implement `create_feature_from_data()` and add a computation pass in `FeatureComputationTask`.

## Merge Pipeline Reference

### How the generic merge works

1. `merge_all_for_engine_sync(db, engine_name, candle_df, pair)` reads the strategy's `required_features`
2. For each tag, finds all matching `DataSourceDescriptor` entries
3. For each descriptor, queries MongoDB with pair filter + optional time range
4. Reindexes to candle timestamps (ffill/bfill per ColumnSpec)
5. Fills remaining NaN with neutral values

### Fill value conventions

| Data Type | Fill Value | Rationale |
|-----------|-----------|-----------|
| Funding rate | 0.0 | No funding = no signal |
| OI value | 0.0 (with bfill) | Use earliest known, else zero |
| Buy ratio | 0.5 | Balanced = no crowd signal |
| Liquidations | 0.0 | No liquidation = no event |
| Fear & Greed | 50.0 | Neutral sentiment |
| DVol | 0.0 | Missing = no vol data |

### ffill+bfill semantics

For `oi_value` only. ORDER MATTERS:
1. **ffill first** — propagate last known value forward
2. **bfill second** — fill leading NaNs with earliest known value

This ensures: (a) no gaps within the series, (b) bars before first OI doc get the earliest known value rather than zero.

### Time-range filtering

`merge_source_sync` accepts `start_ts` and `end_ts` (millis). When provided:
- Query filter: `timestamp_utc >= (start_ts - 24h buffer)` AND `<= end_ts`
- The 24h buffer ensures ffill has an anchor before the window starts
- Critical for walk-forward: prevents future data leaking via forward-fill

## Timestamp Conventions

| Convention | Used By | Example |
|-----------|---------|---------|
| Int64 millis UTC | Most MongoDB collections | `1777107600000` |
| Float seconds | Arb service, some legacy | `1777107600.0` |
| Python datetime | Features collection, task_executions | `datetime(2026, 4, 25, ...)` |
| Pair naming: `BTC-USDT` | All pipeline collections | Hyphenated |
| Symbol naming: `BTCUSDT` | Arb collections | No separator (Bybit native) |

The `DataSourceDescriptor.sort_unit` field handles translation. No need to convert stored data.

## Monitoring & Retention

### DataRetentionTask

Runs daily at 05:30 UTC. Deletes docs older than configured retention per collection. Config in `RETENTION_CONFIG` dict. Replaces broken MongoDB TTL indexes (which required datetime fields but our collections use Int64 millis).

### Feature Catalog

`python cli.py feature-catalog` — shows all sources, doc counts, freshness, consumers, quality issues.

Health statuses:
- **healthy**: latest doc within expected freshness threshold
- **stale**: latest doc older than threshold (check collector)
- **empty**: zero docs (collection exists but no data)
- **missing**: collection doesn't exist in MongoDB

### Data Quality Checks

Descriptors with `expected_ranges` get quality checks. StorageGuard samples latest docs and flags values outside range. Example: `funding_rate` outside `[-0.01, 0.01]` = anomaly.

## Troubleshooting

### Zero trades in backtest

Most common cause: NaN in a merged column. `BacktestingEngineBase.prepare_market_data()` calls `dropna(inplace=True)` which kills rows with ANY NaN. Check:
1. Does the collection have data for the pair in the backtest time range?
2. Is the `fill_value` set correctly in the descriptor?
3. Run `python cli.py feature-catalog` to check freshness

### Stale features

Check `python cli.py feature-catalog` for stale sources. Common causes:
- Collector task failed (check `task_executions` collection)
- External API changed (check collector logs in `tail -f /tmp/ql-pipeline-launchd.log`)
- Collection dropped accidentally

### Walk-forward lookahead

If walk-forward results look suspiciously good, check that `start_ts` and `end_ts` are passed to the merge engine. Without them, future data leaks via forward-fill.

### Adding a composite source

If your data doesn't fit the flat `DataSourceDescriptor` pattern (e.g., options surface with strike/expiry dimensions), use `CompositeDataSourceDescriptor`:
1. Write a merge function in `app/data_sources/mergers.py` with signature `(db, candle_df, pair, start_ts, end_ts) -> pd.DataFrame`
2. Register with `merge_fn` pointing to the function
3. Run `validate_registry()` to verify the signature matches
