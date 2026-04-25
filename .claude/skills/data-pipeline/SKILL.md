---
name: data-pipeline
description: "Feature store and data pipeline operations. Load when adding data sources, features, debugging data quality, or checking pipeline health. Covers the Data Source Registry, generic merge engine, feature computation, retention, and monitoring."
disable-model-invocation: true
---

# Data Pipeline & Feature Store

Load this skill when:
- Adding a new data source or feature
- Debugging data quality or stale features
- Checking pipeline health
- Modifying merge logic or retention config
- Working on any strategy that needs new data

## Architecture

```
DATA COLLECTION (pipeline tasks + standalone collectors)
  ↓ writes to MongoDB / parquet
DATA SOURCE REGISTRY (app/data_sources/registry.py — SINGLE registration point)
  ↓ DataSourceDescriptor (flat) or CompositeDataSourceDescriptor (multi-dim)
GENERIC MERGE ENGINE (app/data_sources/merge.py)
  ↓ merge_all_for_engine_sync(db, engine_name, df, pair, start_ts, end_ts)
  ↓ used by: _backtest_worker, bulk_backtest_task, walk_forward_task
FEATURE COMPUTATION (app/features/ with @screening_feature decorator)
  ↓ FeatureComputationTask runs all features, upserts to MongoDB
MONITORING (python cli.py feature-catalog)
```

## Key Files

| File | Purpose |
|------|---------|
| `app/data_sources/__init__.py` | DataSourceDescriptor, CompositeDataSourceDescriptor, ColumnSpec |
| `app/data_sources/registry.py` | DATA_SOURCE_REGISTRY — all data sources registered here |
| `app/data_sources/merge.py` | Generic merge engine (sync + async) |
| `app/data_sources/mergers.py` | Custom merge functions for composite sources |
| `app/data_sources/catalog.py` | Feature catalog builder |
| `app/features/__init__.py` | ALL_FEATURES via @screening_feature decorator |
| `app/features/decorators.py` | @screening_feature decorator |
| `app/tasks/data_collection/data_retention_task.py` | Scheduled cleanup (RETENTION_CONFIG) |
| `docs/feature_store_handbook.md` | Full reference documentation |

## How to Add a New Data Source (5 steps)

1. **Write collector task** in `app/tasks/data_collection/`
2. **Add to pipeline** in `config/hermes_pipeline.yml`
3. **Register descriptor** in `app/data_sources/registry.py`:
   ```python
   "your_source": DataSourceDescriptor(
       name="your_source",
       collection="your_collection",
       feature_tag="your_tag",
       columns={"field": ColumnSpec("col_name", fill_value=0.0)},
   ),
   ```
4. **Add tag to strategy** in `strategy_registry.py`: `required_features=["your_tag"]`
5. **Add retention** in `data_retention_task.py` RETENTION_CONFIG

## How to Add a New Feature (3 steps)

1. Create `app/features/your_feature.py` with `@screening_feature` decorator
2. Add fault-isolated import to `app/features/__init__.py`
3. If MongoDB-sourced: implement `create_feature_from_data()` + add pass in FeatureComputationTask

## Critical Rules

### Fill Values (NaN safety)
BacktestingEngine calls `dropna(inplace=True)`. ANY NaN kills the row. Always specify fill values:
- `0.0` for additive quantities (funding_rate, liquidations)
- `0.5` for ratios centered at 0.5 (buy_ratio)
- `50.0` for indices centered at 50 (Fear & Greed)

### ffill+bfill Semantics
For `oi_value` ONLY. ORDER MATTERS: ffill FIRST, then bfill leading NaNs.

### Time-Range Filtering
Walk-forward backtests MUST pass `start_ts`/`end_ts` to prevent lookahead bias. The merge engine adds a 24h buffer before start for ffill anchor.

### Retention Thresholds
Backtest-critical collections (funding, OI, LS, liquidations) need **730d** retention. Backtests run up to 365d and walk-forward needs buffer. NEVER set below 400d for these.

### Controller Self-Containment
Controllers run in Docker — no `app.*` imports allowed. For live data, use the standard `_merge_from_mongodb` template:
```python
collections_config = [
    ("collection", "ts_field", "val_field", "col_name", fill_val, "ms"),
]
return self._merge_from_mongodb(df, pair, collections_config)
```

## Health Check

```bash
python cli.py feature-catalog       # Dashboard with health status
python cli.py feature-catalog --json # Machine-readable
```

## Current Data Sources (13 registered)

| Tag | Sources | Consumers |
|-----|---------|-----------|
| derivatives | bybit_funding, bybit_oi, bybit_ls_ratio, binance_funding, coinalyze_liq | E2-E4, X1-X10, H2, M1 |
| funding_spread | hl_funding, bybit_funding_for_spread, binance_funding_for_spread | X8 |
| cross_exchange_oi | coinalyze_oi | (none yet) |
| sentiment | fear_greed | (none yet) |
| volatility | deribit_dvol | (none yet) |
| options | deribit_options (composite) | (none yet) |
| whale | whale_consensus | (none yet) |

## Standalone Sources (not in registry)

| Collection | Consumer | Notes |
|-----------|----------|-------|
| arb_bb_spot_perp_snapshots | TierEngine (H2) | 21M+ docs, consumed via aggregation pipeline |
| arb_bn_usdc_bb_perp_snapshots | TierEngine (H2) | 18M+ docs |
| hyperliquid_l2_snapshots_1s | Research | 747K docs |
| hyperliquid_recent_trades_1s | Research | 1.5M docs |

## Backfill Needed

The following collections had data deleted by the initial (incorrect) 90d retention run:
- `bybit_funding_rates` — now starts 2026-01-25 (was 2020-03-25)
- `bybit_open_interest` — now starts 2026-01-25 (was 2021-04)
- `bybit_ls_ratio` — now starts 2026-01-25 (was 2024-11)
- `binance_funding_rates` — now starts 2026-01-25 (was 2020-01)

Retention is now 730d. Data needs re-backfilling via Bybit/Binance historical APIs.
