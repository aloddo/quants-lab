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

## V2 DataEngine (multi-resolution)

**File**: `app/data_engine.py`

```python
from app.data_engine import DataEngine, FeatureRequest

engine = DataEngine()
df = engine.get_features(
    pair="BTC-USDT",
    requests=[
        FeatureRequest("atr", "5m", "candles", {"period": 14}),
        FeatureRequest("funding_zscore", "1h", "derivatives", {"window": 30}),
        FeatureRequest("spread_p90", "1m", "arb_hl_bybit_spread_stats_1m"),
    ],
    start_ts=1776000000, end_ts=1777100000,
)
```

For backtesting with multi-resolution candles:
```python
engine.inject_into_backtest_engine(bt, pair="BTC-USDT", intervals=["5m", "15m"])
```

Available features in FEATURE_COMPUTE: atr, atr_percentile, ema, rsi, returns,
volume_zscore, funding_zscore, oi_rsi, oi_change_pct, ls_zscore, liq_zscore,
hl_funding_zscore, funding_spread, spread_p90, spread_mean, spread_exceedance.

## Tick Aggregation (arb data)

**File**: `app/tasks/data_collection/tick_aggregation_task.py`

Runs daily 04:00 UTC. Compresses 5s raw ticks to 1m distributional stats:
- Per-direction spreads (hl_over_bb, bb_over_hl): mean, std, min, max, p25-p90
- Exceedance: time_above_5/10/15bps
- Raw ticks kept 14d, stats kept 90d

**Arb screening**: `python scripts/analyze_arb_spreads.py --days 7`

## Continuous Validation

**File**: `app/tasks/backtesting/continuous_validation_task.py`

Weekly Sunday 3am UTC. Compares recent PF against pair_historical baseline.
Alerts via Telegram on degradation (PF drop) or critical (PF < 1.0).
Currently scoped to X10.

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

## Standalone Sources (not in registry, consumed directly at tick level)

These collections need SUB-SECOND data for arb strategies. They do NOT go through
the 1h candle merge engine. The feature store serves directional strategies (1h).
Arb strategies operate on entirely separate data at different resolution.

| Collection | Consumer | Frequency | Collector Script | tmux Session |
|-----------|----------|-----------|-----------------|--------------|
| arb_bb_spot_perp_snapshots | TierEngine (H2) | 5s | `scripts/arb_dual_collector.py` | `arb-dual-collector` |
| arb_bn_usdc_bb_perp_snapshots | TierEngine (H2) | 5s | `scripts/arb_dual_collector.py` | `arb-dual-collector` |
| **arb_hl_bybit_perp_snapshots** | **HL-Bybit arb research** | **5s** | **`scripts/arb_hl_bybit_collector.py`** | **`arb-hl-bybit`** |
| hyperliquid_l2_snapshots_1s | Research | 1s | pipeline task | pipeline |
| hyperliquid_recent_trades_1s | Research | 1s | pipeline task | pipeline |

### Starting/Stopping Collectors

```bash
# HL-Bybit spread collector (30 pairs, 5s polls)
tmux new-session -d -s arb-hl-bybit "set -a && source .env && set +a && python scripts/arb_hl_bybit_collector.py --max-pairs 30"

# Dual venue collector (Bybit spot/perp + Binance USDC)
tmux new-session -d -s arb-dual-collector "set -a && source .env && set +a && python scripts/arb_dual_collector.py"
```

## Two Data Tiers (know which one you're working in)

| Tier | Resolution | Merge Engine | Strategies | Files |
|------|-----------|--------------|------------|-------|
| **Directional** | 1h candles | Feature Store (registry + merge) | X10, X8, E3, future directional | `app/data_sources/` |
| **Arb/HFT** | 1-5s ticks | Direct MongoDB queries + WebSocket | H2 V3, future HL-Bybit arb | `app/services/arb/`, standalone collectors |

The feature store does NOT serve arb strategies. They need tick-level data that
the 1h merge engine can't provide. When working on arb, use the standalone
collectors and direct MongoDB aggregation pipelines, not the feature store.

## Backfill Procedures

### When adding a new data source

Every new collector MUST have a backfill mechanism. The pipeline only fetches
recent data incrementally. Historical data for backtesting requires explicit backfill.

### Current backfill scripts

| Script | Purpose | Usage |
|--------|---------|-------|
| `scripts/backfill_hyperliquid_history.py` | HL candles + funding | `python scripts/backfill_hyperliquid_history.py --days 365 --max-pairs 65` |
| `scripts/fix_oi_duplicates.py` | OI dedup (one-time) | Already run |

### Backfill needed (data deleted by incorrect 90d retention, now fixed to 730d)

| Collection | Current Start | Original Start | Action |
|-----------|--------------|----------------|--------|
| bybit_funding_rates | 2026-01-25 | 2020-03-25 | Re-backfill via Bybit API |
| bybit_open_interest | 2026-01-25 | 2021-04 | Re-backfill via Bybit API |
| bybit_ls_ratio | 2026-01-25 | 2024-11 | Re-backfill via Bybit API |
| binance_funding_rates | 2026-01-25 | 2020-01 | Re-backfill via Binance API |

### How to backfill a new collection

1. Write a backfill script in `scripts/` (see `backfill_hyperliquid_history.py` as template)
2. The script should: query the API with historical date range, upsert to MongoDB with dedup
3. Run it BEFORE adding the source to the pipeline (pipeline only fetches recent)
4. Document the script in this skill file under "Current backfill scripts"
5. Add the collection to `RETENTION_CONFIG` in `data_retention_task.py`
