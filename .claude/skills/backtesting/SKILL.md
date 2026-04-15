---
name: backtesting
description: "Run QuantsLab backtests via BacktestingEngine. Covers controller config, engine registry, walk-forward discipline, and all gotchas. Load when building or running backtests."
---

# QuantsLab Backtesting

## Known Limitations (read before trusting any result)

| Limitation | Impact |
|------------|--------|
| No order book | Fill = candle open after signal. Reality: queue behind makers |
| Candle-based | Intrabar dynamics lost. Same-candle TP+SL -> SL (worst-case) |
| No latency | Execution assumed instantaneous. Reality: 50-500ms |
| No market impact | Fine at current sizing, changes with scale |
| No funding | Estimate post-hoc: ~0.01%/8h, longs pay ~70% of time |

**Backtest = upper bound. Paper trading is mandatory before capital.**

---

## Engine Registry

All engine-specific configuration lives in `app/engines/strategy_registry.py`. Single source of truth.
All engines MUST have `deployment_mode="hb_native"`. The controller IS the strategy —
same code runs in backtest (BacktestingEngine) and live (HB bot container).

```python
from app.engines.strategy_registry import get_strategy, build_backtest_config

# Get metadata for any registered engine
meta = get_strategy("E1")  # resolution, intervals, exit_params, deployment_mode, etc.

# Build a complete backtest config -- handles candles, exits, trailing stop
config = build_backtest_config(engine_name="E1", connector="bybit_perpetual", pair="BTC-USDT")
```

### Resolution rules
Each engine declares its `backtesting_resolution` in the registry. The rule: resolution must match the finest-grained interval the controller's trigger layer operates on.
- Single-layer (e.g. 1h only): resolution = "1h"
- Two-layer (e.g. 1h setup + 5m trigger): resolution = "5m"
- **Never hard-code resolution** -- always read from registry.

### candles_config
Each engine's registry entry declares `intervals` (e.g. `["1h", "5m"]`). `build_backtest_config()` constructs the correct `CandlesConfig` list automatically. Controllers' `model_post_init` only adds the primary interval -- multi-interval engines need explicit config via the registry.

---

## BacktestingEngine Usage

```python
from core.backtesting import BacktestingEngine

backtesting = BacktestingEngine(load_cached_data=True)

result = await backtesting.run_backtesting(
    config=config,
    start=start_ts,      # unix timestamp (int)
    end=end_ts,
    backtesting_resolution=meta["backtesting_resolution"],
    trade_cost=0.000375,
)

# Workaround: close_types bug returns int(0) on zero trades
if not isinstance(result.results.get("close_types"), dict):
    result.results["close_types"] = {}

print(result.get_results_summary())         # text summary
fig = result.get_backtesting_figure()        # plotly: candlestick + entries/exits + cum PnL
df = result.executors_df                     # DataFrame: one row per trade
pdf = result.processed_data                  # features DataFrame from controller
```

---

## Bulk Backtest

**Notebook**: `research_notebooks/eda_strategies/e1_compression_breakout/05_bulk_backtest.ipynb` -- works for any engine. Change `ENGINE = "E1"` or `"E2"`.

**Automated task**: `BulkBacktestTask` reads resolution from the registry. Pipeline config only needs `engine` and `connector_name` -- no resolution or controller_config overrides.

**Verdict thresholds**: PF >= 1.3 -> ALLOW, PF >= 1.0 -> WATCH, else BLOCK. Minimum 10 trades.

---

## Walk-Forward Discipline

**NEVER run multiple windows to find "where the strategy works best." That is overfitting.**

### Stress test windows (same params, different regimes)
- Bear Crash: 2021-11-01 -> 2022-06-30
- FTX Shock: 2022-11-01 -> 2022-11-30
- Low-Vol Ranging: 2023-01-01 -> 2023-09-30
- Bull Validation: 2024-07-01 -> 2025-01-01 (LOCKED)
- Out-of-sample: 2025-01-01 -> today (DO NOT RUN until ready)

---

## Fee Reference

- Bybit taker: 0.055% | Bybit maker: +0.020%
- Blended (limit entry + market exit): 0.075% RT = `trade_cost=0.000375`
- Funding NOT modeled. Estimate: ~0.01%/8h, small impact at avg hold < 24h.

---

## Memory Safety — Subprocess Isolation (Apr 2026)

BulkBacktestTask runs each pair's BacktestingEngine in a **subprocess** via `_backtest_worker.py`.
Each pair uses 3-5 GB at 1m resolution. Without subprocess isolation, sequential pairs accumulate
memory until OOM crashes the server (happened Apr 15 — rebooted machine, lost Docker, orphaned 9 positions).

### Architecture
- **Parent** (BulkBacktestTask.execute): loads shared candles once for funding PnL, manages MongoDB writes
- **Child** (`_backtest_worker.py`): loads parquet, merges derivatives, runs engine, returns results via pickle
- When child exits, OS reclaims ALL memory. Parent stays at ~100MB.

### Critical gotchas
- **Pickle dtype issue**: HB's executor DataFrame uses Decimal/PyArrow StringDtype internally. After pickle round-trip, numeric columns like `net_pnl_quote` arrive as strings. MUST call `pd.to_numeric(edf[col], errors="coerce")` in the parent before any arithmetic.
- **Never use standalone backtest scripts**: All backtests go through BulkBacktestTask. It handles trade storage, funding PnL, verdicts, quality metrics. Standalone scripts that skip this produce incomplete data.
- **Parquet end time**: The worker reads parquet end time to cap the backtest window. Never use `now()` — it triggers live API fetches that fail under load.

---

## Gotchas (all discovered through trial and error)

1. **`id` is REQUIRED** -- no default. Every config needs a unique id string.
2. **Resolution must match trigger layer** -- use registry, never hard-code. Wrong resolution = 0 trades.
3. **Multi-interval engines need explicit candles_config** -- model_post_init only adds the primary interval.
4. **`ema_trend = float("nan")` kills everything** -- engine's dropna() removes all rows. Use 0.0 when filter disabled.
5. **Bool -> int conversion timing** -- must happen AFTER signal column is built.
6. **close_types bug** -- returns int(0) on zero trades. Check isinstance before get_results_summary().
7. **`triple_barrier_config` doesn't exist** -- use flat fields: stop_loss, take_profit, time_limit, trailing_stop.
8. **stop_loss/take_profit are Decimal** -- use Decimal("0.015"), not floats.
9. **Signal must be a column in features DataFrame** -- not just a scalar. Engine reads df["signal"] via merge_asof.
10. **Features DataFrame needs timestamp column** -- unix seconds (float).
11. **`get_controller_config_instance_from_dict`** uses `controllers_module="controllers"` but our controllers are at `app.controllers`. Use `build_backtest_config()` from the registry instead.
12. **Adding a new engine**: add entry to `app/engines/registry.py` -- notebook and task auto-adapt.
13. **Fresh BacktestingEngine per pair** -- reusing across multiple `run_backtesting` calls corrupts state. Create per pair + `gc.collect()` after each.
14. **max_records controls API buffer, not data access** -- large values (500+) cause start_time - buffer to exceed cached data, triggering API fetch that fails intermittently. Use small values (50). Controller reads full cached feed directly via candles_feeds.
15. **Smoke test before bulk runs** -- run one pair inline, verify output format and no crashes, THEN run the loop. Saves hours of wasted runtime.
16. **Background commands: no grep pipes** -- use `python -u script.py 2>/dev/null`. Grep on piped output buffers everything and produces 0-byte output files.
17. **Live params MUST match backtest params** -- features, signal scan, and engine eval can use different thresholds than the HB controller. After any backtest validation, explicitly verify alignment. ATR threshold mismatch (0.20 live vs 0.35 backtest) caused 0 live signals despite 2,173 backtest trades.
18. **Feature store should be strategy-agnostic** -- store raw values (ATR percentile, volume ratio), apply thresholds in signal scan. Baked flags (compression_flag, vol_floor_passed) require feature recomputation when thresholds change.
