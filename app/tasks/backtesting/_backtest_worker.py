"""
Subprocess worker for BulkBacktestTask — runs one pair's backtest in isolation.

Called by BulkBacktestTask via subprocess.run(). Loads parquet, merges
data sources from MongoDB via the generic merge engine, runs BacktestingEngine,
serializes results + full executor DataFrame to a pickle file. Parent reads it back.

This ensures full memory reclaim between pairs (3-5 GB per pair at 1m).
All MongoDB writes happen in the parent — this worker only computes.

Usage (called by parent, not directly):
    python -m app.tasks.backtesting._backtest_worker \\
        --engine E3 --connector bybit_perpetual --pair BTC-USDT \\
        --start 1744722000 --end 1776258000 --resolution 1m \\
        --trade-cost 0.000375 --output /tmp/bt_result_XXXX.pkl
"""
import argparse
import asyncio
import logging
import pickle
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient

# Suppress noisy logging in subprocess
logging.basicConfig(level=logging.WARNING, stream=sys.stderr)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from core.backtesting.engine import BacktestingEngine
from core.data_paths import data_paths
from app.engines.strategy_registry import get_strategy, build_backtest_config


def _merge_all_sources(bt, pair: str, connector: str, engine_name: str,
                       start_ts=None, end_ts=None):
    """Merge ALL data sources required by engine into cached candle DataFrame.

    Uses the generic merge engine (app.data_sources.merge) which reads from the
    Data Source Registry. Replaces the old hardcoded _merge_derivatives_sync and
    _merge_funding_spread_sync functions.

    Args:
        bt: BacktestingEngine instance with loaded candle data.
        pair: Trading pair (e.g., "BTC-USDT").
        connector: Connector name (e.g., "bybit_perpetual").
        engine_name: Strategy name (e.g., "E3", "X10").
        start_ts: Optional start timestamp (unix seconds) for time-range filter.
        end_ts: Optional end timestamp (unix seconds) for time-range filter.
    """
    feed_key = f"{connector}_{pair}_1h"
    provider = bt._bt_engine.backtesting_data_provider
    df = provider.candles_feeds.get(feed_key)
    if df is None or len(df) == 0:
        return

    from app.data_sources.merge import merge_all_for_engine_sync

    db = MongoClient("mongodb://localhost:27017").quants_lab

    # Convert seconds to millis for the merge engine
    start_ms = int(start_ts * 1000) if start_ts else None
    end_ms = int(end_ts * 1000) if end_ts else None

    provider.candles_feeds[feed_key] = merge_all_for_engine_sync(
        db, engine_name, df, pair, start_ts=start_ms, end_ts=end_ms
    )


async def run_backtest(engine_name, connector, pair, start_ts, end_ts, resolution, trade_cost):
    """Run backtest for one pair. Returns (results_dict, executors_df_or_None)."""
    engine_meta = get_strategy(engine_name)

    bt = BacktestingEngine(load_cached_data=True)

    # Merge all required data sources via generic merge engine
    if engine_meta.required_features:
        _merge_all_sources(bt, pair, connector, engine_name, start_ts, end_ts)

    config = build_backtest_config(engine_name=engine_name, connector=connector, pair=pair)

    bt_result = await bt.run_backtesting(
        config=config, start=start_ts, end=end_ts,
        backtesting_resolution=resolution, trade_cost=trade_cost,
    )

    # Fix close_types=0 bug
    if not isinstance(bt_result.results.get("close_types"), dict):
        bt_result.results["close_types"] = {}

    # Serialize close_type enum keys to strings
    r = bt_result.results
    ct = r.get("close_types", {})
    r["close_types"] = {
        (k.name if hasattr(k, "name") else str(k)): v for k, v in ct.items()
    }

    # Extract executor DataFrame if available
    edf = None
    try:
        edf = bt_result.executors_df
        if edf is not None and len(edf) > 0:
            # Cast numeric columns to float — HB returns Decimal/string types
            # that break pandas comparisons after pickle round-trip
            numeric_cols = ["net_pnl_quote", "net_pnl_pct", "cum_fees_quote",
                           "filled_amount_quote", "timestamp", "close_timestamp"]
            for col in numeric_cols:
                if col in edf.columns:
                    edf[col] = pd.to_numeric(edf[col], errors="coerce")
            # Convert enum/object columns to strings for pickle safety
            for col in edf.columns:
                if edf[col].dtype == object:
                    edf[col] = edf[col].apply(lambda x: x.name if hasattr(x, "name") else str(x) if not isinstance(x, (int, float, str, type(None))) else x)
        else:
            edf = None
    except (KeyError, AttributeError):
        edf = None

    return r, edf


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True)
    parser.add_argument("--connector", required=True)
    parser.add_argument("--pair", required=True)
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    parser.add_argument("--resolution", required=True)
    parser.add_argument("--trade-cost", type=float, required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    try:
        results, edf = asyncio.run(run_backtest(
            args.engine, args.connector, args.pair,
            args.start, args.end, args.resolution, args.trade_cost,
        ))
        payload = {"status": "ok", "results": results, "executors_df": edf}
    except Exception as e:
        import traceback
        payload = {"status": "error", "error": f"{type(e).__name__}: {str(e)}", "traceback": traceback.format_exc()}

    with open(args.output, "wb") as f:
        pickle.dump(payload, f)


if __name__ == "__main__":
    main()
