"""
Subprocess worker for BulkBacktestTask — runs one pair's backtest in isolation.

Called by BulkBacktestTask via subprocess.run(). Loads parquet, merges
derivatives from MongoDB, runs BacktestingEngine, serializes results + full
executor DataFrame to a pickle file. Parent reads it back.

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


def _merge_derivatives_sync(bt, pair: str, connector: str):
    """Merge derivatives from MongoDB into cached 1h candle DataFrame.

    Mirrors BulkBacktestTask._merge_derivatives_into_candles() using pymongo (sync).
    """
    feed_key = f"{connector}_{pair}_1h"
    provider = bt._bt_engine.backtesting_data_provider
    df = provider.candles_feeds.get(feed_key)
    if df is None or len(df) == 0:
        return

    db = MongoClient("mongodb://localhost:27017").quants_lab
    query = {"pair": pair}
    candle_idx = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    def _merge_series(collection_name, ts_field, val_field, col_name, fill_val, method="ffill"):
        docs = list(db[collection_name].find(query).sort(ts_field, 1))
        if docs:
            sdf = pd.DataFrame(docs)
            if ts_field in sdf.columns and val_field in sdf.columns:
                sdf["ts"] = pd.to_datetime(sdf[ts_field], unit="ms", utc=True)
                sdf = sdf.set_index("ts")[[val_field]].sort_index()
                sdf = sdf[~sdf.index.duplicated(keep="last")]
                reindexed = sdf.reindex(candle_idx, method=method)[val_field]
                if method == "ffill" and col_name == "oi_value":
                    reindexed = reindexed.bfill()
                df[col_name] = reindexed.fillna(fill_val).values
                return
        df[col_name] = fill_val

    _merge_series("bybit_funding_rates", "timestamp_utc", "funding_rate", "funding_rate", 0.0)
    _merge_series("bybit_open_interest", "timestamp_utc", "oi_value", "oi_value", 0.0)
    _merge_series("bybit_ls_ratio", "timestamp_utc", "buy_ratio", "buy_ratio", 0.5)

    # Binance funding
    bin_docs = list(db.binance_funding_rates.find(query).sort("timestamp_utc", 1))
    if bin_docs:
        bdf = pd.DataFrame(bin_docs)
        if "timestamp_utc" in bdf.columns and "funding_rate" in bdf.columns:
            bdf["ts"] = pd.to_datetime(bdf["timestamp_utc"], unit="ms", utc=True)
            bdf = bdf.set_index("ts")[["funding_rate"]].sort_index()
            bdf = bdf.rename(columns={"funding_rate": "binance_funding_rate"})
            bdf = bdf[~bdf.index.duplicated(keep="last")]
            df["binance_funding_rate"] = bdf.reindex(candle_idx, method="ffill")["binance_funding_rate"].fillna(0.0).values
    if "binance_funding_rate" not in df.columns:
        df["binance_funding_rate"] = 0.0

    # Coinalyze liquidations (daily, cross-exchange aggregated)
    liq_docs = list(db.coinalyze_liquidations.find(
        {"pair": pair, "resolution": "daily"}
    ).sort("timestamp_utc", 1))
    if liq_docs:
        liqdf = pd.DataFrame(liq_docs)
        if "timestamp_utc" in liqdf.columns:
            liqdf["ts"] = pd.to_datetime(liqdf["timestamp_utc"], unit="ms", utc=True)
            liqdf = liqdf.set_index("ts").sort_index()
            liqdf = liqdf[~liqdf.index.duplicated(keep="last")]
            for col in ["long_liquidations_usd", "short_liquidations_usd", "total_liquidations_usd"]:
                if col in liqdf.columns:
                    df[col] = liqdf[[col]].reindex(candle_idx, method="ffill")[col].fillna(0.0).values
                else:
                    df[col] = 0.0
    for col in ["long_liquidations_usd", "short_liquidations_usd", "total_liquidations_usd"]:
        if col not in df.columns:
            df[col] = 0.0

    provider.candles_feeds[feed_key] = df


def _merge_funding_spread_sync(bt, pair: str, connector: str):
    """Merge HL + Bybit + Binance funding rates for X8 funding spread strategy.

    Adds columns: hl_funding_rate, bybit_funding_rate, binance_funding_rate
    to the cached 1h candle DataFrame for spread computation in the controller.
    """
    feed_key = f"{connector}_{pair}_1h"
    provider = bt._bt_engine.backtesting_data_provider
    df = provider.candles_feeds.get(feed_key)
    if df is None or len(df) == 0:
        return

    db = MongoClient("mongodb://localhost:27017").quants_lab
    query = {"pair": pair}
    candle_idx = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    # Hyperliquid funding (1h resolution)
    hl_docs = list(db.hyperliquid_funding_rates.find(query).sort("timestamp_utc", 1))
    if hl_docs:
        hl_df = pd.DataFrame(hl_docs)
        if "timestamp_utc" in hl_df.columns and "funding_rate" in hl_df.columns:
            hl_df["ts"] = pd.to_datetime(hl_df["timestamp_utc"], unit="ms", utc=True)
            hl_df = hl_df.set_index("ts")[["funding_rate"]].sort_index()
            hl_df = hl_df.rename(columns={"funding_rate": "hl_funding_rate"})
            hl_df = hl_df[~hl_df.index.duplicated(keep="last")]
            df["hl_funding_rate"] = hl_df.reindex(candle_idx, method="ffill")["hl_funding_rate"].fillna(0.0).values
    if "hl_funding_rate" not in df.columns:
        df["hl_funding_rate"] = 0.0

    # Bybit funding (8h resolution, forward-filled to 1h)
    bybit_docs = list(db.bybit_funding_rates.find(query).sort("timestamp_utc", 1))
    if bybit_docs:
        bybit_df = pd.DataFrame(bybit_docs)
        if "timestamp_utc" in bybit_df.columns and "funding_rate" in bybit_df.columns:
            bybit_df["ts"] = pd.to_datetime(bybit_df["timestamp_utc"], unit="ms", utc=True)
            bybit_df = bybit_df.set_index("ts")[["funding_rate"]].sort_index()
            bybit_df = bybit_df.rename(columns={"funding_rate": "bybit_funding_rate"})
            bybit_df = bybit_df[~bybit_df.index.duplicated(keep="last")]
            df["bybit_funding_rate"] = bybit_df.reindex(candle_idx, method="ffill")["bybit_funding_rate"].fillna(0.0).values
    if "bybit_funding_rate" not in df.columns:
        df["bybit_funding_rate"] = 0.0

    # Binance funding (8h resolution, forward-filled to 1h)
    bin_docs = list(db.binance_funding_rates.find(query).sort("timestamp_utc", 1))
    if bin_docs:
        bin_df = pd.DataFrame(bin_docs)
        if "timestamp_utc" in bin_df.columns and "funding_rate" in bin_df.columns:
            bin_df["ts"] = pd.to_datetime(bin_df["timestamp_utc"], unit="ms", utc=True)
            bin_df = bin_df.set_index("ts")[["funding_rate"]].sort_index()
            bin_df = bin_df.rename(columns={"funding_rate": "binance_funding_rate"})
            bin_df = bin_df[~bin_df.index.duplicated(keep="last")]
            df["binance_funding_rate"] = bin_df.reindex(candle_idx, method="ffill")["binance_funding_rate"].fillna(0.0).values
    if "binance_funding_rate" not in df.columns:
        df["binance_funding_rate"] = 0.0

    provider.candles_feeds[feed_key] = df


async def run_backtest(engine_name, connector, pair, start_ts, end_ts, resolution, trade_cost):
    """Run backtest for one pair. Returns (results_dict, executors_df_or_None)."""
    engine_meta = get_strategy(engine_name)

    bt = BacktestingEngine(load_cached_data=True)

    # Merge derivatives if needed
    if "derivatives" in (engine_meta.required_features or []):
        _merge_derivatives_sync(bt, pair, connector)

    # Merge funding spread data for X8
    if "funding_spread" in (engine_meta.required_features or []):
        _merge_funding_spread_sync(bt, pair, connector)

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
