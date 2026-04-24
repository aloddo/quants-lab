#!/usr/bin/env python3
"""
X10 Smoke Test — single pair through BacktestingEngine.
Validates: controller imports, signal generation, trade execution, result format.
Run this BEFORE bulk backtest to catch issues in 60 seconds instead of hours.
"""
import asyncio
import gc
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure we can import from project root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017/quants_lab")
os.environ.setdefault("MONGO_DATABASE", "quants_lab")


async def main():
    from core.backtesting import BacktestingEngine
    from app.engines.strategy_registry import get_strategy, build_backtest_config

    pair = "BTC-USDT"
    engine_name = "X10"
    meta = get_strategy(engine_name)

    print(f"=== X10 Smoke Test: {pair} ===")
    print(f"Resolution: {meta.backtesting_resolution}")
    print(f"Controller: {meta.controller_module}")

    # Build config
    config = build_backtest_config(
        engine_name=engine_name,
        connector="bybit_perpetual",
        pair=pair,
    )
    print(f"Config built: {config.controller_name}")
    print(f"  z_total_threshold: {config.z_total_threshold}")
    print(f"  z_imb_threshold: {config.z_imb_threshold}")
    print(f"  z_window: {config.z_window}")
    print(f"  sl_atr_mult: {config.sl_atr_mult}")
    print(f"  tp_atr_mult: {config.tp_atr_mult}")
    print(f"  time_limit: {config.time_limit_seconds}s")

    # Use last 90 days for smoke test (enough for z-score warmup + trades)
    end_ts = int(datetime(2026, 4, 21, tzinfo=timezone.utc).timestamp())
    start_ts = end_ts - 90 * 86400  # 90 days

    print(f"\nBacktest window: {datetime.utcfromtimestamp(start_ts)} to {datetime.utcfromtimestamp(end_ts)}")

    # Need to merge liq data before running — the BacktestingEngine loads from parquet
    # which doesn't have liq columns. The controller's _merge_liq_from_mongodb handles this.
    # But we need to check if the controller's vectorized path works.

    engine = BacktestingEngine(load_cached_data=True)

    result = await engine.run_backtesting(
        config=config,
        start=start_ts,
        end=end_ts,
        backtesting_resolution=meta.backtesting_resolution,
        trade_cost=0.000375,
    )

    # Check results
    close_types = result.results.get("close_types", {})
    if not isinstance(close_types, dict):
        close_types = {}

    total_trades = sum(close_types.values()) if close_types else 0
    print(f"\n=== Results ===")
    print(f"Total trades: {total_trades}")
    print(f"Close types: {close_types}")

    if total_trades > 0:
        summary = result.get_results_summary()
        print(f"\n{summary}")

        edf = result.executors_df
        if edf is not None and len(edf) > 0:
            # Check for pickle dtype issues
            for col in ["net_pnl_quote", "net_pnl_pct"]:
                if col in edf.columns:
                    edf[col] = pd.to_numeric(edf[col], errors="coerce")

            print(f"\nTrade sample (first 5):")
            cols = [c for c in ["side", "net_pnl_quote", "close_type", "close_type_name"] if c in edf.columns]
            if cols:
                print(edf[cols].head().to_string())

        # Check processed_data has signal column
        pdf = result.processed_data
        if pdf is not None:
            has_signal = "signal" in pdf.columns if hasattr(pdf, 'columns') else False
            print(f"\nProcessed data shape: {pdf.shape if hasattr(pdf, 'shape') else 'N/A'}")
            print(f"Has 'signal' column: {has_signal}")
            if has_signal:
                signal_counts = pdf["signal"].value_counts().to_dict()
                print(f"Signal distribution: {signal_counts}")
    else:
        print("\n⚠ ZERO TRADES — check signal generation")
        # Debug: check if processed_data has signals
        pdf = result.processed_data
        if pdf is not None and hasattr(pdf, 'columns'):
            print(f"Processed data columns: {list(pdf.columns)}")
            if "signal" in pdf.columns:
                sc = pdf["signal"].value_counts().to_dict()
                print(f"Signal distribution: {sc}")
            if "liq_total_z" in pdf.columns:
                print(f"liq_total_z stats: max={pdf['liq_total_z'].max():.2f}, "
                      f"mean={pdf['liq_total_z'].mean():.2f}")
            if "total_liquidations_usd" in pdf.columns:
                print(f"total_liquidations_usd: max={pdf['total_liquidations_usd'].max():.2f}, "
                      f"non-zero={(pdf['total_liquidations_usd'] > 0).sum()}")
            else:
                print("⚠ total_liquidations_usd NOT in columns — merge failed")

    del engine
    gc.collect()

    return 0 if total_trades > 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
