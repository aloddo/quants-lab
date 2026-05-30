"""
E3 Parameter Sensitivity — Phase 1.3c mini sensitivity + Optuna readiness check.

Perturbs key parameters +/- 1 step from baseline and checks for cliff edges
and boundary solutions. Uses the same derivatives merge as BulkBacktestTask.

Usage:
  python scripts/e3_param_sensitivity.py
  python scripts/e3_param_sensitivity.py --pair ADA-USDT
"""
import argparse
import asyncio
import gc
import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.WARNING, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

TOP_PAIRS = ["ADA-USDT", "SUI-USDT", "SOL-USDT", "XRP-USDT"]

# Sensitivity grid: each param with values to test
# Baseline: funding_streak_min=3, tp=0.03, sl=0.05
SENSITIVITY = {
    "funding_streak_min": {
        "values": [2, 3, 4, 5],
        "baseline": 3,
        "apply": lambda config, v: setattr(config, "funding_streak_min", v),
    },
    "take_profit": {
        "values": [0.02, 0.025, 0.03, 0.035, 0.04],
        "baseline": 0.03,
        "apply": lambda config, v: setattr(config, "take_profit", Decimal(str(v))),
    },
    "stop_loss": {
        "values": [0.03, 0.04, 0.05, 0.06, 0.07],
        "baseline": 0.05,
        "apply": lambda config, v: setattr(config, "stop_loss", Decimal(str(v))),
    },
}


async def load_shared_candles_with_derivatives(pairs, connector, engine_name):
    """Load parquet candles and merge derivatives from MongoDB (same as BulkBacktestTask)."""
    from core.backtesting.engine import BacktestingEngine
    from app.engines.strategy_registry import get_strategy
    from app.tasks.backtesting.bulk_backtest_task import BulkBacktestTask

    # Load parquet cache
    _cache = BacktestingEngine(load_cached_data=True)
    shared_candles = _cache._bt_engine.backtesting_data_provider.candles_feeds.copy()
    del _cache
    gc.collect()

    # Merge derivatives using BulkBacktestTask methods
    engine_meta = get_strategy(engine_name)
    if "derivatives" in engine_meta.required_features:
        # Create a lightweight BulkBacktestTask to borrow its merge methods
        _merger = object.__new__(BulkBacktestTask)
        _merger.connector_name = connector
        _merger.engine_name = engine_name

        # Set up async MongoDB client (same as the task system uses)
        import motor.motor_asyncio
        mongo_uri = os.environ["MONGO_URI"]
        _merger.mongodb_client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)

        n = await _merger._merge_derivatives_into_candles(shared_candles, pairs)
        print(f"  Merged derivatives into {n}/{len(pairs)} pairs")

    return shared_candles


def get_parquet_end_time(connector, pairs, resolution="1m"):
    """Find earliest end time across parquet files (same as BulkBacktestTask)."""
    from core.data_paths import data_paths
    import pandas as pd
    min_end = float("inf")
    for pair in pairs[:5]:
        f = data_paths.candles_dir / f"{connector}|{pair}|{resolution}.parquet"
        if f.exists():
            df = pd.read_parquet(f, columns=["timestamp"])
            end = df["timestamp"].max()
            if end < min_end:
                min_end = end
    if min_end == float("inf"):
        return int(datetime.now(timezone.utc).timestamp())
    return int(min_end)


async def run_backtest(pair, engine_name, connector, shared_candles,
                       trade_cost=0.000375, config_overrides=None,
                       start_ts=None, end_ts=None):
    """Run a single backtest using pre-loaded shared candles."""
    from core.backtesting.engine import BacktestingEngine
    from app.engines.strategy_registry import get_strategy, build_backtest_config

    meta = get_strategy(engine_name)

    # Time range — use provided or compute from parquet
    if end_ts is None:
        end_ts = int(datetime.now(timezone.utc).timestamp())
    if start_ts is None:
        start_ts = end_ts - 365 * 86400

    # Build config
    config = build_backtest_config(
        engine_name=engine_name,
        connector=connector,
        pair=pair,
    )

    # Apply overrides
    if config_overrides:
        for fn in config_overrides:
            fn(config)

    # Fresh engine with shared candles
    bt_engine = BacktestingEngine(load_cached_data=False)
    bt_engine._bt_engine.backtesting_data_provider.candles_feeds = shared_candles

    try:
        result = await bt_engine.run_backtesting(
            config=config,
            start=start_ts,
            end=end_ts,
            backtesting_resolution=meta.backtesting_resolution,
            trade_cost=trade_cost,
        )

        if not isinstance(result.results.get("close_types"), dict):
            result.results["close_types"] = {}

        r = result.results
        return {
            "pf": r.get("profit_factor", 0) or 0,
            "trades": r.get("total_executors", 0) or 0,
            "sharpe": r.get("sharpe_ratio", 0) or 0,
            "wr": (r.get("accuracy_long", 0) or 0) * 100,
            "pnl": r.get("net_pnl_quote", 0) or 0,
        }
    except Exception as e:
        logger.warning(f"Backtest failed for {pair}: {e}")
        return {"pf": 0, "trades": 0, "sharpe": 0, "wr": 0, "pnl": 0, "error": str(e)}
    finally:
        del bt_engine
        gc.collect()


async def main(pairs):
    engine = "E3"
    connector = "bybit_perpetual"

    print("=" * 100)
    print(f"  Phase 1.3c: PARAMETER SENSITIVITY — {engine}")
    print(f"  Pairs: {', '.join(pairs)}")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 100)

    # Compute time range from parquet data (avoids API fetch attempts)
    all_pairs = pairs + ["BTC-USDT"]
    end_ts = get_parquet_end_time(connector, all_pairs, resolution="1m")
    start_ts = end_ts - 365 * 86400
    print(f"\n  Time range: {datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime('%Y-%m-%d')} "
          f"to {datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime('%Y-%m-%d')}")

    # Load shared candles once (expensive)
    print("\n  Loading candles and merging derivatives...")
    shared_candles = await load_shared_candles_with_derivatives(
        all_pairs,
        connector, engine
    )

    # Run baseline first
    print("\n  Running baseline...")
    baseline_results = {}
    for pair in pairs:
        r = await run_backtest(pair, engine, connector, shared_candles,
                               start_ts=start_ts, end_ts=end_ts)
        baseline_results[pair] = r
        print(f"    {pair:<16} PF={r['pf']:.3f}  trades={r['trades']}  Sharpe={r['sharpe']:.2f}")

    # Sweep each parameter
    all_sweeps = {}
    for param_name, param_cfg in SENSITIVITY.items():
        print(f"\n  {'─' * 80}")
        print(f"  Sweeping: {param_name}")
        print(f"  Values: {param_cfg['values']}  (baseline={param_cfg['baseline']})")
        print(f"  {'─' * 80}")

        sweep_results = {}
        for val in param_cfg["values"]:
            override_fn = lambda config, v=val, p=param_cfg: p["apply"](config, v)
            pair_results = {}
            for pair in pairs:
                r = await run_backtest(
                    pair, engine, connector, shared_candles,
                    config_overrides=[override_fn],
                    start_ts=start_ts, end_ts=end_ts
                )
                pair_results[pair] = r

            avg_pf = np.mean([r["pf"] for r in pair_results.values()])
            avg_trades = np.mean([r["trades"] for r in pair_results.values()])
            is_base = (val == param_cfg["baseline"])
            marker = " <<< BASELINE" if is_base else ""

            print(f"    {param_name}={val:<6}  avg_PF={avg_pf:.3f}  avg_trades={avg_trades:.0f}  "
                  + "  ".join(f"{p.split('-')[0]}={r['pf']:.3f}" for p, r in pair_results.items())
                  + marker)

            sweep_results[val] = {"avg_pf": avg_pf, "avg_trades": avg_trades, "pairs": pair_results}

        all_sweeps[param_name] = sweep_results

        # ── Analysis ──
        baseline_val = param_cfg["baseline"]
        baseline_pf = sweep_results[baseline_val]["avg_pf"]
        baseline_trades = sweep_results[baseline_val]["avg_trades"]
        values = param_cfg["values"]
        baseline_idx = values.index(baseline_val)

        # Cliff detection
        cliff_detected = False
        for delta_name, delta_idx in [("left", baseline_idx - 1), ("right", baseline_idx + 1)]:
            if 0 <= delta_idx < len(values):
                neighbor = values[delta_idx]
                neighbor_pf = sweep_results[neighbor]["avg_pf"]
                drop = (baseline_pf - neighbor_pf) / baseline_pf * 100 if baseline_pf > 0 else 0
                if abs(drop) > 30:
                    print(f"    *** CLIFF: {param_name} {baseline_val}->{neighbor}: "
                          f"PF drops {drop:.1f}% ***")
                    cliff_detected = True

        if not cliff_detected:
            print(f"    PASS: No cliff edges (PF stable within +/-1 step)")

        # Boundary check
        best_val = max(sweep_results.keys(), key=lambda v: sweep_results[v]["avg_pf"])
        if best_val == values[0] or best_val == values[-1]:
            print(f"    *** BOUNDARY: Best {param_name}={best_val} at search space edge — "
                  f"expand range ***")
        else:
            print(f"    PASS: Best {param_name}={best_val} is interior")

        # Trade count drift (identity guard)
        for val in values:
            val_trades = sweep_results[val]["avg_trades"]
            if baseline_trades > 0:
                ratio = val_trades / baseline_trades
                if ratio > 2.0 or ratio < 0.5:
                    print(f"    *** IDENTITY: {param_name}={val} changes trade count by "
                          f"{ratio:.1f}x — this is a different strategy ***")

    # ── Summary ──
    print(f"\n{'=' * 100}")
    print(f"  PARAMETER SENSITIVITY SUMMARY")
    print(f"{'=' * 100}")

    all_pass = True
    for param_name, sweep in all_sweeps.items():
        values = SENSITIVITY[param_name]["values"]
        baseline_val = SENSITIVITY[param_name]["baseline"]
        baseline_pf = sweep[baseline_val]["avg_pf"]

        # Compute flatness: std of PFs across sweep / mean
        pfs = [sweep[v]["avg_pf"] for v in values]
        cv = np.std(pfs) / np.mean(pfs) if np.mean(pfs) > 0 else 0

        best_val = max(values, key=lambda v: sweep[v]["avg_pf"])
        best_pf = sweep[best_val]["avg_pf"]

        is_boundary = best_val == values[0] or best_val == values[-1]
        status = "BOUNDARY" if is_boundary else "OK"
        if cv > 0.15:
            status = "FRAGILE"

        print(f"  {param_name:<22} baseline_PF={baseline_pf:.3f}  "
              f"best={best_val}({best_pf:.3f})  CV={cv:.3f}  [{status}]")
        if status != "OK":
            all_pass = False

    if all_pass:
        print(f"\n  VERDICT: All parameters stable — edge is robust to perturbation")
    else:
        print(f"\n  VERDICT: Some parameters show instability — review flagged items")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair", default=None)
    args = parser.parse_args()

    pairs = [args.pair] if args.pair else TOP_PAIRS
    asyncio.run(main(pairs))
