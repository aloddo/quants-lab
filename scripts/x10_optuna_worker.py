#!/usr/bin/env python3
"""
X10 Optuna worker — runs ONE trial in an isolated subprocess.
Reads params from input JSON, writes results to output JSON.
Memory freed on process exit.
"""
import asyncio
import gc
import json
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017/quants_lab")
os.environ.setdefault("MONGO_DATABASE", "quants_lab")


async def run_trial(pairs, start_ts, end_ts, overrides):
    import pandas as pd
    from core.backtesting import BacktestingEngine
    from app.engines.strategy_registry import get_strategy, build_backtest_config

    meta = get_strategy("X10")
    all_pnls = []
    all_close_types = Counter()

    for pair in pairs:
        config = build_backtest_config(engine_name="X10", connector="bybit_perpetual", pair=pair)
        for k, v in overrides.items():
            if hasattr(config, k):
                setattr(config, k, v)

        engine = BacktestingEngine(load_cached_data=True)
        try:
            result = await engine.run_backtesting(
                config=config, start=start_ts, end=end_ts,
                backtesting_resolution=meta.backtesting_resolution,
                trade_cost=0.000375,
            )
            ct = result.results.get("close_types", {})
            if not isinstance(ct, dict):
                ct = {}
            for k_ct, v_ct in ct.items():
                all_close_types[k_ct] += v_ct

            n = sum(ct.values())
            if n > 0 and result.executors_df is not None:
                edf = result.executors_df
                edf["net_pnl_quote"] = pd.to_numeric(edf["net_pnl_quote"], errors="coerce")
                all_pnls.extend(edf["net_pnl_quote"].dropna().tolist())
        except Exception as e:
            pass

        del engine
        gc.collect()

    return {
        "pnls": [float(p) for p in all_pnls],
        "close_types": dict(all_close_types),
    }


def main():
    input_path = sys.argv[1]
    output_path = sys.argv[2]

    with open(input_path) as f:
        params = json.load(f)

    result = asyncio.run(run_trial(
        params["pairs"],
        params["start_ts"],
        params["end_ts"],
        params["overrides"],
    ))

    with open(output_path, "w") as f:
        json.dump(result, f)


if __name__ == "__main__":
    main()
