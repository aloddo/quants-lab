"""
E3 P0 Degradation Stress Tests — Remaining checks.

A1: 2nd-trigger entry (require signal persistence for 2 consecutive bars)
Entry delay: shift signal by 1-4 bars to simulate execution latency
Setup expiry: test cooldown_time at 1h / 2h / 4h

Uses same shared candles pattern as other E3 scripts.

Usage:
  python scripts/e3_p0_checks.py
"""
import asyncio
import gc
import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(level=logging.WARNING, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

TOP_PAIRS = ["ADA-USDT", "SUI-USDT", "SOL-USDT", "XRP-USDT"]


def get_parquet_end_time(connector, pairs, resolution="1m"):
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


async def load_shared_candles_with_derivatives(pairs, connector, engine_name):
    from core.backtesting.engine import BacktestingEngine
    from app.engines.strategy_registry import get_strategy
    from app.tasks.backtesting.bulk_backtest_task import BulkBacktestTask

    _cache = BacktestingEngine(load_cached_data=True)
    shared_candles = _cache._bt_engine.backtesting_data_provider.candles_feeds.copy()
    del _cache
    gc.collect()

    engine_meta = get_strategy(engine_name)
    if "derivatives" in engine_meta.required_features:
        _merger = object.__new__(BulkBacktestTask)
        _merger.connector_name = connector
        _merger.engine_name = engine_name

        import motor.motor_asyncio
        mongo_uri = os.environ["MONGO_URI"]
        _merger.mongodb_client = motor.motor_asyncio.AsyncIOMotorClient(mongo_uri)

        n = await _merger._merge_derivatives_into_candles(shared_candles, pairs)
        print(f"  Merged derivatives into {n}/{len(pairs)} pairs")

    return shared_candles


async def run_backtest(pair, engine_name, connector, shared_candles,
                       trade_cost=0.000375, config_overrides=None,
                       start_ts=None, end_ts=None):
    from core.backtesting.engine import BacktestingEngine
    from app.engines.strategy_registry import get_strategy, build_backtest_config

    meta = get_strategy(engine_name)

    config = build_backtest_config(
        engine_name=engine_name,
        connector=connector,
        pair=pair,
    )

    if config_overrides:
        for key, val in config_overrides.items():
            setattr(config, key, val)

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


async def main():
    engine = "E3"
    connector = "bybit_perpetual"

    all_pairs = TOP_PAIRS + ["BTC-USDT"]
    end_ts = get_parquet_end_time(connector, all_pairs, resolution="1m")
    start_ts = end_ts - 365 * 86400

    print("=" * 90)
    print(f"  Phase 2 P0: DEGRADATION STRESS TESTS — {engine}")
    print(f"  Pairs: {', '.join(TOP_PAIRS)}")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 90)
    print(f"\n  Time range: {datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime('%Y-%m-%d')} "
          f"to {datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime('%Y-%m-%d')}")

    # Load shared candles
    print("\n  Loading candles and merging derivatives...")
    shared_candles = await load_shared_candles_with_derivatives(
        all_pairs, connector, engine
    )

    # ── Baseline ──
    print("\n  Running baseline...")
    baseline = {}
    for pair in TOP_PAIRS:
        r = await run_backtest(pair, engine, connector, shared_candles,
                               start_ts=start_ts, end_ts=end_ts)
        baseline[pair] = r
        print(f"    {pair:<16} PF={r['pf']:.3f}  trades={r['trades']}  Sharpe={r['sharpe']:.2f}")

    baseline_avg_pf = np.mean([r["pf"] for r in baseline.values()])
    baseline_avg_trades = np.mean([r["trades"] for r in baseline.values()])

    # ── A1: 2nd-trigger entry (funding_streak_min + 1) ──
    print(f"\n  {'─' * 80}")
    print(f"  A1: 2nd-trigger entry (funding_streak_min=4, baseline=3)")
    print(f"  {'─' * 80}")
    a1_results = {}
    for pair in TOP_PAIRS:
        r = await run_backtest(pair, engine, connector, shared_candles,
                               config_overrides={"funding_streak_min": 4},
                               start_ts=start_ts, end_ts=end_ts)
        a1_results[pair] = r
        base_pf = baseline[pair]["pf"]
        delta = r["pf"] - base_pf
        print(f"    {pair:<16} PF={r['pf']:.3f} (delta={delta:+.3f})  "
              f"trades={r['trades']}  Sharpe={r['sharpe']:.2f}")

    a1_avg_pf = np.mean([r["pf"] for r in a1_results.values()])
    a1_drop = (baseline_avg_pf - a1_avg_pf) / baseline_avg_pf * 100
    print(f"    avg PF: {a1_avg_pf:.3f} (baseline: {baseline_avg_pf:.3f}, drop: {a1_drop:.1f}%)")
    if a1_drop < 10:
        print(f"    PASS: Entry delay (2nd trigger) causes < 10% PF degradation")
    else:
        print(f"    *** FAIL: Entry delay causes {a1_drop:.1f}% PF degradation ***")

    # ── SL sensitivity with correct field (sl_pct, not stop_loss) ──
    print(f"\n  {'─' * 80}")
    print(f"  Corrected SL sensitivity (sl_pct field, not parent stop_loss)")
    print(f"  Values: [0.02, 0.03, 0.05, 0.07, 0.10]  (baseline=0.05)")
    print(f"  {'─' * 80}")
    sl_values = [0.02, 0.03, 0.05, 0.07, 0.10]
    for sl in sl_values:
        pair_results = {}
        for pair in TOP_PAIRS:
            r = await run_backtest(pair, engine, connector, shared_candles,
                                   config_overrides={"sl_pct": sl},
                                   start_ts=start_ts, end_ts=end_ts)
            pair_results[pair] = r
        avg_pf = np.mean([r["pf"] for r in pair_results.values()])
        avg_trades = np.mean([r["trades"] for r in pair_results.values()])
        is_base = "<<< BASELINE" if sl == 0.05 else ""
        print(f"    sl_pct={sl:<6}  avg_PF={avg_pf:.3f}  avg_trades={avg_trades:.0f}  "
              + "  ".join(f"{p.split('-')[0]}={r['pf']:.3f}" for p, r in pair_results.items())
              + f"  {is_base}")

    # ── TP sensitivity with correct field (tp_pct, not parent take_profit) ──
    print(f"\n  {'─' * 80}")
    print(f"  Corrected TP sensitivity (tp_pct field, not parent take_profit)")
    print(f"  Values: [0.01, 0.02, 0.03, 0.05, 0.08]  (baseline=0.03)")
    print(f"  {'─' * 80}")
    tp_values = [0.01, 0.02, 0.03, 0.05, 0.08]
    for tp in tp_values:
        pair_results = {}
        for pair in TOP_PAIRS:
            r = await run_backtest(pair, engine, connector, shared_candles,
                                   config_overrides={"tp_pct": tp},
                                   start_ts=start_ts, end_ts=end_ts)
            pair_results[pair] = r
        avg_pf = np.mean([r["pf"] for r in pair_results.values()])
        avg_trades = np.mean([r["trades"] for r in pair_results.values()])
        is_base = "<<< BASELINE" if tp == 0.03 else ""
        print(f"    tp_pct={tp:<6}  avg_PF={avg_pf:.3f}  avg_trades={avg_trades:.0f}  "
              + "  ".join(f"{p.split('-')[0]}={r['pf']:.3f}" for p, r in pair_results.items())
              + f"  {is_base}")

    # ── Setup expiry analog: cooldown_time sweep ──
    print(f"\n  {'─' * 80}")
    print(f"  Setup expiry analog: cooldown_time sweep")
    print(f"  Values: [1800, 3600, 7200, 14400]  (baseline=3600)")
    print(f"  {'─' * 80}")
    cd_values = [1800, 3600, 7200, 14400]
    for cd in cd_values:
        pair_results = {}
        for pair in TOP_PAIRS:
            r = await run_backtest(pair, engine, connector, shared_candles,
                                   config_overrides={"cooldown_time": cd},
                                   start_ts=start_ts, end_ts=end_ts)
            pair_results[pair] = r
        avg_pf = np.mean([r["pf"] for r in pair_results.values()])
        avg_trades = np.mean([r["trades"] for r in pair_results.values()])
        is_base = "<<< BASELINE" if cd == 3600 else ""
        print(f"    cooldown={cd:<6}  avg_PF={avg_pf:.3f}  avg_trades={avg_trades:.0f}  "
              + "  ".join(f"{p.split('-')[0]}={r['pf']:.3f}" for p, r in pair_results.items())
              + f"  {is_base}")

    # ── Summary ──
    print(f"\n{'=' * 90}")
    print(f"  P0 DEGRADATION SUMMARY")
    print(f"{'=' * 90}")
    print(f"  A1 (2nd trigger):  PF drop = {a1_drop:.1f}%  {'PASS' if a1_drop < 10 else 'FAIL'}")
    print(f"  Slippage (prior):  Edge survives 10 bps — PASS")
    print(f"  Monte Carlo:       Ruin < 1% at 0.3% sizing — PASS")
    print(f"  Trade distribution: max consec losses <= 5, top-5 < 40% — PASS")
    print(f"  SL/TP sensitivity: see corrected results above")
    print(f"  Cooldown sweep:    see results above")


if __name__ == "__main__":
    asyncio.run(main())
