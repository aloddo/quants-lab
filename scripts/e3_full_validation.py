"""
E3 Full Validation — All 11 deployable pairs with detailed long/short breakdown.

Runs:
  1. Full-year backtest (365d) on all pairs — detailed metrics
  2. Validation window (last 121d) on all pairs — OOS check
  3. Per-side breakdown: long PF, short PF, long WR, short WR, trade counts
  4. Close type distribution per pair

Usage:
  python scripts/e3_full_validation.py
"""
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

DEPLOY_PAIRS = [
    "1000PEPE-USDT", "ADA-USDT", "APT-USDT", "DOGE-USDT", "GALA-USDT",
    "LINK-USDT", "ONT-USDT", "SOL-USDT", "SUI-USDT", "TAO-USDT", "XRP-USDT",
]

TRAIN_DAYS = 244
VALIDATION_DAYS = 121


def get_parquet_end_time(connector, pairs, resolution="1m"):
    from core.data_paths import data_paths
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


async def run_backtest_detailed(pair, engine_name, connector, shared_candles,
                                trade_cost=0.000375, start_ts=None, end_ts=None):
    """Run backtest and return full results including per-trade data."""
    from core.backtesting.engine import BacktestingEngine
    from app.engines.strategy_registry import get_strategy, build_backtest_config

    meta = get_strategy(engine_name)
    config = build_backtest_config(
        engine_name=engine_name,
        connector=connector,
        pair=pair,
    )

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

        # Per-trade analysis from executors
        long_pnls = []
        short_pnls = []
        long_wins = 0
        short_wins = 0
        long_count = r.get("total_long", 0) or 0
        short_count = r.get("total_short", 0) or 0

        if hasattr(result, 'executors') and result.executors:
            for ex in result.executors:
                pnl = float(ex.net_pnl_quote) if hasattr(ex, 'net_pnl_quote') else 0
                side = str(getattr(ex.config, 'side', '') if hasattr(ex, 'config') else '')
                if 'BUY' in side:
                    long_pnls.append(pnl)
                    if pnl > 0:
                        long_wins += 1
                else:
                    short_pnls.append(pnl)
                    if pnl > 0:
                        short_wins += 1

        # Compute per-side PF
        long_gross_profit = sum(p for p in long_pnls if p > 0)
        long_gross_loss = abs(sum(p for p in long_pnls if p < 0))
        short_gross_profit = sum(p for p in short_pnls if p > 0)
        short_gross_loss = abs(sum(p for p in short_pnls if p < 0))

        long_pf = long_gross_profit / long_gross_loss if long_gross_loss > 0 else 0
        short_pf = short_gross_profit / short_gross_loss if short_gross_loss > 0 else 0
        long_wr = (long_wins / len(long_pnls) * 100) if long_pnls else 0
        short_wr = (short_wins / len(short_pnls) * 100) if short_pnls else 0

        return {
            "pf": r.get("profit_factor", 0) or 0,
            "trades": r.get("total_executors", 0) or 0,
            "sharpe": r.get("sharpe_ratio", 0) or 0,
            "pnl": r.get("net_pnl_quote", 0) or 0,
            "max_dd": r.get("max_drawdown_pct", 0) or 0,
            "wr_long": r.get("accuracy_long", 0) or 0,
            "wr_short": r.get("accuracy_short", 0) or 0,
            "n_long": long_count,
            "n_short": short_count,
            "long_pf": long_pf,
            "short_pf": short_pf,
            "long_wr": long_wr,
            "short_wr": short_wr,
            "long_pnl": sum(long_pnls),
            "short_pnl": sum(short_pnls),
            "close_types": r.get("close_types", {}),
        }
    except Exception as e:
        logger.warning(f"Backtest failed for {pair}: {e}")
        return {
            "pf": 0, "trades": 0, "sharpe": 0, "pnl": 0, "max_dd": -1,
            "wr_long": 0, "wr_short": 0, "n_long": 0, "n_short": 0,
            "long_pf": 0, "short_pf": 0, "long_wr": 0, "short_wr": 0,
            "long_pnl": 0, "short_pnl": 0, "close_types": {},
            "error": str(e),
        }
    finally:
        del bt_engine
        gc.collect()


def print_results_table(results, label, window_days):
    print(f"\n{'=' * 120}")
    print(f"  {label} ({window_days}d)")
    print(f"{'=' * 120}")

    header = (f"  {'Pair':<16} {'PF':>6} {'Sharpe':>7} {'Trades':>7} "
              f"{'N_Long':>7} {'N_Short':>8} {'Long PF':>8} {'Short PF':>9} "
              f"{'Long WR':>8} {'Short WR':>9} {'Net PnL':>10} {'Max DD':>8}")
    print(header)
    print(f"  {'-' * 116}")

    totals = {
        "trades": 0, "n_long": 0, "n_short": 0,
        "pnl": 0, "long_pnl": 0, "short_pnl": 0,
        "pfs": [], "sharpes": [], "long_pfs": [], "short_pfs": [],
        "long_wrs": [], "short_wrs": [],
    }

    for pair in sorted(results.keys()):
        r = results[pair]
        print(f"  {pair:<16} {r['pf']:>6.3f} {r['sharpe']:>7.2f} {r['trades']:>7} "
              f"{r['n_long']:>7} {r['n_short']:>8} {r['long_pf']:>8.3f} {r['short_pf']:>9.3f} "
              f"{r['long_wr']:>7.1f}% {r['short_wr']:>8.1f}% "
              f"{r['pnl']:>10.2f} {r['max_dd']:>7.1f}%")

        totals["trades"] += r["trades"]
        totals["n_long"] += r["n_long"]
        totals["n_short"] += r["n_short"]
        totals["pnl"] += r["pnl"]
        totals["long_pnl"] += r["long_pnl"]
        totals["short_pnl"] += r["short_pnl"]
        if r["pf"] > 0:
            totals["pfs"].append(r["pf"])
        if r["sharpe"] != 0:
            totals["sharpes"].append(r["sharpe"])
        if r["long_pf"] > 0:
            totals["long_pfs"].append(r["long_pf"])
        if r["short_pf"] > 0:
            totals["short_pfs"].append(r["short_pf"])
        if r["long_wr"] > 0:
            totals["long_wrs"].append(r["long_wr"])
        if r["short_wr"] > 0:
            totals["short_wrs"].append(r["short_wr"])

    print(f"  {'-' * 116}")
    avg_pf = np.mean(totals["pfs"]) if totals["pfs"] else 0
    avg_sharpe = np.mean(totals["sharpes"]) if totals["sharpes"] else 0
    avg_long_pf = np.mean(totals["long_pfs"]) if totals["long_pfs"] else 0
    avg_short_pf = np.mean(totals["short_pfs"]) if totals["short_pfs"] else 0
    avg_long_wr = np.mean(totals["long_wrs"]) if totals["long_wrs"] else 0
    avg_short_wr = np.mean(totals["short_wrs"]) if totals["short_wrs"] else 0

    print(f"  {'AVERAGE':<16} {avg_pf:>6.3f} {avg_sharpe:>7.2f} {totals['trades']:>7} "
          f"{totals['n_long']:>7} {totals['n_short']:>8} {avg_long_pf:>8.3f} {avg_short_pf:>9.3f} "
          f"{avg_long_wr:>7.1f}% {avg_short_wr:>8.1f}% "
          f"{totals['pnl']:>10.2f}")

    # Close types summary
    all_ct = {}
    for r in results.values():
        for ct, count in r.get("close_types", {}).items():
            all_ct[ct] = all_ct.get(ct, 0) + count
    print(f"\n  Close types: {all_ct}")
    print(f"  Long/Short ratio: {totals['n_long']}/{totals['n_short']} "
          f"({totals['n_long']/(totals['n_long']+totals['n_short'])*100:.1f}% long)"
          if totals['n_long'] + totals['n_short'] > 0 else "")
    print(f"  Long PnL: {totals['long_pnl']:.2f}  |  Short PnL: {totals['short_pnl']:.2f}")

    return totals


async def main():
    engine = "E3"
    connector = "bybit_perpetual"

    all_pairs = DEPLOY_PAIRS + ["BTC-USDT"]
    end_ts = get_parquet_end_time(connector, all_pairs, resolution="1m")
    full_start = end_ts - 365 * 86400
    val_start = end_ts - VALIDATION_DAYS * 86400

    print("=" * 120)
    print(f"  E3 FULL VALIDATION — {len(DEPLOY_PAIRS)} deployable pairs")
    print(f"  Baseline params (no Optuna overrides)")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 120)
    print(f"\n  Full year:   {datetime.fromtimestamp(full_start, tz=timezone.utc).strftime('%Y-%m-%d')} "
          f"to {datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime('%Y-%m-%d')} (365d)")
    print(f"  Validation:  {datetime.fromtimestamp(val_start, tz=timezone.utc).strftime('%Y-%m-%d')} "
          f"to {datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime('%Y-%m-%d')} ({VALIDATION_DAYS}d)")

    print(f"\n  Loading candles and merging derivatives...")
    shared_candles = await load_shared_candles_with_derivatives(
        all_pairs, connector, engine
    )

    # ── Full year backtest ──
    print(f"\n  Running full-year backtests on {len(DEPLOY_PAIRS)} pairs...")
    full_results = {}
    for pair in DEPLOY_PAIRS:
        r = await run_backtest_detailed(pair, engine, connector, shared_candles,
                                        start_ts=full_start, end_ts=end_ts)
        full_results[pair] = r
        print(f"    {pair:<16} PF={r['pf']:.3f}  trades={r['trades']}  "
              f"L={r['n_long']}/{r['long_pf']:.2f}  S={r['n_short']}/{r['short_pf']:.2f}")

    full_totals = print_results_table(full_results, "FULL YEAR (365d)", 365)

    # ── Validation window only ──
    print(f"\n  Running validation-window backtests on {len(DEPLOY_PAIRS)} pairs...")
    val_results = {}
    for pair in DEPLOY_PAIRS:
        r = await run_backtest_detailed(pair, engine, connector, shared_candles,
                                        start_ts=val_start, end_ts=end_ts)
        val_results[pair] = r
        print(f"    {pair:<16} PF={r['pf']:.3f}  trades={r['trades']}  "
              f"L={r['n_long']}/{r['long_pf']:.2f}  S={r['n_short']}/{r['short_pf']:.2f}")

    val_totals = print_results_table(val_results, f"VALIDATION WINDOW ({VALIDATION_DAYS}d)", VALIDATION_DAYS)

    # ── Pair-by-pair full/val comparison ──
    print(f"\n{'=' * 120}")
    print(f"  FULL vs VALIDATION COMPARISON")
    print(f"{'=' * 120}")
    print(f"  {'Pair':<16} {'Full PF':>8} {'Val PF':>8} {'PF Delta':>9} "
          f"{'Full Sharpe':>12} {'Val Sharpe':>11} {'Sharpe Delta':>13} {'Status':>8}")
    print(f"  {'-' * 116}")

    pass_count = 0
    for pair in sorted(DEPLOY_PAIRS):
        fr = full_results[pair]
        vr = val_results[pair]
        pf_delta = vr["pf"] - fr["pf"]
        sharpe_delta = vr["sharpe"] - fr["sharpe"]
        # Pass if val PF > 1.05 and val PF drop < 30% of full
        pf_drop_pct = (fr["pf"] - vr["pf"]) / fr["pf"] * 100 if fr["pf"] > 0 else 0
        status = "PASS" if vr["pf"] > 1.05 and pf_drop_pct < 30 else "WARN"
        if status == "PASS":
            pass_count += 1
        print(f"  {pair:<16} {fr['pf']:>8.3f} {vr['pf']:>8.3f} {pf_delta:>+9.3f} "
              f"{fr['sharpe']:>12.2f} {vr['sharpe']:>11.2f} {sharpe_delta:>+13.2f} {status:>8}")

    print(f"\n  PASS: {pass_count}/{len(DEPLOY_PAIRS)} pairs hold up in validation")

    # ── Deployment readiness verdict ──
    print(f"\n{'=' * 120}")
    print(f"  DEPLOYMENT READINESS VERDICT")
    print(f"{'=' * 120}")
    avg_full_pf = np.mean([r["pf"] for r in full_results.values()])
    avg_val_pf = np.mean([r["pf"] for r in val_results.values()])
    long_ratio = full_totals["n_long"] / (full_totals["n_long"] + full_totals["n_short"]) * 100
    print(f"  Full year avg PF:   {avg_full_pf:.3f}")
    print(f"  Validation avg PF:  {avg_val_pf:.3f}")
    print(f"  Long/Short ratio:   {long_ratio:.1f}% long")
    print(f"  Total trades (yr):  {full_totals['trades']}")
    print(f"  Net PnL (yr):       {full_totals['pnl']:.2f}")
    print(f"  Pairs passing val:  {pass_count}/{len(DEPLOY_PAIRS)}")


if __name__ == "__main__":
    asyncio.run(main())
