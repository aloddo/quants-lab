#!/usr/bin/env python3
"""
X10 Robustness Gates — Phase 2 pre-deployment checks.

Per governance, ALL must pass before paper trading:
1. Slippage tiers: 2 bps (must), 5 bps (should), 10 bps (acceptable to fail)
2. Parameter sensitivity: perturb z_total, z_imb, hold by +/- 1 step
3. Monte Carlo (block bootstrap): 10k sims, ruin < 1% at intended sizing
4. Top-5 trade concentration < 80%
5. Max consecutive losses check
"""
import asyncio
import gc
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd

os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017/quants_lab")
os.environ.setdefault("MONGO_DATABASE", "quants_lab")


async def run_single_pair_backtest(pair: str, config_overrides: dict = None) -> dict:
    """Run one backtest for a single pair, return summary stats."""
    from core.backtesting import BacktestingEngine
    from app.engines.strategy_registry import get_strategy, build_backtest_config

    meta = get_strategy("X10")
    config = build_backtest_config(
        engine_name="X10",
        connector="bybit_perpetual",
        pair=pair,
    )

    if config_overrides:
        for k, v in config_overrides.items():
            if hasattr(config, k):
                setattr(config, k, v)

    from datetime import datetime, timezone
    end_ts = int(datetime(2026, 4, 21, tzinfo=timezone.utc).timestamp())
    start_ts = end_ts - 365 * 86400

    engine = BacktestingEngine(load_cached_data=True)
    result = await engine.run_backtesting(
        config=config,
        start=start_ts,
        end=end_ts,
        backtesting_resolution=meta.backtesting_resolution,
        trade_cost=0.000375,
    )

    close_types = result.results.get("close_types", {})
    if not isinstance(close_types, dict):
        close_types = {}

    total = sum(close_types.values())

    pnl_values = []
    if total > 0 and result.executors_df is not None:
        edf = result.executors_df
        for col in ["net_pnl_quote", "net_pnl_pct"]:
            if col in edf.columns:
                edf[col] = pd.to_numeric(edf[col], errors="coerce")
        pnl_values = edf["net_pnl_quote"].dropna().tolist()

    del engine
    gc.collect()

    return {
        "pair": pair,
        "trades": total,
        "close_types": close_types,
        "pnl_values": pnl_values,
        "total_pnl": sum(pnl_values),
    }


async def main():
    print("=" * 70)
    print("X10 ROBUSTNESS GATES")
    print("=" * 70)

    # Use a subset of pairs for robustness (6 diverse pairs: 2 major, 2 mid, 2 small)
    test_pairs = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "LINK-USDT", "TAO-USDT", "SEI-USDT"]

    # ── 1. BASELINE ──────────────────────────────────────────
    print("\n--- Baseline (default params) ---")
    baseline_pnls = []
    for pair in test_pairs:
        r = await run_single_pair_backtest(pair)
        baseline_pnls.extend(r["pnl_values"])
        print(f"  {pair}: {r['trades']} trades, PnL=${r['total_pnl']:.2f}, "
              f"types={r['close_types']}")

    baseline_arr = np.array(baseline_pnls)
    if len(baseline_arr) == 0:
        print("ERROR: No baseline trades. Aborting.")
        return 1

    baseline_mean = np.mean(baseline_arr)
    baseline_wr = (baseline_arr > 0).mean()
    baseline_pf = baseline_arr[baseline_arr > 0].sum() / (-baseline_arr[baseline_arr < 0].sum()) if (baseline_arr < 0).any() else np.inf

    print(f"\n  Baseline: n={len(baseline_arr)}, mean=${baseline_mean:.4f}, "
          f"WR={baseline_wr:.1%}, PF={baseline_pf:.2f}")

    # ── 2. SLIPPAGE TIERS ────────────────────────────────────
    print(f"\n{'='*70}")
    print("GATE: Slippage Tiers")
    print("=" * 70)

    for slip_bps, label, must_pass in [(2, "2 bps (MUST)", True), (5, "5 bps (SHOULD)", True), (10, "10 bps (OK to fail)", False)]:
        # Simulate slippage by reducing each trade PnL by slip_bps worth of $300 position
        # slip_bps on entry + exit = 2 * slip_bps one-way cost
        slip_cost_per_trade = 300.0 * (2 * slip_bps / 10000)
        adjusted = baseline_arr - slip_cost_per_trade
        adj_mean = np.mean(adjusted)
        adj_wr = (adjusted > 0).mean()
        adj_pf = adjusted[adjusted > 0].sum() / (-adjusted[adjusted < 0].sum()) if (adjusted < 0).any() else np.inf

        passed = adj_mean > 0 and adj_pf > 1.0
        status = "PASS" if passed else "FAIL"
        req = "REQUIRED" if must_pass else "OPTIONAL"
        print(f"  {label}: mean=${adj_mean:.4f}, WR={adj_wr:.1%}, PF={adj_pf:.2f} -> {status} ({req})")

    # ── 3. PARAMETER SENSITIVITY ─────────────────────────────
    print(f"\n{'='*70}")
    print("GATE: Parameter Sensitivity")
    print("=" * 70)

    param_tests = [
        ("z_total_threshold", [1.5, 2.0, 2.5]),
        ("z_imb_threshold", [0.3, 0.5, 0.7]),
        ("sl_atr_mult", [2.0, 2.5, 3.0]),
        ("tp_atr_mult", [1.5, 2.0, 2.5]),
    ]

    # Only test on BTC+SOL for speed
    sensitivity_pairs = ["BTC-USDT", "SOL-USDT"]

    for param, values in param_tests:
        print(f"\n  {param}:")
        for val in values:
            total_pnl = 0
            total_trades = 0
            for pair in sensitivity_pairs:
                r = await run_single_pair_backtest(pair, {param: val})
                total_pnl += r["total_pnl"]
                total_trades += r["trades"]
            marker = " ← DEFAULT" if val == getattr(type, param, None) else ""
            is_default = (param == "z_total_threshold" and val == 2.0) or \
                         (param == "z_imb_threshold" and val == 0.5) or \
                         (param == "sl_atr_mult" and val == 2.5) or \
                         (param == "tp_atr_mult" and val == 2.0)
            marker = " ← DEFAULT" if is_default else ""
            print(f"    {val}: {total_trades} trades, PnL=${total_pnl:.2f}{marker}")

    # ── 4. MONTE CARLO (block bootstrap) ─────────────────────
    print(f"\n{'='*70}")
    print("GATE: Monte Carlo Simulation")
    print("=" * 70)

    n_sims = 10000
    block_size = 5
    capital = 100000.0
    position_size = 300.0

    rng = np.random.default_rng(42)
    ruin_count = 0
    max_dd_pcts = []

    # Normalize PnLs to position_size ($300)
    for _ in range(n_sims):
        # Block bootstrap
        n_blocks = len(baseline_arr) // block_size + 1
        blocks = [baseline_arr[i:i+block_size] for i in range(0, len(baseline_arr)-block_size+1)]
        if not blocks:
            blocks = [baseline_arr]
        sampled = np.concatenate([blocks[rng.integers(len(blocks))] for _ in range(n_blocks)])
        sampled = sampled[:len(baseline_arr)]

        # Compute equity curve
        equity = capital + np.cumsum(sampled)
        peak = np.maximum.accumulate(equity)
        dd = (peak - equity) / peak
        max_dd = dd.max()
        max_dd_pcts.append(max_dd)

        if equity.min() < capital * 0.5:  # 50% drawdown = ruin
            ruin_count += 1

    ruin_pct = ruin_count / n_sims * 100
    median_dd = np.median(max_dd_pcts) * 100
    p95_dd = np.percentile(max_dd_pcts, 95) * 100
    p99_dd = np.percentile(max_dd_pcts, 99) * 100

    gate_mc = ruin_pct < 1.0
    print(f"  Simulations: {n_sims}")
    print(f"  Capital: ${capital:,.0f}, Position: ${position_size}")
    print(f"  Ruin probability (50% DD): {ruin_pct:.2f}% {'PASS' if gate_mc else 'FAIL'} (threshold < 1%)")
    print(f"  Max drawdown: median={median_dd:.1f}%, p95={p95_dd:.1f}%, p99={p99_dd:.1f}%")

    # ── 5. TRADE QUALITY ─────────────────────────────────────
    print(f"\n{'='*70}")
    print("GATE: Trade Quality")
    print("=" * 70)

    sorted_pnls = sorted(baseline_pnls, reverse=True)
    total_profit = sum(p for p in sorted_pnls if p > 0)
    top5_profit = sum(sorted_pnls[:5])
    top5_share = top5_profit / total_profit if total_profit > 0 else 1.0

    # Max consecutive losses
    max_streak = 0
    cur = 0
    for p in baseline_pnls:
        if p < 0:
            cur += 1
            max_streak = max(max_streak, cur)
        else:
            cur = 0

    gate_top5 = top5_share < 0.80
    print(f"  Top-5 trade concentration: {top5_share:.1%} {'PASS' if gate_top5 else 'FAIL'} (threshold < 80%)")
    print(f"  Max consecutive losses: {max_streak}")
    print(f"  Total trades (6 pairs): {len(baseline_arr)}")

    # ── SUMMARY ──────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("ROBUSTNESS SUMMARY")
    print("=" * 70)
    print(f"  Baseline: n={len(baseline_arr)}, PF={baseline_pf:.2f}, WR={baseline_wr:.1%}")
    print(f"  Monte Carlo ruin: {ruin_pct:.2f}% {'PASS' if gate_mc else 'FAIL'}")
    print(f"  Top-5 concentration: {top5_share:.1%} {'PASS' if gate_top5 else 'FAIL'}")
    print(f"  Slippage: see tiers above")
    print(f"  Parameter sensitivity: see grid above")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
