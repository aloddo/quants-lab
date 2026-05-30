#!/usr/bin/env python3
"""
E1 Phase 2 P1 — Monte Carlo block bootstrap.
Per research-process skill: block size 5-10 trades, 10k simulations, ruin < 1%.
"""
import asyncio
import gc
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.backtesting.engine import BacktestingEngine
from app.engines.registry import get_engine, build_backtest_config

CONNECTOR = "bybit_perpetual"
DAYS = 365
TRADE_COST = 0.000375
N_SIMULATIONS = 10000
BLOCK_SIZE = 7  # 5-10 per skill
RUIN_THRESHOLD_PCT = 20.0  # max drawdown % that counts as ruin
POSITION_SIZE_PCT = 0.3  # 0.3% of capital per trade
INITIAL_CAPITAL = 100000  # Bybit demo

# Pairs with most trades from bulk run
PAIRS = ["BTC-USDT", "ETH-USDT", "SOL-USDT", "ADA-USDT", "DOGE-USDT",
         "LINK-USDT", "XRP-USDT", "AVAX-USDT", "SUI-USDT", "APT-USDT",
         "GALA-USDT", "BLUR-USDT", "DOT-USDT", "LTC-USDT", "NEAR-USDT",
         "TRUMP-USDT", "SEI-USDT", "WIF-USDT"]


async def collect_trades():
    """Run backtests and collect all executor PnL data."""
    engine_meta = get_engine("E1")
    resolution = engine_meta["backtesting_resolution"]
    now_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = now_ts - DAYS * 86400

    all_pnls = []
    for pair in PAIRS:
        try:
            bt = BacktestingEngine(load_cached_data=True)
            config = build_backtest_config(engine_name="E1", connector=CONNECTOR, pair=pair)
            result = await bt.run_backtesting(
                config=config, start=start_ts, end=now_ts,
                backtesting_resolution=resolution, trade_cost=TRADE_COST,
            )
            if not isinstance(result.results.get("close_types"), dict):
                result.results["close_types"] = {}

            edf = result.executors_df
            if len(edf) > 0:
                # PnL as fraction of position size (R-multiple)
                pnls = edf["net_pnl_quote"].values
                all_pnls.extend(pnls.tolist())
                print(f"  {pair:>12s}  {len(edf):>3d} trades  avg={np.mean(pnls):+.4f}")

            del bt, result
            gc.collect()
        except Exception as e:
            print(f"  {pair:>12s}  ERROR: {e}")

    return np.array(all_pnls)


def block_bootstrap(pnls: np.ndarray, n_sims: int, block_size: int,
                    capital: float, position_pct: float, ruin_dd_pct: float):
    """
    Block bootstrap Monte Carlo simulation.
    Returns dict with ruin probability and percentile curves.
    """
    n_trades = len(pnls)
    n_blocks = max(n_trades // block_size, 10)
    sim_length = n_blocks * block_size

    ruin_count = 0
    final_pnls = []
    max_dds = []
    worst_dd = 0

    for i in range(n_sims):
        # Draw random blocks with replacement
        block_starts = np.random.randint(0, n_trades - block_size + 1, size=n_blocks)
        sim_pnls = np.concatenate([pnls[s:s + block_size] for s in block_starts])[:sim_length]

        # Cumulative PnL curve
        cum_pnl = np.cumsum(sim_pnls)
        running_max = np.maximum.accumulate(cum_pnl)
        drawdowns = running_max - cum_pnl

        # Position sizing: each trade is position_pct of capital
        # PnL values are already in quote (USDT), scaled to $100 notional
        # Scale to actual position size
        position_size = capital * position_pct / 100
        scale = position_size / 100  # backtest used $100 notional
        scaled_dd = drawdowns * scale
        scaled_dd_pct = scaled_dd / capital * 100

        max_dd_pct = scaled_dd_pct.max()
        max_dds.append(max_dd_pct)
        final_pnls.append(cum_pnl[-1] * scale)

        if max_dd_pct >= ruin_dd_pct:
            ruin_count += 1

        if max_dd_pct > worst_dd:
            worst_dd = max_dd_pct

    ruin_prob = ruin_count / n_sims * 100
    final_pnls = np.array(final_pnls)
    max_dds = np.array(max_dds)

    return {
        "ruin_probability_pct": ruin_prob,
        "ruin_count": ruin_count,
        "n_simulations": n_sims,
        "ruin_threshold_dd_pct": ruin_dd_pct,
        "worst_dd_pct": worst_dd,
        "median_final_pnl": float(np.median(final_pnls)),
        "p5_final_pnl": float(np.percentile(final_pnls, 5)),
        "p25_final_pnl": float(np.percentile(final_pnls, 25)),
        "p75_final_pnl": float(np.percentile(final_pnls, 75)),
        "p95_final_pnl": float(np.percentile(final_pnls, 95)),
        "median_max_dd_pct": float(np.median(max_dds)),
        "p95_max_dd_pct": float(np.percentile(max_dds, 95)),
        "p99_max_dd_pct": float(np.percentile(max_dds, 99)),
    }


async def main():
    print(f"E1 Monte Carlo Block Bootstrap")
    print(f"Simulations: {N_SIMULATIONS} | Block size: {BLOCK_SIZE}")
    print(f"Capital: ${INITIAL_CAPITAL:,} | Position: {POSITION_SIZE_PCT}%")
    print(f"Ruin threshold: {RUIN_THRESHOLD_PCT}% max drawdown")
    print(f"\nCollecting trade data from {len(PAIRS)} pairs...\n")

    pnls = await collect_trades()
    print(f"\nTotal trades collected: {len(pnls)}")
    print(f"Mean PnL per trade: {np.mean(pnls):+.4f}")
    print(f"Median PnL per trade: {np.median(pnls):+.4f}")
    print(f"Std: {np.std(pnls):.4f}")
    print(f"Win rate: {(pnls > 0).sum() / len(pnls) * 100:.1f}%")

    print(f"\nRunning {N_SIMULATIONS:,} simulations...")
    mc = block_bootstrap(pnls, N_SIMULATIONS, BLOCK_SIZE,
                         INITIAL_CAPITAL, POSITION_SIZE_PCT, RUIN_THRESHOLD_PCT)

    print(f"\n{'='*60}")
    print(f"  MONTE CARLO RESULTS")
    print(f"{'='*60}")
    print(f"\n  Ruin probability: {mc['ruin_probability_pct']:.2f}%  ({mc['ruin_count']}/{mc['n_simulations']})")
    print(f"  Ruin threshold:   {mc['ruin_threshold_dd_pct']:.1f}% max drawdown")
    print(f"  Worst sim DD:     {mc['worst_dd_pct']:.2f}%")
    print(f"\n  Max Drawdown Distribution:")
    print(f"    Median:  {mc['median_max_dd_pct']:.2f}%")
    print(f"    P95:     {mc['p95_max_dd_pct']:.2f}%")
    print(f"    P99:     {mc['p99_max_dd_pct']:.2f}%")
    print(f"\n  Final PnL Distribution (${INITIAL_CAPITAL:,} capital, {POSITION_SIZE_PCT}% sizing):")
    print(f"    P5:      ${mc['p5_final_pnl']:+,.2f}")
    print(f"    P25:     ${mc['p25_final_pnl']:+,.2f}")
    print(f"    Median:  ${mc['median_final_pnl']:+,.2f}")
    print(f"    P75:     ${mc['p75_final_pnl']:+,.2f}")
    print(f"    P95:     ${mc['p95_final_pnl']:+,.2f}")

    # Gate check
    print(f"\n{'='*60}")
    print(f"  GATE CHECK (per quant-governance skill)")
    print(f"{'='*60}")
    if mc["ruin_probability_pct"] < 1.0:
        print(f"  [PASS] Ruin probability {mc['ruin_probability_pct']:.2f}% < 1.0%")
    else:
        print(f"  [FAIL] Ruin probability {mc['ruin_probability_pct']:.2f}% >= 1.0%")
        print(f"         Reduce position size or investigate")

    if mc["p5_final_pnl"] > 0:
        print(f"  [PASS] 5th percentile PnL is positive (${mc['p5_final_pnl']:+,.2f})")
    else:
        print(f"  [WARN] 5th percentile PnL is negative (${mc['p5_final_pnl']:+,.2f})")

    # Save
    import json
    out = Path("app/data/processed/e1_monte_carlo.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        json.dump(mc, f, indent=2)
    print(f"\n  Results saved: {out}")


if __name__ == "__main__":
    asyncio.run(main())
