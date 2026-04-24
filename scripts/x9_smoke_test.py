#!/usr/bin/env python3
"""
X9 Funding Crowding — Smoke test: single pair through HB BacktestingEngine.

Uses the same worker code path as BulkBacktestTask._backtest_worker for
accurate results. One pair, one subprocess, full memory isolation.

Usage:
    set -a && source .env && set +a && \
    PAIR=DOGE-USDT DAYS=365 \
    /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/x9_smoke_test.py
"""
import asyncio
import os
import sys
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from app.tasks.backtesting._backtest_worker import run_backtest

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
logger = logging.getLogger("x9_smoke")

PAIR = os.getenv("PAIR", "DOGE-USDT")
BACKTEST_DAYS = int(os.getenv("DAYS", "365"))
ENGINE = "X9"
CONNECTOR = "bybit_perpetual"
RESOLUTION = "1m"
TRADE_COST = 0.000375


def main():
    print(f"\n{'='*60}")
    print(f"X9 SMOKE TEST: {PAIR} ({BACKTEST_DAYS} days)")
    print(f"{'='*60}\n")

    # Use end time from cached data, not current time
    # (cached parquet may be 30-60 min behind, and if end_ts > cache end,
    # BacktestingDataProvider re-fetches the feed fresh, losing merged derivatives)
    from core.data_paths import data_paths
    _c1h = pd.read_parquet(data_paths.candles_dir / f"bybit_perpetual|{PAIR}|1h.parquet", columns=["timestamp"])
    _c1m = pd.read_parquet(data_paths.candles_dir / f"bybit_perpetual|{PAIR}|1m.parquet", columns=["timestamp"])
    end_ts = int(min(_c1h["timestamp"].max(), _c1m["timestamp"].max()))
    start_ts = end_ts - BACKTEST_DAYS * 86400

    print(f"Window: {datetime.fromtimestamp(start_ts, tz=timezone.utc)} → {datetime.fromtimestamp(end_ts, tz=timezone.utc)}")
    print(f"Running BacktestingEngine via worker (same path as BulkBacktestTask)...\n")

    results, edf = asyncio.run(run_backtest(
        engine_name=ENGINE,
        connector=CONNECTOR,
        pair=PAIR,
        start_ts=start_ts,
        end_ts=end_ts,
        resolution=RESOLUTION,
        trade_cost=TRADE_COST,
    ))

    print(f"\n{'='*60}")
    print(f"RESULTS: {PAIR}")
    print(f"{'='*60}")

    if results is None:
        print("\n❌ Backtest returned None — check logs above")
        sys.exit(1)

    n_trades = results.get("total_executors", 0)
    print(f"Trades: {n_trades}")

    if n_trades == 0:
        print("\n⚠️  ZERO TRADES — check signal generation")
        print(f"Results dict: {results}")
        sys.exit(1)

    # Extract key metrics from results dict
    pf = results.get("profit_factor", 0)
    sharpe = results.get("sharpe_ratio", 0)
    max_dd = results.get("max_drawdown_pct", 0)
    total_pnl = results.get("net_pnl_quote", 0)
    win_rate = results.get("win_rate", 0)
    close_types = results.get("close_types", {})

    print(f"\nTotal PnL: ${total_pnl:.2f}")
    print(f"Win rate: {win_rate:.2%}")
    print(f"Profit Factor: {pf:.2f}")
    print(f"Sharpe: {sharpe:.2f}")
    print(f"Max Drawdown: {max_dd:.2%}")
    print(f"Close types: {close_types}")

    # Side split from executor DataFrame
    if edf is not None and len(edf) > 0:
        print(f"\n--- Side Split ---")
        for side_val in sorted(edf["side"].unique()):
            side_df = edf[edf["side"] == side_val]
            side_pnl = side_df["net_pnl_quote"]
            n = len(side_df)
            wr = (side_pnl > 0).sum() / n if n > 0 else 0
            gp = side_pnl[side_pnl > 0].sum()
            gl = abs(side_pnl[side_pnl < 0].sum())
            spf = gp / gl if gl > 0 else float("inf")
            print(f"  {side_val}: {n} trades, WR={wr:.2%}, PF={spf:.2f}, total=${side_pnl.sum():.2f}")

        print(f"\n--- Close Type Breakdown ---")
        for ct in sorted(edf["close_type_name"].unique() if "close_type_name" in edf.columns else []):
            ct_df = edf[edf["close_type_name"] == ct]
            ct_pnl = ct_df["net_pnl_quote"]
            print(f"  {ct}: {len(ct_df)} trades, avg=${ct_pnl.mean():.2f}, total=${ct_pnl.sum():.2f}")

    # Verdict
    print(f"\n{'='*60}")
    if pf >= 1.3 and n_trades >= 30:
        print(f"✅ SMOKE TEST PASSED — PF {pf:.2f} >= 1.3, {n_trades} trades")
    elif pf >= 1.0:
        print(f"⚠️  MARGINAL — PF {pf:.2f} (positive but below 1.3 ALLOW threshold)")
    else:
        print(f"❌ FAILED — PF {pf:.2f} < 1.0")


if __name__ == "__main__":
    main()
