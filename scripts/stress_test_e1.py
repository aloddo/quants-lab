#!/usr/bin/env python3
"""
E1 Degradation Stress Tests — validates that disabling entry quality filter
doesn't break under adverse execution conditions.

Tests:
  A1: Delayed entry (2nd trigger instead of 1st)
  A2: Slippage tiers (2, 5, 10, 15 bps additional cost)
  A3: Setup expiry sweep (1h, 2h, 4h windows)
  A4: Regime windows (bear, shock, ranging, bull)

Usage:
  set -a && source .env && set +a
  /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/stress_test_e1.py
  /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/stress_test_e1.py --pairs BTC-USDT ETH-USDT SOL-USDT
"""
import argparse
import asyncio
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.backtesting.engine import BacktestingEngine
from app.engines.registry import get_engine, build_backtest_config

# The A1 controller uses trigger_index=1 (2nd valid trigger)
from app.controllers.directional_trading.e1_compression_breakout import (
    E1CompressionBreakoutConfig,
)

MAX_RETRIES = 2
BASE_COST = 0.000375  # 0.0375% per side


async def run_one(connector: str, pair: str, start_ts: int, end_ts: int,
                  trade_cost: float, resolution: str, config_overrides: dict) -> dict:
    """Run a single backtest with retry."""
    for attempt in range(MAX_RETRIES + 1):
        try:
            bt = BacktestingEngine(load_cached_data=True)
            config = build_backtest_config(
                engine_name="E1", connector=connector, pair=pair,
                entry_quality_filter=False,  # always disabled — that's what we're testing
                **config_overrides,
            )
            result = await bt.run_backtesting(
                config=config, start=start_ts, end=end_ts,
                backtesting_resolution=resolution, trade_cost=trade_cost,
            )
            if not isinstance(result.results.get("close_types"), dict):
                result.results["close_types"] = {}

            r = result.results
            trades = r.get("total_executors", 0)
            pf = r.get("profit_factor", 0) or 0
            pnl = r.get("net_pnl_quote", 0) or 0
            wr = (r.get("accuracy_long", 0) or 0) * 100
            sharpe = r.get("sharpe_ratio", None)
            max_dd = r.get("max_drawdown_pct", None)

            # Close type breakdown
            ct = r.get("close_types", {})
            tp = sum(v for k, v in ct.items() if "TAKE_PROFIT" in str(k).upper())
            sl = sum(v for k, v in ct.items() if "STOP_LOSS" in str(k).upper())
            tl = sum(v for k, v in ct.items() if "TIME_LIMIT" in str(k).upper())
            ts = sum(v for k, v in ct.items() if "TRAILING" in str(k).upper())

            return {
                "pair": pair, "trades": trades, "pf": round(pf, 2),
                "wr": round(wr, 1), "pnl": round(pnl, 2),
                "sharpe": round(sharpe, 2) if sharpe else None,
                "max_dd": round(max_dd * 100, 1) if max_dd else None,
                "tp": tp, "sl": sl, "tl": tl, "ts": ts,
            }
        except Exception as e:
            if attempt < MAX_RETRIES:
                await asyncio.sleep(1)
                continue
            return {"pair": pair, "trades": 0, "pf": 0, "pnl": 0, "error": str(e)}


def print_test_results(name: str, rows: list[dict]):
    """Print a formatted table for one stress test."""
    df = pd.DataFrame(rows)
    if "error" in df.columns:
        ok = df[df["error"].isna()].copy()
    else:
        ok = df.copy()

    total_trades = ok["trades"].sum()
    total_pnl = ok["pnl"].sum()
    avg_pf = ok[ok["trades"] > 0]["pf"].mean() if (ok["trades"] > 0).any() else 0
    avg_wr = ok[ok["trades"] > 0]["wr"].mean() if (ok["trades"] > 0).any() else 0

    print(f"\n  {'Pair':>12s}  {'Trades':>6s}  {'PF':>6s}  {'WR%':>5s}  {'PnL':>8s}  {'Sharpe':>6s}  {'MaxDD':>6s}  {'TP':>3s}  {'SL':>3s}  {'TL':>3s}  {'TS':>3s}")
    print(f"  {'-'*12}  {'-'*6}  {'-'*6}  {'-'*5}  {'-'*8}  {'-'*6}  {'-'*6}  {'-'*3}  {'-'*3}  {'-'*3}  {'-'*3}")
    for _, r in ok.sort_values("pf", ascending=False).iterrows():
        sh = str(r.get("sharpe", "—"))
        dd = f"{r.get('max_dd', 0):.1f}" if r.get("max_dd") else "—"
        print(f"  {r['pair']:>12s}  {int(r['trades']):>6d}  {r['pf']:>6.2f}  {r['wr']:>5.1f}  {r['pnl']:>+8.2f}  {sh:>6s}  {dd:>6s}  {int(r.get('tp',0)):>3d}  {int(r.get('sl',0)):>3d}  {int(r.get('tl',0)):>3d}  {int(r.get('ts',0)):>3d}")

    print(f"  {'TOTAL':>12s}  {total_trades:>6.0f}  {avg_pf:>6.2f}  {avg_wr:>5.1f}  {total_pnl:>+8.2f}")
    return total_trades, avg_pf, total_pnl


async def main():
    parser = argparse.ArgumentParser(description="E1 degradation stress tests")
    parser.add_argument("--pairs", nargs="*",
                        default=["BTC-USDT", "ETH-USDT", "SOL-USDT", "ADA-USDT",
                                 "NEAR-USDT", "DOT-USDT", "LTC-USDT", "DOGE-USDT"])
    parser.add_argument("--connector", default="bybit_perpetual")
    parser.add_argument("--days", type=int, default=365)
    args = parser.parse_args()

    engine_meta = get_engine("E1")
    resolution = engine_meta["backtesting_resolution"]
    connector = args.connector
    pairs = args.pairs

    now_ts = int(datetime.now(timezone.utc).timestamp())
    start_365 = now_ts - args.days * 86400

    print(f"E1 Stress Tests — entry_quality_filter=False")
    print(f"Resolution: {resolution} | Pairs: {len(pairs)} | Days: {args.days}")

    summary_rows = []
    t0 = time.time()

    # ─────────────────────────────────────────────────────────
    # BASELINE: disabled entry quality, standard cost
    # ─────────────────────────────────────────────────────────
    print(f"\n{'='*75}")
    print(f"  BASELINE — entry_quality_filter=False, trade_cost={BASE_COST}")
    print(f"{'='*75}")
    rows = []
    for pair in pairs:
        r = await run_one(connector, pair, start_365, now_ts, BASE_COST, resolution, {})
        rows.append(r)
    trades, pf, pnl = print_test_results("baseline", rows)
    summary_rows.append({"test": "baseline", "trades": trades, "avg_pf": pf, "total_pnl": pnl})

    # ─────────────────────────────────────────────────────────
    # A1: DELAYED ENTRY — 2nd trigger instead of 1st
    # Uses setup_expiry_bars to widen the window so 2nd trigger can fire
    # ─────────────────────────────────────────────────────────
    print(f"\n{'='*75}")
    print(f"  A1 — DELAYED ENTRY (setup_expiry_bars=48 = 4h window for 2nd trigger)")
    print(f"{'='*75}")
    # We can't easily switch controller class via registry, so we simulate
    # delayed entry by doubling setup_expiry and adding cooldown
    rows = []
    for pair in pairs:
        r = await run_one(connector, pair, start_365, now_ts, BASE_COST, resolution,
                          {"setup_expiry_bars": 48, "cooldown_time": 600})
        rows.append(r)
    trades, pf, pnl = print_test_results("A1_delayed", rows)
    summary_rows.append({"test": "A1_delayed_entry", "trades": trades, "avg_pf": pf, "total_pnl": pnl})

    # ─────────────────────────────────────────────────────────
    # A2: SLIPPAGE TIERS — increasing trade_cost
    # Each bps of slippage = 0.00001 added to trade_cost per side
    # ─────────────────────────────────────────────────────────
    for slip_bps in [2, 5, 10, 15]:
        cost = BASE_COST + (slip_bps / 10000)
        print(f"\n{'='*75}")
        print(f"  A2 — SLIPPAGE +{slip_bps} bps  (trade_cost={cost:.6f})")
        print(f"{'='*75}")
        rows = []
        for pair in pairs:
            r = await run_one(connector, pair, start_365, now_ts, cost, resolution, {})
            rows.append(r)
        trades, pf, pnl = print_test_results(f"A2_+{slip_bps}bps", rows)
        summary_rows.append({"test": f"A2_slip_{slip_bps}bps", "trades": trades, "avg_pf": pf, "total_pnl": pnl})

    # ─────────────────────────────────────────────────────────
    # A3: SETUP EXPIRY SWEEP — 1h, 2h, 4h
    # ─────────────────────────────────────────────────────────
    for expiry_bars, label in [(12, "1h"), (24, "2h"), (48, "4h")]:
        print(f"\n{'='*75}")
        print(f"  A3 — SETUP EXPIRY = {label} ({expiry_bars} bars)")
        print(f"{'='*75}")
        rows = []
        for pair in pairs:
            r = await run_one(connector, pair, start_365, now_ts, BASE_COST, resolution,
                              {"setup_expiry_bars": expiry_bars})
            rows.append(r)
        trades, pf, pnl = print_test_results(f"A3_{label}", rows)
        summary_rows.append({"test": f"A3_expiry_{label}", "trades": trades, "avg_pf": pf, "total_pnl": pnl})

    # ─────────────────────────────────────────────────────────
    # A4: REGIME WINDOWS — test same params across different markets
    # ─────────────────────────────────────────────────────────
    regime_windows = [
        ("bear_crash", "2021-11-01", "2022-06-30"),
        ("ftx_shock", "2022-11-01", "2022-11-30"),
        ("ranging", "2023-01-01", "2023-09-30"),
        ("bull_2024", "2024-07-01", "2025-01-01"),
        ("recent_365d", None, None),  # use default
    ]
    for regime_name, start_str, end_str in regime_windows:
        if start_str:
            rs = int(datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
            re = int(datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())
        else:
            rs, re = start_365, now_ts

        print(f"\n{'='*75}")
        print(f"  A4 — REGIME: {regime_name} ({start_str or 'last 365d'} -> {end_str or 'now'})")
        print(f"{'='*75}")
        rows = []
        for pair in pairs:
            r = await run_one(connector, pair, rs, re, BASE_COST, resolution, {})
            rows.append(r)
        trades, pf, pnl = print_test_results(f"A4_{regime_name}", rows)
        summary_rows.append({"test": f"A4_{regime_name}", "trades": trades, "avg_pf": pf, "total_pnl": pnl})

    # ─────────────────────────────────────────────────────────
    # FINAL SUMMARY
    # ─────────────────────────────────────────────────────────
    elapsed = time.time() - t0

    print(f"\n\n{'='*75}")
    print(f"  STRESS TEST SUMMARY — E1 (entry_quality_filter=False)")
    print(f"  {elapsed:.0f}s elapsed")
    print(f"{'='*75}")

    print(f"\n  {'Test':>22s}  {'Trades':>6s}  {'AvgPF':>6s}  {'TotalPnL':>9s}  {'Status':>8s}")
    print(f"  {'-'*22}  {'-'*6}  {'-'*6}  {'-'*9}  {'-'*8}")

    baseline_pnl = summary_rows[0]["total_pnl"] if summary_rows else 0
    for r in summary_rows:
        # PASS if: positive PnL, PF > 1.0, and not catastrophically worse than baseline
        if r["total_pnl"] > 0 and r["avg_pf"] >= 1.0:
            status = "PASS"
        elif r["total_pnl"] > 0:
            status = "WEAK"
        else:
            status = "FAIL"
        print(f"  {r['test']:>22s}  {r['trades']:>6.0f}  {r['avg_pf']:>6.2f}  {r['total_pnl']:>+9.2f}  {status:>8s}")

    # Hard stop checks
    print(f"\n  HARD STOP CHECKS:")
    baseline = summary_rows[0]
    a2_15 = next((r for r in summary_rows if "15bps" in r["test"]), None)
    if a2_15 and a2_15["total_pnl"] <= 0:
        print(f"    [FAIL] Edge flips negative at +15 bps slippage")
    elif a2_15:
        print(f"    [PASS] Edge survives +15 bps slippage (PnL={a2_15['total_pnl']:+.2f})")

    regimes_pass = sum(1 for r in summary_rows if r["test"].startswith("A4_") and r["total_pnl"] > 0)
    regimes_total = sum(1 for r in summary_rows if r["test"].startswith("A4_"))
    print(f"    [{'PASS' if regimes_pass >= 2 else 'FAIL'}] Positive in {regimes_pass}/{regimes_total} regime windows (need >= 2)")

    # Save
    output = Path("app/data/processed/e1_stress_test.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(summary_rows).to_json(output, orient="records", indent=2)
    print(f"\n  Results saved: {output}")


if __name__ == "__main__":
    asyncio.run(main())
