#!/usr/bin/env python3
"""
X12 Phase 1.3 — Long/Short Asymmetry Analysis

Split trade analysis by side for FARTCOIN/HYPE/SOL on the TRAIN window.
If one side >> other: implement directional filter.
"""

import asyncio
import json
import logging
import os
import pickle
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

logging.basicConfig(level=logging.WARNING)

PYTHON = "/Users/hermes/miniforge3/envs/quants-lab/bin/python"
CONNECTOR = "bybit_perpetual"
TRADE_COST = 0.000375
ENGINE = "X12"

PAIRS = {
    "FARTCOIN-USDT": {"z_threshold": 2.0, "z_window": 24},
    "HYPE-USDT":     {"z_threshold": 2.0, "z_window": 48},
    "SOL-USDT":      {"z_threshold": 1.5, "z_window": 48},
}

# Full train window
TRAIN_START = "2025-10-01"
TRAIN_END = "2026-02-28"


def to_unix(date_str: str) -> int:
    return int(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def run_with_executors(pair: str, params: dict) -> dict:
    """Run backtest via subprocess, return results + executor-level PnL by side."""
    start_ts = to_unix(TRAIN_START)
    end_ts = to_unix(TRAIN_END)
    z_t = params["z_threshold"]
    z_w = params["z_window"]

    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
        output_path = f.name

    script = f'''
import asyncio, sys, os, logging, pickle
import pandas as pd
logging.basicConfig(level=logging.WARNING)
sys.path.insert(0, "/Users/hermes/quants-lab")
os.environ["MONGO_URI"] = "mongodb://localhost:27017/quants_lab"
os.environ["MONGO_DATABASE"] = "quants_lab"

from decimal import Decimal
from pymongo import MongoClient
from core.backtesting.engine import BacktestingEngine
from app.engines.strategy_registry import build_backtest_config
from app.data_sources.merge import merge_all_for_engine_sync

async def run():
    bt = BacktestingEngine(load_cached_data=True)
    feed_key = "{CONNECTOR}_{pair}_1h"
    provider = bt._bt_engine.backtesting_data_provider
    df = provider.candles_feeds.get(feed_key)
    if df is None:
        return {{"error": "no candle data"}}

    db = MongoClient("mongodb://localhost:27017").quants_lab
    merged = merge_all_for_engine_sync(db, "{ENGINE}", df, "{pair}",
                                        start_ts={start_ts}*1000, end_ts={end_ts}*1000)
    provider.candles_feeds[feed_key] = merged
    db.client.close()

    config = build_backtest_config(
        engine_name="{ENGINE}", connector="{CONNECTOR}", pair="{pair}",
        total_amount_quote=Decimal("100"),
        z_threshold={z_t}, z_window={z_w},
    )

    bt_result = await bt.run_backtesting(
        config=config, start={start_ts}, end={end_ts},
        backtesting_resolution="1m", trade_cost={TRADE_COST},
    )

    r = bt_result.results
    edf = bt_result.executors_df

    # Extract side-split data
    side_data = {{"longs": [], "shorts": []}}
    if edf is not None and len(edf) > 0:
        edf["net_pnl_quote"] = pd.to_numeric(edf["net_pnl_quote"], errors="coerce")
        edf["close_type"] = edf["close_type"].apply(
            lambda x: x.name if hasattr(x, "name") else str(x)
        )

        # Determine side from config or custom_info
        # In HB V2 executors, side is in config.side or the executor's side field
        for _, row in edf.iterrows():
            pnl = float(row["net_pnl_quote"])
            ct = str(row.get("close_type", ""))

            # Parse side from config dict
            cfg = row.get("config", {{}})
            if isinstance(cfg, dict):
                side_val = cfg.get("side", "")
            else:
                side_val = ""

            side_str = str(side_val).upper()
            if "BUY" in side_str or side_str == "1" or "LONG" in side_str:
                side_data["longs"].append({{"pnl": pnl, "close_type": ct}})
            elif "SELL" in side_str or side_str == "2" or "SHORT" in side_str:
                side_data["shorts"].append({{"pnl": pnl, "close_type": ct}})
            else:
                # Fallback: check net_pnl_pct or other indicators
                side_data["longs"].append({{"pnl": pnl, "close_type": ct, "side_unknown": True}})

    return {{
        "status": "ok",
        "total_trades": r.get("total_executors", 0),
        "total_pf": round(float(r.get("profit_factor", 0)), 3),
        "total_pnl": round(float(r.get("net_pnl_quote", 0)), 2),
        "total_wr": round(float(r.get("accuracy", 0)) * 100, 1),
        "total_long": r.get("total_long", 0),
        "total_short": r.get("total_short", 0),
        "acc_long": round(float(r.get("accuracy_long", 0)) * 100, 1),
        "acc_short": round(float(r.get("accuracy_short", 0)) * 100, 1),
        "side_data": side_data,
    }}

try:
    result = asyncio.run(run())
except Exception as e:
    result = {{"status": "error", "error": str(e)}}

with open("{output_path}", "wb") as f:
    pickle.dump(result, f)
'''

    env = os.environ.copy()
    env["MONGO_URI"] = "mongodb://localhost:27017/quants_lab"
    env["MONGO_DATABASE"] = "quants_lab"

    proc = subprocess.run(
        [PYTHON, "-c", script], env=env, timeout=600,
        capture_output=True, text=True,
    )

    if proc.returncode != 0:
        return {"error": f"subprocess failed: {(proc.stderr or '')[-200:]}"}

    with open(output_path, "rb") as f:
        payload = pickle.load(f)
    os.unlink(output_path)

    return payload


def analyze_side(trades_list, label):
    """Compute metrics for one side."""
    if not trades_list:
        return {"label": label, "trades": 0, "pf": 0, "pnl": 0, "wr": 0, "mean_r": 0}

    pnls = [t["pnl"] for t in trades_list]
    gp = sum(p for p in pnls if p > 0)
    gl = abs(sum(p for p in pnls if p < 0))
    pf = gp / gl if gl > 0 else float("inf")
    wins = sum(1 for p in pnls if p > 0)
    wr = wins / len(pnls) * 100
    mean_pnl = sum(pnls) / len(pnls)
    total_pnl = sum(pnls)

    # Close type breakdown
    ct_counts = {}
    for t in trades_list:
        ct = t.get("close_type", "UNKNOWN").upper()
        ct_counts[ct] = ct_counts.get(ct, 0) + 1

    return {
        "label": label,
        "trades": len(pnls),
        "pf": round(pf, 3),
        "pnl": round(total_pnl, 2),
        "mean_pnl": round(mean_pnl, 4),
        "wr": round(wr, 1),
        "close_types": ct_counts,
    }


def main():
    print("=" * 80)
    print("X12 Phase 1.3 — Long/Short Asymmetry Analysis")
    print(f"Train window: {TRAIN_START} -> {TRAIN_END}")
    print("=" * 80)

    all_results = {}
    for pair, params in PAIRS.items():
        print(f"\n{'='*60}")
        print(f"  {pair} | z_t={params['z_threshold']}, z_w={params['z_window']}")
        print(f"{'='*60}")

        payload = run_with_executors(pair, params)
        if payload.get("status") == "error":
            print(f"  ERROR: {payload.get('error', 'unknown')}")
            continue

        print(f"  Total: {payload['total_trades']} trades, PF={payload['total_pf']}, "
              f"PnL=${payload['total_pnl']}, WR={payload['total_wr']}%")
        print(f"  Engine reports: L={payload['total_long']}, S={payload['total_short']}, "
              f"AccL={payload['acc_long']}%, AccS={payload['acc_short']}%")

        sd = payload.get("side_data", {})
        long_stats = analyze_side(sd.get("longs", []), "LONG")
        short_stats = analyze_side(sd.get("shorts", []), "SHORT")

        print(f"\n  {'Side':<8} {'N':>5} {'PF':>6} {'PnL':>8} {'MeanPnL':>8} {'WR':>6}")
        print(f"  {'-'*50}")
        for s in [long_stats, short_stats]:
            print(f"  {s['label']:<8} {s['trades']:>5} {s['pf']:>6.3f} "
                  f"{s['pnl']:>8.2f} {s['mean_pnl']:>8.4f} {s['wr']:>5.1f}%")

        for s in [long_stats, short_stats]:
            if s["close_types"]:
                ct_str = ", ".join(f"{k}={v}" for k, v in sorted(s["close_types"].items()))
                print(f"  {s['label']} exits: {ct_str}")

        # Asymmetry assessment
        if long_stats["trades"] > 0 and short_stats["trades"] > 0:
            pf_ratio = long_stats["pf"] / short_stats["pf"] if short_stats["pf"] > 0 else float("inf")
            if pf_ratio > 2.0 or pf_ratio < 0.5:
                print(f"\n  ** ASYMMETRY DETECTED: L/S PF ratio = {pf_ratio:.2f}")
                if pf_ratio > 2.0:
                    print(f"     LONG side significantly stronger. Consider long-only filter.")
                else:
                    print(f"     SHORT side significantly stronger. Consider short-only filter.")
            else:
                print(f"\n  Sides balanced (PF ratio: {pf_ratio:.2f}). No directional filter needed.")
        elif long_stats["trades"] == 0:
            print(f"\n  !! No long trades parsed — side detection might be broken")

        all_results[pair] = {
            "total": payload,
            "long": long_stats,
            "short": short_stats,
        }

    # Save
    output_path = "/Users/hermes/quants-lab/app/data/cache/x12_long_short_analysis.json"
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\nSaved to {output_path}")


if __name__ == "__main__":
    main()
