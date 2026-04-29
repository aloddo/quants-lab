#!/usr/bin/env python3
"""
X12 Phase 2 P0 — Time Limit Sweep

Test time_limit at 4h / 6h / 8h / 12h for FARTCOIN/HYPE/SOL.
8h is current default. Check if longer/shorter improves.
"""

import json
import os
import pickle
import subprocess
import sys
import tempfile
from datetime import datetime, timezone

PYTHON = "/Users/hermes/miniforge3/envs/quants-lab/bin/python"
CONNECTOR = "bybit_perpetual"
TRADE_COST = 0.000375
ENGINE = "X12"

PAIRS = {
    "FARTCOIN-USDT": {"z_threshold": 2.0, "z_window": 24},
    "HYPE-USDT":     {"z_threshold": 2.0, "z_window": 48},
    "SOL-USDT":      {"z_threshold": 1.5, "z_window": 48},
}

TIME_LIMITS = [4 * 3600, 6 * 3600, 8 * 3600, 12 * 3600]  # seconds
TRAIN_START = "2025-10-01"
TRAIN_END = "2026-02-28"


def to_unix(date_str: str) -> int:
    return int(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def run_single(pair: str, params: dict, time_limit: int) -> dict:
    start_ts = to_unix(TRAIN_START)
    end_ts = to_unix(TRAIN_END)
    z_t = params["z_threshold"]
    z_w = params["z_window"]

    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
        output_path = f.name

    script = f'''
import asyncio, sys, os, logging, pickle
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
        time_limit_seconds={time_limit},
    )

    bt_result = await bt.run_backtesting(
        config=config, start={start_ts}, end={end_ts},
        backtesting_resolution="1m", trade_cost={TRADE_COST},
    )

    r = bt_result.results
    ct = r.get("close_types", {{}})
    if not isinstance(ct, dict):
        ct = {{}}
    ct_str = {{(k.name if hasattr(k, "name") else str(k)).upper(): v for k, v in ct.items()}}

    return {{
        "status": "ok",
        "trades": r.get("total_executors", 0),
        "pf": round(float(r.get("profit_factor", 0)), 3),
        "pnl": round(float(r.get("net_pnl_quote", 0)), 2),
        "wr": round(float(r.get("accuracy", 0)) * 100, 1),
        "sharpe": round(float(r.get("sharpe_ratio", 0)), 3),
        "dd": round(float(r.get("max_drawdown_usd", 0)), 2),
        "close_types": ct_str,
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

    proc = subprocess.run([PYTHON, "-c", script], env=env, timeout=600,
                         capture_output=True, text=True)

    if proc.returncode != 0:
        return {"error": f"subprocess failed (rc={proc.returncode})"}

    with open(output_path, "rb") as f:
        payload = pickle.load(f)
    os.unlink(output_path)
    return payload


def main():
    print("=" * 80)
    print("X12 Phase 2 P0 — Time Limit Sweep")
    print(f"Train: {TRAIN_START} -> {TRAIN_END}")
    print("=" * 80)

    results = {}
    total = len(PAIRS) * len(TIME_LIMITS)
    done = 0

    for pair, params in PAIRS.items():
        results[pair] = []
        for tl in TIME_LIMITS:
            done += 1
            tl_h = tl // 3600
            print(f"\n[{done}/{total}] {pair} | time_limit={tl_h}h")

            payload = run_single(pair, params, tl)
            if payload.get("status") == "error":
                print(f"  ERROR: {payload.get('error', '')[:100]}")
                continue

            ct = payload.get("close_types", {})
            tp = sum(v for k, v in ct.items() if "TAKE_PROFIT" in k)
            sl = sum(v for k, v in ct.items() if "STOP_LOSS" in k)
            tl_exits = sum(v for k, v in ct.items() if "TIME_LIMIT" in k)
            ts = sum(v for k, v in ct.items() if "TRAILING" in k)

            row = {
                "time_limit_h": tl_h,
                "trades": payload["trades"],
                "pf": payload["pf"],
                "pnl": payload["pnl"],
                "wr": payload["wr"],
                "sharpe": payload["sharpe"],
                "dd": payload["dd"],
                "tp": tp, "sl": sl, "tl": tl_exits, "ts": ts,
            }
            results[pair].append(row)

            print(f"  N={row['trades']}, PF={row['pf']}, PnL=${row['pnl']}, WR={row['wr']}%, "
                  f"Sharpe={row['sharpe']}")
            print(f"  Exits: TP={tp} SL={sl} TL={tl_exits} TS={ts}")

    # Summary
    print(f"\n\n{'='*90}")
    print("TIME LIMIT SWEEP SUMMARY")
    print(f"{'='*90}")
    for pair, rows in results.items():
        print(f"\n{pair}:")
        print(f"  {'TL':>4}h {'N':>5} {'PF':>6} {'PnL':>8} {'WR':>6} {'Sharpe':>7} {'TP':>4} {'SL':>4} {'TL':>4} {'TS':>4}")
        print(f"  {'-'*65}")
        for r in rows:
            marker = " <-- current" if r["time_limit_h"] == 8 else ""
            print(f"  {r['time_limit_h']:>4}h {r['trades']:>5} {r['pf']:>6.3f} {r['pnl']:>8.2f} "
                  f"{r['wr']:>5.1f}% {r['sharpe']:>7.3f} {r['tp']:>4} {r['sl']:>4} {r['tl']:>4} {r['ts']:>4}{marker}")

    # Save
    with open("/Users/hermes/quants-lab/app/data/cache/x12_time_limit_sweep.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved to app/data/cache/x12_time_limit_sweep.json")


if __name__ == "__main__":
    main()
