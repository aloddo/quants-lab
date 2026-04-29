#!/usr/bin/env python3
"""
X12 Phase 1.2 — Regime Stress Tests

Run FARTCOIN/HYPE/SOL across 4 regime windows with IDENTICAL params.
Uses subprocess isolation (each backtest in separate process for memory reclaim).
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

REGIMES = {
    "bear_crash":  ("2025-11-01", "2025-11-30"),
    "shock_event": ("2026-01-25", "2026-02-10"),
    "low_vol":     ("2025-12-01", "2025-12-31"),
    "recovery":    ("2026-02-11", "2026-02-28"),
}


def to_unix(date_str: str) -> int:
    return int(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def run_single(pair: str, params: dict, regime: str, start_str: str, end_str: str) -> dict:
    """Run one backtest in a subprocess — full memory isolation."""
    start_ts = to_unix(start_str)
    end_ts = to_unix(end_str)

    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
        output_path = f.name

    z_t = params["z_threshold"]
    z_w = params["z_window"]

    # Minimal subprocess script — returns results dict + max_consec_loss
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
    ct = r.get("close_types", {{}})
    if not isinstance(ct, dict):
        ct = {{}}
    ct_str = {{(k.name if hasattr(k, "name") else str(k)).upper(): v for k, v in ct.items()}}

    # Max consecutive losses
    mcl = 0
    try:
        edf = bt_result.executors_df
        if edf is not None and len(edf) > 0:
            pnls = pd.to_numeric(edf["net_pnl_quote"], errors="coerce").tolist()
            c = 0
            for p in pnls:
                if p < 0:
                    c += 1
                    mcl = max(mcl, c)
                else:
                    c = 0
    except Exception:
        pass

    return {{
        "trades": r.get("total_executors", 0),
        "pf": round(float(r.get("profit_factor", 0)), 3),
        "pnl": round(float(r.get("net_pnl_quote", 0)), 2),
        "dd": round(float(r.get("max_drawdown_usd", 0)), 2),
        "wr": round(float(r.get("accuracy", 0)) * 100, 1),
        "longs": r.get("total_long", 0),
        "shorts": r.get("total_short", 0),
        "close_types": ct_str,
        "max_consec_loss": mcl,
        "sharpe": round(float(r.get("sharpe_ratio", 0)), 3),
    }}

try:
    result = asyncio.run(run())
    result["status"] = "ok"
except Exception as e:
    import traceback
    result = {{"status": "error", "error": str(e)}}

with open("{output_path}", "wb") as f:
    pickle.dump(result, f)
'''

    env = os.environ.copy()
    env["MONGO_URI"] = "mongodb://localhost:27017/quants_lab"
    env["MONGO_DATABASE"] = "quants_lab"

    proc = subprocess.run(
        [PYTHON, "-c", script],
        env=env, timeout=600,
        capture_output=True, text=True,
    )

    if proc.returncode != 0:
        stderr_tail = (proc.stderr or "")[-300:]
        try:
            os.unlink(output_path)
        except OSError:
            pass
        return {
            "pair": pair, "regime": regime, "start": start_str, "end": end_str,
            "error": f"rc={proc.returncode}: {stderr_tail}", "trades": 0,
        }

    try:
        with open(output_path, "rb") as f:
            payload = pickle.load(f)
        os.unlink(output_path)
    except Exception as e:
        return {
            "pair": pair, "regime": regime, "start": start_str, "end": end_str,
            "error": f"pickle error: {e}", "trades": 0,
        }

    if payload.get("status") == "error":
        return {
            "pair": pair, "regime": regime, "start": start_str, "end": end_str,
            "error": payload.get("error", "unknown"), "trades": 0,
        }

    ct = payload.get("close_types", {})
    tp = sum(v for k, v in ct.items() if "TAKE_PROFIT" in k)
    sl = sum(v for k, v in ct.items() if "STOP_LOSS" in k)
    tl = sum(v for k, v in ct.items() if "TIME_LIMIT" in k)
    ts = sum(v for k, v in ct.items() if "TRAILING" in k)

    return {
        "pair": pair, "regime": regime, "start": start_str, "end": end_str,
        "trades": payload.get("trades", 0),
        "pf": payload.get("pf", 0),
        "pnl": payload.get("pnl", 0),
        "dd": payload.get("dd", 0),
        "wr": payload.get("wr", 0),
        "sharpe": payload.get("sharpe", 0),
        "tp": tp, "sl": sl, "tl": tl, "ts_exit": ts,
        "longs": payload.get("longs", 0),
        "shorts": payload.get("shorts", 0),
        "max_consec_loss": payload.get("max_consec_loss", 0),
    }


def main():
    results = []
    total = len(PAIRS) * len(REGIMES)
    done = 0

    for pair, params in PAIRS.items():
        for regime, (start, end) in REGIMES.items():
            done += 1
            print(f"\n[{done}/{total}] {pair} | {regime} | {start} -> {end}")
            print(f"  Params: z_t={params['z_threshold']}, z_w={params['z_window']}")

            r = run_single(pair, params, regime, start, end)
            results.append(r)

            if "error" in r:
                print(f"  ERROR: {r['error'][:150]}")
            else:
                print(f"  Trades={r['trades']}, PF={r['pf']}, PnL=${r['pnl']}, DD=${r['dd']}, WR={r['wr']}%")
                print(f"  Sharpe={r['sharpe']}, L/S={r['longs']}/{r['shorts']}, MCL={r['max_consec_loss']}")
                print(f"  Exits: TP={r['tp']} SL={r['sl']} TL={r['tl']} TS={r['ts_exit']}")

    # Summary table
    print(f"\n\n{'='*90}")
    print("REGIME STRESS TEST SUMMARY -- X12 HL Price Lead")
    print(f"{'='*90}")
    print(f"{'Pair':<16} {'Regime':<14} {'N':>4} {'PF':>6} {'PnL':>8} {'DD':>8} {'WR':>6} {'Sharpe':>7} {'MCL':>4}")
    print("-" * 90)
    for r in results:
        if "error" not in r:
            print(f"{r['pair']:<16} {r['regime']:<14} {r['trades']:>4} {r['pf']:>6.2f} "
                  f"{r['pnl']:>8.2f} {r['dd']:>8.2f} {r['wr']:>5.1f}% {r['sharpe']:>7.3f} {r['max_consec_loss']:>4}")
        else:
            print(f"{r['pair']:<16} {r['regime']:<14} ERROR: {r['error'][:40]}")

    # Break conditions
    print(f"\n{'='*90}")
    print("BREAK CONDITION CHECK")
    print(f"{'='*90}")
    for pair in PAIRS:
        pr = [r for r in results if r["pair"] == pair and "error" not in r]
        pf_vals = [r["pf"] for r in pr if r["trades"] >= 5]
        profitable = sum(1 for r in pr if r["pf"] > 1.0 and r["trades"] >= 5)
        print(f"\n{pair}:")
        print(f"  Profitable regimes: {profitable}/{len(pr)}")
        if pf_vals:
            print(f"  PF range: {min(pf_vals):.3f} - {max(pf_vals):.3f}")
        for r in pr:
            if r["trades"] >= 5 and r["pf"] < 0.8:
                print(f"  !! BREAK: {r['regime']} PF={r['pf']} -- significant loss")
            if r.get("max_consec_loss", 0) > 5:
                print(f"  !! BREAK: {r['regime']} MCL={r['max_consec_loss']}")
            if 0 < r["trades"] < 5:
                print(f"  !! LOW N: {r['regime']} only {r['trades']} trades")

    # Governance gate
    print(f"\n{'='*90}")
    print("GOVERNANCE: Positive expectancy in >= 2 independent regime windows")
    print(f"{'='*90}")
    for pair in PAIRS:
        pr = [r for r in results if r["pair"] == pair and "error" not in r]
        profitable = [r for r in pr if r["pf"] > 1.0 and r["trades"] >= 10]
        verdict = "PASS" if len(profitable) >= 2 else "FAIL"
        print(f"  {pair}: {len(profitable)} profitable regimes (N>=10) -> {verdict}")
        for r in profitable:
            print(f"    {r['regime']}: PF={r['pf']}, {r['trades']} trades, Sharpe={r['sharpe']}")

    # Save
    output_path = "/Users/hermes/quants-lab/app/data/cache/x12_regime_stress.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nSaved to {output_path}")


if __name__ == "__main__":
    main()
