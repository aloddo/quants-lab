#!/usr/bin/env python3
"""
X12 Phase 1.3b — Discriminator Analysis

What distinguishes X12 winners from losers?
- Premium magnitude at entry (bigger premium = better convergence?)
- ATR regime at entry (volatile vs calm?)
- Time of day (funding settlement windows?)
- Signal strength (extreme z vs marginal z?)

Goal: ONE clean filter that improves PF without killing trade count.
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

TRAIN_START = "2025-10-01"
TRAIN_END = "2026-02-28"


def to_unix(date_str: str) -> int:
    return int(datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())


def run_discriminator(pair: str, params: dict) -> dict:
    """Run backtest and extract detailed per-trade features for discriminator analysis."""
    start_ts = to_unix(TRAIN_START)
    end_ts = to_unix(TRAIN_END)
    z_t = params["z_threshold"]
    z_w = params["z_window"]

    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
        output_path = f.name

    script = f'''
import asyncio, sys, os, logging, pickle
import pandas as pd
import numpy as np
logging.basicConfig(level=logging.WARNING)
sys.path.insert(0, "/Users/hermes/quants-lab")
os.environ["MONGO_URI"] = "mongodb://localhost:27017/quants_lab"
os.environ["MONGO_DATABASE"] = "quants_lab"

from decimal import Decimal
from pymongo import MongoClient
from core.backtesting.engine import BacktestingEngine
from app.engines.strategy_registry import build_backtest_config
from app.data_sources.merge import merge_all_for_engine_sync
import pandas_ta as ta

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

    # Get processed data to match features at trade entry
    features_df = merged.copy()
    hl_close = pd.to_numeric(features_df["hl_close"], errors="coerce")
    by_close = pd.to_numeric(features_df["close"], errors="coerce")
    features_df["hl_premium_bps"] = ((hl_close - by_close) / by_close * 10000).fillna(0)
    features_df["atr"] = ta.atr(features_df["high"], features_df["low"], features_df["close"], length=14)
    features_df["atr_pct"] = features_df["atr"] / features_df["close"] * 100
    features_df["volume_z"] = (features_df["volume"] - features_df["volume"].rolling(24).mean()) / features_df["volume"].rolling(24).std()

    mu = features_df["hl_premium_bps"].rolling({z_w}, min_periods=max(6, {z_w}//4)).mean()
    sd = features_df["hl_premium_bps"].rolling({z_w}, min_periods=max(6, {z_w}//4)).std().replace(0, np.nan)
    features_df["premium_z"] = ((features_df["hl_premium_bps"] - mu) / sd).replace([np.inf, -np.inf], np.nan).fillna(0)

    features_df["dt"] = pd.to_datetime(features_df["timestamp"], unit="s", utc=True)
    features_df["hour"] = features_df["dt"].dt.hour

    trades_data = []
    if edf is not None and len(edf) > 0:
        edf["net_pnl_quote"] = pd.to_numeric(edf["net_pnl_quote"], errors="coerce")
        edf["timestamp"] = pd.to_numeric(edf["timestamp"], errors="coerce")

        for _, row in edf.iterrows():
            entry_ts = float(row["timestamp"])
            pnl = float(row["net_pnl_quote"])

            # Parse side
            cfg = row.get("config", {{}})
            side = str(cfg.get("side", "")).upper() if isinstance(cfg, dict) else ""

            # Find closest candle bar
            ts_diffs = abs(features_df["timestamp"] - entry_ts)
            closest_idx = ts_diffs.idxmin()
            feat_row = features_df.loc[closest_idx]

            trades_data.append({{
                "pnl": pnl,
                "win": pnl > 0,
                "side": side,
                "premium_z": float(feat_row.get("premium_z", 0)),
                "premium_bps": float(feat_row.get("hl_premium_bps", 0)),
                "atr_pct": float(feat_row.get("atr_pct", 0)),
                "volume_z": float(feat_row.get("volume_z", 0)),
                "hour": int(feat_row.get("hour", 0)),
                "close_type": str(row.get("close_type", "")),
            }})

    return {{
        "status": "ok",
        "total_trades": r.get("total_executors", 0),
        "total_pf": round(float(r.get("profit_factor", 0)), 3),
        "trades_data": trades_data,
    }}

try:
    result = asyncio.run(run())
except Exception as e:
    import traceback
    result = {{"status": "error", "error": str(e), "tb": traceback.format_exc()}}

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
        return {"error": f"subprocess failed: {(proc.stderr or '')[-300:]}"}

    with open(output_path, "rb") as f:
        payload = pickle.load(f)
    os.unlink(output_path)

    return payload


def analyze_discriminators(pair: str, trades_data: list):
    """Analyze what distinguishes winners from losers."""
    import statistics

    winners = [t for t in trades_data if t["win"]]
    losers = [t for t in trades_data if not t["win"]]

    print(f"\n  Winners: {len(winners)}, Losers: {len(losers)}")

    def compare_feature(feature_name, w_vals, l_vals):
        if not w_vals or not l_vals:
            return None
        w_mean = statistics.mean(w_vals)
        l_mean = statistics.mean(l_vals)
        pooled = w_vals + l_vals
        pooled_std = statistics.stdev(pooled) if len(pooled) > 1 else 1
        separation = abs(w_mean - l_mean) / pooled_std if pooled_std > 0 else 0
        return {
            "feature": feature_name,
            "w_mean": round(w_mean, 4),
            "l_mean": round(l_mean, 4),
            "sep_pct": round(separation * 100, 1),
            "direction": "higher=better" if w_mean > l_mean else "lower=better",
        }

    features_to_check = [
        ("premium_z (abs)", [abs(t["premium_z"]) for t in winners], [abs(t["premium_z"]) for t in losers]),
        ("premium_bps (abs)", [abs(t["premium_bps"]) for t in winners], [abs(t["premium_bps"]) for t in losers]),
        ("atr_pct", [t["atr_pct"] for t in winners], [t["atr_pct"] for t in losers]),
        ("volume_z", [t["volume_z"] for t in winners], [t["volume_z"] for t in losers]),
    ]

    print(f"\n  {'Feature':<20} {'W_mean':>8} {'L_mean':>8} {'Sep%':>6} {'Direction'}")
    print(f"  {'-'*60}")
    for name, w_vals, l_vals in features_to_check:
        result = compare_feature(name, w_vals, l_vals)
        if result:
            print(f"  {result['feature']:<20} {result['w_mean']:>8.4f} {result['l_mean']:>8.4f} "
                  f"{result['sep_pct']:>5.1f}% {result['direction']}")

    # Time of day analysis
    hour_buckets = {}
    for t in trades_data:
        h = t["hour"]
        if h not in hour_buckets:
            hour_buckets[h] = {"wins": 0, "losses": 0, "pnl": 0}
        hour_buckets[h]["pnl"] += t["pnl"]
        if t["win"]:
            hour_buckets[h]["wins"] += 1
        else:
            hour_buckets[h]["losses"] += 1

    print(f"\n  Hourly breakdown (top 5 best, top 5 worst by PnL):")
    sorted_hours = sorted(hour_buckets.items(), key=lambda x: x[1]["pnl"], reverse=True)
    for h, stats in sorted_hours[:5]:
        total = stats["wins"] + stats["losses"]
        wr = stats["wins"] / total * 100 if total > 0 else 0
        print(f"    Hour {h:>2}: N={total:>3}, WR={wr:>5.1f}%, PnL=${stats['pnl']:>7.2f}")
    print(f"    ...")
    for h, stats in sorted_hours[-3:]:
        total = stats["wins"] + stats["losses"]
        wr = stats["wins"] / total * 100 if total > 0 else 0
        print(f"    Hour {h:>2}: N={total:>3}, WR={wr:>5.1f}%, PnL=${stats['pnl']:>7.2f}")

    # ATR regime split (above/below median)
    atr_values = [t["atr_pct"] for t in trades_data]
    if atr_values:
        median_atr = statistics.median(atr_values)
        high_atr = [t for t in trades_data if t["atr_pct"] >= median_atr]
        low_atr = [t for t in trades_data if t["atr_pct"] < median_atr]

        def calc_pf(trades):
            gp = sum(t["pnl"] for t in trades if t["pnl"] > 0)
            gl = abs(sum(t["pnl"] for t in trades if t["pnl"] < 0))
            return gp / gl if gl > 0 else float("inf")

        print(f"\n  ATR regime split (median={median_atr:.3f}%):")
        print(f"    High ATR: N={len(high_atr)}, PF={calc_pf(high_atr):.3f}, "
              f"WR={sum(1 for t in high_atr if t['win'])/len(high_atr)*100:.1f}%")
        print(f"    Low ATR:  N={len(low_atr)}, PF={calc_pf(low_atr):.3f}, "
              f"WR={sum(1 for t in low_atr if t['win'])/len(low_atr)*100:.1f}%")


def main():
    print("=" * 80)
    print("X12 Phase 1.3b — Discriminator Analysis")
    print(f"Train: {TRAIN_START} -> {TRAIN_END}")
    print("=" * 80)

    for pair, params in PAIRS.items():
        print(f"\n{'='*60}")
        print(f"  {pair} | z_t={params['z_threshold']}, z_w={params['z_window']}")
        print(f"{'='*60}")

        payload = run_discriminator(pair, params)
        if payload.get("status") == "error":
            print(f"  ERROR: {payload.get('error', 'unknown')}")
            if "tb" in payload:
                print(f"  {payload['tb'][-300:]}")
            continue

        trades_data = payload.get("trades_data", [])
        if not trades_data:
            print("  No trade data extracted")
            continue

        print(f"  Total: {payload['total_trades']} trades, PF={payload['total_pf']}")
        analyze_discriminators(pair, trades_data)


if __name__ == "__main__":
    main()
