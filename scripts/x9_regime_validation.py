#!/usr/bin/env python3
"""
X9 Funding-Crowding — Regime Validation (multi-year, 4 regime windows)

Research process requires: "Positive expectancy in >= 2 independent regime windows"

We have 1h candles back to 2021 (4-5 years) but only 1m candles from Mar 2025.
This script tests the SIGNAL across all regimes using the Codex pandas methodology
(forward returns at funding event timestamps), NOT the HB BacktestingEngine.

This validates whether the edge exists in each regime. The HB backtest at 1m
(which validates exit mechanics) is limited to the 410-day 1m window.

Regime windows:
  1. 2022 Bear (May-Nov 2022): Luna crash + FTX collapse, sustained downtrend
  2. 2023 Range (Jan-Sep 2023): Low-vol sideways, BTC $16k-30k
  3. 2024 Bull (Jan-Jul 2024): ETF approval, BTC $40k-70k
  4. 2025 Mixed (Jul 2025-Apr 2026): Recent period, mixed conditions

Usage:
    MONGO_URI=mongodb://localhost:27017/quants_lab MONGO_DATABASE=quants_lab \
    /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/x9_regime_validation.py
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient
from scipy import stats

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
CANDLES_DIR = Path("app/data/cache/candles")
COST_BPS = 4.0
HOLD_EVENTS = 6  # 48h

# Best config from Codex discovery
FUNDING_Z_THRESHOLD = 1.0
STREAK_MIN = 2
OI_Z_MIN = 0.0
Z_WINDOW = 30
Z_MIN_PERIODS = 15

PAIRS = ["ADA-USDT", "BNB-USDT", "DOGE-USDT", "DOT-USDT", "LINK-USDT", "NEAR-USDT", "XRP-USDT"]

# Regime windows (start, end, label, description)
REGIME_WINDOWS = [
    ("2022-05-01", "2022-11-30", "2022_BEAR", "Luna+FTX bear crash"),
    ("2023-01-01", "2023-09-30", "2023_RANGE", "Low-vol sideways"),
    ("2024-01-01", "2024-07-31", "2024_BULL", "ETF bull run"),
    ("2025-07-01", "2026-04-20", "2025_MIXED", "Recent mixed conditions"),
]


def compute_streak(sign_series: pd.Series) -> pd.Series:
    streak = []
    cur = 0
    prev = 0
    for v in sign_series.astype(float).fillna(0.0).to_numpy():
        s = int(np.sign(v))
        if s == 0:
            cur = 0
        elif s == prev:
            cur += 1
        else:
            cur = 1
        streak.append(cur)
        if s != 0:
            prev = s
    return pd.Series(streak, index=sign_series.index, dtype=float)


def load_pair_panel(pair: str, db) -> pd.DataFrame | None:
    """Load full history panel for a pair (1h candles + funding + OI)."""
    candle_path = CANDLES_DIR / f"bybit_perpetual|{pair}|1h.parquet"
    if not candle_path.exists():
        return None

    candles = pd.read_parquet(candle_path, columns=["timestamp", "close"])
    candles["ts"] = pd.to_datetime(candles["timestamp"], unit="s", utc=True)
    px = candles.set_index("ts")[["close"]].sort_index().rename(columns={"close": "px"})

    fdocs = list(
        db["bybit_funding_rates"]
        .find({"pair": pair}, {"_id": 0, "timestamp_utc": 1, "funding_rate": 1})
        .sort("timestamp_utc", 1)
    )
    odocs = list(
        db["bybit_open_interest"]
        .find({"pair": pair}, {"_id": 0, "timestamp_utc": 1, "oi_value": 1})
        .sort("timestamp_utc", 1)
    )

    if len(fdocs) < 100:
        return None

    fdf = pd.DataFrame(fdocs)
    fdf["ts"] = pd.to_datetime(fdf["timestamp_utc"], unit="ms", utc=True)
    fdf = fdf.set_index("ts")[["funding_rate"]].sort_index()

    panel = fdf.copy()
    if odocs:
        odf = pd.DataFrame(odocs)
        odf["ts"] = pd.to_datetime(odf["timestamp_utc"], unit="ms", utc=True)
        odf = odf.set_index("ts")[["oi_value"]].sort_index()
        panel = panel.join(odf.resample("1h").last().ffill(), how="left")
        panel["oi_value"] = panel["oi_value"].ffill()
    else:
        panel["oi_value"] = 0.0

    panel = panel.join(px, how="left")
    panel["px"] = panel["px"].ffill()
    panel = panel.dropna(subset=["funding_rate", "px"])

    if len(panel) < 100:
        return None

    # Compute features
    fr = panel["funding_rate"]
    fr_mu = fr.rolling(Z_WINDOW, min_periods=Z_MIN_PERIODS).mean()
    fr_sd = fr.rolling(Z_WINDOW, min_periods=Z_MIN_PERIODS).std().replace(0, np.nan)
    panel["fr_z"] = (fr - fr_mu) / fr_sd

    if "oi_value" in panel.columns and (panel["oi_value"] != 0).any():
        oi_log = np.log(panel["oi_value"].replace(0, np.nan)).ffill()
        oi_delta = oi_log.diff(1)
        oi_mu = oi_delta.rolling(Z_WINDOW, min_periods=Z_MIN_PERIODS).mean()
        oi_sd = oi_delta.rolling(Z_WINDOW, min_periods=Z_MIN_PERIODS).std().replace(0, np.nan)
        panel["oi_z"] = (oi_delta - oi_mu) / oi_sd
        # Fill NaN with 0.0 (neutral — passes oi_z_min=0.0 threshold)
        # OI data only starts Apr 2025; without this fill, all pre-2025 rows are killed
        panel["oi_z"] = panel["oi_z"].fillna(0.0)
    else:
        panel["oi_z"] = 0.0

    panel["streak"] = compute_streak(np.sign(fr))
    panel = panel.dropna(subset=["fr_z"])

    return panel


def make_trades(panel: pd.DataFrame) -> pd.DataFrame:
    """Generate trades from panel using X9 signal rules."""
    fr = panel["funding_rate"].to_numpy()
    fr_z = panel["fr_z"].to_numpy()
    oi_z = panel["oi_z"].fillna(-999).to_numpy()
    streak = panel["streak"].to_numpy()
    px = panel["px"].to_numpy()
    idx = panel.index
    n = len(panel)

    trades = []
    i = 0
    while i < n - HOLD_EVENTS - 1:
        # Check trigger
        if (abs(fr_z[i]) >= FUNDING_Z_THRESHOLD
                and streak[i] >= STREAK_MIN
                and oi_z[i] >= OI_Z_MIN):
            side = -int(np.sign(fr[i]))  # crowd_revert
            if side == 0:
                i += 1
                continue

            entry_i = i + 1
            exit_i = min(i + HOLD_EVENTS, n - 1)
            entry_px = px[entry_i]
            exit_px = px[exit_i]
            ret_bps = side * ((exit_px - entry_px) / entry_px) * 1e4 - 2.0 * COST_BPS

            trades.append({
                "entry_ts": idx[entry_i],
                "side": "LONG" if side == 1 else "SHORT",
                "ret_bps": ret_bps,
            })
            i = exit_i + 1
        else:
            i += 1

    return pd.DataFrame(trades) if trades else pd.DataFrame()


def main():
    print("=" * 70)
    print("X9 FUNDING-CROWDING — REGIME VALIDATION (multi-year)")
    print("=" * 70)

    client = MongoClient(MONGO_URI)
    db = client["quants_lab"]

    # Load all pair panels (full history)
    print("\nLoading pair panels (full history)...")
    panels = {}
    for pair in PAIRS:
        panel = load_pair_panel(pair, db)
        if panel is not None:
            panels[pair] = panel
            start = panel.index.min().strftime("%Y-%m")
            end = panel.index.max().strftime("%Y-%m")
            print(f"  {pair}: {len(panel)} events ({start} → {end})")

    # Run each regime window
    print(f"\n{'='*70}")
    print("REGIME RESULTS")
    print(f"{'='*70}")

    regime_results = []
    for start_str, end_str, label, description in REGIME_WINDOWS:
        start_dt = pd.Timestamp(start_str, tz="UTC")
        end_dt = pd.Timestamp(end_str, tz="UTC")

        print(f"\n{'─'*50}")
        print(f"{label}: {description} ({start_str} → {end_str})")
        print(f"{'─'*50}")

        all_trades = []
        pair_results = []
        for pair, panel in panels.items():
            window = panel[(panel.index >= start_dt) & (panel.index <= end_dt)]
            if len(window) < 30:
                continue

            trades = make_trades(window)
            if trades.empty:
                pair_results.append({"pair": pair, "trades": 0, "mean_bps": 0})
                continue

            all_trades.append(trades)
            mean_bps = trades["ret_bps"].mean()
            n_trades = len(trades)
            wr = (trades["ret_bps"] > 0).mean()
            n_long = (trades["side"] == "LONG").sum()
            n_short = (trades["side"] == "SHORT").sum()
            pair_results.append({
                "pair": pair, "trades": n_trades, "mean_bps": mean_bps,
                "wr": wr, "n_long": n_long, "n_short": n_short,
            })
            flag = "✅" if mean_bps > 0 else "❌"
            print(f"  {flag} {pair}: {n_trades} trades, mean={mean_bps:+.1f} bps, WR={wr:.0%} (L={n_long}, S={n_short})")

        # Portfolio stats
        if all_trades:
            combined = pd.concat(all_trades, ignore_index=True)
            portfolio_mean = combined["ret_bps"].mean()
            portfolio_median = combined["ret_bps"].median()
            portfolio_n = len(combined)
            portfolio_wr = (combined["ret_bps"] > 0).mean()
            pval = float(stats.ttest_1samp(combined["ret_bps"], 0).pvalue) if portfolio_n >= 12 else float("nan")
            n_positive_pairs = sum(1 for pr in pair_results if pr["mean_bps"] > 0 and pr["trades"] > 0)
            n_active_pairs = sum(1 for pr in pair_results if pr["trades"] > 0)

            print(f"\n  PORTFOLIO: {portfolio_n} trades, mean={portfolio_mean:+.1f} bps, median={portfolio_median:+.1f} bps, WR={portfolio_wr:.0%}, p={pval:.4f}")
            print(f"  Positive pairs: {n_positive_pairs}/{n_active_pairs}")

            regime_results.append({
                "regime": label,
                "description": description,
                "trades": portfolio_n,
                "mean_bps": portfolio_mean,
                "median_bps": portfolio_median,
                "wr": portfolio_wr,
                "p_value": pval,
                "positive_pairs": n_positive_pairs,
                "active_pairs": n_active_pairs,
                "PASS": portfolio_mean > 0 and portfolio_n >= 20,
            })
        else:
            print(f"\n  NO TRADES in this regime window")
            regime_results.append({
                "regime": label, "description": description,
                "trades": 0, "mean_bps": 0, "PASS": False,
            })

    # Summary
    print(f"\n{'='*70}")
    print("REGIME VALIDATION SUMMARY")
    print(f"{'='*70}")

    n_pass = 0
    for r in regime_results:
        status = "✅ PASS" if r.get("PASS") else "❌ FAIL"
        if r["trades"] > 0:
            print(f"  {status} {r['regime']:12s}: {r['trades']:3d} trades, mean={r['mean_bps']:+7.1f} bps, WR={r.get('wr', 0):.0%}, p={r.get('p_value', 1):.4f}")
        else:
            print(f"  {status} {r['regime']:12s}: no trades")
        if r.get("PASS"):
            n_pass += 1

    print(f"\n  Positive regimes: {n_pass}/{len(regime_results)}")
    if n_pass >= 2:
        print(f"  ✅ REGIME GATE PASSED (>= 2 independent positive regimes)")
    else:
        print(f"  ❌ REGIME GATE FAILED (need >= 2 independent positive regimes)")


if __name__ == "__main__":
    main()
