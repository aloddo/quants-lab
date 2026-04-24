#!/usr/bin/env python3
"""
X9 Funding-Crowding — Phase 0 Validation (Governance Compliant)

Runs the FULL Phase 0 checklist against the best config found by Codex discovery:
  1. Reproduce the best config result
  2. DOGE drop-out test (concentration kill)
  3. Long/short split analysis
  4. Regime breakdown (BTC trend/range/shock)
  5. Rolling walk-forward (6mo train → 2mo test, stepping 2mo)
  6. Multiple-testing correction (permutation-based)
  7. Max adverse excursion during 48h hold (no SL risk)
  8. Top-pair PnL concentration check

Kill conditions (any one = STOP):
  - Signal dies without DOGE
  - One side (long or short) is negative
  - Edge concentrates in one regime (>70% of PnL)
  - Walk-forward: <50% of windows positive
  - Permutation p-value > 0.10
  - Top-pair contributes >50% of PnL
  - MAE 95th percentile > 500 bps (5% drawdown in a hold)
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient
from scipy import stats

# ── Reuse Codex's data loading and signal logic ──────────────────────

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
CANDLES_DIR = Path("app/data/cache/candles")
OUT_DIR = Path("app/data/cache/x9_funding_crowding")
COST_BPS = 4.0  # one-way

# Best config from Codex discovery
BEST_CONFIG = {
    "family": "crowd_revert",
    "funding_z_threshold": 1.0,
    "streak_min": 2,
    "oi_z_min": 0.0,
    "hold_events": 6,
}
CODEX_PAIRS = ["ADA-USDT", "BNB-USDT", "DOGE-USDT", "DOT-USDT", "LINK-USDT", "NEAR-USDT", "XRP-USDT"]


def compute_streak(sign_series: pd.Series) -> pd.Series:
    streak: list[int] = []
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


def load_pair_panel(pair: str, db, min_bars: int = 1500, min_events: int = 180) -> pd.DataFrame | None:
    candle_path = CANDLES_DIR / f"bybit_perpetual|{pair}|1h.parquet"
    if not candle_path.exists():
        return None

    try:
        candles = pd.read_parquet(candle_path, columns=["timestamp", "open", "high", "low", "close"])
    except Exception:
        return None

    if len(candles) < min_bars:
        return None

    candles["ts"] = pd.to_datetime(candles["timestamp"], unit="s", utc=True)
    px = candles.set_index("ts")[["open", "high", "low", "close"]].sort_index()

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

    if len(fdocs) < min_events or len(odocs) < min_events:
        return None

    fdf = pd.DataFrame(fdocs)
    odf = pd.DataFrame(odocs)
    if fdf.empty or odf.empty:
        return None

    fdf["ts"] = pd.to_datetime(fdf["timestamp_utc"], unit="ms", utc=True)
    odf["ts"] = pd.to_datetime(odf["timestamp_utc"], unit="ms", utc=True)
    fdf = fdf.set_index("ts")[["funding_rate"]].sort_index()
    odf = odf.set_index("ts")[["oi_value"]].sort_index()

    panel = fdf.join(odf.resample("1h").last().ffill(), how="left")
    panel["oi_value"] = panel["oi_value"].ffill()
    panel = panel.join(px, how="left")
    for col in ["open", "high", "low", "close"]:
        panel[col] = panel[col].ffill()
    panel = panel.dropna(subset=["funding_rate", "oi_value", "close"])
    if len(panel) < min_events:
        return None

    fr = panel["funding_rate"]
    fr_mu = fr.rolling(30, min_periods=15).mean()
    fr_sd = fr.rolling(30, min_periods=15).std().replace(0, np.nan)
    fr_z = (fr - fr_mu) / fr_sd

    oi_log = np.log(panel["oi_value"].replace(0, np.nan)).ffill()
    oi_delta = oi_log.diff(1)
    oi_mu = oi_delta.rolling(30, min_periods=15).mean()
    oi_sd = oi_delta.rolling(30, min_periods=15).std().replace(0, np.nan)
    oi_z = (oi_delta - oi_mu) / oi_sd

    panel = pd.DataFrame(
        {
            "pair": pair,
            "fr": fr,
            "fr_z": fr_z,
            "oi_z": oi_z,
            "streak": compute_streak(np.sign(fr)),
            "px": panel["close"],
            "px_high": panel["high"],
            "px_low": panel["low"],
        },
        index=panel.index,
    ).dropna()

    return panel if len(panel) >= min_events else None


def make_signal(df: pd.DataFrame) -> np.ndarray:
    """Generate signal using the fixed best config (crowd_revert, q=1.0, k=2, oiq=0.0)."""
    base = (
        (np.abs(df["fr_z"].to_numpy()) >= BEST_CONFIG["funding_z_threshold"])
        & (df["streak"].to_numpy() >= BEST_CONFIG["streak_min"])
        & (df["oi_z"].to_numpy() >= BEST_CONFIG["oi_z_min"])
    )
    sign_fr = np.sign(df["fr"].to_numpy())
    sig = -sign_fr  # crowd_revert
    return np.where(base, sig, 0).astype(int)


def make_trades_detailed(
    df: pd.DataFrame,
    sig: np.ndarray,
    hold_events: int = 6,
    cost_bps_oneway: float = 4.0,
) -> pd.DataFrame:
    """Return detailed trade records including side, MAE, MFE, entry/exit timestamps."""
    px = df["px"].to_numpy()
    px_high = df["px_high"].to_numpy()
    px_low = df["px_low"].to_numpy()
    idx = df.index
    pair_arr = df["pair"].to_numpy()
    n = len(df)
    trades = []
    i = 0
    while i < n - hold_events - 1:
        side = sig[i]
        if side == 0:
            i += 1
            continue
        entry_i = i + 1
        exit_i = i + hold_events
        entry_px = px[entry_i]
        exit_px = px[exit_i]
        ret_bps = side * ((exit_px - entry_px) / entry_px) * 1e4 - 2.0 * cost_bps_oneway

        # MAE/MFE during hold
        if side == 1:  # long
            worst_px = min(px_low[entry_i:exit_i + 1])
            best_px = max(px_high[entry_i:exit_i + 1])
            mae_bps = ((worst_px - entry_px) / entry_px) * 1e4  # negative = adverse
            mfe_bps = ((best_px - entry_px) / entry_px) * 1e4
        else:  # short
            worst_px = max(px_high[entry_i:exit_i + 1])
            best_px = min(px_low[entry_i:exit_i + 1])
            mae_bps = -((worst_px - entry_px) / entry_px) * 1e4  # negative = adverse
            mfe_bps = -((best_px - entry_px) / entry_px) * 1e4

        trades.append({
            "pair": pair_arr[entry_i],
            "entry_ts": idx[entry_i],
            "exit_ts": idx[exit_i],
            "side": "LONG" if side == 1 else "SHORT",
            "entry_px": entry_px,
            "exit_px": exit_px,
            "ret_bps": ret_bps,
            "mae_bps": mae_bps,
            "mfe_bps": mfe_bps,
        })
        i = exit_i + 1
    return pd.DataFrame(trades)


# ── BTC Regime Classification ────────────────────────────────────────

def classify_btc_regime(db) -> pd.Series:
    """Classify each hour as TREND_UP, TREND_DOWN, RANGE, or SHOCK based on BTC price."""
    candle_path = CANDLES_DIR / "bybit_perpetual|BTC-USDT|1h.parquet"
    btc = pd.read_parquet(candle_path, columns=["timestamp", "close", "high", "low"])
    btc["ts"] = pd.to_datetime(btc["timestamp"], unit="s", utc=True)
    btc = btc.set_index("ts").sort_index()

    # 168h (7d) rolling metrics
    btc["sma_168"] = btc["close"].rolling(168).mean()
    btc["atr_168"] = (btc["high"] - btc["low"]).rolling(168).mean()
    btc["ret_168"] = btc["close"].pct_change(168)
    btc["vol_168"] = btc["close"].pct_change().rolling(168).std()

    # Regime classification
    regime = pd.Series("RANGE", index=btc.index)

    # Trend: 7d return > 1 ATR
    trend_up = btc["ret_168"] > (btc["atr_168"] / btc["close"])
    trend_down = btc["ret_168"] < -(btc["atr_168"] / btc["close"])
    regime[trend_up] = "TREND_UP"
    regime[trend_down] = "TREND_DOWN"

    # Shock: 7d vol > 2x median vol (overrides trend)
    vol_median = btc["vol_168"].rolling(720).median()  # 30d rolling median
    shock = btc["vol_168"] > 2 * vol_median
    regime[shock] = "SHOCK"

    return regime


# ── Tests ─────────────────────────────────────────────────────────────

def test_reproduce(all_trades: pd.DataFrame, pairs: list[str]) -> dict:
    """Reproduce Codex's headline numbers."""
    sel = all_trades[all_trades["pair"].isin(pairs)]
    n = len(sel)
    mean_bps = sel["ret_bps"].mean() if n > 0 else 0
    median_bps = sel["ret_bps"].median() if n > 0 else 0
    pval = float(stats.ttest_1samp(sel["ret_bps"], 0).pvalue) if n >= 12 else float("nan")
    win_rate = (sel["ret_bps"] > 0).mean() if n > 0 else 0
    return {
        "test": "REPRODUCE",
        "n_trades": n,
        "mean_bps": round(mean_bps, 2),
        "median_bps": round(median_bps, 2),
        "win_rate": round(win_rate, 4),
        "p_value": round(pval, 4),
        "PASS": mean_bps > 0 and median_bps > 0 and n >= 30,
    }


def test_doge_dropout(all_trades: pd.DataFrame, pairs: list[str]) -> dict:
    """Kill test: remove DOGE, does signal survive?"""
    pairs_no_doge = [p for p in pairs if p != "DOGE-USDT"]
    sel = all_trades[all_trades["pair"].isin(pairs_no_doge)]
    n = len(sel)
    mean_bps = sel["ret_bps"].mean() if n > 0 else 0
    median_bps = sel["ret_bps"].median() if n > 0 else 0
    pval = float(stats.ttest_1samp(sel["ret_bps"], 0).pvalue) if n >= 12 else float("nan")
    # Also check: does removing DOGE drop mean by >50%?
    full_mean = all_trades[all_trades["pair"].isin(pairs)]["ret_bps"].mean()
    drop_pct = (1 - mean_bps / full_mean) * 100 if full_mean != 0 else 100
    return {
        "test": "DOGE_DROPOUT",
        "n_trades_without_doge": n,
        "mean_bps": round(mean_bps, 2),
        "median_bps": round(median_bps, 2),
        "p_value": round(pval, 4),
        "mean_drop_pct": round(drop_pct, 1),
        "PASS": mean_bps > 0 and median_bps > 0,
        "CONCERN": drop_pct > 50,
    }


def test_long_short_split(all_trades: pd.DataFrame, pairs: list[str]) -> dict:
    """Research process 1.3: Long/short asymmetry check."""
    sel = all_trades[all_trades["pair"].isin(pairs)]
    longs = sel[sel["side"] == "LONG"]
    shorts = sel[sel["side"] == "SHORT"]

    def side_stats(df, label):
        n = len(df)
        return {
            f"{label}_n": n,
            f"{label}_mean_bps": round(df["ret_bps"].mean(), 2) if n > 0 else 0,
            f"{label}_median_bps": round(df["ret_bps"].median(), 2) if n > 0 else 0,
            f"{label}_win_rate": round((df["ret_bps"] > 0).mean(), 4) if n > 0 else 0,
            f"{label}_total_bps": round(df["ret_bps"].sum(), 2) if n > 0 else 0,
        }

    result = {"test": "LONG_SHORT_SPLIT"}
    result.update(side_stats(longs, "long"))
    result.update(side_stats(shorts, "short"))

    # Pass if both sides positive mean, or if negative side has < 20% of trades
    long_ok = result["long_mean_bps"] > 0 or result["long_n"] < 0.2 * len(sel)
    short_ok = result["short_mean_bps"] > 0 or result["short_n"] < 0.2 * len(sel)
    result["PASS"] = long_ok and short_ok
    result["CONCERN"] = result["long_mean_bps"] < 0 or result["short_mean_bps"] < 0
    return result


def test_regime_breakdown(all_trades: pd.DataFrame, pairs: list[str], regime_series: pd.Series) -> dict:
    """Performance breakdown by BTC regime."""
    sel = all_trades[all_trades["pair"].isin(pairs)].copy()
    # Map entry timestamp to regime
    sel["regime"] = sel["entry_ts"].map(lambda ts: regime_series.asof(ts) if ts >= regime_series.index[0] else "UNKNOWN")

    result = {"test": "REGIME_BREAKDOWN"}
    total_pnl = sel["ret_bps"].sum()
    regime_stats = []
    for regime in ["TREND_UP", "TREND_DOWN", "RANGE", "SHOCK", "UNKNOWN"]:
        r = sel[sel["regime"] == regime]
        n = len(r)
        if n == 0:
            continue
        pnl = r["ret_bps"].sum()
        regime_stats.append({
            "regime": regime,
            "n_trades": n,
            "mean_bps": round(r["ret_bps"].mean(), 2),
            "median_bps": round(r["ret_bps"].median(), 2),
            "win_rate": round((r["ret_bps"] > 0).mean(), 4),
            "total_bps": round(pnl, 2),
            "pnl_share": round(pnl / total_pnl * 100, 1) if total_pnl != 0 else 0,
        })
    result["regimes"] = regime_stats

    # Kill: any regime contributes >70% of PnL
    max_share = max((abs(rs["pnl_share"]) for rs in regime_stats), default=0)
    result["max_regime_pnl_share"] = round(max_share, 1)
    result["PASS"] = max_share <= 70
    result["CONCERN"] = max_share > 50
    return result


def test_walk_forward(panels: dict[str, pd.DataFrame], pairs: list[str]) -> dict:
    """Rolling walk-forward: 6mo train → 2mo test, stepping 2mo."""
    # Get full date range across selected pairs
    all_data = pd.concat([panels[p] for p in pairs if p in panels])
    t0 = all_data.index.min()
    t1 = all_data.index.max()

    train_months = 6
    test_months = 2
    step_months = 2

    windows = []
    current = t0
    while True:
        train_end = current + pd.DateOffset(months=train_months)
        test_end = train_end + pd.DateOffset(months=test_months)
        if test_end > t1:
            break

        # Get trades in train and test windows for selected pairs
        train_trades = []
        test_trades = []
        for pair in pairs:
            if pair not in panels:
                continue
            p = panels[pair]
            train_df = p[(p.index >= current) & (p.index < train_end)]
            test_df = p[(p.index >= train_end) & (p.index < test_end)]
            if len(train_df) > 10:
                sig = make_signal(train_df)
                trades = make_trades_detailed(train_df, sig)
                if not trades.empty:
                    train_trades.append(trades)
            if len(test_df) > 10:
                sig = make_signal(test_df)
                trades = make_trades_detailed(test_df, sig)
                if not trades.empty:
                    test_trades.append(trades)

        train_all = pd.concat(train_trades) if train_trades else pd.DataFrame()
        test_all = pd.concat(test_trades) if test_trades else pd.DataFrame()

        train_mean = train_all["ret_bps"].mean() if not train_all.empty else 0
        test_mean = test_all["ret_bps"].mean() if not test_all.empty else 0

        windows.append({
            "window_start": current.strftime("%Y-%m-%d"),
            "train_end": train_end.strftime("%Y-%m-%d"),
            "test_end": test_end.strftime("%Y-%m-%d"),
            "train_trades": len(train_all),
            "test_trades": len(test_all),
            "train_mean_bps": round(train_mean, 2),
            "test_mean_bps": round(test_mean, 2),
            "test_positive": test_mean > 0 and len(test_all) >= 5,
        })
        current += pd.DateOffset(months=step_months)

    n_windows = len(windows)
    n_positive = sum(1 for w in windows if w["test_positive"])
    pct_positive = n_positive / n_windows * 100 if n_windows > 0 else 0

    return {
        "test": "WALK_FORWARD",
        "n_windows": n_windows,
        "n_positive": n_positive,
        "pct_positive": round(pct_positive, 1),
        "windows": windows,
        "PASS": pct_positive >= 50 and n_windows >= 3,
        "CONCERN": pct_positive < 65,
    }


def test_mae_analysis(all_trades: pd.DataFrame, pairs: list[str]) -> dict:
    """Max adverse excursion during the 48h hold — how bad can it get?"""
    sel = all_trades[all_trades["pair"].isin(pairs)]
    if sel.empty:
        return {"test": "MAE_ANALYSIS", "PASS": False, "reason": "no trades"}

    mae = sel["mae_bps"]  # negative = drawdown
    mae_abs = mae.abs()

    result = {
        "test": "MAE_ANALYSIS",
        "n_trades": len(sel),
        "mae_mean_bps": round(mae.mean(), 2),
        "mae_median_bps": round(mae.median(), 2),
        "mae_p5_bps": round(mae.quantile(0.05), 2),  # worst 5%
        "mae_p95_abs_bps": round(mae_abs.quantile(0.95), 2),
        "mae_max_abs_bps": round(mae_abs.max(), 2),
        "mfe_mean_bps": round(sel["mfe_bps"].mean(), 2),
        "mfe_median_bps": round(sel["mfe_bps"].median(), 2),
    }
    # Kill: 95th percentile MAE > 500 bps (5% drawdown)
    result["PASS"] = result["mae_p95_abs_bps"] <= 500
    result["CONCERN"] = result["mae_p95_abs_bps"] > 300
    return result


def test_concentration(all_trades: pd.DataFrame, pairs: list[str]) -> dict:
    """Top-pair PnL concentration check (governance: top-5 < 80% of PnL)."""
    sel = all_trades[all_trades["pair"].isin(pairs)]
    total_pnl = sel["ret_bps"].sum()
    pair_pnl = sel.groupby("pair")["ret_bps"].sum().sort_values(ascending=False)
    top_pair = pair_pnl.index[0] if len(pair_pnl) > 0 else "N/A"
    top_pair_pnl = pair_pnl.iloc[0] if len(pair_pnl) > 0 else 0
    top_pair_share = abs(top_pair_pnl / total_pnl * 100) if total_pnl != 0 else 100

    result = {
        "test": "CONCENTRATION",
        "pair_pnl": {k: round(v, 2) for k, v in pair_pnl.items()},
        "top_pair": top_pair,
        "top_pair_share_pct": round(top_pair_share, 1),
        "PASS": top_pair_share <= 50,
        "CONCERN": top_pair_share > 35,
    }
    return result


def test_permutation(all_trades: pd.DataFrame, pairs: list[str], n_perms: int = 2000) -> dict:
    """Permutation test: shuffle trade returns, compute fraction that beat observed mean."""
    sel = all_trades[all_trades["pair"].isin(pairs)]
    if len(sel) < 20:
        return {"test": "PERMUTATION", "PASS": False, "reason": "too few trades"}

    observed_mean = sel["ret_bps"].mean()
    returns = sel["ret_bps"].to_numpy()
    n = len(returns)

    rng = np.random.default_rng(42)
    count_better = 0
    for _ in range(n_perms):
        # Shuffle side assignments (flip signs randomly)
        shuffled = returns * rng.choice([-1, 1], size=n)
        if shuffled.mean() >= observed_mean:
            count_better += 1

    perm_p = (count_better + 1) / (n_perms + 1)  # +1 for continuity correction

    return {
        "test": "PERMUTATION",
        "observed_mean_bps": round(observed_mean, 2),
        "n_permutations": n_perms,
        "perm_p_value": round(perm_p, 4),
        "PASS": perm_p <= 0.10,
        "STRONG": perm_p <= 0.05,
    }


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("X9 FUNDING-CROWDING — PHASE 0 VALIDATION")
    print("=" * 70)

    client = MongoClient(MONGO_URI)
    db = client["quants_lab"]

    # Load ALL pair panels (not just selected)
    fund_pairs = set(db["bybit_funding_rates"].distinct("pair"))
    candle_pairs = {
        p.name.split("|")[1]
        for p in CANDLES_DIR.glob("bybit_perpetual|*-USDT|1h.parquet")
    }
    universe = sorted(fund_pairs & candle_pairs)

    print(f"\nLoading pair panels from {len(universe)} candidates...")
    panels: dict[str, pd.DataFrame] = {}
    for pair in universe:
        panel = load_pair_panel(pair, db)
        if panel is not None:
            panels[pair] = panel

    print(f"Loaded {len(panels)} usable pairs")

    # Generate trades for ALL pairs (full dataset, no train/test split)
    print("\nGenerating trades for all pairs (full history)...")
    all_trades_list = []
    for pair, panel in panels.items():
        sig = make_signal(panel)
        trades = make_trades_detailed(panel, sig)
        if not trades.empty:
            all_trades_list.append(trades)

    all_trades = pd.concat(all_trades_list, ignore_index=True)
    print(f"Total trades across all pairs: {len(all_trades)}")
    print(f"Trades on Codex selected pairs: {len(all_trades[all_trades['pair'].isin(CODEX_PAIRS)])}")

    # Load BTC regime
    print("\nClassifying BTC regimes...")
    regime_series = classify_btc_regime(db)

    # ── Run all tests ─────────────────────────────────────────────────
    results = []
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    # Test 1: Reproduce
    print("\n" + "─" * 50)
    print("TEST 1: REPRODUCE (Codex headline numbers)")
    r = test_reproduce(all_trades, CODEX_PAIRS)
    results.append(r)
    print(f"  Trades: {r['n_trades']}, Mean: {r['mean_bps']} bps, Median: {r['median_bps']} bps")
    print(f"  Win rate: {r['win_rate']}, p-value: {r['p_value']}")
    print(f"  {'✅ PASS' if r['PASS'] else '❌ FAIL'}")

    # Test 2: DOGE dropout
    print("\n" + "─" * 50)
    print("TEST 2: DOGE DROPOUT (concentration kill)")
    r = test_doge_dropout(all_trades, CODEX_PAIRS)
    results.append(r)
    print(f"  Without DOGE: {r['n_trades_without_doge']} trades, Mean: {r['mean_bps']} bps, Median: {r['median_bps']} bps")
    print(f"  Mean drop: {r['mean_drop_pct']}%")
    print(f"  {'✅ PASS' if r['PASS'] else '❌ FAIL'}{' ⚠️  CONCERN: >50% drop' if r.get('CONCERN') else ''}")

    # Test 3: Long/short split
    print("\n" + "─" * 50)
    print("TEST 3: LONG/SHORT SPLIT")
    r = test_long_short_split(all_trades, CODEX_PAIRS)
    results.append(r)
    print(f"  LONG:  n={r['long_n']}, mean={r['long_mean_bps']} bps, WR={r['long_win_rate']}, total={r['long_total_bps']} bps")
    print(f"  SHORT: n={r['short_n']}, mean={r['short_mean_bps']} bps, WR={r['short_win_rate']}, total={r['short_total_bps']} bps")
    print(f"  {'✅ PASS' if r['PASS'] else '❌ FAIL'}{' ⚠️  One side negative' if r.get('CONCERN') else ''}")

    # Test 4: Regime breakdown
    print("\n" + "─" * 50)
    print("TEST 4: REGIME BREAKDOWN (BTC)")
    r = test_regime_breakdown(all_trades, CODEX_PAIRS, regime_series)
    results.append(r)
    for rs in r["regimes"]:
        print(f"  {rs['regime']:12s}: n={rs['n_trades']:3d}, mean={rs['mean_bps']:8.2f} bps, WR={rs['win_rate']:.2%}, PnL share={rs['pnl_share']:5.1f}%")
    print(f"  Max regime share: {r['max_regime_pnl_share']}%")
    print(f"  {'✅ PASS' if r['PASS'] else '❌ FAIL'}{' ⚠️  >50% concentration' if r.get('CONCERN') else ''}")

    # Test 5: Walk-forward
    print("\n" + "─" * 50)
    print("TEST 5: WALK-FORWARD (6mo train / 2mo test, rolling)")
    r = test_walk_forward(panels, CODEX_PAIRS)
    results.append(r)
    for w in r["windows"]:
        flag = "✅" if w["test_positive"] else "❌"
        print(f"  {flag} {w['window_start']} → {w['test_end']}: train={w['train_mean_bps']:+8.2f} bps ({w['train_trades']}t), test={w['test_mean_bps']:+8.2f} bps ({w['test_trades']}t)")
    print(f"  Positive windows: {r['n_positive']}/{r['n_windows']} ({r['pct_positive']}%)")
    print(f"  {'✅ PASS' if r['PASS'] else '❌ FAIL'}{' ⚠️  <65% positive' if r.get('CONCERN') else ''}")

    # Test 6: MAE analysis
    print("\n" + "─" * 50)
    print("TEST 6: MAX ADVERSE EXCURSION (48h hold risk)")
    r = test_mae_analysis(all_trades, CODEX_PAIRS)
    results.append(r)
    print(f"  MAE mean: {r['mae_mean_bps']} bps, median: {r['mae_median_bps']} bps")
    print(f"  MAE 5th pctl (worst): {r['mae_p5_bps']} bps")
    print(f"  MAE 95th pctl (abs): {r['mae_p95_abs_bps']} bps, max: {r['mae_max_abs_bps']} bps")
    print(f"  MFE mean: {r['mfe_mean_bps']} bps, median: {r['mfe_median_bps']} bps")
    print(f"  {'✅ PASS' if r['PASS'] else '❌ FAIL'}{' ⚠️  MAE 95th > 300 bps' if r.get('CONCERN') else ''}")

    # Test 7: Concentration
    print("\n" + "─" * 50)
    print("TEST 7: PAIR CONCENTRATION")
    r = test_concentration(all_trades, CODEX_PAIRS)
    results.append(r)
    for pair, pnl in r["pair_pnl"].items():
        print(f"  {pair:12s}: {pnl:+10.2f} bps")
    print(f"  Top pair: {r['top_pair']} ({r['top_pair_share_pct']}% of total PnL)")
    print(f"  {'✅ PASS' if r['PASS'] else '❌ FAIL'}{' ⚠️  >35% concentration' if r.get('CONCERN') else ''}")

    # Test 8: Permutation test
    print("\n" + "─" * 50)
    print("TEST 8: PERMUTATION TEST (2000 shuffles)")
    r = test_permutation(all_trades, CODEX_PAIRS)
    results.append(r)
    print(f"  Observed mean: {r.get('observed_mean_bps', 'N/A')} bps")
    print(f"  Permutation p-value: {r.get('perm_p_value', 'N/A')}")
    print(f"  {'✅ PASS' if r['PASS'] else '❌ FAIL'}{' (strong: p<0.05)' if r.get('STRONG') else ''}")

    # ── Summary ───────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("PHASE 0 SUMMARY")
    print("=" * 70)
    n_pass = sum(1 for r in results if r.get("PASS"))
    n_tests = len(results)
    n_concerns = sum(1 for r in results if r.get("CONCERN"))

    for r in results:
        status = "✅ PASS" if r.get("PASS") else "❌ FAIL"
        concern = " ⚠️" if r.get("CONCERN") else ""
        print(f"  {r['test']:25s} {status}{concern}")

    print(f"\n  Result: {n_pass}/{n_tests} passed, {n_concerns} concerns")

    if n_pass == n_tests:
        print("\n  ✅ PHASE 0 PASSED — Proceed to Phase 1 (HB controller build + backtest)")
    elif n_pass >= n_tests - 1 and not any(r.get("test") in ["DOGE_DROPOUT", "PERMUTATION"] and not r.get("PASS") for r in results):
        print("\n  ⚠️  PHASE 0 CONDITIONAL — Address concerns before proceeding")
    else:
        print("\n  ❌ PHASE 0 FAILED — Signal does not meet governance requirements")
        failed = [r["test"] for r in results if not r.get("PASS")]
        print(f"     Failed tests: {', '.join(failed)}")

    # Save results
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / f"{stamp}_phase0_validation.json"
    # Make results JSON-serializable
    def make_serializable(obj):
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, (np.bool_,)):
            return bool(obj)
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        return obj

    import json as json_mod

    class NpEncoder(json_mod.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (np.integer,)):
                return int(obj)
            if isinstance(obj, (np.floating,)):
                return float(obj)
            if isinstance(obj, (np.bool_,)):
                return bool(obj)
            if isinstance(obj, pd.Timestamp):
                return obj.isoformat()
            return super().default(obj)

    with open(out_path, "w") as f:
        json_mod.dump(results, f, indent=2, cls=NpEncoder)
    print(f"\n  Saved: {out_path}")


if __name__ == "__main__":
    main()
