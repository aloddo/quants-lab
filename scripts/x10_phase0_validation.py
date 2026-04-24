#!/usr/bin/env python3
"""
X10 Phase 0 Validation — Liquidation Exhaustion Reversion

Independent validation of the liq_exhaust_revert hypothesis per governance gates.
This script does NOT reuse Alberto's sweep code — it rebuilds the signal from scratch
to catch any implementation bugs or leakage.

Phase 0 gates (all must pass):
  1. Signal edge exists (mean bps > 0, median bps > 0)
  2. Per-pair consistency (>= 60% of pairs profitable OOS)
  3. Long/short split (both sides profitable independently)
  4. Regime breakdown (edge in >= 2 regimes)
  5. Top-5 trade concentration (< 80% of PnL)
  6. Permutation test (p < 0.01)
  7. Walk-forward consistency (>= 3/4 windows positive)
  8. MAE analysis (for SL sizing in controller)

Hypothesis: When total liquidations spike (z > threshold) AND liquidation imbalance
is extreme (|imb_z| > threshold), the forced selling/buying is exhausted. Fade the
imbalance direction: if longs were liquidated more, the selling pressure is done → go LONG.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from pymongo import MongoClient

# ── Config ──────────────────────────────────────────────────────

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
CANDLES_DIR = Path("app/data/cache/candles")

# Signal params (from sweep best config)
Z_TOTAL_THRESHOLD = 2.0   # min total liq z-score
Z_IMB_THRESHOLD = 0.5     # min |imbalance z-score|
HOLD_HOURS = 12
Z_WINDOW = 72             # rolling z-score window (72 hours = 3 days)
Z_MIN_PERIODS = 24

# Evaluation
COST_BPS_ONEWAY = 4.0     # conservative: 0.02% maker + 0.055% taker / 2
TRAIN_FRAC = 0.60         # 60% train, 40% test (generous OOS)
MIN_BARS = 3000

# The 18 pairs from sweep (all liquid, all have Coinalyze data)
PAIRS = [
    "ADA-USDT", "APT-USDT", "AVAX-USDT", "BCH-USDT", "BNB-USDT", "BTC-USDT",
    "DOGE-USDT", "DOT-USDT", "ETH-USDT", "LINK-USDT", "LTC-USDT", "NEAR-USDT",
    "SEI-USDT", "SOL-USDT", "SUI-USDT", "TAO-USDT", "WLD-USDT", "XRP-USDT",
]

# Date range matching sweep (1 year)
START = pd.Timestamp("2025-04-21", tz="UTC")
END = pd.Timestamp("2026-04-21", tz="UTC")


# ── Utilities ───────────────────────────────────────────────────

def rolling_z(s: pd.Series, window: int, min_periods: int) -> pd.Series:
    mu = s.rolling(window, min_periods=min_periods).mean()
    sd = s.rolling(window, min_periods=min_periods).std().replace(0, np.nan)
    return (s - mu) / sd


def classify_regime(ret_series: pd.Series, window: int = 168) -> pd.Series:
    """Classify regime: TREND_UP, TREND_DOWN, RANGE, SHOCK."""
    roll_ret = ret_series.rolling(window).sum()
    roll_vol = ret_series.rolling(window).std() * np.sqrt(window)
    regime = pd.Series("RANGE", index=ret_series.index)
    regime[roll_ret > roll_vol * 0.5] = "TREND_UP"
    regime[roll_ret < -roll_vol * 0.5] = "TREND_DOWN"
    # Shock: vol spike > 2x normal
    vol_z = rolling_z(roll_vol, window * 2, window)
    regime[vol_z > 2.0] = "SHOCK"
    return regime


# ── Data Loading ────────────────────────────────────────────────

def load_panel(pair: str, db) -> Optional[pd.DataFrame]:
    """Load 1h candles + Coinalyze liquidations for a pair."""
    # Candles
    ppath = CANDLES_DIR / f"bybit_perpetual|{pair}|1h.parquet"
    if not ppath.exists():
        return None
    px = pd.read_parquet(ppath, columns=["timestamp", "open", "high", "low", "close", "volume"])

    warmup_start = START - pd.Timedelta(days=30)
    s_ts = int(warmup_start.timestamp())
    e_ts = int(END.timestamp())
    px = px[(px["timestamp"] >= s_ts) & (px["timestamp"] <= e_ts)].copy()
    if len(px) < MIN_BARS:
        return None

    px["ts"] = pd.to_datetime(px["timestamp"], unit="s", utc=True)
    px = px.set_index("ts").sort_index()

    # Coinalyze liquidations
    sm = int(warmup_start.timestamp() * 1000)
    em = int(END.timestamp() * 1000)
    ldocs = list(
        db.coinalyze_liquidations.find(
            {"pair": pair, "timestamp_utc": {"$gte": sm, "$lte": em}},
            {"_id": 0, "timestamp_utc": 1, "long_liquidations_usd": 1,
             "short_liquidations_usd": 1, "total_liquidations_usd": 1}
        ).sort("timestamp_utc", 1)
    )
    if len(ldocs) < 200:
        print(f"  {pair}: only {len(ldocs)} liq docs, skipping")
        return None

    ldf = pd.DataFrame(ldocs)
    ldf["ts"] = pd.to_datetime(ldf["timestamp_utc"], unit="ms", utc=True)
    ldf = ldf.set_index("ts").sort_index()
    ldf = ldf[["long_liquidations_usd", "short_liquidations_usd", "total_liquidations_usd"]]
    ldf = ldf.resample("1h").last().fillna(0.0)

    # Merge
    df = px[["close", "high", "low", "volume"]].join(ldf, how="left")
    df[["long_liquidations_usd", "short_liquidations_usd", "total_liquidations_usd"]] = \
        df[["long_liquidations_usd", "short_liquidations_usd", "total_liquidations_usd"]].fillna(0.0)

    # Features
    lt = df["total_liquidations_usd"]
    ll = df["long_liquidations_usd"]
    ls = df["short_liquidations_usd"]

    df["liq_total_z"] = rolling_z(np.log1p(lt), Z_WINDOW, Z_MIN_PERIODS)
    df["liq_imb"] = (ll - ls) / (lt + 1.0)
    df["liq_imb_z"] = rolling_z(df["liq_imb"], Z_WINDOW, Z_MIN_PERIODS)

    # Return for regime classification
    df["ret_1h"] = df["close"].pct_change(1)
    df["regime"] = classify_regime(df["ret_1h"])

    # ATR for MAE analysis
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - df["close"].shift(1)).abs(),
        (df["low"] - df["close"].shift(1)).abs()
    ], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(14, min_periods=7).mean()

    # Clean
    for c in ["liq_total_z", "liq_imb_z"]:
        df[c] = df[c].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    df = df[(df.index >= START) & (df.index <= END)].copy()
    df = df.dropna(subset=["close"])

    if len(df) < MIN_BARS:
        return None
    return df


# ── Signal Generation ───────────────────────────────────────────

def generate_signal(df: pd.DataFrame) -> np.ndarray:
    """Generate liq_exhaust_revert signal. Returns +1 (long), -1 (short), 0 (no trade)."""
    ltz = df["liq_total_z"].to_numpy()
    liz = df["liq_imb_z"].to_numpy()

    sig = np.zeros(len(df), dtype=np.int8)
    cond = (ltz >= Z_TOTAL_THRESHOLD) & (np.abs(liz) >= Z_IMB_THRESHOLD)
    sig[cond] = -np.sign(liz[cond]).astype(np.int8)
    return sig


# ── Trade Simulation ────────────────────────────────────────────

@dataclass
class Trade:
    entry_bar: int
    exit_bar: int
    side: int       # +1 long, -1 short
    entry_px: float
    exit_px: float
    bps: float
    regime: str
    # MAE/MFE for SL/TP sizing
    mae_bps: float  # max adverse excursion
    mfe_bps: float  # max favorable excursion


def simulate_trades(df: pd.DataFrame, sig: np.ndarray, cost_bps: float) -> List[Trade]:
    """Fixed hold period simulation with MAE/MFE tracking."""
    close = df["close"].to_numpy()
    high = df["high"].to_numpy()
    low = df["low"].to_numpy()
    regimes = df["regime"].to_numpy()
    n = len(close)
    trades: List[Trade] = []

    i = 0
    while i < n - HOLD_HOURS - 1:
        side = sig[i]
        if side == 0:
            i += 1
            continue

        entry_i = i + 1
        exit_i = i + HOLD_HOURS
        if exit_i >= n:
            break

        entry_px = close[entry_i]
        exit_px = close[exit_i]

        # Compute MAE and MFE during hold
        if side == 1:  # long
            worst = np.min(low[entry_i:exit_i + 1])
            best = np.max(high[entry_i:exit_i + 1])
            mae = (entry_px - worst) / entry_px * 1e4
            mfe = (best - entry_px) / entry_px * 1e4
        else:  # short
            worst = np.max(high[entry_i:exit_i + 1])
            best = np.min(low[entry_i:exit_i + 1])
            mae = (worst - entry_px) / entry_px * 1e4
            mfe = (entry_px - best) / entry_px * 1e4

        ret_bps = side * ((exit_px - entry_px) / entry_px) * 1e4 - 2.0 * cost_bps

        trades.append(Trade(
            entry_bar=entry_i,
            exit_bar=exit_i,
            side=side,
            entry_px=entry_px,
            exit_px=exit_px,
            bps=ret_bps,
            regime=regimes[entry_i],
            mae_bps=mae,
            mfe_bps=mfe,
        ))

        i = exit_i + 1  # no overlapping trades

    return trades


# ── Analysis Functions ──────────────────────────────────────────

def pf(bps_arr: np.ndarray) -> float:
    if bps_arr.size == 0:
        return np.nan
    pos = bps_arr[bps_arr > 0].sum()
    neg = -bps_arr[bps_arr < 0].sum()
    return float(pos / neg) if neg > 0 else np.nan


def analyze_trades(trades: List[Trade], label: str) -> dict:
    if not trades:
        return {"label": label, "n": 0}

    bps = np.array([t.bps for t in trades])
    mae = np.array([t.mae_bps for t in trades])
    mfe = np.array([t.mfe_bps for t in trades])

    return {
        "label": label,
        "n": len(trades),
        "mean_bps": float(np.mean(bps)),
        "median_bps": float(np.median(bps)),
        "pf": pf(bps),
        "wr": float((bps > 0).mean()),
        "total_bps": float(np.sum(bps)),
        "std_bps": float(np.std(bps)),
        "mae_median": float(np.median(mae)),
        "mae_p75": float(np.percentile(mae, 75)),
        "mae_p95": float(np.percentile(mae, 95)),
        "mfe_median": float(np.median(mfe)),
        "mfe_p75": float(np.percentile(mfe, 75)),
    }


def top5_concentration(trades: List[Trade]) -> float:
    """What fraction of total PnL comes from top 5 trades?"""
    if len(trades) < 10:
        return 1.0
    bps = sorted([t.bps for t in trades], reverse=True)
    total = sum(b for b in bps if b > 0)
    if total <= 0:
        return 1.0
    top5 = sum(bps[:5])
    return top5 / total


def max_consecutive_losses(trades: List[Trade]) -> int:
    max_streak = 0
    cur = 0
    for t in trades:
        if t.bps < 0:
            cur += 1
            max_streak = max(max_streak, cur)
        else:
            cur = 0
    return max_streak


def permutation_test(trades: List[Trade], n_perms: int = 3000) -> float:
    """Permutation test: shuffle trade directions, check how often we beat observed mean."""
    if len(trades) < 20:
        return 1.0

    bps = np.array([t.bps for t in trades])
    observed = np.mean(bps)

    rng = np.random.default_rng(42)
    count_ge = 0
    for _ in range(n_perms):
        # Shuffle the sign assignment (not the returns themselves)
        shuffled = bps * rng.choice([-1, 1], size=len(bps))
        if np.mean(shuffled) >= observed:
            count_ge += 1

    return (count_ge + 1) / (n_perms + 1)


def walk_forward(df: pd.DataFrame, n_windows: int = 4) -> List[dict]:
    """Expanding walk-forward: train on first k windows, test on k+1."""
    sig = generate_signal(df)
    n = len(df)
    window_size = n // (n_windows + 1)

    results = []
    for w in range(1, n_windows + 1):
        test_start = w * window_size
        test_end = min((w + 1) * window_size, n)

        test_trades = simulate_trades(
            df.iloc[test_start:test_end],
            sig[test_start:test_end],
            COST_BPS_ONEWAY
        )

        stats = analyze_trades(test_trades, f"WF window {w}")
        results.append(stats)

    return results


# ── Main ────────────────────────────────────────────────────────

def main():
    db_name = MONGO_URI.rsplit("/", 1)[-1] if "/" in MONGO_URI else "quants_lab"
    db = MongoClient(MONGO_URI.rsplit("/", 1)[0])[db_name]

    print("=" * 70)
    print("X10 PHASE 0 VALIDATION — Liquidation Exhaustion Reversion")
    print("=" * 70)
    print(f"Signal: liq_total_z >= {Z_TOTAL_THRESHOLD}, |liq_imb_z| >= {Z_IMB_THRESHOLD}")
    print(f"Hold: {HOLD_HOURS}h, Cost: {COST_BPS_ONEWAY} bps one-way")
    print(f"Period: {START.date()} to {END.date()}")
    print(f"Train/Test split: {TRAIN_FRAC:.0%} / {1-TRAIN_FRAC:.0%}")
    print()

    # Load data
    print("Loading panels...")
    panels: Dict[str, pd.DataFrame] = {}
    for pair in PAIRS:
        df = load_panel(pair, db)
        if df is not None:
            panels[pair] = df
            print(f"  {pair}: {len(df)} bars, {df.index[0].date()} to {df.index[-1].date()}")

    print(f"\n{len(panels)} pairs loaded\n")

    # ── GATE 1-2: Signal edge + per-pair consistency ──────────
    print("=" * 70)
    print("GATE 1-2: Signal Edge + Per-Pair Consistency")
    print("=" * 70)

    all_train_trades: List[Trade] = []
    all_test_trades: List[Trade] = []
    pair_test_results: List[dict] = []

    for pair, df in panels.items():
        sig = generate_signal(df)
        n = len(df)
        cut = int(n * TRAIN_FRAC)

        train_trades = simulate_trades(df.iloc[:cut], sig[:cut], COST_BPS_ONEWAY)
        test_trades = simulate_trades(df.iloc[cut:], sig[cut:], COST_BPS_ONEWAY)

        all_train_trades.extend(train_trades)
        all_test_trades.extend(test_trades)

        tr_stats = analyze_trades(train_trades, f"{pair} train")
        te_stats = analyze_trades(test_trades, f"{pair} test")
        pair_test_results.append(te_stats)

        tr_n = tr_stats["n"]
        te_n = te_stats["n"]
        tr_bps = tr_stats.get("mean_bps", 0)
        te_bps = te_stats.get("mean_bps", 0)
        te_pf = te_stats.get("pf", 0)
        te_wr = te_stats.get("wr", 0)

        status = "✓" if te_bps > 0 and te_n >= 5 else "✗"
        print(f"  {pair:15s} train: n={tr_n:4d} {tr_bps:+7.1f}bps | "
              f"test: n={te_n:4d} {te_bps:+7.1f}bps PF={te_pf:.2f} WR={te_wr:.1%} {status}")

    # Pooled stats
    train_stats = analyze_trades(all_train_trades, "POOLED train")
    test_stats = analyze_trades(all_test_trades, "POOLED test")

    print(f"\n  POOLED TRAIN: n={train_stats['n']}, mean={train_stats.get('mean_bps',0):+.1f}bps, "
          f"median={train_stats.get('median_bps',0):+.1f}bps, PF={train_stats.get('pf',0):.2f}, "
          f"WR={train_stats.get('wr',0):.1%}")
    print(f"  POOLED TEST:  n={test_stats['n']}, mean={test_stats.get('mean_bps',0):+.1f}bps, "
          f"median={test_stats.get('median_bps',0):+.1f}bps, PF={test_stats.get('pf',0):.2f}, "
          f"WR={test_stats.get('wr',0):.1%}")

    profitable_pairs = sum(1 for r in pair_test_results if r["n"] >= 5 and r.get("mean_bps", 0) > 0)
    total_pairs = sum(1 for r in pair_test_results if r["n"] >= 5)
    pair_share = profitable_pairs / total_pairs if total_pairs > 0 else 0

    gate1 = test_stats.get("mean_bps", 0) > 0 and test_stats.get("median_bps", 0) > 0
    gate2 = pair_share >= 0.60

    print(f"\n  GATE 1 (mean>0 AND median>0 OOS): {'PASS' if gate1 else 'FAIL'}")
    print(f"  GATE 2 (>=60% pairs profitable): {profitable_pairs}/{total_pairs} = {pair_share:.0%} "
          f"{'PASS' if gate2 else 'FAIL'}")

    # ── GATE 3: Long/Short Split ──────────────────────────────
    print(f"\n{'='*70}")
    print("GATE 3: Long/Short Split")
    print("=" * 70)

    long_trades = [t for t in all_test_trades if t.side == 1]
    short_trades = [t for t in all_test_trades if t.side == -1]

    long_stats = analyze_trades(long_trades, "LONG")
    short_stats = analyze_trades(short_trades, "SHORT")

    print(f"  LONG:  n={long_stats['n']}, mean={long_stats.get('mean_bps',0):+.1f}bps, "
          f"PF={long_stats.get('pf',0):.2f}, WR={long_stats.get('wr',0):.1%}")
    print(f"  SHORT: n={short_stats['n']}, mean={short_stats.get('mean_bps',0):+.1f}bps, "
          f"PF={short_stats.get('pf',0):.2f}, WR={short_stats.get('wr',0):.1%}")

    gate3 = (long_stats.get("mean_bps", 0) > 0 and short_stats.get("mean_bps", 0) > 0
             and long_stats["n"] >= 20 and short_stats["n"] >= 20)
    print(f"\n  GATE 3 (both sides profitable): {'PASS' if gate3 else 'FAIL'}")

    # ── GATE 4: Regime Breakdown ──────────────────────────────
    print(f"\n{'='*70}")
    print("GATE 4: Regime Breakdown")
    print("=" * 70)

    regime_trades: Dict[str, List[Trade]] = {}
    for t in all_test_trades:
        regime_trades.setdefault(t.regime, []).append(t)

    profitable_regimes = 0
    for regime in ["TREND_UP", "TREND_DOWN", "RANGE", "SHOCK"]:
        trades = regime_trades.get(regime, [])
        stats = analyze_trades(trades, regime)
        n = stats["n"]
        bps = stats.get("mean_bps", 0)
        pf_val = stats.get("pf", 0)
        pnl_share = stats.get("total_bps", 0) / test_stats.get("total_bps", 1) * 100 if test_stats.get("total_bps", 0) != 0 else 0

        is_profitable = n >= 10 and bps > 0
        if is_profitable:
            profitable_regimes += 1

        status = "✓" if is_profitable else ("✗" if n >= 10 else "—")
        print(f"  {regime:12s}: n={n:4d}, mean={bps:+7.1f}bps, PF={pf_val:.2f}, "
              f"PnL share={pnl_share:+5.1f}% {status}")

    gate4 = profitable_regimes >= 2
    print(f"\n  GATE 4 (edge in >= 2 regimes): {profitable_regimes} regimes "
          f"{'PASS' if gate4 else 'FAIL'}")

    # ── GATE 5: Top-5 Concentration ──────────────────────────
    print(f"\n{'='*70}")
    print("GATE 5: Top-Trade Concentration")
    print("=" * 70)

    top5 = top5_concentration(all_test_trades)
    max_consec = max_consecutive_losses(all_test_trades)

    gate5 = top5 < 0.80
    print(f"  Top-5 trade PnL share: {top5:.1%} ({'PASS' if gate5 else 'FAIL'} — threshold < 80%)")
    print(f"  Max consecutive losses: {max_consec}")

    # ── GATE 6: Permutation Test ─────────────────────────────
    print(f"\n{'='*70}")
    print("GATE 6: Permutation Test")
    print("=" * 70)

    perm_p = permutation_test(all_test_trades, n_perms=3000)
    gate6 = perm_p < 0.01
    print(f"  Permutation p-value: {perm_p:.4f} ({'PASS' if gate6 else 'FAIL'} — threshold < 0.01)")

    # ── GATE 7: Walk-Forward ──────────────────────────────────
    print(f"\n{'='*70}")
    print("GATE 7: Walk-Forward (4 windows, all pairs pooled)")
    print("=" * 70)

    # Pool all pairs into one time-aligned dataframe for walk-forward
    # Use BTC as the common time index
    btc_df = panels.get("BTC-USDT")
    if btc_df is not None:
        wf_all_trades_by_window: Dict[int, List[Trade]] = {i: [] for i in range(1, 5)}
        n_btc = len(btc_df)
        window_size = n_btc // 5

        for pair, df in panels.items():
            sig = generate_signal(df)
            n = len(df)
            pw = n // 5
            for w in range(1, 5):
                ws = w * pw
                we = min((w + 1) * pw, n)
                trades = simulate_trades(df.iloc[ws:we], sig[ws:we], COST_BPS_ONEWAY)
                wf_all_trades_by_window[w].extend(trades)

        positive_windows = 0
        for w in range(1, 5):
            trades = wf_all_trades_by_window[w]
            stats = analyze_trades(trades, f"WF-{w}")
            n = stats["n"]
            bps = stats.get("mean_bps", 0)
            is_pos = n >= 10 and bps > 0
            if is_pos:
                positive_windows += 1
            print(f"  Window {w}: n={n:4d}, mean={bps:+7.1f}bps, PF={stats.get('pf',0):.2f} "
                  f"{'✓' if is_pos else '✗'}")

        gate7 = positive_windows >= 3
        print(f"\n  GATE 7 ({positive_windows}/4 positive): {'PASS' if gate7 else 'FAIL'}")
    else:
        gate7 = False
        print("  GATE 7: SKIP (no BTC data)")

    # ── GATE 8: MAE Analysis (for SL sizing) ─────────────────
    print(f"\n{'='*70}")
    print("GATE 8: MAE/MFE Analysis (for controller SL/TP sizing)")
    print("=" * 70)

    if all_test_trades:
        mae = np.array([t.mae_bps for t in all_test_trades])
        mfe = np.array([t.mfe_bps for t in all_test_trades])

        winners = [t for t in all_test_trades if t.bps > 0]
        losers = [t for t in all_test_trades if t.bps <= 0]

        win_mae = np.array([t.mae_bps for t in winners]) if winners else np.array([0])
        lose_mae = np.array([t.mae_bps for t in losers]) if losers else np.array([0])
        win_mfe = np.array([t.mfe_bps for t in winners]) if winners else np.array([0])

        print(f"  ALL trades MAE: median={np.median(mae):.0f}bps, p75={np.percentile(mae,75):.0f}bps, p95={np.percentile(mae,95):.0f}bps")
        print(f"  Winners   MAE: median={np.median(win_mae):.0f}bps, p75={np.percentile(win_mae,75):.0f}bps")
        print(f"  Losers    MAE: median={np.median(lose_mae):.0f}bps, p75={np.percentile(lose_mae,75):.0f}bps")
        print(f"  Winners   MFE: median={np.median(win_mfe):.0f}bps, p75={np.percentile(win_mfe,75):.0f}bps")
        print()

        # SL recommendation: should survive 75th pctl winner MAE
        sl_rec = np.percentile(win_mae, 85)
        tp_rec = np.percentile(win_mfe, 50)
        print(f"  Recommended SL: >= {sl_rec:.0f} bps (85th pctl winner MAE)")
        print(f"  Recommended TP: ~{tp_rec:.0f} bps (50th pctl winner MFE)")
        print(f"  R:R estimate: {tp_rec/sl_rec:.2f}" if sl_rec > 0 else "  R:R: N/A")

        # ATR-based recommendation
        atrs = []
        for t in all_test_trades:
            pair = None
            for p, df in panels.items():
                if t.entry_bar < len(df):
                    atrs.append(df["atr_14"].iloc[min(t.entry_bar, len(df)-1)])
                    break
        if atrs:
            median_atr_pct = np.median(atrs) / np.mean([t.entry_px for t in all_test_trades]) * 100
            print(f"  Median ATR(14): {np.median(atrs):.2f} ({median_atr_pct:.2f}% of price)")

        gate8 = True  # informational, not a pass/fail
    else:
        gate8 = True

    # ── SUMMARY ──────────────────────────────────────────────
    print(f"\n{'='*70}")
    print("PHASE 0 SUMMARY")
    print("=" * 70)

    gates = [
        ("Gate 1: Signal edge (mean>0, median>0)", gate1),
        ("Gate 2: Per-pair consistency (>=60%)", gate2),
        ("Gate 3: Long/short split (both +)", gate3),
        ("Gate 4: Regime diversity (>=2)", gate4),
        ("Gate 5: Top-5 concentration (<80%)", gate5),
        ("Gate 6: Permutation test (p<0.01)", gate6),
        ("Gate 7: Walk-forward (>=3/4)", gate7),
        ("Gate 8: MAE analysis (informational)", gate8),
    ]

    all_pass = True
    for name, passed in gates:
        status = "PASS ✓" if passed else "FAIL ✗"
        print(f"  {status}  {name}")
        if not passed and "informational" not in name:
            all_pass = False

    print(f"\n  OVERALL: {'PHASE 0 PASSED — proceed to controller build' if all_pass else 'PHASE 0 FAILED — investigate failures'}")
    print("=" * 70)

    return 0 if all_pass else 1


if __name__ == "__main__":
    sys.exit(main())
