#!/usr/bin/env python3
"""SPRINT: analyze sprint_trades.parquet -- edge concentration + conditioning report.

Sections (per fold + pooled):
  A. Rank-depth curve (top-10/20/30/50/100): edge vs flow tradeoff
  B. Consensus-K buckets + burst-position (early/middle/late/solo, 30m window)
  C. Side / coin / hold / leader-conviction splits
  D. Latency curve (0.5/1/2/5s)
  E. Regression: ov_bps ~ K + burst + pre-returns + conviction + FE(coin,side,hour,day),
     cluster SE by coin-day (codex spec 2026-06-11)

Run: python research/v16/sprint_analysis.py [--in app/data/v16/sprint_trades.parquet]
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent
sys.path.insert(0, str(_REPO / "research" / "v15"))
import leadlag_clean_rank_sim as S

TEST_DAYS = {"fold1": 63.0, "fold2": 38.0}
BURST_MS = 30 * 60_000


def wallet_median(df, col):
    """Validated convention: median across per-wallet means."""
    if df.empty:
        return np.nan
    return df.groupby("wallet")[col].mean().median()


def block(df, col="ov_bps", days=1.0):
    if df.empty:
        return "n=0"
    return (f"n={len(df):5d} ({len(df)/days:5.1f}/d) | mean {df[col].mean():+7.2f} | "
            f"med {df[col].median():+7.2f} | wmed {wallet_median(df, col):+7.2f} | "
            f">0 {(df[col] > 0).mean()*100:4.0f}%")


def add_burst(df):
    """burst features among SAME fold cohort trades: same coin+side entries within +/-30m."""
    df = df.sort_values("entry_ts").reset_index(drop=True)
    n_before = np.zeros(len(df), dtype=np.int32)
    n_total = np.zeros(len(df), dtype=np.int32)
    for (f, c, d_), g in df.groupby(["fold", "coin", "dir"]):
        ts = g.entry_ts.values
        idx = g.index.values
        for j, t in enumerate(ts):
            in_w = (ts >= t - BURST_MS) & (ts <= t + BURST_MS)
            n_total[idx[j]] = int(in_w.sum())          # includes self
            n_before[idx[j]] = int(((ts >= t - BURST_MS) & (ts < t)).sum())
    df["burst_n"] = n_total
    df["burst_before"] = n_before
    pos = np.where(df.burst_n > 1, df.burst_before / (df.burst_n - 1), np.nan)
    df["burst_pos"] = pos                                # 0=first, 1=last
    df["burst_bucket"] = np.select(
        [df.burst_n == 1, pos <= 0.2, pos >= 0.6],
        ["solo", "early", "late"], default="middle")
    return df


def add_prereturns(df):
    for label, mins in (("pre5m", 5), ("pre1h", 60), ("pre4h", 240)):
        vals = np.full(len(df), np.nan)
        for i, (c, t) in enumerate(zip(df.coin.values, df.entry_ts.values)):
            m0 = S.mark_at(c, int(t) - mins * 60_000)
            m1 = S.mark_at(c, int(t))
            if m0 and m1 and m0 > 0:
                vals[i] = (m1 - m0) / m0 * 1e4
        df[label] = vals
    # signed pre-return: positive = entering WITH the move
    for label in ("pre5m", "pre1h", "pre4h"):
        df[f"{label}_signed"] = df[label] * df["dir"]
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default=str(_REPO / "app" / "data" / "v16" / "sprint_trades.parquet"))
    ap.add_argument("--days", type=float, default=0, help="override test days (forward mode)")
    args = ap.parse_args()
    df = pd.read_parquet(args.inp)
    print(f"{len(df)} trades, folds: {df.fold.unique().tolist()}")
    df = add_burst(df)
    df = add_prereturns(df)

    for fold, g in df.groupby("fold"):
        days = args.days or TEST_DAYS.get(fold, 30.0)
        print(f"\n{'='*100}\n{fold} ({len(g)} trades, {days:.0f} test days) "
              f"faithful wmed {wallet_median(g,'faithful_bps'):+.2f} | overlay wmed {wallet_median(g,'ov_bps'):+.2f}")
        print(f"\n  A. RANK DEPTH (overlay nets)")
        for N in (10, 20, 30, 50, 100):
            sub = g[g["rank"] <= N]
            print(f"    top-{N:3d}: {block(sub, days=days)}")
        print(f"\n  B1. CONSENSUS K (k_same among top-100 at entry)")
        for lab, m in (("K=0", g.k_same == 0), ("K=1", g.k_same == 1), ("K=2", g.k_same == 2),
                       ("K3-4", (g.k_same >= 3) & (g.k_same <= 4)), ("K>=5", g.k_same >= 5)):
            print(f"    {lab:5s}: {block(g[m], days=days)}")
        print(f"\n  B2. BURST POSITION (same coin+side entries within 30m)")
        for lab in ("solo", "early", "middle", "late"):
            print(f"    {lab:6s}: {block(g[g.burst_bucket == lab], days=days)}")
        print(f"\n  C. SPLITS")
        for lab, m in (("LONG", g.dir > 0), ("SHORT", g.dir < 0)):
            print(f"    {lab:5s}: {block(g[m], days=days)}")
        for c, sub in g.groupby("coin"):
            if len(sub) >= 30:
                print(f"    {c:5s}: {block(sub, days=days)}")
        g2 = g.assign(hold_b=pd.cut(g.hold_h, [0, 1, 6, 24, 72, 1e9],
                                    labels=["<1h", "1-6h", "6-24h", "1-3d", ">3d"]))
        for hb, sub in g2.groupby("hold_b", observed=True):
            print(f"    hold {hb:5s}: {block(sub, days=days)}")
        conv = g[g.leader_open_notional > 0]
        if len(conv) >= 100:
            qs = conv.leader_open_notional.quantile([0.25, 0.5, 0.75])
            cb = pd.cut(conv.leader_open_notional, [0, qs[0.25], qs[0.5], qs[0.75], np.inf],
                        labels=["q1", "q2", "q3", "q4"])
            for q, sub in conv.groupby(cb, observed=True):
                print(f"    conviction {q}: {block(sub, days=days)}")
        print(f"\n  D. LATENCY (faithful nets, mean | wallet-median)")
        for col, lab in (("fl_500", "0.5s"), ("fl_1000", "1s"), ("faithful_bps", "2s"), ("fl_5000", "5s")):
            sub = g.dropna(subset=[col])
            print(f"    {lab:4s}: mean {sub[col].mean():+7.2f} | wmed {wallet_median(sub, col):+7.2f}")
        print(f"\n  E. PRE-RETURN (signed, bps): entering-with-move check (beta confound)")
        for col in ("pre5m_signed", "pre1h_signed", "pre4h_signed"):
            print(f"    {col}: mean {g[col].mean():+8.1f} | corr(ov_bps) {g[col].corr(g.ov_bps):+0.3f}")

    # E. pooled regression with controls (codex spec)
    try:
        import statsmodels.formula.api as smf
        d = df.dropna(subset=["pre5m_signed", "pre1h_signed", "pre4h_signed"]).copy()
        d["k_b"] = pd.cut(d.k_same, [-1, 0, 1, 2, 4, 1e9], labels=["0", "1", "2", "3_4", "5p"])
        d["day"] = (d.entry_ts // 86_400_000).astype(int)
        d["coin_day"] = d.coin + "_" + d.day.astype(str)
        d["conv_log"] = np.log10(d.leader_open_notional.clip(lower=1))
        m = smf.ols("ov_bps ~ C(k_b) + C(burst_bucket) + pre5m_signed + pre1h_signed + pre4h_signed"
                    " + conv_log + rank + C(coin) + C(dir) + C(hour_utc) + C(fold)", data=d).fit(
            cov_type="cluster", cov_kwds={"groups": d["coin_day"]})
        print(f"\n{'='*100}\nE. POOLED REGRESSION (cluster SE by coin-day, n={int(m.nobs)})")
        keep = [i for i in m.params.index if any(k in i for k in
                ("k_b", "burst", "pre", "conv", "rank", "Intercept"))]
        out = pd.DataFrame({"coef": m.params[keep], "se": m.bse[keep], "p": m.pvalues[keep]})
        print(out.round(3).to_string())
    except ImportError:
        print("statsmodels unavailable; skipped regression")

    out_path = Path(args.inp).with_name(Path(args.inp).stem + "_enriched.parquet")
    df.to_parquet(out_path, index=False)
    print(f"\nenriched table -> {out_path}")


if __name__ == "__main__":
    main()
