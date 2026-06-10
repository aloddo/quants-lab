#!/usr/bin/env python3
"""Rolling per-fold re-selection -- the canonical copy test (research; bot PAUSED).

Every prior result used a FIXED, stale Apr-May markout pool. This tests the copy thesis the RIGHT way: does
a wallet's INFORMEDNESS in period t-1 predict its beta-neutral directional return in period t? (fresh
selection every period). If selected-by-t-1 wallets do NOT beat the rest in t, OOS, across folds -> the copy
thesis is airtight-closed. If they do -> a real rolling edge the stale pool was hiding.

Informedness(wallet, fold) = mean direction-adjusted forward 4h return of the wallet's OPEN entries within
the fold, using per-coin fill-VWAP (a fill-derived markout). Walk-forward: rank wallets by fold[t-1]
informedness (min activity), measure their fold[t] mean return vs the rest. Significance at the WALLET level
(MW across wallets) to avoid pseudo-replication. Memory-safe: ONE fold (~1 month of files) at a time ->
per-wallet aggregates only.
"""
from __future__ import annotations
import glob, os
import numpy as np, pandas as pd, scipy.stats as ss

FEE_RT = 0.000864
SLIP = 0.0002
BUCKET_MS = 900_000
H = 16          # 4h forward
MIN_N = 30      # min opens in a fold for a wallet to be rankable
TOPK = 40       # selected set size

FOLDS = [
    ("2025-12", 20251201, 20251231),
    ("2026-01", 20260101, 20260131),
    ("2026-02", 20260201, 20260228),
    ("2026-03", 20260301, 20260331),
    ("2026-04", 20260401, 20260430),
    ("2026-05", 20260501, 20260527),
]


def fold_wallet_stats(lo, hi):
    """Return DataFrame[wallet, mean_ret, n] for OPEN entries in [lo,hi], dir-adj fwd 4h via fill-VWAP."""
    files = sorted(glob.glob("app/data/hl_s3_fills_v2/*.parquet"))
    files = [f for f in files if os.path.basename(f)[:-8].isdigit()
             and lo <= int(os.path.basename(f)[:-8]) <= hi]
    price = {}
    entries = []   # (wallet, coin, side, bucket)
    for f in files:
        df = pd.read_parquet(f, columns=["wallet", "coin", "price", "size", "time", "dir"])
        df = df[~df["coin"].astype(str).str.contains(":")]
        if df.empty:
            continue
        df["b"] = (df["time"] // BUCKET_MS) * BUCKET_MS
        df["sz"] = pd.to_numeric(df["size"], errors="coerce").fillna(0.0)
        df["px"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
        df["pv"] = df["px"] * df["sz"]
        vw = df.groupby(["coin", "b"]).apply(lambda x: x["pv"].sum() / max(x["sz"].sum(), 1e-9))
        for (coin, b), v in vw.items():
            price.setdefault(coin, {})[b] = v
        op = df[df["dir"].isin(["Open Long", "Open Short"])]
        for r in op.itertuples():
            entries.append((r.wallet, r.coin, 1 if r.dir == "Open Long" else -1, r.b))
    # per-wallet accumulate forward returns
    agg = {}
    for w, coin, side, b in entries:
        pm = price.get(coin)
        if not pm:
            continue
        p0 = pm.get(b); p1 = pm.get(b + H * BUCKET_MS)
        if not p0 or not p1 or p0 <= 0 or p1 <= 0:
            continue
        r = side * (p1 / p0 - 1.0) - FEE_RT - SLIP
        s = agg.setdefault(w, [0.0, 0])
        s[0] += r; s[1] += 1
    rows = [(w, v[0] / v[1], v[1]) for w, v in agg.items() if v[1] >= MIN_N]
    return pd.DataFrame(rows, columns=["wallet", "mean_ret", "n"])


def main():
    stats = {}
    for name, lo, hi in FOLDS:
        s = fold_wallet_stats(lo, hi)
        stats[name] = s.set_index("wallet")
        print(f"{name}: {len(s)} rankable wallets (>= {MIN_N} opens), "
              f"univ mean_ret {s['mean_ret'].mean()*1e4:.1f} bps")
    names = [n for n, _, _ in FOLDS]
    print(f"\n=== ROLLING SELECTION: top-{TOPK} by fold[t-1] informedness -> fold[t] beta-neutral mean (net) ===")
    print(f"{'t-1 -> t':>18} | {'SEL t bps':>10} {'REST t bps':>11} {'edge':>7} | p(SEL>REST) | {'persist r':>9}")
    sel_edges = []
    for i in range(1, len(names)):
        prev, cur = stats[names[i - 1]], stats[names[i]]
        common = prev.index.intersection(cur.index)
        if len(common) < TOPK + 20:
            print(f"{names[i-1]+' -> '+names[i]:>18} | insufficient overlap ({len(common)})")
            continue
        prev_c = prev.loc[common].sort_values("mean_ret", ascending=False)
        sel = prev_c.head(TOPK).index
        rest = prev_c.index.difference(sel)
        sret = cur.loc[sel, "mean_ret"].values
        rret = cur.loc[rest, "mean_ret"].values
        p = ss.mannwhitneyu(sret, rret, alternative="greater")[1]
        # persistence: rank corr of informedness t-1 vs t across common wallets
        pr = ss.spearmanr(prev.loc[common, "mean_ret"], cur.loc[common, "mean_ret"])[0]
        edge = (sret.mean() - rret.mean()) * 1e4
        sel_edges.append(edge)
        print(f"{names[i-1]+' -> '+names[i]:>18} | {sret.mean()*1e4:>10.1f} {rret.mean()*1e4:>11.1f} {edge:>7.1f} | "
              f"{p:>10.4f} | {pr:>9.3f}")
    if sel_edges:
        print(f"\nmean selected-edge across folds: {np.mean(sel_edges):.1f} bps "
              f"({sum(e>0 for e in sel_edges)}/{len(sel_edges)} folds positive)")


if __name__ == "__main__":
    main()
