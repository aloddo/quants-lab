#!/usr/bin/env python3
"""Rolling selection SWEEP over horizon x activity-floor -- "keep pushing" on the
entry-informedness KILL (2026-07-04). The canonical single-config run showed no OOS
persistence at H=4h, MIN_N=30 (mean edge -13.8 bps, persist r~0). This asks: is there
ANY (horizon, activity-floor) where selected-by-t-1 wallets persist AND beat the rest OOS?

Design: ONE pass over the fills per fold (memory-safe, one fold in RAM at a time, per-wallet
aggregates only -- same streaming discipline as the canonical script). For each OPEN entry we
compute the dir-adj forward return at MULTIPLE horizons {1h,4h,24h} off the same entry VWAP.
Then, in-memory and cheap, we run the rolling top-40 selection for every (H, MIN_N) config.

Reuses the canonical fold/fee/slip/bucket constants. No pool, no per-row output -> within the
mandatory streaming-IO safe pattern (aggregates only).
"""
from __future__ import annotations
import glob, os
import numpy as np, pandas as pd, scipy.stats as ss

FEE_RT = 0.000864
SLIP = 0.0002
BUCKET_MS = 900_000
H_LIST = [4, 16, 96]        # 1h, 4h, 24h forward (in 15min buckets)
H_LABEL = {4: "1h", 16: "4h", 96: "24h"}
MIN_N_LIST = [30, 100, 200]
TOPK = 40

FOLDS = [
    ("2025-12", 20251201, 20251231),
    ("2026-01", 20260101, 20260131),
    ("2026-02", 20260201, 20260228),
    ("2026-03", 20260301, 20260331),
    ("2026-04", 20260401, 20260430),
    ("2026-05", 20260501, 20260527),
]


def fold_wallet_stats(lo, hi):
    """Return dict[H] -> DataFrame[wallet, mean_ret, n] for OPEN entries, dir-adj fwd at each H."""
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
    # per-wallet accumulate forward returns at each horizon
    agg = {h: {} for h in H_LIST}   # agg[h][wallet] = [sum_ret, n]
    for w, coin, side, b in entries:
        pm = price.get(coin)
        if not pm:
            continue
        p0 = pm.get(b)
        if not p0 or p0 <= 0:
            continue
        for h in H_LIST:
            p1 = pm.get(b + h * BUCKET_MS)
            if not p1 or p1 <= 0:
                continue
            r = side * (p1 / p0 - 1.0) - FEE_RT - SLIP
            s = agg[h].setdefault(w, [0.0, 0])
            s[0] += r; s[1] += 1
    out = {}
    for h in H_LIST:
        rows = [(w, v[0] / v[1], v[1]) for w, v in agg[h].items() if v[1] >= 1]
        out[h] = pd.DataFrame(rows, columns=["wallet", "mean_ret", "n"]).set_index("wallet")
    return out


def run_config(stats_by_h, names, H, MIN_N):
    """stats_by_h[name][H] -> DataFrame. Run rolling top-40 selection for this (H, MIN_N)."""
    sel_edges, ps, prs = [], [], []
    lines = []
    for i in range(1, len(names)):
        prev = stats_by_h[names[i - 1]][H]
        cur = stats_by_h[names[i]][H]
        prev = prev[prev["n"] >= MIN_N]
        cur = cur[cur["n"] >= MIN_N]
        common = prev.index.intersection(cur.index)
        if len(common) < TOPK + 20:
            lines.append(f"  {names[i-1]}->{names[i]}: insufficient overlap ({len(common)})")
            continue
        prev_c = prev.loc[common].sort_values("mean_ret", ascending=False)
        sel = prev_c.head(TOPK).index
        rest = prev_c.index.difference(sel)
        sret = cur.loc[sel, "mean_ret"].values
        rret = cur.loc[rest, "mean_ret"].values
        p = ss.mannwhitneyu(sret, rret, alternative="greater")[1]
        pr = ss.spearmanr(prev.loc[common, "mean_ret"], cur.loc[common, "mean_ret"])[0]
        edge = (sret.mean() - rret.mean()) * 1e4
        sel_edges.append(edge); ps.append(p); prs.append(pr)
        lines.append(f"  {names[i-1]}->{names[i]}: SEL {sret.mean()*1e4:7.1f} REST {rret.mean()*1e4:7.1f} "
                     f"edge {edge:7.1f} bps | p {p:.3f} | persist r {pr:+.3f}")
    if sel_edges:
        summ = (f"  MEAN edge {np.mean(sel_edges):+.1f} bps | {sum(e>0 for e in sel_edges)}/{len(sel_edges)} pos "
                f"| mean p {np.mean(ps):.3f} | mean persist r {np.mean(prs):+.3f}")
    else:
        summ = "  no evaluable folds"
    return lines, summ


def main():
    # compute per-fold, per-horizon wallet stats ONCE
    stats_by_h = {}
    for name, lo, hi in FOLDS:
        stats_by_h[name] = fold_wallet_stats(lo, hi)
        n30 = (stats_by_h[name][16]["n"] >= 30).sum()
        print(f"{name}: {n30} wallets >=30 opens (4h), univ4h mean_ret "
              f"{stats_by_h[name][16].loc[stats_by_h[name][16]['n']>=30,'mean_ret'].mean()*1e4:.1f} bps", flush=True)
    names = [n for n, _, _ in FOLDS]

    print("\n" + "=" * 78)
    print("SWEEP: does ANY (horizon, activity-floor) give OOS-persistent selection?")
    print("=" * 78)
    grid = []
    for H in H_LIST:
        for MIN_N in MIN_N_LIST:
            lines, summ = run_config(stats_by_h, names, H, MIN_N)
            print(f"\n--- H={H_LABEL[H]}  MIN_N={MIN_N} ---")
            for ln in lines:
                print(ln)
            print(summ)
            grid.append((H_LABEL[H], MIN_N, summ.strip()))
    print("\n" + "=" * 78)
    print("GRID SUMMARY (mean OOS edge / folds-positive / mean persist r):")
    for hl, mn, s in grid:
        print(f"  H={hl:>3} MIN_N={mn:>3} | {s}")


if __name__ == "__main__":
    main()
