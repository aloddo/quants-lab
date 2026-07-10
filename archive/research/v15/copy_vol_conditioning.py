#!/usr/bin/env python3
"""Volatility conditioning of the informed-copy edge (research; bot PAUSED).

The 6-fold walk-forward showed the markout-informed pool's beta-neutral edge is REGIME-DEPENDENT (strong in
Jan chop + Feb crash, negative in Dec decline + Mar recovery). To avoid fold-level curve-fitting, test the
hypothesis at the ENTRY level with an economically-motivated regime variable: trailing realized VOLATILITY
of the coin at entry. If the informed-minus-control edge rises MONOTONICALLY with vol (high-vol -> edge,
low-vol -> none/negative), that is a first-principles regime rule, supported by ~142k entries, not 6 folds.

Per coin: fill-VWAP 15m series -> log returns -> trailing vol over W buckets at each entry. Bin entries by
GLOBAL vol quintile; per bin report informed vs control beta-neutral 4h dir-adj return + Mann-Whitney.
Also a 70/30 time SPLIT: pick the vol threshold on the FIRST 70% of days, apply to the held-out last 30%.
"""
from __future__ import annotations
import glob, os
import numpy as np, pandas as pd, scipy.stats as ss
from research.v15.entry_copy_overlay_sim import select_informed
from research.v15.entry_copy_robustness2 import freq_matched_control

FEE_RT = 0.000864
SLIP = 0.0002
BUCKET_MS = 900_000
H = 16          # 4h forward
VOLW = 16       # trailing 4h vol window


def main():
    feat = "app/data/wallet_alpha/wallet_features.parquet"
    informed, _ = select_informed(feat, max_freq=10, min_tstat=1.0, min_winrate=0.50, top_k=80)
    control = freq_matched_control(feat, informed, max_freq=10, top_k=80)
    inf_set, ctl_set = set(informed["wallet"]), set(control["wallet"])

    files = sorted(glob.glob("app/data/hl_s3_fills_v2/*.parquet"))
    files = [f for f in files if os.path.basename(f)[:-8].isdigit()
             and 20251201 <= int(os.path.basename(f)[:-8]) <= 20260527]
    print(f"{len(files)} day-files; informed={len(inf_set)} control={len(ctl_set)}")

    price = {}        # coin -> dict(bucket -> vwap)
    entries = []      # (coin, side, bucket, grp, ymd)
    for f in files:
        ymd = int(os.path.basename(f)[:-8])
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
        op = df[(df["dir"].isin(["Open Long", "Open Short"])) & (df["wallet"].isin(inf_set | ctl_set))]
        for r in op.itertuples():
            grp = "inf" if r.wallet in inf_set else "ctl"
            entries.append((r.coin, 1 if r.dir == "Open Long" else -1, r.b, grp, ymd))
    print(f"price coins={len(price)} entries={len(entries)}")

    # per-coin sorted bucket arrays + trailing-vol lookup
    coin_arr = {}
    for coin, pm in price.items():
        bs = np.array(sorted(pm.keys()))
        vs = np.array([pm[b] for b in bs])
        lr = np.diff(np.log(np.clip(vs, 1e-12, None)), prepend=np.log(max(vs[0], 1e-12)))
        coin_arr[coin] = (bs, vs, lr, {b: i for i, b in enumerate(bs)})

    rows = []
    for coin, side, b, grp, ymd in entries:
        ca = coin_arr.get(coin)
        if not ca:
            continue
        bs, vs, lr, idx = ca
        i = idx.get(b)
        if i is None or i < VOLW or i + H >= len(bs):
            continue
        p0, p1 = vs[i], vs[i + H]
        if p0 <= 0 or p1 <= 0:
            continue
        ret = side * (p1 / p0 - 1.0) - FEE_RT - SLIP
        vol = lr[i - VOLW:i].std()
        rows.append((ymd, grp, ret, vol))
    d = pd.DataFrame(rows, columns=["ymd", "grp", "ret", "vol"]).dropna()
    print(f"usable entries with vol: {len(d)}\n")

    # GLOBAL vol quintiles
    d["vq"] = pd.qcut(d["vol"], 5, labels=False, duplicates="drop")
    print("=== EDGE vs trailing-vol quintile (informed - control, beta-neutral 4h, net) ===")
    print(f"{'volQ':>5} {'vol~bps':>8} | {'INF bps':>9} {'n':>6} | {'CTL bps':>9} {'n':>6} | {'edge':>7} p(INF>CTL)")
    for q in sorted(d["vq"].dropna().unique()):
        s = d[d["vq"] == q]
        I = s[s["grp"] == "inf"]["ret"].values; C = s[s["grp"] == "ctl"]["ret"].values
        p = ss.mannwhitneyu(I, C, alternative="greater")[1] if len(I) >= 10 and len(C) >= 10 else float("nan")
        print(f"{int(q):>5} {s['vol'].median()*1e4:>8.1f} | {I.mean()*1e4:>9.1f} {len(I):>6} | "
              f"{C.mean()*1e4:>9.1f} {len(C):>6} | {(I.mean()-C.mean())*1e4:>7.1f} {p:.4f}")

    # 70/30 TIME split: choose vol threshold (top-2 quintiles boundary) on first 70% of days, apply to last 30%
    days = np.array(sorted(d["ymd"].unique()))
    cut = days[int(len(days) * 0.7)]
    tr, te = d[d["ymd"] < cut], d[d["ymd"] >= cut]
    thr = tr["vol"].quantile(0.6)   # "high vol" = top 40%, threshold learned on TRAIN only
    print(f"\n70/30 split at {cut}: high-vol threshold (train 60th pct) = {thr*1e4:.1f}bps")
    for label, seg in (("TRAIN", tr), ("HELD-OUT", te)):
        hi = seg[seg["vol"] >= thr]
        I = hi[hi["grp"] == "inf"]["ret"].values; C = hi[hi["grp"] == "ctl"]["ret"].values
        p = ss.mannwhitneyu(I, C, alternative="greater")[1] if len(I) >= 10 and len(C) >= 10 else float("nan")
        print(f"  {label:>9} high-vol: INF {I.mean()*1e4:7.1f}bps (n{len(I)}) | CTL {C.mean()*1e4:7.1f}bps "
              f"(n{len(C)}) | edge {(I.mean()-C.mean())*1e4:6.1f} | p(INF>CTL)={p:.4f}")


if __name__ == "__main__":
    main()
