#!/usr/bin/env python3
"""Consensus conviction-concentration test -- "keep pushing" step 4 (2026-07-04).

The crowd-follow signal is real + correctly signed but its gross edge (~1-2.5 bps) sits far below
taker cost (10.64 bps RT). This asks: does the edge CONCENTRATE in high-conviction events (near-
unanimous crowd, |imb|->1, large crowd) enough to clear maker (2.88) or taker (10.64) cost? The
prior test used sign(imb) on ALL events, diluting extreme signals with near-zero ones.

Method: per (coin,bucket) with >=MIN_PART wallets, conviction = |imb|, dir = sign(imb). Forward
return in the crowd direction at horizons {15m,30m,1h,4h} (flow impact often front-loads then decays).
Pool events across all 6 folds, decile by conviction, report mean GROSS directional forward return
per decile (bps) vs the maker/taker cost lines, plus per-fold min (consistency). If even the top
conviction decile at the best horizon is < maker cost, aggregate crowd-flow is closed.
Memory-safe: per-fold aggregate to (coin,bucket); concat modest float arrays only.
"""
from __future__ import annotations
import glob, os
import numpy as np, pandas as pd

BUCKET_MS = 900_000
H_LIST = [1, 2, 4, 16]     # 15m, 30m, 1h, 4h
H_LABEL = {1: "15m", 2: "30m", 4: "1h", 16: "4h"}
MIN_PART = 5               # a real crowd
MAKER_RT = 0.000288        # HL maker round trip
TAKER_RT = 0.000864        # HL taker round trip

FOLDS = [
    ("2025-12", 20251201, 20251231),
    ("2026-01", 20260101, 20260131),
    ("2026-02", 20260201, 20260228),
    ("2026-03", 20260301, 20260331),
    ("2026-04", 20260401, 20260430),
    ("2026-05", 20260501, 20260527),
]


def fold_events(lo, hi):
    files = sorted(glob.glob("app/data/hl_s3_fills_v2/*.parquet"))
    files = [f for f in files if os.path.basename(f)[:-8].isdigit()
             and lo <= int(os.path.basename(f)[:-8]) <= hi]
    price = {}
    flow = {}
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
            key = (r.coin, r.b)
            e = flow.setdefault(key, [0.0, 0.0, set()])
            notional = r.px * r.sz
            if r.dir == "Open Long":
                e[0] += notional
            else:
                e[1] += notional
            e[2].add(r.wallet)
    rows = []
    for (coin, b), (ln, sn, ws) in flow.items():
        gross = ln + sn
        if gross <= 0 or len(ws) < MIN_PART:
            continue
        imb = (ln - sn) / gross
        d = 1.0 if imb >= 0 else -1.0
        pm = price.get(coin, {})
        p0 = pm.get(b)
        if not p0 or p0 <= 0:
            continue
        rec = {"conv": abs(imb), "part": len(ws)}
        any_fwd = False
        for h in H_LIST:
            p1 = pm.get(b + h * BUCKET_MS)
            rec[f"g{h}"] = d * (p1 / p0 - 1.0) if (p1 and p1 > 0) else np.nan
            any_fwd = any_fwd or not np.isnan(rec[f"g{h}"])
        if any_fwd:
            rows.append(rec)
    return pd.DataFrame(rows)


def main():
    parts = []
    for name, lo, hi in FOLDS:
        d = fold_events(lo, hi)
        d["fold"] = name
        parts.append(d)
        print(f"{name}: {len(d)} events (part>={MIN_PART})", flush=True)
    allev = pd.concat(parts, ignore_index=True)
    print(f"\nTOTAL {len(allev)} events. Cost lines: maker RT {MAKER_RT*1e4:.2f} bps | taker RT {TAKER_RT*1e4:.2f} bps")
    print("=" * 90)
    print("GROSS directional forward return by CONVICTION decile (crowd direction), bps")
    print("=" * 90)
    for h in H_LIST:
        col = f"g{h}"
        sub = allev[allev[col].notna()].copy()
        sub["dec"] = pd.qcut(sub["conv"], 10, labels=False, duplicates="drop")
        print(f"\n--- horizon {H_LABEL[h]} (n={len(sub)}) ---")
        print(f"{'decile':>6} {'conv_lo':>8} {'conv_hi':>8} {'n':>8} {'gross bps':>10} {'foldmin bps':>11}")
        for dec in sorted(sub["dec"].dropna().unique()):
            g = sub[sub["dec"] == dec]
            gross = g[col].mean() * 1e4
            fmin = g.groupby("fold")[col].mean().min() * 1e4
            print(f"{int(dec):>6} {g['conv'].min():>8.2f} {g['conv'].max():>8.2f} {len(g):>8} "
                  f"{gross:>10.2f} {fmin:>11.2f}")
        top = sub[sub["dec"] == sub["dec"].max()]
        tg = top[col].mean() * 1e4
        verdict = ("BEATS TAKER" if tg > TAKER_RT*1e4 else "beats maker" if tg > MAKER_RT*1e4 else "sub-maker (dead)")
        print(f"  top-decile gross {tg:.2f} bps -> {verdict}")


if __name__ == "__main__":
    main()
