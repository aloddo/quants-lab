#!/usr/bin/env python3
"""Multi-regime beta-neutral walk-forward of the markout-informed copy pool (research; bot PAUSED).

Addresses codex #5 (single-window -> needs walk-forward across regimes) and #2 (regime coverage). We have
203 days of fills (2025-07 .. 2026-05) spanning multiple regimes (Feb -25% crash, Dec -21%, Apr +7% rallies,
chop). Question: does the markout-INFORMED pool (selected on Apr9-May12 markout) show ANY beta-neutral
directional skill vs a frequency-matched no-edge control, in regimes OUTSIDE the selection window?

Metric per OPEN entry (dir startswith 'Open', majors): direction-adjusted forward return over h 15m buckets
using per-coin fill-VWAP series, net fees+slip. Compare informed vs control per regime fold.
If informed never beats control across folds -> copy thesis airtight-closed. Memory-safe (per-day streaming).
"""
from __future__ import annotations
import glob, os
import numpy as np, pandas as pd, scipy.stats as ss
from research.v15.entry_copy_overlay_sim import select_informed
from research.v15.entry_copy_robustness2 import freq_matched_control

FEE_RT = 0.000864
SLIP = 0.0002
BUCKET_MS = 900_000
H = 16  # 4h forward

# regime folds by YYYYMMDD (inclusive ranges), chosen from the BTC regime map
FOLDS = [
    ("2025-12 decline", 20251201, 20251231),
    ("2026-01 chop",    20260101, 20260131),
    ("2026-02 crash",   20260201, 20260228),
    ("2026-03 recover", 20260301, 20260331),
    ("2026-04 rally",   20260401, 20260430),
    ("2026-05 late",    20260501, 20260527),
]


def main():
    feat = "app/data/wallet_alpha/wallet_features.parquet"
    informed, _ = select_informed(feat, max_freq=10, min_tstat=1.0, min_winrate=0.50, top_k=80)
    control = freq_matched_control(feat, informed, max_freq=10, top_k=80)
    inf_set = set(informed["wallet"]); ctl_set = set(control["wallet"])
    print(f"informed={len(inf_set)} control={len(ctl_set)}")

    files = sorted(glob.glob("app/data/hl_s3_fills_v2/*.parquet"))
    files = [f for f in files if os.path.basename(f)[:-8].isdigit()
             and 20251201 <= int(os.path.basename(f)[:-8]) <= 20260527]
    print(f"{len(files)} day-files in [20251201,20260527]")

    price = {}        # coin -> dict(bucket -> vwap)
    entries = []      # (coin, side, bucket, group, yyyymmdd)
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
            side = 1 if r.dir == "Open Long" else -1
            entries.append((r.coin, side, r.b, grp, ymd))
    print(f"price coins={len(price)}, entries={len(entries)}")

    def fwd(coin, b, side):
        pm = price.get(coin)
        if not pm:
            return None
        p0 = pm.get(b)
        p1 = pm.get(b + H * BUCKET_MS)
        if not p0 or not p1 or p0 <= 0 or p1 <= 0:
            return None
        return side * (p1 / p0 - 1.0) - FEE_RT - SLIP

    rows = []
    for coin, side, b, grp, ymd in entries:
        r = fwd(coin, b, side)
        if r is not None:
            rows.append((ymd, grp, r))
    d = pd.DataFrame(rows, columns=["ymd", "grp", "ret"])
    print(f"\n=== MULTI-REGIME BETA-NEUTRAL (dir-adj fwd 4h return, net fees+slip): informed vs control ===")
    print(f"{'fold':>18} | {'INF bps':>9} {'win%':>5} {'n':>6} | {'CTL bps':>9} {'n':>6} | p(INF>CTL)")
    for name, lo, hi in FOLDS:
        sub = d[(d["ymd"] >= lo) & (d["ymd"] <= hi)]
        I = sub[sub["grp"] == "inf"]["ret"].values
        C = sub[sub["grp"] == "ctl"]["ret"].values
        p = ss.mannwhitneyu(I, C, alternative="greater")[1] if len(I) >= 10 and len(C) >= 10 else float("nan")
        iw = 100 * (I > 0).mean() if len(I) else float("nan")
        print(f"{name:>18} | {(I.mean()*1e4 if len(I) else float('nan')):>9.1f} {iw:>5.0f} {len(I):>6} | "
              f"{(C.mean()*1e4 if len(C) else float('nan')):>9.1f} {len(C):>6} | {p:.4f}")
    # pooled
    I = d[d["grp"] == "inf"]["ret"].values; C = d[d["grp"] == "ctl"]["ret"].values
    p = ss.mannwhitneyu(I, C, alternative="greater")[1] if len(I) >= 10 and len(C) >= 10 else float("nan")
    print(f"{'POOLED':>18} | {I.mean()*1e4:>9.1f} {100*(I>0).mean():>5.0f} {len(I):>6} | "
          f"{C.mean()*1e4:>9.1f} {len(C):>6} | {p:.4f}")


if __name__ == "__main__":
    main()
