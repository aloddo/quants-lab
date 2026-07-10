#!/usr/bin/env python3
"""Consensus / crowding FLOW signal -- "keep pushing" step 3 (2026-07-04).

Wallet-level selection is closed (entry-informedness KILL + 9-config horizon/floor sweep: no
individual wallet's past markout predicts its future OOS). This aggregates the crowd instead of
picking members: for each (coin, time-bucket), does the NET directional flow of copy-wallet opens
predict the coin's forward return? A tradeable order-flow signal, independent of any single wallet
persisting.

Signal per (coin, bucket) = size-weighted imbalance = (longN - shortN)/(longN + shortN) in [-1,1],
comparable across coins. Require >= MIN_PART distinct wallets opening in the bucket (a real crowd).
Forward return = dir at horizon h off the bucket VWAP. Because the copy universe is net-NEGATIVE
(-5..-16 bps/fold), we test the crowd as a FADE as well as a FOLLOW signal.

Walk-forward: on fold[t-1] measure sign(corr(imbalance, fwd_ret)) -> choose FOLLOW (+) or FADE (-);
apply that sign on fold[t] OOS. Strategy return per event = chosen_sign * sign(imbalance) * fwd_ret
- costs (taker RT). Report per-fold OOS mean net return + t-stat, and the pooled Spearman corr.
Memory-safe: one fold in RAM, aggregate to (coin,bucket) rows only.
"""
from __future__ import annotations
import glob, os
import numpy as np, pandas as pd, scipy.stats as ss

FEE_RT = 0.000864
SLIP = 0.0002
COST = FEE_RT + SLIP
BUCKET_MS = 900_000
H_LIST = [4, 16]           # 1h, 4h forward
H_LABEL = {4: "1h", 16: "4h"}
MIN_PART_LIST = [3, 5, 10] # min distinct wallets opening in a (coin,bucket)

FOLDS = [
    ("2025-12", 20251201, 20251231),
    ("2026-01", 20260101, 20260131),
    ("2026-02", 20260201, 20260228),
    ("2026-03", 20260301, 20260331),
    ("2026-04", 20260401, 20260430),
    ("2026-05", 20260501, 20260527),
]


def fold_bucket_flow(lo, hi):
    """Return DataFrame[coin,b,imb,part,fwd_1h,fwd_4h] aggregated per (coin,bucket)."""
    files = sorted(glob.glob("app/data/hl_s3_fills_v2/*.parquet"))
    files = [f for f in files if os.path.basename(f)[:-8].isdigit()
             and lo <= int(os.path.basename(f)[:-8]) <= hi]
    price = {}
    # per (coin,b): long_notional, short_notional, set(wallets)
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
            e = flow.setdefault(key, [0.0, 0.0, set()])  # longNotional, shortNotional, wallets
            notional = r.px * r.sz
            if r.dir == "Open Long":
                e[0] += notional
            else:
                e[1] += notional
            e[2].add(r.wallet)
    rows = []
    for (coin, b), (ln, sn, ws) in flow.items():
        gross = ln + sn
        if gross <= 0:
            continue
        imb = (ln - sn) / gross
        pm = price.get(coin, {})
        p0 = pm.get(b)
        if not p0 or p0 <= 0:
            continue
        rec = {"coin": coin, "b": b, "imb": imb, "part": len(ws)}
        ok = False
        for h in H_LIST:
            p1 = pm.get(b + h * BUCKET_MS)
            rec[f"fwd_{h}"] = (p1 / p0 - 1.0) if (p1 and p1 > 0) else np.nan
            ok = ok or not np.isnan(rec[f"fwd_{h}"])
        if ok:
            rows.append(rec)
    return pd.DataFrame(rows)


def eval_config(folds_df, names, H, MIN_PART):
    col = f"fwd_{H}"
    # per-fold filtered event tables
    ev = {}
    for n in names:
        d = folds_df[n]
        d = d[(d["part"] >= MIN_PART) & d[col].notna()].copy()
        ev[n] = d
    lines = []
    oos_means, oos_ts = [], []
    for i in range(1, len(names)):
        prev, cur = ev[names[i - 1]], ev[names[i]]
        if len(prev) < 30 or len(cur) < 30:
            lines.append(f"  {names[i-1]}->{names[i]}: thin ({len(prev)}/{len(cur)})")
            continue
        # choose FOLLOW/FADE on IS corr sign
        is_corr = ss.spearmanr(prev["imb"], prev[col])[0]
        sign = 1.0 if is_corr >= 0 else -1.0
        # OOS strategy return: enter in chosen direction, magnitude = |imb| gate already by MIN_PART
        dir_signal = sign * np.sign(cur["imb"].values)
        strat = dir_signal * cur[col].values - COST
        m = np.nanmean(strat)
        t = m / (np.nanstd(strat) / np.sqrt(len(strat)) + 1e-12)
        oos_corr = ss.spearmanr(cur["imb"], cur[col])[0]
        oos_means.append(m * 1e4); oos_ts.append(t)
        lab = "FOLLOW" if sign > 0 else "FADE"
        lines.append(f"  {names[i-1]}->{names[i]}: {lab} (IS r{is_corr:+.3f}) | OOS {m*1e4:+7.1f} bps net "
                     f"t={t:+.2f} n={len(cur)} | OOS r{oos_corr:+.3f}")
    if oos_means:
        summ = (f"  MEAN OOS {np.mean(oos_means):+.1f} bps net | {sum(x>0 for x in oos_means)}/{len(oos_means)} pos "
                f"| mean t {np.mean(oos_ts):+.2f}")
    else:
        summ = "  no evaluable folds"
    return lines, summ


def main():
    folds_df = {}
    for name, lo, hi in FOLDS:
        d = fold_bucket_flow(lo, hi)
        folds_df[name] = d
        print(f"{name}: {len(d)} (coin,bucket) flow events | median part {d['part'].median():.0f} "
              f"| mean imb {d['imb'].mean():+.3f}", flush=True)
    names = [n for n, _, _ in FOLDS]
    print("\n" + "=" * 78)
    print("CONSENSUS FLOW: does crowd imbalance predict forward return? (follow vs fade, OOS)")
    print("=" * 78)
    grid = []
    for H in H_LIST:
        for MP in MIN_PART_LIST:
            lines, summ = eval_config(folds_df, names, H, MP)
            print(f"\n--- H={H_LABEL[H]}  MIN_PART={MP} ---")
            for ln in lines:
                print(ln)
            print(summ)
            grid.append((H_LABEL[H], MP, summ.strip()))
    print("\n" + "=" * 78)
    print("GRID SUMMARY (mean OOS net bps / folds-pos / mean t):")
    for hl, mp, s in grid:
        print(f"  H={hl:>3} MIN_PART={mp:>3} | {s}")


if __name__ == "__main__":
    main()
