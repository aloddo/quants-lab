#!/usr/bin/env python3
"""Abnormal-consensus feasibility test (research only; bot PAUSED).

Hypothesis: a real-time signal that does NOT rely on any wallet's persistent skill -- when an ABNORMAL number
of independent wallets enter the SAME coin in the SAME direction within a short window, the aggregated
directional flow predicts a forward move. (Raw consensus is base-rate noise -- 1350 K>=10 events/day -- so
we condition on ABNORMALITY vs the coin's own typical activity + directional imbalance.)

Signal per (coin, 15m bucket):
  n_long  = distinct wallets with dir=="Open Long"
  n_short = distinct wallets with dir=="Open Short"
  z       = (n_long+n_short - coin_median) / coin_std         # abnormal activity
  imb     = (n_long - n_short) / (n_long + n_short)           # directional imbalance
  EVENT if z >= Z and |imb| >= IMB; side = long if imb>0 else short.
Forward return: direction-adjusted bucket-VWAP move h buckets ahead, net fees+slippage.
NULL (baseline): same coin, ALL buckets, direction = sign(imb) of that bucket -- i.e. "follow the majority
side at a random time" without the abnormality condition. If EVENT >> baseline, abnormal consensus adds value.

Uses hl_s3_fills_v2 (wallet, coin, side, price, dir, time, size). In-sample feasibility window (default last
14 days of available fills). Memory-safe: per-day -> bucket aggregates (tiny).
"""
from __future__ import annotations
import argparse, glob, os
import numpy as np, pandas as pd, scipy.stats as ss

FEE_RT = 0.000864
SLIP = 0.0002
BUCKET_MS = 900_000  # 15m


def load_buckets(files):
    rows = []
    for f in files:
        df = pd.read_parquet(f, columns=["wallet", "coin", "price", "size", "time", "dir"])
        df = df[~df["coin"].astype(str).str.contains(":")]   # majors only
        if df.empty:
            continue
        df["b"] = (df["time"] // BUCKET_MS) * BUCKET_MS
        df["sz"] = pd.to_numeric(df["size"], errors="coerce").fillna(0.0)
        df["px"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0)
        df["pv"] = df["px"] * df["sz"]
        opens = df[df["dir"].isin(["Open Long", "Open Short"])]
        gl = opens[opens["dir"] == "Open Long"].groupby(["coin", "b"])["wallet"].nunique().rename("n_long")
        gs = opens[opens["dir"] == "Open Short"].groupby(["coin", "b"])["wallet"].nunique().rename("n_short")
        vw = df.groupby(["coin", "b"]).apply(lambda x: x["pv"].sum() / max(x["sz"].sum(), 1e-9)).rename("vwap")
        g = pd.concat([gl, gs, vw], axis=1).reset_index()
        rows.append(g)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True).fillna({"n_long": 0, "n_short": 0})
    return out.groupby(["coin", "b"]).agg(n_long=("n_long", "sum"), n_short=("n_short", "sum"),
                                          vwap=("vwap", "mean")).reset_index()


def run(buckets, Z, IMB, h_buckets):
    ev_rets, base_rets = [], []
    for coin, g in buckets.groupby("coin"):
        g = g.sort_values("b").reset_index(drop=True)
        if len(g) < h_buckets + 20:
            continue
        tot = g["n_long"] + g["n_short"]
        med, sd = tot.median(), tot.std(ddof=0)
        if not sd or sd <= 0:
            continue
        g["z"] = (tot - med) / sd
        denom = (g["n_long"] + g["n_short"]).replace(0, np.nan)
        g["imb"] = (g["n_long"] - g["n_short"]) / denom
        vwap = g["vwap"].values
        fwd = np.full(len(g), np.nan)
        for i in range(len(g) - h_buckets):
            if vwap[i] > 0 and vwap[i + h_buckets] > 0:
                fwd[i] = vwap[i + h_buckets] / vwap[i] - 1.0
        for i in range(len(g) - h_buckets):
            if np.isnan(fwd[i]) or np.isnan(g["imb"].iloc[i]):
                continue
            side = 1 if g["imb"].iloc[i] > 0 else -1
            dret = side * fwd[i] - FEE_RT - SLIP
            base_rets.append(dret)  # baseline: follow majority side any bucket
            if g["z"].iloc[i] >= Z and abs(g["imb"].iloc[i]) >= IMB:
                ev_rets.append(dret)   # event: abnormal + imbalanced
    ev, ba = np.array(ev_rets), np.array(base_rets)
    p = ss.mannwhitneyu(ev, ba, alternative="greater")[1] if len(ev) >= 10 and len(ba) >= 10 else float("nan")
    return ev, ba, p


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    args = ap.parse_args()
    files = sorted(glob.glob("app/data/hl_s3_fills_v2/*.parquet"))
    files = [f for f in files if os.path.basename(f)[:-8].isdigit()][-args.days:]
    print(f"loading {len(files)} days: {os.path.basename(files[0])}..{os.path.basename(files[-1])}")
    buckets = load_buckets(files)
    print(f"coin-buckets: {len(buckets):,} across {buckets['coin'].nunique()} coins\n")
    print("=== ABNORMAL-CONSENSUS feasibility (direction-adjusted fwd bucket-VWAP return, net fees+slip) ===")
    print(f"{'Z':>4} {'IMB':>4} {'h':>4} | {'EVENT n':>8} {'EVENT bps':>10} {'win%':>5} | {'BASE bps':>9} | p(EV>BASE)")
    for h in (4, 16):           # 1h, 4h
        for Z in (1.5, 2.5):
            for IMB in (0.4,):
                ev, ba, p = run(buckets, Z, IMB, h)
                if len(ev):
                    print(f"{Z:>4} {IMB:>4} {h:>4} | {len(ev):>8} {ev.mean()*1e4:>10.1f} {100*(ev>0).mean():>5.0f} | "
                          f"{ba.mean()*1e4:>9.1f} | {p:.4f}")
                else:
                    print(f"{Z:>4} {IMB:>4} {h:>4} | {0:>8} (no events)")


if __name__ == "__main__":
    main()
