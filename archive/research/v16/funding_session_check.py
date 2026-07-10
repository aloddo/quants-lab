#!/usr/bin/env python3
"""SPRINT bonus (backlog B2+B10): funding-at-entry tilt + session structure on the trade table.

B2: join HL funding rate at entry (coin, hour) onto the 7,911 validated-fold trades; bucket by
SIGNED funding carry (positive = our copied side EARNS funding) and report overlay-net edge.
B10: edge by UTC session (Asia 0-7, EU 7-13, US 13-21, late 21-24).

Run: python research/v16/funding_session_check.py   (~1 min)
"""
from __future__ import annotations
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient

_REPO = Path("/Users/hermes/quants-lab")


def main():
    df = pd.read_parquet(_REPO / "app" / "data" / "v16" / "sprint_trades_enriched.parquet")
    db = MongoClient("mongodb://localhost:27017")["quants_lab"]
    coins = sorted(df.coin.unique())
    t0 = int(df.entry_ts.min() // 1000) - 3700
    t1 = int(df.entry_ts.max() // 1000) + 3700
    fr = list(db["hyperliquid_funding_rates"].find(
        {"coin": {"$in": coins}}, {"coin": 1, "timestamp_utc": 1, "funding_rate": 1, "_id": 0}))
    f = pd.DataFrame(fr).rename(columns={"funding_rate": "rate"})
    ts = f["timestamp_utc"].astype(float)
    if ts.max() > 1e12:
        ts = ts / 1000.0
    f["ts"] = ts
    f = f[(f.ts >= t0) & (f.ts <= t1)].sort_values("ts")
    print(f"{len(f)} funding rows for {f.coin.nunique()} coins")

    out = np.full(len(df), np.nan)
    for c, g in df.groupby("coin"):
        fc = f[f.coin == c]
        if fc.empty:
            continue
        fts = fc.ts.values
        frt = fc.rate.values.astype(float)
        idx = np.searchsorted(fts, df.loc[g.index, "entry_ts"].values / 1000.0, side="right") - 1
        ok = idx >= 0
        vals = np.full(len(g), np.nan)
        vals[ok] = frt[idx[ok]]
        out[g.index] = vals
    df["funding_rate"] = out
    # signed hourly carry in bps: long pays positive funding -> carry = -dir*rate
    df["carry_bph"] = -df["dir"] * df["funding_rate"] * 1e4
    df = df.dropna(subset=["carry_bph"])
    df["carry_hold"] = df["carry_bph"] * df["hold_h"].clip(upper=72)
    print(f"{len(df)} trades with funding")

    def wmed(g):
        return g.groupby("wallet").ov_bps.mean().median()

    for fold, g in df.groupby("fold"):
        print(f"\n=== {fold} ===")
        qs = g.carry_bph.quantile([0.2, 0.4, 0.6, 0.8]).values
        labs = ["pay-hi", "pay-lo", "mid", "earn-lo", "earn-hi"]
        bins = [-np.inf] + list(qs) + [np.inf]
        g2 = g.assign(cb=pd.cut(g.carry_bph, bins, labels=labs))
        for lab, sub in g2.groupby("cb", observed=True):
            print(f"  carry {lab:8s}: n={len(sub):5d} | ov mean {sub.ov_bps.mean():+7.2f} | "
                  f"wmed {wmed(sub):+7.2f} | mean carry/h {sub.carry_bph.mean():+6.3f}bps")
        print(f"  corr(carry_bph, ov_bps) = {g.carry_bph.corr(g.ov_bps):+.3f}")
        g3 = g.assign(sess=pd.cut(g.hour_utc, [-1, 7, 13, 21, 24],
                                  labels=["asia", "eu", "us", "late"]))
        for lab, sub in g3.groupby("sess", observed=True):
            print(f"  sess {lab:5s}: n={len(sub):5d} | ov mean {sub.ov_bps.mean():+7.2f} | "
                  f"wmed {wmed(sub):+7.2f} | {len(sub)/len(g)*100:.0f}% of flow")


if __name__ == "__main__":
    main()
