#!/usr/bin/env python3
"""SPRINT: build assetctx_marks_sprint/ = consolidated assetctx npys + tape-minute bridge.

For the 10 liquid majors: take the (restored) assetctx npy series and append tape minute-last
marks for every minute AFTER the npy's last timestamp. Other coins: straight copy of the npy.
Output dir: app/data/v15/assetctx_marks_sprint/ (research marks for the forward test ONLY;
the canonical assetctx_marks/ stays pure-S3).

Run AFTER: marks re-download + consolidate, and scripts/sprint_tape_marks.py.
"""
from __future__ import annotations
import shutil
from pathlib import Path

import numpy as np
import pandas as pd

V15 = Path("/Users/hermes/quants-lab/app/data/v15")
SRC = V15 / "assetctx_marks"
DST = V15 / "assetctx_marks_sprint"
LIQ = {"ADA", "AVAX", "BNB", "BTC", "CRV", "DOGE", "ETH", "HYPE", "LINK", "SOL"}


def main():
    DST.mkdir(parents=True, exist_ok=True)
    tape = pd.read_parquet(V15 / "tape_marks_minutes.parquet")
    for p in sorted(SRC.glob("*.npy")):
        coin = p.stem
        if coin not in LIQ:
            continue  # forward test only needs liquid majors; skip copies to save time/disk
        a = np.load(p, allow_pickle=False)
        last_ts = float(a[0, -1]) if a.shape[1] else 0.0
        t = tape[(tape.coin == coin) & (tape.ts_ms > last_ts)].sort_values("ts_ms")
        if len(t):
            ts = np.concatenate([a[0], t.ts_ms.values.astype("float64")])
            px = np.concatenate([a[1], t.px.values.astype("float64")])
            out = np.vstack([ts, px])
        else:
            out = a
        np.save(DST / f"{coin}.npy", out)
        span0 = pd.Timestamp(float(out[0, 0]), unit="ms", tz="UTC").date()
        span1 = pd.Timestamp(float(out[0, -1]), unit="ms", tz="UTC").date()
        print(f"{coin}: {a.shape[1]} ctx + {len(t)} tape -> {out.shape[1]} marks [{span0}..{span1}]")
    print(f"DONE -> {DST}")


if __name__ == "__main__":
    main()
