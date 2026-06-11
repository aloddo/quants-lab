#!/usr/bin/env python3
"""SPRINT: build 1m mark npys for the 10 liquid majors from Mongo hyperliquid_candles (closes).

Purpose: unblock v17b replay + knet residual test while the assetctx S3 restore completes.
Coverage: 2025-12-01 -> 2026-05-24 (the candles end; S3-reconstructed, same venue prices).
Output: app/data/v15/assetctx_marks_sprint/COIN.npy -- then scripts/sprint_merge_marks.py appends
the tape bridge (05-24 -> 06-11), giving the full sprint research series.
Caveat (report to codex): candle CLOSE vs assetctx mark_px; rerun on restored assetctx before
the final gate.
"""
from __future__ import annotations
from pathlib import Path

import numpy as np
from pymongo import MongoClient

V15 = Path("/Users/hermes/quants-lab/app/data/v15")
DST = V15 / "assetctx_marks_sprint"
LIQ = ["ADA", "AVAX", "BNB", "BTC", "CRV", "DOGE", "ETH", "HYPE", "LINK", "SOL"]


def main():
    DST.mkdir(parents=True, exist_ok=True)
    db = MongoClient("mongodb://localhost:27017")["quants_lab"]
    t0 = 1764547200000  # 2025-12-01 UTC ms
    for coin in LIQ:
        cur = db["hyperliquid_candles"].find(
            {"coin": coin, "interval": "1m", "timestamp_utc": {"$gte": t0}},
            {"timestamp_utc": 1, "close": 1, "_id": 0}).sort("timestamp_utc", 1)
        ts, px = [], []
        for d in cur:
            ts.append(d["timestamp_utc"]); px.append(d["close"])
        if not ts:
            print(f"{coin}: NO candles"); continue
        arr = np.vstack([np.asarray(ts, dtype="float64"), np.asarray(px, dtype="float64")])
        np.save(DST / f"{coin}.npy", arr)
        import datetime as dt
        print(f"{coin}: {len(ts)} marks [{dt.datetime.utcfromtimestamp(ts[0]/1000).date()}"
              f"..{dt.datetime.utcfromtimestamp(ts[-1]/1000).date()}]")
    print(f"DONE -> {DST}")


if __name__ == "__main__":
    main()
