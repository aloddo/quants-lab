#!/usr/bin/env python3
"""SPRINT: 1-min last-trade price series for the 10 liquid majors from Mongo hl_wallet_trades.

Purpose: bridge the asset_ctxs publication lag (S3 asset_ctxs ends ~2026-06-01; the forward test
needs marks through 06-10). Tape = real traded prices; minute-last on majors is dense.

Outputs app/data/v15/tape_marks_minutes.parquet: coin, ts_ms (minute), px (last trade in minute).
FIDELITY CHECK: on the overlap days (05-25..06-01) compares tape minute-last vs assetctx mark_px
(daily shards) -> reports bps deviation quantiles per coin. This number goes into the codex gate.

Run: python scripts/sprint_tape_marks.py     (~2-4 min)
"""
from __future__ import annotations
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient

REPO = Path("/Users/hermes/quants-lab")
LIQ = ["ADA", "AVAX", "BNB", "BTC", "CRV", "DOGE", "ETH", "HYPE", "LINK", "SOL"]
T0 = pd.Timestamp("2026-05-24", tz="UTC").timestamp()
T1 = pd.Timestamp("2026-06-12", tz="UTC").timestamp()


def main():
    db = MongoClient("mongodb://localhost:27017")["quants_lab"]
    out = []
    for coin in LIQ:
        cur = db["hl_wallet_trades"].find(
            {"coin": coin, "timestamp": {"$gte": T0, "$lt": T1}},
            {"timestamp": 1, "price": 1, "_id": 0}).sort("timestamp", 1)
        rows = [(d["timestamp"], d["price"]) for d in cur]
        if not rows:
            print(f"{coin}: NO tape rows"); continue
        df = pd.DataFrame(rows, columns=["ts", "px"])
        df["minute"] = (df.ts // 60).astype(np.int64) * 60_000  # ms minute floor
        # FIRST trade in minute: aligns with assetctx mark-at-minute-start timing (last-trade
        # showed med 3.4bps dev = intraminute drift; first-trade should roughly halve it)
        m = df.groupby("minute").px.first().reset_index()
        m["coin"] = coin
        out.append(m)
        span_h = (df.ts.max() - df.ts.min()) / 3600
        cov = len(m) / max(1.0, span_h * 60) * 100
        print(f"{coin}: {len(df)} trades -> {len(m)} minute-marks | span {span_h:.0f}h | "
              f"minute coverage {cov:.0f}%")
    tape = pd.concat(out, ignore_index=True).rename(columns={"minute": "ts_ms"})
    tape.to_parquet(REPO / "app" / "data" / "v15" / "tape_marks_minutes.parquet", index=False)
    print(f"\n{len(tape)} rows -> tape_marks_minutes.parquet")

    # fidelity vs assetctx daily shards on overlap
    shard_dir = REPO / "app" / "data" / "v15" / "assetctx_marks_daily"
    shards = sorted(shard_dir.glob("202605*.parquet")) + sorted(shard_dir.glob("202606*.parquet"))
    if shards:
        ac = pd.concat([pd.read_parquet(s) for s in shards], ignore_index=True)
        ac = ac[ac.coin.isin(LIQ)]
        j = tape.merge(ac, on=["coin", "ts_ms"], suffixes=("_tape", "_ac"))
        j["dev_bps"] = (j.px - j["mark"]).abs() / j["mark"] * 1e4
        print("\nFIDELITY tape-vs-assetctx (overlap minutes), abs dev bps:")
        print(j.groupby("coin").dev_bps.quantile([0.5, 0.9, 0.99]).round(2).unstack().to_string())
        print(f"ALL: med {j.dev_bps.median():.2f} | p90 {j.dev_bps.quantile(.9):.2f} | "
              f"p99 {j.dev_bps.quantile(.99):.2f} | n={len(j)}")
    else:
        print("no overlap shards for fidelity check")


if __name__ == "__main__":
    main()
