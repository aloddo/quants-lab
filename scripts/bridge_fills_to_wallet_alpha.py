#!/usr/bin/env python3
"""Bridge existing hl_s3_fills_v2 daily parquets -> wallet_alpha/fills/ schema so the markout pipeline
(phase2->3->6) can run on current data (the pipeline's phase1 wanted the retired hl_s3_raw .lz4 layout).

Maps: time->timestamp_ms, side {B->Buy, A->Sell}, closedPnl->closed_pnl, is_maker<-~crossed.
Keeps a recent window (default last 90 days) to keep the markout run tractable.
Streaming per-file (memory-safe); writes one parquet per date.
"""
import glob, os, sys
from pathlib import Path
import pandas as pd

SRC = Path("app/data/hl_s3_fills_v2")
DST = Path("app/data/wallet_alpha/fills")
DST.mkdir(parents=True, exist_ok=True)
LOOKBACK_DAYS = int(sys.argv[1]) if len(sys.argv) > 1 else 90

files = sorted(glob.glob(str(SRC / "*.parquet")))
files = [f for f in files if Path(f).stem.isdigit()]
files = files[-LOOKBACK_DAYS:]
print(f"bridging {len(files)} daily fill files (last {LOOKBACK_DAYS}d) -> {DST}")
n_total = 0
for f in files:
    date = Path(f).stem
    out = DST / f"{date}.parquet"
    if out.exists():
        continue
    df = pd.read_parquet(f, columns=["wallet", "coin", "side", "size", "price", "time",
                                     "closedPnl", "fee", "crossed", "hash", "notional"])
    o = pd.DataFrame({
        "wallet": df["wallet"].astype(str),
        "coin": df["coin"].astype(str),
        "timestamp_ms": df["time"].astype("int64"),
        "side": df["side"].map({"B": "Buy", "A": "Sell"}).fillna("Sell"),
        "size": pd.to_numeric(df["size"], errors="coerce").fillna(0.0),
        "price": pd.to_numeric(df["price"], errors="coerce").fillna(0.0),
        "notional": pd.to_numeric(df["notional"], errors="coerce").fillna(0.0),
        "closed_pnl": pd.to_numeric(df["closedPnl"], errors="coerce").fillna(0.0),
        "fee": pd.to_numeric(df["fee"], errors="coerce").fillna(0.0),
        "is_maker": ~df["crossed"].astype(bool),
        "hash": df["hash"].astype(str),
    })
    o.to_parquet(out, index=False, compression="snappy")
    n_total += len(o)
    print(f"  {date}: {len(o):,} fills")
print(f"DONE: {n_total:,} fills across {len(files)} days -> {DST}")
