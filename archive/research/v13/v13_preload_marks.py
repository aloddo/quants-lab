#!/usr/bin/env python3
"""V13 marks preloader — bulk-load all HL 1m candle closes to parquet.

Avoids the Mongo I/O thrash when multiple shards bulk-load in parallel.
Run ONCE; shards then mmap the parquet.

Usage:
    python scripts/v13_preload_marks.py \
        --journeys-glob 'app/data/v13/journey_chunks/chunk_*.parquet' \
        --output /tmp/v13_marks.parquet
"""
from __future__ import annotations

import argparse
import glob
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import pymongo

logging.basicConfig(level=logging.INFO, format="%(asctime)s [v13_preload] %(message)s", stream=sys.stdout)
logger = logging.getLogger("v13_preload")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--journeys-glob", default="app/data/v13/journey_chunks/chunk_*.parquet")
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    chunks = sorted(glob.glob(args.journeys_glob))
    logger.info(f"Loading {len(chunks)} chunk parquets to extract coin/time window...")
    df_all = pd.concat([pd.read_parquet(c, columns=["coin", "entry_ts", "exit_ts"]) for c in chunks],
                       ignore_index=True)
    coins = sorted(df_all["coin"].unique().tolist())
    start_ms = int(df_all["entry_ts"].min())
    end_ms = int(df_all["exit_ts"].max())
    logger.info(f"  {len(coins)} coins, window {start_ms} → {end_ms}")

    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["quants_lab"]

    rows = []
    t0 = time.time()
    for i, coin in enumerate(coins, 1):
        for doc in db.hyperliquid_candles.find(
            {"coin": coin, "interval": "1m",
             "timestamp_utc": {"$gte": start_ms, "$lte": end_ms + 60_000}},
            {"timestamp_utc": 1, "close": 1, "_id": 0},
        ):
            rows.append((coin, doc["timestamp_utc"], float(doc["close"])))
        if i % 50 == 0:
            logger.info(f"  [{i}/{len(coins)}] {len(rows):,} marks loaded in {time.time()-t0:.0f}s")

    logger.info(f"Total {len(rows):,} marks in {time.time()-t0:.0f}s, writing parquet...")
    df = pd.DataFrame(rows, columns=["coin", "minute_ms", "close"])
    # Sort + index for fast lookup
    df = df.sort_values(["coin", "minute_ms"]).reset_index(drop=True)
    df.to_parquet(args.output, index=False, compression="zstd")
    logger.info(f"Wrote {args.output} ({Path(args.output).stat().st_size / 1024 / 1024:.1f} MB)")


if __name__ == "__main__":
    main()
