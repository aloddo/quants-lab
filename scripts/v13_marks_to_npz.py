#!/usr/bin/env python3
"""Convert marks parquet → compact per-coin numpy arrays (memmap-friendly).

Output: directory of .npz files, one per coin.
Each .npz: {timestamps: int64 array sorted, closes: float32 array}.

Subsequent shard processes mmap these via np.load(mmap_mode='r').
OS page cache shares pages across processes → no per-shard memory duplication.

Usage:
    python scripts/v13_marks_to_npz.py \
        --marks-parquet /tmp/v13_marks.parquet \
        --output-dir /tmp/v13_marks_npz/
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [v13_marks_npz] %(message)s", stream=sys.stdout)
logger = logging.getLogger("v13_marks_npz")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--marks-parquet", required=True)
    ap.add_argument("--output-dir", required=True)
    args = ap.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    logger.info(f"Loading {args.marks_parquet}...")
    t0 = time.time()
    df = pd.read_parquet(args.marks_parquet)
    logger.info(f"  {len(df):,} marks loaded in {time.time()-t0:.1f}s")

    # Sort per-coin for binary search lookups
    df = df.sort_values(["coin", "minute_ms"])
    coins = df["coin"].unique()
    logger.info(f"Writing {len(coins)} per-coin .npy files to {out}/")
    t0 = time.time()
    for i, coin in enumerate(coins, 1):
        sub = df[df["coin"] == coin]
        # Sanitize coin name for filesystem (PURR/USDC → PURR__USDC etc.)
        safe = str(coin).replace("/", "__").replace(":", "__")
        # Save as separate .npy files (avoids .npz zip overhead, allows true mmap)
        np.save(out / f"{safe}__ts.npy", sub["minute_ms"].to_numpy(dtype=np.int64))
        np.save(out / f"{safe}__close.npy", sub["close"].to_numpy(dtype=np.float32))
        if i % 100 == 0:
            logger.info(f"  [{i}/{len(coins)}] {time.time()-t0:.1f}s")
    logger.info(f"Done in {time.time()-t0:.1f}s. Files: {len(list(out.glob('*.npy')))}")


if __name__ == "__main__":
    main()
