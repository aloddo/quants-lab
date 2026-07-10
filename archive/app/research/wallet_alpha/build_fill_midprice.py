#!/usr/bin/env python3
"""
Build approximate mid-price series from fill data for coins/dates without L2 snapshots.

For each coin, constructs a 1-second mid-price estimate using the median fill price
within each second. This is lower quality than L2 book mid-price but provides coverage
for the full 34-day dataset (April 9 to May 12).

Output is clearly flagged as "fill-derived" so Phase 3 can handle it separately
in significance tests.

Usage:
    python -m app.research.wallet_alpha.build_fill_midprice
"""
import logging
import time
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [fill_mid] %(levelname)s: %(message)s",
)
logger = logging.getLogger("fill_mid")

FILLS_DIR = Path("app/data/wallet_alpha/fills")
OUTPUT_DIR = Path("app/data/wallet_alpha")
FILL_MID_DIR = OUTPUT_DIR / "fill_midprices"


def build_midprice_for_day(parquet_path: Path) -> dict[str, pd.DataFrame]:
    """Build per-coin mid-price series from fill data for one day.

    Uses median fill price per second as the mid estimate.
    Returns dict: coin -> DataFrame[timestamp_ms, mid_price, n_fills].
    """
    df = pd.read_parquet(parquet_path, columns=["coin", "timestamp_ms", "price"])

    # Round timestamp to nearest second
    df["ts_sec"] = (df["timestamp_ms"] // 1000) * 1000

    result = {}
    for coin, cdf in df.groupby("coin"):
        mid = cdf.groupby("ts_sec").agg(
            mid_price=("price", "median"),
            n_fills=("price", "count"),
        ).reset_index()
        mid = mid.rename(columns={"ts_sec": "timestamp_ms"})
        mid["source"] = "fill_derived"
        result[coin] = mid

    return result


def main():
    t0 = time.time()
    FILL_MID_DIR.mkdir(parents=True, exist_ok=True)

    fill_files = sorted(FILLS_DIR.glob("*.parquet"))
    if not fill_files:
        logger.error("No fill files found. Run phase1 first.")
        return

    logger.info(f"Building fill-derived mid-prices for {len(fill_files)} days...")

    all_coins = set()
    for i, ff in enumerate(fill_files):
        date_str = ff.stem
        out_path = FILL_MID_DIR / f"{date_str}.parquet"

        if out_path.exists():
            logger.info(f"[{i+1}/{len(fill_files)}] {date_str}: exists, skipping")
            continue

        logger.info(f"[{i+1}/{len(fill_files)}] {date_str}...")
        t_day = time.time()

        coin_mids = build_midprice_for_day(ff)

        # Combine all coins into one parquet per day
        frames = []
        for coin, mid_df in coin_mids.items():
            mid_df = mid_df.copy()
            mid_df["coin"] = coin
            frames.append(mid_df)
            all_coins.add(coin)

        if frames:
            combined = pd.concat(frames, ignore_index=True)
            combined.to_parquet(out_path, index=False)
            logger.info(f"  {len(coin_mids)} coins, {len(combined):,} second-bars, {time.time()-t_day:.1f}s")

    logger.info(f"\nDone in {time.time()-t0:.0f}s. {len(all_coins)} total coins.")


if __name__ == "__main__":
    main()
