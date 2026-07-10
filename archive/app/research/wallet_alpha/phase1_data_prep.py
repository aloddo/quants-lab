#!/usr/bin/env python3
"""
Phase 1: Data Preparation for Wallet Alpha Research

Reads all S3 LZ4 fill files, extracts individual fills into a flat schema,
writes Parquet partitioned by date. Also produces a universe summary CSV
for quick filtering.

Schema per fill:
  wallet, coin, timestamp_ms, side, size, price, notional, direction,
  closed_pnl, fee, is_maker, hash, block_time_ms

Usage:
    python -m app.research.wallet_alpha.phase1_data_prep
    python -m app.research.wallet_alpha.phase1_data_prep --start 20260409 --end 20260512
"""
import argparse
import json
import logging
import os
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import lz4.frame
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [phase1] %(levelname)s: %(message)s",
)
logger = logging.getLogger("phase1")

S3_RAW_DIR = Path("app/data/hl_s3_raw")
OUTPUT_DIR = Path("app/data/wallet_alpha")
FILLS_DIR = OUTPUT_DIR / "fills"
UNIVERSE_PATH = OUTPUT_DIR / "universe_summary.csv"

# Parquet schema
FILL_SCHEMA = pa.schema([
    ("wallet", pa.string()),
    ("coin", pa.string()),
    ("timestamp_ms", pa.int64()),
    ("side", pa.string()),  # "Buy" or "Sell"
    ("size", pa.float64()),
    ("price", pa.float64()),
    ("notional", pa.float64()),
    ("direction", pa.string()),  # "Open Long", "Close Short", etc.
    ("closed_pnl", pa.float64()),
    ("fee", pa.float64()),
    ("is_maker", pa.bool_()),
    ("hash", pa.string()),
    ("block_time_ms", pa.int64()),
])

# QA counters
qa_stats = {
    "total_blocks": 0,
    "total_fills": 0,
    "duplicate_hashes": 0,
    "missing_timestamps": 0,
    "negative_sizes": 0,
    "zero_prices": 0,
    "days_processed": 0,
}


def parse_side(s: str) -> str:
    """Normalize side field."""
    if s == "B":
        return "Buy"
    elif s == "A":
        return "Sell"
    return s


def parse_direction(d: str) -> str:
    """Normalize direction field."""
    # Already human-readable from HL: "Open Long", "Close Short", etc.
    return d if d else "Unknown"


def process_hour_file(filepath: Path) -> list[dict]:
    """Read one LZ4 file, extract all individual fills."""
    with open(filepath, "rb") as f:
        raw = lz4.frame.decompress(f.read())

    fills = []
    for line in raw.decode("utf-8").split("\n"):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue

        qa_stats["total_blocks"] += 1
        block_time_str = record.get("block_time", "")
        block_time_ms = record.get("time", 0)  # fallback

        # Parse block_time to ms if string
        if block_time_str and isinstance(block_time_str, str):
            try:
                dt = datetime.fromisoformat(block_time_str.replace("Z", "+00:00"))
                block_time_ms = int(dt.timestamp() * 1000)
            except (ValueError, OSError):
                pass

        events = record.get("events", [])
        for event in events:
            if not isinstance(event, list) or len(event) < 2:
                continue

            wallet = event[0].lower()
            fill = event[1]

            timestamp_ms = fill.get("time", 0)
            if not timestamp_ms:
                qa_stats["missing_timestamps"] += 1
                timestamp_ms = block_time_ms

            size = float(fill.get("sz", 0))
            price = float(fill.get("px", 0))

            if size < 0:
                qa_stats["negative_sizes"] += 1
            if price <= 0:
                qa_stats["zero_prices"] += 1
                continue  # skip zero-price fills

            notional = size * price
            fee = float(fill.get("fee", 0))
            # crossed=true means taker (aggressor), false means maker
            is_maker = not fill.get("crossed", True)

            fills.append({
                "wallet": wallet,
                "coin": fill.get("coin", ""),
                "timestamp_ms": timestamp_ms,
                "side": parse_side(fill.get("side", "")),
                "size": size,
                "price": price,
                "notional": notional,
                "direction": parse_direction(fill.get("dir", "")),
                "closed_pnl": float(fill.get("closedPnl", 0)),
                "fee": fee,
                "is_maker": is_maker,
                "hash": fill.get("hash", ""),
                "block_time_ms": block_time_ms,
            })
            qa_stats["total_fills"] += 1

    return fills


def process_day(date_dir: Path) -> pd.DataFrame:
    """Process all hourly files for one day, return DataFrame."""
    all_fills = []
    hour_files = sorted(date_dir.glob("*.lz4"))

    for hf in hour_files:
        fills = process_hour_file(hf)
        all_fills.extend(fills)

    if not all_fills:
        return pd.DataFrame()

    df = pd.DataFrame(all_fills)

    # QA: check for duplicate hashes within the day
    if "hash" in df.columns:
        # Same hash can appear twice (buyer + seller sides), so check (hash, wallet) pairs
        dup_mask = df.duplicated(subset=["hash", "wallet"], keep="first")
        n_dups = dup_mask.sum()
        if n_dups > 0:
            qa_stats["duplicate_hashes"] += n_dups
            logger.warning(f"  {n_dups} duplicate (hash,wallet) pairs in {date_dir.name}")
            df = df[~dup_mask]

    # Sort by timestamp
    df = df.sort_values("timestamp_ms").reset_index(drop=True)

    return df


def compute_universe_summary(fills_dir: Path) -> pd.DataFrame:
    """Compute per-wallet aggregate stats across all days for universe filtering.

    Uses vectorized pandas operations for speed (326M fills would be hours row-by-row).
    Processes one day at a time to limit memory, then merges daily aggregates.
    """
    logger.info("Computing universe summary...")
    daily_aggs = []

    for pq_file in sorted(fills_dir.glob("*.parquet")):
        date_str = pq_file.stem
        logger.info(f"  Aggregating {date_str}...")

        df = pd.read_parquet(
            pq_file,
            columns=["wallet", "coin", "timestamp_ms", "notional", "closed_pnl", "fee", "is_maker"],
        )

        # Per-wallet daily aggregates (vectorized)
        g = df.groupby("wallet")
        day_agg = pd.DataFrame({
            "fill_count": g["notional"].count(),
            "total_notional": g["notional"].sum(),
            "total_pnl": g["closed_pnl"].sum(),
            "total_fees": g["fee"].sum(),
            "first_ts": g["timestamp_ms"].min(),
            "last_ts": g["timestamp_ms"].max(),
            "maker_fills": g["is_maker"].sum(),
            "coins_set": g["coin"].apply(set),
        })
        day_agg["date"] = date_str
        daily_aggs.append(day_agg.reset_index())

    if not daily_aggs:
        return pd.DataFrame()

    # Merge across days
    all_days = pd.concat(daily_aggs, ignore_index=True)
    logger.info(f"  Merging {len(all_days):,} wallet-day rows...")

    g2 = all_days.groupby("wallet")
    summary = pd.DataFrame({
        "fill_count": g2["fill_count"].sum(),
        "total_notional": g2["total_notional"].sum(),
        "total_pnl": g2["total_pnl"].sum(),
        "total_fees": g2["total_fees"].sum(),
        "first_ts": g2["first_ts"].min(),
        "last_ts": g2["last_ts"].max(),
        "maker_fills": g2["maker_fills"].sum(),
        "active_days": g2["date"].nunique(),
        # Merge coin sets across days
        "coins_traded": g2["coins_set"].apply(lambda sets: len(set().union(*sets))),
    })
    summary = summary.reset_index()
    summary["maker_pct"] = summary["maker_fills"] / summary["fill_count"].clip(lower=1)
    summary["fills_per_day"] = summary["fill_count"] / summary["active_days"].clip(lower=1)
    summary = summary.drop(columns=["maker_fills"])

    if len(summary) > 0:
        summary = summary.sort_values("total_notional", ascending=False).reset_index(drop=True)

    return summary


def main():
    parser = argparse.ArgumentParser(description="Phase 1: Build unified fill dataset")
    parser.add_argument("--start", type=str, help="Start date YYYYMMDD")
    parser.add_argument("--end", type=str, help="End date YYYYMMDD")
    parser.add_argument("--skip-universe", action="store_true", help="Skip universe summary")
    args = parser.parse_args()

    FILLS_DIR.mkdir(parents=True, exist_ok=True)

    # Find all date directories
    date_dirs = sorted([d for d in S3_RAW_DIR.iterdir() if d.is_dir() and d.name.isdigit()])

    if args.start:
        date_dirs = [d for d in date_dirs if d.name >= args.start]
    if args.end:
        date_dirs = [d for d in date_dirs if d.name <= args.end]

    logger.info(f"Processing {len(date_dirs)} days from {date_dirs[0].name} to {date_dirs[-1].name}")
    t0 = time.time()

    for i, date_dir in enumerate(date_dirs):
        day_name = date_dir.name
        out_path = FILLS_DIR / f"{day_name}.parquet"

        # Skip if already processed
        if out_path.exists():
            logger.info(f"[{i+1}/{len(date_dirs)}] {day_name}: already exists, skipping")
            qa_stats["days_processed"] += 1
            continue

        logger.info(f"[{i+1}/{len(date_dirs)}] Processing {day_name}...")
        t_day = time.time()

        df = process_day(date_dir)

        if len(df) > 0:
            # Write Parquet with schema enforcement
            table = pa.Table.from_pandas(df, schema=FILL_SCHEMA, preserve_index=False)
            pq.write_table(table, out_path, compression="snappy")
            elapsed = time.time() - t_day
            logger.info(
                f"  {len(df):,} fills, {df['wallet'].nunique():,} wallets, "
                f"{df['coin'].nunique()} coins, {elapsed:.1f}s"
            )
        else:
            logger.warning(f"  No fills for {day_name}")

        qa_stats["days_processed"] += 1

    elapsed_total = time.time() - t0
    logger.info(f"\nProcessing complete in {elapsed_total:.0f}s")
    # Convert numpy int64 to Python int for JSON serialization
    qa_serializable = {k: int(v) if hasattr(v, 'item') else v for k, v in qa_stats.items()}
    logger.info(f"QA Stats: {json.dumps(qa_serializable, indent=2)}")

    # Universe summary
    if not args.skip_universe:
        summary = compute_universe_summary(FILLS_DIR)
        summary.to_csv(UNIVERSE_PATH, index=False)
        logger.info(f"\nUniverse summary: {len(summary):,} total wallets")

        # Apply filters
        filtered = summary[
            (summary["fill_count"] >= 50) &
            (summary["coins_traded"] >= 5) &
            (summary["total_notional"] >= 10_000)
        ]
        logger.info(f"After filters (50+ fills, 5+ coins, $10K+ notional): {len(filtered):,} wallets")

        # Flag potential system addresses (>10K fills/day)
        system = filtered[filtered["fills_per_day"] > 10_000]
        if len(system) > 0:
            logger.info(f"Potential system addresses (>10K fills/day): {len(system)}")
            filtered = filtered[filtered["fills_per_day"] <= 10_000]
            logger.info(f"After removing system addresses: {len(filtered):,} wallets")

        # Save filtered universe
        filtered_path = OUTPUT_DIR / "universe_filtered.csv"
        filtered.to_csv(filtered_path, index=False)
        logger.info(f"Filtered universe saved to {filtered_path}")

        # Print top 20 by volume
        logger.info("\nTop 20 wallets by volume:")
        for _, row in filtered.head(20).iterrows():
            logger.info(
                f"  {row['wallet'][:10]}... fills={row['fill_count']:,} "
                f"coins={row['coins_traded']} vol=${row['total_notional']:,.0f} "
                f"days={row['active_days']} fpd={row['fills_per_day']:.0f}"
            )


if __name__ == "__main__":
    main()
