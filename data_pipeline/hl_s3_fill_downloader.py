#!/usr/bin/env python3
"""
Download and filter Hyperliquid fills from public S3 bucket.

HL publishes all exchange fills to s3://hl-mainnet-node-data (requester-pays).
Files: node_fills_by_block/hourly/{YYYYMMDD}/{HH}.lz4

Usage:
    # Download last 30 days for specific wallets
    python scripts/hl_s3_fill_downloader.py --wallets 0xabc,0xdef --days 30

    # Download specific date range
    python scripts/hl_s3_fill_downloader.py --wallets 0xabc --start 2026-01-01 --end 2026-05-09

    # Store to MongoDB
    python scripts/hl_s3_fill_downloader.py --wallets 0xabc --days 90 --mongo

Requires: AWS credentials (requester-pays bucket), boto3, lz4
"""
import argparse
import json
import logging
import os
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import boto3
import lz4.frame
from botocore.config import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("hl_s3")

BUCKET = "hl-mainnet-node-data"
REGION = "ap-northeast-1"

# Schema changed over time:
# 2025-03-22 to 2025-05-24: node_trades/hourly/
# 2025-05-25 to 2025-07-26: node_fills/hourly/
# 2025-07-28 onward: node_fills_by_block/hourly/
SCHEMA_TRANSITIONS = [
    (datetime(2025, 7, 28, tzinfo=timezone.utc), "node_fills_by_block/hourly"),
    (datetime(2025, 5, 25, tzinfo=timezone.utc), "node_fills/hourly"),
    (datetime(2025, 3, 22, tzinfo=timezone.utc), "node_trades/hourly"),
]


def get_prefix_for_date(dt: datetime) -> str:
    """Get the S3 prefix for a given date based on schema transitions."""
    for transition_date, prefix in SCHEMA_TRANSITIONS:
        if dt >= transition_date:
            return prefix
    return SCHEMA_TRANSITIONS[-1][1]  # oldest schema


def download_and_filter(s3_client, date: datetime, hour: int, target_wallets: set) -> list:
    """Download one hourly file, decompress, filter by wallets."""
    prefix = get_prefix_for_date(date)
    key = f"{prefix}/{date.strftime('%Y%m%d')}/{hour}.lz4"

    try:
        with tempfile.NamedTemporaryFile(suffix=".lz4", delete=True) as tmp:
            s3_client.download_file(
                BUCKET, key, tmp.name,
                ExtraArgs={"RequestPayer": "requester"}
            )

            with open(tmp.name, "rb") as f:
                raw = lz4.frame.decompress(f.read())

            fills = []
            for line in raw.decode("utf-8").split("\n"):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                    # Handle two formats:
                    # 1. Block-based: {"local_time":..., "block_time":..., "events": [["addr", {fill}], ...]}
                    # 2. Flat: {"addr":..., "coin":..., "sz":..., ...}
                    if "events" in record:
                        block_time = record.get("block_time", "")
                        for event in record.get("events", []):
                            if not isinstance(event, list) or len(event) < 2:
                                continue
                            addr = event[0].lower()
                            fill = event[1] if isinstance(event[1], dict) else {}
                            if target_wallets is not None and addr not in target_wallets:
                                continue
                            sz = float(fill.get("sz", 0))
                            px = float(fill.get("px", 0))
                            fills.append({
                                "wallet": addr,
                                "coin": fill.get("coin", ""),
                                "side": fill.get("side", ""),
                                "size": sz,
                                "price": px,
                                "time": int(fill.get("time", 0)),
                                "dir": fill.get("dir", ""),
                                "closedPnl": float(fill.get("closedPnl", 0)),
                                "hash": fill.get("hash", ""),
                                "source": "s3",
                                "notional": sz * px,
                            })
                    else:
                        # Flat format
                        addr = record.get("addr", record.get("user", "")).lower()
                        if target_wallets is not None and addr not in target_wallets:
                            continue
                        sz = float(record.get("sz", 0))
                        px = float(record.get("px", 0))
                        fills.append({
                            "wallet": addr,
                            "coin": record.get("coin", ""),
                            "side": record.get("side", ""),
                            "size": sz,
                            "price": px,
                            "time": int(record.get("time", 0)),
                            "dir": record.get("dir", ""),
                            "closedPnl": float(record.get("closedPnl", 0)),
                            "hash": record.get("hash", ""),
                            "source": "s3",
                            "notional": sz * px,
                        })
                except (json.JSONDecodeError, ValueError, TypeError):
                    continue

            return fills

    except s3_client.exceptions.NoSuchKey:
        return []
    except Exception as e:
        logger.warning(f"Error downloading {key}: {e}")
        return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--wallets", required=False, help="Comma-separated wallet addresses (omit for ALL wallets)")
    parser.add_argument("--all", action="store_true", help="Download ALL wallets (no filter)")
    parser.add_argument("--days", type=int, default=30, help="Days of history to download")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (overrides --days)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (default: today)")
    parser.add_argument("--mongo", action="store_true", help="Store results in MongoDB")
    parser.add_argument("--output", help="Output JSON file path")
    args = parser.parse_args()

    if args.all or not args.wallets:
        target_wallets = None  # None = no filter, keep all
        logger.info("ALL WALLETS mode (no filter)")
    else:
        target_wallets = {w.lower().strip() for w in args.wallets.split(",")}
    logger.info(f"Target wallets: {'ALL' if target_wallets is None else len(target_wallets)}")

    if args.end:
        end_date = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        end_date = datetime.now(timezone.utc)

    if args.start:
        start_date = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start_date = end_date - timedelta(days=args.days)

    logger.info(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    s3 = boto3.client("s3", region_name=REGION)

    # Parquet output directory
    parquet_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "app", "data", "hl_s3_fills")
    os.makedirs(parquet_dir, exist_ok=True)

    current = start_date
    total_hours = int((end_date - start_date).total_seconds() / 3600)
    total_days = int((end_date - start_date).days) + 1
    processed_hours = 0
    total_fills = 0
    days_done = 0

    while current < end_date:
        day_fills = []
        date_str = current.strftime("%Y%m%d")
        parquet_path = os.path.join(parquet_dir, f"{date_str}.parquet")

        # Skip if already downloaded
        if os.path.exists(parquet_path):
            logger.info(f"  {date_str} already exists, skipping")
            current += timedelta(days=1)
            days_done += 1
            processed_hours += 24
            continue

        for hour in range(24):
            hour_dt = current.replace(hour=hour, minute=0, second=0)
            if hour_dt >= end_date:
                break

            fills = download_and_filter(s3, hour_dt, hour, target_wallets)
            day_fills.extend(fills)
            processed_hours += 1

        if day_fills:
            import pandas as pd
            df = pd.DataFrame(day_fills)
            df.to_parquet(parquet_path, index=False, compression="snappy")
            total_fills += len(day_fills)
            days_done += 1
            logger.info(f"  {date_str}: {len(day_fills):,} fills -> {parquet_path} ({days_done}/{total_days} days, {total_fills:,} total)")
        else:
            days_done += 1
            logger.info(f"  {date_str}: 0 fills (skipped)")

        current += timedelta(days=1)

    logger.info(f"DONE: {total_fills:,} fills across {days_done} days -> {parquet_dir}")

    # Summary from parquet files
    import pandas as pd
    all_files = sorted(os.path.join(parquet_dir, f) for f in os.listdir(parquet_dir) if f.endswith(".parquet"))
    if all_files:
        sample = pd.read_parquet(all_files[-1])
        logger.info(f"Latest file columns: {list(sample.columns)}")
        logger.info(f"Latest file rows: {len(sample):,}")
        logger.info(f"Total parquet files: {len(all_files)}")
        total_size = sum(os.path.getsize(f) for f in all_files)
        logger.info(f"Total storage: {total_size/1e9:.2f} GB")

    # Store to MongoDB (only if explicitly requested AND fills are small enough)
    all_fills = []  # empty for backward compat
    if args.mongo:
        from pymongo import MongoClient
        db = MongoClient("mongodb://localhost:27017")["quants_lab"]
        col = db["hl_s3_fills"]
        col.create_index([("wallet", 1), ("time", 1)])
        col.create_index([("coin", 1)])
        col.create_index([("hash", 1)], sparse=True)

        # Upsert to avoid duplicates (use hash if available, else wallet+time)
        inserted = 0
        for fill in all_fills:
            if fill.get("hash") and fill["hash"] != "0x" + "0" * 64:
                key = {"hash": fill["hash"]}
            else:
                key = {"wallet": fill["wallet"], "time": fill["time"], "coin": fill["coin"], "side": fill["side"]}
            result = col.update_one(
                key,
                {"$set": fill},
                upsert=True,
            )
            if result.upserted_id:
                inserted += 1
        logger.info(f"MongoDB: {inserted} new fills inserted into hl_s3_fills")

    # Write to JSON
    if args.output:
        with open(args.output, "w") as f:
            json.dump(all_fills, f)
        logger.info(f"Written to {args.output}")


if __name__ == "__main__":
    main()
