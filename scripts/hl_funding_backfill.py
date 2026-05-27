"""
HL Funding Rate History Backfill

Paginates through fundingHistory API for all coins, going back to their
listing date. Deduplicates against existing data in hyperliquid_funding_rates.

Usage:
    set -a && source .env && set +a
    /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/hl_funding_backfill.py

    # Specific coins only:
    ... scripts/hl_funding_backfill.py --coins BTC ETH FARTCOIN
"""

import argparse
import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone

from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("hl_funding_backfill")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
MONGO_DB = os.getenv("MONGO_DATABASE", "quants_lab")
HL_API_URL = "https://api.hyperliquid.xyz/info"
PAGE_SIZE = 500  # HL returns max 500 per request
RATE_LIMIT_DELAY = 0.25  # seconds between requests


def hl_post(payload: dict, timeout: int = 15) -> list:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(HL_API_URL, data=data, headers={"Content-Type": "application/json"})
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())


def get_all_coins() -> list[str]:
    """Get all tradeable coins from HL."""
    data = hl_post({"type": "metaAndAssetCtxs"})
    meta = data[0]
    return [u["name"] for u in meta["universe"]]


def backfill_coin(db, coin: str) -> int:
    """Backfill all funding history for a coin. Returns total new docs inserted."""
    coll = db["hyperliquid_funding_rates"]

    # Find earliest existing record for this coin
    existing = coll.find_one(
        {"coin": coin},
        sort=[("timestamp_utc", ASCENDING)]
    )

    if existing:
        # We want to go BEFORE the earliest existing record
        end_time = existing["timestamp_utc"]
        if isinstance(end_time, datetime):
            end_time = int(end_time.timestamp() * 1000)
        logger.info(f"  {coin}: existing data starts at {datetime.utcfromtimestamp(end_time/1000)}")
    else:
        end_time = None
        logger.info(f"  {coin}: no existing data, full backfill")

    # Start from HL launch (May 2023 for most coins)
    start_time = int(datetime(2022, 11, 1, tzinfo=timezone.utc).timestamp() * 1000)

    total_inserted = 0
    current_start = start_time
    pages = 0

    while True:
        try:
            records = hl_post({
                "type": "fundingHistory",
                "coin": coin,
                "startTime": current_start,
            })
        except Exception as e:
            logger.warning(f"  {coin}: API error at page {pages}: {e}")
            break

        if not records:
            break

        pages += 1

        # Check if we've gone past our existing data
        last_time = records[-1]["time"]

        # Convert to docs
        docs = []
        for r in records:
            ts = r["time"]
            # Skip if we already have this data
            if end_time and ts >= end_time:
                continue

            docs.append({
                "timestamp_utc": ts,
                "pair": f"{coin}-USD",
                "coin": coin,
                "funding_rate": float(r["fundingRate"]),
                "premium": float(r.get("premium", 0)),
                "recorded_at": datetime.now(timezone.utc),
            })

        if docs:
            try:
                result = coll.insert_many(docs, ordered=False)
                total_inserted += len(result.inserted_ids)
            except BulkWriteError as e:
                total_inserted += e.details.get("nInserted", 0)

        # If we got fewer than PAGE_SIZE, we've reached the end of available history
        if len(records) < PAGE_SIZE:
            break

        # If all records are past our existing data, stop
        if end_time and records[0]["time"] >= end_time:
            break

        # Next page starts after last record
        current_start = last_time + 1
        time.sleep(RATE_LIMIT_DELAY)

    return total_inserted


def also_backfill_forward(db, coin: str) -> int:
    """Also fill any gaps AFTER the latest existing record up to now."""
    coll = db["hyperliquid_funding_rates"]

    latest = coll.find_one(
        {"coin": coin},
        sort=[("timestamp_utc", -1)]
    )

    if not latest:
        return 0

    latest_ts = latest["timestamp_utc"]
    if isinstance(latest_ts, datetime):
        latest_ts = int(latest_ts.timestamp() * 1000)

    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    gap_hours = (now_ts - latest_ts) / 3600000

    if gap_hours < 2:
        return 0  # No meaningful gap

    total_inserted = 0
    current_start = latest_ts + 1

    while current_start < now_ts:
        try:
            records = hl_post({
                "type": "fundingHistory",
                "coin": coin,
                "startTime": current_start,
            })
        except Exception as e:
            logger.warning(f"  {coin}: forward fill API error: {e}")
            break

        if not records:
            break

        docs = [{
            "timestamp_utc": r["time"],
            "pair": f"{coin}-USD",
            "coin": coin,
            "funding_rate": float(r["fundingRate"]),
            "premium": float(r.get("premium", 0)),
            "recorded_at": datetime.now(timezone.utc),
        } for r in records]

        try:
            result = coll.insert_many(docs, ordered=False)
            total_inserted += len(result.inserted_ids)
        except BulkWriteError as e:
            total_inserted += e.details.get("nInserted", 0)

        if len(records) < PAGE_SIZE:
            break

        current_start = records[-1]["time"] + 1
        time.sleep(RATE_LIMIT_DELAY)

    return total_inserted


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--coins", nargs="+", help="Specific coins to backfill")
    args = parser.parse_args()

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    # Ensure index for dedup
    db["hyperliquid_funding_rates"].create_index(
        [("coin", ASCENDING), ("timestamp_utc", ASCENDING)],
        unique=False  # Not unique since we handle dedup via BulkWriteError
    )

    if args.coins:
        coins = args.coins
    else:
        coins = get_all_coins()

    logger.info(f"Backfilling funding history for {len(coins)} coins")

    total_new = 0
    for i, coin in enumerate(coins):
        logger.info(f"[{i+1}/{len(coins)}] Backfilling {coin}...")

        # Backward fill (before existing data)
        n_back = backfill_coin(db, coin)

        # Forward fill (after existing data, up to now)
        n_fwd = also_backfill_forward(db, coin)

        n = n_back + n_fwd
        if n > 0:
            logger.info(f"  {coin}: +{n} new records (back={n_back}, fwd={n_fwd})")
        total_new += n

        time.sleep(RATE_LIMIT_DELAY)

    # Final stats
    total = db["hyperliquid_funding_rates"].count_documents({})
    unique_coins = len(db["hyperliquid_funding_rates"].distinct("coin"))
    logger.info(f"\nBackfill complete: +{total_new} new records")
    logger.info(f"Total: {total:,} docs across {unique_coins} coins")

    client.close()


if __name__ == "__main__":
    main()
