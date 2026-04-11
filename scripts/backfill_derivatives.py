#!/usr/bin/env python3
"""
Backfill historical derivatives data (funding, OI, L/S ratio) from Bybit API.

Uses the same fetch functions and MongoDB collections as the live pipeline.
Pages backwards from the oldest existing data to fill the gap.

Usage:
    python scripts/backfill_derivatives.py --days 365 --pairs ETH-USDT,BTC-USDT
    python scripts/backfill_derivatives.py --days 365 --all-pairs
"""
import asyncio
import argparse
import logging
import sys
import os
from datetime import datetime, timedelta, timezone

import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.bybit_rest import (
    fetch_funding_rates,
    fetch_open_interest,
    fetch_ls_ratio,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger(__name__)

# Rate limit: Bybit allows 120 req/min on public endpoints
RATE_LIMIT_DELAY = 0.6  # seconds between requests


async def get_oldest_timestamp(db, collection: str, pair: str) -> int:
    """Get oldest existing timestamp for a pair in a collection (ms)."""
    doc = await db[collection].find_one(
        {"pair": pair}, sort=[("timestamp_utc", 1)]
    )
    if doc:
        return doc["timestamp_utc"]
    return int(datetime.now(timezone.utc).timestamp() * 1000)


async def backfill_funding(session, db, pair: str, target_start_ms: int):
    """Backfill funding rate history for a pair."""
    collection = "bybit_funding_rates"
    oldest = await get_oldest_timestamp(db, collection, pair)

    if oldest <= target_start_ms:
        logger.info(f"  {pair} funding: already covered (oldest={datetime.fromtimestamp(oldest/1000, tz=timezone.utc).date()})")
        return 0

    total = 0
    end_ms = oldest

    while end_ms > target_start_ms:
        try:
            docs = await fetch_funding_rates(session, pair, limit=200, end_ms=end_ms)
            if not docs:
                break

            # Upsert to avoid duplicates
            for doc in docs:
                await db[collection].update_one(
                    {"pair": pair, "timestamp_utc": doc["timestamp_utc"]},
                    {"$set": doc},
                    upsert=True,
                )

            total += len(docs)
            end_ms = docs[0]["timestamp_utc"] - 1  # page backwards

            await asyncio.sleep(RATE_LIMIT_DELAY)

        except Exception as e:
            logger.warning(f"  {pair} funding error: {e}")
            await asyncio.sleep(2)
            break

    return total


async def backfill_oi(session, db, pair: str, target_start_ms: int):
    """Backfill open interest history for a pair."""
    collection = "bybit_open_interest"
    oldest = await get_oldest_timestamp(db, collection, pair)

    if oldest <= target_start_ms:
        logger.info(f"  {pair} OI: already covered")
        return 0

    total = 0
    end_ms = oldest

    while end_ms > target_start_ms:
        try:
            docs = await fetch_open_interest(session, pair, interval="1h", limit=200, end_ms=end_ms)
            if not docs:
                break

            for doc in docs:
                await db[collection].update_one(
                    {"pair": pair, "timestamp_utc": doc["timestamp_utc"]},
                    {"$set": doc},
                    upsert=True,
                )

            total += len(docs)
            end_ms = docs[0]["timestamp_utc"] - 1

            await asyncio.sleep(RATE_LIMIT_DELAY)

        except Exception as e:
            logger.warning(f"  {pair} OI error: {e}")
            await asyncio.sleep(2)
            break

    return total


async def backfill_ls(session, db, pair: str, target_start_ms: int):
    """Backfill L/S ratio history for a pair."""
    collection = "bybit_ls_ratio"
    oldest = await get_oldest_timestamp(db, collection, pair)

    if oldest <= target_start_ms:
        logger.info(f"  {pair} LS: already covered")
        return 0

    total = 0
    end_ms = oldest

    while end_ms > target_start_ms:
        try:
            # L/S ratio API doesn't support startTime/endTime in the same way
            # It returns the latest N records. We page by adjusting our cursor.
            docs = await fetch_ls_ratio(session, pair, period="1h", limit=200)
            if not docs:
                break

            for doc in docs:
                await db[collection].update_one(
                    {"pair": pair, "timestamp_utc": doc["timestamp_utc"]},
                    {"$set": doc},
                    upsert=True,
                )

            total += len(docs)
            # L/S ratio might not support pagination — break after first batch
            break

        except Exception as e:
            logger.warning(f"  {pair} LS error: {e}")
            break

    return total


async def main():
    parser = argparse.ArgumentParser(description="Backfill derivatives data from Bybit")
    parser.add_argument("--days", type=int, default=365, help="Days to backfill")
    parser.add_argument("--pairs", type=str, help="Comma-separated pairs (e.g. ETH-USDT,BTC-USDT)")
    parser.add_argument("--all-pairs", action="store_true", help="Backfill all pairs with existing candle data")
    parser.add_argument("--funding-only", action="store_true", help="Only backfill funding rates")
    args = parser.parse_args()

    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
    mongo_db = os.getenv("MONGO_DATABASE", "quants_lab")

    client = AsyncIOMotorClient(mongo_uri)
    db = client[mongo_db]

    # Indexes already created by the live pipeline (non-unique, upsert handles dedup)

    target_start = datetime.now(timezone.utc) - timedelta(days=args.days)
    target_start_ms = int(target_start.timestamp() * 1000)

    if args.pairs:
        pairs = [p.strip() for p in args.pairs.split(",")]
    elif args.all_pairs:
        from core.data_paths import data_paths
        pairs = sorted(set(
            f.stem.split("|")[1]
            for f in data_paths.candles_dir.glob("bybit_perpetual|*|1h.parquet")
        ))
    else:
        pairs = [
            "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "DOGE-USDT",
            "ADA-USDT", "AVAX-USDT", "LINK-USDT", "SUI-USDT", "BNB-USDT",
            "LTC-USDT", "BCH-USDT", "DOT-USDT", "NEAR-USDT", "APT-USDT",
            "ARB-USDT", "OP-USDT", "SEI-USDT", "WLD-USDT", "TAO-USDT",
        ]

    logger.info(f"Backfilling {len(pairs)} pairs, target: {target_start.date()} ({args.days} days)")

    stats = {"funding": 0, "oi": 0, "ls": 0}

    async with aiohttp.ClientSession() as session:
        for i, pair in enumerate(pairs):
            logger.info(f"[{i+1}/{len(pairs)}] {pair}")

            n = await backfill_funding(session, db, pair, target_start_ms)
            stats["funding"] += n
            if n: logger.info(f"  {pair} funding: +{n} docs")

            if not args.funding_only:
                n = await backfill_oi(session, db, pair, target_start_ms)
                stats["oi"] += n
                if n: logger.info(f"  {pair} OI: +{n} docs")

                n = await backfill_ls(session, db, pair, target_start_ms)
                stats["ls"] += n
                if n: logger.info(f"  {pair} LS: +{n} docs")

    logger.info(f"Done: {stats['funding']} funding, {stats['oi']} OI, {stats['ls']} LS docs backfilled")
    client.close()


if __name__ == "__main__":
    asyncio.run(main())
