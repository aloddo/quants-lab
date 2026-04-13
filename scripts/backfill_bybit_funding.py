#!/usr/bin/env python3
"""
Backfill full Bybit funding rate history for all pairs.

Bybit API returns max 200 records per call, newest first.
Paginates backward using endTime. History goes back to ~2021 for BTC.

Usage:
    set -a && source .env && set +a
    python scripts/backfill_bybit_funding.py
"""
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import List, Dict

import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

BYBIT_API = "https://api.bybit.com"
BATCH_SIZE = 200
RATE_LIMIT_SLEEP = 0.12  # Bybit rate limit is more strict


async def fetch_pair_history(
    session: aiohttp.ClientSession, pair: str, existing_oldest_ms: int = None
) -> List[Dict]:
    """Fetch full funding rate history for a pair via backward pagination."""
    symbol = pair.replace("-", "")
    all_docs = []

    # Start from the oldest data we already have (or from now)
    if existing_oldest_ms:
        end_time = existing_oldest_ms  # Fetch everything before what we have
    else:
        end_time = int(datetime.now(timezone.utc).timestamp() * 1000)

    while True:
        url = (
            f"{BYBIT_API}/v5/market/funding/history"
            f"?category=linear&symbol={symbol}&limit={BATCH_SIZE}&endTime={end_time}"
        )
        async with session.get(url) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning(f"{pair}: HTTP {resp.status} — {text[:200]}")
                break
            data = await resp.json()

        result = data.get("result", {}).get("list", [])
        if not result:
            break

        for entry in result:
            all_docs.append({
                "pair": pair,
                "symbol": symbol,
                "funding_rate": float(entry["fundingRate"]),
                "timestamp_utc": int(entry["fundingRateTimestamp"]),  # milliseconds
                "source": "bybit",
                "collected_at": int(datetime.now(timezone.utc).timestamp() * 1000),
            })

        # Bybit returns newest first — oldest is last in list
        oldest_ts = int(result[-1]["fundingRateTimestamp"])
        if oldest_ts >= end_time:
            break  # No progress
        end_time = oldest_ts  # Next page: everything before this

        if len(result) < BATCH_SIZE:
            break  # Last page

        await asyncio.sleep(RATE_LIMIT_SLEEP)

    return all_docs


async def main():
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client["quants_lab"]
    coll = db["bybit_funding_rates"]

    # Create unique index
    try:
        await coll.drop_index("pair_1_timestamp_utc_1")
    except Exception:
        pass
    await coll.create_index(
        [("pair", 1), ("timestamp_utc", 1)],
        unique=True,
        background=True,
    )

    # Get pairs
    pairs = sorted(await coll.distinct("pair"))
    if not pairs:
        # Fallback: get from tickers
        pairs = ["BTC-USDT", "ETH-USDT", "SOL-USDT"]
    logger.info(f"Backfilling {len(pairs)} pairs from Bybit (full history)")

    # Current state
    total_before = await coll.count_documents({})
    logger.info(f"Existing: {total_before} docs")

    total_new = 0
    errors = 0

    async with aiohttp.ClientSession(
        headers={"User-Agent": "quants-lab/1.0"}
    ) as session:
        for i, pair in enumerate(pairs):
            try:
                # Find the oldest record we already have for this pair
                oldest_existing = await coll.find_one(
                    {"pair": pair}, sort=[("timestamp_utc", 1)]
                )
                existing_oldest_ms = (
                    oldest_existing["timestamp_utc"] if oldest_existing else None
                )

                docs = await fetch_pair_history(session, pair, existing_oldest_ms)
                if not docs:
                    logger.info(f"  [{i+1}/{len(pairs)}] {pair}: no new data")
                    continue

                # Insert with ordered=False to skip duplicates
                inserted = 0
                try:
                    result = await coll.insert_many(docs, ordered=False)
                    inserted = len(result.inserted_ids)
                except Exception as bulk_err:
                    if hasattr(bulk_err, "details"):
                        inserted = bulk_err.details.get("nInserted", 0)

                total_new += inserted
                logger.info(
                    f"  [{i+1}/{len(pairs)}] {pair}: {len(docs)} fetched, "
                    f"{inserted} new"
                )

            except Exception as e:
                errors += 1
                logger.error(f"  [{i+1}/{len(pairs)}] {pair}: {e}")

    # Final stats
    total_docs = await coll.count_documents({})
    n_pairs = len(await coll.distinct("pair"))
    oldest_doc = await coll.find_one(sort=[("timestamp_utc", 1)])
    newest_doc = await coll.find_one(sort=[("timestamp_utc", -1)])

    logger.info(f"\nBackfill complete:")
    logger.info(f"  Total docs: {total_docs}, Pairs: {n_pairs}")
    logger.info(f"  New inserted: {total_new}, Errors: {errors}")
    if oldest_doc:
        oldest_dt = datetime.fromtimestamp(oldest_doc["timestamp_utc"] / 1000, tz=timezone.utc)
        logger.info(f"  Oldest: {oldest_dt}")
    if newest_doc:
        newest_dt = datetime.fromtimestamp(newest_doc["timestamp_utc"] / 1000, tz=timezone.utc)
        logger.info(f"  Newest: {newest_dt}")
    if oldest_doc and newest_doc:
        days = (newest_doc["timestamp_utc"] - oldest_doc["timestamp_utc"]) / 1000 / 86400
        logger.info(f"  Span: {days:.0f} days")


if __name__ == "__main__":
    asyncio.run(main())
