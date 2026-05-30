#!/usr/bin/env python3
"""
Backfill full Fear & Greed Index history from alternative.me.

Single API call returns ALL history since 2018 (limit=0).
Safe to re-run — uses unique index to skip duplicates.

Usage:
    set -a && source .env && set +a
    python scripts/backfill_fear_greed.py
"""
import asyncio
import logging
from datetime import datetime, timezone

import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FEAR_GREED_API = "https://api.alternative.me/fng/"


async def main():
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client["quants_lab"]
    coll = db["fear_greed_index"]

    # Create unique index
    try:
        await coll.drop_index("timestamp_utc_1")
    except Exception:
        pass
    await coll.create_index(
        [("timestamp_utc", 1)],
        unique=True,
        background=True,
    )

    # Check existing coverage
    existing_count = await coll.count_documents({})
    logger.info(f"Existing docs: {existing_count}")

    # Fetch ALL history (limit=0 returns everything since 2018)
    async with aiohttp.ClientSession(
        headers={"User-Agent": "quants-lab/2.0"},
        timeout=aiohttp.ClientTimeout(total=60),
    ) as session:
        url = f"{FEAR_GREED_API}?limit=0&format=json"
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.error(f"API returned HTTP {resp.status}")
                return
            data = await resp.json()

    entries = data.get("data", [])
    logger.info(f"Fetched {len(entries)} entries from API")

    if not entries:
        logger.warning("No data returned")
        return

    collected_at = int(datetime.now(timezone.utc).timestamp() * 1000)
    docs = []
    for entry in entries:
        ts_sec = int(entry.get("timestamp", 0))
        docs.append({
            "timestamp_utc": ts_sec * 1000,  # to milliseconds
            "value": int(entry.get("value", 0)),
            "classification": entry.get("value_classification", ""),
            "source": "alternative.me",
            "collected_at": collected_at,
        })

    # Insert with ordered=False to skip duplicates
    try:
        result = await coll.insert_many(docs, ordered=False)
        inserted = len(result.inserted_ids)
    except Exception as e:
        inserted = getattr(getattr(e, "details", {}), "get", lambda k, d: 0)("nInserted", 0)
        if hasattr(e, "details"):
            inserted = e.details.get("nInserted", 0)

    total = await coll.count_documents({})
    oldest = await coll.find_one(sort=[("timestamp_utc", 1)])
    newest = await coll.find_one(sort=[("timestamp_utc", -1)])

    logger.info(f"\nBackfill complete:")
    logger.info(f"  Total docs: {total}")
    logger.info(f"  New inserted: {inserted}")
    if oldest:
        oldest_dt = datetime.fromtimestamp(oldest["timestamp_utc"] / 1000, tz=timezone.utc)
        logger.info(f"  Oldest: {oldest_dt}")
    if newest:
        newest_dt = datetime.fromtimestamp(newest["timestamp_utc"] / 1000, tz=timezone.utc)
        logger.info(f"  Newest: {newest_dt}")
    if oldest and newest:
        days = (newest["timestamp_utc"] - oldest["timestamp_utc"]) / 1000 / 86400
        logger.info(f"  Span: {days:.0f} days")


if __name__ == "__main__":
    asyncio.run(main())
