#!/usr/bin/env python3
"""
Backfill full Binance funding rate history for all pairs.

Binance API returns max 200 records per call. Paginates using startTime.
History goes back to 2020-01-01 for BTC/ETH, later for other pairs.

Usage:
    set -a && source .env && set +a
    python scripts/backfill_binance_funding.py
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

BINANCE_FAPI = "https://fapi.binance.com"
BATCH_SIZE = 200  # Binance max per call
START_TIME_MS = 1577836800000  # 2020-01-01 00:00:00 UTC
RATE_LIMIT_SLEEP = 0.1  # seconds between API calls


async def fetch_pair_history(
    session: aiohttp.ClientSession, pair: str
) -> List[Dict]:
    """Fetch full funding rate history for a pair via pagination."""
    symbol = pair.replace("-", "")
    all_docs = []
    start_time = START_TIME_MS

    while True:
        url = (
            f"{BINANCE_FAPI}/fapi/v1/fundingRate"
            f"?symbol={symbol}&limit={BATCH_SIZE}&startTime={start_time}"
        )
        async with session.get(url) as resp:
            if resp.status == 400:
                # Symbol doesn't exist on Binance Futures
                break
            if resp.status != 200:
                logger.warning(f"{pair}: HTTP {resp.status}")
                break
            data = await resp.json()

        if not data:
            break

        for entry in data:
            try:
                mark_price = float(entry.get("markPrice", 0) or 0)
            except (ValueError, TypeError):
                mark_price = 0.0
            all_docs.append({
                "pair": pair,
                "symbol": symbol,
                "funding_rate": float(entry["fundingRate"]),
                "mark_price": mark_price,
                "timestamp_utc": int(entry["fundingTime"]),
                "source": "binance",
                "collected_at": int(datetime.now(timezone.utc).timestamp() * 1000),
            })

        # Move start_time past the last record
        last_ts = int(data[-1]["fundingTime"])
        if last_ts <= start_time:
            break  # No progress, stop
        start_time = last_ts + 1

        if len(data) < BATCH_SIZE:
            break  # Last page

        await asyncio.sleep(RATE_LIMIT_SLEEP)

    return all_docs


async def main():
    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client["quants_lab"]
    coll = db["binance_funding_rates"]

    # Create unique index to prevent duplicates on re-run
    # Drop non-unique index if it exists, then create unique version
    try:
        await coll.drop_index("pair_1_timestamp_utc_1")
    except Exception:
        pass
    await coll.create_index(
        [("pair", 1), ("timestamp_utc", 1)],
        unique=True,
        background=True,
    )

    # Get pairs from Bybit collection (our trading universe)
    pairs = sorted(await db["bybit_funding_rates"].distinct("pair"))
    logger.info(f"Backfilling {len(pairs)} pairs from Binance (full history)")

    # Check existing coverage
    existing = await coll.find_one(sort=[("timestamp_utc", 1)])
    if existing:
        oldest = datetime.fromtimestamp(existing["timestamp_utc"] / 1000, tz=timezone.utc)
        total = await coll.count_documents({})
        logger.info(f"Existing: {total} docs, oldest={oldest}")

    total_new = 0
    total_skipped = 0
    errors = 0

    async with aiohttp.ClientSession(
        headers={"User-Agent": "quants-lab/1.0"}
    ) as session:
        for i, pair in enumerate(pairs):
            try:
                docs = await fetch_pair_history(session, pair)
                if not docs:
                    continue

                # Use ordered=False to skip duplicates silently
                try:
                    result = await coll.insert_many(docs, ordered=False)
                    inserted = len(result.inserted_ids)
                except Exception as bulk_err:
                    # BulkWriteError — some were duplicates
                    inserted = getattr(
                        getattr(bulk_err, "details", {}),
                        "get", lambda k, d: 0
                    )("nInserted", 0)
                    if hasattr(bulk_err, "details"):
                        inserted = bulk_err.details.get("nInserted", 0)

                skipped = len(docs) - inserted
                total_new += inserted
                total_skipped += skipped

                logger.info(
                    f"  [{i+1}/{len(pairs)}] {pair}: {len(docs)} fetched, "
                    f"{inserted} new, {skipped} dups"
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
    logger.info(f"  New inserted: {total_new}, Duplicates skipped: {total_skipped}, Errors: {errors}")
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
