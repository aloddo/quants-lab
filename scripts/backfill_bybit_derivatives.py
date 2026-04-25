#!/usr/bin/env python3
"""
Backfill Bybit funding rates + open interest from the historical API.

The DataRetentionTask deleted data older than 90d (now fixed to 730d).
This script recovers the deleted history by querying Bybit's public API.

Bybit API:
- Funding: GET /v5/market/funding/history?category=linear&symbol={sym}&limit=200&startTime={ms}
- OI: GET /v5/market/open-interest?category=linear&symbol={sym}&intervalTime=1h&limit=200&startTime={ms}

Rate limits: 10 req/s for public endpoints (generous). We use 200ms sleep.

Usage:
    MONGO_URI=mongodb://localhost:27017/quants_lab \
    python scripts/backfill_bybit_derivatives.py --days 730 --max-pairs 50 --type funding
    python scripts/backfill_bybit_derivatives.py --days 730 --max-pairs 50 --type oi
"""

import argparse
import asyncio
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import aiohttp
from pymongo import MongoClient, UpdateOne

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
BYBIT_BASE = "https://api.bybit.com"


async def _fetch_json(session, url, params):
    """Fetch JSON from Bybit API with retry."""
    for attempt in range(3):
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                if resp.status == 429:
                    logger.warning(f"Rate limited, sleeping 5s...")
                    await asyncio.sleep(5)
                    continue
                data = await resp.json()
                if data.get("retCode") != 0:
                    logger.warning(f"API error: {data.get('retMsg')}")
                    return None
                return data
        except Exception as e:
            logger.warning(f"Request failed (attempt {attempt+1}): {e}")
            await asyncio.sleep(2)
    return None


async def backfill_funding(session, db, pair: str, start_ms: int, end_ms: int, sleep_ms: int):
    """Backfill funding rates for one pair. Paginates BACKWARD via endTime."""
    symbol = pair.replace("-", "")
    collection = db["bybit_funding_rates"]
    total = 0
    cursor_ms = end_ms  # Start from now, page backward

    while cursor_ms > start_ms:
        data = await _fetch_json(session, f"{BYBIT_BASE}/v5/market/funding/history", {
            "category": "linear", "symbol": symbol, "limit": 200, "endTime": cursor_ms,
        })

        if not data or not data.get("result", {}).get("list"):
            break

        rows = data["result"]["list"]
        if not rows:
            break

        ops = []
        min_ts = cursor_ms
        for row in rows:
            ts_ms = int(row["fundingRateTimestamp"])
            rate = float(row["fundingRate"])
            ops.append(UpdateOne(
                {"pair": pair, "timestamp_utc": ts_ms},
                {"$set": {
                    "pair": pair, "symbol": symbol,
                    "timestamp_utc": ts_ms, "funding_rate": rate,
                    "source": "bybit", "collected_at": int(time.time() * 1000),
                }},
                upsert=True,
            ))
            min_ts = min(min_ts, ts_ms)

        if ops:
            result = collection.bulk_write(ops, ordered=False)
            total += result.upserted_count

        # Page backward: set endTime before the oldest record in this batch
        if min_ts >= cursor_ms:
            break  # No progress
        cursor_ms = min_ts - 1

        await asyncio.sleep(sleep_ms / 1000.0)

    return total


async def backfill_oi(session, db, pair: str, start_ms: int, end_ms: int, sleep_ms: int):
    """Backfill open interest for one pair. Paginates BACKWARD via endTime."""
    symbol = pair.replace("-", "")
    collection = db["bybit_open_interest"]
    total = 0
    cursor_ms = end_ms

    while cursor_ms > start_ms:
        data = await _fetch_json(session, f"{BYBIT_BASE}/v5/market/open-interest", {
            "category": "linear", "symbol": symbol,
            "intervalTime": "1h", "limit": 200, "endTime": cursor_ms,
        })

        if not data or not data.get("result", {}).get("list"):
            break

        rows = data["result"]["list"]
        if not rows:
            break

        ops = []
        min_ts = cursor_ms
        for row in rows:
            ts_ms = int(row["timestamp"])
            oi_val = float(row["openInterest"])
            ops.append(UpdateOne(
                {"pair": pair, "timestamp_utc": ts_ms},
                {"$set": {
                    "pair": pair, "symbol": symbol,
                    "timestamp_utc": ts_ms, "oi_value": oi_val,
                    "source": "bybit", "collected_at": int(time.time() * 1000),
                }},
                upsert=True,
            ))
            min_ts = min(min_ts, ts_ms)

        if ops:
            result = collection.bulk_write(ops, ordered=False)
            total += result.upserted_count

        if min_ts >= cursor_ms:
            break
        cursor_ms = min_ts - 1

        await asyncio.sleep(sleep_ms / 1000.0)

    return total


async def main():
    parser = argparse.ArgumentParser(description="Backfill Bybit derivatives history")
    parser.add_argument("--type", choices=["funding", "oi", "both"], default="both")
    parser.add_argument("--days", type=int, default=730)
    parser.add_argument("--max-pairs", type=int, default=50)
    parser.add_argument("--sleep-ms", type=int, default=200)
    parser.add_argument("--pair", default=None, help="Single pair")
    args = parser.parse_args()

    db_name = MONGO_URI.rsplit("/", 1)[-1] or "quants_lab"
    client = MongoClient(MONGO_URI)
    db = client[db_name]

    now = datetime.now(timezone.utc)
    start_ms = int((now - timedelta(days=args.days)).timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    # Discover pairs from existing funding data (or use explicit)
    if args.pair:
        pairs = [args.pair]
    else:
        pairs = sorted(db["bybit_funding_rates"].distinct("pair"))[:args.max_pairs]

    logger.info(f"Backfilling {args.type} for {len(pairs)} pairs, {args.days} days")

    async with aiohttp.ClientSession() as session:
        for i, pair in enumerate(pairs):
            if args.type in ("funding", "both"):
                n = await backfill_funding(session, db, pair, start_ms, end_ms, args.sleep_ms)
                logger.info(f"[{i+1}/{len(pairs)}] {pair} funding: {n} new docs")

            if args.type in ("oi", "both"):
                n = await backfill_oi(session, db, pair, start_ms, end_ms, args.sleep_ms)
                logger.info(f"[{i+1}/{len(pairs)}] {pair} OI: {n} new docs")

    client.close()
    logger.info("Backfill complete")


if __name__ == "__main__":
    asyncio.run(main())
