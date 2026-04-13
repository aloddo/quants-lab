#!/usr/bin/env python3
"""
Backfill Coinglass liquidation + OI history for all trading pairs.

Coinglass API supports pagination via start_time/end_time.
History goes back 2-3 years for major pairs.

Usage:
    set -a && source .env && set +a
    python scripts/backfill_coinglass.py
"""
import asyncio
import logging
import os
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict

import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

COINGLASS_API_BASE = "https://open-api-v4.coinglass.com"
RATE_LIMIT_SLEEP = 0.5  # Coinglass is stricter than Binance

# All pairs in our trading universe
PAIRS = [
    "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT", "DOGE-USDT",
    "ADA-USDT", "AVAX-USDT", "LINK-USDT", "DOT-USDT", "UNI-USDT",
    "NEAR-USDT", "APT-USDT", "ARB-USDT", "OP-USDT", "SUI-USDT",
    "SEI-USDT", "WLD-USDT", "LTC-USDT", "BCH-USDT", "BNB-USDT",
]


def _pair_to_symbol(pair: str) -> str:
    return pair.split("-")[0]


async def fetch_liquidation_history(
    session: aiohttp.ClientSession, pair: str, start_ms: int, end_ms: int
) -> List[Dict]:
    """Fetch liquidation history for a pair in a time range."""
    symbol = _pair_to_symbol(pair)
    url = f"{COINGLASS_API_BASE}/api/futures/liquidation/v2/history"
    params = {
        "symbol": symbol,
        "interval": "1h",
        "limit": 500,
        "startTime": start_ms,
        "endTime": end_ms,
    }

    async with session.get(url, params=params) as resp:
        if resp.status != 200:
            return []
        data = await resp.json()

    if data.get("code") != "0":
        return []

    collected_at = int(datetime.now(timezone.utc).timestamp() * 1000)
    docs = []
    for entry in data.get("data", []):
        long_liq = float(entry.get("longLiquidationUsd", 0))
        short_liq = float(entry.get("shortLiquidationUsd", 0))
        total_liq = long_liq + short_liq
        docs.append({
            "pair": pair,
            "timestamp_utc": int(entry.get("createTime", 0)),
            "interval": "1h",
            "long_liquidation_usd": long_liq,
            "short_liquidation_usd": short_liq,
            "total_liquidation_usd": total_liq,
            "liquidation_imbalance": (long_liq - short_liq) / total_liq if total_liq > 0 else 0.0,
            "source": "coinglass",
            "collected_at": collected_at,
        })
    return docs


async def fetch_oi_history(
    session: aiohttp.ClientSession, pair: str, start_ms: int, end_ms: int
) -> List[Dict]:
    """Fetch OI OHLC history for a pair in a time range."""
    symbol = _pair_to_symbol(pair)
    url = f"{COINGLASS_API_BASE}/api/futures/openInterest/ohlc-history"
    params = {
        "symbol": symbol,
        "interval": "1h",
        "limit": 500,
        "startTime": start_ms,
        "endTime": end_ms,
    }

    async with session.get(url, params=params) as resp:
        if resp.status != 200:
            return []
        data = await resp.json()

    if data.get("code") != "0":
        return []

    collected_at = int(datetime.now(timezone.utc).timestamp() * 1000)
    docs = []
    for entry in data.get("data", []):
        docs.append({
            "pair": pair,
            "timestamp_utc": int(entry.get("t", 0)),
            "interval": "1h",
            "oi_open": float(entry.get("o", 0)),
            "oi_high": float(entry.get("h", 0)),
            "oi_low": float(entry.get("l", 0)),
            "oi_close": float(entry.get("c", 0)),
            "source": "coinglass",
            "collected_at": collected_at,
        })
    return docs


async def main():
    api_key = os.environ.get("COINGLASS_API_KEY", "")
    if not api_key:
        logger.error("COINGLASS_API_KEY not set. Export it or source .env first.")
        return

    client = AsyncIOMotorClient("mongodb://localhost:27017")
    db = client["quants_lab"]
    liq_coll = db["coinglass_liquidations"]
    oi_coll = db["coinglass_oi"]

    # Create unique indexes
    for coll in [liq_coll, oi_coll]:
        try:
            await coll.drop_index("pair_1_timestamp_utc_1")
        except Exception:
            pass
        await coll.create_index(
            [("pair", 1), ("timestamp_utc", 1)],
            unique=True,
            background=True,
        )

    headers = {
        "User-Agent": "quants-lab/2.0",
        "coinglassSecret": api_key,
    }

    # Paginate in 30-day chunks going back 2 years
    now = datetime.now(timezone.utc)
    chunk_days = 30
    lookback_days = 730  # 2 years

    total_liq = 0
    total_oi = 0
    errors = 0

    async with aiohttp.ClientSession(
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=120),
    ) as session:
        for pair in PAIRS:
            pair_liq = 0
            pair_oi = 0

            for days_ago_start in range(lookback_days, 0, -chunk_days):
                days_ago_end = max(days_ago_start - chunk_days, 0)
                start_dt = now - timedelta(days=days_ago_start)
                end_dt = now - timedelta(days=days_ago_end)
                start_ms = int(start_dt.timestamp() * 1000)
                end_ms = int(end_dt.timestamp() * 1000)

                try:
                    liq_docs = await fetch_liquidation_history(session, pair, start_ms, end_ms)
                    if liq_docs:
                        try:
                            result = await liq_coll.insert_many(liq_docs, ordered=False)
                            pair_liq += len(result.inserted_ids)
                        except Exception:
                            pass  # Duplicates

                    await asyncio.sleep(RATE_LIMIT_SLEEP)

                    oi_docs = await fetch_oi_history(session, pair, start_ms, end_ms)
                    if oi_docs:
                        try:
                            result = await oi_coll.insert_many(oi_docs, ordered=False)
                            pair_oi += len(result.inserted_ids)
                        except Exception:
                            pass

                    await asyncio.sleep(RATE_LIMIT_SLEEP)

                except Exception as e:
                    errors += 1
                    logger.error(f"{pair} chunk {days_ago_start}d: {e}")

            total_liq += pair_liq
            total_oi += pair_oi
            logger.info(f"{pair}: {pair_liq} liq docs, {pair_oi} OI docs")

    logger.info(f"\nBackfill complete:")
    logger.info(f"  Liquidations: {total_liq} new docs")
    logger.info(f"  OI: {total_oi} new docs")
    logger.info(f"  Errors: {errors}")


if __name__ == "__main__":
    asyncio.run(main())
