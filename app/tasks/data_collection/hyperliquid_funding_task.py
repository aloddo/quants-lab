"""
Hyperliquid perpetual funding rate collection task.

Fetches from Hyperliquid public API (no auth) and writes to MongoDB.
Pair mapping: intersects HL coins with Bybit pairs for consistency.
Schedule: every 15 minutes via YAML DAG.
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set

import aiohttp
from pymongo import UpdateOne

from core.tasks import BaseTask, TaskContext

logger = logging.getLogger(__name__)

from app.tasks.notifying_task import NotifyingTaskMixin

HL_API_URL = "https://api.hyperliquid.xyz/info"

# Hyperliquid coin name -> Bybit base asset prefix
# HL uses kPEPE for 1000PEPE, kBONK for 1000BONK, etc.
_HL_TO_BYBIT_PREFIX = {
    "kPEPE": "1000PEPE",
    "kBONK": "1000BONK",
    "kFLOKI": "1000FLOKI",
    "kSHIB": "1000SHIB",
    "kLUNC": "1000LUNC",
    "kXEC": "1000XEC",
    "kSATS": "1000SATS",
}

_BYBIT_PREFIX_TO_HL = {v: k for k, v in _HL_TO_BYBIT_PREFIX.items()}


def _hl_coin_to_pair(coin: str) -> str:
    """Convert HL coin name to Bybit-style pair: 'BTC' -> 'BTC-USDT', 'kPEPE' -> '1000PEPE-USDT'."""
    base = _HL_TO_BYBIT_PREFIX.get(coin, coin)
    return f"{base}-USDT"


def _pair_to_hl_coin(pair: str) -> str:
    """Convert Bybit-style pair to HL coin: 'BTC-USDT' -> 'BTC', '1000PEPE-USDT' -> 'kPEPE'."""
    base = pair.replace("-USDT", "")
    return _BYBIT_PREFIX_TO_HL.get(base, base)


class HyperliquidFundingTask(NotifyingTaskMixin, BaseTask):
    """Fetch funding rates from Hyperliquid API -> MongoDB."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.collection_name = task_config.get("collection_name", "hyperliquid_funding_rates")
        self.backfill_days = task_config.get("backfill_days", 365)
        self.bybit_collection = task_config.get("bybit_collection", "bybit_funding_rates")

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB connection required for HyperliquidFundingTask")
        # Create unique index
        db = self.mongodb_client.get_database()
        await db[self.collection_name].create_index(
            [("pair", 1), ("timestamp_utc", 1)], unique=True
        )

    async def _get_bybit_pairs(self) -> Set[str]:
        """Get distinct pairs from Bybit funding collection."""
        db = self.mongodb_client.get_database()
        pairs = await db[self.bybit_collection].distinct("pair")
        return set(pairs)

    async def _get_hl_coins(self, session: aiohttp.ClientSession) -> List[str]:
        """Get available coins from Hyperliquid metaAndAssetCtxs."""
        async with session.post(HL_API_URL, json={"type": "metaAndAssetCtxs"}) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.error(f"HL metaAndAssetCtxs failed: HTTP {resp.status} - {text[:200]}")
                return []
            data = await resp.json()
        # Response: [meta_dict, asset_ctxs_list]
        # meta_dict["universe"] is list of {"name": "BTC", ...}
        meta = data[0]
        return [asset["name"] for asset in meta["universe"]]

    async def _get_last_timestamp(self, pair: str) -> Optional[int]:
        """Get last stored timestamp_utc (ms) for a pair."""
        db = self.mongodb_client.get_database()
        doc = await db[self.collection_name].find_one(
            {"pair": pair}, sort=[("timestamp_utc", -1)]
        )
        return doc["timestamp_utc"] if doc else None

    async def _fetch_funding_history(
        self, session: aiohttp.ClientSession, coin: str, start_time: int
    ) -> List[Dict]:
        """Fetch funding history page (up to 500 records) for a coin starting at start_time."""
        payload = {"type": "fundingHistory", "coin": coin, "startTime": start_time}
        async with session.post(HL_API_URL, json=payload) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning(f"HL fundingHistory {coin}: HTTP {resp.status} - {text[:200]}")
                return []
            return await resp.json()

    async def _collect_coin(
        self, session: aiohttp.ClientSession, coin: str, pair: str, backfill_start: int
    ) -> int:
        """Collect all funding history for a coin since backfill_start. Returns doc count."""
        last_ts = await self._get_last_timestamp(pair)
        start_time = last_ts if last_ts else backfill_start

        db = self.mongodb_client.get_database()
        collection = db[self.collection_name]
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        total = 0

        while start_time < now_ms:
            records = await self._fetch_funding_history(session, coin, start_time)
            if not records:
                break

            ops = []
            for r in records:
                ts = int(r["time"])
                ops.append(UpdateOne(
                    {"pair": pair, "timestamp_utc": ts},
                    {"$setOnInsert": {
                        "pair": pair,
                        "coin": coin,
                        "timestamp_utc": ts,
                        "funding_rate": float(r["fundingRate"]),
                        "premium": float(r["premium"]),
                        "recorded_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                    }},
                    upsert=True,
                ))

            if ops:
                result = await collection.bulk_write(ops, ordered=False)
                total += result.upserted_count

            # Paginate: use last record's time as next startTime
            last_time = int(records[-1]["time"])
            if last_time <= start_time:
                # No progress, done
                break
            start_time = last_time

            # Rate limiting: 20 weight per call, 1200/min budget
            await asyncio.sleep(1.0)

        return total

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        stats: Dict[str, Any] = {"coins": 0, "docs": 0, "skipped": 0, "errors": 0}

        backfill_start = int(
            (datetime.now(timezone.utc) - timedelta(days=self.backfill_days)).timestamp() * 1000
        )

        async with aiohttp.ClientSession(
            headers={"User-Agent": "quants-lab/1.0"}
        ) as session:
            # Discover coins: intersect HL universe with Bybit pairs
            hl_coins = await self._get_hl_coins(session)
            bybit_pairs = await self._get_bybit_pairs()

            if not hl_coins:
                logger.warning("No HL coins discovered, aborting")
                return {"status": "completed", "stats": stats}

            # Build coin -> pair mapping for coins that exist on both exchanges
            coin_pair_map = {}
            for coin in hl_coins:
                pair = _hl_coin_to_pair(coin)
                if pair in bybit_pairs:
                    coin_pair_map[coin] = pair

            logger.info(
                f"HL funding: {len(coin_pair_map)} coins matched out of "
                f"{len(hl_coins)} HL / {len(bybit_pairs)} Bybit"
            )

            for coin, pair in sorted(coin_pair_map.items()):
                try:
                    count = await self._collect_coin(session, coin, pair, backfill_start)
                    stats["coins"] += 1
                    stats["docs"] += count
                    if count > 0:
                        logger.info(f"HL funding {coin} ({pair}): {count} new docs")
                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"Error fetching HL funding for {coin}: {e}")
                    continue

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"HyperliquidFundingTask completed in {duration:.1f}s: {stats}")

        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "stats": stats,
            "duration_seconds": duration,
        }

    async def on_success(self, context: TaskContext, result) -> None:
        stats = result.result_data.get("stats", {})
        logger.info(
            f"HyperliquidFundingTask: {stats['coins']} coins, {stats['docs']} new docs"
        )

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"HyperliquidFundingTask failed: {result.error_message}")


async def main():
    """Standalone test execution."""
    from core.tasks.base import ScheduleConfig, TaskConfig

    config = TaskConfig(
        name="hyperliquid_funding_test",
        enabled=True,
        task_class="app.tasks.data_collection.hyperliquid_funding_task.HyperliquidFundingTask",
        schedule=ScheduleConfig(type="frequency", frequency_hours=0.25),
        config={
            "collection_name": "hyperliquid_funding_rates",
            "backfill_days": 365,
        },
    )

    task = HyperliquidFundingTask(config)
    result = await task.run()
    print(f"Status: {result.status}")
    if result.result_data:
        print(f"Stats: {result.result_data.get('stats')}")
    if result.error_message:
        print(f"Error: {result.error_message}")


if __name__ == "__main__":
    asyncio.run(main())
