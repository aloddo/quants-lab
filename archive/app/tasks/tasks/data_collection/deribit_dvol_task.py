"""
Deribit DVOL (implied volatility index) collection task.

Fetches hourly DVOL OHLC data for BTC + ETH from Deribit public API.
No auth needed. Supports full historical backfill (since April 2021) via cursor pagination,
then incremental updates on subsequent runs.

Priority: HIGH — free, institutional-grade vol signal, complements options surface data.
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp
from pymongo import UpdateOne

from core.tasks import BaseTask, TaskContext
from app.tasks.notifying_task import NotifyingTaskMixin

logger = logging.getLogger(__name__)

DERIBIT_API_BASE = "https://www.deribit.com/api/v2"
# DVOL data available since April 2021
DVOL_FLOOR_TIMESTAMP_MS = 1617235200000


class DeribitDvolTask(NotifyingTaskMixin, BaseTask):
    """Fetch DVOL index OHLC for BTC + ETH from Deribit → MongoDB."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.currencies = task_config.get("currencies", ["BTC", "ETH"])
        self.resolution = task_config.get("resolution", 3600)
        self.collection_name = task_config.get("collection_name", "deribit_dvol")

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB connection required for DeribitDvolTask")
        # Ensure unique index on (currency, timestamp_utc)
        collection = self.mongodb_client.db[self.collection_name]
        await collection.create_index(
            [("currency", 1), ("timestamp_utc", 1)],
            unique=True,
        )

    async def _get_last_timestamp(self, currency: str) -> Optional[int]:
        """Get the most recent stored timestamp for a currency."""
        collection = self.mongodb_client.db[self.collection_name]
        doc = await collection.find_one(
            {"currency": currency},
            sort=[("timestamp_utc", -1)],
        )
        return doc["timestamp_utc"] if doc else None

    async def _fetch_page(
        self,
        session: aiohttp.ClientSession,
        currency: str,
        start_ts: int,
        end_ts: int,
    ) -> Dict:
        """Fetch one page of DVOL data."""
        url = (
            f"{DERIBIT_API_BASE}/public/get_volatility_index_data"
            f"?currency={currency}"
            f"&start_timestamp={start_ts}"
            f"&end_timestamp={end_ts}"
            f"&resolution={self.resolution}"
        )
        async with session.get(url) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning(
                    f"DVOL {currency}: HTTP {resp.status} — {text[:200]}"
                )
                return {"data": [], "continuation": "none"}
            data = await resp.json()
            return data.get("result", {"data": [], "continuation": "none"})

    async def _collect_currency(
        self, session: aiohttp.ClientSession, currency: str
    ) -> int:
        """Collect all DVOL data for one currency, returning count of upserted docs."""
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        last_ts = await self._get_last_timestamp(currency)

        if last_ts is not None:
            start_ts = last_ts  # overlap by one point for safety
        else:
            start_ts = DVOL_FLOOR_TIMESTAMP_MS
            logger.info(f"DVOL {currency}: first run, backfilling from April 2021")

        end_ts = now_ms
        total_upserted = 0
        collected_at = now_ms

        while True:
            result = await self._fetch_page(session, currency, start_ts, end_ts)
            rows = result.get("data", [])

            if rows:
                ops = []
                for row in rows:
                    # row: [timestamp_ms, open, high, low, close]
                    ts_ms, dvol_open, dvol_high, dvol_low, dvol_close = row
                    ops.append(UpdateOne(
                        {"currency": currency, "timestamp_utc": ts_ms},
                        {"$set": {
                            "dvol_open": dvol_open,
                            "dvol_high": dvol_high,
                            "dvol_low": dvol_low,
                            "dvol_close": dvol_close,
                            "resolution": self.resolution,
                            "recorded_at": collected_at,
                        }},
                        upsert=True,
                    ))

                collection = self.mongodb_client.db[self.collection_name]
                bulk_result = await collection.bulk_write(ops, ordered=False)
                total_upserted += bulk_result.upserted_count + bulk_result.modified_count

            continuation = result.get("continuation")
            if not continuation or continuation == "none" or not rows:
                break

            # continuation is the next end_timestamp for pagination
            end_ts = int(continuation)
            await asyncio.sleep(0.15)  # stay well under rate limit

        return total_upserted

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        stats: Dict[str, Any] = {"currencies": {}, "total_upserted": 0, "errors": 0}

        async with aiohttp.ClientSession(
            headers={"User-Agent": "quants-lab/2.0"},
            timeout=aiohttp.ClientTimeout(total=300),
        ) as session:
            for currency in self.currencies:
                try:
                    count = await self._collect_currency(session, currency)
                    stats["currencies"][currency] = count
                    stats["total_upserted"] += count
                    logger.info(f"DVOL {currency}: {count} documents upserted")
                    await asyncio.sleep(0.5)
                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"Error collecting DVOL {currency}: {e}")
                    continue

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"DeribitDvolTask completed in {duration:.1f}s: {stats}")

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
            f"DeribitDvolTask: {stats['total_upserted']} total docs upserted "
            f"({stats['currencies']})"
        )

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"DeribitDvolTask failed: {result.error_message}")


async def main():
    """Standalone test execution."""
    from core.tasks.base import ScheduleConfig, TaskConfig

    config = TaskConfig(
        name="deribit_dvol_test",
        enabled=True,
        task_class="app.tasks.data_collection.deribit_dvol_task.DeribitDvolTask",
        schedule=ScheduleConfig(type="frequency", frequency_hours=0.25),
        config={
            "currencies": ["BTC", "ETH"],
            "resolution": 3600,
            "collection_name": "deribit_dvol",
        },
    )

    task = DeribitDvolTask(config)
    result = await task.run()
    print(f"Status: {result.status}")
    if result.result_data:
        print(f"Stats: {result.result_data.get('stats')}")
    if result.error_message:
        print(f"Error: {result.error_message}")


if __name__ == "__main__":
    asyncio.run(main())
