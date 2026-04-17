"""
Fear & Greed Index collection task.

Fetches from alternative.me free API (no auth, no rate limits).
Daily resolution, full history available in single call (2018+).

Priority: MEDIUM — free, easy, adds sentiment dimension.
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

import aiohttp
from pymongo import UpdateOne

from core.tasks import BaseTask, TaskContext
from app.tasks.notifying_task import NotifyingTaskMixin

logger = logging.getLogger(__name__)

FEAR_GREED_API = "https://api.alternative.me/fng/"


class FearGreedTask(NotifyingTaskMixin, BaseTask):
    """Fetch Fear & Greed Index from alternative.me → MongoDB."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        # How many days to fetch (0 = all history)
        self.limit = task_config.get("limit", 7)
        self.collection_name = task_config.get(
            "collection_name", "fear_greed_index"
        )

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB connection required for FearGreedTask")

    async def _fetch_index(
        self, session: aiohttp.ClientSession
    ) -> List[Dict]:
        """Fetch Fear & Greed data."""
        url = f"{FEAR_GREED_API}?limit={self.limit}&format=json"

        async with session.get(url) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning(f"Fear & Greed API: HTTP {resp.status} — {text[:200]}")
                return []
            data = await resp.json()

        collected_at = int(datetime.now(timezone.utc).timestamp() * 1000)
        docs = []
        for entry in data.get("data", []):
            # alternative.me returns timestamp as seconds string
            ts_sec = int(entry.get("timestamp", 0))
            doc = {
                "timestamp_utc": ts_sec * 1000,  # convert to milliseconds
                "value": int(entry.get("value", 0)),
                "classification": entry.get("value_classification", ""),
                "source": "alternative.me",
                "collected_at": collected_at,
            }
            docs.append(doc)

        return docs

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        stats = {"docs": 0, "errors": 0}

        async with aiohttp.ClientSession(
            headers={"User-Agent": "quants-lab/2.0"},
            timeout=aiohttp.ClientTimeout(total=30),
        ) as session:
            try:
                docs = await self._fetch_index(session)
                if docs:
                    db = self.mongodb_client.db
                    collection = db[self.collection_name]
                    ops = [
                        UpdateOne(
                            {"timestamp_utc": doc["timestamp_utc"]},
                            {"$setOnInsert": doc},
                            upsert=True,
                        )
                        for doc in docs
                    ]
                    result = await collection.bulk_write(ops, ordered=False)
                    stats["docs"] = result.upserted_count

            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Error fetching Fear & Greed Index: {e}")

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"FearGreedTask completed in {duration:.1f}s: {stats}")

        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "stats": stats,
            "duration_seconds": duration,
        }

    async def on_success(self, context: TaskContext, result) -> None:
        stats = result.result_data.get("stats", {})
        logger.info(f"FearGreedTask: {stats['docs']} docs inserted")

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"FearGreedTask failed: {result.error_message}")


async def main():
    """Standalone test execution."""
    from core.tasks.base import ScheduleConfig, TaskConfig

    config = TaskConfig(
        name="fear_greed_test",
        enabled=True,
        task_class="app.tasks.data_collection.fear_greed_task.FearGreedTask",
        schedule=ScheduleConfig(type="frequency", frequency_hours=24),
        config={"limit": 3},
    )

    task = FearGreedTask(config)
    result = await task.run()
    print(f"Status: {result.status}")
    if result.result_data:
        print(f"Stats: {result.result_data.get('stats')}")
    if result.error_message:
        print(f"Error: {result.error_message}")


if __name__ == "__main__":
    asyncio.run(main())
