"""
Binance perpetual futures funding rate collection task.

Fetches from Binance public REST API (no API key needed) and writes to MongoDB.
Used for cross-exchange funding spread signals (H2: Bybit funding vs Binance funding).
Schedule: every 15 minutes via YAML DAG.
"""
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import aiohttp
from pymongo import UpdateOne

from core.tasks import BaseTask, TaskContext

logger = logging.getLogger(__name__)


from app.tasks.notifying_task import NotifyingTaskMixin


# Binance uses no separator in symbol names
def _pair_to_binance_symbol(pair: str) -> str:
    """Convert 'BTC-USDT' → 'BTCUSDT'."""
    return pair.replace("-", "")


def _binance_symbol_to_pair(symbol: str) -> str:
    """Convert 'BTCUSDT' → 'BTC-USDT'. Handles USDT suffix only."""
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        return f"{base}-USDT"
    return symbol


class BinanceFundingTask(NotifyingTaskMixin, BaseTask):
    """Fetch funding rates from Binance Futures public API → MongoDB."""

    BINANCE_FAPI_BASE = "https://fapi.binance.com"

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config

        # "auto" = use same pairs as Bybit derivatives task, or explicit list
        self.pairs_config = task_config.get("pairs", "auto")
        self.funding_limit = task_config.get("funding_limit", 5)
        # Bybit pair list for auto mode (read from MongoDB bybit_funding_rates)
        self.auto_from_collection = task_config.get(
            "auto_from_collection", "bybit_funding_rates"
        )

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB connection required for BinanceFundingTask")

    async def _resolve_pairs(self) -> List[str]:
        """Get pair list — either explicit or auto-discovered from Bybit collection."""
        if self.pairs_config != "auto":
            return self.pairs_config

        # Auto: get distinct pairs from Bybit funding collection
        db = self.mongodb_client.get_database()
        pairs = await db[self.auto_from_collection].distinct("pair")
        logger.info(f"Auto-discovered {len(pairs)} pairs from {self.auto_from_collection}")
        return sorted(pairs)

    async def _fetch_funding_rates(
        self, session: aiohttp.ClientSession, pair: str
    ) -> List[Dict]:
        """Fetch recent funding rates for a single pair from Binance."""
        symbol = _pair_to_binance_symbol(pair)
        url = (
            f"{self.BINANCE_FAPI_BASE}/fapi/v1/fundingRate"
            f"?symbol={symbol}&limit={self.funding_limit}"
        )

        async with session.get(url) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning(f"Binance funding {pair}: HTTP {resp.status} — {text[:200]}")
                return []
            data = await resp.json()

        docs = []
        for entry in data:
            docs.append({
                "pair": pair,
                "symbol": symbol,
                "funding_rate": float(entry["fundingRate"]),
                "mark_price": float(entry.get("markPrice", 0)),
                "timestamp_utc": int(entry["fundingTime"]),  # milliseconds (Binance convention)
                "source": "binance",
                "collected_at": int(datetime.now(timezone.utc).timestamp() * 1000),
            })

        return docs

    async def _upsert_docs(self, collection_name: str, documents: List[Dict]) -> int:
        """Upsert documents using bulk_write to handle duplicates gracefully."""
        db = self.mongodb_client.db
        collection = db[collection_name]
        ops = [
            UpdateOne(
                {"pair": doc["pair"], "timestamp_utc": doc["timestamp_utc"]},
                {"$setOnInsert": doc},
                upsert=True,
            )
            for doc in documents
        ]
        result = await collection.bulk_write(ops, ordered=False)
        return result.upserted_count

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        stats = {"pairs": 0, "docs": 0, "errors": 0}

        pairs = await self._resolve_pairs()

        async with aiohttp.ClientSession(
            headers={"User-Agent": "quants-lab/1.0"}
        ) as session:
            for pair in pairs:
                try:
                    docs = await self._fetch_funding_rates(session, pair)
                    if docs:
                        n = await self._upsert_docs("binance_funding_rates", docs)
                        stats["docs"] += n
                    stats["pairs"] += 1

                    # Binance rate limit: 2400 req/min — be polite
                    await asyncio.sleep(0.05)

                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"Error fetching Binance funding for {pair}: {e}")
                    continue

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"BinanceFundingTask completed in {duration:.1f}s: {stats}")

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
            f"BinanceFundingTask: {stats['pairs']} pairs, {stats['docs']} docs inserted"
        )

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"BinanceFundingTask failed: {result.error_message}")


async def main():
    """Standalone test execution."""
    from core.tasks.base import ScheduleConfig, TaskConfig

    config = TaskConfig(
        name="binance_funding_test",
        enabled=True,
        task_class="app.tasks.data_collection.binance_funding_task.BinanceFundingTask",
        schedule=ScheduleConfig(type="frequency", frequency_hours=0.25),
        config={
            "pairs": ["BTC-USDT", "ETH-USDT", "SOL-USDT"],
            "funding_limit": 3,
        },
    )

    task = BinanceFundingTask(config)
    result = await task.run()
    print(f"Status: {result.status}")
    if result.result_data:
        print(f"Stats: {result.result_data.get('stats')}")
    if result.error_message:
        print(f"Error: {result.error_message}")


if __name__ == "__main__":
    asyncio.run(main())
