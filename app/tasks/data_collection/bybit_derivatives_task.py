"""
Combined Bybit derivatives data task — funding rates, open interest, L/S ratio.

Fetches from Bybit public REST API and writes to MongoDB.
Schedule: every 15 minutes via YAML DAG.
"""
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict

import aiohttp

from app.services.bybit_rest import (
    fetch_funding_rates,
    fetch_ls_ratio,
    fetch_open_interest,
    fetch_tickers,
)
from core.tasks import BaseTask, TaskContext

logger = logging.getLogger(__name__)


class BybitDerivativesTask(BaseTask):
    """Fetch funding rates, open interest, and L/S ratio for Bybit USDT perpetuals."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config

        # "auto" = discover from tickers, or explicit list of pairs
        self.pairs_config = task_config.get("pairs", "auto")
        self.min_volume_usd = task_config.get("min_volume_usd", 10_000_000)
        self.oi_interval = task_config.get("oi_interval", "1h")
        self.funding_limit = task_config.get("funding_limit", 5)
        self.oi_limit = task_config.get("oi_limit", 5)
        self.ls_limit = task_config.get("ls_limit", 5)

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB connection required for BybitDerivativesTask")

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        stats = {"pairs": 0, "funding": 0, "oi": 0, "ls": 0, "errors": 0}

        async with aiohttp.ClientSession(
            headers={"User-Agent": "quants-lab/1.0"}
        ) as session:
            # Resolve pair list
            if self.pairs_config == "auto":
                tickers = await fetch_tickers(session)
                pairs = [
                    t["pair"]
                    for t in tickers
                    if t["volume_24h"] >= self.min_volume_usd
                ]
                logger.info(f"Auto-discovered {len(pairs)} pairs with >${self.min_volume_usd/1e6:.0f}M volume")
            else:
                pairs = self.pairs_config

            # Fetch data for each pair
            for pair in pairs:
                try:
                    # Funding rates
                    funding = await fetch_funding_rates(session, pair, limit=self.funding_limit)
                    if funding:
                        await self.mongodb_client.insert_documents(
                            collection_name="bybit_funding_rates",
                            documents=funding,
                            index=[("pair", 1), ("timestamp_utc", 1)],
                        )
                        stats["funding"] += len(funding)

                    # Open interest
                    oi = await fetch_open_interest(
                        session, pair, interval=self.oi_interval, limit=self.oi_limit
                    )
                    if oi:
                        await self.mongodb_client.insert_documents(
                            collection_name="bybit_open_interest",
                            documents=oi,
                            index=[("pair", 1), ("timestamp_utc", 1)],
                        )
                        stats["oi"] += len(oi)

                    # L/S ratio
                    ls = await fetch_ls_ratio(session, pair, limit=self.ls_limit)
                    if ls:
                        await self.mongodb_client.insert_documents(
                            collection_name="bybit_ls_ratio",
                            documents=ls,
                            index=[("pair", 1), ("timestamp_utc", 1)],
                        )
                        stats["ls"] += len(ls)

                    stats["pairs"] += 1

                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"Error fetching derivatives for {pair}: {e}")
                    continue

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"BybitDerivativesTask completed in {duration:.1f}s: {stats}")

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
            f"BybitDerivativesTask: {stats['pairs']} pairs, "
            f"{stats['funding']} funding, {stats['oi']} OI, {stats['ls']} LS docs"
        )

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"BybitDerivativesTask failed: {result.error_message}")


async def main():
    """Standalone test execution."""
    from core.tasks.base import ScheduleConfig, TaskConfig

    config = TaskConfig(
        name="bybit_derivatives_test",
        enabled=True,
        task_class="app.tasks.data_collection.bybit_derivatives_task.BybitDerivativesTask",
        schedule=ScheduleConfig(type="frequency", frequency_hours=0.25),
        config={
            "pairs": ["BTC-USDT", "ETH-USDT"],
            "oi_interval": "1h",
            "funding_limit": 3,
            "oi_limit": 3,
            "ls_limit": 3,
        },
    )

    task = BybitDerivativesTask(config)
    result = await task.run()
    print(f"Status: {result.status}")
    if result.result_data:
        print(f"Stats: {result.result_data.get('stats')}")
    if result.error_message:
        print(f"Error: {result.error_message}")


if __name__ == "__main__":
    asyncio.run(main())
