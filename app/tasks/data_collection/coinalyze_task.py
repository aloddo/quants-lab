"""
Coinalyze cross-exchange data collection task.

Fetches aggregated liquidations and open interest from Coinalyze free API.
Replaces Coinglass (which requires paid plan). Unlocks X5 research.

Symbol format: {BASE}{QUOTE}_PERP.A (aggregated across all exchanges)
API: GET endpoints with api_key param. 40 symbols per request. 40 req/min.
History: daily = unlimited, hourly = ~15-20 day rolling window.
"""
import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import aiohttp
from pymongo import UpdateOne

from core.tasks import BaseTask, TaskContext
from app.tasks.notifying_task import NotifyingTaskMixin

logger = logging.getLogger(__name__)

COINALYZE_BASE = "https://api.coinalyze.net/v1"
# Max symbols per request (API limit)
BATCH_SIZE = 40


class CoinalyzeTask(NotifyingTaskMixin, BaseTask):
    """Fetch cross-exchange liquidations + aggregated OI from Coinalyze."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.api_key = os.environ.get(
            task_config.get("api_key_env", "COINALYZE_API_KEY"), ""
        )
        self.liq_collection = task_config.get("liq_collection", "coinalyze_liquidations")
        self.oi_collection = task_config.get("oi_collection", "coinalyze_oi")
        self.hourly_resolution = task_config.get("hourly_resolution", "1hour")
        self.daily_resolution = task_config.get("daily_resolution", "daily")
        self.backfill_daily_days = task_config.get("backfill_daily_days", 365)
        self.hourly_lookback_days = task_config.get("hourly_lookback_days", 15)

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for CoinalyzeTask")
        if not self.api_key:
            raise RuntimeError("COINALYZE_API_KEY not set")

        db = self.mongodb_client.db
        await db[self.liq_collection].create_index(
            [("pair", 1), ("timestamp_utc", 1), ("resolution", 1)],
            unique=True,
        )
        await db[self.oi_collection].create_index(
            [("pair", 1), ("timestamp_utc", 1), ("resolution", 1)],
            unique=True,
        )

    async def _discover_pairs(self) -> List[str]:
        """Get Bybit pairs that have data, return list of base assets."""
        db = self.mongodb_client.db
        bybit_pairs = await db["bybit_funding_rates"].distinct("pair")
        # Extract base asset: "BTC-USDT" -> "BTC"
        return sorted(set(p.replace("-USDT", "") for p in bybit_pairs if p.endswith("-USDT")))

    def _to_coinalyze_symbol(self, base: str) -> str:
        """Convert base asset to Coinalyze aggregate perp symbol."""
        return f"{base}USDT_PERP.A"

    def _to_pair(self, base: str) -> str:
        """Convert base asset to Bybit-style pair."""
        return f"{base}-USDT"

    async def _fetch(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        symbols: List[str],
        resolution: str,
        from_ts: int,
        to_ts: int,
    ) -> List[Dict]:
        """Fetch one batch from Coinalyze API."""
        symbols_str = ",".join(symbols)
        url = (
            f"{COINALYZE_BASE}/{endpoint}"
            f"?api_key={self.api_key}"
            f"&symbols={symbols_str}"
            f"&interval={resolution}"
            f"&from={from_ts}&to={to_ts}"
        )
        async with session.get(url) as resp:
            if resp.status == 429:
                retry_after = int(float(resp.headers.get("Retry-After", "60")))
                logger.warning(f"Coinalyze rate limited, waiting {retry_after}s")
                await asyncio.sleep(retry_after)
                return await self._fetch(session, endpoint, symbols, resolution, from_ts, to_ts)
            if resp.status != 200:
                text = await resp.text()
                logger.warning(f"Coinalyze {endpoint}: HTTP {resp.status} — {text[:200]}")
                return []
            return await resp.json()

    async def _collect_liquidations(
        self,
        session: aiohttp.ClientSession,
        bases: List[str],
        resolution: str,
        from_ts: int,
        to_ts: int,
    ) -> int:
        """Collect liquidation data for all bases."""
        db = self.mongodb_client.db
        collection = db[self.liq_collection]
        total = 0
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        for i in range(0, len(bases), BATCH_SIZE):
            batch = bases[i : i + BATCH_SIZE]
            symbols = [self._to_coinalyze_symbol(b) for b in batch]

            data = await self._fetch(
                session, "liquidation-history", symbols, resolution, from_ts, to_ts
            )

            ops = []
            for item in data:
                symbol = item.get("symbol", "")
                # Extract base: "BTCUSDT_PERP.A" -> "BTC"
                base = symbol.replace("USDT_PERP.A", "").replace("USDT_PERP.", "")
                pair = self._to_pair(base)

                for row in item.get("history", []):
                    ts_s = row["t"]
                    ops.append(
                        UpdateOne(
                            {"pair": pair, "timestamp_utc": ts_s * 1000, "resolution": resolution},
                            {"$set": {
                                "pair": pair,
                                "timestamp_utc": ts_s * 1000,
                                "long_liquidations_usd": row.get("l", 0),
                                "short_liquidations_usd": row.get("s", 0),
                                "total_liquidations_usd": row.get("l", 0) + row.get("s", 0),
                                "resolution": resolution,
                                "recorded_at": now_ms,
                            }},
                            upsert=True,
                        )
                    )

            if ops:
                result = await collection.bulk_write(ops, ordered=False)
                total += result.upserted_count + result.modified_count

            await asyncio.sleep(2.0)  # stay under 40 req/min

        return total

    async def _collect_oi(
        self,
        session: aiohttp.ClientSession,
        bases: List[str],
        resolution: str,
        from_ts: int,
        to_ts: int,
    ) -> int:
        """Collect open interest data for all bases."""
        db = self.mongodb_client.db
        collection = db[self.oi_collection]
        total = 0
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        for i in range(0, len(bases), BATCH_SIZE):
            batch = bases[i : i + BATCH_SIZE]
            symbols = [self._to_coinalyze_symbol(b) for b in batch]

            data = await self._fetch(
                session, "open-interest-history", symbols, resolution, from_ts, to_ts
            )

            ops = []
            for item in data:
                symbol = item.get("symbol", "")
                base = symbol.replace("USDT_PERP.A", "").replace("USDT_PERP.", "")
                pair = self._to_pair(base)

                for row in item.get("history", []):
                    ts_s = row["t"]
                    ops.append(
                        UpdateOne(
                            {"pair": pair, "timestamp_utc": ts_s * 1000, "resolution": resolution},
                            {"$set": {
                                "pair": pair,
                                "timestamp_utc": ts_s * 1000,
                                "oi_open": row.get("o", 0),
                                "oi_high": row.get("h", 0),
                                "oi_low": row.get("l", 0),
                                "oi_close": row.get("c", 0),
                                "resolution": resolution,
                                "recorded_at": now_ms,
                            }},
                            upsert=True,
                        )
                    )

            if ops:
                result = await collection.bulk_write(ops, ordered=False)
                total += result.upserted_count + result.modified_count

            await asyncio.sleep(1.5)

        return total

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        now_ts = int(start.timestamp())
        stats = {"liq_hourly": 0, "liq_daily": 0, "oi_hourly": 0, "oi_daily": 0, "pairs": 0, "errors": 0}

        bases = await self._discover_pairs()
        stats["pairs"] = len(bases)
        logger.info(f"Coinalyze: collecting for {len(bases)} pairs")

        async with aiohttp.ClientSession(
            headers={"User-Agent": "quants-lab/2.0"},
            timeout=aiohttp.ClientTimeout(total=600),
        ) as session:
            try:
                # Hourly data (rolling ~15-20 day window)
                hourly_from = now_ts - self.hourly_lookback_days * 86400
                stats["liq_hourly"] = await self._collect_liquidations(
                    session, bases, self.hourly_resolution, hourly_from, now_ts
                )
                stats["oi_hourly"] = await self._collect_oi(
                    session, bases, self.hourly_resolution, hourly_from, now_ts
                )

                # Daily data (full backfill, unlimited history)
                daily_from = now_ts - self.backfill_daily_days * 86400
                stats["liq_daily"] = await self._collect_liquidations(
                    session, bases, self.daily_resolution, daily_from, now_ts
                )
                stats["oi_daily"] = await self._collect_oi(
                    session, bases, self.daily_resolution, daily_from, now_ts
                )
            except Exception as e:
                stats["errors"] += 1
                logger.error(f"Coinalyze collection error: {e}")

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"CoinalyzeTask completed in {duration:.1f}s: {stats}")

        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "stats": stats,
            "duration_seconds": duration,
        }

    async def on_success(self, context: TaskContext, result) -> None:
        stats = result.result_data.get("stats", {})
        total = sum(v for k, v in stats.items() if k not in ("pairs", "errors"))
        logger.info(f"CoinalyzeTask: {total} docs across {stats['pairs']} pairs")

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"CoinalyzeTask failed: {result.error_message}")


async def main():
    """Standalone test execution."""
    from core.tasks.base import ScheduleConfig, TaskConfig

    config = TaskConfig(
        name="coinalyze_test",
        enabled=True,
        task_class="app.tasks.data_collection.coinalyze_task.CoinalyzeTask",
        schedule=ScheduleConfig(type="frequency", frequency_hours=0.25),
        config={
            "api_key_env": "COINALYZE_API_KEY",
            "liq_collection": "coinalyze_liquidations",
            "oi_collection": "coinalyze_oi",
            "backfill_daily_days": 365,
            "hourly_lookback_days": 15,
        },
    )

    task = CoinalyzeTask(config)
    result = await task.run()
    print(f"Status: {result.status}")
    if result.result_data:
        print(f"Stats: {result.result_data.get('stats')}")
    if result.error_message:
        print(f"Error: {result.error_message}")


if __name__ == "__main__":
    asyncio.run(main())
