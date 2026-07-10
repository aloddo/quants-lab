"""
Coinglass liquidation + cross-exchange OI collection task.

Fetches from Coinglass open API v4 (API key required, free tier available).
Collects liquidation history and OI OHLC across ALL exchanges.

Priority: HIGH — free tier sufficient, unique cross-exchange aggregation.
API key: Set COINGLASS_API_KEY in .env
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiohttp

from core.tasks import BaseTask, TaskContext
from app.tasks.notifying_task import NotifyingTaskMixin

logger = logging.getLogger(__name__)

COINGLASS_API_BASE = "https://open-api-v4.coinglass.com"

# Map our pair format to Coinglass symbol
def _pair_to_cg_symbol(pair: str) -> str:
    """Convert 'BTC-USDT' → 'BTC'."""
    return pair.split("-")[0]


class CoinglassTask(NotifyingTaskMixin, BaseTask):
    """Fetch liquidations + cross-exchange OI from Coinglass → MongoDB."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.pairs_config = task_config.get("pairs", [
            "BTC-USDT", "ETH-USDT", "SOL-USDT", "XRP-USDT",
            "DOGE-USDT", "ADA-USDT", "AVAX-USDT", "LINK-USDT",
        ])
        self.api_key = task_config.get("api_key", "")
        self.liq_interval = task_config.get("liquidation_interval", "1h")
        self.oi_interval = task_config.get("oi_interval", "1h")
        self.liq_limit = task_config.get("liquidation_limit", 24)
        self.oi_limit = task_config.get("oi_limit", 24)
        self.liq_collection = task_config.get(
            "liquidation_collection", "coinglass_liquidations"
        )
        self.oi_collection = task_config.get("oi_collection", "coinglass_oi")

    async def _resolve_pairs(self) -> List[str]:
        """Get pair list — either explicit or auto-discovered from Bybit collection."""
        if self.pairs_config != "auto":
            return self.pairs_config
        db = self.mongodb_client.get_database()
        pairs = await db["bybit_funding_rates"].distinct("pair")
        logger.info(f"Auto-discovered {len(pairs)} pairs from bybit_funding_rates")
        return sorted(pairs)

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB connection required for CoinglassTask")
        # Try reading API key from env if not in config
        if not self.api_key:
            import os
            self.api_key = os.environ.get("COINGLASS_API_KEY", "")
        if not self.api_key:
            raise RuntimeError(
                "COINGLASS_API_KEY required — set in .env or task config"
            )

    async def _fetch_liquidation_history(
        self, session: aiohttp.ClientSession, pair: str
    ) -> List[Dict]:
        """Fetch liquidation history for a symbol."""
        symbol = _pair_to_cg_symbol(pair)
        url = f"{COINGLASS_API_BASE}/api/futures/liquidation/v2/history"
        params = {
            "symbol": symbol,
            "interval": self.liq_interval,
            "limit": self.liq_limit,
        }

        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning(f"Coinglass liq {pair}: HTTP {resp.status} — {text[:200]}")
                return []
            data = await resp.json()

        if data.get("code") != "0":
            logger.warning(f"Coinglass liq {pair}: API error — {data.get('msg', 'unknown')}")
            return []

        collected_at = int(datetime.now(timezone.utc).timestamp() * 1000)
        docs = []
        for entry in data.get("data", []):
            long_liq = float(entry.get("longLiquidationUsd", 0))
            short_liq = float(entry.get("shortLiquidationUsd", 0))
            total_liq = long_liq + short_liq

            doc = {
                "pair": pair,
                "timestamp_utc": int(entry.get("createTime", 0)),
                "interval": self.liq_interval,
                "long_liquidation_usd": long_liq,
                "short_liquidation_usd": short_liq,
                "total_liquidation_usd": total_liq,
                "liquidation_imbalance": (
                    (long_liq - short_liq) / total_liq if total_liq > 0 else 0.0
                ),
                "source": "coinglass",
                "collected_at": collected_at,
            }
            docs.append(doc)

        return docs

    async def _fetch_oi_history(
        self, session: aiohttp.ClientSession, pair: str
    ) -> List[Dict]:
        """Fetch OI OHLC history across all exchanges."""
        symbol = _pair_to_cg_symbol(pair)
        url = f"{COINGLASS_API_BASE}/api/futures/openInterest/ohlc-history"
        params = {
            "symbol": symbol,
            "interval": self.oi_interval,
            "limit": self.oi_limit,
        }

        async with session.get(url, params=params) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning(f"Coinglass OI {pair}: HTTP {resp.status} — {text[:200]}")
                return []
            data = await resp.json()

        if data.get("code") != "0":
            logger.warning(f"Coinglass OI {pair}: API error — {data.get('msg', 'unknown')}")
            return []

        collected_at = int(datetime.now(timezone.utc).timestamp() * 1000)
        docs = []
        for entry in data.get("data", []):
            doc = {
                "pair": pair,
                "timestamp_utc": int(entry.get("t", 0)),
                "interval": self.oi_interval,
                "oi_open": float(entry.get("o", 0)),
                "oi_high": float(entry.get("h", 0)),
                "oi_low": float(entry.get("l", 0)),
                "oi_close": float(entry.get("c", 0)),
                "source": "coinglass",
                "collected_at": collected_at,
            }
            docs.append(doc)

        return docs

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        stats = {"pairs": 0, "liq_docs": 0, "oi_docs": 0, "errors": 0}

        pairs = await self._resolve_pairs()

        headers = {
            "User-Agent": "quants-lab/2.0",
            "coinglassSecret": self.api_key,
        }

        async with aiohttp.ClientSession(
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as session:
            for pair in pairs:
                try:
                    # Fetch liquidations and OI in parallel per pair
                    liq_docs, oi_docs = await asyncio.gather(
                        self._fetch_liquidation_history(session, pair),
                        self._fetch_oi_history(session, pair),
                    )

                    if liq_docs:
                        await self.mongodb_client.insert_documents(
                            collection_name=self.liq_collection,
                            documents=liq_docs,
                            index=[("pair", 1), ("timestamp_utc", 1)],
                        )
                        stats["liq_docs"] += len(liq_docs)

                    if oi_docs:
                        await self.mongodb_client.insert_documents(
                            collection_name=self.oi_collection,
                            documents=oi_docs,
                            index=[("pair", 1), ("timestamp_utc", 1)],
                        )
                        stats["oi_docs"] += len(oi_docs)

                    stats["pairs"] += 1

                    # Respect Coinglass rate limits
                    await asyncio.sleep(0.3)

                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"Error fetching Coinglass data for {pair}: {e}")
                    continue

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"CoinglassTask completed in {duration:.1f}s: {stats}")

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
            f"CoinglassTask: {stats['pairs']} pairs, "
            f"{stats['liq_docs']} liq docs, {stats['oi_docs']} OI docs"
        )

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"CoinglassTask failed: {result.error_message}")


async def main():
    """Standalone test execution."""
    import os
    from core.tasks.base import ScheduleConfig, TaskConfig

    config = TaskConfig(
        name="coinglass_test",
        enabled=True,
        task_class="app.tasks.data_collection.coinglass_task.CoinglassTask",
        schedule=ScheduleConfig(type="frequency", frequency_hours=0.25),
        config={
            "pairs": ["BTC-USDT", "ETH-USDT"],
            "api_key": os.environ.get("COINGLASS_API_KEY", ""),
            "liquidation_limit": 3,
            "oi_limit": 3,
        },
    )

    task = CoinglassTask(config)
    result = await task.run()
    print(f"Status: {result.status}")
    if result.result_data:
        print(f"Stats: {result.result_data.get('stats')}")
    if result.error_message:
        print(f"Error: {result.error_message}")


if __name__ == "__main__":
    asyncio.run(main())
