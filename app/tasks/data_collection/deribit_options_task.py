"""
Deribit options surface collection task.

Fetches full BTC + ETH options chain from Deribit public API every 15 minutes.
No API key needed. Stores raw option snapshots in MongoDB for IV surface reconstruction.

Priority: HIGH — free, highest information density, institutional-grade signal.
Historical limitation: Deribit API only returns current snapshot. Must poll continuously.
"""
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import aiohttp

from core.tasks import BaseTask, TaskContext
from app.tasks.notifying_task import NotifyingTaskMixin

logger = logging.getLogger(__name__)

DERIBIT_API_BASE = "https://www.deribit.com/api/v2"


class DeribitOptionsSurfaceTask(NotifyingTaskMixin, BaseTask):
    """Fetch full options chain for BTC + ETH from Deribit → MongoDB."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.currencies = task_config.get("currencies", ["BTC", "ETH"])
        self.collection_name = task_config.get(
            "collection_name", "deribit_options_surface"
        )

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB connection required for DeribitOptionsSurfaceTask")

    async def _fetch_instruments(
        self, session: aiohttp.ClientSession, currency: str
    ) -> List[Dict]:
        """Get all active option instruments for a currency."""
        url = (
            f"{DERIBIT_API_BASE}/public/get_instruments"
            f"?currency={currency}&kind=option&expired=false"
        )
        async with session.get(url) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning(f"Deribit instruments {currency}: HTTP {resp.status} — {text[:200]}")
                return []
            data = await resp.json()
            return data.get("result", [])

    async def _fetch_book_summary(
        self, session: aiohttp.ClientSession, currency: str
    ) -> List[Dict]:
        """Get book summary (IV, OI, volume) for ALL options of a currency."""
        url = (
            f"{DERIBIT_API_BASE}/public/get_book_summary_by_currency"
            f"?currency={currency}&kind=option"
        )
        async with session.get(url) as resp:
            if resp.status != 200:
                text = await resp.text()
                logger.warning(f"Deribit book summary {currency}: HTTP {resp.status} — {text[:200]}")
                return []
            data = await resp.json()
            return data.get("result", [])

    async def _fetch_index_price(
        self, session: aiohttp.ClientSession, currency: str
    ) -> float:
        """Get current index (spot) price for underlying."""
        url = f"{DERIBIT_API_BASE}/public/get_index_price?index_name={currency.lower()}_usd"
        async with session.get(url) as resp:
            if resp.status != 200:
                return 0.0
            data = await resp.json()
            return data.get("result", {}).get("index_price", 0.0)

    def _parse_instrument_name(self, name: str) -> Dict:
        """Parse Deribit instrument name: BTC-25APR26-85000-C → components."""
        parts = name.split("-")
        if len(parts) < 4:
            return {}
        return {
            "currency": parts[0],
            "expiry_str": parts[1],
            "strike": float(parts[2]),
            "type": "call" if parts[3] == "C" else "put",
        }

    async def _collect_currency(
        self, session: aiohttp.ClientSession, currency: str
    ) -> List[Dict]:
        """Collect full options surface for one currency."""
        collected_at = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Fetch book summary and index price in parallel
        book_data, index_price = await asyncio.gather(
            self._fetch_book_summary(session, currency),
            self._fetch_index_price(session, currency),
        )

        if not book_data:
            logger.warning(f"No book data for {currency} options")
            return []

        docs = []
        for entry in book_data:
            instrument_name = entry.get("instrument_name", "")
            parsed = self._parse_instrument_name(instrument_name)
            if not parsed:
                continue

            mark_iv = entry.get("mark_iv")
            if mark_iv is None or mark_iv == 0:
                continue  # Skip instruments with no IV

            # Convert Deribit timestamp (milliseconds)
            timestamp_utc = entry.get("creation_timestamp", collected_at)

            doc = {
                "currency": currency,
                "instrument_name": instrument_name,
                "timestamp_utc": collected_at,
                "expiry": parsed["expiry_str"],
                "strike": parsed["strike"],
                "type": parsed["type"],
                "mark_iv": mark_iv / 100.0,  # Deribit returns % — normalize to decimal
                "open_interest": entry.get("open_interest", 0),
                "volume_24h": entry.get("volume", 0),
                "mark_price_crypto": entry.get("mark_price", 0),
                "mark_price_usd": entry.get("usd", 0),
                "bid_iv": (entry.get("bid_iv") or 0) / 100.0,
                "ask_iv": (entry.get("ask_iv") or 0) / 100.0,
                "underlying_price": entry.get("underlying_price", index_price),
                "underlying_index": entry.get("underlying_index", f"{currency.lower()}_usd"),
                "interest_rate": entry.get("interest_rate", 0),
                "mid_price": entry.get("mid_price", 0),
                "collected_at": collected_at,
            }
            docs.append(doc)

        return docs

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        stats = {"currencies": 0, "docs": 0, "errors": 0}

        async with aiohttp.ClientSession(
            headers={"User-Agent": "quants-lab/2.0"},
            timeout=aiohttp.ClientTimeout(total=60),
        ) as session:
            for currency in self.currencies:
                try:
                    docs = await self._collect_currency(session, currency)
                    if docs:
                        await self.mongodb_client.insert_documents(
                            collection_name=self.collection_name,
                            documents=docs,
                            index=[
                                ("currency", 1),
                                ("timestamp_utc", 1),
                                ("expiry", 1),
                                ("strike", 1),
                                ("type", 1),
                            ],
                        )
                        stats["docs"] += len(docs)
                    stats["currencies"] += 1
                    logger.info(
                        f"Deribit {currency}: {len(docs)} option snapshots collected"
                    )

                    # Be polite with rate limits
                    await asyncio.sleep(0.5)

                except Exception as e:
                    stats["errors"] += 1
                    logger.error(f"Error collecting Deribit {currency} options: {e}")
                    continue

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        logger.info(f"DeribitOptionsSurfaceTask completed in {duration:.1f}s: {stats}")

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
            f"DeribitOptionsSurfaceTask: {stats['currencies']} currencies, "
            f"{stats['docs']} option snapshots stored"
        )

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"DeribitOptionsSurfaceTask failed: {result.error_message}")


async def main():
    """Standalone test execution."""
    from core.tasks.base import ScheduleConfig, TaskConfig

    config = TaskConfig(
        name="deribit_options_test",
        enabled=True,
        task_class="app.tasks.data_collection.deribit_options_task.DeribitOptionsSurfaceTask",
        schedule=ScheduleConfig(type="frequency", frequency_hours=0.25),
        config={"currencies": ["BTC", "ETH"]},
    )

    task = DeribitOptionsSurfaceTask(config)
    result = await task.run()
    print(f"Status: {result.status}")
    if result.result_data:
        print(f"Stats: {result.result_data.get('stats')}")
    if result.error_message:
        print(f"Error: {result.error_message}")


if __name__ == "__main__":
    asyncio.run(main())
