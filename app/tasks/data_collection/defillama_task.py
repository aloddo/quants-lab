"""
DefiLlama data collector -- stablecoin flows + DeFi TVL.
Free API, no auth needed. Daily resolution.

Signals:
- Stablecoin supply change (new money entering crypto)
- DeFi TVL change (capital deployment)
- Stablecoin dominance shifts

Collections: defillama_stablecoins, defillama_tvl
"""
import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta

import aiohttp
from pymongo import UpdateOne

from core.tasks import BaseTask, TaskContext
from app.tasks.notifying_task import NotifyingTaskMixin

logger = logging.getLogger(__name__)

STABLECOIN_URL = "https://stablecoins.llama.fi/stablecoincharts/all?stablecoin=1"  # USDT
STABLECOIN_USDC_URL = "https://stablecoins.llama.fi/stablecoincharts/all?stablecoin=2"  # USDC
TVL_URL = "https://api.llama.fi/v2/historicalChainTvl"


class DefiLlamaTask(NotifyingTaskMixin, BaseTask):
    """Collect stablecoin supply + DeFi TVL from DefiLlama."""

    def __init__(self, config):
        super().__init__(config)
        cfg = self.config.config
        self.stable_collection = cfg.get("stable_collection", "defillama_stablecoins")
        self.tvl_collection = cfg.get("tvl_collection", "defillama_tvl")
        self.backfill_days = int(cfg.get("backfill_days", 365))

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required")
        db = self.mongodb_client.db
        await db[self.stable_collection].create_index(
            [("symbol", 1), ("date", 1)], unique=True
        )
        await db[self.tvl_collection].create_index(
            [("date", 1)], unique=True
        )

    async def execute(self, context: TaskContext) -> dict:
        db = self.mongodb_client.db
        results = {"stablecoins": 0, "tvl": 0}

        async with aiohttp.ClientSession() as session:
            # Stablecoins (USDT + USDC)
            for url, symbol in [(STABLECOIN_URL, "USDT"), (STABLECOIN_USDC_URL, "USDC")]:
                try:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        if resp.status != 200:
                            logger.warning(f"DefiLlama stablecoin {symbol}: {resp.status}")
                            continue
                        data = await resp.json()

                    cutoff = int((datetime.now(timezone.utc) - timedelta(days=self.backfill_days)).timestamp())
                    ops = []
                    for point in data:
                        ts = int(point.get("date", 0))
                        if ts < cutoff:
                            continue
                        circ = point.get("totalCirculating", {}).get("peggedUSD", 0)
                        minted = point.get("totalMintedUSD", {}).get("peggedUSD", 0)
                        ops.append(UpdateOne(
                            {"symbol": symbol, "date": ts},
                            {"$set": {
                                "symbol": symbol,
                                "date": ts,
                                "circulating_usd": circ,
                                "minted_usd": minted,
                                "collected_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                            }},
                            upsert=True,
                        ))

                    if ops:
                        r = await db[self.stable_collection].bulk_write(ops, ordered=False)
                        results["stablecoins"] += r.upserted_count + r.modified_count
                        logger.info(f"DefiLlama {symbol}: {r.upserted_count} new, {r.modified_count} updated")
                except Exception as e:
                    logger.error(f"DefiLlama stablecoin {symbol} error: {e}")

            # TVL
            try:
                async with session.get(TVL_URL, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status != 200:
                        logger.warning(f"DefiLlama TVL: {resp.status}")
                    else:
                        data = await resp.json()
                        cutoff = int((datetime.now(timezone.utc) - timedelta(days=self.backfill_days)).timestamp())
                        ops = []
                        for point in data:
                            ts = int(point.get("date", 0))
                            if ts < cutoff:
                                continue
                            ops.append(UpdateOne(
                                {"date": ts},
                                {"$set": {
                                    "date": ts,
                                    "tvl_usd": point.get("tvl", 0),
                                    "collected_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                                }},
                                upsert=True,
                            ))
                        if ops:
                            r = await db[self.tvl_collection].bulk_write(ops, ordered=False)
                            results["tvl"] = r.upserted_count + r.modified_count
                            logger.info(f"DefiLlama TVL: {r.upserted_count} new, {r.modified_count} updated")
            except Exception as e:
                logger.error(f"DefiLlama TVL error: {e}")

        return results
