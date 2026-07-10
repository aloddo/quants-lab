"""
BinanceTopTraderTask — Collects Binance top trader long/short ratio data.

Fetches topLongShortPositionRatio and globalLongShortAccountRatio from
Binance Futures API. Stores to MongoDB for X14 Crowd Fade V2 validation
and future strategy development.

Binance API limits: 500 records per request, max 30 days history per period.
We fetch at 1h resolution for all target pairs every run.

Collections:
  binance_top_trader_ls — top trader positioning (used by X14)
  binance_global_ls — global account ratio
"""
import logging
import time
from datetime import datetime, timezone
from typing import Any

import requests
from pymongo import MongoClient, UpdateOne

from core.tasks import BaseTask

logger = logging.getLogger(__name__)

BINANCE_FUTURES_BASE = "https://fapi.binance.com"

# X14 WF-PASS pairs + some extras for future research
DEFAULT_PAIRS = [
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT",
    "ADAUSDT", "AVAXUSDT", "LINKUSDT", "SUIUSDT", "APTUSDT",
    "NEARUSDT", "AAVEUSDT", "BNBUSDT", "ARBUSDT", "OPUSDT",
    "GALAUSDT", "ONTUSDT", "WLDUSDT", "SEIUSDT", "LTCUSDT",
    "DOTUSDT", "UNIUSDT", "ALGOUSDT", "ATOMUSDT", "BCHUSDT",
    "CRVUSDT", "FILUSDT", "ETCUSDT", "KASUSDT", "TAOUSDT",
    "DRIFTUSDT", "TRUMPUSDT", "KERNLUSDT", "XAUTUSDT", "WIFUSDT",
]


class BinanceTopTraderTask(BaseTask):
    """Fetch and store Binance top trader + global L/S ratios."""

    def __init__(self, config: dict[str, Any] | None = None, **kwargs):
        super().__init__(config=config, **kwargs)
        self.pairs = (config or {}).get("pairs", DEFAULT_PAIRS)
        self.period = (config or {}).get("period", "1h")
        self.limit = (config or {}).get("limit", 50)  # Last 50 records per call

    async def execute(self):
        mongo_uri = self.config.get("mongo_uri", "mongodb://localhost:27017")
        mongo_db = self.config.get("mongo_database", "quants_lab")
        client = MongoClient(mongo_uri)
        db = client[mongo_db]

        # Ensure indexes
        db.binance_top_trader_ls.create_index(
            [("pair", 1), ("timestamp_utc", 1)], unique=True
        )
        db.binance_global_ls.create_index(
            [("pair", 1), ("timestamp_utc", 1)], unique=True
        )

        total_top = 0
        total_global = 0

        for symbol in self.pairs:
            pair = symbol.replace("USDT", "-USDT")

            # 1. Top trader L/S position ratio
            try:
                url = (
                    f"{BINANCE_FUTURES_BASE}/futures/data/topLongShortPositionRatio"
                    f"?symbol={symbol}&period={self.period}&limit={self.limit}"
                )
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    if data:
                        ops = []
                        for d in data:
                            doc = {
                                "pair": pair,
                                "timestamp_utc": d["timestamp"],
                                "long_short_ratio": float(d.get("longShortRatio", 0)),
                                "long_account": float(d.get("longAccount", 0)),
                                "short_account": float(d.get("shortAccount", 0)),
                                "recorded_at": int(time.time() * 1000),
                            }
                            ops.append(UpdateOne(
                                {"pair": pair, "timestamp_utc": d["timestamp"]},
                                {"$set": doc},
                                upsert=True,
                            ))
                        if ops:
                            result = db.binance_top_trader_ls.bulk_write(ops, ordered=False)
                            total_top += result.upserted_count + result.modified_count
            except Exception as e:
                logger.warning(f"Top trader fetch failed for {symbol}: {e}")

            # 2. Global L/S account ratio
            try:
                url2 = (
                    f"{BINANCE_FUTURES_BASE}/futures/data/globalLongShortAccountRatio"
                    f"?symbol={symbol}&period={self.period}&limit={self.limit}"
                )
                resp2 = requests.get(url2, timeout=10)
                if resp2.status_code == 200:
                    data2 = resp2.json()
                    if data2:
                        ops2 = []
                        for d in data2:
                            doc2 = {
                                "pair": pair,
                                "timestamp_utc": d["timestamp"],
                                "long_short_ratio": float(d.get("longShortRatio", 0)),
                                "long_account": float(d.get("longAccount", 0)),
                                "short_account": float(d.get("shortAccount", 0)),
                                "recorded_at": int(time.time() * 1000),
                            }
                            ops2.append(UpdateOne(
                                {"pair": pair, "timestamp_utc": d["timestamp"]},
                                {"$set": doc2},
                                upsert=True,
                            ))
                        if ops2:
                            result2 = db.binance_global_ls.bulk_write(ops2, ordered=False)
                            total_global += result2.upserted_count + result2.modified_count
            except Exception as e:
                logger.warning(f"Global L/S fetch failed for {symbol}: {e}")

            time.sleep(0.15)  # Rate limit

        logger.info(
            f"BinanceTopTrader: stored {total_top} top trader + "
            f"{total_global} global L/S records for {len(self.pairs)} pairs"
        )

        client.close()
        return {
            "top_trader_records": total_top,
            "global_ls_records": total_global,
            "pairs": len(self.pairs),
        }
