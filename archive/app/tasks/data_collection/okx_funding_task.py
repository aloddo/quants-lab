"""
OKXFundingTask -- Collects OKX perpetual funding rate history.

Fetches funding rate history from OKX public API (no auth required).
Stores to MongoDB for cross-exchange funding divergence analysis.

OKX API details:
  - Endpoint: GET /api/v5/public/funding-rate-history
  - Auth: None (public endpoint)
  - Rate limit: ~6 req/s (use 0.2s delay)
  - Interval: 8h funding settlements
  - History: ~92 days per pair (hard API limit)
  - Pagination: 'after' param for older records (fundingTime ms)

Collections:
  okx_funding_rates -- OKX perpetual funding rates (indexed by pair + timestamp_utc)

Cross-exchange divergence thesis:
  OKX-Bybit funding correlation is only 0.51 (BTC). When OKX funding
  diverges high vs Bybit (z>2.0), BTC tends to fall -94 bps in 8h.
  Need 270+ days of data for walk-forward validation.
"""
import logging
import time
from typing import Any

import requests
from pymongo import MongoClient, UpdateOne

from core.tasks import BaseTask

logger = logging.getLogger(__name__)

OKX_BASE = "https://www.okx.com"

# All pairs available on OKX USDT-margined perps (matching our Bybit universe)
DEFAULT_PAIRS = [
    "BTC-USDT-SWAP", "ETH-USDT-SWAP", "SOL-USDT-SWAP", "XRP-USDT-SWAP",
    "DOGE-USDT-SWAP", "ADA-USDT-SWAP", "AVAX-USDT-SWAP", "LINK-USDT-SWAP",
    "SUI-USDT-SWAP", "APT-USDT-SWAP", "NEAR-USDT-SWAP", "AAVE-USDT-SWAP",
    "ARB-USDT-SWAP", "OP-USDT-SWAP", "DOT-USDT-SWAP", "UNI-USDT-SWAP",
    "ALGO-USDT-SWAP", "LTC-USDT-SWAP", "BCH-USDT-SWAP", "GALA-USDT-SWAP",
    "ONT-USDT-SWAP", "WLD-USDT-SWAP", "WIF-USDT-SWAP", "TAO-USDT-SWAP",
    "SEI-USDT-SWAP", "BNB-USDT-SWAP", "CRV-USDT-SWAP",
]

COLLECTION_NAME = "okx_funding_rates"


class OKXFundingTask(BaseTask):
    """Fetch and store OKX funding rate history for cross-exchange analysis."""

    def __init__(self, config: dict[str, Any] | None = None, **kwargs):
        super().__init__(config=config, **kwargs)
        self.pairs = (config or {}).get("pairs", DEFAULT_PAIRS)
        self.limit = (config or {}).get("limit", 100)  # Max 100 per request
        self.collection = (config or {}).get("collection_name", COLLECTION_NAME)

    async def execute(self):
        mongo_uri = self.config.get("mongo_uri", "mongodb://localhost:27017")
        mongo_db = self.config.get("mongo_database", "quants_lab")
        client = MongoClient(mongo_uri)
        db = client[mongo_db]

        # Ensure dedup index
        db[self.collection].create_index(
            [("pair", 1), ("timestamp_utc", 1)], unique=True
        )

        total_records = 0
        errors = 0
        session = requests.Session()
        session.headers.update({"User-Agent": "QuantsLab/1.0"})

        for inst_id in self.pairs:
            # Convert OKX instrument format to our standard pair format
            # BTC-USDT-SWAP -> BTC-USDT
            pair = inst_id.replace("-SWAP", "")

            try:
                # Fetch latest funding rates (paginate to get full history)
                all_records = []
                after = None

                # Page through up to 3 requests per pair (300 records = ~100 days at 8h)
                for page in range(3):
                    url = (
                        f"{OKX_BASE}/api/v5/public/funding-rate-history"
                        f"?instId={inst_id}&limit={self.limit}"
                    )
                    if after:
                        url += f"&after={after}"

                    resp = session.get(url, timeout=10)
                    if resp.status_code != 200:
                        logger.warning(
                            f"OKX funding {inst_id}: HTTP {resp.status_code}"
                        )
                        break

                    data = resp.json()
                    if data.get("code") != "0":
                        logger.warning(
                            f"OKX funding {inst_id}: API error {data.get('msg')}"
                        )
                        break

                    records = data.get("data", [])
                    if not records:
                        break

                    all_records.extend(records)

                    # Set pagination cursor (oldest record's fundingTime)
                    after = records[-1].get("fundingTime")

                    time.sleep(0.2)  # Rate limit between pages

                # Build bulk upserts
                if all_records:
                    ops = []
                    for r in all_records:
                        funding_time = int(r["fundingTime"])
                        doc = {
                            "pair": pair,
                            "timestamp_utc": funding_time,
                            "funding_rate": float(r.get("fundingRate", 0)),
                            "realized_rate": float(r.get("realizedRate", 0)),
                            "inst_id": inst_id,
                            "method": r.get("method", ""),
                            "recorded_at": int(time.time() * 1000),
                        }
                        ops.append(UpdateOne(
                            {"pair": pair, "timestamp_utc": funding_time},
                            {"$set": doc},
                            upsert=True,
                        ))

                    if ops:
                        result = db[self.collection].bulk_write(ops, ordered=False)
                        upserted = result.upserted_count + result.modified_count
                        total_records += upserted

            except Exception as e:
                logger.warning(f"OKX funding fetch failed for {inst_id}: {e}")
                errors += 1

            time.sleep(0.2)  # Rate limit between pairs

        logger.info(
            f"OKXFunding: stored {total_records} records for "
            f"{len(self.pairs)} pairs ({errors} errors)"
        )

        client.close()
        return {
            "records": total_records,
            "pairs": len(self.pairs),
            "errors": errors,
        }
