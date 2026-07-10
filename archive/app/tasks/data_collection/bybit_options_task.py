"""
Bybit options surface collection task.

Fetches BTC + ETH options chain from Bybit every 15 minutes.
Stores tickers with IV, greeks, OI, and volume in MongoDB.
Used for: iron condor strike selection, VRP monitoring, skew tracking.
"""
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from pybit.unified_trading import HTTP

from core.tasks import BaseTask, TaskContext
from app.tasks.notifying_task import NotifyingTaskMixin

logger = logging.getLogger(__name__)


class BybitOptionsSurfaceTask(NotifyingTaskMixin, BaseTask):
    """Fetch BTC + ETH options tickers from Bybit -> MongoDB."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.base_coins = task_config.get("base_coins", ["BTC", "ETH"])
        self.collection_name = task_config.get(
            "collection_name", "bybit_options_surface"
        )

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB connection required for BybitOptionsSurfaceTask")
        self.session = HTTP(testnet=False)  # Public API, no keys needed

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        t0 = time.time()
        db = self.mongodb_client.db
        collection = db[self.collection_name]
        total_inserted = 0
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        for base_coin in self.base_coins:
            try:
                result = self.session.get_tickers(category="option", baseCoin=base_coin)
                if result["retCode"] != 0:
                    logger.warning(f"Bybit options {base_coin}: {result['retMsg']}")
                    continue

                tickers = result["result"]["list"]
                if not tickers:
                    continue

                docs = []
                for t in tickers:
                    symbol = t.get("symbol", "")
                    # Parse symbol: BTC-22MAY26-82000-C-USDT
                    parts = symbol.split("-")
                    if len(parts) < 4:
                        continue

                    doc = {
                        "base_coin": base_coin,
                        "symbol": symbol,
                        "expiry": parts[1] if len(parts) > 1 else "",
                        "strike": float(parts[2]) if len(parts) > 2 else 0,
                        "option_type": "call" if "-C-" in symbol else "put",
                        "bid1_price": _safe_float(t.get("bid1Price")),
                        "ask1_price": _safe_float(t.get("ask1Price")),
                        "bid1_iv": _safe_float(t.get("bid1Iv")),
                        "ask1_iv": _safe_float(t.get("ask1Iv")),
                        "mark_iv": _safe_float(t.get("markIv")),
                        "mark_price": _safe_float(t.get("markPrice")),
                        "last_price": _safe_float(t.get("lastPrice")),
                        "underlying_price": _safe_float(t.get("underlyingPrice")),
                        "open_interest": _safe_float(t.get("openInterest")),
                        "volume_24h": _safe_float(t.get("volume24h")),
                        "turnover_24h": _safe_float(t.get("turnover24h")),
                        "total_volume": _safe_float(t.get("totalVolume")),
                        "total_turnover": _safe_float(t.get("totalTurnover")),
                        "delta": _safe_float(t.get("delta")),
                        "gamma": _safe_float(t.get("gamma")),
                        "vega": _safe_float(t.get("vega")),
                        "theta": _safe_float(t.get("theta")),
                        "change_24h": _safe_float(t.get("change24h")),
                        "high_price_24h": _safe_float(t.get("highPrice24h")),
                        "low_price_24h": _safe_float(t.get("lowPrice24h")),
                        "collected_at": now_ms,
                        "collected_utc": datetime.now(timezone.utc),
                    }
                    docs.append(doc)

                if docs:
                    await collection.insert_many(docs, ordered=False)
                    total_inserted += len(docs)
                    logger.info(
                        f"Bybit options {base_coin}: {len(docs)} contracts collected"
                    )

            except Exception as e:
                logger.error(f"Bybit options {base_coin} error: {e}")

        elapsed = time.time() - t0

        # Ensure index
        await collection.create_index(
            [("base_coin", 1), ("collected_at", -1)],
            background=True,
        )
        await collection.create_index(
            [("symbol", 1), ("collected_at", -1)],
            background=True,
        )

        return {
            "status": "success",
            "records_inserted": total_inserted,
            "elapsed_s": round(elapsed, 1),
        }


def _safe_float(val) -> float:
    """Safely convert to float, return 0.0 on failure."""
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0
