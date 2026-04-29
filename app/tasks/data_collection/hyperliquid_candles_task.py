"""
Hyperliquid candle collection task (direct API, not CLOB).

Fetches 1h candles from Hyperliquid public API and writes to MongoDB
(hyperliquid_candles_1h) + parquet. Incremental: only fetches since last
stored timestamp per pair.

Why this exists: hyperliquid_perpetual is in HB's EXCLUDED_CONNECTORS,
so the standard CandlesDownloaderTask silently skips it. This task calls
the HL REST API directly, same as backfill_hyperliquid_history.py but
as a proper BaseTask for the pipeline DAG.

Schedule: hourly via YAML DAG (runs after candles_downloader_bybit).
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import aiohttp
import pandas as pd
from pymongo import UpdateOne

from core.data_paths import data_paths
from core.tasks import BaseTask, TaskContext

logger = logging.getLogger(__name__)

from app.tasks.notifying_task import NotifyingTaskMixin

HL_API_URL = "https://api.hyperliquid.xyz/info"

# Hyperliquid coin name -> Bybit-style base symbol
_HL_TO_BYBIT_PREFIX = {
    "kPEPE": "1000PEPE",
    "kBONK": "1000BONK",
    "kFLOKI": "1000FLOKI",
    "kSHIB": "1000SHIB",
    "kLUNC": "1000LUNC",
    "kXEC": "1000XEC",
    "kSATS": "1000SATS",
}
_BYBIT_TO_HL_PREFIX = {v: k for k, v in _HL_TO_BYBIT_PREFIX.items()}


def _hl_coin_to_pair(coin: str) -> str:
    base = _HL_TO_BYBIT_PREFIX.get(coin, coin)
    return f"{base}-USDT"


def _pair_to_hl_coin(pair: str) -> str:
    base = pair.replace("-USDT", "")
    return _BYBIT_TO_HL_PREFIX.get(base, base)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


class HyperliquidCandlesTask(NotifyingTaskMixin, BaseTask):
    """Fetch HL 1h candles from REST API -> MongoDB + parquet.

    Config (pipeline YAML):
        collection_name: MongoDB collection (default: hyperliquid_candles_1h)
        interval: candle interval (default: 1h)
        max_pairs: max pairs to fetch (default: 30, by volume)
        lookback_hours: how far back to fetch if no prior data (default: 48)
        bybit_collection: Bybit funding collection for pair intersection
        write_parquet: also write parquet files (default: true)
    """

    def __init__(self, config):
        super().__init__(config)
        tc = self.config.config
        self.collection_name = tc.get("collection_name", "hyperliquid_candles_1h")
        self.interval = tc.get("interval", "1h")
        self.max_pairs = tc.get("max_pairs", 30)
        self.lookback_hours = tc.get("lookback_hours", 48)
        self.bybit_collection = tc.get("bybit_collection", "bybit_funding_rates")
        self.write_parquet = tc.get("write_parquet", True)
        self.priority_pairs = tc.get("priority_pairs", [])  # fetched first, before volume-ranked
        self._chunk_ms = 14 * 24 * 3600 * 1000  # 14-day fetch chunks

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB connection required for HyperliquidCandlesTask")
        db = self.mongodb_client.get_database()
        await db[self.collection_name].create_index(
            [("pair", 1), ("interval", 1), ("timestamp_utc", 1)], unique=True
        )

    async def _get_bybit_pairs(self) -> Set[str]:
        """Get distinct pairs from Bybit funding collection for intersection."""
        db = self.mongodb_client.get_database()
        pairs = await db[self.bybit_collection].distinct("pair")
        return set(pairs)

    async def _get_hl_universe(self, session: aiohttp.ClientSession) -> List[Dict]:
        """Get HL universe with volume, sorted by volume desc."""
        try:
            async with session.post(
                HL_API_URL, json={"type": "metaAndAssetCtxs"}, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    logger.error(f"HL meta failed: HTTP {resp.status}")
                    return []
                data = await resp.json()
        except Exception as e:
            logger.error(f"HL meta request failed: {e}")
            return []

        if not isinstance(data, list) or len(data) < 2:
            return []

        meta = data[0] or {}
        ctxs = data[1] or []
        universe = meta.get("universe", [])

        out = []
        for i, asset in enumerate(universe):
            coin = asset.get("name")
            if not coin or asset.get("isDelisted"):
                continue
            ctx = ctxs[i] if i < len(ctxs) and isinstance(ctxs[i], dict) else {}
            out.append({
                "coin": coin,
                "pair": _hl_coin_to_pair(coin),
                "day_ntl_vlm": _to_float(ctx.get("dayNtlVlm")),
            })
        return sorted(out, key=lambda x: x["day_ntl_vlm"], reverse=True)

    async def _get_last_timestamp(self, pair: str) -> Optional[int]:
        """Get last stored timestamp_utc (ms) for a pair."""
        db = self.mongodb_client.get_database()
        doc = await db[self.collection_name].find_one(
            {"pair": pair, "interval": self.interval},
            sort=[("timestamp_utc", -1)],
        )
        return doc["timestamp_utc"] if doc else None

    async def _fetch_candles(
        self, session: aiohttp.ClientSession, coin: str, start_ms: int, end_ms: int
    ) -> List[Dict]:
        """Fetch candles from HL API with chunking and retry."""
        rows = []
        cursor_end = end_ms
        loops = 0

        while cursor_end >= start_ms and loops < 100:
            loops += 1
            chunk_start = max(start_ms, cursor_end - self._chunk_ms + 1)

            payload = {
                "type": "candleSnapshot",
                "req": {
                    "coin": coin,
                    "interval": self.interval,
                    "startTime": int(chunk_start),
                    "endTime": int(cursor_end),
                },
            }

            for attempt in range(3):
                try:
                    async with session.post(
                        HL_API_URL, json=payload, timeout=aiohttp.ClientTimeout(total=30)
                    ) as resp:
                        if resp.status == 429:
                            await asyncio.sleep(2.0)
                            continue
                        if resp.status != 200:
                            logger.warning(f"HL candles {coin}: HTTP {resp.status}")
                            return rows
                        data = await resp.json()
                    break
                except Exception as e:
                    if attempt == 2:
                        logger.warning(f"HL candles {coin} request failed: {e}")
                        return rows
                    await asyncio.sleep(1.0)
            else:
                return rows

            if not isinstance(data, list) or not data:
                break

            min_ts = cursor_end
            for r in data:
                try:
                    ts_ms = int(r["t"])
                    if ts_ms < start_ms or ts_ms > end_ms:
                        continue
                    rows.append({
                        "timestamp_utc": ts_ms,
                        "open": _to_float(r.get("o")),
                        "high": _to_float(r.get("h")),
                        "low": _to_float(r.get("l")),
                        "close": _to_float(r.get("c")),
                        "volume": _to_float(r.get("v")),
                        "n_trades": int(r.get("n", 0)),
                    })
                    if ts_ms < min_ts:
                        min_ts = ts_ms
                except (TypeError, ValueError, KeyError):
                    continue

            new_cursor = min_ts - 1
            if new_cursor >= cursor_end:
                break
            cursor_end = new_cursor
            if min_ts <= start_ms:
                break

            await asyncio.sleep(0.12)  # rate limit

        return rows

    async def _upsert_candles(self, pair: str, coin: str, rows: List[Dict]) -> int:
        """Upsert candles to MongoDB. Returns count of new/modified docs."""
        if not rows:
            return 0

        db = self.mongodb_client.get_database()
        now_ms = int(time.time() * 1000)
        ops = []
        for r in rows:
            ops.append(UpdateOne(
                {"pair": pair, "interval": self.interval, "timestamp_utc": int(r["timestamp_utc"])},
                {"$set": {
                    "pair": pair,
                    "coin": coin,
                    "interval": self.interval,
                    "timestamp_utc": int(r["timestamp_utc"]),
                    "open": r["open"],
                    "high": r["high"],
                    "low": r["low"],
                    "close": r["close"],
                    "volume": r["volume"],
                    "n_trades": r["n_trades"],
                    "recorded_at": now_ms,
                }},
                upsert=True,
            ))

        result = await db[self.collection_name].bulk_write(ops, ordered=False)
        return (result.upserted_count or 0) + (result.modified_count or 0)

    def _write_parquet(self, pair: str, rows: List[Dict]) -> int:
        """Append candles to parquet file. Returns total row count."""
        if not rows:
            return 0

        candles_dir = data_paths.candles_dir
        path = candles_dir / f"hyperliquid_perpetual|{pair}|{self.interval}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)

        df_new = pd.DataFrame(rows)
        # Add timestamp in seconds (BacktestingEngine convention)
        df_new["timestamp"] = df_new["timestamp_utc"] // 1000

        if path.exists():
            df_old = pd.read_parquet(path)
            df = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df = df_new

        df = (
            df.drop_duplicates(subset=["timestamp"], keep="last")
            .sort_values("timestamp")
            .reset_index(drop=True)
        )
        df.to_parquet(path, index=False)
        return len(df)

    async def execute(self, context: TaskContext) -> Dict:
        """Main task execution."""
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        default_start = now_ms - self.lookback_hours * 3600 * 1000

        async with aiohttp.ClientSession() as session:
            # Get pair universe (intersect HL with Bybit)
            hl_universe = await self._get_hl_universe(session)
            if not hl_universe:
                return {"pairs": 0, "candles": 0, "error": "empty universe"}

            bybit_pairs = await self._get_bybit_pairs()
            eligible = [
                item for item in hl_universe
                if item["pair"] in bybit_pairs
            ]

            # Priority pairs first (X12 targets etc.), then volume-ranked
            priority_set = set(self.priority_pairs)
            priority_items = [item for item in eligible if item["pair"] in priority_set]
            rest_items = [item for item in eligible if item["pair"] not in priority_set]
            selected = (priority_items + rest_items)[:self.max_pairs]

            total_candles = 0
            total_upserts = 0
            errors = 0

            for item in selected:
                coin = item["coin"]
                pair = item["pair"]

                try:
                    last_ts = await self._get_last_timestamp(pair)
                    start_ms = (last_ts + 1) if last_ts else default_start

                    if now_ms - start_ms < 3600 * 1000:
                        # Less than 1 hour gap — skip (will be covered next run)
                        continue

                    rows = await self._fetch_candles(session, coin, start_ms, now_ms)
                    if not rows:
                        continue

                    n_upserts = await self._upsert_candles(pair, coin, rows)
                    total_candles += len(rows)
                    total_upserts += n_upserts

                    if self.write_parquet:
                        self._write_parquet(pair, rows)

                except Exception as e:
                    logger.warning(f"HL candles {pair} failed: {e}")
                    errors += 1

        result = {
            "pairs": len(selected),
            "candles": total_candles,
            "upserts": total_upserts,
            "errors": errors,
        }
        logger.info(f"HyperliquidCandlesTask completed: {result}")
        return result
