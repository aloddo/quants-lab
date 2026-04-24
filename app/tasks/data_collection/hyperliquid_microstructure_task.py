"""
Hyperliquid 1-second microstructure collection task.

Captures:
  - recentTrades
  - l2Book

Persists:
  - MongoDB (deduplicated upserts)
    * hyperliquid_recent_trades_1s
    * hyperliquid_l2_snapshots_1s
  - Parquet chunk files under app/data/cache/hyperliquid_micro_1s/chunks

Designed to run every minute via YAML DAG, collecting ~50 seconds of 1s ticks.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import aiohttp
import pandas as pd
from pymongo import UpdateOne

from app.tasks.notifying_task import NotifyingTaskMixin
from core.data_paths import data_paths
from core.tasks import BaseTask, TaskContext

logger = logging.getLogger(__name__)

HL_API_URL = "https://api.hyperliquid.xyz/info"

HL_TO_BYBIT_PREFIX = {
    "kPEPE": "1000PEPE",
    "kBONK": "1000BONK",
    "kFLOKI": "1000FLOKI",
    "kSHIB": "1000SHIB",
    "kLUNC": "1000LUNC",
    "kXEC": "1000XEC",
    "kSATS": "1000SATS",
}
BYBIT_TO_HL_PREFIX = {v: k for k, v in HL_TO_BYBIT_PREFIX.items()}


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        if math.isfinite(f):
            return f
        return default
    except (TypeError, ValueError):
        return default


def _pair_to_hl_coin(pair: str) -> str:
    base = pair.replace("-USDT", "")
    return BYBIT_TO_HL_PREFIX.get(base, base)


def _hl_coin_to_pair(coin: str) -> str:
    base = HL_TO_BYBIT_PREFIX.get(coin, coin)
    return f"{base}-USDT"


class HyperliquidMicrostructureTask(NotifyingTaskMixin, BaseTask):
    """Collect 1s Hyperliquid trades + L2 snapshots and persist to Mongo/parquet."""

    def __init__(self, config):
        super().__init__(config)
        cfg = self.config.config
        self.pairs_cfg = cfg.get("pairs", "auto")
        self.max_pairs = int(cfg.get("max_pairs", 4))
        self.restrict_to_bybit_pairs = bool(cfg.get("restrict_to_bybit_pairs", True))
        self.bybit_collection = cfg.get("bybit_collection", "bybit_funding_rates")

        self.duration_seconds = int(cfg.get("duration_seconds", 50))
        self.poll_seconds = float(cfg.get("poll_seconds", 1.0))
        self.max_levels = int(cfg.get("max_levels", 20))

        self.write_mongo = bool(cfg.get("write_mongo", True))
        self.write_parquet = bool(cfg.get("write_parquet", True))
        self.book_collection = cfg.get("book_collection", "hyperliquid_l2_snapshots_1s")
        self.trades_collection = cfg.get("trades_collection", "hyperliquid_recent_trades_1s")
        self.out_dir = Path(cfg.get("out_dir", str(data_paths.cache_dir / "hyperliquid_micro_1s")))

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if self.write_mongo and not self.mongodb_client:
            raise RuntimeError("MongoDB connection required for HyperliquidMicrostructureTask")
        self.out_dir.mkdir(parents=True, exist_ok=True)

        if self.write_mongo:
            db = self.mongodb_client.get_database()
            await db[self.book_collection].create_index([("pair", 1), ("timestamp_utc", 1)], unique=True)
            await db[self.trades_collection].create_index([("pair", 1), ("tid", 1), ("time", 1)], unique=True)

    async def _hl_post(self, session: aiohttp.ClientSession, payload: dict, retries: int = 4) -> Any:
        last_err: Exception | None = None
        for i in range(retries):
            try:
                async with session.post(HL_API_URL, json=payload) as resp:
                    text = await resp.text()
                    if resp.status == 200:
                        return json.loads(text)
                    last_err = RuntimeError(f"HTTP {resp.status}: {text[:200]}")
            except Exception as e:  # noqa: BLE001
                last_err = e
            await asyncio.sleep(min(1.5, 0.2 * (2 ** i)))
        raise RuntimeError(f"HL request failed ({payload.get('type')}): {last_err}")

    async def _get_bybit_pairs(self) -> set[str]:
        if not self.mongodb_client:
            return set()
        db = self.mongodb_client.get_database()
        return set(await db[self.bybit_collection].distinct("pair"))

    async def _fetch_meta(self, session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
        data = await self._hl_post(session, {"type": "metaAndAssetCtxs"})
        if not isinstance(data, list) or len(data) < 2:
            return []
        meta = data[0] or {}
        ctxs = data[1] or []
        uni = meta.get("universe", [])

        out: List[Dict[str, Any]] = []
        for i, asset in enumerate(uni):
            coin = asset.get("name")
            if not coin:
                continue
            ctx = ctxs[i] if i < len(ctxs) and isinstance(ctxs[i], dict) else {}
            out.append(
                {
                    "coin": coin,
                    "pair": _hl_coin_to_pair(coin),
                    "day_ntl_vlm": _to_float(ctx.get("dayNtlVlm"), 0.0),
                    "is_delisted": bool(asset.get("isDelisted", False)),
                }
            )
        return out

    async def _resolve_pairs(self, session: aiohttp.ClientSession) -> List[Tuple[str, str]]:
        if isinstance(self.pairs_cfg, list):
            pairs = [str(p).upper() for p in self.pairs_cfg if str(p).strip()]
            return [(pair, _pair_to_hl_coin(pair)) for pair in pairs]
        if isinstance(self.pairs_cfg, str) and self.pairs_cfg.strip().lower() != "auto":
            pairs = [x.strip().upper() for x in self.pairs_cfg.split(",") if x.strip()]
            return [(pair, _pair_to_hl_coin(pair)) for pair in pairs]

        meta = await self._fetch_meta(session)
        meta = [m for m in meta if not m["is_delisted"]]
        if self.restrict_to_bybit_pairs:
            bybit_pairs = await self._get_bybit_pairs()
            meta = [m for m in meta if m["pair"] in bybit_pairs]
        meta = sorted(meta, key=lambda x: x["day_ntl_vlm"], reverse=True)
        if self.max_pairs > 0:
            meta = meta[: self.max_pairs]
        return [(m["pair"], m["coin"]) for m in meta]

    def _compute_book_features(self, levels: list) -> Dict[str, Any]:
        bids = levels[0][: self.max_levels] if len(levels) > 0 else []
        asks = levels[1][: self.max_levels] if len(levels) > 1 else []

        best_bid = _to_float(bids[0]["px"]) if bids else 0.0
        best_ask = _to_float(asks[0]["px"]) if asks else 0.0
        mid = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else 0.0
        spread_bps = ((best_ask - best_bid) / mid) * 1e4 if mid > 0 else 0.0

        bid_sz = float(sum(_to_float(x.get("sz")) for x in bids))
        ask_sz = float(sum(_to_float(x.get("sz")) for x in asks))
        denom = bid_sz + ask_sz
        imbalance = (bid_sz - ask_sz) / denom if denom > 0 else 0.0

        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_px": mid,
            "spread_bps": spread_bps,
            "bid_sz_topn": bid_sz,
            "ask_sz_topn": ask_sz,
            "imbalance_topn": imbalance,
            "levels_bid_json": json.dumps(bids),
            "levels_ask_json": json.dumps(asks),
        }

    async def _fetch_pair_tick(
        self, session: aiohttp.ClientSession, pair: str, coin: str
    ) -> Tuple[Dict[str, Any] | None, List[Dict[str, Any]]]:
        trades_payload = {"type": "recentTrades", "coin": coin}
        book_payload = {"type": "l2Book", "coin": coin}
        trades_raw, book_raw = await asyncio.gather(
            self._hl_post(session, trades_payload),
            self._hl_post(session, book_payload),
        )

        now_ms = int(time.time() * 1000)
        book_row = None
        if isinstance(book_raw, dict) and "levels" in book_raw:
            features = self._compute_book_features(book_raw.get("levels", []))
            book_row = {
                "pair": pair,
                "coin": coin,
                "timestamp_utc": int(book_raw.get("time", now_ms)),
                "recorded_at": now_ms,
                **features,
            }

        trade_rows: List[Dict[str, Any]] = []
        if isinstance(trades_raw, list):
            for t in trades_raw:
                try:
                    trade_rows.append(
                        {
                            "pair": pair,
                            "coin": coin,
                            "time": int(t.get("time", now_ms)),
                            "tid": int(t.get("tid", 0)),
                            "side": str(t.get("side", "")),
                            "px": _to_float(t.get("px")),
                            "sz": _to_float(t.get("sz")),
                            "hash": str(t.get("hash", "")),
                            "recorded_at": now_ms,
                        }
                    )
                except (TypeError, ValueError):
                    continue
        return book_row, trade_rows

    async def _upsert_mongo(
        self,
        books_by_pair: Dict[str, List[Dict[str, Any]]],
        trades_by_pair: Dict[str, List[Dict[str, Any]]],
    ) -> Dict[str, int]:
        stats = {"mongo_book_upserts": 0, "mongo_trade_upserts": 0}
        if not self.write_mongo or not self.mongodb_client:
            return stats

        db = self.mongodb_client.get_database()

        for rows in books_by_pair.values():
            if not rows:
                continue
            ops = [
                UpdateOne(
                    {"pair": r["pair"], "timestamp_utc": int(r["timestamp_utc"])},
                    {"$set": r},
                    upsert=True,
                )
                for r in rows
            ]
            res = await db[self.book_collection].bulk_write(ops, ordered=False)
            stats["mongo_book_upserts"] += int((res.upserted_count or 0) + (res.modified_count or 0))

        for rows in trades_by_pair.values():
            if not rows:
                continue
            ops = [
                UpdateOne(
                    {"pair": r["pair"], "tid": int(r["tid"]), "time": int(r["time"])},
                    {"$set": r},
                    upsert=True,
                )
                for r in rows
            ]
            res = await db[self.trades_collection].bulk_write(ops, ordered=False)
            stats["mongo_trade_upserts"] += int((res.upserted_count or 0) + (res.modified_count or 0))

        return stats

    def _write_parquet_chunks(
        self,
        books_by_pair: Dict[str, List[Dict[str, Any]]],
        trades_by_pair: Dict[str, List[Dict[str, Any]]],
        stamp: str,
    ) -> Dict[str, int]:
        stats = {"parquet_book_rows": 0, "parquet_trade_rows": 0}
        if not self.write_parquet:
            return stats

        day = datetime.now(timezone.utc).strftime("%Y%m%d")
        chunk_dir = self.out_dir / "chunks" / day
        chunk_dir.mkdir(parents=True, exist_ok=True)

        for pair, rows in books_by_pair.items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            path = chunk_dir / f"l2|{pair}|{stamp}.parquet"
            df.to_parquet(path, index=False)
            stats["parquet_book_rows"] += int(len(df))

        for pair, rows in trades_by_pair.items():
            if not rows:
                continue
            df = pd.DataFrame(rows)
            path = chunk_dir / f"trades|{pair}|{stamp}.parquet"
            df.to_parquet(path, index=False)
            stats["parquet_trade_rows"] += int(len(df))

        return stats

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        end_time = time.time() + max(1, self.duration_seconds)
        stats: Dict[str, Any] = {
            "pairs": 0,
            "polls": 0,
            "book_rows": 0,
            "trade_rows": 0,
            "errors": 0,
        }

        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "quants-lab/1.0"}) as session:
            pair_coin = await self._resolve_pairs(session)
            if not pair_coin:
                logger.warning("HyperliquidMicrostructureTask: no pairs resolved")
                return {"status": "completed", "stats": stats}

            stats["pairs"] = len(pair_coin)
            books_by_pair: Dict[str, List[Dict[str, Any]]] = {p: [] for p, _ in pair_coin}
            trades_by_pair: Dict[str, List[Dict[str, Any]]] = {p: [] for p, _ in pair_coin}

            while time.time() < end_time:
                tick_start = time.time()
                tasks = [
                    self._fetch_pair_tick(session=session, pair=pair, coin=coin)
                    for pair, coin in pair_coin
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for (pair, _coin), res in zip(pair_coin, results):
                    if isinstance(res, Exception):
                        stats["errors"] += 1
                        continue
                    book_row, trade_rows = res
                    if book_row is not None:
                        books_by_pair[pair].append(book_row)
                        stats["book_rows"] += 1
                    if trade_rows:
                        trades_by_pair[pair].extend(trade_rows)
                        stats["trade_rows"] += len(trade_rows)

                stats["polls"] += 1
                elapsed = time.time() - tick_start
                await asyncio.sleep(max(0.0, self.poll_seconds - elapsed))

            stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            mongo_stats = await self._upsert_mongo(books_by_pair, trades_by_pair)
            pq_stats = self._write_parquet_chunks(books_by_pair, trades_by_pair, stamp=stamp)

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        out = {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "duration_seconds": duration,
            "stats": {
                **stats,
                **mongo_stats,
                **pq_stats,
                "out_dir": str(self.out_dir),
            },
        }
        logger.info(
            "HyperliquidMicrostructureTask completed in %.1fs: pairs=%s polls=%s books=%s trades=%s errors=%s",
            duration,
            stats["pairs"],
            stats["polls"],
            stats["book_rows"],
            stats["trade_rows"],
            stats["errors"],
        )
        return out

    async def on_success(self, context: TaskContext, result) -> None:
        s = result.result_data.get("stats", {})
        logger.info(
            "HyperliquidMicrostructureTask: %s pairs, %s polls, books=%s trades=%s",
            s.get("pairs", 0),
            s.get("polls", 0),
            s.get("book_rows", 0),
            s.get("trade_rows", 0),
        )

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error("HyperliquidMicrostructureTask failed: %s", result.error_message)


async def main():
    from core.tasks.base import ScheduleConfig, TaskConfig

    cfg = TaskConfig(
        name="hyperliquid_microstructure_test",
        enabled=True,
        task_class="app.tasks.data_collection.hyperliquid_microstructure_task.HyperliquidMicrostructureTask",
        schedule=ScheduleConfig(type="frequency", frequency_hours=1 / 60),
        timeout_seconds=58,
        config={
            "pairs": "BTC-USDT,ETH-USDT",
            "duration_seconds": 8,
            "poll_seconds": 1.0,
            "max_levels": 20,
            "write_mongo": True,
            "write_parquet": True,
        },
    )
    task = HyperliquidMicrostructureTask(cfg)
    result = await task.run()
    print(f"status={result.status}")
    if result.result_data:
        print(result.result_data.get("stats", {}))
    if result.error_message:
        print(result.error_message)


if __name__ == "__main__":
    asyncio.run(main())
