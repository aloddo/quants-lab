#!/usr/bin/env python3
"""
Collect Hyperliquid 1s microstructure snapshots (recent trades + L2 book).

Notes:
  - Hyperliquid public API does not provide deep historical 1s bars.
  - This script creates your own forward history at 1s cadence.

Writes:
  - MongoDB:
      * hyperliquid_recent_trades_1s
      * hyperliquid_l2_snapshots_1s
  - Parquet:
      * app/data/cache/hyperliquid_micro_1s/trades|<PAIR>|YYYYMMDD.parquet
      * app/data/cache/hyperliquid_micro_1s/l2|<PAIR>|YYYYMMDD.parquet
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd
from pymongo import MongoClient, UpdateOne

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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Collect Hyperliquid 1s microstructure snapshots")
    p.add_argument("--pairs", default="BTC-USDT,ETH-USDT,SOL-USDT")
    p.add_argument("--duration-seconds", type=int, default=1800)
    p.add_argument("--poll-seconds", type=float, default=1.0)
    p.add_argument("--max-levels", type=int, default=20)
    p.add_argument("--flush-every", type=int, default=60, help="Flush every N polls")
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab"))
    p.add_argument("--mongo-db", default=os.getenv("MONGO_DATABASE", "quants_lab"))
    p.add_argument("--write-mongo", action="store_true", default=True)
    p.add_argument("--no-write-mongo", dest="write_mongo", action="store_false")
    p.add_argument("--trades-collection", default="hyperliquid_recent_trades_1s")
    p.add_argument("--book-collection", default="hyperliquid_l2_snapshots_1s")
    p.add_argument("--out-dir", default="app/data/cache/hyperliquid_micro_1s")
    return p.parse_args()


def pair_to_coin(pair: str) -> str:
    base = pair.replace("-USDT", "")
    return BYBIT_TO_HL_PREFIX.get(base, base)


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


async def hl_post(session: aiohttp.ClientSession, payload: dict, retries: int = 4) -> Any:
    last_err = None
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


def compute_book_features(levels: list, max_levels: int) -> dict:
    bids = levels[0][:max_levels] if len(levels) > 0 else []
    asks = levels[1][:max_levels] if len(levels) > 1 else []

    best_bid = _to_float(bids[0]["px"]) if bids else 0.0
    best_ask = _to_float(asks[0]["px"]) if asks else 0.0
    mid = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else 0.0
    spread_bps = ((best_ask - best_bid) / mid) * 1e4 if mid > 0 else 0.0

    bid_sz = sum(_to_float(x.get("sz")) for x in bids)
    ask_sz = sum(_to_float(x.get("sz")) for x in asks)
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


async def fetch_pair_tick(session: aiohttp.ClientSession, pair: str, coin: str, max_levels: int) -> tuple[dict | None, list[dict]]:
    trade_payload = {"type": "recentTrades", "coin": coin}
    book_payload = {"type": "l2Book", "coin": coin}
    trades_raw, book_raw = await asyncio.gather(
        hl_post(session, trade_payload),
        hl_post(session, book_payload),
    )

    now_ms = int(time.time() * 1000)

    book_row = None
    if isinstance(book_raw, dict) and "levels" in book_raw:
        feat = compute_book_features(book_raw.get("levels", []), max_levels=max_levels)
        book_row = {
            "pair": pair,
            "coin": coin,
            "timestamp_utc": int(book_raw.get("time", now_ms)),
            "recorded_at": now_ms,
            **feat,
        }

    trades_rows: list[dict] = []
    if isinstance(trades_raw, list):
        for t in trades_raw:
            try:
                trades_rows.append(
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
    return book_row, trades_rows


def _write_append_parquet(path: Path, df_new: pd.DataFrame, dedup_cols: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        df_old = pd.read_parquet(path)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new.copy()
    df = df.drop_duplicates(subset=dedup_cols, keep="last").sort_values(dedup_cols).reset_index(drop=True)
    df.to_parquet(path, index=False)
    return int(len(df))


def flush_storage(
    out_dir: Path,
    books_by_pair: dict[str, list[dict]],
    trades_by_pair: dict[str, list[dict]],
    db,
    write_mongo: bool,
    trades_collection: str,
    book_collection: str,
) -> dict:
    stats = {"book_rows": 0, "trade_rows": 0, "mongo_book_upserts": 0, "mongo_trade_upserts": 0}
    day = datetime.now(timezone.utc).strftime("%Y%m%d")

    for pair, rows in books_by_pair.items():
        if not rows:
            continue
        df = pd.DataFrame(rows)
        p = out_dir / f"l2|{pair}|{day}.parquet"
        _write_append_parquet(p, df, dedup_cols=["pair", "timestamp_utc"])
        stats["book_rows"] += len(df)

        if write_mongo and db is not None:
            ops = []
            for r in rows:
                ops.append(
                    UpdateOne(
                        {"pair": r["pair"], "timestamp_utc": int(r["timestamp_utc"])},
                        {"$set": r},
                        upsert=True,
                    )
                )
            if ops:
                res = db[book_collection].bulk_write(ops, ordered=False)
                stats["mongo_book_upserts"] += int((res.upserted_count or 0) + (res.modified_count or 0))

    for pair, rows in trades_by_pair.items():
        if not rows:
            continue
        df = pd.DataFrame(rows)
        p = out_dir / f"trades|{pair}|{day}.parquet"
        _write_append_parquet(p, df, dedup_cols=["pair", "tid", "time"])
        stats["trade_rows"] += len(df)

        if write_mongo and db is not None:
            ops = []
            for r in rows:
                ops.append(
                    UpdateOne(
                        {"pair": r["pair"], "tid": int(r["tid"]), "time": int(r["time"])},
                        {"$set": r},
                        upsert=True,
                    )
                )
            if ops:
                res = db[trades_collection].bulk_write(ops, ordered=False)
                stats["mongo_trade_upserts"] += int((res.upserted_count or 0) + (res.modified_count or 0))

    return stats


async def main() -> None:
    args = parse_args()
    pairs = [p.strip().upper() for p in args.pairs.split(",") if p.strip()]
    pair_coin = {p: pair_to_coin(p) for p in pairs}
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    mongo = MongoClient(args.mongo_uri) if args.write_mongo else None
    db = mongo[args.mongo_db] if mongo is not None else None
    if db is not None:
        db[args.book_collection].create_index([("pair", 1), ("timestamp_utc", 1)], unique=True)
        db[args.trades_collection].create_index([("pair", 1), ("tid", 1), ("time", 1)], unique=True)

    end_time = time.time() + args.duration_seconds
    poll = 0
    total = {"book_rows": 0, "trade_rows": 0, "mongo_book_upserts": 0, "mongo_trade_upserts": 0}
    books_by_pair: dict[str, list[dict]] = {p: [] for p in pairs}
    trades_by_pair: dict[str, list[dict]] = {p: [] for p in pairs}

    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "quants-lab/1.0"}) as session:
        print(
            f"Collecting Hyperliquid microstructure at ~1s: pairs={pairs}, "
            f"duration={args.duration_seconds}s, poll={args.poll_seconds}s"
        )
        while time.time() < end_time:
            poll += 1
            tick_start = time.time()

            tasks = [
                fetch_pair_tick(session, pair=p, coin=pair_coin[p], max_levels=args.max_levels)
                for p in pairs
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for p, res in zip(pairs, results):
                if isinstance(res, Exception):
                    continue
                book_row, trade_rows = res
                if book_row is not None:
                    books_by_pair[p].append(book_row)
                if trade_rows:
                    trades_by_pair[p].extend(trade_rows)

            if poll % args.flush_every == 0:
                s = flush_storage(
                    out_dir=out_dir,
                    books_by_pair=books_by_pair,
                    trades_by_pair=trades_by_pair,
                    db=db,
                    write_mongo=args.write_mongo,
                    trades_collection=args.trades_collection,
                    book_collection=args.book_collection,
                )
                for k, v in s.items():
                    total[k] += v
                books_by_pair = {p: [] for p in pairs}
                trades_by_pair = {p: [] for p in pairs}
                print(
                    f"  poll={poll} flushed book_rows={s['book_rows']} trade_rows={s['trade_rows']} "
                    f"mongo_book={s['mongo_book_upserts']} mongo_trade={s['mongo_trade_upserts']}"
                )

            elapsed = time.time() - tick_start
            await asyncio.sleep(max(0.0, args.poll_seconds - elapsed))

        # Final flush
        s = flush_storage(
            out_dir=out_dir,
            books_by_pair=books_by_pair,
            trades_by_pair=trades_by_pair,
            db=db,
            write_mongo=args.write_mongo,
            trades_collection=args.trades_collection,
            book_collection=args.book_collection,
        )
        for k, v in s.items():
            total[k] += v

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    meta = {
        "timestamp_utc": stamp,
        "pairs": pairs,
        "duration_seconds": args.duration_seconds,
        "poll_seconds": args.poll_seconds,
        "max_levels": args.max_levels,
        "totals": total,
        "out_dir": str(out_dir),
        "mongo_db": args.mongo_db if args.write_mongo else None,
    }
    meta_path = out_dir / f"{stamp}_micro_1s_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))

    print("\n=== 1s Collection Summary ===")
    print(json.dumps(meta, indent=2))

    if mongo is not None:
        mongo.close()


if __name__ == "__main__":
    asyncio.run(main())
