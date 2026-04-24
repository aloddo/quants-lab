#!/usr/bin/env python3
"""
Backfill Hyperliquid historical candles + funding and persist to MongoDB/parquet.

Writes:
  - MongoDB:
      * hyperliquid_candles
      * hyperliquid_funding_rates
  - Parquet:
      * app/data/cache/candles/hyperliquid_perpetual|<PAIR>|1h.parquet
      * app/data/cache/hyperliquid_funding/hyperliquid_funding|<PAIR>|1h.parquet

Usage:
  /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/backfill_hyperliquid_history.py \
      --days 365 --max-pairs 30 --interval 1m
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp
import pandas as pd
from pymongo import MongoClient, UpdateOne

from core.data_paths import data_paths

HL_API_URL = "https://api.hyperliquid.xyz/info"

# Hyperliquid coin name -> Bybit-style base symbol
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
    p = argparse.ArgumentParser(description="Backfill Hyperliquid history to Mongo + parquet")
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab"))
    p.add_argument("--mongo-db", default=os.getenv("MONGO_DATABASE"))
    p.add_argument("--days", type=int, default=365, help="History lookback in days")
    p.add_argument("--interval", default="1h", choices=["1m", "5m", "15m", "1h", "4h", "1d"])
    p.add_argument("--max-pairs", type=int, default=30)
    p.add_argument("--pair", default=None, help="Single pair like BTC-USDT")
    p.add_argument("--chunk-days", type=int, default=0, help="Candle fetch chunk in days (0 = auto)")
    p.add_argument("--sleep-ms", type=int, default=120, help="Sleep between API calls")
    p.add_argument("--write-mongo", action="store_true", default=True)
    p.add_argument("--no-write-mongo", dest="write_mongo", action="store_false")
    p.add_argument("--write-parquet", action="store_true", default=True)
    p.add_argument("--no-write-parquet", dest="write_parquet", action="store_false")
    p.add_argument("--candles-collection", default="hyperliquid_candles")
    p.add_argument("--funding-collection", default="hyperliquid_funding_rates")
    p.add_argument("--out-dir", default="app/data/cache/hyperliquid_backfill")
    return p.parse_args()


def _db_name_from_uri(uri: str) -> str:
    if "/" not in uri:
        return "quants_lab"
    tail = uri.rsplit("/", 1)[-1]
    return tail or "quants_lab"


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def coin_to_pair(coin: str) -> str:
    base = HL_TO_BYBIT_PREFIX.get(coin, coin)
    return f"{base}-USDT"


def pair_to_coin(pair: str) -> str:
    base = pair.replace("-USDT", "")
    return BYBIT_TO_HL_PREFIX.get(base, base)


async def hl_post(session: aiohttp.ClientSession, payload: dict, retries: int = 5) -> Any:
    last_err = None
    for i in range(retries):
        try:
            async with session.post(HL_API_URL, json=payload) as resp:
                text = await resp.text()
                if resp.status == 200:
                    return json.loads(text)
                last_err = RuntimeError(f"HTTP {resp.status}: {text[:240]}")
        except Exception as e:  # noqa: BLE001
            last_err = e
        await asyncio.sleep(min(2.0, 0.25 * (2 ** i)))
    raise RuntimeError(f"HL request failed: {payload.get('type')} ({last_err})")


async def fetch_meta(session: aiohttp.ClientSession) -> list[dict]:
    data = await hl_post(session, {"type": "metaAndAssetCtxs"})
    if not isinstance(data, list) or len(data) < 2:
        return []
    meta = data[0] or {}
    ctxs = data[1] or []
    uni = meta.get("universe", [])
    out: list[dict] = []
    for i, asset in enumerate(uni):
        coin = asset.get("name")
        if not coin:
            continue
        ctx = ctxs[i] if i < len(ctxs) and isinstance(ctxs[i], dict) else {}
        out.append(
            {
                "coin": coin,
                "pair": coin_to_pair(coin),
                "day_ntl_vlm": _to_float(ctx.get("dayNtlVlm"), 0.0),
                "open_interest": _to_float(ctx.get("openInterest"), 0.0),
                "mark_px": _to_float(ctx.get("markPx"), 0.0),
                "is_delisted": bool(asset.get("isDelisted", False)),
            }
        )
    return out


async def fetch_candles(
    session: aiohttp.ClientSession,
    coin: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    chunk_days: int,
    sleep_ms: int,
) -> list[dict]:
    chunk_ms = int(timedelta(days=chunk_days).total_seconds() * 1000)
    rows: list[dict] = []
    cursor_end = end_ms
    loops = 0
    while cursor_end >= start_ms:
        loops += 1
        if loops > 20000:
            break
        chunk_start = max(start_ms, cursor_end - chunk_ms + 1)
        payload = {
            "type": "candleSnapshot",
            "req": {
                "coin": coin,
                "interval": interval,
                "startTime": int(chunk_start),
                "endTime": int(cursor_end),
            },
        }
        data = await hl_post(session, payload)
        if not isinstance(data, list) or not data:
            break

        min_ts = cursor_end
        for r in data:
            try:
                ts_ms = int(r["t"])
                if ts_ms < start_ms or ts_ms > end_ms:
                    continue
                rows.append(
                    {
                        "timestamp_utc": ts_ms,
                        "timestamp": ts_ms // 1000,
                        "open": _to_float(r.get("o")),
                        "high": _to_float(r.get("h")),
                        "low": _to_float(r.get("l")),
                        "close": _to_float(r.get("c")),
                        "volume": _to_float(r.get("v")),
                        "n_trades": int(r.get("n", 0)),
                    }
                )
                if ts_ms < min_ts:
                    min_ts = ts_ms
            except (TypeError, ValueError, KeyError):
                continue

        new_cursor_end = min_ts - 1
        if new_cursor_end >= cursor_end:
            break
        cursor_end = new_cursor_end
        if min_ts <= start_ms:
            break
        if sleep_ms > 0:
            await asyncio.sleep(sleep_ms / 1000.0)
    # Dedup and sort
    if not rows:
        return rows
    df = pd.DataFrame(rows).drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp")
    return df.to_dict("records")


async def fetch_funding(
    session: aiohttp.ClientSession,
    coin: str,
    start_ms: int,
    end_ms: int,
    sleep_ms: int,
) -> list[dict]:
    rows: list[dict] = []
    cur = start_ms
    loops = 0
    while cur < end_ms:
        loops += 1
        if loops > 10000:
            break
        payload = {"type": "fundingHistory", "coin": coin, "startTime": int(cur)}
        data = await hl_post(session, payload)
        if not isinstance(data, list) or not data:
            break

        max_ts = cur
        for r in data:
            try:
                ts_ms = int(r["time"])
                if ts_ms < start_ms or ts_ms > end_ms:
                    continue
                rows.append(
                    {
                        "timestamp_utc": ts_ms,
                        "funding_rate": _to_float(r.get("fundingRate")),
                        "premium": _to_float(r.get("premium")),
                    }
                )
                if ts_ms > max_ts:
                    max_ts = ts_ms
            except (TypeError, ValueError, KeyError):
                continue

        if max_ts <= cur:
            break
        cur = max_ts + 1
        if len(data) < 500 and max_ts >= end_ms - 4 * 3600 * 1000:
            break
        if sleep_ms > 0:
            await asyncio.sleep(sleep_ms / 1000.0)

    if not rows:
        return rows
    df = pd.DataFrame(rows).drop_duplicates(subset=["timestamp_utc"], keep="last").sort_values("timestamp_utc")
    return df.to_dict("records")


def write_candles_parquet(path: Path, rows: list[dict]) -> int:
    if not rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    df_new = pd.DataFrame(rows)
    if path.exists():
        df_old = pd.read_parquet(path)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new
    df = df.drop_duplicates(subset=["timestamp"], keep="last").sort_values("timestamp").reset_index(drop=True)
    df.to_parquet(path, index=False)
    return int(len(df))


def write_funding_parquet(path: Path, rows: list[dict], pair: str, coin: str) -> int:
    if not rows:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    df_new = pd.DataFrame(rows)
    df_new["pair"] = pair
    df_new["coin"] = coin
    if path.exists():
        df_old = pd.read_parquet(path)
        df = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df = df_new
    df = df.drop_duplicates(subset=["timestamp_utc"], keep="last").sort_values("timestamp_utc").reset_index(drop=True)
    df.to_parquet(path, index=False)
    return int(len(df))


def upsert_candles_mongo(db, collection: str, pair: str, coin: str, interval: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    ops = []
    now_ms = int(time.time() * 1000)
    for r in rows:
        ops.append(
            UpdateOne(
                {"pair": pair, "timestamp_utc": int(r["timestamp_utc"]), "interval": interval},
                {
                    "$set": {
                        "pair": pair,
                        "coin": coin,
                        "interval": interval,
                        "timestamp_utc": int(r["timestamp_utc"]),
                        "open": _to_float(r["open"]),
                        "high": _to_float(r["high"]),
                        "low": _to_float(r["low"]),
                        "close": _to_float(r["close"]),
                        "volume": _to_float(r["volume"]),
                        "n_trades": int(r.get("n_trades", 0)),
                        "recorded_at": now_ms,
                    }
                },
                upsert=True,
            )
        )
    result = db[collection].bulk_write(ops, ordered=False)
    return int((result.upserted_count or 0) + (result.modified_count or 0))


def upsert_funding_mongo(db, collection: str, pair: str, coin: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    ops = []
    now_ms = int(time.time() * 1000)
    for r in rows:
        ts = int(r["timestamp_utc"])
        ops.append(
            UpdateOne(
                {"pair": pair, "timestamp_utc": ts},
                {
                    "$set": {
                        "pair": pair,
                        "coin": coin,
                        "timestamp_utc": ts,
                        "funding_rate": _to_float(r["funding_rate"]),
                        "premium": _to_float(r["premium"]),
                        "recorded_at": now_ms,
                    }
                },
                upsert=True,
            )
        )
    result = db[collection].bulk_write(ops, ordered=False)
    return int((result.upserted_count or 0) + (result.modified_count or 0))


async def main() -> None:
    args = parse_args()

    db_name = args.mongo_db or _db_name_from_uri(args.mongo_uri)
    mongo = MongoClient(args.mongo_uri) if args.write_mongo else None
    db = mongo[db_name] if mongo is not None else None

    if db is not None:
        db[args.candles_collection].create_index(
            [("pair", 1), ("interval", 1), ("timestamp_utc", 1)], unique=True
        )
        db[args.funding_collection].create_index([("pair", 1), ("timestamp_utc", 1)], unique=True)

    now = datetime.now(timezone.utc)
    end_ms = int(now.timestamp() * 1000)
    start_ms = int((now - timedelta(days=args.days)).timestamp() * 1000)
    auto_chunk_days = {"1m": 2, "5m": 7, "15m": 10, "1h": 14, "4h": 30, "1d": 120}
    chunk_days = args.chunk_days if args.chunk_days > 0 else auto_chunk_days.get(args.interval, 14)

    timeout = aiohttp.ClientTimeout(total=40)
    async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "quants-lab/1.0"}) as session:
        meta = await fetch_meta(session)
        if not meta:
            raise SystemExit("No Hyperliquid universe metadata returned.")

        if args.pair:
            pair = args.pair.strip().upper()
            coin = pair_to_coin(pair)
            selected = [{"coin": coin, "pair": pair, "day_ntl_vlm": 0.0, "is_delisted": False}]
        else:
            selected = [m for m in meta if not m["is_delisted"]]
            selected = sorted(selected, key=lambda x: x["day_ntl_vlm"], reverse=True)[: args.max_pairs]

        print(
            f"Backfilling Hyperliquid {args.interval} candles + funding: "
            f"{len(selected)} pairs, {args.days} days"
        )

        funding_dir = data_paths.cache_dir / "hyperliquid_funding"
        candles_dir = data_paths.candles_dir

        summary_rows = []
        total_candles = 0
        total_funding = 0

        for i, item in enumerate(selected, start=1):
            coin = item["coin"]
            pair = item["pair"]
            print(f"[{i}/{len(selected)}] {pair} ({coin}) ...", flush=True)
            try:
                candles = await fetch_candles(
                    session=session,
                    coin=coin,
                    interval=args.interval,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    chunk_days=chunk_days,
                    sleep_ms=args.sleep_ms,
                )
                funding = await fetch_funding(
                    session=session,
                    coin=coin,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    sleep_ms=args.sleep_ms,
                )

                mongo_c = 0
                mongo_f = 0
                if db is not None:
                    mongo_c = upsert_candles_mongo(db, args.candles_collection, pair, coin, args.interval, candles)
                    mongo_f = upsert_funding_mongo(db, args.funding_collection, pair, coin, funding)

                pq_c_rows = 0
                pq_f_rows = 0
                if args.write_parquet:
                    c_path = candles_dir / f"hyperliquid_perpetual|{pair}|{args.interval}.parquet"
                    f_path = funding_dir / f"hyperliquid_funding|{pair}|1h.parquet"
                    pq_c_rows = write_candles_parquet(c_path, candles)
                    pq_f_rows = write_funding_parquet(f_path, funding, pair=pair, coin=coin)

                total_candles += len(candles)
                total_funding += len(funding)

                print(
                    f"    candles={len(candles)} funding={len(funding)} "
                    f"mongo(c/f)={mongo_c}/{mongo_f} parquet_rows(c/f)={pq_c_rows}/{pq_f_rows}",
                    flush=True,
                )

                summary_rows.append(
                    {
                        "pair": pair,
                        "coin": coin,
                        "candles_fetched": len(candles),
                        "funding_fetched": len(funding),
                        "mongo_candles_upserts": mongo_c,
                        "mongo_funding_upserts": mongo_f,
                        "day_ntl_vlm": float(item.get("day_ntl_vlm", 0.0)),
                    }
                )
            except Exception as e:  # noqa: BLE001
                print(f"    FAILED: {type(e).__name__}: {e}", flush=True)
                summary_rows.append(
                    {
                        "pair": pair,
                        "coin": coin,
                        "candles_fetched": 0,
                        "funding_fetched": 0,
                        "mongo_candles_upserts": 0,
                        "mongo_funding_upserts": 0,
                        "day_ntl_vlm": float(item.get("day_ntl_vlm", 0.0)),
                        "error": f"{type(e).__name__}: {e}",
                    }
                )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    summary_csv = out_dir / f"{stamp}_hyperliquid_backfill_summary.csv"
    summary_json = out_dir / f"{stamp}_hyperliquid_backfill_meta.json"
    summary_df = pd.DataFrame(summary_rows)
    summary_df.to_csv(summary_csv, index=False)

    meta = {
        "timestamp_utc": stamp,
        "mongo_uri": args.mongo_uri,
        "mongo_db": db_name,
        "days": args.days,
        "interval": args.interval,
        "chunk_days": int(chunk_days),
        "n_pairs": int(len(summary_rows)),
        "total_candles_fetched": int(total_candles),
        "total_funding_fetched": int(total_funding),
        "write_mongo": bool(args.write_mongo),
        "write_parquet": bool(args.write_parquet),
        "candles_collection": args.candles_collection,
        "funding_collection": args.funding_collection,
        "summary_csv": str(summary_csv),
    }
    summary_json.write_text(json.dumps(meta, indent=2))

    print("\n=== Hyperliquid Backfill Summary ===")
    print(f"Pairs: {len(summary_rows)}")
    print(f"Candles fetched: {total_candles}")
    print(f"Funding rows fetched: {total_funding}")
    print(f"Summary CSV: {summary_csv}")
    print(f"Summary JSON: {summary_json}")

    if mongo is not None:
        mongo.close()


if __name__ == "__main__":
    asyncio.run(main())
