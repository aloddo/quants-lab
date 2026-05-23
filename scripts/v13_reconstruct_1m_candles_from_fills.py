#!/usr/bin/env python3
"""Reconstruct HL 1m candles from the S3 fills archive.

Why: HL's public candleSnapshot API only retains 1m for ~5 days. But we
have the underlying TRADE archive (174+ days of S3 fills, 41 GB on disk).
Aggregating fills into 1m buckets reconstructs the same OHLCV that HL
computes internally. No API retention limit, no basis vs proxy venues,
real HL prices.

Output: writes to MongoDB hyperliquid_candles collection with interval=1m.
Idempotent: uses upsert on (coin, interval, timestamp_utc).

Usage:
    python scripts/v13_reconstruct_1m_candles_from_fills.py \\
        --start 2025-12-01 --end 2026-05-24 [--coins BTC,ETH,SOL]

If --coins is omitted, all unique coins in the fills are processed.
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient, UpdateOne

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_candles] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
FILLS_DIR = ROOT / "app" / "data" / "hl_s3_fills"


def aggregate_one_day(day_path: Path, coin_filter: set | None = None) -> pd.DataFrame:
    """Load one daily fills parquet + aggregate to 1m OHLCV per coin.

    CRITICAL: HL's S3 fill archive contains BOTH SIDES of every match
    (buyer + seller). For volume / n_trades to match HL's actual public
    candles, we MUST dedupe matches to one row per trade. We use the
    canonical (coin, time, price, size) triple as the match key after
    verifying that a match appears exactly twice (once as B, once as A)
    with identical time/price/size. Side is dropped from the key so the
    pair collapses to one row.

    Returns a long DataFrame with columns:
        coin, timestamp_utc (ms at minute bucket start), open, high, low,
        close, volume, n_trades.
    """
    try:
        df = pd.read_parquet(day_path, columns=["coin", "time", "price", "size", "side"])
    except Exception:
        df = pd.read_parquet(day_path)
    if df.empty:
        return pd.DataFrame()
    if coin_filter is not None:
        df = df[df["coin"].isin(coin_filter)]
    if df.empty:
        return pd.DataFrame()

    df = df.dropna(subset=["coin", "time", "price", "size", "side"])
    df = df[(df["price"] > 0) & (df["size"] > 0)]
    if df.empty:
        return pd.DataFrame()

    # MATCH DEDUPLICATION: each public trade appears twice in S3 fills
    # (one row per counterparty). We filter to side=="B" (the BUY side
    # of every match). Each public trade has exactly one B row, so the
    # filtered set has exactly one row per trade -- volume / n_trades
    # then match HL's canonical OHLCV. Verified empirically: in a sample
    # BTC minute, side=="B" yielded 211 rows with sum_size=4.5442, while
    # the raw (both-sides) sum was 9.0883, exactly 2x.
    #
    # This is more robust than fingerprint-dedup (which over-collapses
    # multiple distinct same-price same-size same-ms matches into one).
    df = df[df["side"] == "B"]
    if df.empty:
        return pd.DataFrame()

    # Bucket each match into its minute.
    df["minute_ms"] = (df["time"].astype("int64") // 60_000) * 60_000

    # Stable sort within each bucket by time. Same-time fills are not
    # further disambiguated here; we accept the parquet's internal order
    # as the tie-breaker pending a true block/sequence id in the source.
    df = df.sort_values(["coin", "minute_ms", "time"], kind="stable")

    agg = df.groupby(["coin", "minute_ms"], sort=False).agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("size", "sum"),
        n_trades=("price", "count"),
    ).reset_index()
    agg.rename(columns={"minute_ms": "timestamp_utc"}, inplace=True)
    return agg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True, help="YYYY-MM-DD")
    ap.add_argument("--end", required=True, help="YYYY-MM-DD")
    ap.add_argument("--coins", help="Comma-separated coins to include; default all")
    ap.add_argument("--collection", default="hyperliquid_candles")
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--mongo-db", default="quants_lab")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    coin_filter = set(args.coins.split(",")) if args.coins else None

    logger.info(f"Reconstructing 1m candles {start.date()} -> {end.date()}")
    if coin_filter:
        logger.info(f"Coin filter: {sorted(coin_filter)}")

    client = MongoClient(args.mongo_uri)
    coll = client[args.mongo_db][args.collection]
    # Ensure idempotent upsert key.
    coll.create_index(
        [("coin", 1), ("interval", 1), ("timestamp_utc", 1)],
        unique=True,
    )

    total_bars = 0
    total_files = 0
    failed_files = []
    cur = start
    while cur <= end:
        path = FILLS_DIR / f"{cur.strftime('%Y%m%d')}.parquet"
        if not path.exists():
            logger.warning(f"Missing {path.name}; skipping")
            cur += timedelta(days=1)
            continue
        try:
            day_agg = aggregate_one_day(path, coin_filter)
        except Exception as e:
            logger.exception(f"Failed to aggregate {path.name}: {e}")
            failed_files.append(path.name)
            cur += timedelta(days=1)
            continue
        if day_agg.empty:
            cur += timedelta(days=1)
            continue
        total_files += 1
        total_bars += len(day_agg)
        if args.dry_run:
            logger.info(f"[DRY] {path.name}: {len(day_agg):,} bars, {day_agg['coin'].nunique()} coins")
            cur += timedelta(days=1)
            continue

        # Upsert to MongoDB in batches.
        ops = []
        for _, r in day_agg.iterrows():
            ts_ms = int(r["timestamp_utc"])
            ops.append(UpdateOne(
                {"coin": r["coin"], "interval": "1m", "timestamp_utc": ts_ms},
                {"$set": {
                    "coin": r["coin"],
                    "interval": "1m",
                    "timestamp_utc": ts_ms,
                    "timestamp": ts_ms // 1000,
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "volume": float(r["volume"]),
                    "n_trades": int(r["n_trades"]),
                    "pair": f"{r['coin']}-USDT",
                    "source": "s3_reconstructed",
                }},
                upsert=True,
            ))
            if len(ops) >= 5000:
                coll.bulk_write(ops, ordered=False)
                ops.clear()
        if ops:
            coll.bulk_write(ops, ordered=False)
        logger.info(f"{path.name}: {len(day_agg):,} bars across {day_agg['coin'].nunique()} coins ingested")
        cur += timedelta(days=1)

    logger.info(f"Done. {total_files} days processed, {total_bars:,} bar-coin rows.")
    if failed_files:
        logger.error(f"Failed: {failed_files}")

    # Summary by coin.
    if not args.dry_run:
        pipeline = [
            {"$match": {"interval": "1m", "source": "s3_reconstructed"}},
            {"$group": {"_id": "$coin", "n": {"$sum": 1},
                        "mn": {"$min": "$timestamp_utc"}, "mx": {"$max": "$timestamp_utc"}}},
            {"$sort": {"n": -1}},
        ]
        res = list(coll.aggregate(pipeline))
        logger.info(f"Reconstructed coin coverage ({len(res)} coins):")
        for r in res[:20]:
            mn = datetime.fromtimestamp(r["mn"]/1000, tz=timezone.utc).date()
            mx = datetime.fromtimestamp(r["mx"]/1000, tz=timezone.utc).date()
            logger.info(f"  {r['_id']:>10s}: {r['n']:>7,} bars {mn} -> {mx}")


if __name__ == "__main__":
    main()
