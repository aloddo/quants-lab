#!/usr/bin/env python3
"""All-market HL 1m candle builder from the S3 fills archive (UNFILTERED).

Codex marks-gate (projects/quant/copy-rebuild/2026-06-24-codex-marks-gate): the
universe-filtered fills reconstruction under-covers tail coins (ADA/DOGE 25-33% of
minutes vs ~100% from the original all-market collector). For a deploy-driving
validation the MARK source must be all-market. The HL info candle API cannot serve
historical 1m; this reads the raw node_fills_by_block S3 tape WITHOUT a wallet
filter and aggregates to 1m OHLCV -> coverage parity with the pre-gap collector.

MEMORY-SAFE (binding decisions/2026-05-31-mandatory-streaming-io): processes ONE
hour-file at a time and updates an in-memory (coin,minute)->OHLC dict line-by-line
-- it NEVER buffers the full fills list (an all-market hour can be millions of
fills). Per-day candles are upserted then freed before the next day. install_
memory_guard aborts loud rather than OS-SIGKILL.

Dedup: filter to side=='B' (one row per public trade), matching
v13_reconstruct_1m_candles_from_fills so the two sources are consistent.

Idempotent upsert on (coin, interval, timestamp_utc); source='s3_allmarket_recon'.
Raw data never deleted (Rule 15) -- upsert only.

Usage:
    python research/v15/build_allmarket_1m_candles.py --start 2026-05-24 --end 2026-06-25 [--coins BTC,ADA]
"""
from __future__ import annotations

import argparse
import gc
import json
import logging
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import lz4.frame
from botocore.config import Config
from pymongo import MongoClient, UpdateOne

sys.path.insert(0, str(Path(__file__).resolve().parent))
try:
    from _streaming_io import install_memory_guard  # noqa: E402  MANDATORY
except Exception:  # pragma: no cover
    def install_memory_guard(*a, **k):  # type: ignore
        return None

logging.basicConfig(level=logging.INFO, format="%(asctime)s [allmkt_candles] %(message)s", stream=sys.stdout)
logger = logging.getLogger("allmkt_candles")

BUCKET = "hl-mainnet-node-data"
# Same schema transitions as data_pipeline/hl_s3_fill_downloader.py.
SCHEMA_TRANSITIONS = [
    (datetime(2025, 7, 28, tzinfo=timezone.utc), "node_fills_by_block/hourly"),
    (datetime(2025, 5, 25, tzinfo=timezone.utc), "node_fills/hourly"),
]


def prefix_for(dt: datetime) -> str:
    for d, p in SCHEMA_TRANSITIONS:
        if dt >= d:
            return p
    return SCHEMA_TRANSITIONS[-1][1]


def _update(agg: dict, coin: str, minute_ms: int, px: float):
    """Incremental OHLC update for (coin, minute). Fills arrive in block/time order
    so first-seen=open, last-seen=close."""
    k = (coin, minute_ms)
    o = agg.get(k)
    if o is None:
        agg[k] = [px, px, px, px, 1]  # open, high, low, close, n
    else:
        if px > o[1]:
            o[1] = px
        if px < o[2]:
            o[2] = px
        o[3] = px
        o[4] += 1


def build_hour(s3, day: datetime, hour: int, agg: dict, coin_filter: set | None) -> int:
    key = f"{prefix_for(day)}/{day.strftime('%Y%m%d')}/{hour}.lz4"
    try:
        with tempfile.NamedTemporaryFile(suffix=".lz4", delete=True) as tmp:
            s3.download_file(BUCKET, key, tmp.name, ExtraArgs={"RequestPayer": "requester"})
            with open(tmp.name, "rb") as f:
                raw = lz4.frame.decompress(f.read())
    except s3.exceptions.NoSuchKey:
        return 0
    except Exception as e:
        logger.warning(f"  {key}: {e}")
        return 0

    n = 0
    for line in raw.decode("utf-8").split("\n"):
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except Exception:
            continue
        events = rec.get("events") if isinstance(rec, dict) else None
        if events is None:
            events = [[rec.get("addr", ""), rec]]
        for ev in events:
            if not isinstance(ev, list) or len(ev) < 2:
                continue
            fill = ev[1] if isinstance(ev[1], dict) else {}
            if fill.get("side") != "B":  # one row per public trade (match v13 dedup)
                continue
            coin = fill.get("coin", "")
            if not coin or (coin_filter and coin not in coin_filter):
                continue
            try:
                px = float(fill.get("px", 0))
                t = int(fill.get("time", 0))
            except (TypeError, ValueError):
                continue
            if px <= 0 or t <= 0:
                continue
            _update(agg, coin, (t // 60_000) * 60_000, px)
            n += 1
    del raw
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--coins", help="comma-separated filter; default all")
    ap.add_argument("--collection", default="hyperliquid_candles")
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--mongo-db", default="quants_lab")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--mem-soft-gb", type=float, default=6.0)
    args = ap.parse_args()

    install_memory_guard(soft_gb=args.mem_soft_gb) if "soft_gb" in install_memory_guard.__code__.co_varnames else install_memory_guard()

    coin_filter = set(args.coins.split(",")) if args.coins else None
    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    s3 = boto3.client("s3", region_name="us-east-1", config=Config(
        connect_timeout=10, read_timeout=30, retries={"max_attempts": 6, "mode": "adaptive"},
        max_pool_connections=8))
    coll = MongoClient(args.mongo_uri)[args.mongo_db][args.collection]
    coll.create_index([("coin", 1), ("interval", 1), ("timestamp_utc", 1)], unique=True)

    total_bars = 0
    cur = start
    while cur < end:
        agg: dict = {}
        fills_seen = 0
        for h in range(24):
            fills_seen += build_hour(s3, cur, h, agg, coin_filter)
        if agg and not args.dry_run:
            ops = []
            for (coin, m), (o, hi, lo, c, n) in agg.items():
                ops.append(UpdateOne(
                    {"coin": coin, "interval": "1m", "timestamp_utc": int(m)},
                    {"$set": {"coin": coin, "pair": f"{coin}-USDT", "interval": "1m",
                              "timestamp_utc": int(m), "timestamp": int(m // 1000),
                              "open": o, "high": hi, "low": lo, "close": c, "n_trades": n,
                              "source": "s3_allmarket_recon"}},
                    upsert=True))
            # chunked bulk_write to bound memory
            for i in range(0, len(ops), 5000):
                coll.bulk_write(ops[i:i + 5000], ordered=False)
        logger.info(f"{cur.date()}: {fills_seen:,} B-fills -> {len(agg):,} (coin,minute) bars "
                    f"{'[DRY]' if args.dry_run else 'upserted'}")
        total_bars += len(agg)
        del agg
        gc.collect()
        cur += timedelta(days=1)
    logger.info(f"DONE. {total_bars:,} bar-rows across {(end - start).days} days.")


if __name__ == "__main__":
    main()
