#!/usr/bin/env python3
"""Extract HL HIP-3 oracle marks from S3 explorer_blocks -> Mongo.

THE precise mark source for exotic (HIP-3 builder) dexes. HL's main-perp oracle is
consensus-level and not in explorer_blocks, but builder dexes post their oracle via
`perpDeploy.setOracle.oraclePxs` (per-coin [coin, px], all coins at once, sub-second
cadence). For illiquid HIP-3 coins the last TRADE price diverges materially from the
oracle (observed vntl:ANTHROPIC: trade 544 vs oracle 485.85 = 12%), which -- on a
leveraged position -- is a dominant source of equity-reconstruction drift.

We snapshot the oracle hourly across the window (slow-moving illiquid coins make hourly
ample) into a SEPARATE collection `hyperliquid_oracle`, so nothing is deleted and M01's
get_mark can prefer oracle for prefixed coins with a trade-candle fallback.

Dexes covered: km, vntl, para, flx, cash, xyz, hyna (the setOracle dexes).
Main dex (BTC/ETH/...) is liquid -> trade-candle is already a good proxy; not here.

Usage:
    python v13_extract_oracle_marks.py --start 2025-12-01 --end 2026-05-27 --cadence-min 60
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import boto3
import lz4.frame
import msgpack
from pymongo import MongoClient, UpdateOne

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", stream=sys.stdout)
logger = logging.getLogger("oracle")

BUCKET = "hl-mainnet-node-data"
# Calibration anchors (height, time) verified 2026-05-30 from explorer_blocks headers.
CAL_H0, CAL_T0 = 800000001, datetime(2025, 11, 17, 23, 52, 38, tzinfo=timezone.utc)
CAL_H1, CAL_T1 = 999999900, datetime(2026, 5, 17, 12, 50, 1, tzinfo=timezone.utc)
RATE = (CAL_H1 - CAL_H0) / (CAL_T1 - CAL_T0).total_seconds()  # blocks/sec


def key_for_height(h: int) -> str:
    h = ((h + 99) // 100) * 100
    return f"explorer_blocks/{(h // 100_000_000) * 100_000_000}/{(h // 100_000) * 100_000}/{h}.rmp.lz4"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-05-27")
    ap.add_argument("--cadence-min", type=int, default=60)
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--mongo-db", default="quants_lab")
    ap.add_argument("--collection", default="hyperliquid_oracle")
    args = ap.parse_args()

    s3 = boto3.client("s3", region_name="us-east-1")
    cache: dict[str, list] = {}

    def readf(h: int):
        k = key_for_height(h)
        if k not in cache:
            raw = s3.get_object(Bucket=BUCKET, Key=k, RequestPayer="requester")["Body"].read()
            cache[k] = msgpack.unpackb(lz4.frame.decompress(raw), raw=False)
            if len(cache) > 64:
                cache.pop(next(iter(cache)))
        return cache[k]

    def btime(blk, i=0):
        return datetime.fromisoformat(blk[i]["header"]["block_time"][:26]).replace(tzinfo=timezone.utc)

    def height_for_time(target):
        # Pure linear estimate from the calibrated rate (stable over the window;
        # +/- tens of seconds, negligible for hourly illiquid-coin oracle). ONE
        # correction read only if the estimate lands far off (rate drift guard).
        h = int(CAL_H0 + (target - CAL_T0).total_seconds() * RATE)
        blk = readf(h)
        err = (target - btime(blk)).total_seconds()
        if abs(err) > 600:  # >10 min off -> correct once
            h = max(CAL_H0, int(h + err * RATE))
            blk = readf(h)
        return h, blk

    def oracle_at(target):
        """Latest setOracle ORACLE and MARK price per coin at-or-before target. Scan target's
        file; if empty (no setOracle in this ~window) step back a file (~100 blocks).
        mark = markPxs[0] (the price HL uses for accountValue/uPnL; oracle is FUNDING-only and
        WRONG for m2m -- this mark_px is the field M01 get_mark/_markpx_lookup reads)."""
        out: dict[str, float] = {}
        mark: dict[str, float] = {}
        h, blk = height_for_time(target)
        for _ in range(4):
            for b in blk:
                bts = datetime.fromisoformat(b["header"]["block_time"][:26]).replace(tzinfo=timezone.utc)
                if bts > target:
                    break
                for tx in b.get("txs", []):
                    for a in tx.get("actions", []):
                        if isinstance(a, dict) and "setOracle" in a:
                            so = a["setOracle"]
                            for coin, px in so.get("oraclePxs", []):
                                out[coin] = float(px)
                            # markPxs: outer list len 0/1/2; [0] is the per-coin [coin, px] mark list
                            mp = so.get("markPxs") or []
                            if mp:
                                for coin, px in mp[0]:
                                    try:
                                        mark[coin] = float(px)
                                    except (TypeError, ValueError):
                                        pass
            if out:
                break
            h -= 100
            blk = readf(h)
        return out, mark

    coll = MongoClient(args.mongo_uri)[args.mongo_db][args.collection]
    coll.create_index([("coin", 1), ("timestamp_utc", 1)], unique=True)

    start = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
    end = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
    cadence = timedelta(minutes=args.cadence_min)
    n_snaps = int((end - start) / cadence)
    logger.info(f"Extracting oracle {start.date()}..{end.date()} @ {args.cadence_min}min ({n_snaps:,} snapshots)")

    t0 = time.time()
    cur = start
    n_written = 0
    n_snap = 0
    while cur <= end:
        try:
            o, mk = oracle_at(cur)
        except Exception as e:  # noqa: BLE001
            logger.warning(f"  {cur}: {e!r}")
            cur += cadence
            continue
        ts_ms = int(cur.timestamp() * 1000)
        if o:
            ops = [
                UpdateOne(
                    {"coin": c, "timestamp_utc": ts_ms},
                    {"$set": {"coin": c, "timestamp_utc": ts_ms, "oracle_px": px,
                              "mark_px": mk.get(c), "source": "s3_setOracle"}},
                    upsert=True,
                )
                for c, px in o.items()
            ]
            coll.bulk_write(ops, ordered=False)
            n_written += len(ops)
        n_snap += 1
        if n_snap % 200 == 0:
            logger.info(f"  {cur} | {n_snap:,}/{n_snaps:,} snaps, {n_written:,} px, {len(o)} coins/snap, {(time.time()-t0)/60:.1f}min")
        cur += cadence

    logger.info(f"\nDONE in {(time.time()-t0)/60:.1f} min | {n_snap:,} snapshots, {n_written:,} oracle px written")
    logger.info(f"Collection: {args.mongo_db}.{args.collection}")


if __name__ == "__main__":
    main()
