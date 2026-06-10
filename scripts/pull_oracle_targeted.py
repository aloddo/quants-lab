#!/usr/bin/env python3
"""TARGETED exotic-oracle puller (Alberto 2026-06-04: "get oracle DIRECTLY, do not
download the whole grid of blocks -- it takes forever").

For each candidate wallet we pull HL `setOracle` marks ONLY at the wallet's perpAllTime
anchor timestamps -- the exact instants M1 marks equity against -- not a dense full-window
grid. At each anchor we scan explorer_blocks BACKWARD accumulating the latest setOracle px
per coin and STOP as soon as every exotic coin the wallet holds at that anchor is covered
(the old v13 `oracle_at` broke at the first non-empty block, so slow-posting dexes like
xyz:GOLD got skipped -> the Apr/May coverage gap). Empirically all held coins resolve in
~3 block files; the 6h lookback cap is a backstop for a dormant coin.

We STAMP each mark at the anchor ts (the query instant). M1's _oracle_lookup does an
at-or-before lookup with a 6h staleness cap; stamping at the anchor instant means an exact
age-0 hit -> the prevailing oracle as-of the anchor is used, sidestepping the staleness cap
for slow commodities whose price (correctly) has not changed in hours.

Writes to mongo `hyperliquid_oracle` (source=s3_setOracle_targeted), idempotent upserts.
Raw data, never deleted. Minutes, not hours.

Usage:
    python scripts/pull_oracle_targeted.py [--wallets app/data/v15/m01_validation_wallets.txt]
                                           [--start 2025-12-01] [--end 2026-05-23]
                                           [--max-lookback-min 360]
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, "research/v15")
import boto3  # noqa: E402
import lz4.frame  # noqa: E402
import msgpack  # noqa: E402
import pandas as pd  # noqa: E402
from pymongo import MongoClient, UpdateOne  # noqa: E402

import v15_m01_equity_reconstruct as m01  # noqa: E402

BUCKET = "hl-mainnet-node-data"
# Calibration anchors (height, time) verified 2026-05-30 from explorer_blocks headers.
CAL_H0, CAL_T0 = 800000001, datetime(2025, 11, 17, 23, 52, 38, tzinfo=timezone.utc)
CAL_H1, CAL_T1 = 999999900, datetime(2026, 5, 17, 12, 50, 1, tzinfo=timezone.utc)
RATE = (CAL_H1 - CAL_H0) / (CAL_T1 - CAL_T0).total_seconds()  # blocks/sec ~12.82

_S3 = boto3.client("s3", region_name="us-east-1")
_CACHE: dict[str, list] = {}
_CACHE_MAX = 512


def key_for_height(h: int) -> str:
    h = ((h + 99) // 100) * 100
    return f"explorer_blocks/{(h // 100_000_000) * 100_000_000}/{(h // 100_000) * 100_000}/{h}.rmp.lz4"


def readf(h: int):
    k = key_for_height(h)
    if k not in _CACHE:
        raw = _S3.get_object(Bucket=BUCKET, Key=k, RequestPayer="requester")["Body"].read()
        _CACHE[k] = msgpack.unpackb(lz4.frame.decompress(raw), raw=False)
        if len(_CACHE) > _CACHE_MAX:
            _CACHE.pop(next(iter(_CACHE)))
    return _CACHE[k]


def btime(blk, i=0) -> datetime:
    return datetime.fromisoformat(blk[i]["header"]["block_time"][:26]).replace(tzinfo=timezone.utc)


def height_for_time(target: datetime) -> int:
    """Robust linear height estimate with iterative correction (handles RATE drift past
    CAL_H1=May17). Converges to <30s in a couple reads; bounded at 6 iterations."""
    h = int(CAL_H0 + (target - CAL_T0).total_seconds() * RATE)
    for _ in range(6):
        try:
            blk = readf(h)
        except Exception:  # missing file near tip -> nudge back one file
            h -= 100
            continue
        err = (target - btime(blk)).total_seconds()
        if abs(err) < 30:
            break
        h = max(CAL_H0, int(h + err * RATE))
    return h


def oracle_snapshot(target: datetime, required: set[str], max_lookback_min: int):
    """Latest setOracle px per coin at-or-before `target`, scanning blocks BACKWARD and
    accumulating until every coin in `required` is covered (or the lookback cap is hit).
    Returns (collected: dict[coin->px], missing: set[str], files_read: int)."""
    collected: dict[str, float] = {}
    mark: dict[str, float] = {}            # markPxs[0] -- the price HL uses for accountValue
    h = height_for_time(target)
    files_read = 0
    cap_s = max_lookback_min * 60
    earliest_seen = target  # track how far back we have scanned
    # step back file-by-file; required coins resolve in ~3 files empirically.
    for _ in range(8000):  # hard backstop (8000 files * 100 blocks ~= 7 days)
        try:
            blk = readf(h)
        except Exception:
            h -= 100
            files_read += 1
            continue
        files_read += 1
        for b in blk:
            bts = datetime.fromisoformat(b["header"]["block_time"][:26]).replace(tzinfo=timezone.utc)
            if bts > target:
                continue  # block after the anchor instant -> not yet valid
            if bts < earliest_seen:
                earliest_seen = bts
            for tx in b.get("txs", []):
                for a in tx.get("actions", []):
                    if isinstance(a, dict) and "setOracle" in a:
                        so = a["setOracle"]
                        for coin, px in so.get("oraclePxs", []):
                            # latest-at-or-before: only set on FIRST (most recent) sighting
                            if coin not in collected:
                                try:
                                    collected[coin] = float(px)
                                except (TypeError, ValueError):
                                    pass
                        # markPxs is the price HL uses for accountValue/uPnL (oracle is funding-only).
                        # Outer list len 0/1/2; [0] is the per-coin [coin, px] mark list.
                        mp = so.get("markPxs") or []
                        if mp:
                            for coin, px in mp[0]:
                                if coin not in mark:
                                    try:
                                        mark[coin] = float(px)
                                    except (TypeError, ValueError):
                                        pass
        if required and required.issubset(collected.keys()):
            break
        if (target - earliest_seen).total_seconds() > cap_s:
            break
        h -= 100
    missing = set(required) - set(collected.keys())
    return collected, mark, missing, files_read


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets", default="app/data/v15/m01_validation_wallets.txt")
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-05-23")
    ap.add_argument("--max-lookback-min", type=int, default=360)
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--mongo-db", default="quants_lab")
    ap.add_argument("--collection", default="hyperliquid_oracle")
    args = ap.parse_args()

    S = int(pd.Timestamp(args.start, tz="UTC").timestamp() * 1000)
    E = int(pd.Timestamp(args.end, tz="UTC").timestamp() * 1000 + 86_399_999)
    coll = MongoClient(args.mongo_uri)[args.mongo_db][args.collection]
    coll.create_index([("coin", 1), ("timestamp_utc", 1)], unique=True)
    anchor_df = pd.read_parquet(m01.ANCHOR_PARQUET)

    wallets = [l.strip() for l in open(args.wallets) if l.strip().startswith("0x")]
    print(f"targeted oracle pull: {len(wallets)} wallets, window {args.start}..{args.end}, "
          f"lookback cap {args.max_lookback_min}min", flush=True)

    t0 = time.time()
    grand_written = 0
    for w in wallets:
        wl = w.lower()
        avh = m01.get_portfolio_perp(w)
        anchors = sorted(t for t, v in avh if v > 0.01 and S <= t <= E)
        if not anchors:
            print(f"  {w[:12]}: no anchors in window -> skip", flush=True)
            continue
        fills = m01.load_wallet_fills(w, S, E)
        anchor = m01.load_wallet_anchor(w, anchor_df)
        seed_exotic = set()
        if anchor is not None:
            seed_exotic = {c for c in anchor.positions if ":" in c}
        last_anchor = anchors[-1]
        n_written = 0
        n_missing_total = 0
        files_total = 0
        for t in anchors:
            tgt = datetime.fromtimestamp(t / 1000, tz=timezone.utc)
            held = {c for c in m01.positions_at(fills, t) if ":" in c}
            required = set(held)
            if t == last_anchor:
                required |= seed_exotic  # seeded positions M1 marks at the final anchor
            if not required:
                continue  # no exotic exposure at this anchor -> nothing to mark
            collected, mark, missing, fr = oracle_snapshot(tgt, required, args.max_lookback_min)
            files_total += fr
            if missing:
                n_missing_total += len(missing)
            if collected:
                ops = [
                    UpdateOne(
                        {"coin": c, "timestamp_utc": t},
                        {"$set": {"coin": c, "timestamp_utc": t, "oracle_px": px,
                                  "mark_px": mark.get(c),  # HL accountValue price (None if absent)
                                  "source": "s3_setOracle_targeted"}},
                        upsert=True,
                    )
                    for c, px in collected.items()
                ]
                coll.bulk_write(ops, ordered=False)
                n_written += len(ops)
        grand_written += n_written
        print(f"  {w[:12]}: {len(anchors)} anchors, {n_written} px written, "
              f"{files_total} files read, {n_missing_total} required-coin misses "
              f"({(time.time()-t0)/60:.1f}min elapsed)", flush=True)

    print(f"DONE: {grand_written} oracle px written in {(time.time()-t0)/60:.1f}min", flush=True)


if __name__ == "__main__":
    main()
