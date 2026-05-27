"""
HL Open Interest + Predicted Funding Collector

Snapshots two HL API endpoints every 15 minutes:
1. metaAndAssetCtxs → OI, mark price, volume per coin (all 230 coins in 1 call)
2. predictedFundings → cross-venue predicted funding (HL, Binance, Bybit)

Collections:
    hl_oi_snapshots        — per-coin OI, mark, volume, funding (1 doc per coin per snapshot)
    hl_predicted_funding   — cross-venue predicted funding (1 doc per coin per snapshot)

Usage:
    set -a && source .env && set +a
    /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/hl_oi_predicted_collector.py
"""

import json
import logging
import os
import signal
import sys
import time
import urllib.request
from datetime import datetime, timezone

from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger("hl_oi_collector")

# Config
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
MONGO_DB = os.getenv("MONGO_DATABASE", "quants_lab")
HL_API_URL = "https://api.hyperliquid.xyz/info"
SNAPSHOT_INTERVAL_S = 900  # 15 minutes

# Graceful shutdown
running = True
def handle_signal(signum, frame):
    global running
    running = False
    logger.info("Shutdown signal received")
signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


def hl_post(payload: dict, timeout: int = 15) -> dict:
    """POST to HL info API."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(HL_API_URL, data=data, headers={"Content-Type": "application/json"})
    resp = urllib.request.urlopen(req, timeout=timeout)
    return json.loads(resp.read())


def snapshot_oi(db) -> int:
    """Snapshot OI + market data for all coins."""
    data = hl_post({"type": "metaAndAssetCtxs"})
    meta = data[0]
    ctxs = data[1]
    now = datetime.now(timezone.utc)
    ts_ms = int(now.timestamp() * 1000)

    docs = []
    for i, ctx in enumerate(ctxs):
        if i >= len(meta["universe"]):
            break
        coin = meta["universe"][i]["name"]
        mark_px = float(ctx.get("markPx", 0))
        oi_coins = float(ctx.get("openInterest", 0))
        if mark_px <= 0 or oi_coins <= 0:
            continue

        docs.append({
            "timestamp_utc": ts_ms,
            "recorded_at": now,
            "coin": coin,
            "open_interest": oi_coins,
            "oi_usd": oi_coins * mark_px,
            "mark_px": mark_px,
            "oracle_px": float(ctx.get("oraclePx", 0)),
            "mid_px": float(ctx.get("midPx", 0)),
            "funding": float(ctx.get("funding", 0)),
            "premium": float(ctx.get("premium", 0)),
            "day_ntl_vlm": float(ctx.get("dayNtlVlm", 0)),
            "prev_day_px": float(ctx.get("prevDayPx", 0)),
        })

    if docs:
        coll = db["hl_oi_snapshots"]
        try:
            coll.insert_many(docs, ordered=False)
        except BulkWriteError as e:
            # Dedup by timestamp+coin if any
            logger.warning(f"OI insert: {e.details.get('nInserted', 0)} inserted, some dupes")

    return len(docs)


def snapshot_predicted_funding(db) -> int:
    """Snapshot cross-venue predicted funding for all coins."""
    data = hl_post({"type": "predictedFundings"})
    now = datetime.now(timezone.utc)
    ts_ms = int(now.timestamp() * 1000)

    docs = []
    for item in data:
        coin = item[0]
        venues = item[1]

        doc = {
            "timestamp_utc": ts_ms,
            "recorded_at": now,
            "coin": coin,
        }

        for venue_name, venue_data in venues:
            if venue_data is None:
                continue
            prefix = venue_name.lower().replace("perp", "")  # bin, hl, bybit
            doc[f"{prefix}_funding_rate"] = float(venue_data.get("fundingRate", 0))
            doc[f"{prefix}_next_funding_time"] = venue_data.get("nextFundingTime", 0)
            doc[f"{prefix}_interval_h"] = venue_data.get("fundingIntervalHours", 0)

        docs.append(doc)

    if docs:
        coll = db["hl_predicted_funding"]
        try:
            coll.insert_many(docs, ordered=False)
        except BulkWriteError as e:
            logger.warning(f"Predicted funding insert: {e.details.get('nInserted', 0)} inserted")

    return len(docs)


def ensure_indexes(db):
    """Create indexes for efficient queries."""
    db["hl_oi_snapshots"].create_index([("coin", ASCENDING), ("timestamp_utc", ASCENDING)])
    db["hl_oi_snapshots"].create_index([("timestamp_utc", ASCENDING)])
    db["hl_predicted_funding"].create_index([("coin", ASCENDING), ("timestamp_utc", ASCENDING)])
    db["hl_predicted_funding"].create_index([("timestamp_utc", ASCENDING)])
    logger.info("Indexes ensured")


def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    ensure_indexes(db)

    logger.info(f"Starting HL OI + Predicted Funding collector (every {SNAPSHOT_INTERVAL_S}s)")

    # First snapshot immediately
    try:
        n_oi = snapshot_oi(db)
        n_pf = snapshot_predicted_funding(db)
        logger.info(f"Initial snapshot: {n_oi} OI docs, {n_pf} predicted funding docs")
    except Exception as e:
        logger.error(f"Initial snapshot failed: {e}")

    while running:
        # Sleep in small increments for responsive shutdown
        sleep_until = time.time() + SNAPSHOT_INTERVAL_S
        while running and time.time() < sleep_until:
            time.sleep(5)

        if not running:
            break

        try:
            n_oi = snapshot_oi(db)
            n_pf = snapshot_predicted_funding(db)

            # Stats
            oi_count = db["hl_oi_snapshots"].count_documents({})
            pf_count = db["hl_predicted_funding"].count_documents({})
            logger.info(f"Snapshot: {n_oi} OI + {n_pf} predicted funding | Total: {oi_count:,} OI, {pf_count:,} PF")
        except Exception as e:
            logger.error(f"Snapshot failed: {e}")

    logger.info("Collector stopped gracefully")
    client.close()


if __name__ == "__main__":
    main()
