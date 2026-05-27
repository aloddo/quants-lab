"""
Bybit L2 order book collector.
Polls 25-level orderbook every 2 seconds for top pairs.
Stores imbalance + spread + mid price + depth to MongoDB.

Usage:
  MONGO_URI=... python scripts/bybit_l2_collector.py
"""
import asyncio
import logging
import os
import signal
import time
from datetime import datetime, timezone

import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("bybit_l2_collector")

BYBIT_BASE = "https://api.bybit.com/v5/market/orderbook"
PAIRS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "LINKUSDT",
         "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "APTUSDT", "XRPUSDT"]
POLL_INTERVAL = 2.0  # seconds
DEPTH = 25
COLLECTION = "bybit_l2_snapshots"

running = True

def handle_signal(*_):
    global running
    running = False
    logger.info("Shutdown signal received")

signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


async def fetch_orderbook(session: aiohttp.ClientSession, symbol: str):
    """Fetch 25-level orderbook for one symbol."""
    try:
        async with session.get(BYBIT_BASE, params={
            "category": "linear", "symbol": symbol, "limit": DEPTH
        }, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            data = await resp.json()
            if data.get("retCode") != 0:
                return None
            return data["result"]
    except Exception as e:
        logger.debug(f"Fetch error {symbol}: {e}")
        return None


def compute_snapshot(result: dict, pair_fmt: str) -> dict:
    """Compute imbalance metrics from raw orderbook."""
    bids = result["b"]  # [[price, size], ...]
    asks = result["a"]
    
    bid_sizes = [float(b[1]) for b in bids]
    ask_sizes = [float(a[1]) for a in asks]
    
    best_bid = float(bids[0][0]) if bids else 0
    best_ask = float(asks[0][0]) if asks else 0
    mid_px = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
    spread_bps = (best_ask - best_bid) / mid_px * 10000 if mid_px else 0
    
    bid_sz_total = sum(bid_sizes)
    ask_sz_total = sum(ask_sizes)
    total = bid_sz_total + ask_sz_total
    imbalance = (bid_sz_total - ask_sz_total) / total if total > 0 else 0
    
    # Top-5 imbalance (more sensitive to immediate pressure)
    bid_sz_5 = sum(bid_sizes[:5])
    ask_sz_5 = sum(ask_sizes[:5])
    total_5 = bid_sz_5 + ask_sz_5
    imbalance_top5 = (bid_sz_5 - ask_sz_5) / total_5 if total_5 > 0 else 0
    
    now_ms = int(time.time() * 1000)
    ts_ms = int(result.get("ts", now_ms))
    
    return {
        "pair": pair_fmt,
        "timestamp_utc": ts_ms,
        "recorded_at": now_ms,
        "mid_px": mid_px,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread_bps": spread_bps,
        "bid_sz_total": bid_sz_total,
        "ask_sz_total": ask_sz_total,
        "imbalance": imbalance,
        "imbalance_top5": imbalance_top5,
    }


async def main():
    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
    mongo_db = os.environ.get("MONGO_DATABASE", "quants_lab")
    
    client = AsyncIOMotorClient(mongo_uri)
    db = client[mongo_db]
    coll = db[COLLECTION]
    
    # Create index
    await coll.create_index([("pair", 1), ("timestamp_utc", 1)], unique=True)
    
    pair_map = {s: s[:-4] + "-USDT" for s in PAIRS}  # BTCUSDT -> BTC-USDT
    
    logger.info(f"Starting Bybit L2 collector: {len(PAIRS)} pairs, {POLL_INTERVAL}s interval")
    
    batch = []
    total_stored = 0
    
    async with aiohttp.ClientSession() as session:
        while running:
            start = time.time()
            
            # Fetch all pairs concurrently
            tasks = [fetch_orderbook(session, sym) for sym in PAIRS]
            results = await asyncio.gather(*tasks)
            
            for sym, result in zip(PAIRS, results):
                if result is None:
                    continue
                snap = compute_snapshot(result, pair_map[sym])
                batch.append(snap)
            
            # Flush batch every 10 polls (~20s)
            if len(batch) >= len(PAIRS) * 10:
                try:
                    await coll.insert_many(batch, ordered=False)
                    total_stored += len(batch)
                    if total_stored % (len(PAIRS) * 100) == 0:
                        logger.info(f"Stored {total_stored:,} snapshots ({len(PAIRS)} pairs × {total_stored//len(PAIRS)} polls)")
                except Exception as e:
                    # Duplicates are expected (unique index)
                    pass
                batch = []
            
            elapsed = time.time() - start
            sleep_time = max(0, POLL_INTERVAL - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
    
    # Final flush
    if batch:
        try:
            await coll.insert_many(batch, ordered=False)
            total_stored += len(batch)
        except:
            pass
    
    logger.info(f"Collector stopped. Total stored: {total_stored:,}")

if __name__ == "__main__":
    asyncio.run(main())
