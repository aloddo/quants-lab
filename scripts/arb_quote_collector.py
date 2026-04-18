"""
R1 Quote Collector — Collect synchronized bid/ask snapshots from Binance + Bybit.

Polls every 5 seconds for the top N pairs identified in R0.
Stores to MongoDB arb_quote_snapshots collection.
Designed to run passively for 7+ days via tmux or LaunchDaemon.

Usage:
    python scripts/arb_quote_collector.py [--pairs 15] [--interval 5]
"""
import asyncio
import aiohttp
import argparse
import csv
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pymongo import MongoClient

# Load .env
_env_file = Path(__file__).resolve().parents[1] / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("/tmp/arb-quote-collector.log"),
    ],
)
logger = logging.getLogger(__name__)


def load_top_pairs(n: int = 15) -> list[str]:
    """Load top N pairs from R0 universe discovery results, ranked by spread."""
    csv_path = Path(__file__).resolve().parents[1] / "app/data/cache/arb_universe.csv"
    if not csv_path.exists():
        logger.error(f"Universe file not found: {csv_path}")
        logger.info("Run R0 universe discovery first")
        sys.exit(1)

    pairs = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["viable"] == "True" and float(row["spread_bps"]) > 5:
                pairs.append(row["symbol"])

    # Take top N by spread (CSV is already sorted by spread descending)
    selected = pairs[:n]
    logger.info(f"Selected {len(selected)} pairs for quote collection: {selected}")
    return selected


async def poll_quotes(pairs: list[str], interval: float, db):
    """Poll Binance + Bybit book tickers and store synchronized snapshots."""
    coll = db["arb_quote_snapshots"]
    # Create indexes
    coll.create_index([("timestamp", -1)])
    coll.create_index([("symbol", 1), ("timestamp", -1)])
    # TTL: keep 30 days of quote data
    coll.create_index([("timestamp", 1)], expireAfterSeconds=30 * 86400)

    session = aiohttp.ClientSession()
    poll_count = 0
    error_count = 0
    symbols_str = ",".join(f'"{s}"' for s in pairs)

    logger.info(f"Starting quote collection: {len(pairs)} pairs, {interval}s interval")

    try:
        while True:
            t0 = time.time()
            try:
                # Fetch both exchanges as close together as possible
                bn_task = session.get("https://api.binance.com/api/v3/ticker/bookTicker")
                bb_task = session.get("https://api.bybit.com/v5/market/tickers?category=linear")

                bn_resp, bb_resp = await asyncio.gather(bn_task, bb_task)
                bn_ts = time.time()
                bn_data = await bn_resp.json()
                bb_data = await bb_resp.json()
                bb_ts = time.time()

                # Parse Binance
                bn_books = {}
                for b in bn_data:
                    if b["symbol"] in pairs:
                        bn_books[b["symbol"]] = {
                            "bid": float(b["bidPrice"]),
                            "ask": float(b["askPrice"]),
                            "bid_qty": float(b["bidQty"]),
                            "ask_qty": float(b["askQty"]),
                        }

                # Parse Bybit
                bb_books = {}
                for t in bb_data["result"]["list"]:
                    if t["symbol"] in pairs:
                        bb_books[t["symbol"]] = {
                            "bid": float(t["bid1Price"]) if t["bid1Price"] else 0,
                            "ask": float(t["ask1Price"]) if t["ask1Price"] else 0,
                            "bid_qty": float(t["bid1Size"]) if t["bid1Size"] else 0,
                            "ask_qty": float(t["ask1Size"]) if t["ask1Size"] else 0,
                        }

                # Build synchronized snapshot documents
                now = datetime.now(timezone.utc)
                docs = []
                for sym in pairs:
                    bn = bn_books.get(sym)
                    bb = bb_books.get(sym)
                    if not bn or not bb or bn["ask"] == 0 or bb["ask"] == 0:
                        continue

                    # Executable spread (both directions)
                    s1 = (bb["bid"] - bn["ask"]) / bn["ask"] * 10000  # BUY_BN_SELL_BB
                    s2 = (bn["bid"] - bb["ask"]) / bb["ask"] * 10000  # BUY_BB_SELL_BN

                    docs.append({
                        "symbol": sym,
                        "timestamp": now,
                        "bn_bid": bn["bid"],
                        "bn_ask": bn["ask"],
                        "bn_bid_qty": bn["bid_qty"],
                        "bn_ask_qty": bn["ask_qty"],
                        "bb_bid": bb["bid"],
                        "bb_ask": bb["ask"],
                        "bb_bid_qty": bb["bid_qty"],
                        "bb_ask_qty": bb["ask_qty"],
                        "spread_buy_bn": round(s1, 2),  # BUY_BN_SELL_BB spread
                        "spread_buy_bb": round(s2, 2),  # BUY_BB_SELL_BN spread
                        "best_spread": round(max(s1, s2), 2),
                        "best_direction": "BUY_BN_SELL_BB" if s1 >= s2 else "BUY_BB_SELL_BN",
                        "fetch_latency_ms": round((bb_ts - bn_ts) * 1000, 1),
                    })

                if docs:
                    coll.insert_many(docs)
                    poll_count += 1

                    # Log every 60 polls (~5 min at 5s interval)
                    if poll_count % 60 == 0:
                        # Quick stats
                        wide = sum(1 for d in docs if d["best_spread"] >= 30)
                        avg_spread = sum(d["best_spread"] for d in docs) / len(docs)
                        logger.info(
                            f"Poll #{poll_count}: {len(docs)} pairs, "
                            f"avg spread {avg_spread:.1f}bp, "
                            f"{wide} pairs >30bp, "
                            f"latency {docs[0]['fetch_latency_ms']:.0f}ms"
                        )

            except Exception as e:
                error_count += 1
                logger.error(f"Poll error #{error_count}: {e}")
                if error_count > 100:
                    logger.error("Too many errors, pausing 60s")
                    await asyncio.sleep(60)
                    error_count = 0

            # Sleep for remaining interval
            elapsed = time.time() - t0
            sleep_time = max(0, interval - elapsed)
            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logger.info(f"Quote collector stopped. Total polls: {poll_count}")
    finally:
        await session.close()


def main():
    parser = argparse.ArgumentParser(description="Arb quote collector for R1")
    parser.add_argument("--pairs", type=int, default=15, help="Number of top pairs to collect")
    parser.add_argument("--interval", type=float, default=5.0, help="Poll interval in seconds")
    args = parser.parse_args()

    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
    mongo_db = os.getenv("MONGO_DATABASE", "quants_lab")
    db = MongoClient(mongo_uri)[mongo_db]

    pairs = load_top_pairs(args.pairs)

    # Graceful shutdown
    loop = asyncio.new_event_loop()
    task = loop.create_task(poll_quotes(pairs, args.interval, db))

    def shutdown(sig, frame):
        logger.info(f"Received {sig}, shutting down...")
        task.cancel()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()
        logger.info("Quote collector finished")


if __name__ == "__main__":
    main()
