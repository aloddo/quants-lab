"""
HL Wallet Trade Collector — raw data for metaorder signal validation.

Subscribes to trade feeds on the top N coins by volume from Hyperliquid.
Records every trade with wallet addresses to MongoDB for offline analysis.

No trading, no signals, no risk. Just data.

Collections:
  hl_wallet_trades: {coin, timestamp, side, price, size, notional, buyer, seller, trade_hash}
  hl_wallet_snapshots: periodic stats (coins monitored, trades/min, unique wallets)

Usage:
  python scripts/hl_wallet_collector.py [--coins 50] [--min-volume 100000]
"""
import argparse
import asyncio
import logging
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timezone

from hyperliquid.info import Info
from pymongo import MongoClient, UpdateOne

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("hl_wallet_collector")


class WalletTradeCollector:
    def __init__(self, mongo_uri: str, top_n: int = 50, min_daily_volume: float = 100_000):
        self.top_n = top_n
        self.min_daily_volume = min_daily_volume

        # MongoDB
        client = MongoClient(mongo_uri)
        db_name = mongo_uri.split("/")[-1]
        self._db = client[db_name]
        self._trades_col = self._db["hl_wallet_trades"]
        self._snapshots_col = self._db["hl_wallet_snapshots"]

        # Ensure indexes
        self._trades_col.create_index([("coin", 1), ("timestamp", 1)])
        self._trades_col.create_index([("buyer", 1)])
        self._trades_col.create_index([("seller", 1)])
        self._trades_col.create_index("trade_hash", unique=True, sparse=True)

        # SDK
        self.info = Info(skip_ws=True)

        # Buffer for batch writes
        self._buffer: list[dict] = []
        self._buffer_lock = __import__("threading").Lock()
        self._flush_interval = 10.0  # flush every 10s
        self._last_flush = time.time()

        # Stats
        self._trades_received = 0
        self._trades_written = 0
        self._unique_wallets: set[str] = set()
        self._start_time = 0.0
        self._monitoring_coins: set[str] = set()
        self._running = False

    def _get_top_coins(self) -> list[str]:
        """Get top N coins by 24h volume from HL meta."""
        meta = self.info.meta_and_asset_ctxs()
        if not meta or len(meta) < 2:
            return []
        universe = meta[0].get("universe", [])
        contexts = meta[1]
        pairs = []
        for pair_info, ctx in zip(universe, contexts):
            coin = pair_info.get("name", "")
            daily_vol = float(ctx.get("dayNtlVlm", 0) or 0)
            if daily_vol >= self.min_daily_volume:
                pairs.append((coin, daily_vol))
        pairs.sort(key=lambda x: x[1], reverse=True)
        return [coin for coin, _ in pairs[:self.top_n]]

    def _on_trades(self, coin: str, data) -> None:
        """Callback for HL WS trade updates."""
        try:
            trades = []
            if isinstance(data, list):
                trades = data
            elif isinstance(data, dict) and "data" in data:
                trades = data["data"]
            elif isinstance(data, dict):
                trades = [data]

            now = time.time()
            docs = []
            for trade in trades:
                users = trade.get("users", [])
                if not users or len(users) < 2:
                    continue
                buyer = users[0] if isinstance(users[0], str) else ""
                seller = users[1] if isinstance(users[1], str) else ""
                if not buyer and not seller:
                    continue

                side = trade.get("side", "")
                price = float(trade.get("px", 0) or 0)
                size = float(trade.get("sz", 0) or 0)
                exchange_time = float(trade.get("time", 0) or 0) / 1000.0
                trade_hash = trade.get("hash", "")

                if price <= 0 or size <= 0:
                    continue

                # Drop stale replay trades (>120s old)
                if exchange_time > 0 and now - exchange_time > 120.0:
                    continue

                docs.append({
                    "coin": coin,
                    "timestamp": exchange_time if exchange_time > 0 else now,
                    "side": side,
                    "price": price,
                    "size": size,
                    "notional": price * size,
                    "buyer": buyer,
                    "seller": seller,
                    "trade_hash": trade_hash or None,
                    "recorded_at": now,
                })

                self._unique_wallets.add(buyer)
                self._unique_wallets.add(seller)

            if docs:
                self._trades_received += len(docs)
                with self._buffer_lock:
                    self._buffer.extend(docs)

        except Exception as e:
            logger.debug(f"Trade parse error for {coin}: {e}")

    def _flush_buffer(self) -> None:
        """Write buffered trades to MongoDB."""
        with self._buffer_lock:
            if not self._buffer:
                return
            batch = list(self._buffer)
            self._buffer.clear()

        if not batch:
            return

        try:
            # Use trade_hash for dedup where available
            ops = []
            for doc in batch:
                if doc.get("trade_hash"):
                    ops.append(UpdateOne(
                        {"trade_hash": doc["trade_hash"]},
                        {"$setOnInsert": doc},
                        upsert=True,
                    ))
                else:
                    ops.append(UpdateOne(
                        {"coin": doc["coin"], "timestamp": doc["timestamp"],
                         "buyer": doc["buyer"], "seller": doc["seller"],
                         "price": doc["price"], "size": doc["size"]},
                        {"$setOnInsert": doc},
                        upsert=True,
                    ))
            result = self._trades_col.bulk_write(ops, ordered=False)
            self._trades_written += result.upserted_count
        except Exception as e:
            logger.error(f"MongoDB flush failed: {e}")

    def _log_stats(self) -> None:
        """Log periodic stats."""
        uptime = time.time() - self._start_time
        rate = self._trades_received / max(uptime, 1) * 60
        logger.info(
            f"Collector: {len(self._monitoring_coins)} coins, "
            f"{self._trades_received} received ({rate:.0f}/min), "
            f"{self._trades_written} written, "
            f"{len(self._unique_wallets)} unique wallets, "
            f"uptime={uptime/60:.1f}min"
        )
        # Write snapshot
        self._snapshots_col.insert_one({
            "timestamp": time.time(),
            "coins_monitored": len(self._monitoring_coins),
            "trades_received": self._trades_received,
            "trades_written": self._trades_written,
            "unique_wallets": len(self._unique_wallets),
            "trades_per_min": rate,
            "uptime_s": uptime,
        })

    async def run(self) -> None:
        """Main loop."""
        self._running = True
        self._start_time = time.time()

        # Connect WS
        self.info = Info(skip_ws=False)

        # Get initial coin list
        coins = self._get_top_coins()
        if not coins:
            logger.error("No coins found, exiting")
            return

        # Subscribe to trades for each coin
        for coin in coins:
            self.info.subscribe(
                {"type": "trades", "coin": coin},
                lambda data, c=coin: self._on_trades(c, data),
            )
            self._monitoring_coins.add(coin)

        logger.info(f"Subscribed to {len(coins)} coins: {coins[:10]}...")

        # Main loop: flush buffer + refresh coins periodically
        last_refresh = time.time()
        last_stats = time.time()

        while self._running:
            now = time.time()

            # Flush buffer
            if now - self._last_flush >= self._flush_interval:
                self._flush_buffer()
                self._last_flush = now

            # Log stats every 5 min
            if now - last_stats >= 300:
                self._log_stats()
                last_stats = now

            # Refresh coin list every 15 min
            if now - last_refresh >= 900:
                new_coins = set(self._get_top_coins())
                to_add = new_coins - self._monitoring_coins
                to_remove = self._monitoring_coins - new_coins
                for coin in to_add:
                    self.info.subscribe(
                        {"type": "trades", "coin": coin},
                        lambda data, c=coin: self._on_trades(c, data),
                    )
                for coin in to_remove:
                    try:
                        self.info.unsubscribe({"type": "trades", "coin": coin}, None)
                    except Exception:
                        pass
                if to_add or to_remove:
                    logger.info(f"Coin refresh: +{len(to_add)} -{len(to_remove)} = {len(new_coins)}")
                self._monitoring_coins = new_coins
                last_refresh = now

            await asyncio.sleep(1.0)

        # Final flush
        self._flush_buffer()
        self._log_stats()
        logger.info("Collector stopped")

    def stop(self) -> None:
        self._running = False


def main():
    parser = argparse.ArgumentParser(description="HL Wallet Trade Collector")
    parser.add_argument("--coins", type=int, default=50, help="Top N coins by volume")
    parser.add_argument("--min-volume", type=float, default=100_000, help="Minimum daily volume USD")
    args = parser.parse_args()

    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")

    collector = WalletTradeCollector(
        mongo_uri=mongo_uri,
        top_n=args.coins,
        min_daily_volume=args.min_volume,
    )

    # Handle shutdown
    def shutdown(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        collector.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"""
============================================================
  HL WALLET TRADE COLLECTOR
============================================================
  Top coins: {args.coins}
  Min daily volume: ${args.min_volume:,.0f}
  MongoDB: {mongo_uri}
============================================================
  Pure data collection — no trading, no risk.
  Press Ctrl+C to stop.
============================================================
""")

    # Restart loop: auto-recover from WebSocket crashes
    max_backoff = 300  # 5 min max
    backoff = 10
    while True:
        try:
            asyncio.run(collector.run())
            # Clean exit (Ctrl+C / SIGTERM) — don't restart
            break
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Collector crashed: {e}. Restarting in {backoff}s...")
            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
            # Re-create collector (fresh WS connection)
            collector = WalletTradeCollector(
                mongo_uri=mongo_uri,
                top_n=args.coins,
                min_daily_volume=args.min_volume,
            )


if __name__ == "__main__":
    main()
