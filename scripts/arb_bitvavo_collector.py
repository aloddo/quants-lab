"""
Bitvavo triangular spread collector — captures Bitvavo EUR spot vs Bybit USDT perp spreads.

Triangular arb concept:
  1. Buy token on Bitvavo (EUR) — or sell
  2. Sell token on Bybit perp (USDT) — or buy
  3. EUR/USD FX rate closes the triangle

Polls every 5s. Writes to MongoDB:
- arb_bv_eur_bb_perp_snapshots: Bitvavo EUR vs Bybit USDT perp (all matching pairs)

Also captures EUR/USD rate for proper spread calculation.

Usage:
    set -a && source .env && set +a
    python scripts/arb_bitvavo_collector.py
"""
import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
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
        logging.FileHandler("/tmp/arb-bitvavo-collector.log"),
    ],
)
logger = logging.getLogger(__name__)

POLL_INTERVAL = 5.0
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")


class BitvavoCollector:
    def __init__(self):
        db_name = MONGO_URI.rsplit("/", 1)[-1]
        client = MongoClient(MONGO_URI)
        db = client[db_name]

        self._coll = db["arb_bv_eur_bb_perp_snapshots"]
        self._fx_coll = db["arb_eurusd_snapshots"]

        # TTL 30 days
        self._coll.create_index("timestamp", expireAfterSeconds=30 * 86400)
        self._fx_coll.create_index("timestamp", expireAfterSeconds=30 * 86400)
        # Query indexes
        self._coll.create_index([("symbol_bv", 1), ("timestamp", -1)])

        self._running = True
        self._poll_count = 0
        self._eur_usd_rate = 0.0
        self._last_fx_update = 0.0

        # Pair mappings
        self.bv_bb_map: dict = {}  # Bitvavo symbol -> Bybit perp symbol

    async def _discover_pairs(self, session: aiohttp.ClientSession):
        """Find all Bitvavo EUR pairs that have a matching Bybit USDT perp."""
        # Bitvavo markets
        async with session.get("https://api.bitvavo.com/v2/markets") as resp:
            bv_markets = await resp.json()
        bv_eur = {m["market"]: m["base"] for m in bv_markets
                  if m["quote"] == "EUR" and m["status"] == "trading"}

        # Bybit perp symbols
        async with session.get("https://api.bybit.com/v5/market/instruments-info?category=linear") as resp:
            data = await resp.json()
        bb_perp = {s["symbol"] for s in data["result"]["list"] if s["status"] == "Trading"}

        # Match: Bitvavo BASE-EUR <-> Bybit BASEUSDT
        for bv_sym, base in bv_eur.items():
            bb_sym = base + "USDT"
            if bb_sym in bb_perp:
                self.bv_bb_map[bv_sym] = bb_sym

        logger.info(f"Discovered: {len(bv_eur)} Bitvavo EUR pairs, {len(self.bv_bb_map)} matched with Bybit perps")

    async def _update_fx_rate(self, session: aiohttp.ClientSession):
        """Fetch EUR/USD rate. Updates every 60s."""
        now = time.time()
        if now - self._last_fx_update < 60 and self._eur_usd_rate > 0:
            return

        # Use Bitvavo's own BTC-EUR and a USD BTC price to derive EUR/USD
        # Or use a free FX API
        try:
            # Method 1: Derive from BTC prices on both venues
            # Bitvavo BTC-EUR ticker
            async with session.get("https://api.bitvavo.com/v2/ticker/book?market=BTC-EUR") as resp:
                bv_btc = await resp.json()
            bv_mid = (float(bv_btc["bid"]) + float(bv_btc["ask"])) / 2

            # Bybit BTC-USDT ticker
            async with session.get("https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT") as resp:
                data = await resp.json()
            bb_item = data["result"]["list"][0]
            bb_mid = (float(bb_item["bid1Price"]) + float(bb_item["ask1Price"])) / 2

            if bv_mid > 0 and bb_mid > 0:
                # EUR/USD = BTCUSD / BTCEUR
                self._eur_usd_rate = bb_mid / bv_mid
                self._last_fx_update = now

                # Store FX snapshot
                self._fx_coll.insert_one({
                    "timestamp": datetime.now(timezone.utc),
                    "eur_usd": round(self._eur_usd_rate, 6),
                    "btc_eur_mid": round(bv_mid, 2),
                    "btc_usd_mid": round(bb_mid, 2),
                })

                if self._poll_count % 60 == 0:
                    logger.info(f"EUR/USD rate: {self._eur_usd_rate:.4f} (BTC: EUR={bv_mid:.0f} USD={bb_mid:.0f})")

        except Exception as e:
            logger.warning(f"FX rate update failed: {e}")

    async def run(self):
        logger.info("Bitvavo triangular spread collector starting...")

        async with aiohttp.ClientSession() as session:
            await self._discover_pairs(session)
            await self._update_fx_rate(session)

            if self._eur_usd_rate <= 0:
                logger.error("Cannot determine EUR/USD rate. Exiting.")
                return

            while self._running:
                t0 = time.time()
                self._poll_count += 1

                try:
                    await self._update_fx_rate(session)
                    await self._poll_and_store(session)
                except Exception as e:
                    logger.error(f"Poll error: {e}")

                if self._poll_count % 60 == 0:
                    mins = self._poll_count * POLL_INTERVAL / 60
                    count = self._coll.estimated_document_count()
                    logger.info(f"[{mins:.0f}m] poll#{self._poll_count} | docs: {count:,} | EUR/USD: {self._eur_usd_rate:.4f}")

                elapsed = time.time() - t0
                await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))

    async def _poll_and_store(self, session: aiohttp.ClientSession):
        now = datetime.now(timezone.utc)
        fx = self._eur_usd_rate
        if fx <= 0:
            return

        # Fetch tickers concurrently
        bv_resp, bb_resp = await asyncio.gather(
            session.get("https://api.bitvavo.com/v2/ticker/book"),
            session.get("https://api.bybit.com/v5/market/tickers?category=linear"),
        )

        bv_data = await bv_resp.json()
        bb_data = await bb_resp.json()

        # Parse Bitvavo: {market: {bid, ask}} in EUR
        bv_t = {}
        for t in bv_data:
            if t.get("bid") and t.get("ask"):
                try:
                    bv_t[t["market"]] = {
                        "bid_eur": float(t["bid"]),
                        "ask_eur": float(t["ask"]),
                        "bid_usd": float(t["bid"]) * fx,
                        "ask_usd": float(t["ask"]) * fx,
                    }
                except (ValueError, KeyError):
                    pass

        # Parse Bybit perp: {symbol: {bid, ask}} in USDT
        bb_p = {}
        for t in bb_data["result"]["list"]:
            bid = float(t.get("bid1Price", 0) or 0)
            ask = float(t.get("ask1Price", 0) or 0)
            if bid > 0 and ask > 0:
                bb_p[t["symbol"]] = {"bid": bid, "ask": ask}

        # Compute spreads
        docs = []
        for bv_sym, bb_sym in self.bv_bb_map.items():
            if bv_sym not in bv_t or bb_sym not in bb_p:
                continue

            bv = bv_t[bv_sym]
            bb = bb_p[bb_sym]

            # Direction 1: Buy on Bitvavo (ask EUR->USD), sell perp on Bybit (bid USDT)
            # Profit if Bybit bid > Bitvavo ask (in USD)
            spread_buy_bv_sell_bb = (bb["bid"] - bv["ask_usd"]) / bv["ask_usd"] * 10000

            # Direction 2: Buy perp on Bybit (ask USDT), sell on Bitvavo (bid EUR->USD)
            # Profit if Bitvavo bid > Bybit ask (in USD)
            spread_buy_bb_sell_bv = (bv["bid_usd"] - bb["ask"]) / bb["ask"] * 10000

            best = max(spread_buy_bv_sell_bb, spread_buy_bb_sell_bv)

            docs.append({
                "symbol_bv": bv_sym,
                "symbol_bb": bb_sym,
                "timestamp": now,
                "bv_bid_eur": bv["bid_eur"],
                "bv_ask_eur": bv["ask_eur"],
                "bv_bid_usd": round(bv["bid_usd"], 8),
                "bv_ask_usd": round(bv["ask_usd"], 8),
                "bb_perp_bid": bb["bid"],
                "bb_perp_ask": bb["ask"],
                "eur_usd": round(fx, 6),
                "spread_buy_bv_sell_bb": round(spread_buy_bv_sell_bb, 2),
                "spread_buy_bb_sell_bv": round(spread_buy_bb_sell_bv, 2),
                "best_spread": round(best, 2),
                "direction": "BUY_BV_SELL_BB" if spread_buy_bv_sell_bb > spread_buy_bb_sell_bv else "BUY_BB_SELL_BV",
            })

        if docs:
            self._coll.insert_many(docs)

    def stop(self):
        self._running = False


def main():
    collector = BitvavoCollector()

    def handle_signal(sig, frame):
        logger.info(f"Signal {sig}, stopping...")
        collector.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    asyncio.run(collector.run())


if __name__ == "__main__":
    main()
