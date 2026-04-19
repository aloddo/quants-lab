"""
Dual-venue spread collector — stores Bybit spot-perp + Binance USDC-Bybit perp spreads.

Polls every 5s. Writes to MongoDB:
- arb_bb_spot_perp_snapshots: Bybit spot vs perp spreads (all common pairs)
- arb_bn_usdc_bb_perp_snapshots: Binance USDC vs Bybit perp spreads (all matching pairs)

Run alongside arb_h2_paper.py (which tracks Binance USDT vs Bybit perp for existing pairs).

Usage:
    set -a && source .env && set +a
    python scripts/arb_dual_collector.py
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
        logging.FileHandler("/tmp/arb-dual-collector.log"),
    ],
)
logger = logging.getLogger(__name__)

POLL_INTERVAL = 5.0
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")


class DualVenueCollector:
    def __init__(self):
        db_name = MONGO_URI.rsplit("/", 1)[-1]
        client = MongoClient(MONGO_URI)
        db = client[db_name]

        self._bb_coll = db["arb_bb_spot_perp_snapshots"]
        self._bn_coll = db["arb_bn_usdc_bb_perp_snapshots"]

        # Create TTL indexes (30 days)
        self._bb_coll.create_index("timestamp", expireAfterSeconds=30 * 86400)
        self._bn_coll.create_index("timestamp", expireAfterSeconds=30 * 86400)
        # Query indexes
        self._bb_coll.create_index([("symbol", 1), ("timestamp", -1)])
        self._bn_coll.create_index([("symbol", 1), ("timestamp", -1)])

        self._running = True
        self._poll_count = 0

        # Pair mappings (populated on first poll)
        self.bb_common: set = set()       # Bybit symbols with both spot + perp
        self.bn_usdc_map: dict = {}       # Binance USDC symbol -> Bybit perp symbol

    async def _discover_pairs(self, session: aiohttp.ClientSession):
        """Discover matching pairs on both venues."""
        # Bybit spot symbols
        async with session.get("https://api.bybit.com/v5/market/instruments-info?category=spot") as resp:
            data = await resp.json()
        bb_spot = {s["symbol"] for s in data["result"]["list"] if s["status"] == "Trading"}

        # Bybit perp symbols
        async with session.get("https://api.bybit.com/v5/market/instruments-info?category=linear") as resp:
            data = await resp.json()
        bb_perp = {s["symbol"] for s in data["result"]["list"] if s["status"] == "Trading"}

        self.bb_common = bb_spot & bb_perp

        # Binance USDC pairs
        async with session.get("https://api.binance.com/api/v3/exchangeInfo") as resp:
            data = await resp.json()
        for sym_info in data["symbols"]:
            if (sym_info["quoteAsset"] == "USDC"
                    and sym_info["status"] == "TRADING"
                    and sym_info["isSpotTradingAllowed"]):
                base = sym_info["baseAsset"]
                bb_sym = base + "USDT"
                if bb_sym in bb_perp:
                    self.bn_usdc_map[sym_info["symbol"]] = bb_sym

        logger.info(f"Discovered: {len(self.bb_common)} Bybit spot+perp, {len(self.bn_usdc_map)} Binance USDC+Bybit perp")

    async def run(self):
        logger.info("Dual-venue spread collector starting...")

        async with aiohttp.ClientSession() as session:
            await self._discover_pairs(session)

            while self._running:
                t0 = time.time()
                self._poll_count += 1

                try:
                    await self._poll_and_store(session)
                except Exception as e:
                    logger.error(f"Poll error: {e}")

                # Status every 5 min
                if self._poll_count % 60 == 0:
                    mins = self._poll_count * POLL_INTERVAL / 60
                    bb_count = self._bb_coll.estimated_document_count()
                    bn_count = self._bn_coll.estimated_document_count()
                    logger.info(f"[{mins:.0f}m] poll#{self._poll_count} | BB docs: {bb_count:,} | BN docs: {bn_count:,}")

                elapsed = time.time() - t0
                await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))

    async def _poll_and_store(self, session: aiohttp.ClientSession):
        now = datetime.now(timezone.utc)

        # Fetch all tickers concurrently
        bb_spot_resp, bb_perp_resp, bn_resp = await asyncio.gather(
            session.get("https://api.bybit.com/v5/market/tickers?category=spot"),
            session.get("https://api.bybit.com/v5/market/tickers?category=linear"),
            session.get("https://api.binance.com/api/v3/ticker/bookTicker"),
        )

        bb_spot_data = await bb_spot_resp.json()
        bb_perp_data = await bb_perp_resp.json()
        bn_data = await bn_resp.json()

        bb_s = {t["symbol"]: {"bid": float(t.get("bid1Price", 0) or 0), "ask": float(t.get("ask1Price", 0) or 0)}
                for t in bb_spot_data["result"]["list"]}
        bb_p = {t["symbol"]: {"bid": float(t.get("bid1Price", 0) or 0), "ask": float(t.get("ask1Price", 0) or 0)}
                for t in bb_perp_data["result"]["list"]}
        bn_t = {b["symbol"]: {"bid": float(b["bidPrice"]), "ask": float(b["askPrice"])}
                for b in bn_data}

        # ── Bybit spot vs perp ──
        bb_docs = []
        for sym in self.bb_common:
            if sym not in bb_s or sym not in bb_p:
                continue
            s, p = bb_s[sym], bb_p[sym]
            if s["bid"] <= 0 or p["ask"] <= 0:
                continue

            # Both directions
            spread_buy_perp = (s["bid"] - p["ask"]) / p["ask"] * 10000
            spread_buy_spot = (p["bid"] - s["ask"]) / s["ask"] * 10000
            best = max(spread_buy_perp, spread_buy_spot)

            bb_docs.append({
                "symbol": sym,
                "timestamp": now,
                "spot_bid": s["bid"],
                "spot_ask": s["ask"],
                "perp_bid": p["bid"],
                "perp_ask": p["ask"],
                "spread_buy_perp_sell_spot": round(spread_buy_perp, 2),
                "spread_buy_spot_sell_perp": round(spread_buy_spot, 2),
                "best_spread": round(best, 2),
                "direction": "BUY_PERP_SELL_SPOT" if spread_buy_perp > spread_buy_spot else "BUY_SPOT_SELL_PERP",
            })

        if bb_docs:
            self._bb_coll.insert_many(bb_docs)

        # ── Binance USDC vs Bybit perp ──
        bn_docs = []
        for bn_sym, bb_sym in self.bn_usdc_map.items():
            if bn_sym not in bn_t or bb_sym not in bb_p:
                continue
            bn, p = bn_t[bn_sym], bb_p[bb_sym]
            if bn["bid"] <= 0 or p["ask"] <= 0:
                continue

            spread_buy_perp = (bn["bid"] - p["ask"]) / p["ask"] * 10000
            spread_buy_spot = (p["bid"] - bn["ask"]) / bn["ask"] * 10000
            best = max(spread_buy_perp, spread_buy_spot)

            bn_docs.append({
                "symbol_bn": bn_sym,
                "symbol_bb": bb_sym,
                "timestamp": now,
                "bn_bid": bn["bid"],
                "bn_ask": bn["ask"],
                "bb_perp_bid": p["bid"],
                "bb_perp_ask": p["ask"],
                "spread_buy_perp_sell_bn": round(spread_buy_perp, 2),
                "spread_buy_bn_sell_perp": round(spread_buy_spot, 2),
                "best_spread": round(best, 2),
                "direction": "BUY_PERP_SELL_BN" if spread_buy_perp > spread_buy_spot else "BUY_BN_SELL_PERP",
            })

        if bn_docs:
            self._bn_coll.insert_many(bn_docs)

    def stop(self):
        self._running = False


def main():
    collector = DualVenueCollector()

    def handle_signal(sig, frame):
        logger.info(f"Signal {sig}, stopping...")
        collector.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    asyncio.run(collector.run())


if __name__ == "__main__":
    main()
