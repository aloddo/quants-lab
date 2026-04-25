"""
HL-Bybit perp/perp spread collector — captures synchronized quotes for arb research.

Polls every 5s. Writes to MongoDB:
- arb_hl_bybit_perp_snapshots: HL perp vs Bybit perp bid/ask + spread (all common pairs)

This is the data foundation for the HL-Bybit perp/perp arb strategy.
Unlike the 1h candle pipeline, this captures tick-level prices needed for:
- Executable spread analysis (crossing bid/ask)
- Spread distribution and persistence (how long do wide spreads last?)
- Funding differential timing (HL 1h vs Bybit 8h)
- Entry/exit feasibility at real execution resolution

Schema (arb_hl_bybit_perp_snapshots):
  timestamp (datetime), symbol (BTCUSDT format), pair (BTC-USDT format),
  hl_bid, hl_ask, bb_bid, bb_ask,
  spread_hl_over_bb (bps, positive = HL premium),
  spread_bb_over_hl (bps, positive = BB premium),
  best_spread (bps), direction (HL_PREMIUM or BB_PREMIUM)

Usage:
    set -a && source .env && set +a
    python scripts/arb_hl_bybit_collector.py
    python scripts/arb_hl_bybit_collector.py --pairs BTC-USDT,ETH-USDT,SOL-USDT  # specific pairs
    python scripts/arb_hl_bybit_collector.py --max-pairs 30  # top N by volume
"""
import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Set, Tuple

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
        logging.FileHandler("/tmp/arb-hl-bybit-collector.log"),
    ],
)
logger = logging.getLogger(__name__)

POLL_INTERVAL = 5.0
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
COLLECTION = "arb_hl_bybit_perp_snapshots"

# HL coin name mapping (same as backfill script)
HL_TO_BYBIT = {
    "kPEPE": "1000PEPE", "kBONK": "1000BONK", "kFLOKI": "1000FLOKI",
    "kSHIB": "1000SHIB", "kLUNC": "1000LUNC", "kXEC": "1000XEC", "kSATS": "1000SATS",
}
BYBIT_TO_HL = {v: k for k, v in HL_TO_BYBIT.items()}


def _hl_coin_to_pair(coin: str) -> str:
    """Convert HL coin name to standard pair format: BTC -> BTC-USDT"""
    base = HL_TO_BYBIT.get(coin, coin)
    return f"{base}-USDT"


def _pair_to_hl_coin(pair: str) -> str:
    """Convert pair to HL coin name: BTC-USDT -> BTC"""
    base = pair.replace("-USDT", "")
    return BYBIT_TO_HL.get(base, base)


def _pair_to_symbol(pair: str) -> str:
    """Convert pair to Bybit symbol: BTC-USDT -> BTCUSDT"""
    return pair.replace("-", "")


class HLBybitCollector:
    def __init__(self, max_pairs: int = 0, specific_pairs: List[str] = None):
        db_name = MONGO_URI.rsplit("/", 1)[-1]
        client = MongoClient(MONGO_URI)
        db = client[db_name]
        self._coll = db[COLLECTION]

        # Create indexes
        self._coll.create_index("timestamp", expireAfterSeconds=90 * 86400)  # 90 day TTL
        self._coll.create_index([("symbol", 1), ("timestamp", -1)])
        self._coll.create_index([("pair", 1), ("timestamp", -1)])

        self._running = True
        self._poll_count = 0
        self._max_pairs = max_pairs
        self._specific_pairs = specific_pairs
        self._common_pairs: List[Tuple[str, str, str]] = []  # (pair, hl_coin, bb_symbol)

    async def _discover_pairs(self, session: aiohttp.ClientSession):
        """Discover pairs available on both HL and Bybit perp."""

        if self._specific_pairs:
            self._common_pairs = [
                (p, _pair_to_hl_coin(p), _pair_to_symbol(p))
                for p in self._specific_pairs
            ]
            logger.info(f"Using {len(self._common_pairs)} specified pairs")
            return

        # HL available coins
        async with session.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "meta"},
            headers={"Content-Type": "application/json"},
        ) as resp:
            hl_meta = await resp.json()
        hl_coins = {u["name"] for u in hl_meta.get("universe", [])}
        hl_pairs = {_hl_coin_to_pair(c) for c in hl_coins}

        # Bybit perp symbols
        async with session.get(
            "https://api.bybit.com/v5/market/instruments-info?category=linear"
        ) as resp:
            data = await resp.json()
        bb_perp_symbols = {
            s["symbol"] for s in data["result"]["list"]
            if s["status"] == "Trading" and s["symbol"].endswith("USDT")
        }
        bb_perp_pairs = {
            s.replace("USDT", "-USDT") if "1000" not in s else s.replace("USDT", "-USDT")
            for s in bb_perp_symbols
        }
        # Fix: handle 1000PEPE-USDT etc properly
        bb_perp_pairs_clean = set()
        for sym in bb_perp_symbols:
            if sym.endswith("USDT"):
                base = sym[:-4]
                bb_perp_pairs_clean.add(f"{base}-USDT")

        common = sorted(hl_pairs & bb_perp_pairs_clean)

        if self._max_pairs > 0:
            # Sort by Bybit 24h volume to get top pairs
            async with session.get(
                "https://api.bybit.com/v5/market/tickers?category=linear"
            ) as resp:
                ticker_data = await resp.json()
            vol_map = {}
            for t in ticker_data["result"]["list"]:
                sym = t["symbol"]
                base = sym[:-4] if sym.endswith("USDT") else sym
                pair = f"{base}-USDT"
                vol_map[pair] = float(t.get("turnover24h", 0) or 0)

            common = sorted(common, key=lambda p: vol_map.get(p, 0), reverse=True)
            common = common[:self._max_pairs]

        self._common_pairs = [
            (p, _pair_to_hl_coin(p), _pair_to_symbol(p))
            for p in common
        ]
        logger.info(f"Discovered {len(self._common_pairs)} common HL+Bybit perp pairs")

    async def run(self):
        logger.info(f"HL-Bybit perp/perp spread collector starting (poll={POLL_INTERVAL}s)...")

        async with aiohttp.ClientSession() as session:
            await self._discover_pairs(session)

            if not self._common_pairs:
                logger.error("No common pairs found!")
                return

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
                    count = self._coll.estimated_document_count()
                    logger.info(f"[{mins:.0f}m] poll#{self._poll_count} | docs: {count:,} | pairs: {len(self._common_pairs)}")

                elapsed = time.time() - t0
                await asyncio.sleep(max(0, POLL_INTERVAL - elapsed))

    async def _poll_and_store(self, session: aiohttp.ClientSession):
        now = datetime.now(timezone.utc)

        # Fetch both venues concurrently
        hl_resp, bb_resp = await asyncio.gather(
            session.post(
                "https://api.hyperliquid.xyz/info",
                json={"type": "allMids"},
                headers={"Content-Type": "application/json"},
            ),
            session.get("https://api.bybit.com/v5/market/tickers?category=linear"),
        )

        hl_data = await hl_resp.json()  # dict of coin -> mid price
        bb_ticker_data = await bb_resp.json()

        # Parse Bybit perp bid/ask
        bb_quotes: Dict[str, Dict] = {}
        for t in bb_ticker_data["result"]["list"]:
            bb_quotes[t["symbol"]] = {
                "bid": float(t.get("bid1Price", 0) or 0),
                "ask": float(t.get("ask1Price", 0) or 0),
            }

        # HL allMids returns mid prices only. For bid/ask we need the L2 book.
        # But polling L2 for 65 pairs every 5s is too heavy.
        # Compromise: use allMids as mid price, estimate bid/ask from typical spread.
        # TODO: For top pairs, also poll l2Book for real bid/ask.

        # Actually, let's fetch L2 for all pairs in batches. HL API supports batch.
        # The /info endpoint with type=l2Book requires per-coin calls.
        # For now, use mid price and note that executable spread needs L2 data.

        # Fetch L2 for all pairs in chunks via helper that properly releases responses
        async def _fetch_l2(session, hl_coin):
            """Fetch L2 book for one coin, properly releasing the response."""
            try:
                async with session.post(
                    "https://api.hyperliquid.xyz/info",
                    json={"type": "l2Book", "coin": hl_coin},
                    headers={"Content-Type": "application/json"},
                ) as resp:
                    data = await resp.json()
                    levels = data.get("levels", [[], []])
                    bids = levels[0] if len(levels) > 0 else []
                    asks = levels[1] if len(levels) > 1 else []
                    if bids and asks:
                        return {"bid": float(bids[0].get("px", 0)), "ask": float(asks[0].get("px", 0))}
            except Exception:
                pass
            return None

        hl_books: Dict[str, Dict] = {}
        chunk_size = 20
        for i in range(0, len(self._common_pairs), chunk_size):
            chunk_pairs = self._common_pairs[i:i + chunk_size]
            tasks = [_fetch_l2(session, hl_coin) for _, hl_coin, _ in chunk_pairs]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for (pair, _, _), result in zip(chunk_pairs, results):
                if isinstance(result, dict):
                    hl_books[pair] = result
            if i + chunk_size < len(self._common_pairs):
                await asyncio.sleep(0.1)

        # Build snapshot documents
        docs = []
        for pair, hl_coin, bb_sym in self._common_pairs:
            if pair not in hl_books or bb_sym not in bb_quotes:
                continue

            hl = hl_books[pair]
            bb = bb_quotes[bb_sym]

            if hl["bid"] <= 0 or hl["ask"] <= 0 or bb["bid"] <= 0 or bb["ask"] <= 0:
                continue

            # Executable spreads (crossing bid/ask)
            # Buy BB (at bb_ask), sell HL (at hl_bid) → HL premium
            spread_hl_over = (hl["bid"] - bb["ask"]) / bb["ask"] * 10000
            # Buy HL (at hl_ask), sell BB (at bb_bid) → BB premium
            spread_bb_over = (bb["bid"] - hl["ask"]) / hl["ask"] * 10000
            best = max(spread_hl_over, spread_bb_over)

            docs.append({
                "timestamp": now,
                "symbol": bb_sym,
                "pair": pair,
                "hl_bid": hl["bid"],
                "hl_ask": hl["ask"],
                "bb_bid": bb["bid"],
                "bb_ask": bb["ask"],
                "spread_hl_over_bb": round(spread_hl_over, 2),
                "spread_bb_over_hl": round(spread_bb_over, 2),
                "best_spread": round(best, 2),
                "direction": "HL_PREMIUM" if spread_hl_over > spread_bb_over else "BB_PREMIUM",
            })

        if docs:
            self._coll.insert_many(docs)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="HL-Bybit perp/perp spread collector")
    parser.add_argument("--max-pairs", type=int, default=30, help="Top N pairs by volume (0=all)")
    parser.add_argument("--pairs", default=None, help="Specific pairs (comma-separated)")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="Poll interval in seconds")
    args = parser.parse_args()

    global POLL_INTERVAL
    POLL_INTERVAL = args.poll_interval

    specific = args.pairs.split(",") if args.pairs else None
    collector = HLBybitCollector(max_pairs=args.max_pairs, specific_pairs=specific)

    loop = asyncio.new_event_loop()

    def _shutdown(sig, frame):
        logger.info(f"Received {sig}, shutting down...")
        collector._running = False

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        loop.run_until_complete(collector.run())
    except KeyboardInterrupt:
        logger.info("Interrupted")
    finally:
        loop.close()
        logger.info("Collector stopped")


if __name__ == "__main__":
    main()
