"""
Test script: compare WS price feed against REST polling (paper trader style).
Run for 2-5 minutes. Log discrepancies.

Usage:
    MONGO_URI=... python scripts/test_price_feed.py
"""
import asyncio
import json
import logging
import sys
import time

import aiohttp

sys.path.insert(0, ".")
from app.services.arb.price_feed import PriceFeed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

PAIRS = ["HIGHUSDT", "NOMUSDT", "ALICEUSDT"]
TEST_DURATION = 120  # 2 minutes


async def rest_poll(session: aiohttp.ClientSession, pairs: list[str]) -> dict:
    """Paper-trader-style REST polling (for comparison)."""
    try:
        bn_resp, bb_resp = await asyncio.gather(
            session.get("https://api.binance.com/api/v3/ticker/bookTicker"),
            session.get("https://api.bybit.com/v5/market/tickers?category=linear"),
        )
        bn_data = await bn_resp.json()
        bb_data = await bb_resp.json()

        bn = {b["symbol"]: {"bid": float(b["bidPrice"]), "ask": float(b["askPrice"])}
              for b in bn_data if b["symbol"] in pairs}
        bb = {t["symbol"]: {"bid": float(t["bid1Price"]), "ask": float(t["ask1Price"])}
              for t in bb_data["result"]["list"]
              if t["symbol"] in pairs and t["bid1Price"]}
        return {"bn": bn, "bb": bb}
    except Exception as e:
        logger.error(f"REST poll error: {e}")
        return {}


async def main():
    feed = PriceFeed(PAIRS)

    async with aiohttp.ClientSession() as session:
        await feed.start(session)

        # Wait for initial data
        logger.info("Waiting 5s for WS feeds to warm up...")
        await asyncio.sleep(5)

        start = time.time()
        poll_count = 0
        discrepancies = 0

        while time.time() - start < TEST_DURATION:
            poll_count += 1

            # Get REST snapshot
            rest = await rest_poll(session, PAIRS)

            for sym in PAIRS:
                ws_snap = feed.get_spread(sym)
                rest_bn = rest.get("bn", {}).get(sym)
                rest_bb = rest.get("bb", {}).get(sym)

                if not ws_snap or not rest_bn or not rest_bb:
                    continue

                # Compare WS vs REST prices
                bb_bid_diff = abs(feed.bybit.prices[sym].best_bid - rest_bb["bid"])
                bn_bid_diff = abs(feed.binance.prices[sym].best_bid - rest_bn["bid"])

                bb_pct = bb_bid_diff / rest_bb["bid"] * 10000 if rest_bb["bid"] > 0 else 0
                bn_pct = bn_bid_diff / rest_bn["bid"] * 10000 if rest_bn["bid"] > 0 else 0

                if bb_pct > 5 or bn_pct > 5:
                    discrepancies += 1
                    logger.warning(
                        f"#{poll_count} {sym} DISCREPANCY: "
                        f"BB bid WS={feed.bybit.prices[sym].best_bid} REST={rest_bb['bid']} ({bb_pct:.1f}bp) | "
                        f"BN bid WS={feed.binance.prices[sym].best_bid} REST={rest_bn['bid']} ({bn_pct:.1f}bp)"
                    )

                if poll_count % 12 == 0:  # every ~60s
                    logger.info(
                        f"{sym}: WS spread={ws_snap.spread_bps:.1f}bp "
                        f"fresh={ws_snap.fresh} "
                        f"bb_age={ws_snap.bb_age_ms:.0f}ms bn_age={ws_snap.bn_age_ms:.0f}ms "
                        f"| WS updates: bb={feed.bybit.prices[sym].update_count} bn={feed.binance.prices[sym].update_count}"
                    )

            await asyncio.sleep(5)

        # Summary
        logger.info("=" * 60)
        logger.info(f"Test complete: {poll_count} polls, {discrepancies} discrepancies (>5bp)")
        logger.info(f"Feed status:")
        for sym, status in feed.status().items():
            logger.info(f"  {sym}: bb_updates={status['bb_updates']} bn_updates={status['bn_updates']}")

        await feed.stop()


if __name__ == "__main__":
    asyncio.run(main())
