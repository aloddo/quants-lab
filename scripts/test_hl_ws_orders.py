"""
Standalone HL WS Order Client test — isolates latency and reliability.

Tests:
1. Connection + keepalive
2. IOC order placement (tiny size, expect immediate cancel or fill)
3. REST vs WS latency comparison
4. Multiple rapid orders (stress test)

Usage:
    set -a && source .env && set +a
    python scripts/test_hl_ws_orders.py
"""
import asyncio
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Load .env
_env_file = Path(__file__).resolve().parents[1] / ".env"
if _env_file.exists():
    for line in _env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, val = line.partition("=")
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ws_order_test")


async def main():
    from eth_account import Account
    from hyperliquid.exchange import Exchange
    from hyperliquid.info import Info
    from app.services.hl_mm.ws_order_client import WSOrderClient

    hl_key = os.environ["HL_PRIVATE_KEY"]
    hl_query = os.environ.get("HL_QUERY_ADDRESS", "0x11ca20aeb7cd014cf8406560ae405b12601994b4")

    wallet = Account.from_key(hl_key)
    info = Info("https://api.hyperliquid.xyz", skip_ws=True)
    exchange = Exchange(wallet, "https://api.hyperliquid.xyz", account_address=hl_query)

    # ── Test 1: Connection ──────────────────────────────
    logger.info("=" * 60)
    logger.info("TEST 1: WS Connection")
    ws_client = WSOrderClient(wallet, exchange, is_mainnet=True)
    ok = ws_client.start()
    logger.info(f"  Connected: {ok}")
    logger.info(f"  is_connected: {ws_client.is_connected}")
    if not ok:
        logger.error("WS connection failed — cannot continue")
        return

    await asyncio.sleep(2)
    logger.info(f"  Still connected after 2s: {ws_client.is_connected}")

    # ── Test 2: WS IOC order — tiny size, will not fill (price far from market) ──
    logger.info("=" * 60)
    logger.info("TEST 2: WS IOC order (BTC, buy 0.001 @ $1 — will not fill)")

    t0 = time.time()
    result = await ws_client.place_order(
        coin="BTC", is_buy=True, sz=0.001, price=1.0,
        tif="Ioc", reduce_only=False,
    )
    rtt = (time.time() - t0) * 1000
    logger.info(f"  Result: {result}")
    logger.info(f"  RTT: {rtt:.0f}ms")

    if result is None:
        logger.error("  GOT NONE — this is the 'Empty response' bug!")
        logger.info("  Checking if WS is still connected...")
        logger.info(f"  is_connected: {ws_client.is_connected}")
    else:
        statuses = result.get("statuses", [])
        logger.info(f"  Statuses: {statuses}")

    # ── Test 3: WS IOC at market — should fill or reject immediately ──
    logger.info("=" * 60)
    logger.info("TEST 3: WS IOC at market (ETH, buy 0.01 @ $99999 — aggressive, expect fill or reject)")

    # Get current ETH price
    mids = info.all_mids()
    eth_mid = float(mids.get("ETH", 0))
    logger.info(f"  ETH mid: ${eth_mid:.2f}")

    if eth_mid > 0:
        # Buy IOC at 0.5% above mid — should fill if book has size
        aggressive_price = round(eth_mid * 1.005, 1)
        t0 = time.time()
        result2 = await ws_client.place_order(
            coin="ETH", is_buy=True, sz=0.01, price=aggressive_price,
            tif="Ioc", reduce_only=False,
        )
        rtt2 = (time.time() - t0) * 1000
        logger.info(f"  Price: ${aggressive_price}")
        logger.info(f"  Result: {result2}")
        logger.info(f"  RTT: {rtt2:.0f}ms")

        if result2 and "statuses" in result2:
            s = result2["statuses"][0] if result2["statuses"] else {}
            if "filled" in s:
                logger.info(f"  FILLED: qty={s['filled'].get('totalSz')}, avg={s['filled'].get('avgPx')}")
                # Close immediately
                logger.info("  Closing position...")
                close_result = await ws_client.place_order(
                    coin="ETH", is_buy=False, sz=0.01,
                    price=round(eth_mid * 0.995, 1),
                    tif="Ioc", reduce_only=True,
                )
                logger.info(f"  Close result: {close_result}")
            elif "error" in s:
                logger.info(f"  ERROR: {s['error']}")
            else:
                logger.info(f"  UNEXPECTED: {s}")

    # ── Test 4: REST comparison ─────────────────────────
    logger.info("=" * 60)
    logger.info("TEST 4: REST IOC (BTC, buy 0.001 @ $1 — same as test 2)")

    t0 = time.time()
    rest_result = exchange.order(
        "BTC", True, 0.001, 1.0,
        {"limit": {"tif": "Ioc"}},
        reduce_only=False,
    )
    rtt_rest = (time.time() - t0) * 1000
    logger.info(f"  Result: {rest_result}")
    logger.info(f"  RTT: {rtt_rest:.0f}ms")

    # ── Test 5: Rapid-fire WS orders ────────────────────
    logger.info("=" * 60)
    logger.info("TEST 5: Rapid-fire 5 WS IOC orders (BTC @ $1, will not fill)")

    rtts = []
    nones = 0
    for i in range(5):
        t0 = time.time()
        r = await ws_client.place_order(
            coin="BTC", is_buy=True, sz=0.001, price=1.0,
            tif="Ioc", reduce_only=False,
        )
        rtt = (time.time() - t0) * 1000
        rtts.append(rtt)
        if r is None:
            nones += 1
        logger.info(f"  Order {i+1}: {rtt:.0f}ms, result={'OK' if r else 'NONE'}")

    logger.info(f"  Avg RTT: {sum(rtts)/len(rtts):.0f}ms")
    logger.info(f"  Min/Max: {min(rtts):.0f}/{max(rtts):.0f}ms")
    logger.info(f"  None responses: {nones}/{len(rtts)}")

    # ── Summary ─────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("SUMMARY:")
    logger.info(f"  WS connected: {ws_client.is_connected}")
    logger.info(f"  WS metrics: orders={ws_client.total_orders}, failures={ws_client.total_failures}")
    logger.info(f"  WS avg RTT: {ws_client.avg_rtt_ms:.0f}ms")
    logger.info(f"  REST RTT: {rtt_rest:.0f}ms")

    ws_client.stop()


if __name__ == "__main__":
    asyncio.run(main())
