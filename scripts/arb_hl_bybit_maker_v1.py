#!/usr/bin/env python3
"""
HL-Bybit Maker Arb V1 — rests ALO orders on HL, hedges on BB taker.

Usage:
  python scripts/arb_hl_bybit_maker_v1.py --live --confirm-live --position-usd 20
"""
import argparse
import asyncio
import logging
import os
import signal
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

from eth_account import Account
from hyperliquid.exchange import Exchange

from app.services.arb_hlbb.config import ArbConfig
from app.services.arb_hlbb.instrument_rules import InstrumentManager
from app.services.arb_hlbb.maker_engine import MakerArbEngine
from app.services.arb_hlbb.order_api import BybitOrderAPI
from app.services.arb_hlbb.price_feed import DualPriceFeed
from app.services.arb_hlbb.signal_engine import SignalEngine

logger = logging.getLogger("arb_hlbb_maker")


def main():
    parser = argparse.ArgumentParser(description="HL-Bybit Maker Arb V1")
    parser.add_argument("--live", action="store_true", help="Real execution")
    parser.add_argument("--confirm-live", action="store_true", help="Confirm real money")
    parser.add_argument("--position-usd", type=float, default=20.0)
    parser.add_argument("--pairs", type=str, default=None, help="Comma-separated pairs")
    args = parser.parse_args()

    if args.live and not args.confirm_live:
        print("ERROR: --live requires --confirm-live")
        sys.exit(1)

    if not args.live:
        print("ERROR: maker engine only supports --live mode (needs real ALO orders)")
        sys.exit(1)

    # Logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    )

    # Config
    config = ArbConfig(position_usd=args.position_usd)
    config.telegram_chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if args.pairs:
        config.default_pairs = [p.strip() for p in args.pairs.split(",")]

    logger.info("=" * 60)
    logger.info("HL-Bybit Maker Arb V1 — LIVE")
    logger.info(f"Pairs: {len(config.default_pairs)}")
    logger.info(f"Position: ${config.position_usd}/side")
    logger.info(f"Fee RT: 16.76bp (HL maker entry + BB taker)")
    logger.info("=" * 60)

    # HL setup
    hl_private_key = os.environ.get("HL_PRIVATE_KEY")
    hl_address = os.environ.get("HL_QUERY_ADDRESS",
                                "0x11ca20aeb7cd014cf8406560ae405b12601994b4")
    if not hl_private_key:
        logger.error("HL_PRIVATE_KEY not set")
        sys.exit(1)

    wallet = Account.from_key(hl_private_key)

    # HL Exchange init with retry (constructor makes REST calls that can 429)
    # FIX #10: removed unused hl_info
    for attempt in range(5):
        try:
            hl_exchange = Exchange(wallet, base_url=config.hl_rest_url)
            break
        except Exception as e:
            wait = 10 * (attempt + 1)
            logger.warning(f"HL init failed (attempt {attempt+1}): {e}. Retrying in {wait}s...")
            import time as _t; _t.sleep(wait)
    else:
        logger.error("HL init failed after 5 attempts")
        sys.exit(1)

    # BB setup (reads API keys from env vars internally)
    bb_api = BybitOrderAPI(
        fill_poll_attempts=config.bybit_fill_poll_attempts,
        fill_poll_delay_s=config.bybit_fill_poll_delay_s,
    )

    # Instrument rules
    instrument_mgr = InstrumentManager()
    instrument_mgr.fetch_rules(config.default_pairs)

    # M5 FIX: BB preflight (position mode + leverage)
    bb_symbols = [p.replace("-", "") for p in config.default_pairs]
    ok, errors = bb_api.preflight_check(bb_symbols, leverage=config.leverage)
    if not ok:
        logger.error(f"BB preflight failed: {errors}")
        sys.exit(1)
    logger.info("BB preflight passed (one-way mode, leverage set)")

    # Price feed
    price_feed = DualPriceFeed(
        pairs=config.default_pairs,
        hl_ws_url=config.hl_ws_url,
        bb_ws_url=config.bb_ws_url,
    )

    # Signal engine
    signal_engine = SignalEngine(config)
    signal_engine.seed(config.default_pairs)

    # Maker engine
    engine = MakerArbEngine(
        config=config,
        price_feed=price_feed,
        signal_engine=signal_engine,
        instrument_mgr=instrument_mgr,
        bb_api=bb_api,
        hl_exchange=hl_exchange,
        hl_address=hl_address,
    )

    # Signal handler
    shutdown_event = asyncio.Event()

    def handle_signal(signum, frame):
        logger.info(f"Signal {signum} received — shutting down")
        shutdown_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Run
    async def run():
        # Start price feeds
        price_feed.start()
        logger.info("Waiting for price feed warmup (5s)...")
        await asyncio.sleep(5)

        metrics = price_feed.get_metrics()
        logger.info(f"Feed status: HL={metrics['hl_updates']} BB={metrics['bb_updates']} pairs={metrics['pairs_with_data']}")

        # Start engine
        engine_task = asyncio.create_task(engine.run())

        # C4 FIX: shutdown() first (cancels orders, drains fills, closes positions)
        # THEN cancel the engine task (kills the event loops)
        await shutdown_event.wait()
        await engine.shutdown()  # cancels orders + hedges pending fills + closes positions
        engine_task.cancel()     # now safe to kill loops
        try:
            await engine_task
        except asyncio.CancelledError:
            pass
        price_feed.stop()

    asyncio.run(run())


if __name__ == "__main__":
    main()
