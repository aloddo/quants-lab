#!/usr/bin/env python3
"""
HL Shitcoin Market Making Engine V2 -- Live runner.

Usage:
  # Load env and run with ORDI (default)
  set -a && source .env && set +a
  python scripts/hl_mm_v2_live.py

  # Dry run mode (logs decisions, no orders)
  python scripts/hl_mm_v2_live.py --dry-run

  # Specific coins, custom leverage
  python scripts/hl_mm_v2_live.py --coins ORDI,BIO --leverage 5

  # All options
  python scripts/hl_mm_v2_live.py --coins ORDI --leverage 5 --dry-run --log-level DEBUG

Environment:
  HL_PRIVATE_KEY  -- Hyperliquid agent wallet private key
  HL_ADDRESS      -- Main account address (0x11ca...)
  MONGO_URI       -- MongoDB connection string (default: mongodb://localhost:27017/quants_lab)
"""
import argparse
import asyncio
import logging
import os
import signal
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.hl_mm.orchestrator import HLMarketMaker


def load_env():
    """Load .env file if it exists."""
    env_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"
    )
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, _, val = line.partition("=")
                    key = key.strip()
                    val = val.strip().strip("'\"")
                    if key and val:
                        os.environ.setdefault(key, val)


def main():
    parser = argparse.ArgumentParser(
        description="HL Shitcoin Market Making Engine V2",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--coins", default="ORDI",
        help="Comma-separated initial coins to trade (default: ORDI)",
    )
    parser.add_argument(
        "--leverage", type=int, default=5,
        help="Leverage per coin (default: 5)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log decisions without placing orders",
    )
    parser.add_argument(
        "--mongo-uri", default=None,
        help="MongoDB URI (default: from MONGO_URI env or localhost)",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    args = parser.parse_args()

    # Load environment
    load_env()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s.%(msecs)03d [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
        ],
    )
    # Reduce noise from libraries
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Parse coins
    coins = [c.strip().upper() for c in args.coins.split(",") if c.strip()]
    if not coins:
        print("ERROR: no coins specified")
        sys.exit(1)

    # MongoDB URI
    mongo_uri = args.mongo_uri or os.environ.get(
        "MONGO_URI", "mongodb://localhost:27017/quants_lab"
    )

    # Validate credentials
    if not os.environ.get("HL_PRIVATE_KEY"):
        print("ERROR: HL_PRIVATE_KEY not set")
        sys.exit(1)
    if not os.environ.get("HL_ADDRESS"):
        print("ERROR: HL_ADDRESS not set")
        sys.exit(1)

    # Print config
    address = os.environ.get("HL_ADDRESS", "")
    print("=" * 60)
    print("  HL SHITCOIN MARKET MAKER V2")
    print("=" * 60)
    print(f"  Address:   {address[:8]}...{address[-4:]}")
    print(f"  Coins:     {coins}")
    print(f"  Leverage:  {args.leverage}x")
    print(f"  Dry run:   {args.dry_run}")
    print(f"  MongoDB:   {mongo_uri}")
    print(f"  Log level: {args.log_level}")
    print("=" * 60)

    if args.dry_run:
        print("\n  *** DRY RUN MODE -- no orders will be placed ***\n")

    # Create engine
    mm = HLMarketMaker(
        initial_coins=coins,
        leverage=args.leverage,
        mongo_uri=mongo_uri,
        dry_run=args.dry_run,
    )

    # Handle signals for graceful shutdown
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def handle_signal(signum, frame):
        print(f"\nReceived signal {signum}, shutting down...")
        mm.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # Run
    try:
        loop.run_until_complete(mm.run())
    except KeyboardInterrupt:
        print("\nKeyboard interrupt, shutting down...")
        mm.stop()
    finally:
        # Give shutdown tasks a chance to complete
        pending = asyncio.all_tasks(loop)
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()

    print("Engine stopped.")


if __name__ == "__main__":
    main()
