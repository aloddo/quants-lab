#!/usr/bin/env python3
"""
Gate 4: 72h Live Run — Hardened MM Engine on HL Mainnet.

Kill criteria (any triggers immediate stop):
  - Total loss > $5 (10% of capital)
  - Any single position > $100 notional
  - Fill rate > 50 in first hour
  - Position mismatch > $10 on reconciliation
  - Fill sync blind for > 5 minutes

Launch: BIO as initial coin (best tested, 9.0bp spread, +7.6bp edge/side)
Screener will auto-discover ORDI, PNUT, etc.

Usage:
  source .env && python scripts/hl_mm_gate4_live.py
  source .env && python scripts/hl_mm_gate4_live.py --coins BIO,ORDI --dry-run
"""
import argparse
import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.hl_mm.orchestrator import HLMarketMaker


def main():
    parser = argparse.ArgumentParser(description="Gate 4: 72h Live Run")
    parser.add_argument("--coins", default="BIO", help="Comma-separated initial coins")
    parser.add_argument("--leverage", type=int, default=5, help="Leverage (default: 5x)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run (no orders)")
    parser.add_argument("--log-level", default="INFO", help="Log level")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    # Quiet noisy loggers
    logging.getLogger("websocket").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("hyperliquid").setLevel(logging.WARNING)

    coins = [c.strip() for c in args.coins.split(",")]

    print("=" * 60)
    print(f"  GATE 4: 72h LIVE RUN — HARDENED ENGINE")
    print(f"  Coins: {coins}")
    print(f"  Leverage: {args.leverage}x")
    print(f"  Dry run: {args.dry_run}")
    print(f"  Kill criteria: loss>$5, pos>$100, fills>50/hr")
    print(f"  THIS IS REAL MONEY" if not args.dry_run else "  DRY RUN MODE")
    print("=" * 60)

    mm = HLMarketMaker(
        initial_coins=coins,
        leverage=args.leverage,
        dry_run=args.dry_run,
    )

    asyncio.run(mm.run())


if __name__ == "__main__":
    main()
