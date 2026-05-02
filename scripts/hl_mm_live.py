#!/usr/bin/env python3
"""
HL Market Making Engine V2 — Live runner.

Usage:
  export HL_PRIVATE_KEY="..." && export HL_ADDRESS="..."
  python scripts/hl_mm_live.py
  python scripts/hl_mm_live.py --coins APE --size 3
"""
import argparse
import asyncio
import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.hl_mm.orchestrator import HLMarketMaker
from app.services.hl_mm.avellaneda_quoter import ASConfig
from app.services.hl_mm.inventory_manager import InventoryConfig


def main():
    parser = argparse.ArgumentParser(description="HL AS Market Maker V2")
    parser.add_argument("--coins", default="APE", help="Comma-separated coins")
    parser.add_argument("--size", type=float, default=11.0, help="Order size USD (HL min notional ~$10)")
    parser.add_argument("--max-inv", type=float, default=10.0, help="Max inventory USD per coin")
    parser.add_argument("--min-spread", type=float, default=30.0, help="Min spread bps")
    parser.add_argument("--gamma", type=float, default=0.3, help="Risk aversion")
    parser.add_argument("--testnet", action="store_true", help="Use HL testnet")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    coins = [c.strip() for c in args.coins.split(",")]

    as_config = ASConfig(
        gamma=args.gamma,
        min_spread_bps=args.min_spread,
        max_inventory_usd=args.max_inv,
        order_size_usd=args.size,
        maker_fee_bps=1.44,
    )

    inv_config = InventoryConfig(
        soft_limit_usd=args.max_inv * 0.75,
        hard_limit_usd=args.max_inv,
        emergency_limit_usd=args.max_inv * 1.5,
        max_total_exposure_usd=args.max_inv * len(coins) * 1.2,
        max_daily_loss_usd=2.50,
        max_drawdown_usd=5.00,
    )

    mm = HLMarketMaker(
        coins=coins,
        as_config=as_config,
        inv_config=inv_config,
        testnet=args.testnet,
    )

    logging.info(f"Starting: coins={coins}, size=${args.size}, max_inv=${args.max_inv}, gamma={args.gamma}")
    asyncio.run(mm.run())


if __name__ == "__main__":
    main()
