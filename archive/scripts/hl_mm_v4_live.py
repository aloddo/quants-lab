#!/usr/bin/env python3
"""
HL MM V4 — Live market maker.

One process, many pairs, proper pair screening.
Reuses proven V2/V3 modules for signal detection, fill tracking, risk management.

Usage:
    python scripts/hl_mm_v4_live.py [--pairs 5] [--size 20] [--monitor 75]
"""
import argparse
import asyncio
import logging
import os
import signal
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

from app.services.hl_mm.orchestrator_v4 import HLMMv4


def main():
    parser = argparse.ArgumentParser(description="HL MM V4 Live")
    parser.add_argument("--pairs", type=int, default=5, help="Max active pairs")
    parser.add_argument("--size", type=float, default=20.0, help="Order size USD per side")
    parser.add_argument("--monitor", type=int, default=75, help="Pairs to monitor")
    parser.add_argument("--data-only", action="store_true", help="Data collection only, no quoting")
    args = parser.parse_args()

    mm = HLMMv4(
        max_active_pairs=args.pairs,
        order_size_usd=args.size,
        monitor_pairs=args.monitor,
    )
    if args.data_only:
        mm._data_only = True
        logging.info("DATA COLLECTION MODE — no orders will be placed")

    def shutdown(sig, frame):
        logging.info("Shutdown signal received")
        mm.stop()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    asyncio.run(mm.run())


if __name__ == "__main__":
    main()
