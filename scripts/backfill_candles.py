"""
Backfill historical candles for all pairs to enable regime stress tests.

Downloads 1h and 5m candles from Bybit going back to 2022.
Uses CLOBDataSource smart cache — only fetches gaps, appends to existing parquet.

Usage:
    python scripts/backfill_candles.py [--days 1400] [--intervals 1h,5m]
"""
import asyncio
import argparse
import sys
import warnings
sys.path.insert(0, '/Users/hermes/quants-lab')
warnings.filterwarnings("ignore")

from pathlib import Path
from core.data_sources.clob import CLOBDataSource


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=1400,
                       help='Days of history to fetch (default: 1400 = ~3.8 years to mid-2022)')
    parser.add_argument('--intervals', default='1h,5m',
                       help='Comma-separated intervals (default: 1h,5m)')
    parser.add_argument('--connector', default='bybit_perpetual')
    parser.add_argument('--pair', default=None,
                       help='Single pair to backfill (default: all pairs with existing data)')
    args = parser.parse_args()

    intervals = args.intervals.split(',')
    connector = args.connector

    clob = CLOBDataSource()
    clob.load_candles_cache()

    # Discover pairs from existing parquet files
    candles_dir = Path('app/data/cache/candles')
    if args.pair:
        pairs = [args.pair]
    else:
        pairs = sorted(set(
            f.stem.split('|')[1]
            for f in candles_dir.glob(f'{connector}|*|1h.parquet')
            if len(f.stem.split('|')) == 3
        ))

    print(f"Backfilling {len(pairs)} pairs × {intervals} — {args.days} days history")
    print(f"This will take a while...")

    errors = []
    for i, pair in enumerate(pairs):
        for interval in intervals:
            try:
                print(f"  [{i+1}/{len(pairs)}] {pair} {interval}...", end=' ', flush=True)
                candles = await clob.get_candles_batch_last_days(
                    connector_name=connector,
                    trading_pairs=[pair],
                    interval=interval,
                    days=args.days,
                    batch_size=1,
                    sleep_time=1.0,
                )
                for c in candles:
                    print(f"{len(c.data)} candles")
            except Exception as e:
                print(f"FAILED: {e}")
                errors.append(f"{pair}/{interval}: {e}")

        # Dump cache periodically (every 5 pairs)
        if (i + 1) % 5 == 0:
            clob.dump_candles_cache()
            print(f"  [cache saved — {i+1}/{len(pairs)} pairs done]")

    # Final save
    clob.dump_candles_cache()
    print(f"\nDone. {len(pairs)} pairs backfilled, {len(errors)} errors.")
    if errors:
        print("Errors:")
        for e in errors:
            print(f"  {e}")


if __name__ == "__main__":
    asyncio.run(main())
