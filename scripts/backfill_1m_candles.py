"""
Backfill 1m candle data for all pairs that have 1h parquet data, going back 365 days.

Uses CLOBDataSource smart cache — loads existing parquet first, then fetches only
missing ranges from the exchange. Deduplicates on timestamp automatically.

Usage:
    set -a && source .env && set +a
    python scripts/backfill_1m_candles.py [--days 365] [--pair ETH-USDT]
"""
import asyncio
import argparse
import sys
import time
import warnings

sys.path.insert(0, '/Users/hermes/quants-lab')
warnings.filterwarnings("ignore")

import pandas as pd
from pathlib import Path
from core.data_sources.clob import CLOBDataSource
from core.data_paths import data_paths


CONNECTOR = "bybit_perpetual"
INTERVAL = "1m"


def discover_pairs() -> list[str]:
    """Find all pairs that have 1h parquet files."""
    candles_dir = data_paths.candles_dir
    pairs = sorted(set(
        f.stem.split('|')[1]
        for f in candles_dir.glob(f'{CONNECTOR}|*|1h.parquet')
        if len(f.stem.split('|')) == 3
    ))
    return pairs


def check_existing_1m_coverage(pair: str) -> tuple[str, str]:
    """Check how far back existing 1m data goes. Returns (earliest, latest) as strings."""
    path = data_paths.candles_dir / f"{CONNECTOR}|{pair}|1m.parquet"
    if not path.exists():
        return "none", "none"
    try:
        df = pd.read_parquet(path)
        if df.empty:
            return "none", "none"
        ts = pd.to_datetime(df['timestamp'], unit='s')
        earliest = ts.min().strftime('%Y-%m-%d')
        latest = ts.max().strftime('%Y-%m-%d')
        return earliest, latest
    except Exception:
        return "error", "error"


async def backfill_pair(clob: CLOBDataSource, pair: str, days: int) -> int:
    """Backfill 1m candles for a single pair. Returns number of candles."""
    candles = await clob.get_candles_last_days(
        connector_name=CONNECTOR,
        trading_pair=pair,
        interval=INTERVAL,
        days=days,
    )
    return len(candles.data) if candles and candles.data is not None else 0


async def main():
    parser = argparse.ArgumentParser(description="Backfill 1m candles for 365 days")
    parser.add_argument('--days', type=int, default=365,
                        help='Days of history to fetch (default: 365)')
    parser.add_argument('--pair', default=None,
                        help='Single pair to backfill (default: all pairs with 1h data)')
    parser.add_argument('--skip-load', action='store_true',
                        help='Skip loading existing cache (fresh download)')
    args = parser.parse_args()

    # Discover pairs
    if args.pair:
        pairs = [args.pair]
    else:
        pairs = discover_pairs()

    print(f"=== 1m Candle Backfill ===")
    print(f"Pairs: {len(pairs)}, Target: {args.days} days back")
    print(f"Expected bars per pair: ~{args.days * 24 * 60:,}")
    print()

    # Show current coverage
    print("Current 1m coverage:")
    for pair in pairs:
        earliest, latest = check_existing_1m_coverage(pair)
        print(f"  {pair:20s}  {earliest} to {latest}")
    print()

    # Initialize CLOBDataSource and load existing cache (only 1m data)
    clob = CLOBDataSource()
    if not args.skip_load:
        print("Loading existing 1m parquet cache...")
        clob.load_candles_cache(connector_name=CONNECTOR, interval=INTERVAL)
        print(f"Loaded {len(clob._candles_cache)} cached files")
    print()

    errors = []
    total_candles = 0
    start_all = time.time()

    for i, pair in enumerate(pairs):
        start_pair = time.time()
        try:
            print(f"[{i+1}/{len(pairs)}] {pair}...", end=' ', flush=True)
            n_candles = await backfill_pair(clob, pair, args.days)
            elapsed = time.time() - start_pair
            total_candles += n_candles
            print(f"{n_candles:,} candles ({elapsed:.1f}s)")
        except Exception as e:
            elapsed = time.time() - start_pair
            print(f"FAILED ({elapsed:.1f}s): {type(e).__name__}: {e}")
            errors.append((pair, str(e)))

        # Rate limit between pairs
        await asyncio.sleep(0.5)

        # Save cache every 5 pairs to avoid data loss on crash
        if (i + 1) % 5 == 0:
            clob.dump_candles_cache()
            elapsed_total = time.time() - start_all
            print(f"  --- cache saved ({i+1}/{len(pairs)} pairs, {elapsed_total:.0f}s elapsed) ---")

    # Final save
    clob.dump_candles_cache()
    elapsed_total = time.time() - start_all

    print()
    print(f"=== Done ===")
    print(f"Pairs processed: {len(pairs) - len(errors)}/{len(pairs)}")
    print(f"Total candles: {total_candles:,}")
    print(f"Total time: {elapsed_total:.0f}s ({elapsed_total/60:.1f}min)")

    if errors:
        print(f"\nFailed pairs ({len(errors)}):")
        for pair, err in errors:
            print(f"  {pair}: {err}")


if __name__ == "__main__":
    asyncio.run(main())
