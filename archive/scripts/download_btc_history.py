"""
Download BTC-USDT historical candles for E1 stress tests.
Covers all required windows:
  Bear Crash:      2021-11-01 → 2022-06-30
  FTX Shock:       2022-11-01 → 2022-11-30
  Low-Vol Ranging: 2023-01-01 → 2023-09-30
  Bull Validation: 2024-07-01 → 2025-01-01

Saves parquet to ~/quants-lab/app/data/cache/candles/
File naming: binance_perpetual|BTC-USDT|1h.parquet
"""

import asyncio
import sys
sys.path.insert(0, '/Users/hermes/quants-lab')

import warnings
warnings.filterwarnings("ignore")

from core.data_sources.clob import CLOBDataSource


async def main():
    clob = CLOBDataSource()

    connector = "binance_perpetual"
    pair = "BTC-USDT"
    intervals = ["1h", "1m"]

    # Days needed to cover all windows back from today
    # 2021-11-01 is ~4.5 years ago = ~1650 days
    days = 1700

    print(f"Downloading {pair} candles ({intervals}) — {days} days history")
    print("This will take a few minutes...")

    for interval in intervals:
        print(f"\nFetching {interval}...")
        candles = await clob.get_candles_batch_last_days(
            connector_name=connector,
            trading_pairs=[pair],
            interval=interval,
            days=days,
            batch_size=1,
            sleep_time=1.5,
        )
        for c in candles:
            print(f"  {c.trading_pair} {interval}: {len(c.data)} candles "
                  f"({c.data.index.min()} → {c.data.index.max()})")

    clob.dump_candles_cache()
    print("\nCandles saved to app/data/cache/candles/")


if __name__ == "__main__":
    asyncio.run(main())
