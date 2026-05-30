"""
Download Binance SPOT aggTrades from data.binance.vision.

Trade-level data with millisecond precision: price, qty, side, timestamp.
This is the highest-quality free historical data available for one leg of
the cross-exchange arb.

Usage:
    python scripts/download_binance_spot_trades.py --pairs BANDUSDT,NOMUSDT --days 90
    python scripts/download_binance_spot_trades.py --top 30 --days 90
"""
import argparse
import asyncio
import csv
import gzip
import io
import logging
import os
import sys
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiohttp
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://data.binance.vision/data/spot/daily/aggTrades"
CACHE_DIR = Path(__file__).resolve().parents[1] / "app/data/cache/binance_spot_trades"


def load_top_pairs(n: int) -> list[str]:
    """Load top N pairs from R0 universe discovery."""
    csv_path = Path(__file__).resolve().parents[1] / "app/data/cache/arb_universe.csv"
    if not csv_path.exists():
        logger.error("Run R0 universe discovery first (arb_universe.csv not found)")
        sys.exit(1)

    pairs = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["viable"] == "True":
                pairs.append(row["symbol"])
    return pairs[:n]


async def download_day(
    session: aiohttp.ClientSession,
    symbol: str,
    date: datetime,
    semaphore: asyncio.Semaphore,
) -> pd.DataFrame | None:
    """Download one day of aggTrades for a symbol."""
    date_str = date.strftime("%Y-%m-%d")
    url = f"{BASE_URL}/{symbol}/{symbol}-aggTrades-{date_str}.zip"

    async with semaphore:
        try:
            async with session.get(url) as resp:
                if resp.status == 404:
                    return None  # no data for this date (pair not listed yet)
                if resp.status != 200:
                    logger.warning(f"{symbol} {date_str}: HTTP {resp.status}")
                    return None

                data = await resp.read()

            # Parse ZIP -> CSV -> DataFrame
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                csv_name = zf.namelist()[0]
                with zf.open(csv_name) as f:
                    df = pd.read_csv(
                        f,
                        header=None,
                        names=[
                            "agg_trade_id", "price", "qty", "first_trade_id",
                            "last_trade_id", "timestamp", "is_buyer_maker",
                            "is_best_match",
                        ],
                        dtype={
                            "agg_trade_id": "int64",
                            "price": "float64",
                            "qty": "float64",
                            "timestamp": "int64",
                            "is_buyer_maker": "bool",
                        },
                    )
            return df

        except Exception as e:
            logger.warning(f"{symbol} {date_str}: {e}")
            return None


async def download_symbol(symbol: str, days: int, semaphore: asyncio.Semaphore):
    """Download all days for a symbol, aggregate to 1s OHLCV + spread-relevant fields."""
    out_dir = CACHE_DIR / symbol
    out_dir.mkdir(parents=True, exist_ok=True)

    # Check what we already have
    parquet_path = out_dir / f"{symbol}_trades_{days}d.parquet"
    if parquet_path.exists():
        existing = pd.read_parquet(parquet_path)
        logger.info(f"{symbol}: already have {len(existing)} rows, skipping")
        return len(existing)

    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start_date = end_date - timedelta(days=days)

    dates = [start_date + timedelta(days=i) for i in range(days)]

    async with aiohttp.ClientSession() as session:
        tasks = [download_day(session, symbol, d, semaphore) for d in dates]
        results = await asyncio.gather(*tasks)

    # Filter out None results
    dfs = [r for r in results if r is not None and len(r) > 0]
    if not dfs:
        logger.warning(f"{symbol}: no data found for {days} days")
        return 0

    df = pd.concat(dfs, ignore_index=True)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    # Resample to 1-second bars for manageable size
    # Keep: open, high, low, close, volume, trade_count, buy_volume, sell_volume
    df["second"] = df["timestamp_utc"].dt.floor("1s")
    df["usd_value"] = df["price"] * df["qty"]
    df["buy_value"] = df["usd_value"] * (~df["is_buyer_maker"]).astype(float)
    df["sell_value"] = df["usd_value"] * df["is_buyer_maker"].astype(float)

    bars = df.groupby("second").agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("qty", "sum"),
        volume_usd=("usd_value", "sum"),
        buy_volume_usd=("buy_value", "sum"),
        sell_volume_usd=("sell_value", "sum"),
        trade_count=("price", "count"),
        vwap=("usd_value", lambda x: x.sum() / df.loc[x.index, "qty"].sum()),
    ).reset_index()
    bars = bars.rename(columns={"second": "timestamp"})
    bars = bars.set_index("timestamp")

    # Save
    bars.to_parquet(parquet_path)
    logger.info(
        f"{symbol}: {len(df)} raw trades -> {len(bars)} 1s bars "
        f"({days} days, {parquet_path.stat().st_size / 1e6:.1f}MB)"
    )
    return len(bars)


async def main(pairs: list[str], days: int, concurrency: int = 5):
    """Download aggTrades for all pairs."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    semaphore = asyncio.Semaphore(concurrency)

    logger.info(f"Downloading {len(pairs)} pairs, {days} days each")
    logger.info(f"Concurrency: {concurrency}, output: {CACHE_DIR}")
    logger.info(f"Pairs: {pairs}")

    results = {}
    for symbol in pairs:
        count = await download_symbol(symbol, days, semaphore)
        results[symbol] = count

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("DOWNLOAD SUMMARY")
    logger.info("=" * 60)
    total_bars = 0
    for sym, count in sorted(results.items(), key=lambda x: -x[1]):
        status = "OK" if count > 0 else "MISSING"
        logger.info(f"  {sym:<15} {count:>10} 1s bars  [{status}]")
        total_bars += count
    logger.info(f"  {'TOTAL':<15} {total_bars:>10} 1s bars")
    logger.info(f"  Storage: {CACHE_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pairs", type=str, help="Comma-separated symbols (e.g., BANDUSDT,NOMUSDT)")
    parser.add_argument("--top", type=int, help="Download top N pairs from R0 universe")
    parser.add_argument("--days", type=int, default=90, help="Days of history (default: 90)")
    parser.add_argument("--concurrency", type=int, default=5, help="Max concurrent downloads")
    args = parser.parse_args()

    if args.pairs:
        pairs = [p.strip() for p in args.pairs.split(",")]
    elif args.top:
        pairs = load_top_pairs(args.top)
    else:
        pairs = load_top_pairs(30)

    asyncio.run(main(pairs, args.days, args.concurrency))
