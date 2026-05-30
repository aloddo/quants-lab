"""
Download trade-level data from BOTH Binance SPOT and Bybit PERP.

Binance: data.binance.vision/data/spot/daily/aggTrades/
Bybit:   public.bybit.com/trading/

Both provide daily CSVs with sub-second precision. This is the gold standard
for cross-exchange arb research -- actual executed prices, not candle closes.

After download, resamples to 1-second bars with VWAP, buy/sell volume,
and trade count. This gives us executable price estimates at 1s granularity
while keeping storage manageable.

Usage:
    python scripts/download_arb_trades.py --top 30 --days 90
    python scripts/download_arb_trades.py --pairs BANDUSDT,THETAUSDT --days 180
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
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).resolve().parents[1] / "app/data/cache/arb_trades"

BN_BASE = "https://data.binance.vision/data/spot/daily/aggTrades"
BB_BASE = "https://public.bybit.com/trading"


def load_top_pairs(n: int) -> list[str]:
    """Load top N pairs from R0 universe discovery."""
    csv_path = Path(__file__).resolve().parents[1] / "app/data/cache/arb_universe.csv"
    if not csv_path.exists():
        logger.error("Run R0 universe discovery first")
        sys.exit(1)
    pairs = []
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            if row["viable"] == "True":
                pairs.append(row["symbol"])
    return pairs[:n]


# ── Binance SPOT aggTrades ──────────────────────────────────

async def download_bn_day(
    session: aiohttp.ClientSession, symbol: str, date: datetime, sem: asyncio.Semaphore
) -> pd.DataFrame | None:
    """Download one day of Binance SPOT aggTrades."""
    date_str = date.strftime("%Y-%m-%d")
    url = f"{BN_BASE}/{symbol}/{symbol}-aggTrades-{date_str}.zip"

    async with sem:
        try:
            async with session.get(url) as resp:
                if resp.status == 404:
                    return None
                if resp.status != 200:
                    logger.debug(f"BN {symbol} {date_str}: HTTP {resp.status}")
                    return None
                data = await resp.read()

            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                with zf.open(zf.namelist()[0]) as f:
                    df = pd.read_csv(f, header=None, names=[
                        "agg_trade_id", "price", "qty", "first_trade_id",
                        "last_trade_id", "timestamp", "is_buyer_maker", "is_best_match",
                    ])
            df["exchange"] = "binance"
            df["side"] = np.where(df["is_buyer_maker"], "Sell", "Buy")
            # Binance aggTrades from data.binance.vision use MICROSECONDS (16 digits)
            # not milliseconds. Detect and handle both:
            ts_raw = df["timestamp"].astype(float)
            if ts_raw.iloc[0] > 1e15:  # microseconds (16 digits)
                df["timestamp_s"] = ts_raw / 1e6
            elif ts_raw.iloc[0] > 1e12:  # milliseconds (13 digits)
                df["timestamp_s"] = ts_raw / 1e3
            else:  # already seconds
                df["timestamp_s"] = ts_raw
            df["price"] = df["price"].astype(float)
            df["qty"] = df["qty"].astype(float)
            return df[["timestamp_s", "price", "qty", "side", "exchange"]]
        except Exception as e:
            logger.debug(f"BN {symbol} {date_str}: {e}")
            return None


# ── Bybit PERP trades ──────────────────────────────────────

async def download_bb_day(
    session: aiohttp.ClientSession, symbol: str, date: datetime, sem: asyncio.Semaphore
) -> pd.DataFrame | None:
    """Download one day of Bybit linear PERP trades."""
    date_str = date.strftime("%Y-%m-%d")
    url = f"{BB_BASE}/{symbol}/{symbol}{date_str}.csv.gz"

    async with sem:
        try:
            async with session.get(url) as resp:
                if resp.status == 404:
                    return None
                if resp.status != 200:
                    logger.debug(f"BB {symbol} {date_str}: HTTP {resp.status}")
                    return None
                data = await resp.read()

            raw = gzip.decompress(data).decode()
            df = pd.read_csv(io.StringIO(raw))
            df["exchange"] = "bybit"
            df["timestamp_s"] = df["timestamp"]
            return df[["timestamp_s", "price", "size", "side", "exchange"]].rename(
                columns={"size": "qty"}
            )
        except Exception as e:
            logger.debug(f"BB {symbol} {date_str}: {e}")
            return None


# ── Resample to 1-second bars ───────────────────────────────

def resample_trades(df: pd.DataFrame, exchange: str, interval: str = "1s") -> pd.DataFrame:
    """Resample raw trades to fixed-interval OHLCV bars.

    Args:
        interval: pandas frequency string, e.g. "1s", "5s", "1min"
    """
    df = df.copy()
    df["price"] = df["price"].astype(float)
    df["qty"] = df["qty"].astype(float)
    df["timestamp_s"] = df["timestamp_s"].astype(float)
    # Convert to datetime -- timestamps are in seconds (possibly with decimals)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_s"], unit="s", utc=True)
    df["second"] = df["timestamp_utc"].dt.floor(interval)
    df["usd_value"] = df["price"].astype(float) * df["qty"].astype(float)
    df["is_buy"] = df["side"].str.lower() == "buy"

    bars = df.groupby("second").agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("qty", "sum"),
        volume_usd=("usd_value", "sum"),
        trade_count=("price", "count"),
        buy_count=("is_buy", "sum"),
    ).reset_index()

    # VWAP
    bars["vwap"] = bars["volume_usd"] / bars["volume"]
    bars = bars.rename(columns={"second": "timestamp"}).set_index("timestamp")

    # Prefix columns with exchange
    prefix = "bn_" if exchange == "binance" else "bb_"
    bars.columns = [f"{prefix}{c}" for c in bars.columns]

    return bars


# ── Main download + merge ───────────────────────────────────

async def download_pair(
    symbol: str, days: int, sem: asyncio.Semaphore,
    interval: str = "1s", save_raw: bool = True,
):
    """Download both exchanges for one pair, merge into synchronized bars.

    Args:
        interval: bar resolution ("1s", "5s", "1min"). Majors use "5s" to save space.
        save_raw: whether to store raw trade parquet (set False for majors).
    """
    out_dir = CACHE_DIR / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    itag = interval.replace("min", "m")
    merged_path = out_dir / f"{symbol}_{itag}_merged_{days}d.parquet"

    if merged_path.exists():
        existing = pd.read_parquet(merged_path)
        logger.info(f"{symbol}: already have {len(existing)} merged {interval} bars, skipping")
        return len(existing)

    end_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    dates = [end_date - timedelta(days=i) for i in range(1, days + 1)]

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=60)
    ) as session:
        # Download both exchanges in parallel
        bn_tasks = [download_bn_day(session, symbol, d, sem) for d in dates]
        bb_tasks = [download_bb_day(session, symbol, d, sem) for d in dates]

        logger.info(f"{symbol}: downloading {days} days from both exchanges...")
        all_results = await asyncio.gather(*(bn_tasks + bb_tasks))

        bn_results = all_results[:len(dates)]
        bb_results = all_results[len(dates):]

    # Combine
    bn_dfs = [r for r in bn_results if r is not None and len(r) > 0]
    bb_dfs = [r for r in bb_results if r is not None and len(r) > 0]

    if not bn_dfs:
        logger.warning(f"{symbol}: no Binance SPOT data")
        return 0
    if not bb_dfs:
        logger.warning(f"{symbol}: no Bybit PERP data")
        return 0

    bn_raw = pd.concat(bn_dfs, ignore_index=True)
    bb_raw = pd.concat(bb_dfs, ignore_index=True)

    bn_days = len(bn_dfs)
    bb_days = len(bb_dfs)
    logger.info(
        f"{symbol}: BN {len(bn_raw)} trades ({bn_days}d), "
        f"BB {len(bb_raw)} trades ({bb_days}d)"
    )

    # ── Sanitize raw data before processing ────────────────
    # 1. Drop zero/negative prices (exchange glitches)
    bn_raw = bn_raw[bn_raw["price"].astype(float) > 0].copy()
    bb_raw = bb_raw[bb_raw["price"].astype(float) > 0].copy()

    # 2. Drop zero quantities
    bn_raw = bn_raw[bn_raw["qty"].astype(float) > 0].copy()
    bb_raw = bb_raw[bb_raw["qty"].astype(float) > 0].copy()

    # 3. Drop duplicates (same timestamp + price + qty)
    bn_before = len(bn_raw)
    bn_raw = bn_raw.drop_duplicates(subset=["timestamp_s", "price", "qty"])
    bb_before = len(bb_raw)
    bb_raw = bb_raw.drop_duplicates(subset=["timestamp_s", "price", "qty"])
    if bn_before != len(bn_raw) or bb_before != len(bb_raw):
        logger.info(
            f"{symbol}: deduped BN {bn_before}->{len(bn_raw)}, "
            f"BB {bb_before}->{len(bb_raw)}"
        )

    # 4. Sort by timestamp
    bn_raw = bn_raw.sort_values("timestamp_s").reset_index(drop=True)
    bb_raw = bb_raw.sort_values("timestamp_s").reset_index(drop=True)

    # 5. Check for price outliers (>10 std from rolling median)
    for label, df_check in [("BN", bn_raw), ("BB", bb_raw)]:
        prices = df_check["price"].astype(float)
        median = prices.rolling(1000, min_periods=100, center=True).median()
        std = prices.rolling(1000, min_periods=100, center=True).std()
        outliers = ((prices - median).abs() > 10 * std) & (std > 0)
        if outliers.sum() > 0:
            logger.warning(f"{symbol} {label}: {outliers.sum()} price outliers (>10 std)")

    # ── Save raw trade data (for future strategies) ──────
    bn_raw_path = out_dir / f"{symbol}_bn_spot_trades_{days}d.parquet"
    bb_raw_path = out_dir / f"{symbol}_bb_perp_trades_{days}d.parquet"
    if save_raw:
        bn_raw.to_parquet(bn_raw_path, index=False)
        bb_raw.to_parquet(bb_raw_path, index=False)
        logger.info(
            f"{symbol}: raw trades saved — "
            f"BN {bn_raw_path.stat().st_size / 1e6:.1f}MB, "
            f"BB {bb_raw_path.stat().st_size / 1e6:.1f}MB"
        )
    else:
        logger.info(f"{symbol}: raw trades skipped (bars only mode)")

    # ── Resample to 1s bars ──────────────────────────────
    bn_bars = resample_trades(bn_raw, "binance", interval=interval)
    bb_bars = resample_trades(bb_raw, "bybit", interval=interval)

    # Merge on timestamp (inner join = only seconds where both have data)
    merged = bn_bars.join(bb_bars, how="inner")

    if len(merged) == 0:
        logger.warning(f"{symbol}: no overlapping 1s bars")
        return 0

    # Compute spread from VWAP (better than close for arb research)
    # BUY_BN_SELL_BB: buy Binance spot (at ask ~ vwap), sell Bybit perp (at bid ~ vwap)
    merged["spread_vwap_bps"] = (
        (merged["bb_vwap"] - merged["bn_vwap"]) / merged["bn_vwap"] * 10000
    )
    merged["abs_spread_vwap"] = merged["spread_vwap_bps"].abs()

    # Also compute from close (for comparison with old method)
    merged["spread_close_bps"] = (
        (merged["bb_close"] - merged["bn_close"]) / merged["bn_close"] * 10000
    )

    # Save
    merged.to_parquet(merged_path)
    days_covered = (merged.index[-1] - merged.index[0]).days
    logger.info(
        f"{symbol}: {len(merged)} merged 1s bars, {days_covered} days, "
        f"{merged_path.stat().st_size / 1e6:.1f}MB"
    )
    return len(merged)


async def main(
    pairs: list[str], days: int, concurrency: int = 5,
    interval: str = "1s", save_raw: bool = True,
):
    """Download and merge trade data for all pairs."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    sem = asyncio.Semaphore(concurrency)

    logger.info(f"Downloading {len(pairs)} pairs, {days} days each, {interval} bars")
    logger.info(f"Sources: Binance SPOT (data.binance.vision) + Bybit PERP (public.bybit.com)")
    logger.info(f"Raw trades: {'YES' if save_raw else 'NO (bars only)'}")
    logger.info(f"Output: {CACHE_DIR}")

    results = {}
    for symbol in pairs:
        try:
            count = await download_pair(
                symbol, days, sem, interval=interval, save_raw=save_raw
            )
            results[symbol] = count
        except Exception as e:
            logger.error(f"{symbol}: FAILED - {e}")
            results[symbol] = 0

    # Summary
    print("\n" + "=" * 70)
    print("DOWNLOAD SUMMARY")
    print("=" * 70)
    total = 0
    ok = 0
    for sym, count in sorted(results.items(), key=lambda x: -x[1]):
        status = "OK" if count > 0 else "MISSING"
        if count > 0:
            ok += 1
        print(f"  {sym:<15} {count:>12,} 1s bars  [{status}]")
        total += count
    print(f"  {'TOTAL':<15} {total:>12,} 1s bars")
    print(f"  Pairs with data: {ok}/{len(pairs)}")
    print(f"  Storage: {CACHE_DIR}")
    print("=" * 70)


TIER_MAJORS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]
TIER_MIDCAP = [
    "AVAXUSDT", "LINKUSDT", "SUIUSDT", "ADAUSDT", "DOTUSDT",
    "UNIUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT",
    "MATICUSDT", "LTCUSDT", "BCHUSDT", "AAVEUSDT", "MKRUSDT",
]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pairs", type=str, help="Comma-separated symbols")
    parser.add_argument("--top", type=int, help="Top N pairs from R0 universe")
    parser.add_argument("--tier", type=str, choices=["majors", "midcap", "shitcoins", "all"],
                        help="Download a predefined tier")
    parser.add_argument("--days", type=int, default=90, help="Days of history")
    parser.add_argument("--interval", type=str, default="1s", help="Bar interval (1s, 5s, 1min)")
    parser.add_argument("--no-raw", action="store_true", help="Skip raw trade storage")
    parser.add_argument("--concurrency", type=int, default=5, help="Max concurrent downloads")
    args = parser.parse_args()

    if args.tier == "majors":
        pairs = TIER_MAJORS
        days = min(args.days, 14)  # cap majors at 14 days
        interval = "5s"  # coarser for huge volume
        save_raw = False
        logger.info(f"Tier: MAJORS (14d max, 5s bars, no raw)")
    elif args.tier == "midcap":
        pairs = TIER_MIDCAP
        days = args.days
        interval = args.interval
        save_raw = not args.no_raw
        logger.info(f"Tier: MID-CAP ({days}d, {interval} bars)")
    elif args.tier == "all":
        # Run all tiers sequentially
        logger.info("Running ALL tiers sequentially...")
        asyncio.run(main(TIER_MAJORS, 14, args.concurrency, interval="5s", save_raw=False))
        asyncio.run(main(TIER_MIDCAP, args.days, args.concurrency, interval="1s", save_raw=False))
        top = load_top_pairs(30)
        asyncio.run(main(top, args.days, args.concurrency, interval="1s", save_raw=True))
        sys.exit(0)
    elif args.pairs:
        pairs = [p.strip() for p in args.pairs.split(",")]
        days = args.days
        interval = args.interval
        save_raw = not args.no_raw
    elif args.top:
        pairs = load_top_pairs(args.top)
        days = args.days
        interval = args.interval
        save_raw = not args.no_raw
    else:
        pairs = load_top_pairs(30)
        days = args.days
        interval = args.interval
        save_raw = not args.no_raw

    asyncio.run(main(pairs, days, args.concurrency, interval=interval, save_raw=save_raw))
