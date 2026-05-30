#!/usr/bin/env python3
"""
Backfill Bitvavo spot candles for EUR-quoted pairs.

- Discovers tradable EUR markets on Bitvavo.
- Optionally filters to markets that have matching Bybit USDT perpetual candles.
- Downloads historical candles via Bitvavo REST with pagination.
- Stores parquet files under app/data/cache/candles as:
    bitvavo_spot|BASE-EUR|<interval>.parquet

Usage:
  python scripts/backfill_bitvavo_candles.py --interval 1h --days 1460 --max-pairs 60
  python scripts/backfill_bitvavo_candles.py --pairs BTC-EUR,ETH-EUR,SOL-EUR --interval 1m --days 30
"""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List

import aiohttp
import numpy as np
import pandas as pd


BITVAVO_API = "https://api.bitvavo.com/v2"
CANDLES_DIR = Path("app/data/cache/candles")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill Bitvavo EUR spot candles")
    p.add_argument("--interval", default="1h", choices=["1m", "5m", "15m", "1h", "4h", "1d"])
    p.add_argument("--days", type=int, default=1460, help="History depth in days")
    p.add_argument("--pairs", default="", help="Comma-separated Bitvavo markets (e.g. BTC-EUR,ETH-EUR)")
    p.add_argument("--max-pairs", type=int, default=60)
    p.add_argument("--bybit-match-only", action="store_true", default=True)
    p.add_argument("--no-bybit-match-only", dest="bybit_match_only", action="store_false")
    p.add_argument("--limit", type=int, default=1000, help="Candles per API page")
    p.add_argument("--concurrency", type=int, default=4)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--sleep-ms", type=int, default=120)
    return p.parse_args()


def interval_to_ms(interval: str) -> int:
    return {
        "1m": 60_000,
        "5m": 5 * 60_000,
        "15m": 15 * 60_000,
        "1h": 60 * 60_000,
        "4h": 4 * 60 * 60_000,
        "1d": 24 * 60 * 60_000,
    }[interval]


async def fetch_json(session: aiohttp.ClientSession, url: str):
    async with session.get(url) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"HTTP {resp.status}: {url} :: {text[:200]}")
        return await resp.json()


def bybit_bases_available() -> set[str]:
    bases = set()
    for p in CANDLES_DIR.glob("bybit_perpetual|*-USDT|1h.parquet"):
        pair = p.name.split("|")[1]
        if pair.endswith("-USDT"):
            bases.add(pair[:-5])
    return bases


async def discover_markets(session: aiohttp.ClientSession, bybit_match_only: bool) -> List[Dict]:
    markets = await fetch_json(session, f"{BITVAVO_API}/markets")
    eur = [m for m in markets if m.get("quote") == "EUR" and m.get("status") == "trading"]

    # Rank by 24h quote volume (EUR notionals)
    tickers = await fetch_json(session, f"{BITVAVO_API}/ticker/24h")
    vol_map = {}
    for t in tickers:
        try:
            vol_map[t["market"]] = float(t.get("volumeQuote", 0) or 0)
        except Exception:
            vol_map[t["market"]] = 0.0

    bybit_bases = bybit_bases_available() if bybit_match_only else None

    out = []
    for m in eur:
        market = m["market"]
        base = m["base"]
        if bybit_bases is not None and base not in bybit_bases:
            continue
        out.append(
            {
                "market": market,
                "base": base,
                "quote": "EUR",
                "volume_quote_24h": vol_map.get(market, 0.0),
            }
        )

    out.sort(key=lambda x: x["volume_quote_24h"], reverse=True)
    return out


async def fetch_candles_market(
    session: aiohttp.ClientSession,
    market: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int,
    sleep_ms: int,
) -> pd.DataFrame:
    """
    Bitvavo candles response: [timestamp_ms, open, high, low, close, volume]
    Ordered newest -> oldest.
    """
    rows = []
    cursor_end = end_ms

    while True:
        url = f"{BITVAVO_API}/{market}/candles?interval={interval}&limit={limit}&end={cursor_end}"
        data = await fetch_json(session, url)
        if not data:
            break

        page = []
        for r in data:
            try:
                ts = int(r[0])
                o, h, l, c, v = map(float, r[1:6])
                page.append((ts, o, h, l, c, v))
            except Exception:
                continue

        if not page:
            break

        rows.extend(page)
        earliest = min(x[0] for x in page)
        if earliest <= start_ms:
            break

        # end is effectively exclusive; step cursor backward
        cursor_end = earliest
        await asyncio.sleep(max(0.0, sleep_ms / 1000.0))

    if not rows:
        return pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])

    df = pd.DataFrame(rows, columns=["timestamp_ms", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates(subset=["timestamp_ms"], keep="first")
    df = df[(df["timestamp_ms"] >= start_ms) & (df["timestamp_ms"] <= end_ms)].copy()
    df = df.sort_values("timestamp_ms")

    # Normalize to existing candle schema
    df["timestamp"] = (df["timestamp_ms"] // 1000).astype(np.int64)
    df["quote_asset_volume"] = df["close"] * df["volume"]
    df["n_trades"] = 0.0
    df["taker_buy_base_volume"] = 0.0
    df["taker_buy_quote_volume"] = 0.0

    return df[
        [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_asset_volume",
            "n_trades",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
        ]
    ]


async def backfill_one(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    market: str,
    base: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int,
    sleep_ms: int,
    overwrite: bool,
) -> dict:
    file_path = CANDLES_DIR / f"bitvavo_spot|{base}-EUR|{interval}.parquet"

    if file_path.exists() and not overwrite:
        try:
            old = pd.read_parquet(file_path, columns=["timestamp"])
            if len(old) > 0:
                return {
                    "market": market,
                    "base": base,
                    "status": "skipped_exists",
                    "rows": int(len(old)),
                    "start": int(old["timestamp"].min()),
                    "end": int(old["timestamp"].max()),
                    "path": str(file_path),
                }
        except Exception:
            pass

    async with sem:
        try:
            df = await fetch_candles_market(
                session=session,
                market=market,
                interval=interval,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=limit,
                sleep_ms=sleep_ms,
            )
            if df.empty:
                return {"market": market, "base": base, "status": "empty", "rows": 0, "path": str(file_path)}

            CANDLES_DIR.mkdir(parents=True, exist_ok=True)
            df.to_parquet(file_path, index=False)
            return {
                "market": market,
                "base": base,
                "status": "ok",
                "rows": int(len(df)),
                "start": int(df["timestamp"].min()),
                "end": int(df["timestamp"].max()),
                "path": str(file_path),
            }
        except Exception as e:
            return {"market": market, "base": base, "status": f"error:{type(e).__name__}", "rows": 0, "error": str(e)}


async def main_async(args: argparse.Namespace) -> None:
    now = datetime.now(timezone.utc)
    start_dt = now - timedelta(days=args.days)
    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(now.timestamp() * 1000)

    timeout = aiohttp.ClientTimeout(total=60)
    connector = aiohttp.TCPConnector(limit=max(8, args.concurrency * 2), ssl=False)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        if args.pairs.strip():
            requested = [x.strip().upper() for x in args.pairs.split(",") if x.strip()]
            markets = []
            for m in requested:
                if "-" not in m:
                    continue
                base, quote = m.split("-", 1)
                if quote != "EUR":
                    continue
                markets.append({"market": m, "base": base, "quote": "EUR", "volume_quote_24h": 0.0})
        else:
            markets = await discover_markets(session, bybit_match_only=args.bybit_match_only)

        if args.max_pairs > 0:
            markets = markets[: args.max_pairs]

        print(f"Discovered markets for backfill: {len(markets)}")
        if not markets:
            print("No markets to backfill.")
            return

        sem = asyncio.Semaphore(args.concurrency)
        tasks = [
            backfill_one(
                session=session,
                sem=sem,
                market=m["market"],
                base=m["base"],
                interval=args.interval,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=args.limit,
                sleep_ms=args.sleep_ms,
                overwrite=args.overwrite,
            )
            for m in markets
        ]

        results = await asyncio.gather(*tasks)

    ok = [r for r in results if r.get("status") == "ok"]
    skipped = [r for r in results if r.get("status") == "skipped_exists"]
    empty = [r for r in results if r.get("status") == "empty"]
    errs = [r for r in results if str(r.get("status", "")).startswith("error")]

    print(f"Backfill done: ok={len(ok)} skipped={len(skipped)} empty={len(empty)} errors={len(errs)}")
    if ok:
        rows = pd.DataFrame(ok)
        rows["start_dt"] = pd.to_datetime(rows["start"], unit="s", utc=True)
        rows["end_dt"] = pd.to_datetime(rows["end"], unit="s", utc=True)
        print(rows[["market", "rows", "start_dt", "end_dt"]].head(20).to_string(index=False))

    if errs:
        print("\nErrors:")
        for e in errs[:20]:
            print(e)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    meta_path = CANDLES_DIR / f"bitvavo_backfill_{stamp}_{args.interval}.csv"
    pd.DataFrame(results).to_csv(meta_path, index=False)
    print(f"\nSaved report: {meta_path}")


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
