#!/usr/bin/env python3
"""Validate S3-reconstructed 1m candles against HL's public API.

HL's candle_snapshot API only retains 1m candles for ~5 days, but that
gives us a recent overlap window where we can compare BOTH sources bar-
by-bar. This script:

1. Queries HL info.candle_snapshot for a coin + recent window
2. Queries our MongoDB hyperliquid_candles (source=s3_reconstructed)
3. Joins on (coin, timestamp_utc) and computes mismatch statistics

Output: per-coin mismatch metrics + a summary verdict.

Usage:
    python scripts/v13_validate_reconstructed_candles.py \\
        --coins BTC,ETH,SOL,HYPE --days 3
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone

import aiohttp
import numpy as np
import pandas as pd
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_validate] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

HL_API_URL = "https://api.hyperliquid.xyz/info"


async def fetch_hl_candles(session: aiohttp.ClientSession, coin: str,
                           start_ms: int, end_ms: int) -> list:
    """Fetch HL 1m candles via candle_snapshot. Chunks if window > 5000 bars."""
    rows = []
    cur_end = end_ms
    while cur_end > start_ms:
        payload = {
            "type": "candleSnapshot",
            "req": {"coin": coin, "interval": "1m",
                    "startTime": start_ms, "endTime": cur_end},
        }
        async with session.post(HL_API_URL, json=payload) as r:
            if r.status != 200:
                logger.warning(f"HL {r.status} for {coin}: {await r.text()}")
                break
            data = await r.json()
        if not isinstance(data, list) or not data:
            break
        rows.extend(data)
        new_end = min(int(d["t"]) for d in data) - 1
        if new_end >= cur_end:
            break
        cur_end = new_end
        await asyncio.sleep(0.5)
    seen = set()
    dedup = []
    for r in rows:
        ts = int(r["t"])
        if ts in seen:
            continue
        seen.add(ts)
        dedup.append(r)
    dedup.sort(key=lambda x: int(x["t"]))
    return dedup


async def main_async(coins: list, days: int):
    db = MongoClient("mongodb://localhost:27017")["quants_lab"]
    col = db["hyperliquid_candles"]
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - days * 86_400_000

    per_coin_results = []
    async with aiohttp.ClientSession() as session:
        for coin in coins:
            logger.info(f"Validating {coin}...")
            api_rows = await fetch_hl_candles(session, coin, start_ms, now_ms)
            if not api_rows:
                logger.warning(f"  {coin}: no API data")
                continue
            api_df = pd.DataFrame([{
                "timestamp_utc": int(r["t"]),
                "open_api": float(r["o"]),
                "high_api": float(r["h"]),
                "low_api": float(r["l"]),
                "close_api": float(r["c"]),
                "volume_api": float(r["v"]),
                "n_trades_api": int(r.get("n", 0)),
            } for r in api_rows])
            # Reconstructed candles for same window
            recon_cursor = col.find({
                "coin": coin, "interval": "1m", "source": "s3_reconstructed",
                "timestamp_utc": {"$gte": start_ms, "$lte": now_ms},
            }, {"timestamp_utc": 1, "open": 1, "high": 1, "low": 1, "close": 1,
                 "volume": 1, "n_trades": 1, "_id": 0})
            recon_df = pd.DataFrame(list(recon_cursor))
            if recon_df.empty:
                logger.warning(f"  {coin}: no reconstructed data")
                continue
            recon_df = recon_df.rename(columns={
                "open": "open_recon", "high": "high_recon",
                "low": "low_recon", "close": "close_recon",
                "volume": "volume_recon", "n_trades": "n_trades_recon",
            })

            joined = api_df.merge(recon_df, on="timestamp_utc", how="outer", indicator=True)
            n_total = len(joined)
            n_both = (joined["_merge"] == "both").sum()
            n_api_only = (joined["_merge"] == "left_only").sum()
            n_recon_only = (joined["_merge"] == "right_only").sum()

            both = joined[joined["_merge"] == "both"]
            close_diff_bps = ((both["close_recon"] - both["close_api"]).abs()
                              / both["close_api"]).median() * 1e4 if len(both) else 0
            open_diff_bps = ((both["open_recon"] - both["open_api"]).abs()
                             / both["open_api"]).median() * 1e4 if len(both) else 0
            high_diff_bps = ((both["high_recon"] - both["high_api"]).abs()
                             / both["high_api"]).median() * 1e4 if len(both) else 0
            low_diff_bps = ((both["low_recon"] - both["low_api"]).abs()
                            / both["low_api"]).median() * 1e4 if len(both) else 0
            volume_ratio = (both["volume_recon"] / both["volume_api"].replace(0, np.nan)).median() if len(both) else None
            n_trades_ratio = (both["n_trades_recon"] / both["n_trades_api"].replace(0, np.nan)).median() if len(both) else None

            per_coin_results.append({
                "coin": coin,
                "n_total_bars": n_total,
                "n_both": n_both,
                "n_api_only": n_api_only,
                "n_recon_only": n_recon_only,
                "median_open_diff_bps": float(open_diff_bps),
                "median_high_diff_bps": float(high_diff_bps),
                "median_low_diff_bps": float(low_diff_bps),
                "median_close_diff_bps": float(close_diff_bps),
                "median_volume_ratio": float(volume_ratio) if volume_ratio else None,
                "median_n_trades_ratio": float(n_trades_ratio) if n_trades_ratio else None,
            })
            vol_ratio_str = f"{volume_ratio:.3f}" if volume_ratio else "NA"
            logger.info(f"  {coin}: both={n_both}, api_only={n_api_only}, recon_only={n_recon_only}, "
                        f"close_diff_bps={close_diff_bps:.2f}, vol_ratio={vol_ratio_str}")
            await asyncio.sleep(1.0)

    if not per_coin_results:
        logger.error("Zero coins validated.")
        return

    results = pd.DataFrame(per_coin_results)
    logger.info("\n=== Validation Summary (per coin) ===")
    if len(results) <= 10:
        logger.info(results.to_string(index=False))
    else:
        logger.info(f"  (suppressed; {len(results)} coins -- see aggregated stats below)")

    # Aggregated statistics across all validated coins (the spec section 5.7
    # broader-validation requirement: p50 / p95 / max on each metric).
    def _stats(s: pd.Series) -> dict:
        s = pd.to_numeric(s, errors="coerce").dropna()
        if s.empty:
            return {"p50": None, "p95": None, "max": None}
        return {"p50": float(s.median()), "p95": float(s.quantile(0.95)), "max": float(s.max())}

    close_stats = _stats(results["median_close_diff_bps"])
    open_stats = _stats(results["median_open_diff_bps"])
    high_stats = _stats(results["median_high_diff_bps"])
    low_stats = _stats(results["median_low_diff_bps"])
    vol_dev = (pd.to_numeric(results["median_volume_ratio"], errors="coerce") - 1.0).abs()
    vol_stats = _stats(vol_dev)
    # Missing-bar rate: fraction of API bars not present in reconstruction
    # (api_only / total_bars).
    if "n_total_bars" in results.columns and results["n_total_bars"].sum() > 0:
        missing_rate = float(results["n_api_only"].sum() / results["n_total_bars"].sum())
    else:
        missing_rate = 0.0

    logger.info("\n=== Aggregated bps drift (across all validated coins) ===")
    logger.info(f"  close_diff_bps  p50={close_stats['p50']:.3f} p95={close_stats['p95']:.3f} max={close_stats['max']:.3f}")
    logger.info(f"  open_diff_bps   p50={open_stats['p50']:.3f} p95={open_stats['p95']:.3f} max={open_stats['max']:.3f}")
    logger.info(f"  high_diff_bps   p50={high_stats['p50']:.3f} p95={high_stats['p95']:.3f} max={high_stats['max']:.3f}")
    logger.info(f"  low_diff_bps    p50={low_stats['p50']:.3f} p95={low_stats['p95']:.3f} max={low_stats['max']:.3f}")
    logger.info(f"  |vol_ratio-1|   p50={vol_stats['p50']:.4f} p95={vol_stats['p95']:.4f} max={vol_stats['max']:.4f}")
    logger.info(f"  missing_bar_rate (api_only / total): {missing_rate:.4f}")
    logger.info(f"  coins validated: {len(results)}")

    # Verdict: tighten the thresholds since GPT review noted the 0-bps claim
    # was only for 4 majors. For top-50 + long-tail we expect a wider
    # distribution; pass criteria are p95 close_diff < 5 bps AND p95
    # |vol_ratio-1| < 5%.
    if close_stats["p95"] is not None and vol_stats["p95"] is not None:
        if close_stats["p95"] < 5.0 and vol_stats["p95"] < 0.05:
            logger.info("VERDICT: PASS -- p95 close_diff<5 bps + p95 |vol_dev|<5%")
        else:
            logger.warning(
                f"VERDICT: REVIEW -- p95 close_diff={close_stats['p95']:.2f} bps, "
                f"p95 vol_dev={vol_stats['p95']:.4f}"
            )
    else:
        logger.warning("VERDICT: INSUFFICIENT DATA")


def _discover_coins(top_n: int, random_n: int, seed: int = 42) -> list:
    """Discover coins to validate. `top_n` by total recent volume + `random_n`
    sampled from the long tail. Used by --auto mode.

    Uses MongoDB hyperliquid_candles to rank by sum(volume) over the last 3
    days; the API can fetch the recent window for any of these coins.
    """
    import random
    from pymongo import MongoClient
    db = MongoClient("mongodb://localhost:27017")["quants_lab"]
    col = db["hyperliquid_candles"]
    # Last 3 days window for ranking.
    now_ms = int(time.time() * 1000)
    start_ms = now_ms - 3 * 86_400_000
    pipeline = [
        {"$match": {"interval": "1m", "source": "s3_reconstructed",
                    "timestamp_utc": {"$gte": start_ms, "$lte": now_ms}}},
        {"$group": {"_id": "$coin", "vol_sum": {"$sum": "$volume"}}},
        {"$sort": {"vol_sum": -1}},
    ]
    ranked = list(col.aggregate(pipeline))
    if not ranked:
        return []
    top_coins = [r["_id"] for r in ranked[:top_n]]
    # Long tail = everything past top_n (where vol_sum > 0).
    tail = [r["_id"] for r in ranked[top_n:] if r["vol_sum"] > 0]
    rng = random.Random(seed)
    rng.shuffle(tail)
    long_tail = tail[:random_n]
    return top_coins + long_tail


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--coins", default=None,
                    help="Comma-separated coins (default uses --auto top 50 + random 50)")
    ap.add_argument("--auto", action="store_true",
                    help="Validate top-N + random-N coins instead of the default 4-coin sample")
    ap.add_argument("--top-n", type=int, default=50)
    ap.add_argument("--random-n", type=int, default=50)
    ap.add_argument("--days", type=int, default=3)
    args = ap.parse_args()
    if args.auto:
        coins = _discover_coins(args.top_n, args.random_n)
        logger.info(f"Auto mode: validating {len(coins)} coins (top {args.top_n} + random {args.random_n})")
    elif args.coins is None:
        coins = ["BTC", "ETH", "SOL", "HYPE"]
    else:
        coins = [c.strip() for c in args.coins.split(",")]
    asyncio.run(main_async(coins, args.days))


if __name__ == "__main__":
    main()
