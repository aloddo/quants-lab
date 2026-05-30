"""
Wallet Profiler — Feasibility study: does wallet-level markout alpha exist?

Step 1: Get active wallets from hl_wallet_trades collector
Step 2: Query HL userFills API for each wallet's full history
Step 3: Match trades against 1s L2 mid prices from MongoDB
Step 4: Compute markouts at 30s, 2m, 10m
Step 5: Rank wallets by shrunk markout, output top/bottom deciles

Usage:
  python scripts/wallet_profiler.py [--min-trades 20] [--top-n 200]
"""
import argparse
import asyncio
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

import numpy as np
import pandas as pd
import requests
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("wallet_profiler")


def get_active_wallets(db, min_trades: int = 5, top_n: int = 200) -> list[dict]:
    """Get most active wallets from collector data."""
    pipeline = [
        {"$group": {
            "_id": "$buyer",
            "buy_count": {"$sum": 1},
            "buy_notional": {"$sum": "$notional"},
        }},
        {"$match": {"buy_count": {"$gte": min_trades}}},
        {"$sort": {"buy_notional": -1}},
        {"$limit": top_n},
    ]
    buyers = {d["_id"]: d for d in db.hl_wallet_trades.aggregate(pipeline)}

    pipeline[0]["$group"]["_id"] = "$seller"
    pipeline[0]["$group"]["buy_count"] = pipeline[0]["$group"].pop("buy_count")
    pipeline[0]["$group"]["buy_count"] = {"$sum": 1}
    pipeline[0]["$group"]["buy_notional"] = pipeline[0]["$group"].pop("buy_notional")
    pipeline[0]["$group"]["buy_notional"] = {"$sum": "$notional"}

    # Simpler: just get all wallets that appear as buyer or seller
    all_wallets = defaultdict(lambda: {"trades": 0, "notional": 0})
    for doc in db.hl_wallet_trades.find({}, {"buyer": 1, "seller": 1, "notional": 1, "side": 1}):
        buyer = doc.get("buyer", "")
        seller = doc.get("seller", "")
        notional = doc.get("notional", 0)
        if buyer:
            all_wallets[buyer]["trades"] += 1
            all_wallets[buyer]["notional"] += notional
        if seller:
            all_wallets[seller]["trades"] += 1
            all_wallets[seller]["notional"] += notional

    # Filter and sort
    wallets = [
        {"address": addr, "trades": data["trades"], "notional": data["notional"]}
        for addr, data in all_wallets.items()
        if data["trades"] >= min_trades and addr  # non-empty
    ]
    wallets.sort(key=lambda x: x["notional"], reverse=True)
    return wallets[:top_n]


def fetch_user_fills(address: str, limit: int = 2000) -> list[dict]:
    """Query HL userFills API for a wallet's trade history."""
    try:
        resp = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "userFills", "user": address},
            timeout=15,
        )
        if resp.ok:
            fills = resp.json()
            return fills[:limit] if isinstance(fills, list) else []
    except Exception as e:
        logger.warning(f"Failed to fetch fills for {address[:12]}...: {e}")
    return []


def build_mid_price_series(db, coin: str) -> pd.Series:
    """Build a 1s mid price series for a coin from L2 snapshots."""
    docs = list(db.hyperliquid_l2_snapshots_1s.find(
        {"coin": coin},
        {"timestamp_utc": 1, "best_bid": 1, "best_ask": 1},
    ).sort("timestamp_utc", 1))

    if not docs:
        return pd.Series(dtype=float)

    timestamps = [d["timestamp_utc"] / 1000.0 for d in docs]  # ms -> s
    mids = [(d["best_bid"] + d["best_ask"]) / 2.0 for d in docs]

    series = pd.Series(mids, index=pd.to_datetime(timestamps, unit="s", utc=True))
    series = series[~series.index.duplicated(keep="last")]
    return series.sort_index()


def compute_markouts(fills: list[dict], mid_series: dict[str, pd.Series],
                     horizons_s: list[int] = [30, 120, 600]) -> list[dict]:
    """Compute forward markouts for each fill against mid price series."""
    results = []

    for fill in fills:
        coin = fill.get("coin", "")
        if coin not in mid_series or mid_series[coin].empty:
            continue

        mids = mid_series[coin]
        fill_time_ms = fill.get("time", 0)
        fill_time_s = fill_time_ms / 1000.0
        fill_dt = pd.Timestamp(fill_time_s, unit="s", tz="UTC")
        fill_px = float(fill.get("px", 0))
        side = fill.get("side", "")
        crossed = fill.get("crossed", False)  # True = taker/aggressor
        sz = float(fill.get("sz", 0))

        if fill_px <= 0 or not side:
            continue

        # Find mid price at fill time (nearest 1s snapshot)
        idx = mids.index.searchsorted(fill_dt)
        if idx >= len(mids) or idx == 0:
            continue

        mid_at_fill = mids.iloc[min(idx, len(mids) - 1)]

        # Compute markouts at each horizon
        markouts = {}
        for h in horizons_s:
            target_dt = fill_dt + pd.Timedelta(seconds=h)
            target_idx = mids.index.searchsorted(target_dt)
            if target_idx >= len(mids):
                markouts[f"markout_{h}s"] = None
                continue

            mid_at_horizon = mids.iloc[min(target_idx, len(mids) - 1)]

            # Markout: how much did price move in the aggressor's favor?
            if side == "B":  # buyer aggressed
                markout_bps = (mid_at_horizon - mid_at_fill) / mid_at_fill * 10000
            else:  # seller aggressed
                markout_bps = (mid_at_fill - mid_at_horizon) / mid_at_fill * 10000

            markouts[f"markout_{h}s"] = markout_bps

        results.append({
            "coin": coin,
            "time": fill_time_ms,
            "side": side,
            "px": fill_px,
            "sz": sz,
            "notional": fill_px * sz,
            "crossed": crossed,
            "mid_at_fill": mid_at_fill,
            **markouts,
        })

    return results


def shrunk_mean(values: list[float], population_mean: float = 0.0,
                population_var: float = 25.0) -> float:
    """Bayesian shrinkage: pull small-sample estimates toward population mean."""
    if not values:
        return population_mean
    n = len(values)
    sample_mean = np.mean(values)
    sample_var = np.var(values) if n > 1 else population_var
    # Shrinkage weight: more data = less shrinkage
    shrinkage = population_var / (population_var + sample_var / n)
    return shrinkage * sample_mean + (1 - shrinkage) * population_mean


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--min-trades", type=int, default=5)
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--horizons", default="30,120,600", help="Markout horizons in seconds")
    args = parser.parse_args()

    horizons = [int(h) for h in args.horizons.split(",")]

    mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
    client = MongoClient(mongo_uri)
    db_name = mongo_uri.split("/")[-1]
    db = client[db_name]

    # Step 1: Get active wallets
    logger.info("Step 1: Getting active wallets from collector data...")
    wallets = get_active_wallets(db, min_trades=args.min_trades, top_n=args.top_n)
    logger.info(f"Found {len(wallets)} active wallets")

    if not wallets:
        logger.error("No active wallets found. Is the collector running?")
        return

    # Step 2: Build mid price series for coins with L2 data
    logger.info("Step 2: Building mid price series from L2 snapshots...")
    l2_coins = db.hyperliquid_l2_snapshots_1s.distinct("coin")
    mid_series = {}
    for coin in l2_coins:
        logger.info(f"  Loading {coin}...")
        mid_series[coin] = build_mid_price_series(db, coin)
        logger.info(f"  {coin}: {len(mid_series[coin])} 1s mids")

    # Step 3: Fetch fills and compute markouts for each wallet
    logger.info(f"Step 3: Fetching fills for {len(wallets)} wallets...")
    wallet_markouts = {}
    population_markouts = {f"markout_{h}s": [] for h in horizons}

    for i, w in enumerate(wallets):
        addr = w["address"]
        if i % 20 == 0:
            logger.info(f"  Processing wallet {i+1}/{len(wallets)}...")

        # Rate limit: HL API is generous but don't hammer
        if i > 0 and i % 10 == 0:
            time.sleep(0.5)

        fills = fetch_user_fills(addr)
        if not fills:
            continue

        # Filter to aggressive fills only (crossed=True)
        aggressive_fills = [f for f in fills if f.get("crossed", False)]
        if len(aggressive_fills) < 3:
            continue

        markout_results = compute_markouts(aggressive_fills, mid_series, horizons)
        if not markout_results:
            continue

        # Aggregate markouts per wallet
        wallet_data = {
            "address": addr,
            "total_fills": len(fills),
            "aggressive_fills": len(aggressive_fills),
            "fills_with_markout": len(markout_results),
            "coins": list(set(f["coin"] for f in markout_results)),
            "total_notional": sum(f["notional"] for f in markout_results),
        }

        for h in horizons:
            key = f"markout_{h}s"
            values = [r[key] for r in markout_results if r[key] is not None]
            if values:
                wallet_data[f"{key}_mean"] = np.mean(values)
                wallet_data[f"{key}_std"] = np.std(values)
                wallet_data[f"{key}_n"] = len(values)
                wallet_data[f"{key}_tstat"] = (
                    np.mean(values) / (np.std(values) / np.sqrt(len(values)))
                    if np.std(values) > 0 else 0
                )
                population_markouts[key].extend(values)

        wallet_markouts[addr] = wallet_data

    # Step 4: Apply Bayesian shrinkage and rank
    logger.info(f"Step 4: Ranking {len(wallet_markouts)} wallets with shrinkage...")

    # Population stats for shrinkage
    pop_stats = {}
    for h in horizons:
        key = f"markout_{h}s"
        all_vals = population_markouts[key]
        if all_vals:
            pop_stats[key] = {"mean": np.mean(all_vals), "var": np.var(all_vals)}
        else:
            pop_stats[key] = {"mean": 0, "var": 25.0}

    # Compute shrunk scores
    for addr, data in wallet_markouts.items():
        for h in horizons:
            key = f"markout_{h}s"
            n_key = f"{key}_n"
            mean_key = f"{key}_mean"
            if n_key in data and data[n_key] > 0:
                values_approx = [data[mean_key]] * data[n_key]  # approximate
                data[f"{key}_shrunk"] = shrunk_mean(
                    values_approx,
                    population_mean=pop_stats[key]["mean"],
                    population_var=pop_stats[key]["var"],
                )
            else:
                data[f"{key}_shrunk"] = pop_stats.get(key, {}).get("mean", 0)

    # Step 5: Report
    print("\n" + "=" * 80)
    print("WALLET PROFILING RESULTS")
    print("=" * 80)

    for h in horizons:
        key = f"markout_{h}s"
        ranked = sorted(
            wallet_markouts.values(),
            key=lambda x: x.get(f"{key}_shrunk", 0),
            reverse=True,
        )

        valid = [w for w in ranked if w.get(f"{key}_n", 0) >= 10]
        if not valid:
            print(f"\n{h}s horizon: not enough data")
            continue

        top_n = min(10, len(valid) // 10) or 1
        top = valid[:top_n]
        bottom = valid[-top_n:]

        print(f"\n--- {h}s MARKOUT ---")
        print(f"Population: mean={pop_stats[key]['mean']:+.2f}bps, var={pop_stats[key]['var']:.1f}")
        print(f"Wallets with >=10 fills in L2 window: {len(valid)}")

        print(f"\nTOP {top_n} (smart money candidates):")
        for w in top:
            print(
                f"  {w['address'][:14]}... "
                f"shrunk={w.get(f'{key}_shrunk', 0):+.2f}bps "
                f"raw={w.get(f'{key}_mean', 0):+.2f}bps "
                f"tstat={w.get(f'{key}_tstat', 0):+.2f} "
                f"n={w.get(f'{key}_n', 0)} "
                f"coins={w.get('coins', [])}"
            )

        print(f"\nBOTTOM {top_n} (dumb money candidates):")
        for w in bottom:
            print(
                f"  {w['address'][:14]}... "
                f"shrunk={w.get(f'{key}_shrunk', 0):+.2f}bps "
                f"raw={w.get(f'{key}_mean', 0):+.2f}bps "
                f"tstat={w.get(f'{key}_tstat', 0):+.2f} "
                f"n={w.get(f'{key}_n', 0)} "
                f"coins={w.get('coins', [])}"
            )

        # Is there a gap?
        if top and bottom:
            gap = top[0].get(f"{key}_shrunk", 0) - bottom[-1].get(f"{key}_shrunk", 0)
            print(f"\nTop-bottom gap: {gap:.1f}bps")
            if gap > 5:
                print(">>> SIGNAL EXISTS: meaningful separation between smart and dumb wallets")
            elif gap > 2:
                print(">>> WEAK SIGNAL: some separation, needs more data")
            else:
                print(">>> NO SIGNAL: wallets are indistinguishable")

    # Save to MongoDB for further analysis
    if wallet_markouts:
        db.hl_wallet_profiles.drop()
        db.hl_wallet_profiles.insert_many(list(wallet_markouts.values()))
        logger.info(f"Saved {len(wallet_markouts)} wallet profiles to hl_wallet_profiles")


if __name__ == "__main__":
    main()
