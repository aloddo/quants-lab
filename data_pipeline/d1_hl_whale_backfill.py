"""
D1: Backfill Historical Whale Positions from HL Fills

For each active whale (has fills), crawl userFillsByTime going back as far as
data exists. Replay fills to reconstruct position state at every fill timestamp.

Output: per-whale, per-coin position time series at fill-level resolution.
Store in MongoDB `hl_whale_positions_reconstructed` for IC analysis at any frequency.

Then aggregate to consensus at any desired frequency for the IC backtest.
"""

import asyncio
import aiohttp
import time
import os
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from pymongo import MongoClient, UpdateOne
import pandas as pd
import numpy as np

HL_INFO_URL = "https://api.hyperliquid.xyz/info"
HL_LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
CONCURRENCY = 5  # conservative to avoid rate limits during backfill
LOOKBACK_DAYS = 365
PAGE_WINDOW_DAYS = 30  # 30-day windows for pagination

COINS_OF_INTEREST = {
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK",
    "DOT", "UNI", "NEAR", "APT", "ARB", "OP", "SUI", "SEI",
    "WLD", "LTC", "BCH", "BNB", "TAO", "HYPE", "ENA", "WIF",
    "PEPE", "FET", "ONDO", "RENDER", "INJ", "TIA",
}


def get_db():
    client = MongoClient(MONGO_URI)
    db_name = MONGO_URI.rsplit("/", 1)[-1]
    return client[db_name]


async def fetch_leaderboard(session: aiohttp.ClientSession) -> list[dict]:
    async with session.get(HL_LEADERBOARD_URL) as resp:
        data = await resp.json()
    rows = data.get("leaderboardRows", data) if isinstance(data, dict) else data
    whales = []
    for entry in rows:
        try:
            av = float(entry.get("accountValue", 0))
            if av >= 1_000_000:
                whales.append({"address": entry["ethAddress"], "account_value": av})
        except (ValueError, KeyError):
            continue
    whales.sort(key=lambda x: x["account_value"], reverse=True)
    return whales[:500]


async def scan_active_whales(
    session: aiohttp.ClientSession,
    whales: list[dict],
) -> list[dict]:
    """Find whales that have fills (active traders, not vaults)."""
    print(f"Scanning {len(whales)} whales for fill activity...")
    sem = asyncio.Semaphore(CONCURRENCY * 2)
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - 30 * 86400 * 1000

    async def check(w):
        async with sem:
            try:
                async with session.post(HL_INFO_URL, json={
                    "type": "userFillsByTime",
                    "user": w["address"],
                    "startTime": start_ms,
                    "endTime": end_ms,
                }, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    fills = await resp.json()
                return w["address"], len(fills) if isinstance(fills, list) else 0
            except Exception:
                return w["address"], 0

    results = await asyncio.gather(*[check(w) for w in whales])
    active = [addr for addr, n in results if n > 0]
    print(f"  Active (have fills): {len(active)}/{len(whales)}")
    return active


async def fetch_all_fills(
    session: aiohttp.ClientSession,
    address: str,
    lookback_days: int = LOOKBACK_DAYS,
) -> list[dict]:
    """Fetch all fills for an address, paginating in time windows."""
    end_ms = int(time.time() * 1000)
    all_fills = []

    # Walk backwards in PAGE_WINDOW_DAYS chunks
    window_end = end_ms
    empty_windows = 0

    while True:
        window_start = window_end - PAGE_WINDOW_DAYS * 86400 * 1000
        earliest_allowed = end_ms - lookback_days * 86400 * 1000

        if window_end < earliest_allowed:
            break

        window_start = max(window_start, earliest_allowed)

        try:
            async with session.post(HL_INFO_URL, json={
                "type": "userFillsByTime",
                "user": address,
                "startTime": window_start,
                "endTime": window_end,
            }, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                fills = await resp.json()
        except Exception:
            fills = []

        if not fills or not isinstance(fills, list):
            empty_windows += 1
            if empty_windows >= 3:
                break  # 3 consecutive empty windows = no more history
            window_end = window_start
            continue

        empty_windows = 0
        all_fills.extend(fills)

        if len(fills) >= 2000:
            # Paginated -- narrow window to get remaining fills
            oldest_time = min(int(f["time"]) for f in fills)
            window_end = oldest_time - 1
        else:
            window_end = window_start

    # Deduplicate by (time, coin, side, sz, px) and sort chronologically
    seen = set()
    unique_fills = []
    for f in all_fills:
        key = (f.get("time"), f.get("coin"), f.get("side"), f.get("sz"), f.get("px"))
        if key not in seen:
            seen.add(key)
            unique_fills.append(f)

    unique_fills.sort(key=lambda f: int(f["time"]))
    return unique_fills


def reconstruct_positions(fills: list[dict], address: str) -> list[dict]:
    """
    Replay fills chronologically to reconstruct position state at every fill.

    Each fill changes the position for that coin. Between fills, position is constant.
    Output: one doc per fill with the resulting position state.
    """
    # Track position per coin: {coin: signed_size}
    positions = defaultdict(float)
    entries = defaultdict(float)  # weighted avg entry price

    position_events = []

    for f in fills:
        coin = f.get("coin", "")
        if coin not in COINS_OF_INTEREST:
            continue

        ts_ms = int(f["time"])
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        side = f.get("side", "")  # "B" (buy) or "A" (sell/ask)
        sz = float(f.get("sz", 0))
        px = float(f.get("px", 0))
        direction = f.get("dir", "")  # "Open Long", "Close Short", etc.
        closed_pnl = float(f.get("closedPnl", 0))

        # Compute position change
        if "Open Long" in direction or "Buy" in direction:
            delta = sz
        elif "Open Short" in direction or "Sell" in direction:
            delta = -sz
        elif "Close Long" in direction:
            delta = -sz
        elif "Close Short" in direction:
            delta = sz
        else:
            # Fallback: B = buy = +, A = sell = -
            delta = sz if side == "B" else -sz

        old_pos = positions[coin]
        positions[coin] += delta
        new_pos = positions[coin]

        position_events.append({
            "timestamp_utc": ts,
            "timestamp_ms": ts_ms,
            "address": address,
            "coin": coin,
            "fill_side": side,
            "fill_sz": sz,
            "fill_px": px,
            "fill_dir": direction,
            "closed_pnl": closed_pnl,
            "position_before": round(old_pos, 8),
            "position_after": round(new_pos, 8),
            "position_notional": abs(round(new_pos * px, 2)),
            "position_side": "LONG" if new_pos > 0 else ("SHORT" if new_pos < 0 else "FLAT"),
        })

    return position_events


def build_consensus_timeseries(
    db,
    resample_freq: str = "1h",
) -> pd.DataFrame:
    """
    From reconstructed positions, build consensus at desired frequency.

    For each (coin, timestamp), compute:
    - net_bias: (sum_long_notional - sum_short_notional) / total
    - n_long, n_short
    - total_notional
    """
    print(f"\nBuilding consensus time series at {resample_freq} frequency...")

    # Load all reconstructed position events
    docs = list(db.hl_whale_positions_reconstructed.find(
        {}, {"_id": 0, "timestamp_utc": 1, "address": 1, "coin": 1,
             "position_after": 1, "fill_px": 1, "position_notional": 1,
             "position_side": 1}
    ).sort("timestamp_utc", 1))

    if not docs:
        print("  No reconstructed positions found.")
        return pd.DataFrame()

    df = pd.DataFrame(docs)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)

    print(f"  Loaded {len(df)} position events")
    print(f"  Date range: {df['timestamp_utc'].min()} to {df['timestamp_utc'].max()}")
    print(f"  Whales: {df['address'].nunique()}, Coins: {df['coin'].nunique()}")

    # For each whale+coin, forward-fill position to create continuous series
    # Then resample to desired frequency
    all_consensus = []

    # Get unique timestamps at desired frequency
    min_t = df["timestamp_utc"].min().floor(resample_freq)
    max_t = df["timestamp_utc"].max().ceil(resample_freq)
    time_index = pd.date_range(min_t, max_t, freq=resample_freq, tz=timezone.utc)

    for coin in sorted(df["coin"].unique()):
        coin_df = df[df["coin"] == coin].copy()

        # For each whale, create position series and forward-fill
        whale_positions = {}  # {addr: Series of position_after at event times}

        for addr in coin_df["address"].unique():
            addr_df = coin_df[coin_df["address"] == addr].sort_values("timestamp_utc")
            # Keep last position per timestamp
            series = addr_df.groupby("timestamp_utc").last()["position_after"]
            # Reindex to full time grid and forward-fill
            series = series.reindex(time_index, method="ffill").fillna(0)
            whale_positions[addr] = series

        if not whale_positions:
            continue

        # Build position matrix: (time x whales)
        pos_matrix = pd.DataFrame(whale_positions, index=time_index)

        # Get latest price per coin for notional calculation
        last_prices = coin_df.groupby("timestamp_utc")["fill_px"].last()
        last_prices = last_prices.reindex(time_index, method="ffill")

        for t in time_index:
            positions_at_t = pos_matrix.loc[t]
            px = last_prices.get(t, 0)
            if px == 0 or positions_at_t.abs().sum() == 0:
                continue

            longs = positions_at_t[positions_at_t > 0]
            shorts = positions_at_t[positions_at_t < 0]

            long_notional = (longs * px).sum()
            short_notional = (shorts.abs() * px).sum()
            total = long_notional + short_notional

            if total < 1000:  # skip trivial
                continue

            net_bias = (long_notional - short_notional) / total

            all_consensus.append({
                "timestamp_utc": t,
                "coin": coin,
                "net_bias": round(net_bias, 4),
                "long_notional": round(long_notional, 2),
                "short_notional": round(short_notional, 2),
                "total_notional": round(total, 2),
                "n_long": len(longs[longs != 0]),
                "n_short": len(shorts[shorts != 0]),
                "n_total": len(longs[longs != 0]) + len(shorts[shorts != 0]),
                "source": "reconstructed",
            })

    consensus_df = pd.DataFrame(all_consensus)
    print(f"  Built {len(consensus_df)} consensus snapshots")
    if not consensus_df.empty:
        print(f"  Coins covered: {consensus_df['coin'].nunique()}")
        print(f"  Time range: {consensus_df['timestamp_utc'].min()} to {consensus_df['timestamp_utc'].max()}")

    return consensus_df


async def main():
    print("=" * 70)
    print("D1: Hyperliquid Whale Position Backfill")
    print(f"Lookback: {LOOKBACK_DAYS} days")
    print("=" * 70)

    db = get_db()

    async with aiohttp.ClientSession() as session:
        # Step 1: Find active whales
        whales = await fetch_leaderboard(session)
        active_addrs = await scan_active_whales(session, whales)

        if not active_addrs:
            print("ERROR: No active whales found")
            return

        # Step 2: Fetch fills for each active whale
        print(f"\nFetching fills for {len(active_addrs)} active whales...")
        sem = asyncio.Semaphore(CONCURRENCY)

        total_fills = 0
        total_events = 0

        for i, addr in enumerate(active_addrs):
            async with sem:
                t0 = time.time()
                fills = await fetch_all_fills(session, addr, LOOKBACK_DAYS)
                elapsed = time.time() - t0

                if not fills:
                    continue

                # Reconstruct positions
                events = reconstruct_positions(fills, addr)

                if events:
                    # Store in MongoDB
                    db.hl_whale_positions_reconstructed.insert_many(events)
                    total_events += len(events)

                total_fills += len(fills)

                # Find date range
                if fills:
                    oldest = datetime.fromtimestamp(int(fills[0]["time"]) / 1000, tz=timezone.utc)
                    newest = datetime.fromtimestamp(int(fills[-1]["time"]) / 1000, tz=timezone.utc)
                    days = (newest - oldest).days
                else:
                    days = 0

                coins = len(set(f["coin"] for f in fills if f.get("coin") in COINS_OF_INTEREST))
                print(f"  [{i+1}/{len(active_addrs)}] {addr[:10]}...: "
                      f"{len(fills)} fills, {len(events)} events, "
                      f"{coins} coins, {days}d history, {elapsed:.1f}s")

    print(f"\n{'─'*70}")
    print(f"BACKFILL COMPLETE")
    print(f"  Active whales: {len(active_addrs)}")
    print(f"  Total fills fetched: {total_fills:,}")
    print(f"  Position events stored: {total_events:,}")

    # Create indexes
    db.hl_whale_positions_reconstructed.create_index([("timestamp_utc", 1), ("coin", 1)])
    db.hl_whale_positions_reconstructed.create_index([("coin", 1), ("timestamp_utc", 1)])
    db.hl_whale_positions_reconstructed.create_index([("address", 1), ("coin", 1), ("timestamp_utc", 1)])

    # Step 3: Build consensus time series
    consensus_df = build_consensus_timeseries(db, resample_freq="1h")

    if not consensus_df.empty:
        # Store consensus
        db.hl_whale_consensus_reconstructed.drop()
        db.hl_whale_consensus_reconstructed.insert_many(consensus_df.to_dict("records"))
        db.hl_whale_consensus_reconstructed.create_index([("timestamp_utc", 1), ("coin", 1)])
        db.hl_whale_consensus_reconstructed.create_index([("coin", 1), ("timestamp_utc", 1)])
        print(f"  Stored {len(consensus_df)} consensus docs in hl_whale_consensus_reconstructed")

    print(f"\nDone. Run d1_hl_whale_ic_backtest.py with --reconstructed flag to test.")


if __name__ == "__main__":
    asyncio.run(main())
