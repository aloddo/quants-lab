"""
D1: Hyperliquid Whale Position Collector

Runs every 15 minutes (via cron or loop), polls top whales' positions,
computes per-coin consensus metrics, and stores snapshots in MongoDB.

After 48h+ of data, run d1_hl_whale_backtest.py to correlate with Bybit returns.

Collections:
  - hl_whale_consensus: per-coin consensus metrics (one doc per coin per snapshot)
  - hl_whale_positions: individual whale positions (full detail, for debugging)
  - hl_whale_snapshots: snapshot metadata (timing, counts)

Usage:
  # Single snapshot:
  python scripts/d1_hl_whale_collector.py --once

  # Continuous (15-min loop):
  python scripts/d1_hl_whale_collector.py

  # Custom interval:
  python scripts/d1_hl_whale_collector.py --interval 900
"""

import asyncio
import aiohttp
import argparse
import time
import sys
import os
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne

# ── Config ──────────────────────────────────────────────────────────────
HL_INFO_URL = "https://api.hyperliquid.xyz/info"
HL_LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
MONGO_DB = os.environ.get("MONGO_DATABASE", "quants_lab")

MIN_ACCOUNT_VALUE_USD = 1_000_000
MAX_WHALES = 500
CONCURRENCY = 10
DEFAULT_INTERVAL_S = 900  # 15 minutes

COINS_OF_INTEREST = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK",
    "DOT", "UNI", "NEAR", "APT", "ARB", "OP", "SUI", "SEI",
    "WLD", "LTC", "BCH", "BNB", "TAO", "HYPE", "ENA", "WIF",
    "PEPE", "FET", "ONDO", "RENDER", "INJ", "TIA",
]


def get_db():
    client = MongoClient(MONGO_URI)
    db_name = MONGO_URI.rsplit("/", 1)[-1] if "/" in MONGO_URI else MONGO_DB
    return client[db_name]


async def fetch_leaderboard(session: aiohttp.ClientSession) -> list[dict]:
    async with session.get(HL_LEADERBOARD_URL) as resp:
        data = await resp.json()

    whales = []
    rows = data.get("leaderboardRows", data) if isinstance(data, dict) else data
    for entry in rows:
        try:
            av = float(entry.get("accountValue", 0))
            if av >= MIN_ACCOUNT_VALUE_USD:
                whales.append({
                    "address": entry["ethAddress"],
                    "account_value": av,
                    "display_name": entry.get("displayName", ""),
                })
        except (ValueError, KeyError):
            continue

    whales.sort(key=lambda x: x["account_value"], reverse=True)
    return whales[:MAX_WHALES]


async def fetch_clearinghouse_state(
    session: aiohttp.ClientSession,
    address: str,
    semaphore: asyncio.Semaphore,
) -> tuple[str, dict | None]:
    async with semaphore:
        try:
            payload = {"type": "clearinghouseState", "user": address}
            async with session.post(
                HL_INFO_URL, json=payload,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return address, None
                return address, await resp.json()
        except Exception:
            return address, None


async def fetch_market_data(session: aiohttp.ClientSession) -> dict:
    payload = {"type": "metaAndAssetCtxs"}
    async with session.post(HL_INFO_URL, json=payload) as resp:
        data = await resp.json()

    meta, ctxs = data[0], data[1]
    market = {}
    for i, asset in enumerate(meta["universe"]):
        coin = asset["name"]
        ctx = ctxs[i]
        market[coin] = {
            "mark_px": float(ctx.get("markPx") or 0),
            "open_interest": float(ctx.get("openInterest") or 0),
            "funding_rate": float(ctx.get("funding") or 0),
            "premium": float(ctx.get("premium") or 0),
            "day_volume": float(ctx.get("dayNtlVlm") or 0),
        }
    return market


async def collect_snapshot() -> dict:
    """Run one collection cycle, return summary."""
    ts = datetime.now(timezone.utc)
    t0 = time.time()

    async with aiohttp.ClientSession() as session:
        # Fetch whales + market data in parallel
        whales_task = fetch_leaderboard(session)
        market_task = fetch_market_data(session)
        whales, market = await asyncio.gather(whales_task, market_task)

        if not whales:
            return {"error": "no whales found", "ts": ts}

        # Fetch all positions
        semaphore = asyncio.Semaphore(CONCURRENCY)
        tasks = [
            fetch_clearinghouse_state(session, w["address"], semaphore)
            for w in whales
        ]
        results = await asyncio.gather(*tasks)

    # Build address -> whale mapping
    addr_to_whale = {w["address"]: w for w in whales}

    # Extract positions and compute consensus
    position_docs = []
    coin_agg = {}  # coin -> {long_notional, short_notional, n_long, n_short, positions: [...]}

    success_count = 0
    for address, state in results:
        if state is None:
            continue
        success_count += 1

        whale = addr_to_whale.get(address, {})
        margin = state.get("marginSummary", {})
        av = float(margin.get("accountValue", 0))

        for pos_wrapper in state.get("assetPositions", []):
            p = pos_wrapper.get("position", pos_wrapper)
            coin = p.get("coin", "")
            szi = float(p.get("szi", 0))
            if szi == 0 or coin not in COINS_OF_INTEREST:
                continue

            entry_px = float(p.get("entryPx", 0))
            pos_value = abs(float(p.get("positionValue", szi * entry_px)))
            unrealized_pnl = float(p.get("unrealizedPnl", 0))
            leverage_info = p.get("leverage", {})
            lev = float(leverage_info.get("value", 1)) if isinstance(leverage_info, dict) else 1

            side = "LONG" if szi > 0 else "SHORT"

            position_docs.append({
                "timestamp_utc": ts,
                "address": address,
                "coin": coin,
                "side": side,
                "size": abs(szi),
                "notional_usd": pos_value,
                "entry_px": entry_px,
                "unrealized_pnl": unrealized_pnl,
                "leverage": lev,
                "account_value": av,
            })

            if coin not in coin_agg:
                coin_agg[coin] = {"long_n": 0, "short_n": 0, "long_usd": 0.0, "short_usd": 0.0, "sizes": []}
            agg = coin_agg[coin]
            if side == "LONG":
                agg["long_n"] += 1
                agg["long_usd"] += pos_value
            else:
                agg["short_n"] += 1
                agg["short_usd"] += pos_value
            agg["sizes"].append(pos_value)

    # Build consensus docs
    consensus_docs = []
    for coin, agg in coin_agg.items():
        total = agg["long_usd"] + agg["short_usd"]
        if total == 0:
            continue

        net_bias = (agg["long_usd"] - agg["short_usd"]) / total
        n_total = agg["long_n"] + agg["short_n"]
        hhi = sum((s / total) ** 2 for s in agg["sizes"])
        top_whale_pct = max(agg["sizes"]) / total

        mkt = market.get(coin, {})

        consensus_docs.append({
            "timestamp_utc": ts,
            "coin": coin,
            "net_bias": round(net_bias, 4),
            "long_notional": round(agg["long_usd"], 2),
            "short_notional": round(agg["short_usd"], 2),
            "total_notional": round(total, 2),
            "n_long": agg["long_n"],
            "n_short": agg["short_n"],
            "n_total": n_total,
            "long_pct": round(agg["long_n"] / n_total, 4) if n_total > 0 else 0.5,
            "hhi": round(hhi, 4),
            "top_whale_pct": round(top_whale_pct, 4),
            "mark_px": mkt.get("mark_px", 0),
            "hl_oi": mkt.get("open_interest", 0),
            "hl_funding": mkt.get("funding_rate", 0),
            "hl_volume": mkt.get("day_volume", 0),
        })

    # Store in MongoDB
    db = get_db()
    elapsed = time.time() - t0

    if consensus_docs:
        db.hl_whale_consensus.insert_many(consensus_docs)
    if position_docs:
        db.hl_whale_positions.insert_many(position_docs)

    snapshot_meta = {
        "timestamp_utc": ts,
        "whales_queried": len(whales),
        "whales_success": success_count,
        "positions_found": len(position_docs),
        "coins_covered": len(consensus_docs),
        "fetch_time_s": round(elapsed, 1),
        "total_long_usd": sum(d["long_notional"] for d in consensus_docs),
        "total_short_usd": sum(d["short_notional"] for d in consensus_docs),
    }
    db.hl_whale_snapshots.insert_one(snapshot_meta)

    return snapshot_meta


def ensure_indexes():
    """Create indexes for efficient querying."""
    db = get_db()
    db.hl_whale_consensus.create_index([("timestamp_utc", 1), ("coin", 1)])
    db.hl_whale_consensus.create_index([("coin", 1), ("timestamp_utc", 1)])
    db.hl_whale_positions.create_index([("timestamp_utc", 1)])
    db.hl_whale_positions.create_index([("coin", 1), ("timestamp_utc", 1)])
    db.hl_whale_snapshots.create_index([("timestamp_utc", 1)])


async def run_loop(interval_s: int):
    """Run collector in a loop."""
    ensure_indexes()
    print(f"D1 Whale Collector started (interval={interval_s}s)")
    print(f"MongoDB: {MONGO_URI}")
    print(f"Whales: top {MAX_WHALES} with AV >= ${MIN_ACCOUNT_VALUE_USD/1e6:.0f}M")
    print(f"Coins: {len(COINS_OF_INTEREST)}")
    print(f"{'─'*60}")

    while True:
        try:
            summary = await collect_snapshot()
            ts = summary.get("timestamp_utc", datetime.now(timezone.utc))
            ts_str = ts.strftime("%H:%M:%S") if hasattr(ts, "strftime") else str(ts)
            print(
                f"[{ts_str}] "
                f"whales={summary.get('whales_success', 0)}/{summary.get('whales_queried', 0)}, "
                f"positions={summary.get('positions_found', 0)}, "
                f"coins={summary.get('coins_covered', 0)}, "
                f"L=${summary.get('total_long_usd', 0)/1e6:.0f}M / "
                f"S=${summary.get('total_short_usd', 0)/1e6:.0f}M, "
                f"time={summary.get('fetch_time_s', 0):.1f}s"
            )
        except Exception as e:
            print(f"[ERROR] {e}", file=sys.stderr)

        await asyncio.sleep(interval_s)


async def run_once():
    """Run a single snapshot."""
    ensure_indexes()
    summary = await collect_snapshot()
    print(f"Snapshot complete:")
    for k, v in summary.items():
        if k == "timestamp_utc":
            print(f"  {k}: {v.isoformat()}")
        elif isinstance(v, float) and v > 1e6:
            print(f"  {k}: ${v/1e6:.1f}M")
        else:
            print(f"  {k}: {v}")


def main():
    parser = argparse.ArgumentParser(description="D1 HL Whale Position Collector")
    parser.add_argument("--once", action="store_true", help="Run single snapshot")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL_S, help="Seconds between sweeps")
    args = parser.parse_args()

    if args.once:
        asyncio.run(run_once())
    else:
        asyncio.run(run_loop(args.interval))


if __name__ == "__main__":
    main()
