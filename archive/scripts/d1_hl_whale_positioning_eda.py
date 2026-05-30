"""
D1: Hyperliquid Whale Positioning EDA (Phase 0)

Goal: Determine if HL whale position concentration predicts Bybit price moves.

Approach:
1. Fetch HL leaderboard → top whales by account value
2. Query each whale's clearinghouseState → live positions
3. Compute per-coin whale consensus metrics:
   - net_bias = (long_notional - short_notional) / total_notional
   - whale_count_long, whale_count_short
   - hhi = Herfindahl concentration of position sizes
4. Merge with Bybit 1h forward returns
5. Check if whale consensus predicts direction

No auth needed. All public HL data.
"""

import asyncio
import aiohttp
import pandas as pd
import numpy as np
from datetime import datetime, timezone
import json
import time
import sys
import os

# ── Config ──────────────────────────────────────────────────────────────
HL_INFO_URL = "https://api.hyperliquid.xyz/info"
HL_LEADERBOARD_URL = "https://stats-data.hyperliquid.xyz/Mainnet/leaderboard"
MIN_ACCOUNT_VALUE_USD = 1_000_000  # $1M+ whales
MAX_WHALES = 500  # cap for speed
CONCURRENCY = 10  # parallel requests
COINS_OF_INTEREST = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK",
    "DOT", "UNI", "NEAR", "APT", "ARB", "OP", "SUI", "SEI",
    "WLD", "LTC", "BCH", "BNB", "TAO", "HYPE", "ENA", "WIF",
    "PEPE", "FET", "ONDO", "RENDER", "INJ", "TIA",
]

OUTPUT_DIR = "app/data/cache/d1_hl_whale"
os.makedirs(OUTPUT_DIR, exist_ok=True)


async def fetch_leaderboard(session: aiohttp.ClientSession) -> list[dict]:
    """Fetch HL leaderboard (all addresses with PnL data)."""
    print(f"[1/5] Fetching HL leaderboard...")
    async with session.get(HL_LEADERBOARD_URL) as resp:
        data = await resp.json()

    # Filter to whales
    whales = []
    for entry in data.get("leaderboardRows", data) if isinstance(data, dict) else data:
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
    whales = whales[:MAX_WHALES]
    print(f"  Found {len(whales)} whales with AV >= ${MIN_ACCOUNT_VALUE_USD/1e6:.0f}M")
    print(f"  Top 5: {[f'{w['display_name'] or w['address'][:10]}... (${w['account_value']/1e6:.1f}M)' for w in whales[:5]]}")
    return whales


async def fetch_clearinghouse_state(
    session: aiohttp.ClientSession,
    address: str,
    semaphore: asyncio.Semaphore,
) -> dict | None:
    """Fetch a single whale's positions."""
    async with semaphore:
        try:
            payload = {"type": "clearinghouseState", "user": address}
            async with session.post(HL_INFO_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                return data
        except Exception as e:
            return None


async def fetch_all_whale_positions(
    session: aiohttp.ClientSession,
    whales: list[dict],
) -> list[dict]:
    """Fetch positions for all whales with concurrency control."""
    print(f"\n[2/5] Fetching positions for {len(whales)} whales (concurrency={CONCURRENCY})...")
    semaphore = asyncio.Semaphore(CONCURRENCY)
    tasks = [
        fetch_clearinghouse_state(session, w["address"], semaphore)
        for w in whales
    ]

    results = []
    done = 0
    for coro in asyncio.as_completed(tasks):
        result = await coro
        done += 1
        if done % 50 == 0:
            print(f"  Progress: {done}/{len(whales)}")
        if result is not None:
            results.append(result)

    print(f"  Got positions for {len(results)}/{len(whales)} whales")
    return results


def extract_positions(clearinghouse_states: list[dict], whales: list[dict]) -> pd.DataFrame:
    """Extract all positions into a flat DataFrame."""
    rows = []
    for i, state in enumerate(clearinghouse_states):
        if not state:
            continue
        margin = state.get("marginSummary", {})
        av = float(margin.get("accountValue", 0))
        positions = state.get("assetPositions", [])

        for pos in positions:
            p = pos.get("position", pos)
            coin = p.get("coin", "")
            szi = float(p.get("szi", 0))
            if szi == 0:
                continue

            entry_px = float(p.get("entryPx", 0))
            pos_value = abs(float(p.get("positionValue", szi * entry_px)))
            unrealized_pnl = float(p.get("unrealizedPnl", 0))
            leverage_info = p.get("leverage", {})
            lev_value = float(leverage_info.get("value", 1)) if isinstance(leverage_info, dict) else 1

            rows.append({
                "coin": coin,
                "side": "LONG" if szi > 0 else "SHORT",
                "size": abs(szi),
                "notional_usd": pos_value,
                "entry_px": entry_px,
                "unrealized_pnl": unrealized_pnl,
                "leverage": lev_value,
                "account_value": av,
            })

    df = pd.DataFrame(rows)
    print(f"\n[3/5] Extracted {len(df)} positions across {df['coin'].nunique()} coins")
    return df


def compute_whale_consensus(positions_df: pd.DataFrame) -> pd.DataFrame:
    """Compute per-coin whale positioning metrics."""
    if positions_df.empty:
        return pd.DataFrame()

    # Filter to coins of interest
    df = positions_df[positions_df["coin"].isin(COINS_OF_INTEREST)].copy()

    metrics = []
    for coin, group in df.groupby("coin"):
        longs = group[group["side"] == "LONG"]
        shorts = group[group["side"] == "SHORT"]

        long_notional = longs["notional_usd"].sum()
        short_notional = shorts["notional_usd"].sum()
        total_notional = long_notional + short_notional

        if total_notional == 0:
            continue

        net_bias = (long_notional - short_notional) / total_notional  # -1 to +1
        n_long = len(longs)
        n_short = len(shorts)
        n_total = n_long + n_short

        # Herfindahl concentration (higher = more concentrated in fewer whales)
        shares = (group["notional_usd"] / total_notional) ** 2
        hhi = shares.sum()

        # Top whale dominance
        top_whale_pct = group["notional_usd"].max() / total_notional if total_notional > 0 else 0

        # Average leverage
        avg_leverage = group["leverage"].mean()

        # Aggregate unrealized PnL
        total_upnl = group["unrealized_pnl"].sum()

        metrics.append({
            "coin": coin,
            "net_bias": net_bias,
            "long_notional": long_notional,
            "short_notional": short_notional,
            "total_notional": total_notional,
            "n_long": n_long,
            "n_short": n_short,
            "n_total": n_total,
            "long_pct": n_long / n_total if n_total > 0 else 0.5,
            "hhi": hhi,
            "top_whale_pct": top_whale_pct,
            "avg_leverage": avg_leverage,
            "total_unrealized_pnl": total_upnl,
        })

    consensus = pd.DataFrame(metrics).sort_values("total_notional", ascending=False)
    return consensus


async def fetch_hl_market_data(session: aiohttp.ClientSession) -> dict:
    """Fetch current HL market data (OI, funding, prices)."""
    payload = {"type": "metaAndAssetCtxs"}
    async with session.post(HL_INFO_URL, json=payload) as resp:
        data = await resp.json()

    meta = data[0]
    ctxs = data[1]
    universe = meta["universe"]

    market = {}
    for i, asset in enumerate(universe):
        coin = asset["name"]
        if coin in COINS_OF_INTEREST:
            ctx = ctxs[i]
            market[coin] = {
                "mark_px": float(ctx.get("markPx", 0)),
                "mid_px": float(ctx.get("midPx", 0)),
                "oracle_px": float(ctx.get("oraclePx", 0)),
                "open_interest": float(ctx.get("openInterest", 0)),
                "funding_rate": float(ctx.get("funding", 0)),
                "premium": float(ctx.get("premium", 0)),
                "day_volume": float(ctx.get("dayNtlVlm", 0)),
            }
    return market


async def main():
    ts = datetime.now(timezone.utc)
    print(f"=" * 70)
    print(f"D1: Hyperliquid Whale Positioning EDA")
    print(f"Timestamp: {ts.isoformat()}")
    print(f"=" * 70)

    async with aiohttp.ClientSession() as session:
        # Step 1: Get whale addresses
        whales = await fetch_leaderboard(session)

        if not whales:
            print("ERROR: No whales found in leaderboard")
            return

        # Step 2: Get all positions
        t0 = time.time()
        states = await fetch_all_whale_positions(session, whales)
        elapsed = time.time() - t0
        print(f"  Fetch time: {elapsed:.1f}s ({len(whales)/elapsed:.1f} req/s effective)")

        # Step 3: Extract and compute consensus
        positions_df = extract_positions(states, whales)
        if positions_df.empty:
            print("ERROR: No positions found")
            return

        consensus = compute_whale_consensus(positions_df)

        # Step 4: Get HL market data
        print(f"\n[4/5] Fetching HL market data...")
        market = await fetch_hl_market_data(session)

        # Merge market data
        market_df = pd.DataFrame.from_dict(market, orient="index").reset_index()
        market_df.columns = ["coin"] + list(market_df.columns[1:])
        consensus = consensus.merge(market_df, on="coin", how="left")

    # ── Analysis ──────────────────────────────────────────────────────
    print(f"\n[5/5] Analysis")
    print(f"\n{'='*70}")
    print(f"WHALE POSITIONING SNAPSHOT ({ts.strftime('%Y-%m-%d %H:%M UTC')})")
    print(f"{'='*70}")
    print(f"Whales queried: {len(whales)} (AV >= ${MIN_ACCOUNT_VALUE_USD/1e6:.0f}M)")
    print(f"Positions found: {len(positions_df)}")
    print(f"Coins with whale activity: {len(consensus)}")

    # Table: Whale consensus per coin
    print(f"\n{'─'*70}")
    print(f"PER-COIN WHALE CONSENSUS (sorted by total notional)")
    print(f"{'─'*70}")
    print(f"{'Coin':<8} {'Bias':>6} {'Long$':>10} {'Short$':>10} {'Total$':>10} {'#L':>4} {'#S':>4} {'HHI':>5} {'TopWh%':>6} {'AvgLev':>6} {'Funding':>9}")
    print(f"{'─'*8} {'─'*6} {'─'*10} {'─'*10} {'─'*10} {'─'*4} {'─'*4} {'─'*5} {'─'*6} {'─'*6} {'─'*9}")

    for _, row in consensus.iterrows():
        bias_str = f"{row['net_bias']:+.2f}"
        funding = row.get('funding_rate', 0)
        funding_str = f"{funding*100:+.4f}%" if pd.notna(funding) else "N/A"
        print(
            f"{row['coin']:<8} {bias_str:>6} "
            f"{row['long_notional']/1e6:>9.1f}M {row['short_notional']/1e6:>9.1f}M "
            f"{row['total_notional']/1e6:>9.1f}M "
            f"{row['n_long']:>4} {row['n_short']:>4} "
            f"{row['hhi']:>5.3f} {row['top_whale_pct']*100:>5.1f}% "
            f"{row['avg_leverage']:>5.1f}x {funding_str:>9}"
        )

    # Extreme positioning (potential signals)
    print(f"\n{'─'*70}")
    print(f"EXTREME POSITIONING (|net_bias| > 0.5)")
    print(f"{'─'*70}")
    extreme = consensus[consensus["net_bias"].abs() > 0.5].sort_values("net_bias")
    if extreme.empty:
        print("  No extreme positioning detected")
    else:
        for _, row in extreme.iterrows():
            direction = "LONG" if row["net_bias"] > 0 else "SHORT"
            print(f"  {row['coin']}: {row['net_bias']:+.2f} bias ({direction} heavy)")
            print(f"    {row['n_long']}L / {row['n_short']}S whales, "
                  f"${row['total_notional']/1e6:.1f}M total, "
                  f"HHI={row['hhi']:.3f}, top whale={row['top_whale_pct']*100:.1f}%")

    # Funding alignment check
    print(f"\n{'─'*70}")
    print(f"FUNDING vs WHALE BIAS ALIGNMENT")
    print(f"{'─'*70}")
    print(f"(Positive funding = longs pay shorts. If whales are long AND funding is positive,")
    print(f" they're paying to hold = strong conviction. If misaligned = potential signal)")
    for _, row in consensus.iterrows():
        funding = row.get("funding_rate", 0)
        if pd.isna(funding) or funding == 0:
            continue
        bias = row["net_bias"]
        funding_direction = "long_pay" if funding > 0 else "short_pay"
        whale_direction = "LONG" if bias > 0 else "SHORT"

        # Misalignment: whales long but funding negative (shorts pay → cheap to be long)
        # or whales short but funding positive (longs pay → cheap to be short)
        aligned = (bias > 0 and funding > 0) or (bias < 0 and funding < 0)
        if abs(bias) > 0.3:
            status = "ALIGNED (conviction)" if aligned else "MISALIGNED (signal?)"
            print(f"  {row['coin']}: whale={whale_direction} ({bias:+.2f}), funding={funding_direction} ({funding*100:+.4f}%) → {status}")

    # Concentration risk analysis
    print(f"\n{'─'*70}")
    print(f"CONCENTRATION RISK (HHI > 0.25 = single whale dominates)")
    print(f"{'─'*70}")
    concentrated = consensus[consensus["hhi"] > 0.25].sort_values("hhi", ascending=False)
    if concentrated.empty:
        print("  No highly concentrated positions")
    else:
        for _, row in concentrated.iterrows():
            print(f"  {row['coin']}: HHI={row['hhi']:.3f}, top whale has {row['top_whale_pct']*100:.1f}% of total notional")

    # Summary stats
    print(f"\n{'─'*70}")
    print(f"SUMMARY STATISTICS")
    print(f"{'─'*70}")
    total_long = consensus["long_notional"].sum()
    total_short = consensus["short_notional"].sum()
    overall_bias = (total_long - total_short) / (total_long + total_short) if (total_long + total_short) > 0 else 0
    print(f"  Total whale long exposure: ${total_long/1e6:.1f}M")
    print(f"  Total whale short exposure: ${total_short/1e6:.1f}M")
    print(f"  Overall market bias: {overall_bias:+.3f}")
    print(f"  Coins with extreme bias (|b|>0.5): {len(extreme)}")
    print(f"  Avg positions per whale: {len(positions_df) / len(states):.1f}")

    # Save snapshots
    ts_str = ts.strftime("%Y%m%d_%H%M")
    consensus.to_csv(f"{OUTPUT_DIR}/consensus_{ts_str}.csv", index=False)
    positions_df.to_csv(f"{OUTPUT_DIR}/positions_{ts_str}.csv", index=False)
    print(f"\n  Saved to {OUTPUT_DIR}/consensus_{ts_str}.csv")
    print(f"  Saved to {OUTPUT_DIR}/positions_{ts_str}.csv")

    # Signal quality indicators
    print(f"\n{'='*70}")
    print(f"SIGNAL QUALITY ASSESSMENT")
    print(f"{'='*70}")
    n_extreme = len(extreme)
    n_coins = len(consensus)
    avg_hhi = consensus["hhi"].mean()
    print(f"  Extreme bias coins: {n_extreme}/{n_coins} ({n_extreme/n_coins*100:.0f}%)")
    print(f"  Average HHI: {avg_hhi:.3f} ({'concentrated' if avg_hhi > 0.15 else 'distributed'})")
    print(f"  Refresh feasibility: {len(whales)} whales @ {CONCURRENCY} concurrent = ~{len(whales)/CONCURRENCY/1.4:.0f}s per sweep")
    print(f"\n  NEXT STEP: Run this every 15 min for 48h, then correlate")
    print(f"  whale bias changes with Bybit 1h forward returns.")
    print(f"  If IC > 0.05 on bias_change → direction, proceed to controller.")


if __name__ == "__main__":
    asyncio.run(main())
