#!/usr/bin/env python3
"""
Copy Target Wallet Analysis — proper due diligence before copying.

For each candidate wallet, this script checks:
1. Portfolio structure (how many positions, long/short bias, concentration)
2. Trade behavior (directional swing trader vs portfolio rebalancer vs MM)
3. Per-trade edge (TWAP round-trip PnL with realistic fees)
4. Current live portfolio from HL API

Reads from: hl_wallet_trades (MongoDB), HL public API
Output: ranked candidates with full profile

Usage:
    python scripts/copy_target_analysis.py --top 30
"""
import argparse
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone

import numpy as np
import requests
from pymongo import MongoClient

HL_API = "https://api.hyperliquid.xyz"

def get_portfolio(addr: str) -> dict:
    """Get live portfolio from HL API."""
    try:
        r = requests.post(f"{HL_API}/info",
                          json={"type": "clearinghouseState", "user": addr}, timeout=10)
        data = r.json()
        if data is None:
            return {"type": "agent_key", "equity": 0, "positions": 0}

        margin = data.get("marginSummary", {})
        equity = float(margin.get("accountValue", 0))

        positions = []
        for p in data.get("assetPositions", []):
            pos = p["position"]
            sz = float(pos.get("szi", 0))
            if abs(sz) < 0.001:
                continue
            entry = float(pos.get("entryPx", 0))
            positions.append({
                "coin": pos["coin"], "sz": sz,
                "notional": abs(sz) * entry,
                "side": "LONG" if sz > 0 else "SHORT",
            })

        longs = [p for p in positions if p["side"] == "LONG"]
        shorts = [p for p in positions if p["side"] == "SHORT"]
        total_long = sum(p["notional"] for p in longs)
        total_short = sum(p["notional"] for p in shorts)
        total_notional = total_long + total_short

        # Classify trader type
        n_pos = len(positions)
        if n_pos == 0:
            trader_type = "inactive"
        elif n_pos <= 5:
            trader_type = "concentrated"
        elif n_pos <= 20:
            trader_type = "diversified"
        elif n_pos <= 50:
            trader_type = "portfolio_manager"
        else:
            trader_type = "systematic_basket"

        # Directional bias
        if total_notional > 0:
            long_pct = total_long / total_notional * 100
            if long_pct > 70:
                bias = "STRONG_LONG"
            elif long_pct > 55:
                bias = "SLIGHT_LONG"
            elif long_pct < 30:
                bias = "STRONG_SHORT"
            elif long_pct < 45:
                bias = "SLIGHT_SHORT"
            else:
                bias = "NEUTRAL"
        else:
            bias = "FLAT"

        return {
            "type": trader_type,
            "equity": equity,
            "positions": n_pos,
            "longs": len(longs),
            "shorts": len(shorts),
            "long_notional": total_long,
            "short_notional": total_short,
            "bias": bias,
            "long_pct": total_long / total_notional * 100 if total_notional > 0 else 0,
        }
    except Exception as e:
        return {"type": "error", "equity": 0, "positions": 0, "error": str(e)}


def analyze_trade_behavior(trades: list) -> dict:
    """Classify trading behavior from trade data."""
    if len(trades) < 10:
        return {"behavior": "insufficient_data"}

    # Group by coin
    by_coin = defaultdict(list)
    for t in trades:
        by_coin[t["coin"]].append(t)

    # For each coin, check if trades oscillate (buy-sell-buy) or are one-directional
    oscillation_scores = []
    for coin, coin_trades in by_coin.items():
        if len(coin_trades) < 3:
            continue
        # Count direction changes
        directions = [t["side"] for t in coin_trades]
        changes = sum(1 for i in range(1, len(directions)) if directions[i] != directions[i-1])
        osc_rate = changes / (len(directions) - 1)
        oscillation_scores.append(osc_rate)

    avg_oscillation = np.mean(oscillation_scores) if oscillation_scores else 0

    # Trade size consistency (MMs have very consistent sizes)
    notionals = [t["not"] for t in trades]
    cv = np.std(notionals) / np.mean(notionals) if np.mean(notionals) > 0 else 0

    # Inter-trade timing
    timestamps = sorted(t["ts"] for t in trades)
    gaps = [timestamps[i+1] - timestamps[i] for i in range(len(timestamps)-1)]
    median_gap = np.median(gaps) if gaps else 0

    # Classify
    if avg_oscillation > 0.6:
        behavior = "market_maker_or_arb"
    elif avg_oscillation > 0.4:
        behavior = "mixed_oscillating"
    elif len(by_coin) > 30:
        behavior = "basket_rebalancer"
    else:
        behavior = "directional_trader"

    return {
        "behavior": behavior,
        "oscillation": avg_oscillation,
        "coins_traded": len(by_coin),
        "size_cv": cv,
        "median_gap_s": median_gap,
        "trades": len(trades),
    }


def compute_copy_pnl(trades: list, fee_rt_bps: float = 7.76) -> dict:
    """Compute copy-tradeable PnL using TWAP round-trip matching.
    fee_rt_bps = 5.76 (IOC+maker) + 2.0 (slippage) = 7.76"""
    by_coin = defaultdict(list)
    for t in trades:
        by_coin[t["coin"]].append(t)

    round_trips = []

    for coin, coin_trades in by_coin.items():
        if len(coin_trades) < 2:
            continue

        # Build episodes (120s gap)
        episodes = []
        current = [coin_trades[0]]
        for t in coin_trades[1:]:
            if t["ts"] - current[-1]["ts"] <= 120:
                current.append(t)
            else:
                episodes.append(current)
                current = [t]
        episodes.append(current)

        # Classify directional episodes
        dir_eps = []
        for ep in episodes:
            buy_not = sum(t["not"] for t in ep if t["side"] == "BUY")
            sell_not = sum(t["not"] for t in ep if t["side"] == "SELL")
            total = buy_not + sell_not
            net = buy_not - sell_not
            if total == 0 or abs(net) / total < 0.6 or abs(net) < 500:
                continue

            side = "BUY" if net > 0 else "SELL"
            same_side = [t for t in ep if t["side"] == side]
            if not same_side:
                continue
            vwap = sum(t["px"] * t["not"] for t in same_side) / sum(t["not"] for t in same_side)
            dir_eps.append({"side": side, "vwap": vwap, "net": abs(net), "ts": ep[0]["ts"]})

        # Match round trips
        i = 0
        while i < len(dir_eps) - 1:
            entry = dir_eps[i]
            for j in range(i + 1, len(dir_eps)):
                if dir_eps[j]["side"] != entry["side"]:
                    ex = dir_eps[j]
                    hold = ex["ts"] - entry["ts"]
                    if 0 < hold < 86400 and entry["vwap"] > 0 and ex["vwap"] > 0:
                        if entry["side"] == "BUY":
                            pnl = (ex["vwap"] - entry["vwap"]) / entry["vwap"] * 10000
                        else:
                            pnl = (entry["vwap"] - ex["vwap"]) / entry["vwap"] * 10000
                        round_trips.append({"pnl": pnl, "hold_min": hold / 60, "coin": coin})
                    i = j + 1
                    break
            else:
                i += 1

    if not round_trips:
        return {"trips": 0}

    pnls = np.array([t["pnl"] for t in round_trips])
    net_pnls = pnls - fee_rt_bps

    return {
        "trips": len(round_trips),
        "gross_avg": float(np.mean(pnls)),
        "net_avg": float(np.mean(net_pnls)),
        "wr_net": float(np.mean(net_pnls > 0) * 100),
        "hold_min": float(np.mean([t["hold_min"] for t in round_trips])),
        "coins": len(set(t["coin"] for t in round_trips)),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--top", type=int, default=30, help="Top N wallets to analyze")
    parser.add_argument("--min-trades", type=int, default=50)
    parser.add_argument("--check-portfolio", action="store_true", default=True)
    args = parser.parse_args()

    db = MongoClient("mongodb://localhost:27017")["quants_lab"]
    col = db["hl_wallet_trades"]

    # Get top wallets by trade count
    pipeline = [
        {"$group": {"_id": "$buyer", "total": {"$sum": "$notional"}, "count": {"$sum": 1}}},
        {"$match": {"count": {"$gte": args.min_trades}, "total": {"$gte": 50000}}},
        {"$sort": {"count": -1}},
        {"$limit": args.top * 2},
    ]
    candidates = [r for r in col.aggregate(pipeline, allowDiskUse=True) if r["_id"]]

    print(f"Analyzing {len(candidates)} candidate wallets...\n")

    results = []
    for i, cand in enumerate(candidates):
        addr = cand["_id"]

        # Get trades
        buys = list(col.find({"buyer": addr}, {"coin": 1, "notional": 1, "timestamp": 1, "price": 1}).sort("timestamp", 1).limit(2000))
        sells = list(col.find({"seller": addr}, {"coin": 1, "notional": 1, "timestamp": 1, "price": 1}).sort("timestamp", 1).limit(2000))

        all_t = []
        for t in buys:
            all_t.append({"coin": t["coin"], "side": "BUY", "not": t["notional"], "ts": t["timestamp"], "px": t.get("price", 0)})
        for t in sells:
            all_t.append({"coin": t["coin"], "side": "SELL", "not": t["notional"], "ts": t["timestamp"], "px": t.get("price", 0)})
        all_t.sort(key=lambda x: x["ts"])

        if len(all_t) < 10:
            continue

        # Analyze behavior
        behavior = analyze_trade_behavior(all_t)

        # Compute copy PnL
        pnl = compute_copy_pnl(all_t)

        # Get live portfolio (rate-limited)
        portfolio = {}
        if args.check_portfolio and i < args.top:
            portfolio = get_portfolio(addr)
            time.sleep(0.3)  # rate limit protection

        results.append({
            "addr": addr,
            "total_notional": cand["total"],
            "trade_count": cand["count"],
            **behavior,
            **pnl,
            **{f"pf_{k}": v for k, v in portfolio.items()},
        })

        if (i + 1) % 10 == 0:
            print(f"  {i+1}/{len(candidates)} analyzed")

    # Filter and rank
    # MUST be: directional_trader or concentrated, have >=5 round trips, net positive
    good = [r for r in results
            if r.get("trips", 0) >= 5
            and r.get("net_avg", -999) > 0
            and r.get("behavior") in ("directional_trader", "mixed_oscillating")
            and r.get("pf_type", "unknown") in ("concentrated", "diversified", "inactive", "agent_key", "error", "unknown")]

    bad_type = [r for r in results
                if r.get("trips", 0) >= 5
                and r.get("net_avg", -999) > 0
                and r.get("behavior") not in ("directional_trader", "mixed_oscillating")]

    print(f"\n{'='*80}")
    print(f"RESULTS: {len(results)} wallets analyzed")
    print(f"  Profitable + directional + focused: {len(good)}")
    print(f"  Profitable but wrong type (basket/MM/rebalancer): {len(bad_type)}")
    print(f"{'='*80}")

    good.sort(key=lambda x: x.get("net_avg", 0) * x.get("trips", 0), reverse=True)

    print(f"\nTOP COPY TARGETS (directional traders, profitable after fees):")
    print(f"{'Addr':>22} {'Trips':>6} {'Net':>7} {'WR':>5} {'Hold':>6} {'Type':>12} {'PfType':>15} {'Pos':>4} {'Bias':>12} {'Equity':>10}")
    for r in good[:20]:
        pf_type = r.get("pf_type", "?")
        pf_pos = r.get("pf_positions", "?")
        pf_bias = r.get("pf_bias", "?")
        pf_eq = r.get("pf_equity", 0)
        print(f"{r['addr'][:20]}.. {r.get('trips',0):>6} {r.get('net_avg',0):>+6.1f} {r.get('wr_net',0):>4.0f}% {r.get('hold_min',0):>5.0f}m {r.get('behavior','?'):>12} {pf_type:>15} {pf_pos:>4} {pf_bias:>12} ${pf_eq:>9,.0f}")

    if bad_type:
        print(f"\nREJECTED (profitable but wrong trader type):")
        for r in bad_type[:10]:
            print(f"  {r['addr'][:20]}.. {r.get('trips',0)} trips, +{r.get('net_avg',0):.1f}bp net, behavior={r.get('behavior')} pf_type={r.get('pf_type','?')} positions={r.get('pf_positions','?')}")


if __name__ == "__main__":
    main()
