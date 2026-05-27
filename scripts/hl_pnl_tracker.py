#!/usr/bin/env python3
"""
HL PnL Tracker -- Exchange-sourced truth for copy trading performance.

Pulls fills, funding, and ledger from Hyperliquid API. Computes daily PnL,
cumulative equity curve, per-coin attribution, and key metrics (Sharpe, win rate,
max drawdown). This is the source of truth, not the copy trader's internal log.

Usage:
    python scripts/hl_pnl_tracker.py                  # Last 14 days
    python scripts/hl_pnl_tracker.py --days 30         # Last 30 days
    python scripts/hl_pnl_tracker.py --since 2026-05-13 # From specific date (V11 era)
    python scripts/hl_pnl_tracker.py --csv report.csv  # Export CSV
"""
import argparse
import json
import logging
import math
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pnl_tracker] %(levelname)s: %(message)s",
)
logger = logging.getLogger("pnl_tracker")

# Parent wallet (funds)
PARENT_ADDR = "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
FEE_RT_BPS = 8.64


def fetch_all_fills(info, addr: str, start_ts: int, end_ts: int) -> list:
    """Fetch all fills, handling the 2000-fill API cap by chunking backwards."""
    all_fills = []
    chunk_end = end_ts
    while True:
        fills = info.user_fills_by_time(addr, start_ts, chunk_end)
        if not fills:
            break
        all_fills.extend(fills)
        if len(fills) < 2000:
            break
        chunk_end = fills[0]["time"] - 1
        logger.info(
            f"  Chunk: {len(fills)} fills, back to "
            f"{datetime.fromtimestamp(fills[0]['time']/1000)}"
        )

    # Deduplicate by tid
    seen = set()
    unique = []
    for f in all_fills:
        if f["tid"] not in seen:
            seen.add(f["tid"])
            unique.append(f)
    unique.sort(key=lambda x: x["time"])
    return unique


def fetch_funding(info, addr: str, start_ts: int, end_ts: int) -> list:
    """Fetch funding payments."""
    return info.user_funding_history(addr, start_ts, end_ts)


def fetch_ledger(info, addr: str, start_ts: int, end_ts: int) -> list:
    """Fetch non-funding ledger (deposits, withdrawals, transfers)."""
    return info.user_non_funding_ledger_updates(addr, start_ts, end_ts)


def compute_daily_pnl(fills: list, funding: list) -> dict:
    """Compute daily PnL breakdown from fills and funding."""
    daily = defaultdict(lambda: {
        "fills": 0,
        "closed_pnl": 0.0,
        "fees": 0.0,
        "funding": 0.0,
        "coins": set(),
        "maker_fills": 0,
        "taker_fills": 0,
    })

    for f in fills:
        day = datetime.fromtimestamp(f["time"] / 1000).strftime("%Y-%m-%d")
        daily[day]["fills"] += 1
        daily[day]["closed_pnl"] += float(f["closedPnl"])
        daily[day]["fees"] += float(f["fee"])
        daily[day]["coins"].add(f["coin"])
        if f.get("crossed"):
            daily[day]["taker_fills"] += 1
        else:
            daily[day]["maker_fills"] += 1

    for f in funding:
        day = datetime.fromtimestamp(f["time"] / 1000).strftime("%Y-%m-%d")
        daily[day]["funding"] += float(f["delta"]["usdc"])

    return dict(daily)


def compute_coin_attribution(fills: list) -> dict:
    """Per-coin PnL attribution."""
    coins = defaultdict(lambda: {
        "fills": 0, "closed_pnl": 0.0, "fees": 0.0, "notional": 0.0
    })
    for f in fills:
        coin = f["coin"]
        coins[coin]["fills"] += 1
        coins[coin]["closed_pnl"] += float(f["closedPnl"])
        coins[coin]["fees"] += float(f["fee"])
        coins[coin]["notional"] += float(f["px"]) * float(f["sz"])
    return dict(coins)


def compute_metrics(daily: dict) -> dict:
    """Compute key performance metrics."""
    days = sorted(daily.keys())
    if not days:
        return {}

    daily_nets = []
    for day in days:
        d = daily[day]
        net = d["closed_pnl"] - d["fees"] + d["funding"]
        daily_nets.append(net)

    daily_nets = np.array(daily_nets)
    cumulative = np.cumsum(daily_nets)

    # Max drawdown on cumulative PnL
    peak = np.maximum.accumulate(cumulative)
    drawdown = cumulative - peak
    max_dd = float(drawdown.min()) if len(drawdown) > 0 else 0

    # Win rate (profitable days)
    win_days = int((daily_nets > 0).sum())
    total_days = len(daily_nets)

    # Sharpe (annualized, daily returns)
    if daily_nets.std() > 0:
        sharpe = float(daily_nets.mean() / daily_nets.std() * math.sqrt(365))
    else:
        sharpe = 0.0

    total_fills = sum(d["fills"] for d in daily.values())
    total_closed = sum(d["closed_pnl"] for d in daily.values())
    total_fees = sum(d["fees"] for d in daily.values())
    total_funding = sum(d["funding"] for d in daily.values())
    total_net = total_closed - total_fees + total_funding

    return {
        "total_days": total_days,
        "total_fills": total_fills,
        "total_closed_pnl": total_closed,
        "total_fees": total_fees,
        "total_funding": total_funding,
        "total_net": total_net,
        "daily_avg": float(daily_nets.mean()),
        "daily_std": float(daily_nets.std()),
        "sharpe_annual": sharpe,
        "win_rate": win_days / total_days if total_days > 0 else 0,
        "win_days": win_days,
        "max_drawdown": max_dd,
        "best_day": float(daily_nets.max()),
        "worst_day": float(daily_nets.min()),
    }


def print_report(daily: dict, coins: dict, metrics: dict, current_equity: float, unrealized: float):
    """Print formatted PnL report."""
    print("\n" + "=" * 80)
    print("  HYPERLIQUID PnL REPORT (Exchange Source of Truth)")
    print("=" * 80)

    # Daily table
    print(f"\n{'Day':<12} {'Fills':>5} {'ClosedPnL':>11} {'Fees':>9} {'Funding':>9} {'Net':>11} {'CumNet':>11}  Coins")
    print("-" * 95)
    cum = 0
    for day in sorted(daily.keys()):
        d = daily[day]
        net = d["closed_pnl"] - d["fees"] + d["funding"]
        cum += net
        coin_str = ",".join(sorted(d["coins"]))[:30]
        print(
            f"{day:<12} {d['fills']:>5} "
            f"${d['closed_pnl']:>+10.4f} "
            f"${d['fees']:>8.4f} "
            f"${d['funding']:>+8.4f} "
            f"${net:>+10.4f} "
            f"${cum:>+10.4f}  "
            f"{coin_str}"
        )

    # Metrics
    print(f"\n{'=' * 80}")
    print("  METRICS")
    print(f"{'=' * 80}")
    print(f"  Trading days:      {metrics['total_days']}")
    print(f"  Total fills:       {metrics['total_fills']}")
    print(f"  Closed PnL:       ${metrics['total_closed_pnl']:+.4f}")
    print(f"  Total fees:       ${metrics['total_fees']:.4f}")
    print(f"  Total funding:    ${metrics['total_funding']:+.4f}")
    print(f"  Net PnL:          ${metrics['total_net']:+.4f}")
    print(f"  + Unrealized:     ${unrealized:+.2f}")
    print(f"  = Total PnL:      ${metrics['total_net'] + unrealized:+.4f}")
    print(f"  Current equity:   ${current_equity:.2f}")
    print()
    print(f"  Daily avg:        ${metrics['daily_avg']:+.4f}")
    print(f"  Daily std:        ${metrics['daily_std']:.4f}")
    print(f"  Sharpe (annual):  {metrics['sharpe_annual']:.2f}")
    print(f"  Win rate:         {metrics['win_rate']:.1%} ({metrics['win_days']}/{metrics['total_days']} days)")
    print(f"  Max drawdown:     ${metrics['max_drawdown']:.2f}")
    print(f"  Best day:         ${metrics['best_day']:+.4f}")
    print(f"  Worst day:        ${metrics['worst_day']:+.4f}")

    # Top coins
    print(f"\n{'=' * 80}")
    print("  PER-COIN ATTRIBUTION (top 15 by absolute net PnL)")
    print(f"{'=' * 80}")
    sorted_coins = sorted(coins.items(), key=lambda x: abs(x[1]["closed_pnl"] - x[1]["fees"]), reverse=True)
    print(f"  {'Coin':<15} {'Fills':>5} {'ClosedPnL':>11} {'Fees':>9} {'Net':>11} {'Notional':>12}")
    print("  " + "-" * 65)
    for coin, data in sorted_coins[:15]:
        net = data["closed_pnl"] - data["fees"]
        print(
            f"  {coin:<15} {data['fills']:>5} "
            f"${data['closed_pnl']:>+10.4f} "
            f"${data['fees']:>8.4f} "
            f"${net:>+10.4f} "
            f"${data['notional']:>11.2f}"
        )

    print()


def main():
    parser = argparse.ArgumentParser(description="HL PnL Tracker")
    parser.add_argument("--days", type=int, default=14, help="Lookback days")
    parser.add_argument("--since", type=str, help="Start date YYYY-MM-DD (overrides --days)")
    parser.add_argument("--csv", type=str, help="Export daily PnL to CSV")
    parser.add_argument("--addr", type=str, default=PARENT_ADDR, help="Wallet address")
    args = parser.parse_args()

    from hyperliquid.info import Info
    info = Info(skip_ws=True)

    end_ts = int(time.time() * 1000)
    if args.since:
        since_dt = datetime.strptime(args.since, "%Y-%m-%d")
        start_ts = int(since_dt.timestamp() * 1000)
        label = f"since {args.since}"
    else:
        start_ts = end_ts - args.days * 24 * 3600 * 1000
        label = f"{args.days} days"

    logger.info(f"Fetching data for {args.addr[:12]}... ({label})")

    # Fetch all data
    fills = fetch_all_fills(info, args.addr, start_ts, end_ts)
    funding = fetch_funding(info, args.addr, start_ts, end_ts)
    logger.info(f"Fills: {len(fills)}, Funding: {len(funding)}")

    # Current state
    state = info.user_state(args.addr)
    spot = info.spot_user_state(args.addr)
    unrealized = sum(
        float(p["position"]["unrealizedPnl"])
        for p in state.get("assetPositions", [])
        if float(p["position"]["szi"]) != 0
    )
    current_equity = float(spot["balances"][0]["total"])

    # Compute
    daily = compute_daily_pnl(fills, funding)
    coins = compute_coin_attribution(fills)
    metrics = compute_metrics(daily)

    # Print
    print_report(daily, coins, metrics, current_equity, unrealized)

    # CSV export
    if args.csv:
        import csv
        with open(args.csv, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["day", "fills", "closed_pnl", "fees", "funding", "net", "cum_net"])
            cum = 0
            for day in sorted(daily.keys()):
                d = daily[day]
                net = d["closed_pnl"] - d["fees"] + d["funding"]
                cum += net
                writer.writerow([day, d["fills"], f"{d['closed_pnl']:.4f}", f"{d['fees']:.4f}",
                                f"{d['funding']:.4f}", f"{net:.4f}", f"{cum:.4f}"])
        logger.info(f"Exported to {args.csv}")


if __name__ == "__main__":
    main()
