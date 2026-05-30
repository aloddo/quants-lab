#!/usr/bin/env python3
"""
V11 Copy Trader Performance Report

Generates daily performance metrics for the 2-week validation gate.
Run daily or on-demand to track progress toward capital deployment.

Usage:
    python scripts/v11_performance_report.py
    python scripts/v11_performance_report.py --json  # machine-readable output
"""
import argparse
import json
import sys
from datetime import datetime, timedelta

import numpy as np
from pymongo import MongoClient
from scipy import stats

MONGO_URI = "mongodb://localhost:27017/quants_lab"
COLLECTION = "unified_copy_trades"
GATE_DATE = datetime(2026, 5, 28)  # 2-week gate from V11 launch (May 14)
GATE_DAYS = 14


def load_trades():
    client = MongoClient(MONGO_URI)
    db = client["quants_lab"]
    return list(db[COLLECTION].find({}).sort("timestamp", 1))


def daily_breakdown(trades):
    daily = {}
    for t in trades:
        day = t["timestamp"].strftime("%Y-%m-%d")
        if day not in daily:
            daily[day] = {
                "trades": 0, "pnl": 0.0, "wins": 0,
                "gross_win": 0.0, "gross_loss": 0.0,
                "coins": set(), "wallets": set(),
            }
        pnl = t.get("pnl_usd", 0)
        daily[day]["trades"] += 1
        daily[day]["pnl"] += pnl
        daily[day]["coins"].add(t.get("coin", ""))
        daily[day]["wallets"].add(t.get("target_wallet", "")[:10])
        if pnl > 0:
            daily[day]["wins"] += 1
            daily[day]["gross_win"] += pnl
        else:
            daily[day]["gross_loss"] += abs(pnl)
    return daily


def compute_stats(trades):
    pnls = np.array([t.get("pnl_usd", 0) for t in trades])
    if len(pnls) == 0:
        return {}

    wins = pnls[pnls > 0]
    losses = pnls[pnls <= 0]

    # t-test
    t_stat, p_val = stats.ttest_1samp(pnls, 0) if len(pnls) > 1 else (0, 1)

    # Drawdown (trade-level)
    cum = np.cumsum(pnls)
    running_max = np.maximum.accumulate(cum)
    drawdowns = cum - running_max
    max_dd = drawdowns.min()

    # Effect size
    effect_size = pnls.mean() / pnls.std() if pnls.std() > 0 else 0

    # Trades needed for 80% power at 5% alpha
    n_needed = ((1.645 + 0.842) ** 2) / (effect_size ** 2) if effect_size > 0 else float("inf")

    return {
        "total_trades": len(pnls),
        "win_rate": len(wins) / len(pnls),
        "mean_pnl": float(pnls.mean()),
        "median_pnl": float(np.median(pnls)),
        "std_pnl": float(pnls.std()),
        "total_pnl": float(pnls.sum()),
        "avg_win": float(wins.mean()) if len(wins) > 0 else 0,
        "avg_loss": float(losses.mean()) if len(losses) > 0 else 0,
        "win_loss_ratio": float(abs(wins.mean() / losses.mean())) if len(wins) > 0 and len(losses) > 0 and losses.mean() != 0 else 0,
        "profit_factor": float(wins.sum() / abs(losses.sum())) if losses.sum() != 0 else float("inf"),
        "max_drawdown": float(max_dd),
        "t_stat": float(t_stat),
        "p_value_1sided": float(p_val / 2),
        "significant_5pct": bool(p_val / 2 < 0.05),
        "effect_size": float(effect_size),
        "trades_for_80pct_power": int(min(n_needed, 99999)),
    }


def print_report(trades, daily, overall_stats):
    print("=" * 70)
    print("V11 COPY TRADER -- 2-WEEK VALIDATION REPORT")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    # Daily table
    print(f"\n{'Date':12s} {'Trades':>7s} {'Wins':>5s} {'WR':>6s} {'PnL':>10s} {'Cum':>10s} {'PF':>6s} {'Coins':>6s}")
    print("-" * 68)
    cum_pnl = 0
    daily_pnls = []
    for day in sorted(daily.keys()):
        d = daily[day]
        cum_pnl += d["pnl"]
        wr = d["wins"] / d["trades"] * 100 if d["trades"] > 0 else 0
        pf = d["gross_win"] / d["gross_loss"] if d["gross_loss"] > 0 else float("inf")
        pf_str = f"{pf:.1f}" if pf < 100 else "INF"
        daily_pnls.append(d["pnl"])
        print(f"{day:12s} {d['trades']:7d} {d['wins']:5d} {wr:5.0f}% ${d['pnl']:+8.2f} ${cum_pnl:+8.2f} {pf_str:>6s} {len(d['coins']):6d}")

    # Cumulative stats
    s = overall_stats
    print(f"\n--- CUMULATIVE ---")
    print(f"Trades: {s['total_trades']} | WR: {s['win_rate']*100:.1f}% | PF: {s['profit_factor']:.2f}")
    print(f"Total PnL: ${s['total_pnl']:.2f} | Mean: ${s['mean_pnl']:.4f} | Max DD: ${s['max_drawdown']:.2f}")
    print(f"Avg win: ${s['avg_win']:.4f} | Avg loss: ${s['avg_loss']:.4f} | W/L ratio: {s['win_loss_ratio']:.2f}")
    print(f"t-stat: {s['t_stat']:.3f} | p-value: {s['p_value_1sided']:.4f} | Significant: {'YES' if s['significant_5pct'] else 'NO'}")

    # Hold time and exit type stats
    holds = [t.get("hold_s", 0) for t in trades if t.get("hold_s", 0) > 0]
    if holds:
        h = np.array(holds)
        print(f"Hold time: median {np.median(h)/60:.0f}min, mean {h.mean()/60:.0f}min, p10 {np.percentile(h,10)/60:.0f}min, p90 {np.percentile(h,90)/60:.0f}min")
    from collections import Counter
    exit_types = Counter(t.get("exit_type", "?") for t in trades)
    print(f"Exit types: {dict(exit_types)}")

    # Gate metrics
    profitable_days = sum(1 for p in daily_pnls if p > 0)
    max_losing = 0
    current = 0
    for p in daily_pnls:
        if p <= 0:
            current += 1
            max_losing = max(max_losing, current)
        else:
            current = 0

    days_elapsed = len(daily_pnls)
    days_remaining = max(0, (GATE_DATE - datetime.now()).days)

    print(f"\n--- 2-WEEK GATE (May 14 - May 28) ---")
    print(f"Days elapsed: {days_elapsed}/{GATE_DAYS}")
    print(f"Days remaining: {days_remaining}")
    print(f"Profitable days: {profitable_days}/{days_elapsed} ({profitable_days/max(days_elapsed,1)*100:.0f}%)")
    print(f"Max losing streak: {max_losing} days")
    print(f"Worst day: ${min(daily_pnls):.2f}" if daily_pnls else "N/A")
    print(f"Best day: ${max(daily_pnls):.2f}" if daily_pnls else "N/A")

    # Projection
    if days_elapsed > 0:
        daily_avg = sum(daily_pnls) / days_elapsed
        projected_total = daily_avg * GATE_DAYS
        print(f"\nProjected 14-day PnL (at current rate): ${projected_total:.2f}")
        print(f"Projected monthly (30 days): ${daily_avg * 30:.2f}")

    print(f"\n{'=' * 70}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    trades = load_trades()
    if not trades:
        print("No V11 trades found in unified_copy_trades")
        sys.exit(1)

    daily = daily_breakdown(trades)
    overall = compute_stats(trades)

    if args.json:
        # Serialize for automation
        output = {
            "generated": datetime.now().isoformat(),
            "stats": overall,
            "daily": {
                day: {k: v for k, v in d.items() if k not in ("coins", "wallets")}
                for day, d in daily.items()
            },
            "gate": {
                "days_elapsed": len(daily),
                "days_remaining": max(0, (GATE_DATE - datetime.now()).days),
                "profitable_days": sum(1 for d in daily.values() if d["pnl"] > 0),
                "on_track": all(d["pnl"] > -5 for d in daily.values()),  # no catastrophic day
            },
        }
        print(json.dumps(output, indent=2, default=str))
    else:
        print_report(trades, daily, overall)


if __name__ == "__main__":
    main()
