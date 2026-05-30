#!/Users/hermes/miniforge3/envs/quants-lab/bin/python
"""
Weekend paper trading report — go/no-go analysis for Monday live deployment.

Run: python scripts/arb_paper_report.py

Go-live gates:
  1. ≥100 trades completed
  2. Win rate ≥60%
  3. Profit factor ≥1.5
  4. No single pair >30% of total P&L (diversification)
  5. Average hold time <24h (capital turns)
  6. Max drawdown < $50 (at $200/trade sizing)
"""
import os, sys
from pathlib import Path

# Load .env
env_file = Path(__file__).resolve().parents[1] / ".env"
for line in env_file.read_text().splitlines():
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip())

from pymongo import MongoClient
from datetime import datetime, timezone
import numpy as np

db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab"))[
    os.getenv("MONGO_DATABASE", "quants_lab")
]


def report():
    positions = list(db["arb_positions"].find().sort("created_at", -1))
    if not positions:
        print("❌ No positions found. Is the paper engine running?")
        return

    closed = [p for p in positions if p["status"] == "CLOSED"]
    opened = [p for p in positions if p["status"] == "OPEN"]
    failed = [p for p in positions if p["status"] == "FAILED"]

    print(f"{'='*60}")
    print(f"  ARB PAPER TRADING REPORT")
    print(f"  Generated: {datetime.now():%Y-%m-%d %H:%M}")
    print(f"{'='*60}")

    print(f"\n  Positions: {len(closed)} closed, {len(opened)} open, {len(failed)} failed")

    if not closed:
        print("\n  ❌ No closed trades yet — need more time")
        return

    # Core metrics
    net_pnls = [p.get("net_pnl", 0) for p in closed]
    total_pnl = sum(net_pnls)
    avg_pnl = np.mean(net_pnls)
    wins = sum(1 for p in net_pnls if p > 0)
    wr = wins / len(closed)
    gross_wins = sum(p for p in net_pnls if p > 0)
    gross_losses = abs(sum(p for p in net_pnls if p < 0))
    pf = gross_wins / gross_losses if gross_losses > 0 else 99

    hold_times = [(p.get("exit_time", 0) - p.get("entry_time", 0)) / 3600 for p in closed if p.get("exit_time")]
    avg_hold = np.mean(hold_times) if hold_times else 0

    # Equity curve
    equity = np.cumsum(net_pnls)
    max_dd = (equity - np.maximum.accumulate(equity)).min()

    # Per-pair breakdown
    pair_pnl = {}
    for p in closed:
        sym = p["symbol"]
        pair_pnl[sym] = pair_pnl.get(sym, 0) + p.get("net_pnl", 0)

    if total_pnl > 0:
        max_pair_pct = max(pair_pnl.values()) / total_pnl * 100
        top_pair = max(pair_pnl, key=pair_pnl.get)
    else:
        max_pair_pct = 0
        top_pair = "N/A"

    print(f"\n{'─'*40}")
    print(f"  PERFORMANCE")
    print(f"{'─'*40}")
    print(f"  Total P&L:       ${total_pnl:.2f}")
    print(f"  Avg P&L/trade:   ${avg_pnl:.4f}")
    print(f"  Win rate:        {wr:.1%}")
    print(f"  Profit factor:   {pf:.2f}")
    print(f"  Avg hold time:   {avg_hold:.1f}h")
    print(f"  Max drawdown:    ${max_dd:.2f}")
    print(f"  Top pair:        {top_pair} ({max_pair_pct:.0f}% of P&L)")

    # Go/no-go gates
    print(f"\n{'─'*40}")
    print(f"  GO/NO-GO GATES")
    print(f"{'─'*40}")

    gates = [
        ("≥100 trades", len(closed) >= 100, f"{len(closed)} trades"),
        ("Win rate ≥60%", wr >= 0.60, f"{wr:.1%}"),
        ("Profit factor ≥1.5", pf >= 1.5, f"{pf:.2f}"),
        ("No pair >30% of P&L", max_pair_pct <= 30 or total_pnl <= 0, f"{top_pair} {max_pair_pct:.0f}%"),
        ("Avg hold <24h", avg_hold < 24, f"{avg_hold:.1f}h"),
        ("Max DD < $50", max_dd > -50, f"${max_dd:.2f}"),
    ]

    all_pass = True
    for name, passed, value in gates:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"  {status}  {name}: {value}")
        if not passed:
            all_pass = False

    print(f"\n{'─'*40}")
    if all_pass:
        print(f"  ✅ ALL GATES PASS — READY FOR LIVE DEPLOYMENT")
    else:
        print(f"  ❌ NOT READY — fix failing gates before going live")
    print(f"{'─'*40}")

    # Per-pair table
    print(f"\n  TOP PAIRS:")
    sorted_pairs = sorted(pair_pnl.items(), key=lambda x: x[1], reverse=True)
    for sym, pnl in sorted_pairs[:10]:
        pair_trades = [p for p in closed if p["symbol"] == sym]
        pair_wr = sum(1 for p in pair_trades if p.get("net_pnl", 0) > 0) / len(pair_trades)
        print(f"    {sym:<16} ${pnl:>7.2f}  n={len(pair_trades):>3}  WR={pair_wr:.0%}")

    if any(pnl < 0 for _, pnl in sorted_pairs):
        print(f"\n  LOSING PAIRS:")
        for sym, pnl in sorted_pairs:
            if pnl < 0:
                pair_trades = [p for p in closed if p["symbol"] == sym]
                print(f"    {sym:<16} ${pnl:>7.2f}  n={len(pair_trades):>3}")


if __name__ == "__main__":
    report()
