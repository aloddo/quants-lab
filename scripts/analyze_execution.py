#!/usr/bin/env python3
"""
Execution analysis — fee model, slippage measurement, fill analysis.

Queries exchange_executions and exchange_closed_pnl from MongoDB.
Produces updated trade_cost recommendation for backtests.

Usage:
  set -a && source /Users/hermes/quants-lab/.env && set +a
  /Users/hermes/miniforge3/envs/quants-lab/bin/python scripts/analyze_execution.py
"""
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pymongo import MongoClient

db = MongoClient("mongodb://localhost:27017").quants_lab


def analyze_fees():
    """Fee model from exchange_executions (Trade type only)."""
    print("=" * 70)
    print("  FEE MODEL")
    print("=" * 70)

    trades = list(db.exchange_executions.find({"exec_type": "Trade"}))
    if not trades:
        print("  No trade executions found.")
        return {}

    maker = [t for t in trades if t.get("is_maker")]
    taker = [t for t in trades if not t.get("is_maker")]

    total = len(trades)
    maker_pct = len(maker) / total * 100
    taker_pct = len(taker) / total * 100

    print(f"\n  Fills: {total} total ({len(maker)} maker / {len(taker)} taker)")
    print(f"  Maker ratio: {maker_pct:.1f}% / Taker ratio: {taker_pct:.1f}%")

    # Fee rates
    maker_rates = [abs(float(t.get("fee_rate", 0))) for t in maker]
    taker_rates = [abs(float(t.get("fee_rate", 0))) for t in taker]
    all_rates = [abs(float(t.get("fee_rate", 0))) for t in trades]
    all_values = [float(t.get("exec_value", 0)) for t in trades]

    total_value = sum(all_values)
    weighted_rate = (
        sum(r * v for r, v in zip(all_rates, all_values)) / total_value
        if total_value
        else 0
    )

    print(f"\n  Fee rates:")
    if maker_rates:
        print(f"    Maker: {statistics.mean(maker_rates)*100:.4f}% avg (N={len(maker_rates)})")
    if taker_rates:
        print(f"    Taker: {statistics.mean(taker_rates)*100:.4f}% avg (N={len(taker_rates)})")
    print(f"    Value-weighted blend: {weighted_rate*100:.4f}%")
    print(f"    Total traded value: ${total_value:,.2f}")

    # Per-pair breakdown
    pair_data = defaultdict(lambda: {"maker": 0, "taker": 0, "total_fee": 0.0, "total_value": 0.0})
    for t in trades:
        pair = t.get("pair", "?")
        pd = pair_data[pair]
        if t.get("is_maker"):
            pd["maker"] += 1
        else:
            pd["taker"] += 1
        pd["total_fee"] += abs(float(t.get("exec_fee", 0)))
        pd["total_value"] += float(t.get("exec_value", 0))

    print(f"\n  Per-pair fee rates:")
    print(f"  {'Pair':>14s}  {'Fills':>5s}  {'Maker%':>6s}  {'AvgFee%':>7s}  {'Value':>10s}")
    print(f"  {'-'*14}  {'-'*5}  {'-'*6}  {'-'*7}  {'-'*10}")
    for pair in sorted(pair_data.keys()):
        pd = pair_data[pair]
        fills = pd["maker"] + pd["taker"]
        mk_pct = pd["maker"] / fills * 100 if fills else 0
        avg_fee = pd["total_fee"] / pd["total_value"] * 100 if pd["total_value"] else 0
        print(f"  {pair:>14s}  {fills:>5d}  {mk_pct:>5.0f}%  {avg_fee:>6.4f}%  ${pd['total_value']:>9,.2f}")

    # Recommendation
    current_model = 0.000375
    print(f"\n  RECOMMENDATION:")
    print(f"    Current backtest trade_cost: {current_model*100:.4f}% per side")
    print(f"    Actual value-weighted rate:  {weighted_rate*100:.4f}% per side")
    print(f"    Difference: {(weighted_rate - current_model) / current_model * 100:+.0f}%")
    if weighted_rate > current_model * 1.1:
        recommended = round(weighted_rate, 6)
        print(f"    >>> UPDATE trade_cost to {recommended} ({recommended*100:.4f}%)")
    else:
        print(f"    >>> Current trade_cost is acceptable (within 10% of actual)")

    return {
        "total_fills": total,
        "maker_ratio": maker_pct / 100,
        "taker_ratio": taker_pct / 100,
        "weighted_fee_rate": weighted_rate,
        "current_model": current_model,
    }


def analyze_pnl():
    """Closed PnL analysis from exchange_closed_pnl."""
    print(f"\n{'=' * 70}")
    print("  CLOSED PnL ANALYSIS")
    print("=" * 70)

    pnl_docs = list(db.exchange_closed_pnl.find())
    if not pnl_docs:
        print("  No closed PnL data found.")
        return {}

    total_pnl = sum(float(d.get("closed_pnl", 0)) for d in pnl_docs)
    total_entry = sum(float(d.get("cum_entry_value", 0)) for d in pnl_docs)
    total_fees = sum(
        float(d.get("open_fee", 0)) + float(d.get("close_fee", 0)) for d in pnl_docs
    )
    wins = [d for d in pnl_docs if float(d.get("closed_pnl", 0)) > 0]
    losses = [d for d in pnl_docs if float(d.get("closed_pnl", 0)) <= 0]

    print(f"\n  Round-trips: {len(pnl_docs)} total ({len(wins)} wins / {len(losses)} losses)")
    print(f"  Win rate: {len(wins)/len(pnl_docs)*100:.1f}%")
    print(f"  Total PnL: ${total_pnl:+.2f}")
    print(f"  Total fees: ${total_fees:.2f}")
    print(f"  PnL before fees: ${total_pnl + total_fees:+.2f}")
    print(f"  Total entry value: ${total_entry:,.2f}")
    print(f"  Return on capital: {total_pnl / total_entry * 100:+.2f}%" if total_entry else "")

    # Win/loss distribution
    if wins:
        win_pnls = [float(d["closed_pnl"]) for d in wins]
        print(f"\n  Winners: avg=${statistics.mean(win_pnls):+.2f}, "
              f"median=${statistics.median(win_pnls):+.2f}, "
              f"max=${max(win_pnls):+.2f}")
    if losses:
        loss_pnls = [float(d["closed_pnl"]) for d in losses]
        print(f"  Losers:  avg=${statistics.mean(loss_pnls):+.2f}, "
              f"median=${statistics.median(loss_pnls):+.2f}, "
              f"max=${min(loss_pnls):+.2f}")

    # Per-pair PnL
    pair_pnl = defaultdict(lambda: {"pnl": 0.0, "count": 0, "wins": 0})
    for d in pnl_docs:
        pair = d.get("pair", "?")
        pnl = float(d.get("closed_pnl", 0))
        pair_pnl[pair]["pnl"] += pnl
        pair_pnl[pair]["count"] += 1
        if pnl > 0:
            pair_pnl[pair]["wins"] += 1

    print(f"\n  Per-pair PnL:")
    print(f"  {'Pair':>14s}  {'Trades':>6s}  {'WR%':>5s}  {'PnL':>10s}")
    print(f"  {'-'*14}  {'-'*6}  {'-'*5}  {'-'*10}")
    for pair in sorted(pair_pnl.keys(), key=lambda p: pair_pnl[p]["pnl"], reverse=True):
        pd = pair_pnl[pair]
        wr = pd["wins"] / pd["count"] * 100 if pd["count"] else 0
        print(f"  {pair:>14s}  {pd['count']:>6d}  {wr:>4.0f}%  ${pd['pnl']:>+9.2f}")

    return {
        "total_roundtrips": len(pnl_docs),
        "win_rate": len(wins) / len(pnl_docs) if pnl_docs else 0,
        "total_pnl": total_pnl,
        "total_fees": total_fees,
    }


def analyze_slippage():
    """Entry/exit price analysis from closed PnL (approximate slippage)."""
    print(f"\n{'=' * 70}")
    print("  SLIPPAGE PROXY (Entry-Exit Spread)")
    print("=" * 70)

    pnl_docs = list(db.exchange_closed_pnl.find())
    if not pnl_docs:
        print("  No closed PnL data found.")
        return {}

    # For each round-trip, compute the spread between entry and exit
    spreads_bps = []
    for d in pnl_docs:
        entry = float(d.get("avg_entry_price", 0))
        exit_p = float(d.get("avg_exit_price", 0))
        if entry <= 0:
            continue
        spread_bps = abs(exit_p - entry) / entry * 10000
        side = d.get("side", "?")
        pnl = float(d.get("closed_pnl", 0))
        spreads_bps.append({
            "pair": d.get("pair", "?"),
            "side": side,
            "entry": entry,
            "exit": exit_p,
            "spread_bps": spread_bps,
            "pnl": pnl,
        })

    if not spreads_bps:
        print("  No valid entry/exit pairs.")
        return {}

    all_spreads = [s["spread_bps"] for s in spreads_bps]
    print(f"\n  Round-trips analyzed: {len(all_spreads)}")
    print(f"  Entry-exit spread (absolute):")
    print(f"    Mean:   {statistics.mean(all_spreads):>6.1f} bps")
    print(f"    Median: {statistics.median(all_spreads):>6.1f} bps")
    print(f"    P25:    {sorted(all_spreads)[len(all_spreads)//4]:>6.1f} bps")
    print(f"    P75:    {sorted(all_spreads)[3*len(all_spreads)//4]:>6.1f} bps")
    print(f"    Max:    {max(all_spreads):>6.1f} bps")

    # Bucket distribution
    safe = sum(1 for s in all_spreads if s < 100)
    borderline = sum(1 for s in all_spreads if 100 <= s < 300)
    wide = sum(1 for s in all_spreads if s >= 300)
    print(f"\n  Bucket distribution:")
    print(f"    <100 bps (tight):    {safe:>3d} ({safe/len(all_spreads)*100:.0f}%)")
    print(f"    100-300 bps (normal): {borderline:>3d} ({borderline/len(all_spreads)*100:.0f}%)")
    print(f"    >300 bps (wide):     {wide:>3d} ({wide/len(all_spreads)*100:.0f}%)")

    # Note on limitations
    print(f"\n  NOTE: This measures entry-exit price spread, NOT slippage.")
    print(f"  True slippage requires decision_price from HB executor history,")
    print(f"  which is not yet systematically collected.")
    print(f"  This metric shows position price movement, not fill quality.")

    return {
        "n_roundtrips": len(all_spreads),
        "mean_spread_bps": statistics.mean(all_spreads),
        "median_spread_bps": statistics.median(all_spreads),
    }


def analyze_funding_income():
    """Funding fee income/expense from Funding exec_type."""
    print(f"\n{'=' * 70}")
    print("  FUNDING INCOME")
    print("=" * 70)

    funding_docs = list(db.exchange_executions.find({"exec_type": "Funding"}))
    if not funding_docs:
        print("  No funding executions found.")
        return {}

    total_income = 0.0
    pair_funding = defaultdict(float)
    for d in funding_docs:
        fee = float(d.get("exec_fee", 0))
        # Negative fee = income (exchange pays you), positive = cost
        total_income += -fee  # flip sign: positive = income
        pair_funding[d.get("pair", "?")] += -fee

    print(f"\n  Funding events: {len(funding_docs)}")
    print(f"  Net funding income: ${total_income:+.2f}")
    print(f"  {'(positive = received, negative = paid)':>45s}")

    print(f"\n  Per-pair funding:")
    print(f"  {'Pair':>14s}  {'Events':>6s}  {'Net Income':>10s}")
    print(f"  {'-'*14}  {'-'*6}  {'-'*10}")
    for pair in sorted(pair_funding.keys(), key=lambda p: pair_funding[p], reverse=True):
        count = sum(1 for d in funding_docs if d.get("pair") == pair)
        print(f"  {pair:>14s}  {count:>6d}  ${pair_funding[pair]:>+9.2f}")

    return {"total_funding_income": total_income, "n_events": len(funding_docs)}


def summary(fee_model, pnl_model, slippage_model, funding_model):
    """Print consolidated summary and recommendations."""
    print(f"\n{'=' * 70}")
    print("  SUMMARY & RECOMMENDATIONS")
    print("=" * 70)

    print(f"\n  Data quality: {fee_model.get('total_fills', 0)} trade fills, "
          f"{pnl_model.get('total_roundtrips', 0)} round-trips, "
          f"{funding_model.get('n_events', 0)} funding events")
    print(f"  Statistical confidence: LOW (need N>100 fills for robustness)")

    actual_fee = fee_model.get("weighted_fee_rate", 0)
    current_fee = fee_model.get("current_model", 0.000375)
    fee_gap = (actual_fee - current_fee) / current_fee * 100 if current_fee else 0

    print(f"\n  1. FEE MODEL")
    print(f"     Current trade_cost:  {current_fee*100:.4f}%")
    print(f"     Actual blended rate: {actual_fee*100:.4f}% ({fee_gap:+.0f}%)")
    print(f"     Maker ratio: {fee_model.get('maker_ratio', 0)*100:.0f}%")
    if fee_gap > 10:
        print(f"     >>> ACTION: Update trade_cost to {round(actual_fee, 6)}")
    else:
        print(f"     >>> No change needed")

    print(f"\n  2. PnL IMPACT")
    print(f"     Paper trading PnL: ${pnl_model.get('total_pnl', 0):+.2f}")
    print(f"     Funding income: ${funding_model.get('total_funding_income', 0):+.2f}")
    print(f"     Win rate: {pnl_model.get('win_rate', 0)*100:.0f}%")

    print(f"\n  3. NEXT STEPS")
    print(f"     - Collect more fills (target: N>100 Trade fills)")
    print(f"     - Add decision_price tracking to executor history for true slippage")
    print(f"     - Re-run after E3 V2 paper trading has 1 week of data")
    print(f"     - Slippage tier backtesting BEFORE real capital deployment")


def main():
    print(f"\n  Execution Analysis — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Data source: MongoDB quants_lab")
    print(f"  {'=' * 50}\n")

    fee_model = analyze_fees()
    pnl_model = analyze_pnl()
    slippage_model = analyze_slippage()
    funding_model = analyze_funding_income()
    summary(fee_model, pnl_model, slippage_model, funding_model)


if __name__ == "__main__":
    main()
