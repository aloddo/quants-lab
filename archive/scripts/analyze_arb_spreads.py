#!/usr/bin/env python3
"""
Arb Spread Viability Report — reads 1m distributional stats, ranks pairs by tradability.

For each pair, answers:
- Is the spread consistently above fee thresholds?
- How often can we execute (time above threshold)?
- Which direction is more profitable (HL premium vs BB premium)?
- What's the typical spread regime (p25/p50/p75/p90)?

Usage:
    MONGO_URI=mongodb://localhost:27017/quants_lab \
    python scripts/analyze_arb_spreads.py [--collection arb_hl_bybit_spread_stats_1m] [--days 7]
"""

import argparse
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
from pymongo import MongoClient

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")


def main():
    parser = argparse.ArgumentParser(description="Arb spread viability analysis")
    parser.add_argument("--collection", default="arb_hl_bybit_spread_stats_1m")
    parser.add_argument("--days", type=int, default=7, help="Lookback in days")
    parser.add_argument("--min-minutes", type=int, default=100, help="Min data points per pair")
    args = parser.parse_args()

    db_name = MONGO_URI.rsplit("/", 1)[-1] or "quants_lab"
    client = MongoClient(MONGO_URI)
    db = client[db_name]
    coll = db[args.collection]

    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    docs = list(coll.find({"timestamp": {"$gte": cutoff}}))

    if not docs:
        print(f"No data in {args.collection} after {cutoff}")
        return

    df = pd.DataFrame(docs)
    print(f"\n{'='*100}")
    print(f"  ARB SPREAD VIABILITY REPORT — {args.collection}")
    print(f"  Data: {len(df):,} minute-bars, {df['pair'].nunique()} pairs, last {args.days}d")
    print(f"{'='*100}\n")

    # Per-pair analysis
    results = []
    for pair, gdf in df.groupby("pair"):
        if len(gdf) < args.min_minutes:
            continue

        results.append({
            "pair": pair,
            "minutes": len(gdf),
            # Best spread (either direction)
            "best_p90_avg": gdf["best_p90"].mean(),
            "best_max_avg": gdf["best_max"].mean(),
            "best_mean_avg": gdf["best_mean"].mean(),
            # Exceedance rates
            "pct_above_5bps": gdf["time_above_5bps"].mean() * 100,
            "pct_above_10bps": gdf["time_above_10bps"].mean() * 100,
            "pct_above_15bps": gdf["time_above_15bps"].mean() * 100,
            # Direction A (e.g., HL premium)
            "a_p90_avg": gdf["a_p90"].mean(),
            "a_mean_avg": gdf["a_mean"].mean(),
            # Direction B (e.g., BB premium)
            "b_p90_avg": gdf["b_p90"].mean(),
            "b_mean_avg": gdf["b_mean"].mean(),
            # Dominant direction
            "dominant_dir": "A" if gdf["a_p90"].mean() > gdf["b_p90"].mean() else "B",
            # Ticks per minute (data density)
            "avg_ticks": gdf["count"].mean(),
        })

    if not results:
        print("Not enough data for analysis. Need more collection time.")
        return

    rdf = pd.DataFrame(results).sort_values("best_p90_avg", ascending=False)

    # Tier classification
    # Tier 1: P90 > 10bps consistently (tradable at taker-taker fees)
    # Tier 2: P90 > 5bps consistently (tradable with maker on one side)
    # Tier 3: P90 > 2bps (marginal, needs maker-maker or carry overlay)
    # Tier 4: P90 < 2bps (not tradable)
    def classify(row):
        if row["best_p90_avg"] >= 10 and row["pct_above_10bps"] >= 30:
            return "TIER 1 (taker-taker)"
        elif row["best_p90_avg"] >= 5 and row["pct_above_5bps"] >= 30:
            return "TIER 2 (maker-taker)"
        elif row["best_p90_avg"] >= 2:
            return "TIER 3 (marginal)"
        else:
            return "TIER 4 (no arb)"

    rdf["tier"] = rdf.apply(classify, axis=1)

    print(f"{'Pair':<16} {'Tier':<22} {'P90':>6} {'Max':>6} {'>5bp':>6} {'>10bp':>6} {'>15bp':>6} {'Dir':>4} {'Mins':>6}")
    print("-" * 90)
    for _, r in rdf.iterrows():
        print(
            f"{r['pair']:<16} {r['tier']:<22} "
            f"{r['best_p90_avg']:>5.1f} {r['best_max_avg']:>5.1f} "
            f"{r['pct_above_5bps']:>5.0f}% {r['pct_above_10bps']:>5.0f}% {r['pct_above_15bps']:>5.0f}% "
            f"{'HL' if r['dominant_dir']=='A' else 'BB':>4} {r['minutes']:>5}"
        )

    # Summary
    t1 = rdf[rdf["tier"].str.contains("TIER 1")]
    t2 = rdf[rdf["tier"].str.contains("TIER 2")]
    t3 = rdf[rdf["tier"].str.contains("TIER 3")]
    print(f"\nSummary: {len(t1)} Tier 1, {len(t2)} Tier 2, {len(t3)} Tier 3, {len(rdf) - len(t1) - len(t2) - len(t3)} Tier 4")
    if len(t1) > 0:
        print(f"\nTier 1 pairs (tradable taker-taker): {', '.join(t1['pair'].tolist())}")
    if len(t2) > 0:
        print(f"Tier 2 pairs (tradable maker-taker): {', '.join(t2['pair'].tolist())}")

    client.close()


if __name__ == "__main__":
    main()
