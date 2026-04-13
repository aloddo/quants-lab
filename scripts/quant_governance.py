"""
Quant Governance Gateway — final deployment decision for any engine.

Pulls data from all validation stages and produces a single DEPLOY/WATCH/REJECT
verdict per pair with composite score and reasoning.

Data sources:
  1. Bulk backtest (pair_historical) — full-period PF, Sharpe, trades, close types
  2. Walk-forward (walk_forward_results) — OOS performance, overfitting detection
  3. Trade-level (backtest_trades) — monthly consistency, loss streaks, side analysis

Usage:
  python scripts/quant_governance.py --engine E3
  python scripts/quant_governance.py --engine E3 --pair XRP-USDT
"""
import argparse
import os
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from pymongo import MongoClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ── Gate thresholds ──────────────────────────────────────────

# Gate 1: Bulk backtest minimums (hard block if any fails)
BULK_MIN_TRADES = 30
BULK_MIN_PF = 1.05          # at least marginally positive

# Gate 2: Walk-forward (hard block if fails)
WF_MIN_TEST_PF = 1.10       # OOS must show edge
WF_MAX_OVERFIT_RATIO = 2.0  # train/test > 2.0 = red flag
WF_MIN_FOLDS = 1            # at least 1 complete fold

# Gate 3: Composite score thresholds
DEPLOY_THRESHOLD = 0.55     # composite >= this → DEPLOY
WATCH_THRESHOLD = 0.30      # composite >= this → WATCH, else REJECT


# ── Composite score weights ──────────────────────────────────

WEIGHTS = {
    "oos_pf":         0.25,   # Out-of-sample PF (walk-forward test periods)
    "bulk_pf":        0.15,   # Full-period PF (365d stability)
    "floor_pf":       0.10,   # min(test, train, bulk) — worst-case performance
    "stability":      0.15,   # train/test consistency (penalize divergence either way)
    "sharpe":         0.10,   # Risk-adjusted returns (from bulk)
    "monthly_wr":     0.10,   # % of months profitable
    "loss_streak":    0.05,   # Inverse of max consecutive losses (resilience)
    "trade_volume":   0.05,   # Statistical significance
    "side_balance":   0.05,   # Both sides profitable, or dominant side strong
}


def fetch_bulk(db, engine: str) -> dict:
    """Fetch bulk backtest results keyed by pair."""
    docs = list(db["pair_historical"].find({"engine": engine}))
    return {d["pair"]: d for d in docs}


def fetch_walk_forward(db, engine: str) -> dict:
    """Fetch latest walk-forward run, grouped by pair."""
    # Find latest run_id
    latest = db["walk_forward_results"].find_one(
        {"engine": engine}, sort=[("created_at", -1)]
    )
    if not latest:
        return {}

    run_id = latest["run_id"]
    docs = list(db["walk_forward_results"].find({"engine": engine, "run_id": run_id}))

    pairs = {}
    for doc in docs:
        pair = doc["pair"]
        if pair not in pairs:
            pairs[pair] = {"train": [], "test": [], "run_id": run_id}
        pairs[pair][doc["period_type"]].append(doc)
    return pairs


def fetch_trade_stats(db, engine: str, pair: str) -> dict:
    """Compute trade-level statistics for a pair."""
    trades = list(db["backtest_trades"].find({"engine": engine, "pair": pair}))
    if not trades:
        return {"monthly_wr": 0, "max_loss_streak": 0, "short_pf": 0, "long_pf": 0}

    tdf = pd.DataFrame(trades)

    # Monthly win rate
    tdf["month"] = pd.to_datetime(tdf["timestamp"], unit="s").dt.to_period("M")
    monthly = tdf.groupby("month")["net_pnl_quote"].sum()
    win_months = (monthly > 0).sum()
    total_months = len(monthly)
    monthly_wr = win_months / total_months if total_months > 0 else 0

    # Max consecutive losses
    is_loss = (tdf["net_pnl_quote"] < 0).astype(int)
    streaks = is_loss.groupby((is_loss != is_loss.shift()).cumsum()).sum()
    max_loss_streak = int(streaks.max()) if len(streaks) > 0 else 0

    # Side-specific PF
    def side_pf(subset):
        if len(subset) == 0:
            return 0
        wins = subset[subset["net_pnl_quote"] > 0]["net_pnl_quote"].sum()
        losses = abs(subset[subset["net_pnl_quote"] <= 0]["net_pnl_quote"].sum())
        return wins / losses if losses > 0 else 0

    short = tdf[tdf["side"].str.contains("SELL", na=False)]
    long = tdf[tdf["side"].str.contains("BUY", na=False)]

    return {
        "monthly_wr": monthly_wr,
        "max_loss_streak": max_loss_streak,
        "short_pf": side_pf(short),
        "long_pf": side_pf(long),
        "n_short": len(short),
        "n_long": len(long),
        "total_trades": len(tdf),
    }


def compute_composite(bulk: dict, wf: dict, trade_stats: dict) -> tuple:
    """Compute composite score and component breakdown.

    Returns (score: float, components: dict, gate_failures: list).
    """
    gate_failures = []
    components = {}

    # ── Extract metrics ──

    bulk_pf = bulk.get("profit_factor", 0) or 0
    bulk_sharpe = bulk.get("sharpe", 0) or 0
    bulk_trades = bulk.get("trades", 0) or 0

    test_pfs = [d.get("profit_factor", 0) for d in wf.get("test", [])]
    train_pfs = [d.get("profit_factor", 0) for d in wf.get("train", [])]
    test_pf = np.mean(test_pfs) if test_pfs else 0
    train_pf = np.mean(train_pfs) if train_pfs else 0
    n_folds = len(test_pfs)
    total_test_trades = sum(d.get("trades", 0) for d in wf.get("test", []))

    monthly_wr = trade_stats.get("monthly_wr", 0)
    max_loss_streak = trade_stats.get("max_loss_streak", 0)
    short_pf = trade_stats.get("short_pf", 0)
    long_pf = trade_stats.get("long_pf", 0)

    # ── Gate 1: Bulk minimums ──

    if bulk_trades < BULK_MIN_TRADES:
        gate_failures.append(f"BULK: trades={bulk_trades} < {BULK_MIN_TRADES}")
    if bulk_pf < BULK_MIN_PF:
        gate_failures.append(f"BULK: PF={bulk_pf:.2f} < {BULK_MIN_PF}")

    # ── Gate 2: Walk-forward ──

    if n_folds < WF_MIN_FOLDS:
        gate_failures.append(f"WF: folds={n_folds} < {WF_MIN_FOLDS}")
    if test_pf < WF_MIN_TEST_PF:
        gate_failures.append(f"WF: test_PF={test_pf:.2f} < {WF_MIN_TEST_PF}")
    if test_pf > 0 and train_pf > 0:
        overfit_ratio = train_pf / test_pf
        if overfit_ratio > WF_MAX_OVERFIT_RATIO:
            gate_failures.append(
                f"WF: overfit ratio={overfit_ratio:.2f} > {WF_MAX_OVERFIT_RATIO}"
            )

    # ── Gate 3: Composite score ──

    floor_pf = min(test_pf, train_pf, bulk_pf) if all([test_pf, train_pf, bulk_pf]) else 0

    # Stability: 1.0 when train/test are equal, 0.0 when ratio > 2x
    if test_pf > 0 and train_pf > 0:
        ratio = train_pf / test_pf
        stability = max(1.0 - abs(ratio - 1.0), 0.0)
    else:
        stability = 0

    # Normalize each component to 0-1 range
    components["oos_pf"] = np.clip((test_pf - 1.0) / 0.5, 0, 1)
    components["bulk_pf"] = np.clip((bulk_pf - 1.0) / 0.5, 0, 1)
    components["floor_pf"] = np.clip((floor_pf - 1.0) / 0.5, 0, 1)
    components["stability"] = stability
    components["sharpe"] = np.clip(max(bulk_sharpe, 0) / 2.5, 0, 1)
    components["monthly_wr"] = monthly_wr
    components["loss_streak"] = np.clip(1.0 - (max_loss_streak - 3) / 15, 0, 1)
    components["trade_volume"] = np.clip(total_test_trades / 100, 0, 1)

    # Side balance: reward if dominant side is strong, penalize if one side is negative
    dominant_pf = max(short_pf, long_pf)
    weak_pf = min(short_pf, long_pf)
    if weak_pf >= 1.0:
        components["side_balance"] = np.clip((dominant_pf - 1.0) / 0.5, 0, 1)
    elif weak_pf >= 0.9:
        components["side_balance"] = np.clip((dominant_pf - 1.0) / 0.5, 0, 1) * 0.7
    else:
        components["side_balance"] = np.clip((dominant_pf - 1.0) / 0.5, 0, 1) * 0.4

    # Fold discount: reduce score if only 1 fold
    fold_multiplier = min(n_folds / 2.0, 1.0)

    # Weighted sum
    score = sum(WEIGHTS[k] * components[k] for k in WEIGHTS) * fold_multiplier

    return score, components, gate_failures


def evaluate_engine(db, engine: str, target_pair: str = None):
    """Run full governance evaluation for an engine."""

    bulk_data = fetch_bulk(db, engine)
    wf_data = fetch_walk_forward(db, engine)

    if not wf_data:
        print(f"No walk-forward results found for {engine}. Run walk-forward first.")
        return

    run_id = next(iter(wf_data.values()))["run_id"]
    pairs = sorted(wf_data.keys())
    if target_pair:
        pairs = [p for p in pairs if p == target_pair]

    results = []
    for pair in pairs:
        bulk = bulk_data.get(pair, {})
        wf = wf_data.get(pair, {})
        trade_stats = fetch_trade_stats(db, engine, pair)

        score, components, gate_failures = compute_composite(bulk, wf, trade_stats)

        # Determine verdict
        if gate_failures:
            verdict = "REJECT"
        elif score >= DEPLOY_THRESHOLD:
            verdict = "DEPLOY"
        elif score >= WATCH_THRESHOLD:
            verdict = "WATCH"
        else:
            verdict = "REJECT"

        results.append({
            "pair": pair,
            "verdict": verdict,
            "score": score,
            "components": components,
            "gate_failures": gate_failures,
            "test_pf": np.mean([d.get("profit_factor", 0) for d in wf.get("test", [])]),
            "train_pf": np.mean([d.get("profit_factor", 0) for d in wf.get("train", [])]),
            "bulk_pf": bulk.get("profit_factor", 0) or 0,
            "bulk_sharpe": bulk.get("sharpe", 0) or 0,
            "monthly_wr": trade_stats.get("monthly_wr", 0),
            "folds": len(wf.get("test", [])),
            "short_pf": trade_stats.get("short_pf", 0),
            "long_pf": trade_stats.get("long_pf", 0),
            "trades": trade_stats.get("total_trades", 0),
        })

    results.sort(key=lambda x: -x["score"])

    # ── Print report ──

    print()
    print(f"{'=' * 110}")
    print(f"  QUANT GOVERNANCE REPORT — {engine}")
    print(f"  Walk-forward run: {run_id}")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'=' * 110}")
    print()

    # Summary counts
    deploy = [r for r in results if r["verdict"] == "DEPLOY"]
    watch = [r for r in results if r["verdict"] == "WATCH"]
    reject = [r for r in results if r["verdict"] == "REJECT"]
    print(f"  DEPLOY: {len(deploy)}    WATCH: {len(watch)}    REJECT: {len(reject)}")
    print()

    # Leaderboard
    print(f"  {'Pair':<16} {'Verdict':>8} {'Score':>6} {'TestPF':>7} {'TrnPF':>7} {'BulkPF':>7} {'Sharpe':>7} {'MoWR':>5} {'ShrtPF':>7} {'LngPF':>6} {'Trades':>7}")
    print(f"  {'-' * 104}")

    for r in results:
        marker = {"DEPLOY": ">>", "WATCH": " >", "REJECT": "  "}[r["verdict"]]
        print(
            f"{marker}{r['pair']:<16} {r['verdict']:>8} {r['score']:>6.3f} "
            f"{r['test_pf']:>7.2f} {r['train_pf']:>7.2f} {r['bulk_pf']:>7.2f} "
            f"{r['bulk_sharpe']:>7.2f} {r['monthly_wr']*100:>4.0f}% "
            f"{r['short_pf']:>7.2f} {r['long_pf']:>6.2f} {r['trades']:>7}"
        )

    # Detailed cards for DEPLOY candidates
    if deploy:
        print()
        print(f"  {'─' * 60}")
        print(f"  DEPLOY CANDIDATES — Detailed Breakdown")
        print(f"  {'─' * 60}")
        for r in deploy:
            print(f"\n  {r['pair']}")
            print(f"    Composite Score: {r['score']:.3f} (threshold: {DEPLOY_THRESHOLD})")
            print(f"    Components:")
            for k, v in r["components"].items():
                bar = "#" * int(v * 20)
                print(f"      {k:<14} {v:.2f}  {bar}")
            print(f"    Short PF: {r['short_pf']:.2f} ({r.get('trades', 0)} trades)")
            print(f"    Long PF:  {r['long_pf']:.2f}")

    # Gate failure details for near-misses
    near_misses = [r for r in watch if r["score"] >= DEPLOY_THRESHOLD * 0.8]
    if near_misses:
        print()
        print(f"  {'─' * 60}")
        print(f"  NEAR-MISSES (WATCH, score >= {DEPLOY_THRESHOLD * 0.8:.2f})")
        print(f"  {'─' * 60}")
        for r in near_misses:
            failures = r["gate_failures"]
            print(f"\n  {r['pair']} (score: {r['score']:.3f})")
            if failures:
                for f in failures:
                    print(f"    GATE FAIL: {f}")
            else:
                print(f"    Below DEPLOY threshold ({r['score']:.3f} < {DEPLOY_THRESHOLD})")

    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Quant Governance Gateway")
    parser.add_argument("--engine", required=True, help="Engine name (e.g. E3)")
    parser.add_argument("--pair", default=None, help="Evaluate single pair")
    args = parser.parse_args()

    client = MongoClient(os.environ["MONGO_URI"])
    db = client[os.environ["MONGO_DATABASE"]]
    evaluate_engine(db, args.engine, target_pair=args.pair)
