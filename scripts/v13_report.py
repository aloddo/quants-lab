#!/usr/bin/env python3
"""V13 Script 5/5: Strategy report.

Per projects/quant/v13 Section 6.8. Output is a markdown report structured
around the six questions in fixed order, plus a pass/fail table per Section
6.3.

Inputs:
    --walk-forward-results <path>    walk_forward_results.parquet from script 4
    --wallet-metrics <path>          wallet_metrics.parquet (latest fold) for trait analysis
    --output <path>                  Default: app/data/v13/strategy_report.md

Outputs:
    strategy_report.md
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "strategy_report.md"

# Pass/fail thresholds per v13 Section 6.3.
THR = {
    "sharpe_min": 1.5,
    "max_dd_max": 25.0,
    "worst_day_max": 10.0,
    "random_pct_min": 95.0,
    "fee_drag_max_pct": 30.0,
    "robust_remove_min_sharpe": 0.0,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--walk-forward-results", required=True)
    ap.add_argument("--wallet-metrics", required=False)
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    wf = pd.read_parquet(args.walk_forward_results)
    if wf.empty:
        print("walk_forward_results is empty; cannot generate report")
        return

    successful = wf[wf["status"] == "ok"]
    if successful.empty:
        print("No successful folds; cannot evaluate")
        return

    mean_test_sharpe = float(successful["test_sharpe"].mean())
    median_test_sharpe = float(successful["test_sharpe"].median())
    mean_dd = float(successful["test_max_dd_pct"].mean())
    mean_worst_day = float(successful["test_worst_day_pct"].mean())
    mean_pct_rank = float(successful["random_pct_rank"].mean())
    median_pct_rank = float(successful["random_pct_rank"].median())
    latest_fold_sharpe = float(successful.iloc[-1]["test_sharpe"])

    # Pass / fail per row.
    pf = {
        "OOS Sharpe (mean across folds)": (mean_test_sharpe, ">= 1.5", mean_test_sharpe >= THR["sharpe_min"]),
        "OOS max drawdown (mean)": (mean_dd, "<= 25", mean_dd <= THR["max_dd_max"]),
        "OOS worst single-day loss (mean abs)": (abs(mean_worst_day), "<= 10", abs(mean_worst_day) <= THR["worst_day_max"]),
        "Random-portfolio percentile rank (mean)": (mean_pct_rank, ">= 95", mean_pct_rank >= THR["random_pct_min"]),
        "Most recent fold profitable (sharpe > 0)": (latest_fold_sharpe, "> 0", latest_fold_sharpe > 0),
    }

    # Robustness: top-1 / top-5 / top-10 removal.
    rmv = {}
    for k in [1, 5, 10]:
        col = f"remove_top{k}_sharpe"
        if col in successful.columns:
            vals = successful[col].dropna()
            if not vals.empty:
                rmv[k] = float(vals.mean())
    robust_ok = all(v > THR["robust_remove_min_sharpe"] for v in rmv.values()) if rmv else False
    pf["Top-1/5/10 removal robustness (all sharpe > 0)"] = (rmv, "all > 0", robust_ok)

    all_pass = all(v[2] for v in pf.values())

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("w") as f:
        f.write(f"# V13 Backtest Report\n\n")
        f.write(f"Generated from walk_forward_results.parquet (`{args.walk_forward_results}`).\n\n")
        f.write(f"Folds attempted: {len(wf)}\n")
        f.write(f"Folds successful: {len(successful)}\n\n")

        f.write("## Verdict\n\n")
        f.write(f"**Overall: {'PASS' if all_pass else 'FAIL'}**\n\n")
        f.write("Pass / Fail table (Section 6.3):\n\n")
        f.write("| Criterion | Value | Threshold | Result |\n")
        f.write("|-----------|-------|-----------|--------|\n")
        for label, (val, thr, ok) in pf.items():
            f.write(f"| {label} | {val} | {thr} | {'PASS' if ok else 'FAIL'} |\n")
        f.write("\n")

        # The six final questions.
        f.write("## The Six Questions (Section 6.8)\n\n")

        f.write("### 1. Do top wallets outperform random wallets out of sample?\n\n")
        f.write(f"Mean random-portfolio percentile rank across folds: **{mean_pct_rank:.1f}** "
                f"(median {median_pct_rank:.1f}). Threshold to claim selection edge: >= 95.\n\n")
        f.write(f"Per-fold ranks: {successful['random_pct_rank'].round(1).tolist()}\n\n")
        f.write(f"**Answer: {'YES' if mean_pct_rank >= 95 else 'NO'}**\n\n")

        f.write("### 2. Does the edge survive fees, slippage, and latency?\n\n")
        f.write("Pending dedicated ablations (fees 1x/1.5x/2x, slippage 0/realistic/punitive, "
                "polling 30s/1m/5m/10m). Current run used realistic fees (4.32 bps/side) + 5 bps slippage.\n\n")

        f.write("### 3. Is performance persistent across folds?\n\n")
        f.write(f"Test Sharpe per fold: {successful['test_sharpe'].round(2).tolist()}\n\n")
        f.write(f"Mean {mean_test_sharpe:.2f}, median {median_test_sharpe:.2f}, "
                f"std {float(successful['test_sharpe'].std()):.2f}.\n\n")
        latest_pos = "yes" if latest_fold_sharpe > 0 else "no"
        f.write(f"Latest fold profitable: **{latest_pos}** (Sharpe {latest_fold_sharpe:.2f})\n\n")

        f.write("### 4. Is alpha independent of BTC, ETH, perp index, and alt basket beta?\n\n")
        f.write("Pending multi-factor regression on test equity curves; requires market index data "
                "alongside test results.\n\n")

        f.write("### 5. Is the edge diversified or concentrated in a few wallets?\n\n")
        if rmv:
            for k, s in sorted(rmv.items()):
                f.write(f"- Remove top {k}: mean test Sharpe = {s:.2f}\n")
        else:
            f.write("- Remove-top tests did not run (insufficient eligible wallets).\n")
        f.write(f"\n**Answer: {'diversified' if robust_ok else 'concentrated or fragile'}**\n\n")

        f.write("### 6. Which wallet traits predict future copyable PnL?\n\n")
        if args.wallet_metrics and Path(args.wallet_metrics).exists():
            wm = pd.read_parquet(args.wallet_metrics)
            elig = wm[wm["eligible"]]
            if not elig.empty:
                corr_cols = [
                    "sharpe_pct", "sortino_pct", "max_dd_pct", "journey_win_rate",
                    "profit_factor", "median_journey_bps", "median_holding_hours",
                    "turnover_per_day", "pnl_concentration_coin", "pnl_concentration_day",
                    "btc_eth_r2", "hl_index_beta", "fee_drag",
                ]
                target = "wallet_score"
                rows = []
                for c in corr_cols:
                    if c in elig.columns:
                        try:
                            r = elig[[c, target]].corr().iloc[0, 1]
                            rows.append((c, r))
                        except Exception:
                            pass
                rows.sort(key=lambda x: abs(x[1] or 0), reverse=True)
                f.write("Pearson correlation of each trait with composite wallet_score (eligible only):\n\n")
                f.write("| Trait | Correlation with wallet_score |\n")
                f.write("|-------|------------------------------:|\n")
                for c, r in rows:
                    f.write(f"| {c} | {r:+.3f} |\n")
                f.write("\n")
            else:
                f.write("No eligible wallets in metrics file.\n\n")
        else:
            f.write("(wallet_metrics.parquet not provided; trait analysis skipped)\n\n")

        # Per-fold details table.
        f.write("## Per-Fold Detail\n\n")
        f.write("| Fold | Train | Test | K | Gross | Val Sharpe | Test Sharpe | DD% | Net% | %rank vs random |\n")
        f.write("|-----:|-------|------|--:|------:|-----------:|------------:|----:|-----:|----------------:|\n")
        for _, row in successful.iterrows():
            f.write(
                f"| {int(row['fold'])} | {row['train_start']}..{row['train_end']} | "
                f"{row['test_start']}..{row['test_end']} | {int(row['best_K'])} | "
                f"{row['best_gross']:.1f} | {row['val_sharpe']:.2f} | "
                f"{row['test_sharpe']:.2f} | {row['test_max_dd_pct']:.1f} | "
                f"{row['test_net_return_pct']:.1f} | {row['random_pct_rank']:.0f} |\n"
            )
        f.write("\n")
        f.write("---\n")
        f.write("Report generated by `scripts/v13_report.py`.\n")

    print(f"Wrote report to {out}")


if __name__ == "__main__":
    main()
