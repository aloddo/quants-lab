#!/usr/bin/env python3
"""V13 Module 10 — Pass-Fail Gates.

Per spec: projects/quant/v13/modules/10-pass-fail-gates

Single canonical GO/NO-GO decision from per-fold portfolio sim outputs.

6 deployment gates:
- G1 Net Sharpe: agg_sharpe >= 0.5
- G2 Folds positive: >= 5 of 8 folds with net_pnl > 0 (excluding pruned)
- G3 Max DD: max_dd_pct <= 15% across folds
- G4 Worst day: worst_day_pct >= -5% across folds
- G5 Regime coverage: >= 2 (trend, vol) cells with positive contribution
- G6 Random-null: agg_sharpe > random_null p95
ALL must pass for GO.

ANTI_CORR_PRUNED folds contribute zero-return to aggregation (Module 09 + spec 6.3).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

# === Constants (Module 10 spec) ===
MIN_NET_SHARPE = 0.5
MIN_FOLDS_POSITIVE = 5
MAX_DD_PCT = 0.15
WORST_DAY_PCT = 0.05
MIN_REGIME_CELLS = 2


@dataclass
class FoldResult:
    fold_n: int
    daily_returns: pd.Series        # date → daily return %
    summary: dict                   # net_pnl, sharpe, max_dd_pct, worst_day_pct
    regime_tags: dict               # {trend, vol}
    anti_corr_pruned: bool = False


@dataclass
class GateDecision:
    go: bool
    failures: list[str] = field(default_factory=list)
    summary: dict = field(default_factory=dict)


def compute_aggregate_metrics(fold_results: list[FoldResult]) -> dict:
    """Aggregate per-fold metrics. Anti-corr-pruned folds contribute zero-return series.

    Returns dict with: agg_sharpe, folds_positive, max_dd_pct, worst_day_pct.

    codex m10 r1 fixes:
    - Pruned folds' summary max_dd_pct + worst_day_pct EXCLUDED from aggregate (spec: zero-return).
    - NaN summary values treated as FAILURES (replaced with worst-case sentinel that gates catch).
    - Pruned zero-return index uses fold.daily_returns.index if non-empty, else skipped (caller
      should ensure non-empty index upstream).
    """
    if not fold_results:
        return {"agg_sharpe": 0.0, "folds_positive": 0, "max_dd_pct": 0.0, "worst_day_pct": 0.0}

    all_daily = []
    for fr in fold_results:
        if fr.anti_corr_pruned:
            # codex m10 r1 fix #4: skip pruned folds with empty index (no contribution).
            if len(fr.daily_returns.index) > 0:
                zero_series = pd.Series(0.0, index=fr.daily_returns.index)
                all_daily.append(zero_series)
        else:
            all_daily.append(fr.daily_returns)
    if not all_daily:
        aggregated = pd.Series(dtype=float)
    else:
        aggregated = pd.concat(all_daily)

    if len(aggregated) > 0 and aggregated.std() > 0:
        annualized_sharpe = float(aggregated.mean() / aggregated.std() * np.sqrt(365))
    else:
        annualized_sharpe = 0.0
    folds_positive = sum(
        1 for fr in fold_results
        if fr.summary.get("net_pnl", 0) > 0 and not fr.anti_corr_pruned
    )
    # codex m10 r1 fix #1: pruned folds' risk metrics EXCLUDED.
    # codex m10 r1 fix #3: NaN summaries → conservative sentinel that triggers gate failure.
    non_pruned = [fr for fr in fold_results if not fr.anti_corr_pruned]
    # codex m10 r2 fix: coerce first, then check finite. Catches np.float32 NaN, Decimal NaN, "nan" strings.
    def _safe_dd(v):
        if v is None:
            return float("inf")
        try:
            f = float(v)
            return f if np.isfinite(f) else float("inf")
        except (TypeError, ValueError):
            return float("inf")
    def _safe_worst(v):
        if v is None:
            return float("-inf")
        try:
            f = float(v)
            return f if np.isfinite(f) else float("-inf")
        except (TypeError, ValueError):
            return float("-inf")
    # codex m10 r3 fix: pass None (not 0.0) as default → _safe_* maps to inf/-inf sentinel → gate fails.
    # Missing metric is NOT a safe pass.
    max_dd = max((_safe_dd(fr.summary.get("max_dd_pct")) for fr in non_pruned), default=0.0)
    worst_day = min((_safe_worst(fr.summary.get("worst_day_pct")) for fr in non_pruned), default=0.0)
    return {
        "agg_sharpe": annualized_sharpe,
        "folds_positive": folds_positive,
        "max_dd_pct": max_dd,
        "worst_day_pct": worst_day,
    }


def evaluate_regime_coverage(fold_results: list[FoldResult]) -> int:
    """Count number of distinct (trend, vol) cells with net positive mean Sharpe.
    Anti-corr-pruned folds excluded.
    """
    cell_sharpes: dict[tuple, list[float]] = {}
    for fr in fold_results:
        if fr.anti_corr_pruned:
            continue
        cell = (fr.regime_tags.get("trend"), fr.regime_tags.get("vol"))
        cell_sharpes.setdefault(cell, []).append(fr.summary.get("sharpe", 0.0))
    return sum(1 for sharpes in cell_sharpes.values() if float(np.mean(sharpes)) > 0)


def evaluate_random_null(agg_sharpe: float, random_null: dict) -> bool:
    """Strategy aggregate Sharpe must exceed random_null p95.

    codex m10 r1 fix: require finite p95 + n_trials > 0. Module 11 returns p95=-inf when
    pool too small or all trials failed — that's "not computed", not "ranked beat".
    """
    p95 = random_null.get("p95_sharpe")
    n_trials = random_null.get("n_trials", 0)
    if p95 is None or not isinstance(p95, (int, float)) or not np.isfinite(p95):
        return False
    if not isinstance(n_trials, int) or n_trials <= 0:
        return False
    return agg_sharpe > float(p95)


def evaluate_gates(fold_results: list[FoldResult], random_null: Optional[dict] = None) -> GateDecision:
    """ALL 6 gates must pass for GO."""
    agg = compute_aggregate_metrics(fold_results)
    failures: list[str] = []

    # G1
    if agg["agg_sharpe"] < MIN_NET_SHARPE:
        failures.append(f"G1: agg_sharpe {agg['agg_sharpe']:.2f} < {MIN_NET_SHARPE}")
    # G2
    if agg["folds_positive"] < MIN_FOLDS_POSITIVE:
        failures.append(f"G2: folds_positive {agg['folds_positive']}/{len(fold_results)} < {MIN_FOLDS_POSITIVE}")
    # G3
    if agg["max_dd_pct"] > MAX_DD_PCT:
        failures.append(f"G3: max_dd {agg['max_dd_pct']:.1%} > {MAX_DD_PCT:.0%}")
    # G4
    if agg["worst_day_pct"] < -WORST_DAY_PCT:
        failures.append(f"G4: worst_day {agg['worst_day_pct']:.1%} < -{WORST_DAY_PCT:.0%}")
    # G5
    regime_cells = evaluate_regime_coverage(fold_results)
    if regime_cells < MIN_REGIME_CELLS:
        failures.append(f"G5: regime_cells {regime_cells} < {MIN_REGIME_CELLS}")
    # G6
    if random_null is None:
        failures.append("G6: random_null not provided")
        random_p95 = None
    else:
        random_p95 = random_null.get("p95_sharpe")
        if not evaluate_random_null(agg["agg_sharpe"], random_null):
            failures.append(f"G6: agg_sharpe {agg['agg_sharpe']:.2f} <= random_null_p95 {random_p95}")

    return GateDecision(
        go=(len(failures) == 0),
        failures=failures,
        summary={
            **agg,
            "regime_cells": regime_cells,
            "random_null_p95": random_p95,
        },
    )
