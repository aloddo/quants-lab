"""V13 Module 10 — Pass-Fail Gates tests.

Maps to per-Module 12 spec fixtures F10-1 through F10-8.
"""
import pytest
import sys
import numpy as np
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))


def _mk_fold(n: int, daily_returns_mean: float, daily_returns_std: float, n_days: int,
             net_pnl: float, sharpe: float, max_dd_pct: float = 0.05,
             worst_day_pct: float = -0.02, trend: str = "bull", vol: str = "mid",
             pruned: bool = False, seed: int = 42):
    from v13_pass_fail_gates import FoldResult
    rng = np.random.default_rng(seed + n)
    daily = pd.Series(
        rng.normal(daily_returns_mean, daily_returns_std, n_days),
        index=pd.date_range("2025-12-01", periods=n_days, freq="D"),
    )
    return FoldResult(
        fold_n=n,
        daily_returns=daily,
        summary={"net_pnl": net_pnl, "sharpe": sharpe, "max_dd_pct": max_dd_pct, "worst_day_pct": worst_day_pct},
        regime_tags={"trend": trend, "vol": vol},
        anti_corr_pruned=pruned,
    )


def test_F10_1_all_gates_pass_GO():
    from v13_pass_fail_gates import evaluate_gates
    # 8 folds with strong positive metrics
    folds = [
        _mk_fold(i, daily_returns_mean=0.002, daily_returns_std=0.01, n_days=15,
                 net_pnl=10.0, sharpe=1.5, trend=("bull" if i % 2 == 0 else "bear"), vol=("mid" if i % 3 == 0 else "high"))
        for i in range(8)
    ]
    decision = evaluate_gates(folds, random_null={"p95_sharpe": 0.4, "n_trials": 300})
    if not decision.go:
        # Print failures for diagnostic
        print("FAILURES:", decision.failures)
        print("SUMMARY:", decision.summary)
    assert decision.go, f"expected GO; failures={decision.failures}"
    assert decision.failures == []


def test_F10_2_NO_GO_low_sharpe():
    from v13_pass_fail_gates import evaluate_gates
    folds = [
        _mk_fold(i, daily_returns_mean=0.0001, daily_returns_std=0.01, n_days=15,
                 net_pnl=1.0, sharpe=0.1)
        for i in range(8)
    ]
    decision = evaluate_gates(folds, random_null={"p95_sharpe": 0.4, "n_trials": 300})
    assert not decision.go
    assert any("G1" in f for f in decision.failures)


def test_F10_3_NO_GO_few_folds_positive():
    from v13_pass_fail_gates import evaluate_gates
    folds = [
        _mk_fold(i, daily_returns_mean=0.002, daily_returns_std=0.01, n_days=15,
                 net_pnl=(10.0 if i < 3 else -10.0), sharpe=1.5)
        for i in range(8)
    ]
    decision = evaluate_gates(folds, random_null={"p95_sharpe": 0.4, "n_trials": 300})
    # 3 positive < 5 → fail G2
    assert not decision.go
    assert any("G2" in f for f in decision.failures)


def test_F10_4_NO_GO_max_dd():
    from v13_pass_fail_gates import evaluate_gates
    folds = [
        _mk_fold(i, daily_returns_mean=0.002, daily_returns_std=0.01, n_days=15,
                 net_pnl=10.0, sharpe=1.5, max_dd_pct=0.20)  # 20% > 15%
        for i in range(8)
    ]
    decision = evaluate_gates(folds, random_null={"p95_sharpe": 0.4, "n_trials": 300})
    assert not decision.go
    assert any("G3" in f for f in decision.failures)


def test_F10_5_NO_GO_worst_day():
    from v13_pass_fail_gates import evaluate_gates
    folds = [
        _mk_fold(i, daily_returns_mean=0.002, daily_returns_std=0.01, n_days=15,
                 net_pnl=10.0, sharpe=1.5, worst_day_pct=-0.08)  # -8% < -5%
        for i in range(8)
    ]
    decision = evaluate_gates(folds, random_null={"p95_sharpe": 0.4, "n_trials": 300})
    assert not decision.go
    assert any("G4" in f for f in decision.failures)


def test_F10_6_NO_GO_one_regime_cell():
    """All folds in same (trend, vol) → only 1 cell, fail G5."""
    from v13_pass_fail_gates import evaluate_gates
    folds = [
        _mk_fold(i, daily_returns_mean=0.002, daily_returns_std=0.01, n_days=15,
                 net_pnl=10.0, sharpe=1.5, trend="bull", vol="mid")
        for i in range(8)
    ]
    decision = evaluate_gates(folds, random_null={"p95_sharpe": 0.4, "n_trials": 300})
    assert not decision.go
    assert any("G5" in f for f in decision.failures)


def test_F10_7_NO_GO_random_null_beats():
    from v13_pass_fail_gates import evaluate_gates
    folds = [
        _mk_fold(i, daily_returns_mean=0.0005, daily_returns_std=0.01, n_days=15,
                 net_pnl=5.0, sharpe=0.6, trend=("bull" if i % 2 == 0 else "bear"))
        for i in range(8)
    ]
    # random p95 = 0.9 > our sharpe → fail G6
    decision = evaluate_gates(folds, random_null={"p95_sharpe": 0.9, "n_trials": 300})
    assert not decision.go
    assert any("G6" in f for f in decision.failures)


def test_F10_8_anti_corr_pruned_zero_return_treatment():
    """3 of 8 folds pruned → contribute zero-return to aggregation; folds_positive only counts non-pruned."""
    from v13_pass_fail_gates import evaluate_gates, compute_aggregate_metrics
    folds = [
        _mk_fold(i, daily_returns_mean=0.002, daily_returns_std=0.01, n_days=15,
                 net_pnl=10.0, sharpe=1.5,
                 pruned=(i < 3))
        for i in range(8)
    ]
    agg = compute_aggregate_metrics(folds)
    # 5 non-pruned folds with positive PnL → folds_positive=5
    assert agg["folds_positive"] == 5
    # Aggregated sharpe diluted by 3 zero-return folds
    folds_no_prune = [
        _mk_fold(i, daily_returns_mean=0.002, daily_returns_std=0.01, n_days=15,
                 net_pnl=10.0, sharpe=1.5, pruned=False)
        for i in range(8)
    ]
    agg_no_prune = compute_aggregate_metrics(folds_no_prune)
    assert agg["agg_sharpe"] < agg_no_prune["agg_sharpe"], "pruned should dilute aggregate sharpe"


def test_F10_random_null_missing_treated_as_fail():
    from v13_pass_fail_gates import evaluate_gates
    folds = [
        _mk_fold(i, daily_returns_mean=0.002, daily_returns_std=0.01, n_days=15,
                 net_pnl=10.0, sharpe=1.5, trend=("bull" if i % 2 == 0 else "bear"))
        for i in range(8)
    ]
    decision = evaluate_gates(folds, random_null=None)
    assert not decision.go
    assert any("G6" in f for f in decision.failures)


def test_F10_empty_folds():
    from v13_pass_fail_gates import evaluate_gates, compute_aggregate_metrics
    agg = compute_aggregate_metrics([])
    assert agg["folds_positive"] == 0
    decision = evaluate_gates([], random_null={"p95_sharpe": 0.5, "n_trials": 300})
    assert not decision.go
