"""V13 Module 09 — Walk-Forward Folds tests.

Maps to per-Module 12 spec fixtures F9-1 through F9-6.
"""
import pytest
import sys
import numpy as np
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))


def test_F9_1_eight_folds_rolling_15_day_step():
    from v13_walk_forward_folds import build_folds
    start = date(2025, 12, 1)
    folds = build_folds(start, n_folds=8)
    assert len(folds) == 8
    # Fold 1: train 12/01-12/30, val 12/31-01/14, test 01/15-01/29
    f1 = folds[0]
    assert f1.train_start == date(2025, 12, 1)
    assert f1.train_end == date(2025, 12, 30)
    assert f1.val_start == date(2025, 12, 31)
    assert f1.val_end == date(2026, 1, 14)
    assert f1.test_start == date(2026, 1, 15)
    assert f1.test_end == date(2026, 1, 29)
    # Fold 2 starts 15 days after fold 1
    f2 = folds[1]
    assert f2.train_start == f1.train_start + timedelta(days=15)


def test_F9_2_no_leak_train_lt_val_lt_test_internal():
    """Each fold's train < val < test internally (no per-fold leak)."""
    from v13_walk_forward_folds import build_folds
    folds = build_folds(date(2025, 12, 1), n_folds=8)
    for f in folds:
        assert f.train_end < f.val_start
        assert f.val_end < f.test_start


def test_F9_3_regime_tags_classification():
    """BTC trend buckets + vol buckets per spec."""
    from v13_walk_forward_folds import classify_btc_trend, classify_volatility
    # BULL: +15%
    assert classify_btc_trend(115, 100) == "BULL"
    # NEUTRAL: +5%
    assert classify_btc_trend(105, 100) == "NEUTRAL"
    # BEAR: -15%
    assert classify_btc_trend(85, 100) == "BEAR"
    # Edge: ±10% exactly is NEUTRAL (strict > / <)
    assert classify_btc_trend(110, 100) == "NEUTRAL"
    assert classify_btc_trend(90, 100) == "NEUTRAL"
    # Unknown if 30d ago price 0
    assert classify_btc_trend(115, 0) == "UNKNOWN"

    # Volatility
    train_dvols = list(range(50, 100))  # range 50-99
    # p33 ~= 66, p66 ~= 82
    assert classify_volatility(40, train_dvols) == "LOW"
    assert classify_volatility(75, train_dvols) == "MID"
    assert classify_volatility(90, train_dvols) == "HIGH"
    # Too few samples → UNKNOWN
    assert classify_volatility(50, [50, 60]) == "UNKNOWN"


def test_F9_3_tag_fold_regime_integration():
    from v13_walk_forward_folds import build_folds, tag_fold_regime
    folds = build_folds(date(2025, 12, 1), n_folds=1)
    f = folds[0]

    def fake_market_data(d):
        # Linear BTC price up
        days = (d - date(2025, 12, 1)).days
        return {
            "btc_price": 50_000 + days * 500,
            "hl_perp_price": 100 + days * 1,
            "btc_dvol": 60 + (days % 10),
        }

    tags = tag_fold_regime(f, fake_market_data)
    assert "btc_trend" in tags
    assert "hl_trend" in tags
    assert "vol" in tags
    assert f.regime_tags == tags


def test_F9_4_anti_corr_greedy_below_threshold_keeps():
    """Wallets with low correlation get added; high-corr blocked."""
    from v13_walk_forward_folds import anti_corr_greedy_fill
    # Wallet A and B uncorrelated; C is highly correlated with A
    rng = np.random.default_rng(42)
    A = rng.normal(0, 1, 30)
    B = rng.normal(0, 1, 30)
    C = A + rng.normal(0, 0.01, 30)  # highly correlated with A

    scores = {"A": 1.0, "B": 0.9, "C": 0.8}
    daily_returns = {"A": A, "B": B, "C": C}
    selected, pruned = anti_corr_greedy_fill(
        scores, daily_returns, threshold=0.6, K_target=3, candidate_multiplier=3
    )
    assert "A" in selected
    assert "B" in selected  # low corr with A
    assert "C" not in selected  # high corr with A
    # 2 selected, K_target=3 → 2 >= 1.5 → NOT pruned
    assert not pruned


def test_F9_5_anti_corr_pruned_flag_when_majority_excluded():
    """If fewer than K_target/2 selected → ANTI_CORR_PRUNED = True."""
    from v13_walk_forward_folds import anti_corr_greedy_fill
    rng = np.random.default_rng(1)
    A = rng.normal(0, 1, 30)
    # All others highly correlated with A
    scores = {f"W{i}": 1.0 - i * 0.01 for i in range(10)}
    daily_returns = {f"W{i}": A + rng.normal(0, 0.01, 30) for i in range(10)}
    selected, pruned = anti_corr_greedy_fill(
        scores, daily_returns, threshold=0.6, K_target=5, candidate_multiplier=3
    )
    assert len(selected) == 1  # only first wallet
    assert pruned  # 1 < 5/2=2.5


def test_F9_anti_corr_no_positive_scores_pruned():
    from v13_walk_forward_folds import anti_corr_greedy_fill
    scores = {"A": -0.1, "B": 0.0, "C": -0.5}
    selected, pruned = anti_corr_greedy_fill(
        scores, daily_returns_by_wallet={}, threshold=0.6, K_target=3,
    )
    assert selected == []
    assert pruned


def test_F9_anti_corr_negative_correlation_accepted():
    """codex m09 r1 fix: SIGNED correlation. Negative corr should pass (diversifying)."""
    from v13_walk_forward_folds import anti_corr_greedy_fill
    rng = np.random.default_rng(42)
    A = rng.normal(0, 1, 30)
    B_neg = -A  # perfectly negative correlation
    scores = {"A": 1.0, "B": 0.9}
    daily_returns = {"A": A, "B": B_neg}
    selected, pruned = anti_corr_greedy_fill(scores, daily_returns, threshold=0.6, K_target=2)
    # B is NEGATIVELY correlated with A → max signed corr = -1 → < 0.6 → ACCEPT
    assert "A" in selected
    assert "B" in selected, "negative correlation should pass anti-corr (diversifying)"


def test_F9_anti_corr_accepts_spec_multiplier_kwarg():
    """codex m09 r1 fix: spec uses `multiplier=3`, code originally only accepted `candidate_multiplier`."""
    from v13_walk_forward_folds import anti_corr_greedy_fill
    rng = np.random.default_rng(7)
    A = rng.normal(0, 1, 30)
    B = rng.normal(0, 1, 30)
    scores = {"A": 1.0, "B": 0.9}
    daily_returns = {"A": A, "B": B}
    # Should NOT raise TypeError with multiplier kwarg
    selected, _ = anti_corr_greedy_fill(scores, daily_returns, threshold=0.8, K_target=2, multiplier=3)
    assert len(selected) >= 1


def test_F9_regime_lookback_uses_30d_not_45d():
    """codex m09 r1 CRITICAL fix: regime trend uses test_start - 30d, not train_start (=test_start - 45d)."""
    from v13_walk_forward_folds import build_folds, tag_fold_regime
    folds = build_folds(date(2025, 12, 1), n_folds=1)
    f = folds[0]
    # f.train_start=2025-12-01, f.test_start=2026-01-15 → 30d lookback = 2025-12-16
    lookback_dates = []
    def md(d):
        lookback_dates.append(d)
        return {"btc_price": 100, "hl_perp_price": 100, "btc_dvol": 60}
    tag_fold_regime(f, md)
    # Should be called with test_start AND (test_start - 30d), NOT train_start
    expected_lookback = date(2025, 12, 16)
    assert expected_lookback in lookback_dates, f"30d lookback {expected_lookback} not queried. Called: {sorted(set(lookback_dates))[:5]}"


def test_F9_classify_btc_trend_nan_returns_UNKNOWN():
    """codex m09 r2 fix: NaN/Inf prices must return UNKNOWN, not falsely NEUTRAL."""
    from v13_walk_forward_folds import classify_btc_trend
    assert classify_btc_trend(float('nan'), 100) == "UNKNOWN"
    assert classify_btc_trend(100, float('nan')) == "UNKNOWN"
    assert classify_btc_trend(float('inf'), 100) == "UNKNOWN"
    assert classify_btc_trend(100, -100) == "UNKNOWN"  # already covered


def test_F9_classify_volatility_nan_returns_UNKNOWN():
    """codex m09 r2 fix: NaN dvol must return UNKNOWN, not MID."""
    from v13_walk_forward_folds import classify_volatility
    train_dvols = list(range(50, 100))
    assert classify_volatility(float('nan'), train_dvols) == "UNKNOWN"
    assert classify_volatility(float('inf'), train_dvols) == "UNKNOWN"


def test_F9_tag_fold_regime_nan_market_data_returns_UNKNOWN():
    """codex m09 r2 fix: NaN values in market_data_fn → all tags UNKNOWN."""
    from v13_walk_forward_folds import build_folds, tag_fold_regime
    folds = build_folds(date(2025, 12, 1), n_folds=1)
    f = folds[0]
    tags = tag_fold_regime(f, lambda d: {
        "btc_price": float("nan"),
        "hl_perp_price": float("nan"),
        "btc_dvol": float("nan"),
    })
    assert tags["btc_trend"] == "UNKNOWN"
    assert tags["hl_trend"] == "UNKNOWN"
    assert tags["vol"] == "UNKNOWN"


def test_F9_regime_missing_data_returns_UNKNOWN():
    """codex m09 r1 fix: missing market data → UNKNOWN, not falsely BEAR/LOW."""
    from v13_walk_forward_folds import build_folds, tag_fold_regime
    folds = build_folds(date(2025, 12, 1), n_folds=1)
    f = folds[0]
    # market_data_fn returns empty dict for everything
    tags = tag_fold_regime(f, lambda d: {})
    assert tags["btc_trend"] == "UNKNOWN"
    assert tags["hl_trend"] == "UNKNOWN"
    assert tags["vol"] == "UNKNOWN"


def test_F9_6_anti_corr_threshold_sensitivity():
    """Threshold 0.8 lets more in; threshold 0.4 lets fewer in."""
    from v13_walk_forward_folds import anti_corr_greedy_fill
    rng = np.random.default_rng(7)
    A = rng.normal(0, 1, 30)
    B_mid = A * 0.5 + rng.normal(0, 0.5, 30)  # ~0.5 corr
    scores = {"A": 1.0, "B": 0.9}
    daily_returns = {"A": A, "B": B_mid}

    # Threshold 0.4: B blocked
    sel_04, _ = anti_corr_greedy_fill(scores, daily_returns, threshold=0.4, K_target=2)
    # Threshold 0.8: B accepted
    sel_08, _ = anti_corr_greedy_fill(scores, daily_returns, threshold=0.8, K_target=2)
    assert len(sel_08) >= len(sel_04)
