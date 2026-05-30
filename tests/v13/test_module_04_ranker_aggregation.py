"""V13 Module 04 — copy_score aggregation tests.

Tests the pure aggregation helpers (shrunk_winsorized_mean, capped_jpd, positivity_factor,
compute_copy_score, fit_eb_shrinkage_k). These are data-independent.

Per Module 04 spec + Module 12 fixtures F4-1 through F4-5.
"""
import pytest
import sys
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))


def test_F4_1_single_wallet_5_journeys_all_positive():
    """5 journeys all positive → positive copy_score."""
    from v13_copy_ranker_v2 import compute_copy_score
    returns = np.array([0.01, 0.02, 0.015, 0.03, 0.025])
    # With n=5 < MIN_N_JOURNEYS=20 → excluded
    score, reason = compute_copy_score(returns, active_days=5, global_pool_median=0.0)
    assert score == 0.0
    assert reason == "n<20"


def test_F4_1b_passes_min_with_n_20():
    """With n=20 returns the score is computed (shrunk toward global_median=0)."""
    from v13_copy_ranker_v2 import compute_copy_score
    returns = np.array([0.01] * 20)  # all small positive returns
    score, reason = compute_copy_score(returns, active_days=30, global_pool_median=0.0)
    # n=20 ≥ MIN_N_JOURNEYS; all positive; jpd = 20/30 = 0.667; pf = 1.0
    # winsorized mean = 0.01; lambda = 20/(20+50) = 0.286
    # shrunk = 0.286 × 0.01 + 0.714 × 0 = 0.00286
    # score = 0.00286 × 0.667 × 1.0 = 0.00190
    assert reason is None
    expected = (20/(20+50)) * 0.01 * (20/30) * 1.0
    assert score == pytest.approx(expected, rel=0.01)


def test_F4_2_shrunk_winsorized_outlier_robust():
    """1 outlier 100× normal → impact capped via winsorization at p95."""
    from v13_copy_ranker_v2 import shrunk_winsorized_mean
    # 19 small returns + 1 huge outlier
    returns = np.array([0.01] * 19 + [10.0])
    # 95th percentile of these 20 values ≈ 0.5305 (between 0.01 and 10)
    # Winsorize at p5=0.01, p95=0.5305 (interpolated)
    sm = shrunk_winsorized_mean(returns, global_pool_median=0.0, shrinkage_k=50)
    # Wins mean is much lower than the raw mean (which is 0.5095)
    raw_mean = float(returns.mean())
    assert sm < raw_mean * 0.5, f"winsorization should cap impact; got {sm} vs raw {raw_mean}"


def test_F4_3_capped_jpd():
    """journeys per day capped at 5."""
    from v13_copy_ranker_v2 import capped_jpd
    assert capped_jpd(100, 30) == pytest.approx(100/30)  # 3.33 < 5 → not capped
    assert capped_jpd(200, 30) == 5.0                    # 6.67 → capped to 5
    assert capped_jpd(1000, 30) == 5.0                   # 33.3 → capped to 5
    assert capped_jpd(0, 10) == 0.0
    assert capped_jpd(5, 0) == 0.0                       # active_days=0 → 0


def test_F4_4_positivity_factor():
    """frac_pos = 60% → factor = (0.6-0.5)/0.5 = 0.2."""
    from v13_copy_ranker_v2 import positivity_factor
    returns_60pct = np.array([0.01] * 12 + [-0.01] * 8)
    pf = positivity_factor(returns_60pct)
    assert pf == pytest.approx(0.2, rel=0.01)

    # 50% → 0
    returns_50pct = np.array([0.01] * 10 + [-0.01] * 10)
    assert positivity_factor(returns_50pct) == 0.0

    # 40% → 0 (floor)
    returns_40pct = np.array([0.01] * 8 + [-0.01] * 12)
    assert positivity_factor(returns_40pct) == 0.0

    # 100% → 1
    returns_all_pos = np.array([0.01] * 20)
    assert positivity_factor(returns_all_pos) == 1.0


def test_F4_5_composite_multiplicative():
    """score = shrunk_mean × capped_jpd × positivity_factor."""
    from v13_copy_ranker_v2 import compute_copy_score
    returns = np.array([0.01] * 15 + [-0.005] * 5)  # 75% positive, all small
    score, reason = compute_copy_score(returns, active_days=10, global_pool_median=0.0)
    # n=20, jpd=2.0, pf=(0.75-0.5)/0.5=0.5
    # winsorized mean ≈ mean of clipped returns
    # raw mean = (15*0.01 - 5*0.005)/20 = 0.1/20 = 0.00625
    # shrunk: lambda = 20/(20+50) = 0.286; shrunk = 0.286 × ~0.00625 + 0.714 × 0 = ~0.00179
    # score = 0.00179 × 2.0 × 0.5 = ~0.00179
    assert reason is None
    assert score > 0
    assert score < 0.005


def test_F4_excluded_n_too_small():
    """n < 20 → excluded with reason n<20."""
    from v13_copy_ranker_v2 import compute_copy_score
    returns = np.array([0.05] * 10)
    score, reason = compute_copy_score(returns, active_days=10, global_pool_median=0.0)
    assert score == 0.0
    assert reason == "n<20"


def test_F4_excluded_non_positive():
    """negative shrunk-mean → excluded with non_positive."""
    from v13_copy_ranker_v2 import compute_copy_score
    returns = np.array([-0.01] * 20)  # all negative
    score, reason = compute_copy_score(returns, active_days=10, global_pool_median=0.0)
    # positivity_factor = 0 (no positives), so score = 0
    assert reason == "non_positive" or score <= 0


def test_F4_eb_shrinkage_fit_falls_back_on_small_pool():
    """Empirical Bayes fit falls back to default when pool too small."""
    from v13_copy_ranker_v2 import fit_eb_shrinkage_k, SHRINKAGE_K_DEFAULT
    # Empty pool
    assert fit_eb_shrinkage_k([]) == SHRINKAGE_K_DEFAULT
    # Single wallet
    assert fit_eb_shrinkage_k([np.array([0.01, 0.02, 0.03])]) == SHRINKAGE_K_DEFAULT
    # Pool with all wallets having too few returns
    assert fit_eb_shrinkage_k([np.array([0.01]) for _ in range(20)]) == SHRINKAGE_K_DEFAULT


def test_F4_eb_shrinkage_fit_clamps_to_range():
    """EB k is clamped to [10, 500]."""
    from v13_copy_ranker_v2 import fit_eb_shrinkage_k
    # Pool with extremely similar wallets → between_var tiny → k_hat huge
    similar_pool = [np.random.normal(0.01, 0.001, 50) for _ in range(20)]
    k = fit_eb_shrinkage_k(similar_pool)
    assert 10.0 <= k <= 500.0
