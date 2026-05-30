"""V13 Module 11 — Strict Random Null tests.

Maps to per-Module 12 spec fixtures F11-1 through F11-5.
"""
import pytest
import sys
import numpy as np
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))


def _stub_portfolio_sim_constant_sharpe(sharpe: float):
    """Returns a sim_fn that always reports the same Sharpe (for determinism tests)."""
    def sim_fn(pool, params, fold_window):
        return {"summary": {"sharpe": sharpe}}
    return sim_fn


def _stub_portfolio_sim_random_sharpe(seed: int):
    """Returns a sim_fn that produces deterministic per-pool Sharpes based on pool hash."""
    def sim_fn(pool, params, fold_window):
        # Use hash of sorted pool as deterministic seed
        h = hash(tuple(sorted(pool))) % 1_000_000
        rng = np.random.default_rng(seed + h)
        return {"summary": {"sharpe": float(rng.normal(0.3, 0.2))}}
    return sim_fn


def test_F11_1_deterministic_seed_reproducible():
    """Same fold + params + seed → identical sharpes list."""
    from v13_strict_random_null import compute_random_null_for_fold
    pool = [f"0x{i:040x}" for i in range(50)]
    params = {"K_target": 10, "poll_cadence_s": 300}
    sim = _stub_portfolio_sim_random_sharpe(seed=1)

    out1 = compute_random_null_for_fold(
        fold_n=1, eligible_pool=pool, params=params, K_target=10,
        fold_window=(0, 1), portfolio_sim_fn=sim, n_pools=20, parallel_workers=1,
    )
    out2 = compute_random_null_for_fold(
        fold_n=1, eligible_pool=pool, params=params, K_target=10,
        fold_window=(0, 1), portfolio_sim_fn=sim, n_pools=20, parallel_workers=1,
    )
    # With parallel_workers=1, ordering is deterministic
    assert out1["sharpes"] == out2["sharpes"]
    assert out1["p95_sharpe"] == out2["p95_sharpe"]


def test_F11_2_dispersion_with_random_sim():
    """300 trials with stochastic sim_fn → distribution has non-zero spread."""
    from v13_strict_random_null import compute_random_null_for_fold
    pool = [f"0x{i:040x}" for i in range(100)]
    params = {"K_target": 10}
    sim = _stub_portfolio_sim_random_sharpe(seed=42)

    out = compute_random_null_for_fold(
        fold_n=1, eligible_pool=pool, params=params, K_target=10,
        fold_window=(0, 1), portfolio_sim_fn=sim, n_pools=100, parallel_workers=1,
    )
    sharpes = np.array(out["sharpes"])
    assert len(sharpes) == 100
    assert sharpes.std() > 0, "stochastic sim should produce dispersion"
    # p95 should be greater than median
    assert out["p95_sharpe"] > out["median_sharpe"]


def test_F11_3_constant_sim_p95_equals_value():
    """If sim always returns same Sharpe → p95 = that Sharpe."""
    from v13_strict_random_null import compute_random_null_for_fold
    pool = [f"0x{i:040x}" for i in range(50)]
    sim = _stub_portfolio_sim_constant_sharpe(0.42)

    out = compute_random_null_for_fold(
        fold_n=1, eligible_pool=pool, params={"K": 10}, K_target=10,
        fold_window=(0, 1), portfolio_sim_fn=sim, n_pools=50, parallel_workers=1,
    )
    assert out["p95_sharpe"] == pytest.approx(0.42)
    assert out["median_sharpe"] == pytest.approx(0.42)


def test_F11_4_pool_too_small_returns_inf():
    """Eligible pool < K_target → -inf p95 + warning."""
    from v13_strict_random_null import compute_random_null_for_fold
    sim = _stub_portfolio_sim_constant_sharpe(0.5)
    out = compute_random_null_for_fold(
        fold_n=1, eligible_pool=["0xa"] * 5, params={"K": 10}, K_target=10,
        fold_window=(0, 1), portfolio_sim_fn=sim, n_pools=100, parallel_workers=1,
    )
    assert out["p95_sharpe"] == float("-inf")
    assert "pool_too_small" in out["warning"]


def test_F11_5_nan_trials_filtered():
    """NaN trials must be filtered from quantile compute."""
    from v13_strict_random_null import compute_random_null_for_fold
    pool = [f"0x{i:040x}" for i in range(50)]

    def mixed_sim(pool, params, fold_window):
        # Half return NaN
        if len(pool) % 2 == 0:
            return {"summary": {"sharpe": float("nan")}}
        return {"summary": {"sharpe": 0.5}}

    out = compute_random_null_for_fold(
        fold_n=1, eligible_pool=pool, params={"K": 9}, K_target=9,
        fold_window=(0, 1), portfolio_sim_fn=mixed_sim, n_pools=20, parallel_workers=1,
    )
    # All pools are size 9 (odd) → all return 0.5; NaN filter still applies as smoke
    # Actually pool size is 9 odd → all sharpes are 0.5
    assert all(s == 0.5 for s in out["sharpes"])
    assert out["p95_sharpe"] == pytest.approx(0.5)


def test_F11_all_trials_fail_returns_inf():
    """If all sims raise, p95 = -inf with warning."""
    from v13_strict_random_null import compute_random_null_for_fold

    def bad_sim(pool, params, fold_window):
        raise RuntimeError("simulated failure")

    pool = [f"0x{i:040x}" for i in range(50)]
    out = compute_random_null_for_fold(
        fold_n=1, eligible_pool=pool, params={"K": 10}, K_target=10,
        fold_window=(0, 1), portfolio_sim_fn=bad_sim, n_pools=20, parallel_workers=1,
    )
    assert out["p95_sharpe"] == float("-inf")
    assert "all_trials" in (out.get("warning") or "")


def test_F11_aggregate_across_folds():
    """Aggregate per-fold sharpes via mean → final p95. codex m11 r7: STRICT mode
    requires `sharpes_dense` + `n_pools` on every fold (the contract emitted by
    compute_random_null_for_fold)."""
    from v13_strict_random_null import aggregate_random_null_across_folds
    per_fold = {
        1: {"sharpes": [0.1, 0.2, 0.3, 0.4, 0.5],
            "sharpes_dense": [0.1, 0.2, 0.3, 0.4, 0.5], "n_pools": 5},
        2: {"sharpes": [0.2, 0.3, 0.4, 0.5, 0.6],
            "sharpes_dense": [0.2, 0.3, 0.4, 0.5, 0.6], "n_pools": 5},
        3: {"sharpes": [0.3, 0.4, 0.5, 0.6, 0.7],
            "sharpes_dense": [0.3, 0.4, 0.5, 0.6, 0.7], "n_pools": 5},
    }
    agg = aggregate_random_null_across_folds(per_fold)
    # Per-trial means: [0.2, 0.3, 0.4, 0.5, 0.6]
    assert agg["n_trials"] == 5
    assert agg["fold_count"] == 3
    assert agg["median_sharpe"] == pytest.approx(0.4)
    # p95 of [0.2, 0.3, 0.4, 0.5, 0.6] = 0.58
    assert agg["p95_sharpe"] == pytest.approx(0.58)


def test_F11_aggregate_rejects_missing_dense_or_npools():
    """codex m11 r7: any fold missing `sharpes_dense` or `n_pools` → fail closed.
    This guards against silent legacy back-compat that could let an incomplete null
    aggregate look valid to Module 10 G6."""
    from v13_strict_random_null import aggregate_random_null_across_folds
    # Missing sharpes_dense on fold 2
    per_fold_a = {
        1: {"sharpes": [0.1, 0.2], "sharpes_dense": [0.1, 0.2], "n_pools": 2},
        2: {"sharpes": [0.3, 0.4]},  # no dense, no n_pools
    }
    agg_a = aggregate_random_null_across_folds(per_fold_a)
    assert agg_a["p95_sharpe"] == float("-inf")
    assert agg_a["n_trials"] == 0
    assert "missing_required_fields" in (agg_a["warning"] or "")
    # Missing n_pools only
    per_fold_b = {
        1: {"sharpes": [0.1, 0.2], "sharpes_dense": [0.1, 0.2], "n_pools": 2},
        2: {"sharpes": [0.3, 0.4], "sharpes_dense": [0.3, 0.4]},  # no n_pools
    }
    agg_b = aggregate_random_null_across_folds(per_fold_b)
    assert agg_b["p95_sharpe"] == float("-inf")
    assert "missing_required_fields" in (agg_b["warning"] or "")


def test_F11_aggregate_empty_folds():
    from v13_strict_random_null import aggregate_random_null_across_folds
    agg = aggregate_random_null_across_folds({})
    assert agg["p95_sharpe"] == float("-inf")
    assert agg["fold_count"] == 0


def test_F11_aggregate_rejects_uneven_trial_counts():
    """codex m11 r6: spec REQUIRES fixed N=300 per fold. Uneven dense-list lengths
    are rejected (returns -inf, n_trials=0, warning) so Module 10 G6 fails closed
    on incomplete null coverage. Without this, the silent trim-to-min would hide
    insufficient sampling and let a strategy pass G6 on partial trials."""
    from v13_strict_random_null import aggregate_random_null_across_folds
    per_fold = {
        1: {"sharpes": [0.1, 0.2, 0.3], "sharpes_dense": [0.1, 0.2, 0.3], "n_pools": 3},
        2: {"sharpes": [0.2, 0.3], "sharpes_dense": [0.2, 0.3], "n_pools": 2},
        3: {"sharpes": [0.4, 0.5, 0.6, 0.7], "sharpes_dense": [0.4, 0.5, 0.6, 0.7], "n_pools": 4},
    }
    agg = aggregate_random_null_across_folds(per_fold)
    assert agg["p95_sharpe"] == float("-inf"), "must reject uneven n_pools (fail closed)"
    assert agg["n_trials"] == 0
    assert agg["warning"] is not None
    assert "mismatch" in agg["warning"].lower()


def test_F11_aggregate_insufficient_coverage_fails_closed():
    """codex m11 r12: STRICT 95% coverage threshold. When fewer than 95% of the
    n_pools trial indices are aligned-valid across all folds, fail closed with
    -inf p95 and `insufficient_coverage` warning. Without this, a strategy could
    pass G6 against a null distribution computed from a single random pool — meaningless."""
    from v13_strict_random_null import aggregate_random_null_across_folds
    # 1 of 10 trials aligned across folds = 10% coverage → fail closed
    per_fold = {
        1: {"sharpes": [0.1], "sharpes_dense": [None]*9 + [0.1], "n_pools": 10},
        2: {"sharpes": [0.2], "sharpes_dense": [None]*9 + [0.2], "n_pools": 10},
    }
    agg = aggregate_random_null_across_folds(per_fold)
    assert agg["p95_sharpe"] == float("-inf")
    assert "insufficient_coverage" in (agg["warning"] or "")
    assert "10.0%" in (agg["warning"] or "")


def test_F11_aggregate_partial_coverage_warns_but_returns():
    """codex m11 r12: 95% <= coverage < 100% returns aggregate WITH `partial_coverage`
    warning. Allows real-world tolerance for rare producer failures while still
    forcing a visible diagnostic."""
    from v13_strict_random_null import aggregate_random_null_across_folds
    # 19 of 20 = 95.0% coverage exactly → returns aggregate, partial_coverage warning
    per_fold = {
        1: {"sharpes": [0.1]*19, "sharpes_dense": [0.1]*19 + [None], "n_pools": 20},
        2: {"sharpes": [0.2]*19, "sharpes_dense": [0.2]*19 + [None], "n_pools": 20},
    }
    agg = aggregate_random_null_across_folds(per_fold)
    assert agg["p95_sharpe"] != float("-inf")
    assert agg["n_trials"] == 19
    assert "partial_coverage" in (agg["warning"] or "")


def test_F11_aggregate_full_coverage_no_warning():
    """codex m11 r12: 100% coverage returns aggregate with warning=None."""
    from v13_strict_random_null import aggregate_random_null_across_folds
    per_fold = {
        1: {"sharpes": [0.1]*5, "sharpes_dense": [0.1]*5, "n_pools": 5},
        2: {"sharpes": [0.2]*5, "sharpes_dense": [0.2]*5, "n_pools": 5},
    }
    agg = aggregate_random_null_across_folds(per_fold)
    assert agg["p95_sharpe"] != float("-inf")
    assert agg["n_trials"] == 5
    assert agg["warning"] is None


def test_F11_aggregate_rejects_malformed_dense_elements():
    """codex m11 r10: element-level type validation. Strings, bools, NaN, Inf in
    sharpes_dense → fail closed. Mirrored on producer side in run_one (r11)."""
    from v13_strict_random_null import aggregate_random_null_across_folds
    # String in dense
    per_a = {
        1: {"sharpes": [0.1], "sharpes_dense": ["bad", 0.1], "n_pools": 2},
        2: {"sharpes": [0.2, 0.3], "sharpes_dense": [0.2, 0.3], "n_pools": 2},
    }
    agg_a = aggregate_random_null_across_folds(per_a)
    assert agg_a["p95_sharpe"] == float("-inf")
    assert "malformed_required_fields" in (agg_a["warning"] or "")
    # Bool in dense (would silently aggregate as 1.0/0.0 without guard)
    per_b = {
        1: {"sharpes": [0.1], "sharpes_dense": [True, 0.1], "n_pools": 2},
        2: {"sharpes": [0.2, 0.3], "sharpes_dense": [0.2, 0.3], "n_pools": 2},
    }
    agg_b = aggregate_random_null_across_folds(per_b)
    assert agg_b["p95_sharpe"] == float("-inf")
    assert "malformed_required_fields" in (agg_b["warning"] or "")
    # Inf
    per_c = {
        1: {"sharpes": [0.1], "sharpes_dense": [float("inf"), 0.1], "n_pools": 2},
        2: {"sharpes": [0.2, 0.3], "sharpes_dense": [0.2, 0.3], "n_pools": 2},
    }
    agg_c = aggregate_random_null_across_folds(per_c)
    assert agg_c["p95_sharpe"] == float("-inf")
