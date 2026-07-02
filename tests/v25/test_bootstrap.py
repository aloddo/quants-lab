"""v25 bootstrap + hurdle unit tests (synthetic data only)."""
import numpy as np
import pandas as pd
import pytest

from v25_bootstrap import (bonferroni_lcb, hurdle_daily_usd, joint_lcb,
                           single_rule_lcb, stationary_bootstrap_indices)
from v25_common import MS_DAY
from v25_r2_lcb import wallet_lcb


class TestHurdle:
    def test_frozen_formula(self):
        # hurdle = (10/10000) x 150 x trips/days = 0.15 x trips/days
        assert hurdle_daily_usd(100, 100) == pytest.approx(0.15)
        assert hurdle_daily_usd(206, 103) == pytest.approx(0.30)
        assert hurdle_daily_usd(0, 103) == 0.0
        assert hurdle_daily_usd(10, 0) == float("inf")


class TestStationaryBootstrap:
    def test_shape_and_range(self):
        rng = np.random.default_rng(42)
        idx = stationary_bootstrap_indices(50, 7, rng)
        assert idx.shape == (50,)
        assert idx.min() >= 0 and idx.max() < 50

    def test_deterministic(self):
        a = stationary_bootstrap_indices(100, 7, np.random.default_rng(42))
        b = stationary_bootstrap_indices(100, 7, np.random.default_rng(42))
        assert np.array_equal(a, b)

    def test_empty(self):
        assert stationary_bootstrap_indices(0, 7, np.random.default_rng(1)).size == 0


def _segments(mu, seed):
    rng = np.random.default_rng(seed)
    return [rng.normal(mu, 1.0, 21) for _ in range(4)] + [rng.normal(mu, 1.0, 18)]


class TestJointLCB:
    def test_deterministic(self):
        segs = {"R1": _segments(0.5, 1), "R2": _segments(0.3, 2)}
        a = joint_lcb(segs, n_resamples=500)
        b = joint_lcb(segs, n_resamples=500)
        assert a["rules"]["R1"]["lcb_maxstat"] == b["rules"]["R1"]["lcb_maxstat"]
        assert a["rules"]["R2"]["lcb_bonferroni"] == b["rules"]["R2"]["lcb_bonferroni"]

    def test_lcb_below_mean_and_ordering(self):
        segs = {"R1": _segments(1.0, 1), "R2": _segments(1.0, 2)}
        out = joint_lcb(segs, n_resamples=1000)
        for r in ("R1", "R2"):
            assert out["rules"][r]["lcb_maxstat"] < out["rules"][r]["mean_daily_usd"]
        assert out["c_maxstat"] > 0

    def test_positive_signal_clears_zero(self):
        # strong positive mean, tiny noise: LCB must stay above 0
        segs = {"R1": _segments(5.0, 3), "R2": _segments(5.0, 4)}
        out = joint_lcb(segs, n_resamples=1000)
        assert out["rules"]["R1"]["lcb_maxstat"] > 0

    def test_mismatched_fold_lengths_raise(self):
        segs = {"R1": [np.zeros(21)], "R2": [np.zeros(19)]}
        with pytest.raises(ValueError):
            joint_lcb(segs, n_resamples=10)

    def test_maxstat_at_least_as_conservative_as_single(self):
        # familywise c uses max over rules -> per-rule LCB <= its own 95% one-sided LCB
        segs2 = {"R1": _segments(0.5, 5), "R2": _segments(-0.5, 6)}
        out2 = joint_lcb(segs2, n_resamples=2000)
        segs1 = {"R1": segs2["R1"]}
        out1 = joint_lcb(segs1, n_resamples=2000)
        assert out2["rules"]["R1"]["lcb_maxstat"] <= out1["rules"]["R1"]["lcb_maxstat"] + 1e-9


class TestBonferroniFallback:
    def test_deterministic_and_below_mean(self):
        segs = {"R1": _segments(1.0, 7), "R2": _segments(0.5, 8)}
        a = bonferroni_lcb(segs, n_resamples=1000)
        b = bonferroni_lcb(segs, n_resamples=1000)
        assert a["method"] == "bonferroni_fallback"
        assert a["alpha_per_rule"] == pytest.approx(0.025)
        for r in ("R1", "R2"):
            assert a["rules"][r]["lcb_maxstat"] == b["rules"][r]["lcb_maxstat"]
            assert a["rules"][r]["lcb_maxstat"] < a["rules"][r]["mean_daily_usd"]

    def test_handles_empty_rule(self):
        # this is exactly the case that makes the joint bootstrap non-finite/raise
        out = bonferroni_lcb({"R1": _segments(1.0, 9), "R2": []}, n_resamples=200)
        assert np.isfinite(out["rules"]["R1"]["lcb_maxstat"])
        assert not np.isfinite(out["rules"]["R2"]["lcb_maxstat"])


class TestSingleRuleLCB:
    def test_holdout_statistic(self):
        segs = [np.random.default_rng(1).normal(2.0, 1.0, 35)]
        out = single_rule_lcb(segs, n_resamples=1000, level=0.95)
        assert out["n_days"] == 35
        assert out["lcb"] < out["mean_daily_usd"]
        assert out["lcb"] > 0                      # strong signal clears zero

    def test_empty(self):
        out = single_rule_lcb([], n_resamples=100)
        assert out["n_days"] == 0 and np.isnan(out["lcb"])


class TestWalletLCB:
    def _trips(self, n, mu, start):
        rng = np.random.default_rng(7)
        return pd.DataFrame({
            "net_bps": rng.normal(mu, 5.0, n),
            # FROZEN: trip belongs to its EXIT date (UTC)
            "exit_fill_ts_last": start + (rng.integers(0, 60, n) * MS_DAY
                                          + rng.integers(0, MS_DAY, n)),
        })

    def test_deterministic(self):
        start = pd.Timestamp("2025-12-01").value // 10**6
        asof = start + 60 * MS_DAY
        t = self._trips(120, 20.0, start)
        a = wallet_lcb(t, start, asof)
        b = wallet_lcb(t, start, asof)
        assert a["lcb_bps"] == b["lcb_bps"]
        assert a["n_trips"] == 120

    def test_lcb_below_mean_and_signal_detected(self):
        start = pd.Timestamp("2025-12-01").value // 10**6
        asof = start + 60 * MS_DAY
        t = self._trips(200, 30.0, start)
        r = wallet_lcb(t, start, asof)
        assert r["lcb_bps"] < r["mean_bps"]
        assert r["lcb_bps"] > 0

    def test_exit_date_bucketing(self):
        # trips whose entries were long ago but exits cluster on one date must all land
        # in that single exit-date bucket (regression: entry-date bucketing is forbidden)
        start = pd.Timestamp("2025-12-01").value // 10**6
        asof = start + 60 * MS_DAY
        exit_day = start + 30 * MS_DAY
        t = pd.DataFrame({"net_bps": [10.0] * 60,
                          "exit_fill_ts_last": [exit_day + 1000] * 60})
        r = wallet_lcb(t, start, asof)
        # all trips share ONE calendar-date bucket: resamples either include that date
        # (mean 10) or exclude it (-inf) -> the 10% LCB collapses to one of those
        assert r["lcb_bps"] in (10.0, -np.inf) or r["lcb_bps"] == pytest.approx(10.0)

    def test_empty(self):
        start = pd.Timestamp("2025-12-01").value // 10**6
        r = wallet_lcb(pd.DataFrame(columns=["net_bps", "exit_fill_ts_last"]),
                       start, start + MS_DAY)
        assert np.isnan(r["lcb_bps"])
