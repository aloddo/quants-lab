"""Estimand: zero-exposure exclusion, time weighting (via assembly), Holm p
construction (plus-one, recentered, failure p = 1), K-scaling tiers, joint max-stat
sanity."""
import numpy as np
import pandas as pd

from v25_common import MS_DAY, MS_MIN, MarksIndex
from v26_common import FoldMarks, k_real_tier
from v26_estimand import (daily_excess, holm_adjust, holm_fallback, joint_maxstat,
                          sample_ok)
from v26_fees import FeeEngine
from v26_overlays import TRIP_COLS

T0 = pd.Timestamp("2026-03-01").value // 10**6


class TestDailyExcess:
    def test_zero_exposure_days_excluded(self):
        ex = daily_excess(np.array([5.0, 3.0]), np.array([150.0, 0.0]),
                          np.array([300.0, 0.0]))
        assert np.isnan(ex[1])                         # flat day: excluded, not zero
        assert abs(ex[0] - (5.0 - 0.0010 * 150.0) / 300.0) < 1e-15

    def test_delta_mult_estimand_shift(self):
        # LCB bar delta x1.25 == extra (M-1) x 10bps x admitted / gross per day (D8)
        base = daily_excess(np.array([5.0]), np.array([150.0]), np.array([300.0]))
        shifted = daily_excess(np.array([5.0]), np.array([150.0]), np.array([300.0]),
                               delta_mult=1.25)
        assert abs((base[0] - shifted[0]) - 0.25 * 0.0010 * 150.0 / 300.0) < 1e-15

    def test_min_nonzero_days(self):
        row = np.concatenate([np.full(29, 0.001), np.full(10, np.nan)])
        assert not sample_ok(row)
        assert sample_ok(np.full(30, 0.001))


class TestHolm:
    def test_plus_one_and_recentered(self):
        # constant positive series: every boot mean == hat, null = 0 < hat => count 0
        M = np.full((1, 20), 0.005)
        r = holm_fallback(M, [20], family_size=1, n_resamples=200)
        assert abs(r["p_raw"][0] - 1.0 / 201.0) < 1e-12
        # constant NEGATIVE series: null 0 >= hat always => p = 1 (never significant)
        Mn = np.full((1, 20), -0.005)
        rn = holm_fallback(Mn, [20], family_size=1, n_resamples=200)
        assert rn["p_raw"][0] == 1.0

    def test_holm_adjust_step_down_and_family_never_shrinks(self):
        adj = holm_adjust(np.array([0.01, 0.04]), family_size=4)
        assert abs(adj[0] - 0.04) < 1e-12              # (4-0) x 0.01
        assert abs(adj[1] - 0.12) < 1e-12              # max(0.04, 3 x 0.04)
        # runtime failures enter the family with p = 1: family_size > len(p)
        adj2 = holm_adjust(np.array([0.005]), family_size=10)
        assert abs(adj2[0] - 0.05) < 1e-12

    def test_failure_p_is_one_capped(self):
        adj = holm_adjust(np.array([1.0, 0.2]), family_size=2)
        assert adj[0] == 1.0


class TestJointMaxstat:
    def test_shared_draw_lcb_below_mean_and_deterministic(self):
        rng = np.random.default_rng(0)
        M = rng.normal(0.001, 0.01, size=(3, 42))
        a = joint_maxstat(M, [21, 21], n_resamples=400)
        b = joint_maxstat(M, [21, 21], n_resamples=400)
        assert np.allclose(a["lcb"], b["lcb"])          # seeded, deterministic
        assert (a["lcb"] <= a["mean"] + 1e-15).all()
        assert a["c_maxstat"] > 0

    def test_masked_days_do_not_poison(self):
        M = np.full((2, 21), 0.002)
        M[1, :10] = np.nan                              # config 2: 10 excluded days
        r = joint_maxstat(M, [21], n_resamples=200)
        assert np.isfinite(r["lcb"]).all()


class TestKScaling:
    def test_tiers(self):
        full = k_real_tier(15)
        assert full["entity_min"] == 15 and full["trips_min"] == 150
        assert full["delta_mult"] == 1.0 and full["entity_caps"] and full["coin_caps"]
        mid = k_real_tier(7)
        assert mid["entity_min"] == 7 and mid["delta_mult"] == 1.25
        assert not mid["entity_caps"] and mid["coin_caps"]      # coin caps KEPT
        conc = k_real_tier(3)
        assert conc["trips_min"] == 300 and conc["delta_mult"] == 1.5
        assert not conc["coin_caps"] and conc["label"] == "CONCENTRATED"


class TestAssemblyTimeWeighting:
    def test_time_weighted_gross_and_flat_day_excluded(self, marks_dir,
                                                       zero_fee_snapshot):
        from v26_run_grid import assemble_config_fold
        n_min = 2 * 1440
        marks_dir("BTC", [T0 + i * MS_MIN for i in range(n_min)], [100.0] * n_min)
        fm = FoldMarks(MarksIndex(cache_dir=marks_dir.dir), T0, T0 + 2 * MS_DAY)
        trip = dict(zip(TRIP_COLS, [
            "0xw", "BTC", 1, 1, 1000.0, T0, T0 + MS_MIN, 100.0, False,
            "MIRROR", T0 + 12 * 60 * MS_MIN, T0 + 12 * 60 * MS_MIN + MS_MIN, 100.0,
            False, False, False, "ok", "", float(T0 + 12 * 60 * MS_MIN)]))
        trips = pd.DataFrame([trip])
        fee = FeeEngine(zero_fee_snapshot, "BASE")
        r = assemble_config_fold(trips, fm, fee, 2.5, "150", {})
        # held slots 1..720 of day 0 => time-weighted avg gross = 150 x 720/1440 = 75
        assert abs(r["avg_gross"][0] - 75.0) < 1e-9
        assert r["avg_gross"][1] == 0.0                 # day 1 flat
        ex = daily_excess(r["daily_pnl"], r["admitted"], r["avg_gross"])
        assert np.isfinite(ex[0]) and np.isnan(ex[1])   # zero-exposure day excluded
        assert r["admitted"][0] == 150.0 and r["n_realized"] == 1
        assert abs(r["total_pnl"]) < 1e-9               # flat px, zero fees

    def test_coalescing_and_counters(self, marks_dir, zero_fee_snapshot):
        from v26_run_grid import assemble_config_fold
        n_min = 1440
        marks_dir("BTC", [T0 + i * MS_MIN for i in range(n_min)], [100.0] * n_min)
        fm = FoldMarks(MarksIndex(cache_dir=marks_dir.dir), T0, T0 + MS_DAY)
        mk = lambda jid, sig: dict(zip(TRIP_COLS, [
            "0xw", "BTC", jid, 1, 1000.0, sig, sig + MS_MIN, 100.0, False,
            "MIRROR", sig + 60 * MS_MIN, sig + 61 * MS_MIN, 100.0,
            False, False, False, "ok", "", float(sig + 60 * MS_MIN)]))
        trips = pd.DataFrame([mk(1, T0), mk(2, T0 + 10 * MS_MIN)])   # overlapping
        fee = FeeEngine(zero_fee_snapshot, "BASE")
        r = assemble_config_fold(trips, fm, fee, 2.5, "150", {})
        assert r["counters"]["dup_coalesced"] == 1      # one open lot per (wallet,coin)
        assert r["counters"]["entries"] == 1


class TestExitFillClock:
    """codex code-gate #4: the estimand/DD/gross clock runs to the DELAYED EXIT FILL,
    not the exit signal (v25 gate-b r3 regression pattern adapted to the minute grid)."""

    def _run(self, marks_dir, zero_fee_snapshot, exit_signal_min, exit_fill_min,
             n_days=1, exit_px=98.0):
        from v26_run_grid import assemble_config_fold
        n_min = n_days * 1440
        marks_dir("BTC", [T0 + i * MS_MIN for i in range(n_min)], [100.0] * n_min)
        fm = FoldMarks(MarksIndex(cache_dir=marks_dir.dir), T0, T0 + n_days * MS_DAY)
        trip = dict(zip(TRIP_COLS, [
            "0xw", "BTC", 1, 1, 1000.0, T0, T0 + MS_MIN, 100.0, False,
            "MIRROR", T0 + exit_signal_min * MS_MIN, T0 + exit_fill_min * MS_MIN,
            exit_px, False, False, False, "ok", "",
            float(T0 + exit_signal_min * MS_MIN)]))
        fee = FeeEngine(zero_fee_snapshot, "BASE")
        return assemble_config_fold(pd.DataFrame([trip]), fm, fee, 2.5, "150", {})

    def test_gross_exposure_remains_until_exit_fill(self, marks_dir,
                                                    zero_fee_snapshot):
        # exit signal at minute 10, delayed fill at minute 12: the position must stay
        # in the gross/unreal series through minute 11 (slots 1..11 = 11 held slots),
        # not stop at the signal
        r = self._run(marks_dir, zero_fee_snapshot, 10, 12)
        assert abs(r["avg_gross"][0] - 150.0 * 11 / 1440) < 1e-9
        assert abs(r["total_pnl"] - 150.0 * (98.0 / 100.0 - 1.0)) < 1e-9

    def test_realized_pnl_lands_on_fill_day(self, marks_dir, zero_fee_snapshot):
        # signal on day 0 (minute 1438), fill on day 1 (minute 1441): the realized
        # PnL must land in day 1's attribution; day 0 ends flat-marked (unreal 0)
        r = self._run(marks_dir, zero_fee_snapshot, 1438, 1441, n_days=2)
        assert abs(r["daily_pnl"][0]) < 1e-9
        assert abs(r["daily_pnl"][1] - 150.0 * (98.0 / 100.0 - 1.0)) < 1e-9
        # exposure spans the day boundary through the fill
        assert r["avg_gross"][1] > 0.0

    def test_terminal_row_unchanged(self, marks_dir, zero_fee_snapshot):
        # TERMINAL rows keep the window-end clock (fill == end)
        from v26_run_grid import assemble_config_fold
        n_min = 1440
        marks_dir("BTC", [T0 + i * MS_MIN for i in range(n_min)], [100.0] * n_min)
        fm = FoldMarks(MarksIndex(cache_dir=marks_dir.dir), T0, T0 + MS_DAY)
        end = T0 + MS_DAY
        trip = dict(zip(TRIP_COLS, [
            "0xw", "BTC", 1, 1, 1000.0, T0, T0 + MS_MIN, 100.0, False,
            "TERMINAL", end, end, 100.0, False, False, False, "ok", "",
            float("nan")]))
        fee = FeeEngine(zero_fee_snapshot, "BASE")
        r = assemble_config_fold(pd.DataFrame([trip]), fm, fee, 2.5, "150", {})
        assert r["n_terminal"] == 1 and r["n_realized"] == 0
        assert abs(r["total_pnl"]) < 1e-9
