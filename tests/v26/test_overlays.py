"""E2/E3 exit state machines: stop dominates, trail activation + 0.70 giveback,
priority order at the same mark, no discretionary hold (max_hold always fires),
leader mirror still fires above the stop. Synthetic marks only."""
import numpy as np
import pandas as pd
import pytest

from v25_common import MS_DAY, MS_MIN, MarksIndex
from v26_common import FoldMarks, MAX_HOLD_MS
from v26_overlays import extract_candidate_journeys, overlay_exit_plan

T0 = pd.Timestamp("2026-03-01").value // 10**6


def fm_for(marks_dir, closes, n_days=2, coin="BTC"):
    minutes = [T0 + i * MS_MIN for i in range(len(closes))]
    marks_dir(coin, minutes, closes)
    m = MarksIndex(cache_dir=marks_dir.dir)
    return FoldMarks(m, T0, T0 + n_days * MS_DAY)


def plan(fm, exit_style, mirror_ts=float("nan"), side=1, entry_px=100.0,
         entry_fee=0.0, exit_cost=0.0, entry_fill=T0 + MS_MIN):
    return overlay_exit_plan(fm, "BTC", side, entry_fill, entry_px, entry_fee,
                             exit_cost, mirror_ts, exit_style)


class TestE2:
    def test_stop_fires_at_first_crossing_mark(self, marks_dir):
        # closes at T0+1m..T0+5m: 100,100,98.9,100,100 -> cum -1.1% at T0+3m
        fm = fm_for(marks_dir, [100, 100, 98.9, 100, 100])
        assert plan(fm, "E2") == ("STOP", T0 + 3 * MS_MIN)

    def test_stop_dominates_mirror_at_same_mark(self, marks_dir):
        fm = fm_for(marks_dir, [100, 100, 98.9, 100, 100])
        assert plan(fm, "E2", mirror_ts=T0 + 3 * MS_MIN) == ("STOP", T0 + 3 * MS_MIN)

    def test_mirror_still_fires_above_stop(self, marks_dir):
        # never below -1%: the leader FIRST_CLOSE mirror stays active (winner path)
        fm = fm_for(marks_dir, [100, 100, 99.5, 99.8, 100.2])
        assert plan(fm, "E2", mirror_ts=T0 + 5 * MS_MIN) == ("MIRROR", T0 + 5 * MS_MIN)

    def test_mirror_before_stop_wins(self, marks_dir):
        fm = fm_for(marks_dir, [100, 100, 100, 98.9, 100])   # stop would be T0+4m
        assert plan(fm, "E2", mirror_ts=T0 + 2 * MS_MIN) == ("MIRROR", T0 + 2 * MS_MIN)

    def test_accrued_exit_cost_tightens_stop(self, marks_dir):
        # -0.7% price move + 0.35% accrued exit cost crosses -1% only with the cost
        fm = fm_for(marks_dir, [100, 100, 99.3, 100, 100])
        assert plan(fm, "E2")[0] == "TERMINAL"                # without costs: no stop
        assert plan(fm, "E2", exit_cost=0.0035) == ("STOP", T0 + 3 * MS_MIN)


class TestE3:
    CLOSES = [100, 100, 100, 103, 104, 102.9, 102.7, 102.7]

    def test_trail_activation_and_giveback(self, marks_dir):
        # +3R at T0+4m (103), peak 4% at T0+5m, trail exits at cum <= 0.70 x peak
        # = 2.8%: 102.9 (2.9%) holds, 102.7 (2.7%) exits
        fm = fm_for(marks_dir, self.CLOSES)
        assert plan(fm, "E3") == ("TRAIL", T0 + 7 * MS_MIN)

    def test_trail_replaces_mirror_after_activation(self, marks_dir):
        fm = fm_for(marks_dir, self.CLOSES)
        # leader closes AFTER activation (T0+4m): mirror is REPLACED, trail decides
        assert plan(fm, "E3", mirror_ts=T0 + 4 * MS_MIN + 30_000) == \
            ("TRAIL", T0 + 7 * MS_MIN)

    def test_mirror_before_activation_wins(self, marks_dir):
        fm = fm_for(marks_dir, self.CLOSES)
        assert plan(fm, "E3", mirror_ts=T0 + 3 * MS_MIN) == ("MIRROR", T0 + 3 * MS_MIN)

    def test_stop_beats_trail_at_same_mark(self, marks_dir):
        # post-activation gap to 98 crosses BOTH the trail and the -1R stop: STOP wins
        fm = fm_for(marks_dir, [100, 100, 100, 103, 98, 98])
        assert plan(fm, "E3") == ("STOP", T0 + 5 * MS_MIN)

    def test_pre_activation_stop(self, marks_dir):
        fm = fm_for(marks_dir, [100, 100, 98.9, 103, 104])
        assert plan(fm, "E3") == ("STOP", T0 + 3 * MS_MIN)


class TestMaxHoldAndTerminal:
    def test_no_discretionary_hold_maxhold_fires(self, marks_dir):
        # no mirror, no trigger: forced exit at the first mark >= entry_fill + 7d
        fm = fm_for(marks_dir, [100.0] * 10, n_days=9)
        due = T0 + MS_MIN + MAX_HOLD_MS
        for style in ("E1", "E2", "E3"):
            assert plan(fm, style) == ("MAXHOLD", due)

    def test_mirror_beats_maxhold_at_same_mark(self, marks_dir):
        fm = fm_for(marks_dir, [100.0] * 10, n_days=9)
        due = T0 + MS_MIN + MAX_HOLD_MS
        assert plan(fm, "E1", mirror_ts=due) == ("MIRROR", due)

    def test_terminal_when_window_shorter_than_maxhold(self, marks_dir):
        fm = fm_for(marks_dir, [100.0] * 10, n_days=2)
        for style in ("E1", "E2", "E3"):
            assert plan(fm, style) == ("TERMINAL", T0 + 2 * MS_DAY)


class TestCandidateExtraction:
    def test_first_close_mirror_and_cold_start(self):
        a = pd.DataFrame([
            # pre-window entry: never copied (cold start)
            ("0xw", "ETH", T0 - MS_DAY, "ENTRY", 1.0, 100.0, 1.0, 1, np.nan, False),
            ("0xw", "ETH", T0 + MS_MIN, "EXIT", -1.0, 100.0, 0.0, 1, np.nan, False),
            # in-window journey: entry, addon grows denominator, trims cross 85%
            ("0xw", "BTC", T0 + MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 2, np.nan, False),
            ("0xw", "BTC", T0 + 2 * MS_MIN, "ADDON", 1.0, 100.0, 2.0, 2, np.nan, False),
            ("0xw", "BTC", T0 + 3 * MS_MIN, "TRIM", -1.0, 100.0, 1.0, 2, np.nan, False),
            ("0xw", "BTC", T0 + 4 * MS_MIN, "TRIM", -0.8, 100.0, 0.2, 2, np.nan, False),
        ], columns=["wallet", "coin", "ts", "action_type", "signed_size", "price",
                    "position_after", "journey_id", "closing_journey_id",
                    "is_liquidation"])
        c = extract_candidate_journeys(a, T0, T0 + MS_DAY)
        assert len(c) == 1                       # ETH pre-window journey not copied
        r = c.iloc[0]
        assert r["coin"] == "BTC" and r["journey_id"] == 2
        # cumulative reverse 180 / accumulated 200 = 90% >= 85% at the SECOND trim
        assert r["mirror_ts"] == T0 + 4 * MS_MIN

    def test_reverse_closes_never_opens(self):
        a = pd.DataFrame([
            ("0xw", "BTC", T0 + MS_MIN, "ENTRY", 1.0, 100.0, 1.0, 3, np.nan, False),
            ("0xw", "BTC", T0 + 5 * MS_MIN, "REVERSE", -2.0, 100.0, -1.0, 4, 3.0, False),
        ], columns=["wallet", "coin", "ts", "action_type", "signed_size", "price",
                    "position_after", "journey_id", "closing_journey_id",
                    "is_liquidation"])
        c = extract_candidate_journeys(a, T0, T0 + MS_DAY)
        assert len(c) == 1 and c.iloc[0]["journey_id"] == 3
        assert c.iloc[0]["mirror_ts"] == T0 + 5 * MS_MIN
