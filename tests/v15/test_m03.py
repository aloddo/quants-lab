"""V15 M3 fold-geometry tests. Asserts the codex-SHIP design contract (modules/m3-design)."""
import sys
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v15"))
import v15_m03_fold_geometry as m03  # noqa: E402


@pytest.fixture
def folds():
    return m03.build_folds()


# === I1: 3-way ordering + exact window lengths ===
def test_i1_ordering_and_lengths(folds):
    assert len(folds) == 8
    for f in folds:
        assert f.train_start < f.val_start < f.test_start
        assert (f.train_end_excl - f.train_start).days == 42
        assert (f.val_end_excl - f.val_start).days == 14
        assert (f.test_end_excl - f.test_start).days == 14
        # half-open contiguity within a fold
        assert f.train_end_excl == f.val_start
        assert f.val_end_excl == f.test_start
        # pretest = [train_start, test_start)
        assert f.pretest_start == f.train_start
        assert f.pretest_end_excl == f.test_start


# === I2: test windows contiguous + non-overlapping ===
def test_i2_test_windows_contiguous(folds):
    for i in range(len(folds) - 1):
        assert folds[i].test_end_excl == folds[i + 1].test_start


# === I3: chained OOS span == 112d AND exact endpoints ===
def test_i3_chained_oos_exact(folds):
    assert folds[0].test_start == date(2026, 1, 26)
    assert folds[-1].test_end_excl == date(2026, 5, 18)
    span = (folds[-1].test_end_excl - folds[0].test_start).days
    assert span == 112
    assert [f.oos_chain_order if hasattr(f, "oos_chain_order") else f.fold_id for f in folds] == list(range(1, 9))


def test_exact_calendar_dates(folds):
    expected = [
        (date(2025, 12, 1), date(2026, 1, 12), date(2026, 1, 26), date(2026, 2, 9)),
        (date(2025, 12, 15), date(2026, 1, 26), date(2026, 2, 9), date(2026, 2, 23)),
        (date(2025, 12, 29), date(2026, 2, 9), date(2026, 2, 23), date(2026, 3, 9)),
        (date(2026, 1, 12), date(2026, 2, 23), date(2026, 3, 9), date(2026, 3, 23)),
        (date(2026, 1, 26), date(2026, 3, 9), date(2026, 3, 23), date(2026, 4, 6)),
        (date(2026, 2, 9), date(2026, 3, 23), date(2026, 4, 6), date(2026, 4, 20)),
        (date(2026, 2, 23), date(2026, 4, 6), date(2026, 4, 20), date(2026, 5, 4)),
        (date(2026, 3, 9), date(2026, 4, 20), date(2026, 5, 4), date(2026, 5, 18)),
    ]
    for f, (ts, vs, tst, te) in zip(folds, expected):
        assert (f.train_start, f.val_start, f.test_start, f.test_end_excl) == (ts, vs, tst, te)


# === I6: folds frame carries no pnl/eligibility/ranking field ===
def test_i6_no_forbidden_fields():
    df = m03.folds_to_frame(m03.build_folds(), market_data_fn=None)
    forbidden = {"pnl", "roe", "score", "rank", "eligible", "profit", "net_pnl"}
    for col in df.columns:
        assert not any(bad in col.lower() for bad in forbidden), col
    # regime is reporting-only and UNKNOWN with no market data
    assert set(df["btc_trend_bucket"]) == {"UNKNOWN"}


# === Activity pass on a synthetic dataset ===
def _synthetic():
    """One wallet active (1 journey opened) in F1.test and F3.test only -> G5 active=2."""
    folds = m03.build_folds()
    f1, f3 = folds[0], folds[2]

    def mid_ms(d):
        return m03._ms(d) + 12 * 3600 * 1000  # midday of the window start

    actions = pd.DataFrame(
        [
            {"wallet": "0xa", "ts": mid_ms(f1.test_start), "action_type": "ENTRY"},
            {"wallet": "0xa", "ts": mid_ms(f3.test_start), "action_type": "ENTRY"},
            {"wallet": "0xb", "ts": mid_ms(folds[0].train_start), "action_type": "ENTRY"},
        ]
    )
    journeys = pd.DataFrame(
        [
            {"wallet": "0xa", "entry_ts": mid_ms(f1.test_start), "exit_ts": mid_ms(f1.test_start) + 3600000},
            {"wallet": "0xa", "entry_ts": mid_ms(f3.test_start), "exit_ts": np.nan},  # open journey
            {"wallet": "0xb", "entry_ts": mid_ms(folds[0].train_start), "exit_ts": mid_ms(folds[0].train_start) + 3600000},
        ]
    )
    return folds, actions, journeys


def test_activity_g5_counts_open_journeys():
    folds, actions, journeys = _synthetic()
    wide, summary = m03.build_activity(folds, actions, journeys)
    a = summary.set_index("key")
    # 0xa: active in test of F1 and F3 (the open journey in F3 still counts) -> 2
    assert a.loc["0xa", "active_test_folds"] == 2
    assert a.loc["0xa", "active_folds_for_g5"] == 2
    # 0xb: only active in F1 train -> 0 test folds
    assert a.loc["0xb", "active_test_folds"] == 0
    assert a.loc["0xb", "active_train_folds"] >= 1


def test_i5_g5_equals_test_folds():
    folds, actions, journeys = _synthetic()
    _, summary = m03.build_activity(folds, actions, journeys)
    assert (summary["active_folds_for_g5"] == summary["active_test_folds"]).all()


def test_i4_half_open_no_boundary_leak():
    """A journey opened exactly at test_end_excl belongs to the NEXT window, not this test."""
    folds = m03.build_folds()
    f1 = folds[0]
    boundary_ms = m03._ms(f1.test_end_excl)  # == f2.test_start
    actions = pd.DataFrame([{"wallet": "0xc", "ts": boundary_ms, "action_type": "ENTRY"}])
    journeys = pd.DataFrame([{"wallet": "0xc", "entry_ts": boundary_ms, "exit_ts": np.nan}])
    wide, _ = m03.build_activity(folds, actions, journeys)
    f1row = wide[(wide["key"] == "0xc") & (wide["fold_id"] == 1)].iloc[0]
    f2row = wide[(wide["key"] == "0xc") & (wide["fold_id"] == 2)].iloc[0]
    assert not f1row["active_test"]  # boundary is exclusive end of F1 test
    assert f2row["active_test"]      # and inclusive start of F2 test


def test_pretest_is_train_plus_val():
    folds, actions, journeys = _synthetic()
    wide, _ = m03.build_activity(folds, actions, journeys)
    # 0xb active in F1 train -> active_pretest True for F1
    f1b = wide[(wide["key"] == "0xb") & (wide["fold_id"] == 1)].iloc[0]
    assert f1b["active_pretest"]
    # pretest never includes test: 0xa active in F1 test but NOT F1 train/val -> pretest False
    f1a = wide[(wide["key"] == "0xa") & (wide["fold_id"] == 1)].iloc[0]
    assert f1a["active_test"] and not f1a["active_pretest"]


def test_activity_excludes_unreplayable_actions_and_journeys():
    folds = m03.build_folds()
    ts = m03._ms(folds[0].test_start) + 1
    actions = pd.DataFrame([
        {"wallet": "0xgood", "ts": ts, "stream_replay_valid": True},
        {"wallet": "0xbad", "ts": ts, "stream_replay_valid": False},
    ])
    journeys = pd.DataFrame([
        {"wallet": "0xgood", "entry_ts": ts, "lifecycle_valid": True,
         "stream_replay_valid": True},
        {"wallet": "0xbad", "entry_ts": ts, "lifecycle_valid": True,
         "stream_replay_valid": False},
    ])
    _, summary = m03.build_activity(folds, actions, journeys)
    got = summary.set_index("key")
    assert got.loc["0xgood", "active_test_folds"] == 1
    assert "0xbad" not in got.index
