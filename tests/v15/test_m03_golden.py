"""GOLDEN regression test for v15_m03_fold_geometry (trust audit 2026-07-10).

Locks the fold calendar geometry + the audit fixes so the output cannot silently drift:
- exact 8-fold walk-forward calendar (dates),
- test windows contiguous + non-overlapping + half-open,
- pretest excludes test (look-ahead-safe),
- the misleading active_folds_for_g5 alias is DROPPED (codex ruling); active_pretest_folds is the safe field,
- ms-magnitude guard rejects a seconds-epoch column,
- is_full_test_fold reflects real archive coverage (not hardcoded True).
"""
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m03_fold_geometry as M


def test_golden_fold_calendar():
    folds = M.build_folds(start=date(2025, 12, 1), n_folds=8)
    assert len(folds) == 8
    # exact geometry, fold 1
    assert (folds[0].train_start, folds[0].train_end_excl) == (date(2025, 12, 1), date(2026, 1, 12))
    assert (folds[0].val_start, folds[0].val_end_excl) == (date(2026, 1, 12), date(2026, 1, 26))
    assert (folds[0].test_start, folds[0].test_end_excl) == (date(2026, 1, 26), date(2026, 2, 9))
    # pretest = [train_start, test_start) excludes test
    assert (folds[0].pretest_start, folds[0].pretest_end_excl) == (date(2025, 12, 1), date(2026, 1, 26))
    # last fold
    assert (folds[7].test_start, folds[7].test_end_excl) == (date(2026, 5, 4), date(2026, 5, 18))


def test_test_windows_contiguous_nonoverlapping():
    folds = M.build_folds()
    for i in range(len(folds) - 1):
        assert folds[i].test_end_excl == folds[i + 1].test_start  # contiguous, no gap/overlap
        assert folds[i].test_start < folds[i].test_end_excl       # half-open, non-empty


def test_pretest_excludes_test_no_lookahead():
    for f in M.build_folds():
        assert f.pretest_end_excl == f.test_start  # selection window ends exactly at test start


def test_n_folds_guard():
    with pytest.raises(ValueError):
        M.build_folds(n_folds=0)


def test_is_full_test_fold_reflects_coverage():
    folds = M.build_folds()
    # archive ends before the last fold's test -> that fold must be marked NOT full
    df = M.folds_to_frame(folds, archive_end_excl=date(2026, 5, 10))
    assert df.iloc[7]["is_full_test_fold"] == False  # fold8 test_end 2026-05-18 > 2026-05-10
    # full coverage -> all True
    df2 = M.folds_to_frame(folds, archive_end_excl=date(2026, 6, 1))
    assert df2["is_full_test_fold"].all()


def test_ms_magnitude_guard_rejects_seconds():
    folds = M.build_folds()
    # seconds-epoch (2026-01-27) — would silently become 1970 if parsed as ms
    sec = 1769472000
    actions = pd.DataFrame({"wallet": ["0xa"], "ts": [sec]})
    journeys = pd.DataFrame({"wallet": ["0xa"], "entry_ts": [sec]})
    with pytest.raises(ValueError, match="epoch-ms"):
        M.build_activity(folds, actions, journeys)


def test_g5_lookahead_alias_dropped():
    """AUDIT (codex ruling b): the misleading active_folds_for_g5 alias (held look-ahead active_test_folds) is
    DROPPED. The enforced G5 lives in m06b on active_pretest_folds. m03 still emits active_test_folds (diagnostic)
    and active_pretest_folds (look-ahead-safe), but no "for_g5" footgun."""
    folds = M.build_folds()
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    t_in_last_test = ms(date(2026, 5, 10))  # fold-8 test, after every pretest window
    actions = pd.DataFrame({"wallet": ["0xa", "0xa"], "ts": [t_in_last_test, t_in_last_test + 1]})
    journeys = pd.DataFrame({"wallet": ["0xa"], "entry_ts": [t_in_last_test]})
    _, summary = M.build_activity(folds, actions, journeys)
    row = summary[summary["key"] == "0xa"].iloc[0]
    assert "active_folds_for_g5" not in summary.columns  # footgun gone
    assert row["active_test_folds"] >= 1                 # diagnostic: was active in a test window
    assert row["active_pretest_folds"] == 0              # look-ahead-safe field: not active in any pretest
