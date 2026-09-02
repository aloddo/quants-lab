from __future__ import annotations

from datetime import date
from types import SimpleNamespace

import pytest

from data_pipeline import hl_s3_fills_daily_refresh as fills
from data_pipeline import hl_s3_misc_daily_refresh as misc


def _fills_args(tmp_path):
    return SimpleNamespace(
        n_workers=1,
        no_candles=False,
        out_dir=str(tmp_path / "fills"),
        candles_out_dir=str(tmp_path / "candles"),
        dry_run=False,
    )


def _misc_args(tmp_path):
    return SimpleNamespace(
        n_workers=1,
        funding_out_dir=str(tmp_path / "funding"),
        ledger_out_dir=str(tmp_path / "ledger"),
        dry_run=False,
    )


def test_fills_partial_day_is_not_written(monkeypatch, tmp_path):
    calls = iter(range(24))

    def fake_fetch(*_args):
        hour = next(calls)
        return [], {}, 0, hour != 23

    monkeypatch.setattr(fills, "fetch_hour", fake_fetch)
    with pytest.raises(RuntimeError, match="23/24"):
        fills.refresh_day(object(), date(2026, 8, 1), {"0xabc"}, _fills_args(tmp_path))

    assert not list(tmp_path.rglob("*.parquet"))


def test_fills_wholly_absent_day_is_explicitly_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(fills, "fetch_hour", lambda *_args: ([], {}, 0, False))
    row = fills.refresh_day(object(), date(2026, 8, 1), {"0xabc"}, _fills_args(tmp_path))
    assert row["status"] == "missing"
    assert row["hours_found"] == 0
    assert not list(tmp_path.rglob("*.parquet"))


def test_misc_single_hour_error_is_not_written(monkeypatch, tmp_path):
    calls = iter(range(24))

    def fake_fetch(*_args):
        hour = next(calls)
        if hour == 23:
            return [], [], False, False, "read timeout"
        return [], [], True, False, None

    monkeypatch.setattr(misc, "fetch_hour", fake_fetch)
    with pytest.raises(RuntimeError, match="1 hour.*failed.*23/24 ok"):
        misc.refresh_day(object(), date(2026, 8, 1), {"0xabc"}, _misc_args(tmp_path))

    assert not list(tmp_path.rglob("*.parquet"))


def test_misc_partly_missing_day_is_not_written(monkeypatch, tmp_path):
    calls = iter(range(24))

    def fake_fetch(*_args):
        hour = next(calls)
        if hour == 23:
            return [], [], False, True, "NoSuchKey"
        return [], [], True, False, None

    monkeypatch.setattr(misc, "fetch_hour", fake_fetch)
    with pytest.raises(RuntimeError, match="23/24"):
        misc.refresh_day(object(), date(2026, 8, 1), {"0xabc"}, _misc_args(tmp_path))

    assert not list(tmp_path.rglob("*.parquet"))


def test_daily_wrapper_preserves_both_stage_rcs_and_exits_nonzero():
    text = (fills.REPO / "scripts" / "hl_s3_fills_daily_refresh.sh").read_text()
    assert "|| fills_rc=$?" in text
    assert "|| misc_rc=$?" in text
    assert 'exit "$pipeline_rc"' in text
    assert "non-zero ($?)" not in text
