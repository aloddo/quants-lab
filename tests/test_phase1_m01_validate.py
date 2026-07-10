from pathlib import Path

import pandas as pd

from research.phase1_m01_validate import validate


def _write_fixture(tmp_path: Path, quarantined: bool = False) -> tuple[Path, Path]:
    series = tmp_path / "series.parquet"
    audit = tmp_path / "series.audit.parquet"
    pd.DataFrame(
        {
            "wallet": ["0x1", "0x1"],
            "date": pd.to_datetime(["2026-01-01", "2026-01-02"]),
            "equity_usd": [100.0, 101.0],
            "position_value_usd": [-10.0, 5.0],
            "gross_position_notional_usd": [20.0, 5.0],
            "recon_incomplete": [False, False],
        }
    ).to_parquet(series, index=False)
    pd.DataFrame(
        {
            "wallet": ["0x1"],
            "quarantined": [quarantined],
            "unknown_ledger_types": [[]],
            "median_inter_anchor_drift_pct": [0.0],
            "max_inter_anchor_drift_pct": [0.0],
            "frac_incomplete_rows": [0.0],
            "n_incomplete_rows": [0],
            "n_inter_anchor_checks": [2],
        }
    ).to_parquet(audit, index=False)
    return series, audit


def test_m01_validator_accepts_consistent_pair(tmp_path: Path) -> None:
    series, audit = _write_fixture(tmp_path)
    report = validate(series, audit)
    assert report["hard_fail"] is False
    assert not any(report["failures"].values())


def test_m01_validator_detects_quarantine_rule_mismatch(tmp_path: Path) -> None:
    series, audit = _write_fixture(tmp_path, quarantined=True)
    report = validate(series, audit)
    assert report["hard_fail"] is True
    assert report["failures"]["quarantine_rule_mismatches"] == 1


def test_m01_validator_requires_two_inter_anchor_checks(tmp_path: Path) -> None:
    series, audit = _write_fixture(tmp_path)
    d = pd.read_parquet(audit)
    d["n_inter_anchor_checks"] = 1
    d.to_parquet(audit, index=False)
    report = validate(series, audit)
    assert report["hard_fail"] is True
    assert report["failures"]["quarantine_rule_mismatches"] == 1
