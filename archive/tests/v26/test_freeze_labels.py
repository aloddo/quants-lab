"""FREEZE content-hashing (codex code-gate #7): the validator hashes the ACTUAL
configs.parquet + prune ledger contents (file bytes + canonical content), never trusts
registry_sha.json; --resamples is rejected on full runs. EXPLORATORY labels (codex
code-gate #8): every output artifact carries the label as parquet file-level metadata +
banner column, and as a top-level JSON field."""
import json
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from test_registry import counts
from v26_common import (EXPLORATORY_LABEL, canonical_sha256,
                        stamp_parquet_exploratory, write_exploratory_parquet)
from v26_registry import enumerate_registry, write_registry

REPO = Path(__file__).resolve().parent.parent.parent


def _assert_labeled(path):
    meta = pq.read_schema(path).metadata
    assert meta[b"exploratory"] == b"true"
    assert meta[b"label"] == EXPLORATORY_LABEL.encode()
    df = pd.read_parquet(path)
    assert "exploratory" in df.columns
    if len(df):
        assert df["exploratory"].all()


@pytest.fixture
def freeze_env(tmp_path, monkeypatch):
    import v26_freeze as vf
    data = tmp_path / "v26data"
    registry = enumerate_registry(counts())
    write_registry(registry, data)
    doc = tmp_path / "prereg.md"
    doc.write_text("# frozen grid prereg (test)\n")
    snap = tmp_path / "fee_snapshot.json"
    snap.write_text(json.dumps({"data": {"feeSchedule": {"cross": "0.00045",
                                                         "add": "0.000015"}}}))
    v25fz = tmp_path / "FREEZE.json"
    v25fz.write_text(json.dumps({"prereg_doc_sha256": "x", "git_commit": "y",
                                 "marks_cache_combined_sha256": "z",
                                 "input_manifest": {}}))
    monkeypatch.setattr(vf, "REPO", tmp_path)
    monkeypatch.setattr(vf, "V26_DATA", data)
    monkeypatch.setattr(vf, "GRID_PREREG_DOC", doc)
    monkeypatch.setattr(vf, "FEE_SNAPSHOT_PATH", snap)
    monkeypatch.setattr(vf, "V25_FREEZE_PATH", v25fz)
    monkeypatch.setattr(vf, "git_commit", lambda: "deadbeef")
    monkeypatch.setattr(vf, "_clean", lambda: None)
    freeze_path = tmp_path / "FREEZE-GRID.json"
    return vf, data, freeze_path


class TestFreezeContentHashing:
    def _freeze(self, vf, freeze_path):
        fz = vf.compute_freeze()
        freeze_path.write_text(json.dumps(fz))
        return fz

    def test_valid_freeze_roundtrip(self, freeze_env):
        vf, data, freeze_path = freeze_env
        fz = self._freeze(vf, freeze_path)
        assert "registry_file_sha256" in fz and "prune_ledger_file_sha256" in fz
        out = vf.validate_freeze_grid(freeze_path=freeze_path)
        assert out["registry_sha256"] == fz["registry_sha256"]

    def test_tampered_configs_parquet_fails_despite_registry_sha_json(
            self, freeze_env):
        # THE codex point: registry_sha.json is left untouched (still matches the
        # freeze) but the actual configs.parquet contents changed -- must FAIL
        vf, data, freeze_path = freeze_env
        self._freeze(vf, freeze_path)
        df = pd.read_parquet(data / "configs.parquet")
        df.loc[df[df["status"] == "RUN"].index[0], "status"] = "PRUNED"
        write_exploratory_parquet(df, data / "configs.parquet")
        with pytest.raises(SystemExit, match="sha mismatch"):
            vf.validate_freeze_grid(freeze_path=freeze_path)

    def test_tampered_prune_ledger_fails(self, freeze_env):
        vf, data, freeze_path = freeze_env
        self._freeze(vf, freeze_path)
        led = pd.read_parquet(data / "prune_ledger.parquet")
        write_exploratory_parquet(led.iloc[1:], data / "prune_ledger.parquet")
        with pytest.raises(SystemExit, match="sha mismatch"):
            vf.validate_freeze_grid(freeze_path=freeze_path)

    def test_compute_freeze_cross_checks_meta_against_contents(self, freeze_env):
        vf, data, freeze_path = freeze_env
        meta = json.loads((data / "registry_sha.json").read_text())
        meta["registry_sha256"] = "0" * 64
        (data / "registry_sha.json").write_text(json.dumps(meta))
        with pytest.raises(SystemExit, match="does not match"):
            vf.compute_freeze()

    def test_resamples_override_rejected_on_full_runs(self):
        r = subprocess.run(
            [sys.executable, str(REPO / "research/v26/v26_run_grid.py"),
             "--confirm-grid", "--resamples", "5"],
            capture_output=True, text=True, cwd=REPO)
        assert r.returncode != 0
        assert "--resamples is allowed ONLY under --smoke" in (r.stderr + r.stdout)


class TestExploratoryLabels:
    def test_registry_artifacts_labeled(self, tmp_path):
        registry = enumerate_registry(counts())
        meta = write_registry(registry, tmp_path)
        _assert_labeled(tmp_path / "configs.parquet")
        _assert_labeled(tmp_path / "prune_ledger.parquet")
        # the banner column is presentation, not content: canonical hash unchanged
        assert canonical_sha256(pd.read_parquet(tmp_path / "configs.parquet")) \
            == meta["registry_sha256"] == canonical_sha256(registry)
        assert json.loads((tmp_path / "registry_sha.json").read_text())["exploratory"]

    def test_stamp_existing_parquet(self, tmp_path):
        p = tmp_path / "daily_grid.parquet"
        pd.DataFrame({"a": [1, 2]}).to_parquet(p, index=False)
        stamp_parquet_exploratory(p)
        _assert_labeled(p)

    def test_write_outputs_labels_every_artifact(self, tmp_path):
        from v26_run_grid import write_outputs
        registry = enumerate_registry(counts())
        cid = registry[registry["status"] == "RUN"].iloc[0]["config_id"]
        aggs = [{"config_id": cid, "runtime_failure": False, "undersupplied": True,
                 "tier": {"label": "CONCENTRATED"}, "k_real_min": 3,
                 "mean_excess": 0.001, "adjusted_lcb": -0.001,
                 "adjusted_lcb_14d": -0.002, "estimand_pass": False, "PASS": False,
                 "criteria": {"min_test_days_60": True}, "n_realized": 10,
                 "trips_per_day": 0.5, "maker_fill_rate": float("nan"),
                 "variant_used": "BASE", "worst_pooled": -1.0,
                 "stress_by_seed": {"17": 1.0}}]
        verdict = {"label": EXPLORATORY_LABEL, "method": "joint_maxstat"}
        write_outputs(aggs, registry, verdict, tmp_path)
        for name in ("frontier.parquet", "kill_ledger.parquet", "marginals.parquet"):
            _assert_labeled(tmp_path / name)
        v = json.loads((tmp_path / "verdict_grid.json").read_text())
        assert v["exploratory"] is True and v["label"] == EXPLORATORY_LABEL
        fr = pd.read_parquet(tmp_path / "frontier.parquet")
        assert "EXPLORATORY" in fr.iloc[0]["labels"]
        kl = pd.read_parquet(tmp_path / "kill_ledger.parquet")
        assert (kl["label"] == "EXPLORATORY").all()
