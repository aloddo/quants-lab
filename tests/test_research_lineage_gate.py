import hashlib
import importlib.util
import json
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "tools" / "research_lineage_gate.py"
SPEC = importlib.util.spec_from_file_location("research_lineage_gate", MODULE_PATH)
gate = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(gate)


def _write_json(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, sort_keys=True), encoding="utf-8")


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def valid_fixture(repo: Path) -> Path:
    artifacts = repo / "run"
    files = {
        "experiment_manifest": ("experiment.yml", "name: test\n"),
        "provenance": (
            "provenance.json",
            {"status": "complete", "completed_utc": "2026-08-03T00:00:00Z", "git_dirty": False,
             "outputs": {"result": {"sha256": "abc"}}},
        ),
        "registry_receipt": ("receipt.json", {}),
        "roster_source": ("roster.json", {"wallets": ["0xabc"]}),
        "m06b_manifest": (
            "m06b.json",
            {"investable": True, "non_investable_reasons": [], "slippage_calibration_version": "v1"},
        ),
        "m08_result": ("m08.json", {"pass": True}),
        "m09_result": (
            "m09.json",
            {"status": "complete", "n_folds": 8, "chained_roe": 0.2,
             "max_chained_dd": 0.1, "chained_calmar": 2.0},
        ),
        "m10_verdict": ("m10.json", {"all_pass": True, "greenlight": "live_small"}),
        "live_replay_parity": ("parity.json", {"status": "complete", "pass": True}),
    }
    artifact_manifest = {}
    for name, (filename, body) in files.items():
        path = artifacts / filename
        if isinstance(body, str):
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(body, encoding="utf-8")
        else:
            _write_json(path, body)
        artifact_manifest[name] = {"path": str(path.relative_to(repo)), "sha256": _sha(path)}

    provenance = artifacts / "provenance.json"
    receipt = artifacts / "receipt.json"
    _write_json(receipt, {"event": "complete", "event_hash": "b" * 64,
                          "provenance_sha256": _sha(provenance)})
    artifact_manifest["registry_receipt"]["sha256"] = _sha(receipt)

    deployment = artifacts / "deployment_manifest.json"
    _write_json(
        deployment,
        {
            "schema_version": gate.SCHEMA_VERSION,
            "status": "complete",
            "deployable": True,
            "git_dirty": False,
            "required_gates": {name: True for name in gate.REQUIRED_GATES},
            "selected_wallets": ["0xAbC"],
            "artifacts": artifact_manifest,
        },
    )
    config = repo / "config.json"
    _write_json(
        config,
        {"global": {"research_lineage": {
            "deployment_manifest": str(deployment.relative_to(repo)),
            "deployment_manifest_sha256": _sha(deployment),
        }}, "wallets": {"0xabc": {}}},
    )
    return config


def test_valid_lineage_passes(tmp_path):
    config = valid_fixture(tmp_path)
    result, rc = gate.verify(config, tmp_path)
    assert rc == 0
    assert result["ok"] is True
    assert len(result["lineage_digest"]) == 64


def test_artifact_mutation_fails(tmp_path):
    config = valid_fixture(tmp_path)
    (tmp_path / "run" / "m09.json").write_text("{}", encoding="utf-8")
    result, rc = gate.verify(config, tmp_path)
    assert rc == 1
    assert any("artifact.m09_result: SHA-256 mismatch" in e for e in result["errors"])


def test_noninvestable_m06b_fails_even_when_manifest_claims_pass(tmp_path):
    config = valid_fixture(tmp_path)
    m06b = tmp_path / "run" / "m06b.json"
    _write_json(m06b, {"investable": False, "non_investable_reasons": ["no_slippage_calibration_version"]})
    deployment = tmp_path / "run" / "deployment_manifest.json"
    data = json.loads(deployment.read_text())
    data["artifacts"]["m06b_manifest"]["sha256"] = _sha(m06b)
    _write_json(deployment, data)
    cfg = json.loads(config.read_text())
    cfg["global"]["research_lineage"]["deployment_manifest_sha256"] = _sha(deployment)
    _write_json(config, cfg)
    result, rc = gate.verify(config, tmp_path)
    assert rc == 1
    assert "artifact.m06b_manifest: M6b is non-investable" in result["errors"]


def test_roster_wallets_must_match_lineage(tmp_path):
    config = valid_fixture(tmp_path)
    cfg = json.loads(config.read_text())
    cfg["wallets"]["0xdef"] = {}
    _write_json(config, cfg)
    result, rc = gate.verify(config, tmp_path)
    assert rc == 1
    assert "deployment_manifest: selected_wallets != live config wallets" in result["errors"]


def test_missing_lineage_fails(tmp_path):
    config = tmp_path / "config.json"
    _write_json(config, {"global": {}, "wallets": {"0xabc": {}}})
    result, rc = gate.verify(config, tmp_path)
    assert rc == 1
    assert result["errors"] == ["global.research_lineage is required"]
