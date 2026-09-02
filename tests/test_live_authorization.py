import hashlib
import importlib.util
import json
import sys
from pathlib import Path


TOOLS = Path(__file__).parents[1] / "tools"
sys.path.insert(0, str(TOOLS))
SPEC = importlib.util.spec_from_file_location("live_authorization", TOOLS / "live_authorization.py")
auth = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(auth)


def test_missing_arm_fails_before_live(tmp_path, monkeypatch):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"global": {}, "wallets": {}}))
    monkeypatch.setattr(auth, "verify_lineage", lambda *_: ({"lineage_digest": "x"}, 1))
    result = auth.verify_live_authorization(config, tmp_path, tmp_path / "pause")
    assert result["ok"] is False
    assert ".ARM_COPY missing" in result["errors"]


def test_matching_arm_and_lineage_pass(tmp_path, monkeypatch):
    config = tmp_path / "config.json"
    config.write_text(json.dumps({"global": {}, "wallets": {}}))
    digest = "d" * 64
    monkeypatch.setattr(auth, "verify_lineage", lambda *_: ({"lineage_digest": digest}, 0))
    sha = hashlib.sha256(config.read_bytes()).hexdigest()
    (tmp_path / ".ARM_COPY").write_text(
        f"config=config.json\nconfig_sha256={sha}\nlineage_rc=0\nlineage_digest={digest}\n")
    result = auth.verify_live_authorization(config, tmp_path, tmp_path / "pause")
    assert result["ok"] is True


def test_config_mutation_invalidates_arm(tmp_path, monkeypatch):
    config = tmp_path / "config.json"
    config.write_text("{}")
    old_sha = hashlib.sha256(config.read_bytes()).hexdigest()
    digest = "d" * 64
    (tmp_path / ".ARM_COPY").write_text(
        f"config=config.json\nconfig_sha256={old_sha}\nlineage_rc=0\nlineage_digest={digest}\n")
    config.write_text('{"changed": true}')
    monkeypatch.setattr(auth, "verify_lineage", lambda *_: ({"lineage_digest": digest}, 0))
    result = auth.verify_live_authorization(config, tmp_path, tmp_path / "pause")
    assert "config bytes differ from armed config hash" in result["errors"]
