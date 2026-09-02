import importlib.util
import json
from pathlib import Path


MODULE_PATH = Path(__file__).parents[1] / "tools" / "experiment_registry.py"
SPEC = importlib.util.spec_from_file_location("experiment_registry", MODULE_PATH)
registry = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(registry)


def _base(kind, run_id="run-1"):
    return {
        "event": kind,
        "run_id": run_id,
        "recorded_utc": "2026-08-03T00:00:00+00:00",
        "manifest_path": "manifest.yml",
        "manifest_sha256": "a" * 64,
        "run_dir": "run",
    }


def test_registry_chain_accepts_valid_append(tmp_path):
    path = tmp_path / "registry.jsonl"
    first = registry.append_event(path, _base("start"))
    second = registry.append_event(path, _base("complete"))
    events, errors = registry.read_and_verify(path)
    assert errors == []
    assert second["previous_event_hash"] == first["event_hash"]
    assert [e["event"] for e in events] == ["start", "complete"]


def test_registry_detects_historical_mutation(tmp_path):
    path = tmp_path / "registry.jsonl"
    registry.append_event(path, _base("start"))
    registry.append_event(path, _base("complete"))
    lines = path.read_text().splitlines()
    first = json.loads(lines[0])
    first["run_dir"] = "tampered"
    lines[0] = json.dumps(first, sort_keys=True, separators=(",", ":"))
    path.write_text("\n".join(lines) + "\n")
    _, errors = registry.read_and_verify(path)
    assert any("event_hash mismatch" in error for error in errors)


def test_registry_refuses_append_after_broken_chain(tmp_path):
    path = tmp_path / "registry.jsonl"
    registry.append_event(path, _base("start"))
    path.write_text(path.read_text().replace('"run-1"', '"run-x"'))
    try:
        registry.append_event(path, _base("start", "run-2"))
    except RuntimeError as exc:
        assert "registry verification failed" in str(exc)
    else:
        raise AssertionError("broken registry was appended to")
