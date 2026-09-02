import json
import os
import subprocess
from pathlib import Path


REPO = Path(__file__).parents[1]


def test_failed_run_gets_failed_provenance_and_terminal_registry_receipt(tmp_path):
    folds = tmp_path / "folds.parquet"
    folds.write_bytes(b"placeholder")
    run_dir = tmp_path / "run"
    manifest = tmp_path / "manifest.yml"
    manifest.write_text(
        "\n".join([
            "name: deliberate_failure",
            "inputs:",
            f"  folds: {folds}",
            "output:",
            f"  dir: {run_dir}",
            "m07:",
            "  windows: [pretest]",
            "  sizing_mode: fixed_position",
            "  copy_policy: full_mirror",
            "  copy_latency_ms: 4000",
            "  fixed_target_exposure: 0.1",
            "resources:",
            "  mem_safe_floor_gb: 0",
        ]) + "\n",
        encoding="utf-8",
    )
    registry = tmp_path / "registry.jsonl"
    env = os.environ.copy()
    env["EXPERIMENT_REGISTRY"] = str(registry)
    proc = subprocess.run(
        ["bash", "scripts/experiment.sh", str(manifest), "--stage", "m07"],
        cwd=REPO, env=env, text=True, capture_output=True,
    )
    assert proc.returncode != 0
    provenance = json.loads((run_dir / "provenance.json").read_text())
    assert provenance["status"] == "failed"
    assert provenance["exit_code"] != 0
    receipts = list(run_dir.glob("registry_*.json"))
    assert len(receipts) == 1
    receipt = json.loads(receipts[0].read_text())
    assert receipt["event"] == "fail"
    events = [json.loads(line) for line in registry.read_text().splitlines()]
    assert [event["event"] for event in events] == ["start", "fail"]
    assert events[1]["previous_event_hash"] == events[0]["event_hash"]
