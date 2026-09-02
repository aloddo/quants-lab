#!/usr/bin/env python3
"""Fail-closed verifier for the research lineage behind a live copy roster.

The live config pins one deployment manifest by SHA-256.  That manifest in turn
pins every decision artifact used to approve the roster.  This verifier is run
both when an operator arms the trader and immediately before every launch.

This is deliberately a verifier, not an approval generator: it cannot turn an
incomplete experiment into an approved deployment.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "copy-research-lineage-v1"
REQUIRED_GATES = (
    "data_quality",
    "point_in_time",
    "m06b_investable",
    "m08_survival",
    "m09_chained",
    "m10_verdict",
    "live_replay_parity",
)
REQUIRED_ARTIFACTS = (
    "experiment_manifest",
    "provenance",
    "registry_receipt",
    "roster_source",
    "m06b_manifest",
    "m08_result",
    "m09_result",
    "m10_verdict",
    "live_replay_parity",
)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as fh:
        return json.load(fh)


def _inside_repo(repo: Path, raw_path: Any, errors: list[str], label: str) -> Path | None:
    if not isinstance(raw_path, str) or not raw_path.strip():
        errors.append(f"{label}: path is missing")
        return None
    candidate = Path(raw_path)
    candidate = candidate if candidate.is_absolute() else repo / candidate
    try:
        resolved = candidate.resolve(strict=True)
    except (FileNotFoundError, RuntimeError, OSError) as exc:
        errors.append(f"{label}: cannot resolve {raw_path!r}: {exc}")
        return None
    try:
        resolved.relative_to(repo)
    except ValueError:
        errors.append(f"{label}: path escapes repository: {raw_path!r}")
        return None
    # A symlink can be retargeted without changing the pinned leaf bytes.  Reject
    # any symlink component so the provenance name itself is stable.
    relative = candidate.absolute().relative_to(repo)
    cursor = repo
    for part in relative.parts:
        cursor = cursor / part
        if cursor.is_symlink():
            errors.append(f"{label}: symlinked path component is forbidden: {cursor}")
            return None
    if not resolved.is_file():
        errors.append(f"{label}: not a regular file: {raw_path!r}")
        return None
    return resolved


def _finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def verify(config_path: Path, repo: Path) -> tuple[dict[str, Any], int]:
    errors: list[str] = []
    observed: dict[str, str] = {}
    try:
        config = load_json(config_path)
    except Exception as exc:
        result = {"ok": False, "errors": [f"config unreadable: {exc}"]}
        return result, 2

    lineage = (config.get("global") or {}).get("research_lineage")
    if not isinstance(lineage, dict):
        return {"ok": False, "errors": ["global.research_lineage is required"]}, 1

    manifest_path = _inside_repo(repo, lineage.get("deployment_manifest"), errors, "deployment_manifest")
    declared_manifest_sha = lineage.get("deployment_manifest_sha256")
    manifest: dict[str, Any] = {}
    if manifest_path:
        actual = sha256(manifest_path)
        observed["deployment_manifest"] = actual
        if not isinstance(declared_manifest_sha, str) or actual != declared_manifest_sha.lower():
            errors.append("deployment_manifest: SHA-256 does not match live config")
        try:
            loaded = load_json(manifest_path)
            if not isinstance(loaded, dict):
                raise ValueError("top level is not an object")
            manifest = loaded
        except Exception as exc:
            errors.append(f"deployment_manifest: invalid JSON: {exc}")

    if manifest.get("schema_version") != SCHEMA_VERSION:
        errors.append(f"deployment_manifest: schema_version must be {SCHEMA_VERSION!r}")
    if manifest.get("status") != "complete":
        errors.append("deployment_manifest: status is not complete")
    if manifest.get("deployable") is not True:
        reasons = manifest.get("non_deployable_reasons") or []
        errors.append(f"deployment_manifest: deployable is not true; reasons={reasons!r}")
    if manifest.get("git_dirty") is not False:
        errors.append("deployment_manifest: research run was not from a clean git tree")

    gates = manifest.get("required_gates") or {}
    for gate in REQUIRED_GATES:
        if gates.get(gate) is not True:
            errors.append(f"required gate did not pass: {gate}")

    artifacts = manifest.get("artifacts") or {}
    resolved_artifacts: dict[str, Path] = {}
    for name in REQUIRED_ARTIFACTS:
        entry = artifacts.get(name)
        if not isinstance(entry, dict):
            errors.append(f"artifact missing: {name}")
            continue
        path = _inside_repo(repo, entry.get("path"), errors, f"artifact.{name}")
        declared = entry.get("sha256")
        if path:
            actual = sha256(path)
            observed[name] = actual
            resolved_artifacts[name] = path
            if not isinstance(declared, str) or actual != declared.lower():
                errors.append(f"artifact.{name}: SHA-256 mismatch")

    # The final roster decision is part of the manifest, not inferred from a
    # human-readable authorization sentence.
    selected = manifest.get("selected_wallets")
    config_wallets = (config.get("wallets") or {}).keys()
    if not isinstance(selected, list) or not selected:
        errors.append("deployment_manifest: selected_wallets must be a non-empty list")
    else:
        normalized_selected = [str(w).lower() for w in selected]
        if len(normalized_selected) != len(set(normalized_selected)):
            errors.append("deployment_manifest: selected_wallets contains duplicates")
        if set(normalized_selected) != {str(w).lower() for w in config_wallets}:
            errors.append("deployment_manifest: selected_wallets != live config wallets")

    # Semantic checks prevent a manifest author from labeling failed artifacts
    # as passed while still pinning their bytes correctly.
    for name in ("provenance", "registry_receipt", "m06b_manifest", "m09_result", "m10_verdict", "live_replay_parity"):
        path = resolved_artifacts.get(name)
        if not path:
            continue
        try:
            obj = load_json(path)
        except Exception as exc:
            errors.append(f"artifact.{name}: invalid JSON: {exc}")
            continue
        if name == "provenance":
            if obj.get("status") != "complete" or not isinstance(obj.get("completed_utc"), str):
                errors.append("artifact.provenance: experiment is not explicitly complete")
            if not isinstance(obj.get("outputs"), dict) or not obj["outputs"]:
                errors.append("artifact.provenance: output hash inventory is absent")
            if obj.get("git_dirty") is not False:
                errors.append("artifact.provenance: experiment ran from a dirty git tree")
        elif name == "registry_receipt":
            if obj.get("event") != "complete" or not obj.get("event_hash"):
                errors.append("artifact.registry_receipt: terminal event is not complete")
            provenance_sha = observed.get("provenance")
            if obj.get("provenance_sha256") != provenance_sha:
                errors.append("artifact.registry_receipt: does not bind the pinned provenance")
        elif name == "m06b_manifest":
            if obj.get("investable") is not True or obj.get("non_investable_reasons"):
                errors.append("artifact.m06b_manifest: M6b is non-investable")
            if not obj.get("slippage_calibration_version"):
                errors.append("artifact.m06b_manifest: slippage calibration version is absent")
        elif name == "m09_result":
            if obj.get("status") != "complete" or obj.get("n_folds", 0) <= 0:
                errors.append("artifact.m09_result: chained simulation is incomplete")
            for field in ("chained_roe", "max_chained_dd", "chained_calmar"):
                if not _finite_number(obj.get(field)):
                    errors.append(f"artifact.m09_result: {field} is not finite")
        elif name == "m10_verdict":
            if obj.get("all_pass") is not True or obj.get("greenlight") not in {"live_small", "lp_confirmatory"}:
                errors.append("artifact.m10_verdict: M10 does not greenlight deployment")
        elif name == "live_replay_parity":
            if obj.get("pass") is not True or obj.get("status") != "complete":
                errors.append("artifact.live_replay_parity: replay/live parity did not pass")

    digest_blob = json.dumps(observed, sort_keys=True, separators=(",", ":")).encode()
    result = {
        "ok": not errors,
        "schema_version": SCHEMA_VERSION,
        "config": os.path.relpath(config_path, repo),
        "lineage_digest": hashlib.sha256(digest_blob).hexdigest(),
        "observed_sha256": observed,
        "errors": errors,
    }
    return result, 0 if not errors else 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--json-out", type=Path)
    parser.add_argument("--expect-digest")
    args = parser.parse_args()
    repo = args.repo.resolve()
    config = args.config if args.config.is_absolute() else repo / args.config
    result, rc = verify(config.resolve(), repo)
    if args.expect_digest and result.get("lineage_digest") != args.expect_digest:
        result["ok"] = False
        result.setdefault("errors", []).append("lineage digest differs from armed record")
        rc = 1
    rendered = json.dumps(result, indent=2, sort_keys=True)
    if args.json_out:
        args.json_out.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
