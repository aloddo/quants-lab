#!/usr/bin/env python3
"""In-process live authorization shared by trading entrypoints."""
from __future__ import annotations

import hashlib
from pathlib import Path

from research_lineage_gate import verify as verify_lineage


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def verify_live_authorization(config: Path, repo: Path, pause_path: Path = Path("/tmp/v12_pause")) -> dict:
    repo = repo.resolve()
    config = config.resolve()
    errors: list[str] = []
    if (repo / ".HALT_COPY").exists() or pause_path.exists():
        errors.append("halt flag present")
    arm_path = repo / ".ARM_COPY"
    arm: dict[str, str] = {}
    if not arm_path.is_file():
        errors.append(".ARM_COPY missing")
    else:
        for line in arm_path.read_text(encoding="utf-8").splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                arm[key] = value
    armed_config = Path(arm.get("config", ""))
    armed_config = armed_config if armed_config.is_absolute() else repo / armed_config
    if not arm.get("config") or armed_config.resolve() != config:
        errors.append("armed config differs from requested config")
    actual_config_sha = _sha(config) if config.is_file() else ""
    if arm.get("config_sha256") != actual_config_sha:
        errors.append("config bytes differ from armed config hash")
    if arm.get("lineage_rc") != "0" or not arm.get("lineage_digest"):
        errors.append("armed record has no passing research lineage")
    lineage, lineage_rc = verify_lineage(config, repo)
    if lineage_rc != 0:
        errors.append("research lineage no longer verifies")
    if arm.get("lineage_digest") != lineage.get("lineage_digest"):
        errors.append("research lineage differs from armed digest")
    return {"ok": not errors, "errors": errors, "lineage": lineage}
