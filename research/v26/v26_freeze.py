#!/usr/bin/env python3
"""v26 FREEZE-GRID.json writer + validator (amendment codex #13, frozen).

FREEZE-GRID.json binds together, write-once, BEFORE any test PnL is read:
- content hash of the CURRENT /tmp/v26_grid_prereg.md
- harness git commit (research/v26 tracked and clean) + per-file sha256
- config registry + prune ledger hashes computed from the ACTUAL parquet files
  (file-byte sha256 AND canonical content sha256; registry_sha.json is only
  cross-checked at freeze time, never trusted at validation -- codex code-gate #7)
- fee snapshot sha256 (app/data/research/v25/fee_snapshot_v26.json)
- POINTERS to the v25 FREEZE (its file sha256 + its doc/commit/marks hashes are
  inherited, not recomputed -- the marks/artifact provenance lives there)
- maker model version, resample counts, all seeds (dropout 17/42/137 inherited;
  bootstrap seed 42; no other randomness exists)

The grid runner (v26_run_grid.py --confirm-grid) calls validate_freeze_grid() at
startup and refuses on any mismatch.

Run: .../python research/v26/v26_freeze.py
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import time
from pathlib import Path

import pandas as pd

from v26_common import (DROPOUT_SEEDS, FEE_SNAPSHOT_PATH, FREEZE_GRID_PATH, GRID_PREREG_DOC,
                        GRID_RESAMPLES, GRID_SEED, HOLM_RESAMPLES, MAKER_MODEL_VERSION,
                        REPO, V25_FREEZE_PATH, V26_DATA, canonical_sha256, git_commit,
                        sha256_file)

HARNESS_FILES = ["v26_common.py", "v26_registry.py", "v26_families.py",
                 "v26_overlays.py", "v26_maker.py", "v26_fees.py", "v26_estimand.py",
                 "v26_run_grid.py", "v26_freeze.py"]


def harness_hashes() -> tuple[dict, str]:
    here = Path(__file__).resolve().parent
    hashes = {f: sha256_file(here / f) for f in HARNESS_FILES if (here / f).exists()}
    combined = hashlib.sha256(
        "".join(f"{k}:{v}" for k, v in sorted(hashes.items())).encode()).hexdigest()
    return hashes, combined


def _clean() -> str | None:
    try:
        dirty = subprocess.check_output(
            ["git", "status", "--porcelain", "research/v26", "tests/v26"],
            cwd=REPO).decode().strip()
    except Exception as e:
        return f"git status failed: {e}"
    return f"research/v26 or tests/v26 not clean:\n{dirty}" if dirty else None


def registry_content_hashes(data_dir: Path = None) -> dict:
    """Hashes of the ACTUAL configs.parquet + prune_ledger.parquet (codex code-gate #7):
    sha256 of the file BYTES plus the canonical CONTENT hash recomputed from the loaded
    frames -- registry_sha.json is never trusted for freeze validation."""
    d = data_dir if data_dir is not None else V26_DATA
    out = {}
    for name, key in [("configs.parquet", "registry"),
                      ("prune_ledger.parquet", "prune_ledger")]:
        p = d / name
        if not p.exists():
            raise SystemExit(f"FREEZE-GRID ABORT: {p} missing (build the registry "
                             f"first)")
        out[f"{key}_file_sha256"] = sha256_file(p)
        out[f"{key}_sha256"] = canonical_sha256(pd.read_parquet(p))
    return out


def compute_freeze() -> dict:
    for p, what in [(GRID_PREREG_DOC, "grid pre-registration doc"),
                    (FEE_SNAPSHOT_PATH, "fee snapshot"),
                    (V25_FREEZE_PATH, "v25 FREEZE.json"),
                    (V26_DATA / "registry_sha.json", "registry meta (build the "
                                                     "registry first)")]:
        if not p.exists():
            raise SystemExit(f"FREEZE-GRID ABORT: {what} missing at {p}")
    with open(V26_DATA / "registry_sha.json") as fh:
        reg_meta = json.load(fh)
    reg_hashes = registry_content_hashes()
    # cross-check: the build-time meta must agree with the actual file contents
    for k in ("registry_sha256", "prune_ledger_sha256"):
        if reg_meta.get(k) != reg_hashes[k]:
            raise SystemExit(f"FREEZE-GRID ABORT: registry_sha.json {k} does not match "
                             f"the actual parquet contents ({reg_meta.get(k)} != "
                             f"{reg_hashes[k]}) -- rebuild the registry")
    with open(V25_FREEZE_PATH) as fh:
        v25fz = json.load(fh)
    hh, combined = harness_hashes()
    return {
        "grid_prereg_doc": str(GRID_PREREG_DOC),
        "grid_prereg_sha256": sha256_file(GRID_PREREG_DOC),
        "git_commit": git_commit(),
        "harness_dir": "research/v26",
        "harness_file_sha256": hh,
        "harness_combined_sha256": combined,
        "registry_sha256": reg_hashes["registry_sha256"],
        "registry_file_sha256": reg_hashes["registry_file_sha256"],
        "prune_ledger_sha256": reg_hashes["prune_ledger_sha256"],
        "prune_ledger_file_sha256": reg_hashes["prune_ledger_file_sha256"],
        "registry_counts": {k: reg_meta[k] for k in ("n_cells", "n_run", "n_pruned")},
        "fee_snapshot": str(FEE_SNAPSHOT_PATH.relative_to(REPO)),
        "fee_snapshot_sha256": sha256_file(FEE_SNAPSHOT_PATH),
        "v25_freeze_pointer": {
            "path": str(V25_FREEZE_PATH.relative_to(REPO)),
            "file_sha256": sha256_file(V25_FREEZE_PATH),
            "prereg_doc_sha256": v25fz.get("prereg_doc_sha256"),
            "git_commit": v25fz.get("git_commit"),
            "marks_cache_combined_sha256": v25fz.get("marks_cache_combined_sha256"),
            "input_manifest": v25fz.get("input_manifest"),
        },
        "maker_model_version": MAKER_MODEL_VERSION,
        "resamples": {"joint_maxstat": GRID_RESAMPLES, "holm_fallback": HOLM_RESAMPLES},
        "seeds": {"bootstrap": GRID_SEED, "dropout": DROPOUT_SEEDS},
        "frozen_unix": time.time(),
    }


def validate_freeze_grid(freeze_path: Path = FREEZE_GRID_PATH) -> dict:
    """Validate FREEZE-GRID.json against current doc + code + registry + fee snapshot +
    v25 FREEZE pointer. SystemExit with precise reasons on ANY mismatch."""
    if not freeze_path.exists():
        raise SystemExit(f"FREEZE-GRID VALIDATION: {freeze_path} missing -- run "
                         f"v26_freeze.py before any full grid run")
    with open(freeze_path) as fh:
        fz = json.load(fh)
    errs = []
    doc = sha256_file(GRID_PREREG_DOC) if GRID_PREREG_DOC.exists() else "MISSING"
    if doc != fz.get("grid_prereg_sha256"):
        errs.append(f"grid prereg doc hash mismatch: {doc} != "
                    f"{fz.get('grid_prereg_sha256')}")
    head = git_commit()
    if head != fz.get("git_commit"):
        errs.append(f"git HEAD mismatch: {head} != {fz.get('git_commit')}")
    dirty = _clean()
    if dirty:
        errs.append(dirty)
    _, combined = harness_hashes()
    if combined != fz.get("harness_combined_sha256"):
        errs.append(f"harness hash mismatch: {combined} != "
                    f"{fz.get('harness_combined_sha256')}")
    # codex code-gate #7: hash the ACTUAL configs.parquet + prune ledger contents
    # (file bytes + canonical content), NEVER trust registry_sha.json
    try:
        reg_hashes = registry_content_hashes()
    except SystemExit as e:
        reg_hashes = None
        errs.append(str(e))
    if reg_hashes is not None:
        for key, what in [("registry_file_sha256", "configs.parquet file bytes"),
                          ("registry_sha256", "configs.parquet content"),
                          ("prune_ledger_file_sha256", "prune_ledger.parquet file "
                                                       "bytes"),
                          ("prune_ledger_sha256", "prune_ledger.parquet content")]:
            if reg_hashes[key] != fz.get(key):
                errs.append(f"{what} sha mismatch (changed after freeze): "
                            f"{reg_hashes[key]} != {fz.get(key)}")
    snap = sha256_file(FEE_SNAPSHOT_PATH) if FEE_SNAPSHOT_PATH.exists() else "MISSING"
    if snap != fz.get("fee_snapshot_sha256"):
        errs.append(f"fee snapshot hash mismatch: {snap} != "
                    f"{fz.get('fee_snapshot_sha256')}")
    v25p = fz.get("v25_freeze_pointer", {})
    v25now = sha256_file(V25_FREEZE_PATH) if V25_FREEZE_PATH.exists() else "MISSING"
    if v25now != v25p.get("file_sha256"):
        errs.append(f"v25 FREEZE.json changed since grid freeze: {v25now} != "
                    f"{v25p.get('file_sha256')}")
    if errs:
        raise SystemExit("FREEZE-GRID VALIDATION FAILED -- refusing to run:\n  "
                         + "\n  ".join(errs))
    return fz


def main():
    err = _clean()
    if err:
        raise SystemExit(f"FREEZE-GRID ABORT (harness must be committed first): {err}")
    if FREEZE_GRID_PATH.exists():
        raise SystemExit(f"FREEZE-GRID ABORT: {FREEZE_GRID_PATH} already exists "
                         f"(write-once; delete manually only with a new codex "
                         f"design+code sign-off)")
    fz = compute_freeze()
    FREEZE_GRID_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(FREEZE_GRID_PATH, "w") as fh:
        json.dump(fz, fh, indent=2)
    print(f"FREEZE-GRID written: {FREEZE_GRID_PATH}")
    for k in ("grid_prereg_sha256", "git_commit", "harness_combined_sha256",
              "registry_sha256", "fee_snapshot_sha256"):
        print(f"  {k:28s} {fz[k]}")


if __name__ == "__main__":
    main()
