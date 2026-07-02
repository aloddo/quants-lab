#!/usr/bin/env python3
"""v25 freeze record writer + validator (spec: contamination controls; gate-b blocker #8).

freeze = JOINT (pre-registration doc content hash of the CURRENT /tmp/v25_prereg_v3.md,
harness git commit with research/v25 TRACKED and clean, input sha256 manifest including
m02_actions.parquet AND every marks-cache file under app/data/v15/marks_cache), recorded
write-once in app/data/research/v25/FREEZE.json at codex gate (b).

The fold runner (v25_run_folds.py) calls validate_freeze() at startup and REFUSES to run
full folds on any mismatch between FREEZE.json and the current code + doc + inputs.

Run: /Users/hermes/miniforge3/envs/quants-lab/bin/python research/v25/v25_freeze.py
"""
from __future__ import annotations

import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from v25_common import ACTIONS_PARQUET, L2_CALIB_PATH, MARKS_CACHE_DIR, OUT_DIR, \
    PREREG_DOC, REPO, git_commit, sha256_file

HARNESS_FILES = ["v25_common.py", "v25_gates.py", "v25_r1_causal.py", "v25_r2_lcb.py",
                 "v25_portfolio_sim.py", "v25_bootstrap.py", "v25_run_folds.py",
                 "v25_holdout.py", "v25_ship_config.py", "v25_freeze.py"]
FREEZE_PATH = OUT_DIR / "FREEZE.json"


def harness_hashes() -> tuple[dict, str]:
    here = Path(__file__).resolve().parent
    hashes = {f: sha256_file(here / f) for f in HARNESS_FILES if (here / f).exists()}
    combined = hashlib.sha256(
        "".join(f"{k}:{v}" for k, v in sorted(hashes.items())).encode()).hexdigest()
    return hashes, combined


def marks_cache_manifest() -> tuple[dict, str]:
    """sha256 of EVERY marks-cache file (all of them; blocker #8) + a combined hash."""
    files = sorted(p for p in MARKS_CACHE_DIR.iterdir() if p.is_file())
    hashes = {p.name: sha256_file(p) for p in files}
    combined = hashlib.sha256(
        "".join(f"{k}:{v}" for k, v in sorted(hashes.items())).encode()).hexdigest()
    return hashes, combined


def compute_freeze(include_marks_files: bool = True) -> dict:
    if not PREREG_DOC.exists():
        raise SystemExit(f"FREEZE ABORT: pre-registration doc missing at {PREREG_DOC}")
    if not ACTIONS_PARQUET.exists():
        raise SystemExit(f"FREEZE ABORT: input missing at {ACTIONS_PARQUET}")
    hh, combined = harness_hashes()
    marks_hashes, marks_combined = marks_cache_manifest()
    freeze = {
        "prereg_doc": str(PREREG_DOC),
        # hash of the CURRENT doc content at freeze time (blocker #8: never a stale copy)
        "prereg_doc_sha256": sha256_file(PREREG_DOC),
        "git_commit": git_commit(),
        "harness_dir": "research/v25",
        "harness_file_sha256": hh,
        "harness_combined_sha256": combined,
        "input_manifest": {
            str(ACTIONS_PARQUET.relative_to(REPO)): sha256_file(ACTIONS_PARQUET),
            str(L2_CALIB_PATH.relative_to(REPO)): sha256_file(L2_CALIB_PATH),
        },
        "marks_cache_dir": str(MARKS_CACHE_DIR.relative_to(REPO)),
        "marks_cache_n_files": len(marks_hashes),
        "marks_cache_combined_sha256": marks_combined,
        "frozen_unix": time.time(),
    }
    if include_marks_files:
        freeze["marks_cache_file_sha256"] = marks_hashes
    return freeze


def _harness_tracked_and_clean() -> str | None:
    """Return an error string if research/v25 or tests/v25 is untracked/dirty in git."""
    try:
        dirty = subprocess.check_output(
            ["git", "status", "--porcelain", "research/v25", "tests/v25"],
            cwd=REPO).decode().strip()
    except Exception as e:
        return f"git status failed: {e}"
    if dirty:
        return f"research/v25 or tests/v25 not clean in git:\n{dirty}"
    return None


def validate_freeze(check_inputs: bool = True, freeze_path: Path = FREEZE_PATH) -> dict:
    """Validate FREEZE.json against the CURRENT code + doc (+ inputs when check_inputs).
    Raises SystemExit with a precise reason on ANY mismatch. Returns the freeze dict."""
    if not freeze_path.exists():
        raise SystemExit(f"FREEZE VALIDATION: {freeze_path} missing -- run v25_freeze.py "
                         f"at codex gate (b) before any full fold run")
    with open(freeze_path) as fh:
        fz = json.load(fh)
    errs = []
    doc_hash = sha256_file(PREREG_DOC) if PREREG_DOC.exists() else "MISSING"
    if doc_hash != fz.get("prereg_doc_sha256"):
        errs.append(f"prereg doc hash mismatch: current {doc_hash} != frozen "
                    f"{fz.get('prereg_doc_sha256')}")
    _, combined = harness_hashes()
    if combined != fz.get("harness_combined_sha256"):
        errs.append(f"harness code hash mismatch: current {combined} != frozen "
                    f"{fz.get('harness_combined_sha256')}")
    if check_inputs:
        for rel, want in fz.get("input_manifest", {}).items():
            p = REPO / rel
            got = sha256_file(p) if p.exists() else "MISSING"
            if got != want:
                errs.append(f"input hash mismatch {rel}: current {got} != frozen {want}")
        _, marks_combined = marks_cache_manifest()
        if marks_combined != fz.get("marks_cache_combined_sha256"):
            errs.append(f"marks-cache combined hash mismatch: current {marks_combined} "
                        f"!= frozen {fz.get('marks_cache_combined_sha256')}")
    if errs:
        raise SystemExit("FREEZE VALIDATION FAILED -- refusing to run:\n  "
                         + "\n  ".join(errs))
    return fz


def main():
    err = _harness_tracked_and_clean()
    if err:
        raise SystemExit(f"FREEZE ABORT (harness must be committed first): {err}")
    freeze = compute_freeze()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if FREEZE_PATH.exists():
        raise SystemExit(f"FREEZE ABORT: {FREEZE_PATH} already exists (write-once; delete "
                         f"manually only with a NEW codex gate (b) sign-off)")
    with open(FREEZE_PATH, "w") as fh:
        json.dump(freeze, fh, indent=2)
    print(f"FREEZE written: {FREEZE_PATH}")
    print(f"  doc sha256      {freeze['prereg_doc_sha256']}")
    print(f"  git commit      {freeze['git_commit']}")
    print(f"  harness sha256  {freeze['harness_combined_sha256']}")
    print(f"  marks cache     {freeze['marks_cache_n_files']} files, combined "
          f"{freeze['marks_cache_combined_sha256'][:16]}...")


if __name__ == "__main__":
    main()
