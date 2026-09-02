#!/usr/bin/env python3
"""Append-only, hash-chained registry for canonical research experiments."""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import Any


SCHEMA = "copy-experiment-registry-v1"


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def _event_hash(event: dict[str, Any]) -> str:
    body = {k: v for k, v in event.items() if k != "event_hash"}
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(raw).hexdigest()


def read_and_verify(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    events: list[dict[str, Any]] = []
    errors: list[str] = []
    previous = "GENESIS"
    if not path.exists():
        return events, errors
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            errors.append(f"line {number}: blank line")
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"line {number}: invalid JSON: {exc}")
            continue
        if event.get("schema_version") != SCHEMA:
            errors.append(f"line {number}: bad schema_version")
        if event.get("previous_event_hash") != previous:
            errors.append(f"line {number}: broken previous_event_hash")
        actual = _event_hash(event)
        if event.get("event_hash") != actual:
            errors.append(f"line {number}: event_hash mismatch")
        previous = event.get("event_hash", "")
        events.append(event)
    return events, errors


def append_event(registry: Path, event: dict[str, Any]) -> dict[str, Any]:
    registry.parent.mkdir(parents=True, exist_ok=True)
    lock_path = registry.with_suffix(registry.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        events, errors = read_and_verify(registry)
        if errors:
            raise RuntimeError("registry verification failed: " + "; ".join(errors))
        event["schema_version"] = SCHEMA
        event["previous_event_hash"] = events[-1]["event_hash"] if events else "GENESIS"
        event["event_hash"] = _event_hash(event)
        with registry.open("a", encoding="utf-8") as out:
            out.write(json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n")
            out.flush()
            os.fsync(out.fileno())
        return event


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("event", choices=("start", "complete", "fail", "audit"))
    parser.add_argument("--registry", type=Path, default=Path("app/data/v15/experiment_registry.jsonl"))
    parser.add_argument("--run-id")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--run-dir")
    parser.add_argument("--provenance", type=Path)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--reason")
    args = parser.parse_args()

    if args.event == "audit":
        events, errors = read_and_verify(args.registry)
        print(json.dumps({"ok": not errors, "n_events": len(events), "errors": errors}, indent=2))
        return 0 if not errors else 1

    run_id = args.run_id or str(uuid.uuid4())
    if not args.manifest or not args.run_dir:
        parser.error("start/complete/fail require --manifest and --run-dir")
    event: dict[str, Any] = {
        "event": args.event,
        "run_id": run_id,
        "recorded_utc": dt.datetime.now(dt.UTC).isoformat(),
        "manifest_path": str(args.manifest),
        "manifest_sha256": _sha(args.manifest),
        "run_dir": args.run_dir,
    }
    if args.event != "start":
        events, errors = read_and_verify(args.registry)
        if errors:
            raise SystemExit("registry verification failed: " + "; ".join(errors))
        starts = [e for e in events if e.get("run_id") == run_id and e.get("event") == "start"]
        terminals = [e for e in events if e.get("run_id") == run_id and e.get("event") in {"complete", "fail"}]
        if len(starts) != 1 or terminals:
            raise SystemExit(f"run_id {run_id!r} has invalid transition history")
        if args.provenance and args.provenance.is_file():
            event["provenance_path"] = str(args.provenance)
            event["provenance_sha256"] = _sha(args.provenance)
        if args.reason:
            event["reason"] = args.reason
    written = append_event(args.registry, event)
    if args.receipt:
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(json.dumps(written, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(run_id if args.event == "start" else json.dumps(written, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
