#!/usr/bin/env python3
"""Manifest=reality gate + one-live-config enforcement (Phase 5 guardrail, 2026-07-11).

Asserts the LIVE trading reality matches the sanctioned declaration in ops/live_manifest.json:
  1. EXACTLY ONE copy engine (hl_copy_trader_v17.py) is running -- multi-instance = double-trade risk.
  2. That engine runs the SANCTIONED --config -- a stray/wrong config live is a silent capital risk.
  3. NO forbidden/retired engine (hl_prop_copy, hummingbot, arb, mm) is running.
  4. Reports halt-flag state (informational; a halt is not a failure by itself).

READ-ONLY. No writes, no process control. Exit 0 = reality matches manifest, exit 1 = at least one
mismatch (the guard fires). Motivation: 20 config/*.json files exist; nothing enforced that the one
sanctioned config is what is actually live. Silent config drift is a capital risk.

Usage:
    python scripts/live_manifest_check.py           # human table + PASS/FAIL, exit code
    python scripts/live_manifest_check.py --json      # machine-readable (for the CoS alert cron)
"""
from __future__ import annotations
import json, os, re, subprocess, sys, argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "ops" / "live_manifest.json"


def _ps_matches(pattern: str) -> list[tuple[int, str]]:
    """Return [(pid, cmdline)] for running processes whose full command matches `pattern`.
    Excludes THIS checker process and its parent shell (avoid self/grep false matches)."""
    out = subprocess.run(["ps", "-axo", "pid=,command="], capture_output=True, text=True).stdout
    me = {os.getpid(), os.getppid()}
    hits = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        m = re.match(r"^(\d+)\s+(.*)$", line)
        if not m:
            continue
        pid, cmd = int(m.group(1)), m.group(2)
        if pid in me:
            continue
        if "live_manifest_check" in cmd:   # never count the checker itself
            continue
        if pattern in cmd:
            hits.append((pid, cmd))
    return hits


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    mani = json.loads(MANIFEST.read_text())
    eng = mani["engine"]
    script = eng["script"]                       # strategies/live/hl_copy_trader_v17.py
    script_base = os.path.basename(script)       # hl_copy_trader_v17.py
    sanctioned_cfg = eng["config"]

    failures, rows = [], []

    # 1 + 2: exactly one engine, sanctioned config
    engs = _ps_matches(script_base)
    n = len(engs)
    if n != 1:
        failures.append(f"engine_instances={n} (expected 1)")
    rows.append(("engine_instances", str(n), "PASS" if n == 1 else "FAIL"))

    live_cfgs = []
    for _pid, cmd in engs:
        m = re.search(r"--config\s+(\S+)", cmd)
        live_cfgs.append(m.group(1) if m else "<none>")
    if n == 1:
        lc = live_cfgs[0]
        ok = (lc == sanctioned_cfg) or (os.path.basename(lc) == os.path.basename(sanctioned_cfg))
        if not ok:
            failures.append(f"live_config={lc} != sanctioned {sanctioned_cfg}")
        rows.append(("live_config", lc, "PASS" if ok else "FAIL"))
    else:
        rows.append(("live_config", ",".join(live_cfgs) or "<none>", "FAIL"))

    # 3: no forbidden/retired engine running
    for pat in mani.get("forbidden_engines", []):
        base = os.path.basename(pat)
        hits = _ps_matches(base)
        # hl_copy_trader_v17 must not be caught by a substring like 'hl' patterns; forbidden are distinct names
        hits = [h for h in hits if script_base not in h[1]]
        if hits:
            failures.append(f"forbidden engine RUNNING: {pat} (pid {hits[0][0]})")
            rows.append((f"forbidden:{base}", f"{len(hits)} running", "FAIL"))

    # 4: halt flags (informational)
    halts = [p for p in ("/tmp/v12_pause", str(Path.home() / "quants-lab" / ".HALT_COPY")) if os.path.exists(p)]
    rows.append(("halt_flags", ",".join(halts) if halts else "none", "INFO"))

    result = {"pass": not failures, "failures": failures,
              "engine_instances": n, "live_config": live_cfgs, "halt_flags": halts}

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(f"LIVE MANIFEST CHECK (sanctioned: {script_base} + {os.path.basename(sanctioned_cfg)})\n")
        for name, val, status in rows:
            print(f"  {name:22s} {status:5s} {val}")
        print(f"\nRESULT: {'PASS' if not failures else 'FAIL (' + '; '.join(failures) + ')'}")
    sys.exit(0 if not failures else 1)


if __name__ == "__main__":
    main()
