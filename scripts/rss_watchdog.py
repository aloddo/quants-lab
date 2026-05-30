#!/usr/bin/env python3
"""RSS watchdog: SIGKILL a runaway process before it OOMs the Mac.

HARD GUARD against OOM crashes. Polls a target PID (or its process group),
SIGKILLs if RSS exceeds budget. Logs every check; SIGTERM first, SIGKILL
after grace period.

Created 2026-05-29 after second OOM in 4 days. Use alongside (or instead of)
preflight_rss_budget.py for long-running jobs whose RSS may grow.

Usage:
    # Watch single PID, kill if >5GB RSS
    python scripts/rss_watchdog.py --pid 12345 --budget-gb 5

    # Watch group (parent + children), kill all if total >10GB
    python scripts/rss_watchdog.py --pgid 12345 --budget-gb 10 --interval 10

    # Spawn + watch (cleanest API)
    python scripts/rss_watchdog.py --budget-gb 5 -- python scripts/v13_journey_trace.py ...

Exit codes:
    0 = target exited normally
    1 = target killed by watchdog (OOM averted)
    2 = bad arguments or target failed to start
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

PAGE_SIZE_RSS = 1024  # ps rss reports in KB


def get_rss_kb(pid: int) -> int | None:
    """RSS of a single PID in KB, None if process gone."""
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            capture_output=True, text=True, check=False,
        )
        if out.returncode != 0 or not out.stdout.strip():
            return None
        return int(out.stdout.strip())
    except Exception:
        return None


def get_group_rss_kb(pgid: int) -> tuple[int, list[int]]:
    """Total RSS of all processes in pgid (KB) + their PIDs."""
    out = subprocess.run(
        ["ps", "-o", "pid=,rss=", "-g", str(pgid)],
        capture_output=True, text=True, check=False,
    )
    total = 0
    pids = []
    for line in out.stdout.splitlines():
        parts = line.split()
        if len(parts) == 2:
            try:
                pids.append(int(parts[0]))
                total += int(parts[1])
            except ValueError:
                pass
    return total, pids


def kill_tree(pids: list[int], grace_s: int = 5) -> None:
    """SIGTERM then SIGKILL."""
    for pid in pids:
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    time.sleep(grace_s)
    for pid in pids:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="RSS watchdog")
    ap.add_argument("--pid", type=int, help="Single PID to watch")
    ap.add_argument("--pgid", type=int, help="Process group ID to watch")
    ap.add_argument("--budget-gb", type=float, required=True, help="RSS budget in GB")
    ap.add_argument("--interval", type=float, default=15.0, help="Poll interval seconds")
    ap.add_argument("--grace", type=int, default=5, help="SIGTERM->SIGKILL grace seconds")
    ap.add_argument("--log", default="-", help="Log path or - for stderr")
    ap.add_argument("cmd", nargs=argparse.REMAINDER,
                    help="Command to spawn after -- (alternative to --pid/--pgid)")
    args = ap.parse_args(argv)

    log = sys.stderr if args.log == "-" else open(args.log, "a", buffering=1)

    def emit(msg: str) -> None:
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] {msg}", file=log, flush=True)

    spawned = None
    if args.cmd and args.cmd[0] == "--":
        args.cmd = args.cmd[1:]
    if args.cmd:
        emit(f"WATCHDOG: spawning {' '.join(args.cmd)} budget={args.budget_gb}GB")
        spawned = subprocess.Popen(args.cmd, preexec_fn=os.setsid)
        pgid = spawned.pid  # new session leader
    elif args.pgid:
        pgid = args.pgid
    elif args.pid:
        pgid = None
    else:
        print("error: --pid, --pgid, or trailing command required", file=sys.stderr)
        return 2

    budget_kb = int(args.budget_gb * 1024 * 1024)
    emit(f"WATCHDOG: budget={args.budget_gb}GB ({budget_kb}KB) interval={args.interval}s")

    while True:
        if spawned and spawned.poll() is not None:
            rc = spawned.returncode
            emit(f"WATCHDOG: target exited rc={rc}")
            return 0 if rc == 0 else rc

        if pgid is not None:
            rss_kb, pids = get_group_rss_kb(pgid)
            if not pids:
                emit("WATCHDOG: target group gone")
                return 0
            target_for_kill = pids
        else:
            rss_kb = get_rss_kb(args.pid)
            if rss_kb is None:
                emit(f"WATCHDOG: PID {args.pid} gone")
                return 0
            target_for_kill = [args.pid]

        if rss_kb > budget_kb:
            emit(
                f"WATCHDOG: KILL — RSS={rss_kb/1024/1024:.2f}GB > "
                f"budget={args.budget_gb}GB ({len(target_for_kill)} pids)"
            )
            kill_tree(target_for_kill, grace_s=args.grace)
            return 1

        emit(f"WATCHDOG: ok RSS={rss_kb/1024/1024:.2f}GB / {args.budget_gb}GB")
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
