#!/usr/bin/env python3
"""Pre-flight RSS budget check.

HARD GUARD against OOM crashes. Refuses to greenlight a job whose estimated
RSS won't fit in (free + inactive) - 2GB_safety_margin on this Mac.

Created 2026-05-29 after second OOM in 4 days. State.md rule #1 says NO
parallel python workers without RSS budget proof; this script IS the proof.

Usage:
    python scripts/preflight_rss_budget.py --estimated-gb 5 --workers 4
    -> exit 1 if 5 * 4 = 20GB doesn't fit, exit 0 if it does

    python scripts/preflight_rss_budget.py --estimated-gb 5 --workers 4 \\
        --i-accept-oom-risk
    -> exit 0 always but logs the override (use only when you have a watchdog)

Exit codes:
    0 = OK to launch
    1 = REFUSED, would OOM
    2 = bad arguments
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

DEFAULT_SAFETY_MARGIN_GB = 2.0  # never use last 2GB by default
PAGE_SIZE = 16384  # macOS arm64


def free_inactive_gb() -> float:
    """Free + inactive memory in GB via vm_stat (macOS)."""
    out = subprocess.run(["vm_stat"], capture_output=True, text=True, check=True).stdout
    free = inactive = 0
    for line in out.splitlines():
        if line.startswith("Pages free:"):
            free = int(line.split()[-1].rstrip("."))
        elif line.startswith("Pages inactive:"):
            inactive = int(line.split()[-1].rstrip("."))
    return (free + inactive) * PAGE_SIZE / (1024 ** 3)


def total_estimated_gb(per_worker_gb: float, workers: int) -> float:
    return per_worker_gb * workers


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Pre-flight RSS budget check")
    ap.add_argument("--estimated-gb", type=float, required=True,
                    help="Estimated RSS per worker in GB (use --measure to find)")
    ap.add_argument("--workers", type=int, default=1,
                    help="Number of parallel workers (default 1)")
    ap.add_argument("--i-accept-oom-risk", action="store_true",
                    help="Override the guard. Use only with active RSS watchdog.")
    ap.add_argument("--safety-margin-gb", type=float, default=DEFAULT_SAFETY_MARGIN_GB,
                    help=f"Never use the last N GB of (free+inactive). Default {DEFAULT_SAFETY_MARGIN_GB}. "
                         "Lower (e.g. 1.0) is acceptable ONLY when an rss_watchdog is "
                         "actively running on the target — the watchdog becomes the "
                         "real ceiling, preflight is just a sanity check.")
    ap.add_argument("--json", action="store_true", help="Emit JSON")
    args = ap.parse_args(argv)

    if args.estimated_gb <= 0 or args.workers <= 0:
        print("error: --estimated-gb and --workers must be positive", file=sys.stderr)
        return 2
    if args.safety_margin_gb < 0:
        print("error: --safety-margin-gb must be >= 0", file=sys.stderr)
        return 2

    avail = free_inactive_gb()
    needed = total_estimated_gb(args.estimated_gb, args.workers)
    headroom = avail - args.safety_margin_gb
    ok = needed <= headroom

    msg = (
        f"PREFLIGHT_RSS: needed={needed:.2f}GB ({args.workers}*"
        f"{args.estimated_gb}GB) avail={avail:.2f}GB margin={args.safety_margin_gb:.1f}GB "
        f"headroom={headroom:.2f}GB ok={ok}"
    )

    if args.json:
        import json
        print(json.dumps({
            "ok": ok,
            "needed_gb": round(needed, 2),
            "avail_gb": round(avail, 2),
            "safety_margin_gb": args.safety_margin_gb,
            "headroom_gb": round(headroom, 2),
            "workers": args.workers,
            "per_worker_gb": args.estimated_gb,
            "overridden": args.i_accept_oom_risk,
        }))
    else:
        print(msg, file=sys.stderr)

    if ok:
        return 0
    if args.i_accept_oom_risk:
        print(
            f"PREFLIGHT_RSS_OVERRIDE: {msg} -- proceeding with OOM risk accepted",
            file=sys.stderr,
        )
        return 0
    print(
        f"PREFLIGHT_RSS_REFUSED: would OOM. Reduce workers, free RAM, or pass "
        f"--i-accept-oom-risk (only with rss_watchdog.py active).",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
