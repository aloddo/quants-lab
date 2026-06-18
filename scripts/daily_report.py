#!/usr/bin/env python
"""
daily_report.py -- the DAILY copy-trade report (Alberto 2026-06-15: "instrument all the tracking,
surface those in a daily report... check back in a week when we have enough data"). One artifact per
day: portfolio across all venues + skill-cohort live edge (granular: wallet/coin/long-short) + the
accumulation counter toward a trustworthy verdict + the day's anomalies (hangs, liqs, restarts).

DELIBERATELY does NOT draw a verdict while n is thin -- it reports the counter and says "keep accruing"
until n_closes >= TRUST_N. Prints a Telegram-ready block AND writes a brain page (via gbrain put).

Run: ~/miniforge3/envs/quants-lab/bin/python scripts/daily_report.py [YYYY-MM-DD]
"""
import re
import subprocess
import sys
from datetime import datetime, timezone

PY = "/Users/hermes/miniforge3/envs/quants-lab/bin/python"
ROOT = "/Users/hermes/quants-lab"
LOG = "/tmp/ql-v12-copy-trader-launchd.log"
# RESET (Alberto 2026-06-15): measure from THIS MORNING's engine change (gross-gate + trim bundle,
# commit d8ef6c6, engine READY 10:33:57 CEST = 08:33:57 UTC), NOT the 06-14 23:11 cohort deploy.
DEPLOY = datetime(2026, 6, 15, 8, 33, 57, tzinfo=timezone.utc)
TRUST_N = 200          # closes needed before a verdict is trustworthy (Alberto: don't over-read few points)
BASELINE_BPS = 16.0    # old PnL cohort live effective edge


def _week_window(now):
    """Monday-to-Monday weekly window (Alberto 2026-06-15: 'take a Monday-to-Monday weekly view')."""
    monday = (now - __import__("datetime").timedelta(days=now.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0)
    nxt = monday + __import__("datetime").timedelta(days=7)
    return monday, nxt


def run(cmd):
    try:
        return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=120).stdout.strip()
    except Exception as e:
        return f"(failed: {e})"


def main():
    day = sys.argv[1] if len(sys.argv) > 1 else datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now = datetime.now(timezone.utc)
    days_live = (now - DEPLOY).total_seconds() / 86400.0

    snap = run([PY, "tools/portfolio_snapshot.py"])
    gran = run([PY, "scripts/skill_granular_tracker.py"])
    edge = run([PY, "scripts/skill_edge_tracker.py"])

    # accumulation counter: closes so far (from granular tracker header)
    m = re.search(r"reset: (\d+)\)", gran)
    n_closes = int(m.group(1)) if m else 0
    pct = min(100, n_closes / TRUST_N * 100)

    # day anomalies: restarts (READY lines) + hangs (stall watchdog) + liqs today
    readys = run(["bash", "-lc", f"grep '{day}' {LOG} 2>/dev/null | grep -c 'V17 READY' | head -1"]) or "0"
    stalls = run(["bash", "-lc", f"grep -c 'force-restart' /tmp/v17-stall-watchdog.log 2>/dev/null | head -1"]) or "0"
    liqs_today = run(["bash", "-lc", f"grep '{day}' {LOG} 2>/dev/null | grep -c 'LIQUIDATION:' | head -1"]) or "0"

    wk_start, wk_end = _week_window(now)
    head = (f"=== DAILY COPY-TRADE REPORT {day} ===\n"
            f"measurement RESET to 2026-06-15 10:33 CEST (latest engine change: gross-gate + trim)\n"
            f"weekly window (Mon->Mon): {wk_start.strftime('%Y-%m-%d')} -> {wk_end.strftime('%Y-%m-%d')} "
            f"({days_live:.1f}d since reset)\n"
            f"accumulation {n_closes}/{TRUST_N} closes ({pct:.0f}%) toward trustworthy verdict\n"
            f"VERDICT: {'HOLD -- sample still thin, keep accruing (re-decide next Mon)' if n_closes < TRUST_N else 'sample mature -- read the edge'}\n")

    portfolio = f"\nPORTFOLIO (all venues):\n  {snap}\n"
    anomalies = (f"\nOPS today: engine restarts(READY)={readys} | watchdog force-restarts={stalls} | "
                 f"liquidations={liqs_today}\n")

    report = head + portfolio + anomalies + "\n--- GRANULAR (wallet/coin/long-short) ---\n" + gran + \
        "\n\n--- CLEAN ROUND-TRIP EDGE ---\n" + edge + \
        f"\n\n(Baseline old PnL cohort ~{BASELINE_BPS:.0f}bps. Report is descriptive; no scaling decision " \
        f"until n>={TRUST_N}. Alberto 2026-06-15: don't draw conclusions from few data points.)\n"

    print(report)

    # Telegram-ready compact block (first ~25 lines of substance)
    return report


if __name__ == "__main__":
    main()
