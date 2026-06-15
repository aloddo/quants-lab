#!/usr/bin/env python
"""
skill_edge_tracker.py -- rigorous LIVE per-trade edge read for the skill cohort, parsed from the engine log.
Operationalizes the measurement phase: run each heartbeat for a clean edge number vs the baseline, instead
of eyeballing log lines.

Parses /tmp/ql-v12-copy-trader-launchd.log since the skill-cohort deploy (DEFAULT_SINCE) for:
- EXIT lines tagged [v16_skill_decile] with pnl=<bps> ($<usd>)  -> clean copy round-trips
- LIQUIDATION lines (closedPnl)                                 -> the tail / forced closes
Outputs: n exits, mean/median bps, win-rate, sum $, n liqs + $, trades/day, and vs the 16bps / $51-mo baseline.

Run: ~/miniforge3/envs/quants-lab/bin/python scripts/skill_edge_tracker.py
"""
import re
import sys
from datetime import datetime

LOG = "/tmp/ql-v12-copy-trader-launchd.log"
DEPLOY = "2026-06-14 23:11:00"   # skill cohort live
BASELINE_BPS = 16.0              # old PnL cohort live effective edge
BASELINE_MO = 51.0

EXIT_RE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*EXIT: (\S+) (BUY|SELL) entry=([\d.]+) .*pnl=([+-][\d.]+)bp \(\$([+-][\d.]+)\).*\[(\w+)\]")
ENTRY_RE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*ENTRY FILLED \(IOC\): (\S+) (BUY|SELL) [\d.]+ @ ([\d.]+)")
LIQ_RE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*LIQUIDATION: (\S+) \S+ sz=\S+ @ \S+ closedPnl=([+-]?[\d.]+)")


def parse_ts(s):
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def main():
    since = parse_ts(sys.argv[1] if len(sys.argv) > 1 else DEPLOY)
    post_entries = set()   # (coin, round(entry_px,6)) of TRUE post-deploy skill entries
    exits, carried, liqs = [], [], []
    with open(LOG) as f:
        for line in f:
            m = ENTRY_RE.search(line)
            if m and parse_ts(m.group(1)) >= since:
                post_entries.add((m.group(2), round(float(m.group(4)), 6)))
                continue
            m = EXIT_RE.search(line)
            if m:
                ts = parse_ts(m.group(1))
                if ts >= since:
                    coin, entry_px = m.group(2), round(float(m.group(4)), 6)
                    # TRUE skill round-trip = exit whose entry price matches a post-deploy entry on that coin
                    true_skill = (coin, entry_px) in post_entries
                    row = (ts, coin, m.group(3), float(m.group(5)), float(m.group(6)))
                    (exits if true_skill else carried).append(row)
                continue
            m = LIQ_RE.search(line)
            if m:
                ts = parse_ts(m.group(1))
                if ts >= since:
                    liqs.append((ts, m.group(2), float(m.group(3))))

    import statistics as st
    print(f"=== SKILL COHORT LIVE EDGE (since {since}) ===")
    if exits:
        bps = [e[3] for e in exits]
        usd = [e[4] for e in exits]
        span_h = (exits[-1][0] - exits[0][0]).total_seconds() / 3600 or 1
        wins = sum(1 for b in bps if b > 0)
        print(f"  clean round-trips (skill exits): n={len(exits)}")
        print(f"  mean edge: {st.mean(bps):+.1f} bps | median: {st.median(bps):+.1f} | win-rate: {wins}/{len(exits)} = {wins/len(exits)*100:.0f}%")
        print(f"  sum realized: ${sum(usd):+.2f} | best {max(bps):+.0f} worst {min(bps):+.0f} bps")
        print(f"  span {span_h:.1f}h -> ~{len(exits)/span_h*24:.0f} clean trips/day")
        print(f"  per-coin: " + ", ".join(f"{e[1]} {e[3]:+.0f}" for e in exits[-8:]))
    else:
        print("  no clean skill round-trips yet.")
    if carried:
        print(f"  CARRIED/unknown exits (old book, NOT skill edge): n={len(carried)} "
              f"sum=${sum(c[4] for c in carried):+.2f}")
    if liqs:
        print(f"  LIQUIDATIONS: n={len(liqs)} sum=${sum(l[2] for l in liqs):+.2f} | coins: {[l[1] for l in liqs]}")
    print(f"\n  BASELINE (old PnL cohort): ~{BASELINE_BPS:.0f} bps/trade, ${BASELINE_MO:.0f}/mo")
    if exits:
        e = st.mean([x[3] for x in exits])
        verdict = "ABOVE baseline" if e > BASELINE_BPS else "below baseline"
        print(f"  read: skill clean-exit mean {st.mean([x[3] for x in exits]):+.0f}bps is {verdict} "
              f"(n={len(exits)}; need ~50-100 for trust). Liqs are the tail to watch.")


if __name__ == "__main__":
    main()
