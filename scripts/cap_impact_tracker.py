#!/usr/bin/env python
"""
cap_impact_tracker.py -- per-day tally of FILL vs gate-blocks (NETX / GROSS / KNET / MARGIN) from the engine
log. Purpose: capture the gross-3.5 BASELINE before gross 4.0 ships (w26 priority #1: measure the turnover
lift). After gross_entry_gate_x 3.5->4.0 deploys, re-run to compare gross-block rate + fill rate before/after.
Time-sensitive: the clean pre-deploy window can't be recreated post-deploy.

Read-only. Run: ~/miniforge3/envs/quants-lab/bin/python scripts/cap_impact_tracker.py
"""
import re
from collections import defaultdict

LOG = "/tmp/ql-v12-copy-trader-launchd.log"
PATS = {
    "FILL": re.compile(r"FILL: "),
    "NETX_blk": re.compile(r"NETX CAP: rejected"),
    "GROSS_blk": re.compile(r"GROSS GATE BLOCKED"),
    "KNET_blk": re.compile(r"KNET GATE: rejected"),
    "MARGIN_blk": re.compile(r"Margin BLOCKED"),
    "KNET_bypass": re.compile(r"KNET BYPASS"),
}
DAY = re.compile(r"^(\d{4}-\d\d-\d\d) ")


def main():
    by_day = defaultdict(lambda: defaultdict(int))
    with open(LOG) as fh:
        for line in fh:
            m = DAY.match(line)
            if not m:
                continue
            d = m.group(1)
            for k, p in PATS.items():
                if p.search(line):
                    by_day[d][k] += 1
    days = sorted(by_day)
    print("=== cap-impact per-day tally (gross-3.5 BASELINE; compare after gross 4.0 ships) ===")
    print(f"{'day':<12}{'FILL':>6}{'NETX_blk':>9}{'GROSS_blk':>10}{'KNET_blk':>9}{'MARGIN':>8}{'KN_byp':>7}{'gross_blk%':>11}")
    for d in days:
        r = by_day[d]
        attempts = r["FILL"] + r["NETX_blk"] + r["GROSS_blk"] + r["KNET_blk"] + r["MARGIN_blk"]
        gpct = r["GROSS_blk"] / attempts * 100 if attempts else 0
        print(f"{d:<12}{r['FILL']:>6}{r['NETX_blk']:>9}{r['GROSS_blk']:>10}{r['KNET_blk']:>9}{r['MARGIN_blk']:>8}{r['KNET_bypass']:>7}{gpct:>10.0f}%")
    # recent-window summary (last 3 days = current gross-3.5 baseline)
    recent = days[-3:]
    tot = defaultdict(int)
    for d in recent:
        for k, v in by_day[d].items():
            tot[k] += v
    att = tot["FILL"] + tot["NETX_blk"] + tot["GROSS_blk"] + tot["KNET_blk"] + tot["MARGIN_blk"]
    print(f"\nBASELINE (last 3d {recent[0]}..{recent[-1]}, gross=3.5x): FILL={tot['FILL']} | "
          f"GROSS-blocks={tot['GROSS_blk']} ({tot['GROSS_blk']/att*100 if att else 0:.0f}% of attempts) | "
          f"NETX-blocks={tot['NETX_blk']} | KNET-blocks={tot['KNET_blk']}")
    print("READ: GROSS-blocks are the trades gross 4.0 would ADMIT. After deploy, GROSS-blocks should drop sharply")
    print("and FILL should rise. NETX-blocks (the protective cap) should be ~unchanged. KNET-bypasses track the")
    print("de-risk feature. Re-run post-deploy for the before/after that proves the +8-11% turnover lift live.")


if __name__ == "__main__":
    main()
