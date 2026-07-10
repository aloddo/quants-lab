#!/usr/bin/env python3
"""Live round-trip ledger for the V18 copy-trader.

Parses realized EXIT lines from the launchd log into a per-round-trip ledger and
prints summary stats: count, gross-bps distribution, $ realized, and the fraction
of trips that CLEAR the HL round-trip taker fee (the core economics gate).

Entry on V18 is ALWAYS an IOC taker leg (4.32bp). The exit leg is either a maker
fill (1.44bp) or an IOC-fallback taker leg (4.32bp). So the per-trip RT fee is:
  - exit via maker  -> 4.32 + 1.44 = 5.76bp RT
  - exit via IOC    -> 4.32 + 4.32 = 8.64bp RT
The exit-execution mix (maker vs IOC) is the single biggest lever on net edge:
on a thin +10bp leader move, paying taker on the exit (8.64bp) vs maker (5.76bp)
is the difference between ~+1.5bp and ~+4.3bp net. This tool measures that mix so
the live distribution can be compared to the cohort walk-forward expectation
BEFORE proposing any change to the exit path (Rule 11 + codex-mandatory).

Read-only on the log. No memory risk (line-streamed). Does not touch the live bot.

Usage:
  python tools/live_rt_ledger.py                 # default launchd log
  python tools/live_rt_ledger.py --log /path     # explicit log
  python tools/live_rt_ledger.py --csv out.csv   # also dump rows
"""
import argparse
import re
import statistics as st

LOG_DEFAULT = "/tmp/ql-v12-copy-trader-launchd.log"
ENTRY_TAKER_BPS = 4.32  # HL one-way taker (entry is always IOC taker)
EXIT_MAKER_BPS = 1.44   # HL one-way maker
EXIT_TAKER_BPS = 4.32   # HL one-way taker (IOC fallback)
RT_MAKER_EXIT = ENTRY_TAKER_BPS + EXIT_MAKER_BPS  # 5.76
RT_TAKER_EXIT = ENTRY_TAKER_BPS + EXIT_TAKER_BPS  # 8.64

# IOC-fallback realized exit (engine line 2717): "EXIT:" / "EXIT(PARTIAL):"
# 2026-06-29 01:12:34 ... EXIT: BTC BUY entry=59271.0 exit=59331.0 filled=0.00084/0.00084 pnl=+10.1bp ($+0.0505) acct=...
IOC_RE = re.compile(
    r"^(?P<ts>\S+ \S+).*?EXIT(?:\(PARTIAL\))?: (?P<coin>\S+) (?P<side>\w+) "
    r"entry=(?P<entry>[\d.]+) exit=(?P<exit>[\d.]+) "
    r"filled=(?P<fill>[\d./]+) pnl=(?P<bps>[+-][\d.]+)bp "
    r"\(\$(?P<dollar>[+-][\d.]+)\)"
)
# Maker-immediate realized exit (engine lines 2550 + 3193): "EXIT FILLED (MAKER):"
# filled=.../... is present in one variant and absent in the other -> optional.
MAKER_RE = re.compile(
    r"^(?P<ts>\S+ \S+).*?EXIT FILLED \(MAKER\): (?P<coin>\S+) (?P<side>\w+) "
    r"entry=(?P<entry>[\d.]+) exit=(?P<exit>[\d.]+) "
    r"(?:filled=(?P<fill>[\d./]+) )?pnl=(?P<bps>[+-][\d.]+)bp "
    r"\(\$(?P<dollar>[+-][\d.]+)\)"
)
# Maker POST (engine line 2531, NOT a fill): "EXIT MAKER: {coin} {SIDE} {sz} @ {px}".
# Presence of this before an IOC exit on the same coin => the maker was TRIED and
# timed out (structural / unrecoverable on a leader-flip exit), NOT a direct force-IOC.
MAKER_POST_RE = re.compile(r"EXIT MAKER: (?P<coin>\S+) ")


def _row(m, exit_type, maker_attempted=None):
    d = m.groupdict()
    rt_fee = RT_MAKER_EXIT if exit_type == "maker" else RT_TAKER_EXIT
    return {
        "ts": d["ts"],
        "coin": d["coin"],
        "side": d["side"],
        "entry": float(d["entry"]),
        "exit": float(d["exit"]),
        "gross_bps": float(d["bps"]),
        "dollar": float(d["dollar"]),
        "exit_type": exit_type,
        "rt_fee_bps": rt_fee,
        "net_bps": float(d["bps"]) - rt_fee,
        # for IOC rows: True if a maker rest was posted on this coin first (tried+timed
        # out => not recoverable); False => direct force-IOC (recoverable in principle).
        "maker_attempted": maker_attempted,
    }


def parse(log_path):
    rows = []
    pending_maker = {}  # coin -> True while a maker rest is outstanding (not yet filled/exited)
    with open(log_path, "r", errors="ignore") as fh:
        for line in fh:
            mp = MAKER_POST_RE.search(line)
            if mp and "EXIT FILLED" not in line:
                pending_maker[mp.group("coin")] = True
                continue
            if "pnl=" not in line:
                continue
            if "EXIT FILLED (MAKER)" in line:
                m = MAKER_RE.search(line)
                if m:
                    pending_maker.pop(m.group("coin"), None)
                    rows.append(_row(m, "maker"))
            elif "EXIT:" in line or "EXIT(PARTIAL):" in line:
                m = IOC_RE.search(line)
                if m:
                    coin = m.group("coin")
                    attempted = pending_maker.pop(coin, False)
                    rows.append(_row(m, "ioc", maker_attempted=bool(attempted)))
    return rows


def _stat(xs):
    if not xs:
        return "n/a"
    if len(xs) == 1:
        return f"{xs[0]:+.1f}"
    return f"mean {st.mean(xs):+.1f} / median {st.median(xs):+.1f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default=LOG_DEFAULT)
    ap.add_argument("--csv", default=None)
    args = ap.parse_args()

    rows = parse(args.log)
    n = len(rows)
    print(f"=== LIVE ROUND-TRIP LEDGER ({args.log}) ===")
    if n == 0:
        print("0 realized round-trips yet.")
        return

    gross = [r["gross_bps"] for r in rows]
    net = [r["net_bps"] for r in rows]
    dollars = [r["dollar"] for r in rows]
    n_maker = sum(1 for r in rows if r["exit_type"] == "maker")
    n_ioc = n - n_maker
    # "clears" = net positive after the trip's ACTUAL RT fee (exit-type aware).
    clears = sum(1 for r in rows if r["net_bps"] >= 0)

    print(f"trips={n}  cum_$={sum(dollars):+.4f}")
    print(f"gross_bps: {_stat(gross)}  (min {min(gross):+.1f} max {max(gross):+.1f})")
    print(f"net_bps (exit-type-aware RT fee): {_stat(net)}")
    print(f"net-positive trips: {clears}/{n} = {100*clears/n:.0f}%")
    print(f"exit mix: maker={n_maker} ({100*n_maker/n:.0f}%, RT {RT_MAKER_EXIT}bp)  "
          f"ioc={n_ioc} ({100*n_ioc/n:.0f}%, RT {RT_TAKER_EXIT}bp)")
    if n_maker and n_ioc:
        maker_net = [r["net_bps"] for r in rows if r["exit_type"] == "maker"]
        ioc_net = [r["net_bps"] for r in rows if r["exit_type"] == "ioc"]
        print(f"  net_bps by exit: maker={_stat(maker_net)}  ioc={_stat(ioc_net)}")
    # IOC split: maker-tried-then-timeout (structural, leader-flip moved price away ->
    # NOT recoverable by 'force maker') vs direct force-IOC (recoverable in principle).
    if n_ioc:
        ioc_tried = sum(1 for r in rows if r["exit_type"] == "ioc" and r["maker_attempted"])
        ioc_direct = n_ioc - ioc_tried
        print(f"  ioc split: maker-tried-then-timeout={ioc_tried} (structural)  "
              f"direct-force-ioc={ioc_direct} (recoverable)")
    print("expectation gate: cohort walk-forward median +33.6bps net -> live should converge there.")
    print("lever: IOC exits where a maker was TRIED+timed out are NOT fixed by 'force maker' "
          "(leader-flip moves price away); the real levers are maker-price-aggression and the 60s window.")
    print("--- recent ---")
    for r in rows[-10:]:
        flag = "OK" if r["net_bps"] >= 0 else "<fee"
        print(f"  {r['ts']}  {r['coin']:<14} {r['side']:<5} "
              f"gross={r['gross_bps']:+6.1f}bp  net={r['net_bps']:+6.1f}bp  "
              f"exit={r['exit_type']:<5} ${r['dollar']:+.4f}  {flag}")

    if args.csv:
        import csv
        with open(args.csv, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"wrote {args.csv}")


if __name__ == "__main__":
    main()
