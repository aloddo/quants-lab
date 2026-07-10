#!/usr/bin/env python3
"""Alberto backfill directive (2026-07-06 voice): instead of copying only fresh opens,
also BACKFILL a leader's currently-held position (enter mid-trade) and ride to their close.
His thesis: if they still hold, they expect continuation -> money on the table if we skip it.

This tests whether entering MID-TRADE still pays after our costs, vs entering at the fresh open.
Reuses the SAME machinery as revalidate/holdout (roundtrips + execution_model, per-coin slip + fee,
ALL coins). For each leader round-trip [entry_ts -> exit_ts], we price the edge of entering at
f in {0 (fresh open), 0.25, 0.50, 0.75} of the way through the hold and exiting at the leader's close.

READ-ONLY. Output: per-fraction mean net bps + survival rate. If mid-trade edge stays positive,
Alberto's backfill is safe to wire at small size.
"""
import sys, json, argparse
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, set_latency_ms, apply_entry, apply_exit, set_slip_default_bps
from revalidate_api_execmodel import pull_api

FEE_T = fee_rt(maker=False)
CAP = 500.0 / 1e4
FRACS = [0.0, 0.25, 0.50, 0.75]


def edge_at_fraction(rts, lo, hi, lat, frac):
    """Enter at frac of the way through each hold; exit at leader close. Net bps list."""
    nets = []
    for c, dir_, ets, xts, evw, xvw, g in rts:
        if not (lo <= ets < hi):
            continue
        if xts <= ets:
            continue
        entry_ts = ets + int(frac * (xts - ets))   # 0 = fresh open, >0 = mid-trade backfill
        em = S.mark_at(c, entry_ts + lat)
        xm = S.mark_at(c, xts + lat)
        if em is None or xm is None or em <= 0:
            continue
        ef = apply_entry(c, em, dir_ > 0)
        xf = apply_exit(c, xm, dir_ > 0)
        og = max(-CAP, min(CAP, dir_ * (xf - ef) / ef))
        nets.append(og - FEE_T)
    return nets


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets-file", required=True)
    ap.add_argument("--start", default="2026-05-17")
    ap.add_argument("--end", default="2026-06-01")
    ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--slip-default", type=float, default=0.5)
    ap.add_argument("--sleep", type=float, default=1.0)
    args = ap.parse_args()
    set_latency_ms(args.latency_s * 1000); lat = args.latency_s * 1000
    set_slip_default_bps(args.slip_default)
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    start, end = ms(args.start), ms(args.end)
    import time as _t
    wlist = [l.strip().lower() for l in open(args.wallets_file) if l.strip() and not l.startswith('#')]

    all_rts = []
    for wi, w in enumerate(wlist):
        if wi and args.sleep:
            _t.sleep(args.sleep)
        try:
            t, integ = pull_api(w, start, end)
        except RuntimeError as e:
            print(f"{w[:10]} PULL-FAILED: {e}", file=sys.stderr); continue
        rts = roundtrips(t)
        all_rts.extend(rts)
        print(f"{w[:10]}: {len(rts)} round-trips", file=sys.stderr)

    print("\n" + "=" * 66)
    print(f"BACKFILL EDGE vs FRESH-OPEN  {args.start}..{args.end}")
    print("enter at fraction f of the hold, exit at leader close (net of costs):")
    print(f"{'f':>6} {'n':>5} {'mean_bps':>9} {'median':>8} {'win%':>6} {'P(mean>0)':>10}")
    rng = np.random.default_rng(7)
    for f in FRACS:
        nets = edge_at_fraction(all_rts, start, end, lat, f)
        if not nets:
            print(f"{f:>6} {0:>5}  no data"); continue
        a = np.array(nets) * 1e4
        # bootstrap P(mean>0)
        boot = np.array([rng.choice(a, len(a), replace=True).mean() for _ in range(5000)])
        label = "OPEN" if f == 0.0 else f"{int(f*100)}%"
        print(f"{label:>6} {len(a):>5} {a.mean():>+9.1f} {np.median(a):>+8.1f} "
              f"{100*(a>0).mean():>5.0f}% {(boot>0).mean():>10.3f}")
    print("=" * 66)
    print("Read: if the 25/50/75% rows stay clearly >0, backfilling mid-trade pays -> Alberto's")
    print("continuation thesis holds and backfill is safe to wire at small size.")


if __name__ == "__main__":
    main()
