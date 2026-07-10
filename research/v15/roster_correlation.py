#!/usr/bin/env python3
"""Roster concurrency/correlation quantifier (Fable flag: ~55% correlated, saturation risk).
Reuses canonical API loader from revalidate_api_execmodel. For the gate1_v4 10-wallet roster it
measures how often wallets hold the SAME coin+side SIMULTANEOUSLY -- the driver of effective
diversification and per-coin saturation, which bounds safe per-wallet size ($50 vs $75).

Method: from complete API fills -> roundtrips -> position-holding intervals (coin, sign, ets, xts).
On a time grid over the window, per coin compute net signed wallet count (how many wallets long minus
short). Peak same-side stack per coin = worst-case concurrent exposure. Mean pairwise co-holding
fraction = correlation proxy. NO capital, read-only.
"""
import sys, json, argparse, itertools
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
from revalidate_api_execmodel import pull_api
from fidelity_replay import roundtrips

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config/copy_trader_wallets_gate1_v4.json")
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-06-01")
    ap.add_argument("--sleep", type=float, default=0.5)
    ap.add_argument("--grid-min", type=int, default=60, help="grid resolution in minutes")
    args = ap.parse_args()
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    start, end = ms(args.start), ms(args.end)
    cfg = json.load(open(args.config))
    wallets = [w.lower() for w in cfg["wallets"].keys()]
    print(f"roster: {len(wallets)} wallets | window {args.start}..{args.end} | grid {args.grid_min}min")

    # per-wallet holding intervals: list of (coin, sign, ets, xts)
    import time as _t
    holds = {}
    for wi, w in enumerate(wallets):
        if wi and args.sleep:
            _t.sleep(args.sleep)
        try:
            t, integ = pull_api(w, start, end)
        except RuntimeError as e:
            print(f"  {w[:10]} PULL-FAILED: {e}")
            continue
        rts = roundtrips(t)
        ivals = [(c, 1 if dir_ > 0 else -1, ets, xts) for (c, dir_, ets, xts, evw, xvw, g) in rts]
        holds[w] = ivals
        print(f"  {w[:10]} apiFills={len(t):>6} roundtrips={len(rts):>5} integ={integ:.3f}")

    if len(holds) < 2:
        print("insufficient wallets pulled; abort")
        return

    # time grid
    grid = np.arange(start, end, args.grid_min * 60 * 1000)
    # per (coin) per gridpoint: signed wallet count. Build coin universe.
    coins = sorted(set(c for iv in holds.values() for (c, s, a, b) in iv))
    # For each wallet build fast lookup of active (coin,sign) at time via interval membership.
    # peak same-side stack per coin, and pairwise co-holding fraction.
    peak_stack = {}      # coin -> max simultaneous same-side wallet count (abs)
    stack_time = {}      # coin -> fraction of grid where >=2 wallets same side
    # pairwise co-holding: fraction of active gridpoints where pair shares coin+side
    pair_co = {p: 0 for p in itertools.combinations(sorted(holds.keys()), 2)}
    pair_active = {p: 0 for p in pair_co}

    # precompute per wallet: sorted intervals per coin
    wide = {w: {} for w in holds}
    for w, iv in holds.items():
        for (c, s, a, b) in iv:
            wide[w].setdefault(c, []).append((a, b, s))

    def sign_at(w, c, t):
        for (a, b, s) in wide[w].get(c, []):
            if a <= t < b:
                return s
        return 0

    for c in coins:
        peak = 0
        n_stacked = 0
        for t in grid:
            signs = [sign_at(w, c, t) for w in holds]
            net = sum(signs)
            longs = sum(1 for s in signs if s > 0)
            shorts = sum(1 for s in signs if s < 0)
            same = max(longs, shorts)
            peak = max(peak, same)
            if same >= 2:
                n_stacked += 1
        peak_stack[c] = peak
        stack_time[c] = n_stacked / len(grid)

    # pairwise co-holding over grid
    wl = sorted(holds.keys())
    for t in grid:
        # per wallet active coin->sign map at t
        act = {}
        for w in wl:
            m = {}
            for c in wide[w]:
                s = sign_at(w, c, t)
                if s != 0:
                    m[c] = s
            act[w] = m
        for (w1, w2) in pair_co:
            a1, a2 = act[w1], act[w2]
            if a1 and a2:  # both active somewhere
                pair_active[(w1, w2)] += 1
                shared = sum(1 for c in a1 if c in a2 and a1[c] == a2[c])
                if shared:
                    pair_co[(w1, w2)] += 1

    print("\n=== PEAK SAME-SIDE STACK per coin (wallets holding same coin+side at once) ===")
    top = sorted(peak_stack.items(), key=lambda x: -x[1])[:15]
    for c, pk in top:
        print(f"  {c:<10} peak_same_side={pk:>2}  stacked>=2_timefrac={stack_time[c]:.3f}")

    pair_fracs = [pair_co[p] / pair_active[p] for p in pair_co if pair_active[p] > 0]
    print("\n=== PAIRWISE CO-HOLDING (fraction of both-active time sharing a coin+side) ===")
    if pair_fracs:
        print(f"  mean={np.mean(pair_fracs):.3f}  median={np.median(pair_fracs):.3f}  "
              f"max={np.max(pair_fracs):.3f}  min={np.min(pair_fracs):.3f}  npairs={len(pair_fracs)}")
    max_peak = max(peak_stack.values()) if peak_stack else 0
    worst_stack_coin = max(peak_stack, key=peak_stack.get) if peak_stack else None
    print(f"\nSUMMARY: max concurrent same-side stack = {max_peak} wallets (coin {worst_stack_coin}). "
          f"At $50/wallet that peaks ~${50*max_peak} gross on one coin+side.")

if __name__ == "__main__":
    main()
