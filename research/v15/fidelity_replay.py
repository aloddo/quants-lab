#!/usr/bin/env python3
"""HIGH-FIDELITY copy replay (Alberto: just do what they do).

For each wallet, reconstruct their ACTUAL round-trips from m02 fills (position 0 -> nonzero -> 0:
entry vwap, exit vwap, side, their real hold). Then copy with FIDELITY: enter at the mark a small
latency after their entry, exit at the mark a small latency after their exit (their real exit, no fixed
hold), our fee on top. Measure how much of THEIR realized PnL we keep -- maker and taker fee bounds.
No fixed holds, no posting games. Answers: if they are profitable, are we profitable copying them?

Run: python research/v15/fidelity_replay.py --start 2025-12-01 --end 2026-05-17 --latency-s 2
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, set_latency_ms, slip_oneway

def roundtrips(fills):
    """fills: list of (ts, coin, signed_size, price) ts-sorted. Yield round-trips per coin:
    (dir, entry_ts, exit_ts, entry_vwap, exit_vwap, their_gross)."""
    from collections import defaultdict
    bycoin = defaultdict(list)
    for ts, c, s, p in fills:
        bycoin[c].append((ts, s, p))
    out = []
    for c, fl in bycoin.items():
        pos = 0.0; en = es = ex = exs = 0.0; ets = None; cdir = 0
        for ts, s, p in fl:
            if p is None or p <= 0 or s is None or s == 0:
                continue
            if pos == 0 or (pos > 0) == (s > 0):           # open / add
                if pos == 0:
                    ets = ts; cdir = 1 if s > 0 else -1; en = es = ex = exs = 0.0
                en += abs(s) * p; es += abs(s); pos += s
            else:                                          # reduce / close / reverse
                cl = min(abs(s), abs(pos))
                ex += cl * p; exs += cl; pos += s
                closed_or_reversed = abs(pos) < 1e-9 or (pos > 0) != (cdir > 0)
                if closed_or_reversed and es > 0 and exs > 0:
                    evw = en / es; xvw = ex / exs
                    g = cdir * (xvw - evw) / evw
                    out.append((c, cdir, ets, ts, evw, xvw, g))
                    # A reversal closes the old position and opens the residual
                    # opposite leg at this same fill price.
                    residual = pos
                    en = es = ex = exs = 0.0
                    if abs(residual) < 1e-9:
                        pos = 0.0
                    else:
                        pos = residual; ets = ts; cdir = 1 if residual > 0 else -1
                        en = abs(residual) * p; es = abs(residual)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-05-17")
    ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--min-rt", type=int, default=20)
    ap.add_argument("--liquid-only", action="store_true", help="restrict to l2_calib liquid coins (kills microcap mark artifacts).")
    ap.add_argument("--cap-bps", type=float, default=500.0, help="clip per-trade net to +/- this (kills reconstruction outliers).")
    ap.add_argument("--universe-file", default="app/data/v15/m01_universe_20k_wallets.txt")
    args = ap.parse_args()
    set_latency_ms(args.latency_s * 1000)
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    start, end, lat = ms(args.start), ms(args.end), args.latency_s * 1000

    uni = set(l.strip().lower() for l in open(args.universe_file) if l.strip() and not l.startswith("#"))
    print(f"loading m02 fills for {len(uni)} wallets {args.start}..{args.end} ...")
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(str(S.M02_ACTIONS))
    from collections import defaultdict
    wf = defaultdict(list)
    for b in pf.iter_batches(batch_size=1_000_000, columns=["wallet", "coin", "ts", "signed_size", "price"]):
        d = b.to_pydict()
        for i in range(len(d["wallet"])):
            w = d["wallet"][i]; t = d["ts"][i]
            if w in uni and start <= t <= end:
                wf[w].append((t, d["coin"][i], d["signed_size"][i], d["price"][i]))
    # liquid coins (l2_calib) to kill microcap mark-reconstruction artifacts
    import json
    liquid = set(json.load(open(S._DATA / "l2_calib_10coin.json")).keys()) if args.liquid_only else None
    cap = args.cap_bps / 1e4
    print(f"{len(wf)} wallets with fills; reconstructing round-trips (liquid_only={args.liquid_only}, "
          f"cap={args.cap_bps}bps) ...")
    rows = []
    for w, fl in wf.items():
        fl.sort(key=lambda x: x[0])
        rts = roundtrips(fl)
        their, ours_t, ours_m = [], [], []
        for c, dir_, ets, xts, evw, xvw, g in rts:
            if liquid is not None and c not in liquid:
                continue
            ent = S.mark_at(c, ets + lat); ex = S.mark_at(c, xts + lat)
            if ent is None or ex is None or ent <= 0:
                continue
            og = max(-cap, min(cap, dir_ * (ex - ent) / ent))   # cap kills reconstruction outliers
            their.append(max(-cap, min(cap, g)))
            slip_rt = 2.0 * slip_oneway(c)
            ours_t.append(og - fee_rt(maker=False, coin=c) - slip_rt)
            # Maker is an optimistic fill-probability bound; no crossing slip.
            ours_m.append(og - fee_rt(maker=True, coin=c))
        if len(ours_t) < args.min_rt:
            continue
        rows.append({"wallet": w, "n_rt": len(ours_t),
                     "their_gross_bps": np.mean(their) * 1e4,
                     "our_taker_bps": np.mean(ours_t) * 1e4,
                     "our_maker_bps": np.mean(ours_m) * 1e4})
    df = pd.DataFrame(rows)
    md = lambda s: f"mean {s.mean():.1f} / MEDIAN {s.median():.1f}"
    print(f"\n=== HIGH-FIDELITY REPLAY ({len(df)} wallets, >= {args.min_rt} rt; liquid_only={args.liquid_only}) ===")
    print(f"ALL: their_gross [{md(df.their_gross_bps)}] | our_taker [{md(df.our_taker_bps)}] | our_maker [{md(df.our_maker_bps)}]")
    print(f"corr(their_gross, our_taker): {df.their_gross_bps.corr(df.our_taker_bps):.3f}  (should be high if faithful)")
    prof = df[df.their_gross_bps > 0]
    print(f"\nPROFITABLE wallets ({len(prof)} = {len(prof)/len(df)*100:.0f}%, their_gross>0):")
    print(f"  their_gross [{md(prof.their_gross_bps)}] | our_taker [{md(prof.our_taker_bps)}] | our_maker [{md(prof.our_maker_bps)}]")
    print(f"  of profitable wallets, our_maker stays >0 for: {(prof.our_maker_bps>0).mean()*100:.0f}% | "
          f"our_taker >0 for: {(prof.our_taker_bps>0).mean()*100:.0f}%")
    print(f"  fidelity (our_maker / their_gross): {prof.our_maker_bps.mean()/prof.their_gross_bps.mean():.2f}")
    # top wallets by their gross
    print("\nTOP 8 wallets by their_gross (their -> our_maker -> our_taker):")
    for _, r in df.sort_values("their_gross_bps", ascending=False).head(8).iterrows():
        print(f"  {r.wallet[:12]} n={int(r.n_rt)} their {r.their_gross_bps:.0f} -> maker {r.our_maker_bps:.0f} -> taker {r.our_taker_bps:.0f}")
    df.to_parquet("app/data/v15/fidelity_replay.parquet")


if __name__ == "__main__":
    main()
