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
from execution_model import fee_rt, set_latency_ms

FEE_TAKER = fee_rt(maker=False)
FEE_MAKER = fee_rt(maker=True)


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
            else:                                          # reduce / close
                cl = min(abs(s), abs(pos))
                ex += cl * p; exs += cl; pos += s
                if abs(pos) < 1e-9 and es > 0 and exs > 0:
                    evw = en / es; xvw = ex / exs
                    g = cdir * (xvw - evw) / evw
                    out.append((c, cdir, ets, ts, evw, xvw, g))
                    pos = 0.0; en = es = ex = exs = 0.0
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-12-01")
    ap.add_argument("--end", default="2026-05-17")
    ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--min-rt", type=int, default=20)
    ap.add_argument("--universe-file", default="app/data/v15/m01_nonerroring_wallets.txt")
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
    print(f"{len(wf)} wallets with fills; reconstructing round-trips + fidelity replay ...")
    rows = []
    for w, fl in wf.items():
        fl.sort(key=lambda x: x[0])
        rts = roundtrips(fl)
        if len(rts) < args.min_rt:
            continue
        their, ours_t, ours_m = [], [], []
        for c, dir_, ets, xts, evw, xvw, g in rts:
            their.append(g)
            ent = S.mark_at(c, ets + lat); ex = S.mark_at(c, xts + lat)
            if ent is None or ex is None or ent <= 0:
                continue
            og = dir_ * (ex - ent) / ent
            ours_t.append(og - FEE_TAKER); ours_m.append(og - FEE_MAKER)
        if not ours_t:
            continue
        rows.append({"wallet": w, "n_rt": len(rts),
                     "their_gross_bps": np.mean(their) * 1e4,
                     "our_taker_bps": np.mean(ours_t) * 1e4,
                     "our_maker_bps": np.mean(ours_m) * 1e4})
    df = pd.DataFrame(rows)
    print(f"\n=== HIGH-FIDELITY REPLAY ({len(df)} wallets, >= {args.min_rt} round-trips) ===")
    print(f"ALL wallets: their_gross {df.their_gross_bps.mean():.1f} | our_taker {df.our_taker_bps.mean():.1f} | "
          f"our_maker {df.our_maker_bps.mean():.1f} bps/trade")
    prof = df[df.their_gross_bps > 0]
    print(f"\nPROFITABLE wallets only ({len(prof)} = {len(prof)/len(df)*100:.0f}% of universe, their_gross>0):")
    print(f"  their_gross {prof.their_gross_bps.mean():.1f} | our_taker {prof.our_taker_bps.mean():.1f} | "
          f"our_maker {prof.our_maker_bps.mean():.1f} bps/trade")
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
