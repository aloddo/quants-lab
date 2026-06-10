#!/usr/bin/env python3
"""OOS high-fidelity copy: select wallets on TRAIN, faithfully copy them on TEST (deployability gate).

In-sample, faithful copy of profitable wallets keeps +22.8/+28.5bps (taker/maker). Deployable only if
we can pick those wallets in advance. This: reconstruct each wallet's round-trips, compute our faithful
maker copy edge on TRAIN (entry_ts<split) and TEST (>=split), select the train cohort, measure their
TEST faithful edge vs random. No fixed holds / posting games (Alberto).

Run: python research/v15/fidelity_oos.py --start 2025-12-01 --split 2026-03-15 --end 2026-05-17
"""
from __future__ import annotations
import argparse, sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, set_latency_ms
FEE_M = fee_rt(maker=True); FEE_T = fee_rt(maker=False)


def edge(rts, lo, hi, lat, fee):
    nets = []
    for c, dir_, ets, xts, evw, xvw, g in rts:
        if not (lo <= ets < hi):
            continue
        ent = S.mark_at(c, ets + lat); ex = S.mark_at(c, xts + lat)
        if ent is None or ex is None or ent <= 0:
            continue
        nets.append(dir_ * (ex - ent) / ent - fee)
    if not nets:
        return (None, 0, None)
    a = np.array(nets)
    return (a.mean() * 1e4, len(a), a.std(ddof=1) * 1e4 if len(a) > 1 else None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2025-12-01"); ap.add_argument("--split", default="2026-03-15")
    ap.add_argument("--end", default="2026-05-17"); ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--min-rt", type=int, default=15)
    ap.add_argument("--universe-file", default="app/data/v15/m01_nonerroring_wallets.txt")
    args = ap.parse_args()
    set_latency_ms(args.latency_s * 1000)
    ms = lambda d: int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
    start, split, end, lat = ms(args.start), ms(args.split), ms(args.end), args.latency_s * 1000
    uni = set(l.strip().lower() for l in open(args.universe_file) if l.strip() and not l.startswith("#"))
    print(f"loading m02 fills for {len(uni)} wallets ...")
    import pyarrow.parquet as pq
    from collections import defaultdict
    pf = pq.ParquetFile(str(S.M02_ACTIONS)); wf = defaultdict(list)
    for b in pf.iter_batches(batch_size=1_000_000, columns=["wallet", "coin", "ts", "signed_size", "price"]):
        d = b.to_pydict()
        for i in range(len(d["wallet"])):
            w = d["wallet"][i]; t = d["ts"][i]
            if w in uni and start <= t <= end:
                wf[w].append((t, d["coin"][i], d["signed_size"][i], d["price"][i]))
    rows = []
    for w, fl in wf.items():
        fl.sort(key=lambda x: x[0]); rts = roundtrips(fl)
        trm, trn, trs = edge(rts, start, split, lat, FEE_M)
        tem, ten, _ = edge(rts, split, end, lat, FEE_M)
        tet, _, _ = edge(rts, split, end, lat, FEE_T)
        trt, _, _ = edge(rts, start, split, lat, FEE_T)
        if trm is not None and tem is not None and trn >= args.min_rt and ten >= args.min_rt:
            tstat = (trt / trs * np.sqrt(trn)) if (trs and trs > 0) else 0.0   # train TAKER t-stat
            rows.append({"wallet": w, "train_maker": trm, "train_taker": trt, "train_tstat": tstat,
                         "train_n": trn, "test_maker": tem, "test_taker": tet, "test_n": ten})
    df = pd.DataFrame(rows)
    print(f"\n=== OOS FAITHFUL COPY ({len(df)} wallets, >= {args.min_rt} round-trips both windows) ===")
    print(f"corr(train,test) maker edge: {df.train_maker.corr(df.test_maker):.3f}")
    df = df.sort_values("train_maker", ascending=False).reset_index(drop=True)
    dec = max(5, len(df) // 10)
    top = df.head(dec)
    prof = df[df.train_maker > 0]   # train-profitable selection
    print(f"\nrandom (all) TEST: maker {df.test_maker.mean():.1f} | taker {df.test_taker.mean():.1f} bps/trade")
    print(f"train-TOP decile ({dec}) TEST: maker {top.test_maker.mean():.1f} | taker {top.test_taker.mean():.1f} "
          f"(stays>0: {(top.test_maker>0).mean()*100:.0f}%)")
    print(f"train-PROFITABLE ({len(prof)}={len(prof)/len(df)*100:.0f}%) TEST: maker {prof.test_maker.mean():.1f} | "
          f"taker {prof.test_taker.mean():.1f} (stays>0: {(prof.test_maker>0).mean()*100:.0f}%)")
    edge_top = top.test_maker.mean(); edge_prof = prof.test_maker.mean()
    verdict = "DEPLOYABLE-TRACK" if (edge_prof > 0 and edge_top > 0) else "selection does NOT carry OOS"
    print(f"\nVERDICT: {verdict} (train-profitable TEST maker {edge_prof:+.1f}, top-decile {edge_top:+.1f} bps)")
    df.to_parquet("app/data/v15/fidelity_oos.parquet")


if __name__ == "__main__":
    main()
