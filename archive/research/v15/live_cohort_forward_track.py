#!/usr/bin/env python3
"""live_cohort_forward_track.py -- CLEAN FORWARD shadow-track of the frozen copy-edge classification.

Codex step-1 directive: freeze the rule (done: live_cohort_freeze_classification.parquet, as-of 05-23),
then test ONLY on data AFTER the freeze. The live engine recorded the cohort leaders' fills in
mongo `v17_target_fills` since 06-11 = genuinely OUT-OF-SAMPLE vs the 05-23 freeze.

Test: for each wallet, compute forward copy edge (leader round-trip gross g minus OUR taker fee +
per-coin slippage) on v17_target_fills, group by the FROZEN label (copy+/copy-/insufficient). If
frozen-copy+ wallets show positive forward edge and frozen-copy- negative, the filter predicts forward.

NOTE: offline marks end 05-23, so forward pricing uses the leader's own fill VWAPs (g) not our copy
marks (standard fidelity approach for fresh data). Frozen labels were mark-based -> slight method
difference, documented. Window is SHORT (~11d) -> directional read, re-run as it grows.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v15/live_cohort_forward_track.py
"""
from __future__ import annotations
import json, sys
from collections import defaultdict
from pathlib import Path
import numpy as np, pandas as pd
from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, slip_oneway

FEE_T = fee_rt(maker=False); CAP = 500.0 / 1e4
liquid = set(json.load(open(S._DATA / "l2_calib_10coin.json")).keys())


def main():
    fz = pd.read_parquet("app/data/v15/live_cohort_freeze_classification.parquet")
    label = dict(zip(fz.wallet.str.lower(), fz.cls))
    db = MongoClient("mongodb://localhost:27017").quants_lab
    rows = list(db.v17_target_fills.find({}, {"wallet": 1, "coin": 1, "side": 1, "price": 1,
                                              "size": 1, "ts_epoch": 1, "_id": 0}))
    print(f"v17_target_fills rows: {len(rows)}")
    wf = defaultdict(list)
    for r in rows:
        w = str(r.get("wallet", "")).lower()
        s = r.get("size"); p = r.get("price"); side = r.get("side"); te = r.get("ts_epoch")
        if not (w and s and p and side and te):
            continue
        signed = float(s) * (1.0 if str(side).upper().startswith("B") else -1.0)
        wf[w].append((int(te * 1000), r.get("coin"), signed, float(p)))
    # forward copy edge per wallet (g-based, liquid-only, net taker fee + slippage)
    per = {}
    tmin = min((f[0] for fl in wf.values() for f in fl), default=0)
    tmax = max((f[0] for fl in wf.values() for f in fl), default=0)
    for w, fl in wf.items():
        rts = roundtrips(sorted(fl, key=lambda x: x[0]))
        nets = []
        for c, dir_, ets, xts, evw, xvw, g in rts:
            if c not in liquid:
                continue
            nets.append(max(-CAP, min(CAP, g)) - FEE_T - slip_oneway(c) * 2.0)
        if nets:
            per[w] = (float(np.mean(nets) * 1e4), len(nets))

    print(f"forward window: {pd.Timestamp(tmin,unit='ms',tz='UTC')} .. {pd.Timestamp(tmax,unit='ms',tz='UTC')} "
          f"({(tmax-tmin)/86400e3:.1f}d) | wallets with liquid fwd RTs: {len(per)}\n")
    # group by frozen label
    buckets = defaultdict(list)
    for w, (edge, n) in per.items():
        buckets[label.get(w, "unlabeled")].append((edge, n))
    print(f"{'frozen label':>16}{'n_wallets':>10}{'fwd_edge_bps(mean)':>20}{'wtd_by_n':>12}{'%pos':>7}{'tot_rt':>8}")
    for lab in ["copy+", "copy-", "insufficient", "unlabeled"]:
        b = buckets.get(lab, [])
        if not b:
            print(f"{lab:>16}{0:>10}"); continue
        e = np.array([x[0] for x in b]); ns = np.array([x[1] for x in b])
        wtd = float(np.average(e, weights=ns)) if ns.sum() else float("nan")
        print(f"{lab:>16}{len(b):>10}{e.mean():>20.1f}{wtd:>12.1f}{(e>0).mean()*100:>6.0f}%{int(ns.sum()):>8}")
    print("\nREAD: if frozen copy+ forward edge > 0 and > copy- -> the frozen filter PREDICTS forward (filter")
    print("works OOS). If copy+ ~= copy- forward -> the in-sample lift did NOT carry. ~11d is short: re-run as")
    print("v17_target_fills grows. Method note: forward = g-based (no fresh marks); frozen labels mark-based.")


if __name__ == "__main__":
    main()
