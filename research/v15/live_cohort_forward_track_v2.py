#!/usr/bin/env python3
"""live_cohort_forward_track_v2.py -- CLEAN FORWARD test of the frozen copy-edge labels, RICHER source.

v1 used v17_target_fills (only what our engine saw -> ~12 liquid RTs in 11d, too thin). v2 uses
mongo hl_wallet_trades (the live coin-targeted collector: every trade on top coins WITH buyer/seller
addresses, ~05-04..now). Post-freeze (05-23) it covers ~86/100 cohort wallets with thousands of fills
-> enough round-trips for a real forward read NOW, instead of waiting weeks.

Test: reconstruct each cohort wallet's signed fills post-split (buyer=+size, seller=-size), build
round-trips, compute g-based copy edge (leader round-trip gross minus OUR taker fee + per-coin
slippage), group by the FROZEN label (copy+/copy-/insufficient from live_cohort_freeze_classification).
If frozen-copy+ shows positive forward edge and > copy-, the filter PREDICTS forward.

CAVEATS: labels are mark-based (m02 freeze) / forward is g-based (hl_wallet_trades) -- minor method
mismatch, documented (both net the same fee, so copy+/- rarely flips). Non-liquid coins use the
execution_model class-default slippage (less precise); --liquid-only restricts to calibrated majors.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v15/live_cohort_forward_track_v2.py [--liquid-only]
"""
from __future__ import annotations
import argparse, json, sys
from collections import defaultdict
from pathlib import Path
import numpy as np, pandas as pd
from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, slip_oneway

FEE_T = fee_rt(maker=False); CAP = 500.0 / 1e4
SPLIT_EPOCH = 1779840000   # ~2026-05-26 (post the 05-23 freeze, small buffer)
liquid = set(json.load(open(S._DATA / "l2_calib_10coin.json")).keys())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--liquid-only", action="store_true")
    args = ap.parse_args()
    fz = pd.read_parquet("app/data/v15/live_cohort_freeze_classification.parquet")
    label = dict(zip(fz.wallet.str.lower(), fz.cls))
    live = set(label)
    db = MongoClient("mongodb://localhost:27017").quants_lab
    print(f"loading hl_wallet_trades post-freeze (epoch>={SPLIT_EPOCH}) for {len(live)} cohort wallets ...")
    wf = defaultdict(list); ntr = 0
    cur = db.hl_wallet_trades.find({"timestamp": {"$gte": SPLIT_EPOCH}},
                                   {"buyer": 1, "seller": 1, "coin": 1, "price": 1, "size": 1,
                                    "timestamp": 1, "_id": 0})
    for r in cur:
        ntr += 1
        b = str(r.get("buyer", "")).lower(); s = str(r.get("seller", "")).lower()
        coin = r.get("coin"); px = r.get("price"); sz = r.get("size"); te = r.get("timestamp")
        if not (coin and px and sz and te):
            continue
        tms = int(float(te) * 1000)
        if b in live:
            wf[b].append((tms, coin, float(sz), float(px)))     # buyer = +size
        if s in live:
            wf[s].append((tms, coin, -float(sz), float(px)))    # seller = -size
    print(f"scanned {ntr} post-freeze trades | {len(wf)} cohort wallets with fills")
    per = {}; tmin = 1e18; tmax = 0
    for w, fl in wf.items():
        fl.sort(key=lambda x: x[0]); tmin = min(tmin, fl[0][0]); tmax = max(tmax, fl[-1][0])
        rts = roundtrips(fl); nets = []
        for c, dir_, ets, xts, evw, xvw, g in rts:
            if args.liquid_only and c not in liquid:
                continue
            nets.append(max(-CAP, min(CAP, g)) - FEE_T - slip_oneway(c) * 2.0)
        if nets:
            per[w] = (float(np.mean(nets) * 1e4), len(nets))
    print(f"forward window {pd.Timestamp(int(tmin),unit='ms',tz='UTC')} .. "
          f"{pd.Timestamp(int(tmax),unit='ms',tz='UTC')} ({(tmax-tmin)/86400e3:.1f}d) | "
          f"wallets with RTs: {len(per)} | liquid_only={args.liquid_only}\n")
    buckets = defaultdict(list)
    for w, (edge, n) in per.items():
        buckets[label.get(w, "unlabeled")].append((edge, n))
    print(f"{'frozen label':>16}{'n_wallets':>10}{'fwd_edge_bps':>14}{'wtd_by_n':>10}{'%pos':>7}{'tot_rt':>8}")
    for lab in ["copy+", "copy-", "insufficient", "unlabeled"]:
        b = buckets.get(lab, [])
        if not b:
            print(f"{lab:>16}{0:>10}"); continue
        e = np.array([x[0] for x in b]); ns = np.array([x[1] for x in b])
        wtd = float(np.average(e, weights=ns)) if ns.sum() else float("nan")
        print(f"{lab:>16}{len(b):>10}{e.mean():>14.1f}{wtd:>10.1f}{(e>0).mean()*100:>6.0f}%{int(ns.sum()):>8}")
    print("\nREAD: frozen copy+ forward edge > 0 AND > copy- -> filter PREDICTS forward (clean OOS). This uses")
    print("the RICH hl_wallet_trades source (vs v1's thin v17_target_fills). Still ~4wk window; re-run as grows.")


if __name__ == "__main__":
    main()
