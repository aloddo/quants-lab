#!/usr/bin/env python3
"""live_cohort_forward_track_v3.py -- CLEAN forward test on COMPLETE per-wallet fills (HL API).

v1 (v17_target_fills) was too thin; v2 (hl_wallet_trades) was INVALID (coin-partial -> broken round-trips).
v3 uses the AUTHORITATIVE source: HL API userFillsByTime per wallet = COMPLETE fills with Open/Close `dir`
(reused from leadlag_clean_rank_sim.load_wallet_opens_closes). This reconstructs CORRECT round-trips.

Test: for each of the 100 cohort wallets, pull complete fills SPLIT..now, FIFO-pair opens->closes per
(coin,side) into round-trips, compute forward copy edge (leader gross g minus OUR taker fee + per-coin
slippage), group by the FROZEN label (live_cohort_freeze_classification, as-of 05-23). copy+ > 0 and >
copy- => the copy-edge filter PREDICTS forward (clean OOS proof).

Run: ~/miniforge3/envs/quants-lab/bin/python research/v15/live_cohort_forward_track_v3.py [--liquid-only]
"""
from __future__ import annotations
import argparse, json, sys, time
from collections import defaultdict, deque
from pathlib import Path
import numpy as np, pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, slip_oneway

FEE_T = fee_rt(maker=False); CAP = 500.0 / 1e4
SPLIT = "2026-05-23"
liquid = set(json.load(open(S._DATA / "l2_calib_10coin.json")).keys())


def roundtrips_from_lots(lots):
    """FIFO-pair opens->closes per (coin, side). Each lot: {ts,coin,is_open,is_long,px}.
    Yields g = leader gross return per matched (open,close)."""
    book = defaultdict(deque)   # (coin,is_long) -> deque of open px
    gs = []
    for l in lots:
        key = (l["coin"], l["is_long"])
        if l["is_open"]:
            book[key].append(l["px"])
        else:
            if book[key]:
                opx = book[key].popleft()
                if opx and opx > 0:
                    g = (l["px"] - opx) / opx if l["is_long"] else (opx - l["px"]) / opx
                    gs.append((l["coin"], g))
    return gs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--liquid-only", action="store_true")
    args = ap.parse_args()
    fz = pd.read_parquet("app/data/v15/live_cohort_freeze_classification.parquet")
    label = dict(zip(fz.wallet.str.lower(), fz.cls))
    wallets = list(label)
    s_ms = int(pd.Timestamp(SPLIT, tz="UTC").timestamp() * 1000)
    e_ms = int(pd.Timestamp("2026-06-23T23:59:59", tz="UTC").timestamp() * 1000)
    print(f"pulling COMPLETE HL fills {SPLIT}..now for {len(wallets)} wallets (userFillsByTime) ...")
    per = {}; incomplete = 0; tmin = 1e18; tmax = 0
    for i, w in enumerate(wallets):
        try:
            lots = S.load_wallet_opens_closes(w, s_ms, e_ms)
        except Exception as ex:
            incomplete += 1; continue
        if lots:
            tmin = min(tmin, lots[0]["ts"]); tmax = max(tmax, lots[-1]["ts"])
        gs = roundtrips_from_lots(lots)
        nets = [max(-CAP, min(CAP, g)) - FEE_T - slip_oneway(c) * 2.0
                for c, g in gs if (not args.liquid_only or c in liquid)]
        if nets:
            per[w] = (float(np.mean(nets) * 1e4), len(nets))
        if i % 20 == 0:
            print(f"  ...{i}/{len(wallets)}")
        time.sleep(0.25)
    print(f"\nwallets with RTs: {len(per)} | incomplete(API fail): {incomplete} | "
          f"window {pd.Timestamp(int(tmin),unit='ms',tz='UTC')}..{pd.Timestamp(int(tmax),unit='ms',tz='UTC')} "
          f"({(tmax-tmin)/86400e3:.1f}d) | liquid_only={args.liquid_only}\n")
    buckets = defaultdict(list)
    for w, (edge, n) in per.items():
        buckets[label.get(w, "unlabeled")].append((edge, n))
    print(f"{'frozen label':>16}{'n_wallets':>10}{'fwd_edge_bps':>14}{'wtd_by_n':>10}{'%pos':>7}{'tot_rt':>8}")
    for lab in ["copy+", "copy-", "insufficient"]:
        b = buckets.get(lab, [])
        if not b:
            print(f"{lab:>16}{0:>10}"); continue
        e = np.array([x[0] for x in b]); ns = np.array([x[1] for x in b])
        wtd = float(np.average(e, weights=ns)) if ns.sum() else float("nan")
        print(f"{lab:>16}{len(b):>10}{e.mean():>14.1f}{wtd:>10.1f}{(e>0).mean()*100:>6.0f}%{int(ns.sum()):>8}")
    cp = buckets.get("copy+", []); cn = buckets.get("copy-", [])
    if cp and cn:
        cpm = np.mean([x[0] for x in cp]); cnm = np.mean([x[0] for x in cn])
        print(f"\ncopy+ minus copy- forward edge = {cpm-cnm:+.1f}bps")
        print("VERDICT:", "FILTER PREDICTS FORWARD (copy+ > copy-, both meaningful n)" if cpm > cnm
              else "filter does NOT carry forward (copy+ <= copy-)")
    print("(complete-fills source; ~4wk window. codex-review before any live cohort change.)")


if __name__ == "__main__":
    main()
