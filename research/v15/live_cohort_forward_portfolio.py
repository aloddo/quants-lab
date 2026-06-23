#!/usr/bin/env python3
"""live_cohort_forward_portfolio.py -- codex's FINAL must-do: forward PORTFOLIO replay (deployable metric).

The leader-level forward test (v3) showed copy+ beats copy- by +15-20bps/RT OOS. Codex: before a live
change, run a PORTFOLIO-aware forward replay (current cohort vs copy--removed) with actual sizing /
survivorship / concurrency proxy, on the same 05-23..06-23 complete-fills window. Decision rule:
ROE up, DD not worse, no single wallet/coin dominates, gap survives wallet-bootstrap.

Complete fills via HL API userFillsByTime (authoritative). Portfolio = survivorship-safe equal-slice per
wallet (reuse copy_consistent_winners_oos.portfolio_metrics): dead wallets retained as idle 0. g-based net
(leader gross minus our taker fee + per-coin slippage; no forward marks needed).

Cohorts: CURRENT (all classified live wallets) vs FILTER12 (drop 12 well-sampled copy- n>=20) vs FILTER27
(drop all 27 copy-). Run: ~/miniforge3/envs/quants-lab/bin/python research/v15/live_cohort_forward_portfolio.py
"""
from __future__ import annotations
import json, sys, time
from collections import defaultdict, deque
from pathlib import Path
import numpy as np, pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, slip_oneway
from copy_consistent_winners_oos import portfolio_metrics

FEE_T = fee_rt(maker=False); CAP = 500.0 / 1e4
SPLIT = "2026-05-23"


def rts_nets(lots):
    """FIFO-pair opens->closes per (coin,side); return [(close_ts, net)] g-based net."""
    book = defaultdict(deque); out = []
    for l in lots:
        key = (l["coin"], l["is_long"])
        if l["is_open"]:
            book[key].append(l["px"])
        elif book[key]:
            opx = book[key].popleft()
            if opx and opx > 0:
                g = (l["px"] - opx) / opx if l["is_long"] else (opx - l["px"]) / opx
                out.append((l["ts"], max(-CAP, min(CAP, g)) - FEE_T - slip_oneway(l["coin"]) * 2.0))
    return out


def main():
    fz = pd.read_parquet("app/data/v15/live_cohort_freeze_classification.parquet")
    fz["w"] = fz.wallet.str.lower()
    label = dict(zip(fz.w, fz.cls))
    well_copyneg = set(fz[(fz.cls == "copy-") & (fz.n_rt_liquid >= 20)].w)
    all_copyneg = set(fz[fz.cls == "copy-"].w)
    wallets = list(label)
    s_ms = int(pd.Timestamp(SPLIT, tz="UTC").timestamp() * 1000)
    e_ms = int(pd.Timestamp("2026-06-23T23:59:59", tz="UTC").timestamp() * 1000)
    print(f"pulling COMPLETE fills {SPLIT}..now for {len(wallets)} wallets ...")
    wnets = {}
    for i, w in enumerate(wallets):
        try:
            lots = S.load_wallet_opens_closes(w, s_ms, e_ms)
        except Exception:
            continue
        n = rts_nets(lots)
        if n:
            wnets[w] = n
        if i % 25 == 0:
            print(f"  ...{i}/{len(wallets)}")
        time.sleep(0.22)
    print(f"wallets with forward RTs: {len(wnets)}\n")

    def cohort(drop):
        members = [w for w in wallets if w not in drop]
        lots = [wnets.get(w, []) for w in members]      # retain members w/ 0 fwd RTs as idle
        return members, lots

    def grade(name, drop):
        members, lots = cohort(drop)
        m = portfolio_metrics(lots, 0, None, FEE_T, len(members))
        print(f"{name:>22} n={len(members):>3} ROE={m['roe']:>7.2f}% maxDD={m['maxdd']:>5.1f}% "
              f"turn={m.get('turnover',0):>5.1f} top5={m.get('top5_share',float('nan')):>4.0f}% "
              f"LOO={m.get('loo_roe',float('nan')):>7.2f}% w>0={m['wpos']:>3.0f}%")
        return m

    print(f"{'cohort':>22} (survivorship-safe equal-slice portfolio, g-based net fee+slip, 05-23..now)")
    cur = grade("CURRENT (all)", set())
    f12 = grade("FILTER drop-12-wellCN", well_copyneg)
    f27 = grade("FILTER drop-27-allCN", all_copyneg)
    print(f"\nDELTA vs CURRENT: drop-12 {f12['roe']-cur['roe']:+.2f}pp | drop-27 {f27['roe']-cur['roe']:+.2f}pp")

    # wallet-bootstrap: resample CURRENT members, compare ROE(all) vs ROE(minus well-copyneg) per draw
    rng = np.random.default_rng(7)
    members_all = [w for w in wallets if w in wnets or w in label]
    deltas = []
    base_members = [w for w in wallets]
    for _ in range(300):
        samp = list(rng.choice(base_members, size=len(base_members), replace=True))
        lots_all = [wnets.get(w, []) for w in samp]
        lots_filt = [wnets.get(w, []) for w in samp if w not in well_copyneg]
        if not lots_filt:
            continue
        ra = portfolio_metrics(lots_all, 0, None, FEE_T, len(samp))["roe"]
        rf = portfolio_metrics(lots_filt, 0, None, FEE_T, sum(1 for w in samp if w not in well_copyneg))["roe"]
        deltas.append(rf - ra)
    d = np.array(deltas)
    print(f"\nBOOTSTRAP (drop-12 minus current, 300 draws): mean {d.mean():+.2f}pp | "
          f"P[>0]={ (d>0).mean()*100:.0f}% | 5th pct {np.percentile(d,5):+.2f}pp")
    print("\nDECISION RULE (codex): ROE up + DD not worse + top5/LOO not single-name + bootstrap P[>0] high.")
    print("If met -> propose copy-edge filter (drop well-sampled copy-) as a live change w/ guarded rollout.")


if __name__ == "__main__":
    main()
