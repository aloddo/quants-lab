#!/usr/bin/env python3
"""Forward-validate dropping the 27 martingale leaders (Alberto GO 9944). Complete HL-API fills,
05-23..now OOS, survivorship-safe equal-slice portfolio. CURRENT(88) vs DROP-MARTINGALE(61). Reuses
live_cohort_forward_portfolio helpers (rts_nets) + portfolio_metrics."""
import json, sys, time
from collections import defaultdict, deque
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, slip_oneway
from copy_consistent_winners_oos import portfolio_metrics
from live_cohort_forward_portfolio import rts_nets
SPLIT="2026-05-23"; 
def main():
    cfg=json.load(open("config/copy_trader_wallets_v17_expansion.json")); wallets=[w.lower() for w in cfg["wallets"]]
    mart={l.strip().lower() for l in open("/tmp/martingale_drop.txt") if l.strip()}
    s_ms=int(pd.Timestamp(SPLIT,tz="UTC").timestamp()*1000); e_ms=int(pd.Timestamp("2026-06-23T23:59:59",tz="UTC").timestamp()*1000)
    print(f"pulling complete fills {SPLIT}..now for {len(wallets)} wallets; martingales to drop: {len(mart & set(wallets))}")
    wnets={}
    for i,w in enumerate(wallets):
        try: lots=S.load_wallet_opens_closes(w,s_ms,e_ms)
        except Exception: continue
        n=rts_nets(lots)
        if n: wnets[w]=n
        if i%25==0: print(f"  ...{i}/{len(wallets)}")
        time.sleep(0.22)
    def grade(name,drop):
        members=[w for w in wallets if w not in drop]; lots=[wnets.get(w,[]) for w in members]
        m=portfolio_metrics(lots,0,None,fee_rt(maker=False),len(members))
        print(f"{name:>22} n={len(members):>3} ROE={m['roe']:>7.2f}% maxDD={m['maxdd']:>5.1f}% top5={m.get('top5_share',float('nan')):>4.0f}% LOO={m.get('loo_roe',float('nan')):>7.2f}% w>0={m['wpos']:>3.0f}%")
        return m
    print(f"\n{'cohort':>22} (complete-fills forward, survivorship-safe portfolio, 05-23..now)")
    cur=grade("CURRENT (all 88)",set()); dm=grade("DROP-MARTINGALE",mart)
    print(f"\nDELTA drop-martingale vs current: {dm['roe']-cur['roe']:+.2f}pp ROE | maxDD {dm['maxdd']-cur['maxdd']:+.1f}pp")
    rng=np.random.default_rng(7); deltas=[]
    for _ in range(300):
        samp=list(rng.choice(wallets,size=len(wallets),replace=True))
        ra=portfolio_metrics([wnets.get(w,[]) for w in samp],0,None,fee_rt(maker=False),len(samp))["roe"]
        keep=[w for w in samp if w not in mart]
        rf=portfolio_metrics([wnets.get(w,[]) for w in keep],0,None,fee_rt(maker=False),len(keep))["roe"]
        deltas.append(rf-ra)
    d=np.array(deltas); print(f"BOOTSTRAP (drop-mart - current, 300): mean {d.mean():+.2f}pp | P[>0]={(d>0).mean()*100:.0f}% | 5th {np.percentile(d,5):+.2f}pp")
    print("\nDECISION: drop-martingale ROE up + maxDD down + bootstrap P[>0] high -> apply the veto. codex next.")
if __name__=="__main__": main()
