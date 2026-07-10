#!/usr/bin/env python3
"""Forward copy-edge comparison: FRESH veto+MTM cohort (100) vs CURRENT live (69). Complete HL-API fills,
05-23..now, survivorship-safe portfolio. Reuses live_cohort_forward_portfolio helpers."""
import json, sys, time
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from execution_model import fee_rt
from copy_consistent_winners_oos import portfolio_metrics
from live_cohort_forward_portfolio import rts_nets
def main():
    fresh=[w.lower() for w in json.load(open("/tmp/skill_cohort_deploy.json"))["wallets"]]
    cur=[w.lower() for w in json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"]]
    uni=sorted(set(fresh)|set(cur))
    s_ms=int(pd.Timestamp("2026-05-23",tz="UTC").timestamp()*1000); e_ms=int(pd.Timestamp("2026-06-23T23:59:59",tz="UTC").timestamp()*1000)
    print(f"fresh {len(fresh)} | current {len(cur)} | union {len(uni)} | overlap {len(set(fresh)&set(cur))}")
    wnets={}
    for i,w in enumerate(uni):
        try: lots=S.load_wallet_opens_closes(w,s_ms,e_ms)
        except Exception: continue
        n=rts_nets(lots)
        if n: wnets[w]=n
        if i%30==0: print(f"  ...{i}/{len(uni)}")
        time.sleep(0.2)
    def grade(name,members):
        m=portfolio_metrics([wnets.get(w,[]) for w in members],0,None,fee_rt(maker=False),len(members))
        print(f"{name:>16} n={len(members):>3} ROE={m['roe']:>7.2f}% maxDD={m['maxdd']:>5.1f}% top5={m.get('top5_share',float('nan')):>4.0f}% LOO={m.get('loo_roe',float('nan')):>7.2f}% w>0={m['wpos']:>3.0f}%")
        return m
    print(f"\n{'cohort':>16} (complete-fills forward, survivorship-safe portfolio, 05-23..now)")
    c=grade("CURRENT-69",cur); f=grade("FRESH-100",fresh)
    print(f"\nFRESH vs CURRENT: ROE {f['roe']-c['roe']:+.2f}pp | maxDD {f['maxdd']-c['maxdd']:+.1f}pp")
    print("READ: fresh better if ROE up + maxDD not worse. But fresh = 66 NEW wallets (churn) vs already-clean 69. codex next.")
if __name__=="__main__": main()
