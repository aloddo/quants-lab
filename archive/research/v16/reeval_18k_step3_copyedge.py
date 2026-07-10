#!/usr/bin/env python
"""Re-eval STEP 3 (V15-native): copy-edge survival + forward validation on the martingale+MTM-clean pool.
Take top MTM-clean by skill, pull COMPLETE fills (V15 leadlag_clean_rank_sim), compute TRAIN copy edge
(execution_model fee+slip), rank, take top-100, validate TEST forward (portfolio) vs the live 69."""
import json, sys, time
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "research" / "v15"))
sys.path.insert(0, "research/v15")
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, slip_oneway
from copy_consistent_winners_oos import portfolio_metrics
from live_cohort_forward_portfolio import rts_nets
FEE_T=fee_rt(maker=False); CAP=500.0/1e4
def edge(lots, lo, hi):
    from collections import defaultdict,deque
    book=defaultdict(deque); nets=[]
    for l in lots:
        if not (lo<=l["ts"]<hi): continue
        k=(l["coin"],l["is_long"])
        if l["is_open"]: book[k].append(l["px"])
        elif book[k]:
            opx=book[k].popleft()
            if opx>0:
                g=(l["px"]-opx)/opx if l["is_long"] else (opx-l["px"])/opx
                nets.append((l["ts"], max(-CAP,min(CAP,g))-FEE_T-slip_oneway(l["coin"])*2))
    return nets
def main():
    p=pd.read_parquet("/tmp/reeval_pool_mtm.parquet")
    p=p.dropna(subset=["mtm_dd"])
    clean=p[~((p.mtm_dd>=70)|((p.mtm_dd>=60)&(p.mtm_dd<70)&(p.mtm_ret<=-40)))]
    cand=clean.sort_values("skill",ascending=False).head(250)
    print(f"MTM-clean pool {len(clean)}; top-250 by skill -> copy-edge survival (V15 complete fills)")
    TR0=int(pd.Timestamp("2025-12-01",tz="UTC").timestamp()*1000); SP=int(pd.Timestamp("2026-05-23",tz="UTC").timestamp()*1000)
    TE=int(pd.Timestamp("2026-06-23T23:59:59",tz="UTC").timestamp()*1000)
    tr_edge={}; test_nets={}
    for i,w in enumerate(cand.index):
        try: lots=S.load_wallet_opens_closes(w,TR0,TE)
        except Exception: continue
        tn=edge(lots,TR0,SP); te=edge(lots,SP,TE)
        if tn: tr_edge[w]=np.mean([n for _,n in tn])*1e4
        test_nets[w]=te
        if i%40==0: print(f"  ...{i}/250")
        time.sleep(0.18)
    # rank by TRAIN copy edge, take copy-positive top-100
    ranked=sorted(((w,e) for w,e in tr_edge.items() if e>0), key=lambda x:-x[1])
    pool=[w for w,_ in ranked[:100]]
    print(f"\ntrain-copy-positive: {len(ranked)} | selected top-100 by train copy edge")
    # validate TEST forward portfolio vs the live 69
    cur=[w.lower() for w in json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"]]
    cur_nets={}
    for w in cur:
        try: cur_nets[w]=rts_nets(S.load_wallet_opens_closes(w,SP,TE))
        except Exception: cur_nets[w]=[]
        time.sleep(0.15)
    def grade(name,members,nets):
        m=portfolio_metrics([nets.get(w,[]) for w in members],0,None,FEE_T,len(members))
        print(f"{name:>20} n={len(members):>3} ROE={m['roe']:>7.2f}% maxDD={m['maxdd']:>5.1f}% LOO={m.get('loo_roe',float('nan')):>7.2f}%")
        return m
    print(f"\n=== TEST forward (05-23..now) portfolio: RE-EVAL pool vs LIVE 69 ===")
    g=grade("REEVAL-top100",pool,{w:rts_nets_from_te(test_nets[w]) for w in pool})
    c=grade("LIVE-69",cur,cur_nets)
    print(f"\nREEVAL vs LIVE: ROE {g['roe']-c['roe']:+.2f}pp | maxDD {g['maxdd']-c['maxdd']:+.1f}pp")
    print("VERDICT: re-eval pool better if ROE up + maxDD not worse. Else live 69 stands universe-wide.")
def rts_nets_from_te(te):  # te already [(ts,net)] from edge()
    return te
if __name__=="__main__": main()
