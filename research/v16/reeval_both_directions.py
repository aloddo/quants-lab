#!/usr/bin/env python
"""Both-direction (market-neutral) frontier (Alberto 9966). Pull complete fills for the clean pool, compute
per-SIDE copy edge (long vs short), identify short-skilled leaders, build a direction-balanced book, find
max ROE at maxDD<=10%. V15 fills + execution_model."""
import json, sys, time, pickle
from pathlib import Path
from collections import defaultdict, deque
import numpy as np, pandas as pd
sys.path.insert(0, "research/v15")
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, slip_oneway
FEE_T=fee_rt(maker=False); CAP=500.0/1e4
def side_nets(lots,lo,hi):
    """Return (long_nets, short_nets) round-trip nets [(ts,net)] split by leader side."""
    book=defaultdict(deque); ln=[]; sn=[]
    for l in lots:
        if not(lo<=l["ts"]<hi): continue
        k=(l["coin"],l["is_long"])
        if l["is_open"]: book[k].append(l["px"])
        elif book[k]:
            opx=book[k].popleft()
            if opx>0:
                g=(l["px"]-opx)/opx if l["is_long"] else (opx-l["px"])/opx
                net=max(-CAP,min(CAP,g))-FEE_T-slip_oneway(l["coin"])*2
                (ln if l["is_long"] else sn).append((l["ts"],net))
    return ln,sn
def port(legs):
    """legs = list of [(ts,net)] per slice. Equal slice. Return (ROE%, maxDD%)."""
    if not legs: return (0,0)
    sc=1.0/len(legs); trades=[]; wr=[]
    for n in legs: wr.append(sum(x for _,x in n)); trades+=n
    if not trades: return (0,0)
    roe=sum(sc*x for x in wr)*100
    trades.sort(key=lambda x:x[0]); eq=1.;pk=1.;mdd=0
    for _,x in trades: eq+=sc*x; pk=max(pk,eq); mdd=max(mdd,(pk-eq)/pk)
    return (roe,mdd*100)
def main():
    p=pd.read_parquet("/tmp/reeval_pool_mtm.parquet").dropna(subset=["mtm_dd"])
    clean=p[~((p.mtm_dd>=70)|((p.mtm_dd>=60)&(p.mtm_dd<70)&(p.mtm_ret<=-40)))].sort_values("skill",ascending=False)
    cand=list(clean.head(500).index)
    TR0=int(pd.Timestamp("2025-12-01",tz="UTC").timestamp()*1000); SP=int(pd.Timestamp("2026-05-23",tz="UTC").timestamp()*1000); TE=int(pd.Timestamp("2026-06-23T23:59:59",tz="UTC").timestamp()*1000)
    W={}  # w -> dict(L_fwd, S_fwd, L_tr_edge, S_tr_edge)
    for i,w in enumerate(cand):
        try: lots=S.load_wallet_opens_closes(w,TR0,TE)
        except Exception: continue
        Lf,Sf=side_nets(lots,SP,TE); Lt,St=side_nets(lots,TR0,SP)
        W[w]={"Lf":Lf,"Sf":Sf,"Lte":(np.mean([x for _,x in Lt])*1e4 if Lt else None),"Ste":(np.mean([x for _,x in St])*1e4 if St else None),
              "nL":len(Lt),"nS":len(St)}
        if i%60==0: print(f"  ...{i}/{len(cand)}")
        time.sleep(0.16)
    pickle.dump(W,open("/tmp/both_dir_nets.pkl","wb"))
    # long-edge leaders (train long edge>0, enough); short-edge leaders (train short edge>0, enough)
    longers=[w for w,d in W.items() if d["Lte"] and d["Lte"]>0 and d["nL"]>=15]
    shorters=[w for w,d in W.items() if d["Ste"] and d["Ste"]>0 and d["nS"]>=15]
    print(f"\nclean candidates {len(W)} | LONG-skilled {len(longers)} | SHORT-skilled {len(shorters)}")
    # build books: take top long-edge leaders' LONG legs + top short-edge leaders' SHORT legs
    longers.sort(key=lambda w:-W[w]["Lte"]); shorters.sort(key=lambda w:-W[w]["Ste"])
    print(f"\n{'nL':>4}{'nS':>4}{'ROE%':>9}{'maxDD%':>8}")
    best=None
    for nL,nS in [(40,0),(30,10),(25,15),(20,20),(15,25),(10,30),(20,40),(30,30),(40,20),(50,30)]:
        legs=[W[w]["Lf"] for w in longers[:nL]]+[W[w]["Sf"] for w in shorters[:nS]]
        roe,mdd=port(legs); print(f"{nL:>4}{nS:>4}{roe:>9.1f}{mdd:>8.1f}")
        if mdd<=10 and (best is None or roe>best[2]): best=(nL,nS,roe,mdd)
    print(f"\nMAX ROE at <=10% DD: {best}" if best else "\nstill none <=10% -- need finer short balance")
    print("(ROE abstract/g-based; SHAPE is the signal -- shorts cut the directional DD)")
if __name__=="__main__": main()
