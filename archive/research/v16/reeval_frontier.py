#!/usr/bin/env python
"""Efficient frontier: max forward ROE at portfolio maxDD<=10% from the clean pool (Alberto 9963).
Pull forward nets (V15 complete fills) for top clean candidates; build ROE-vs-maxDD frontier."""
import json, sys, time
from pathlib import Path
from collections import defaultdict, deque
import numpy as np, pandas as pd
sys.path.insert(0, "research/v15")
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, slip_oneway
FEE_T=fee_rt(maker=False); CAP=500.0/1e4
def nets(lots,lo,hi):
    book=defaultdict(deque); out=[]
    for l in lots:
        if not(lo<=l["ts"]<hi): continue
        k=(l["coin"],l["is_long"])
        if l["is_open"]: book[k].append(l["px"])
        elif book[k]:
            opx=book[k].popleft()
            if opx>0:
                g=(l["px"]-opx)/opx if l["is_long"] else (opx-l["px"])/opx
                out.append((l["ts"],max(-CAP,min(CAP,g))-FEE_T-slip_oneway(l["coin"])*2))
    return out
def port(members,wn):
    if not members: return (0,0)
    slice_cap=1.0/len(members); trades=[]; wr=[]
    for w in members:
        n=wn.get(w,[])
        wr.append(sum(x for _,x in n)); trades+=n
    if not trades: return (0,0)
    roe=sum(slice_cap*x for x in wr)*100
    trades.sort(key=lambda x:x[0]); eq=1.0;pk=1.0;mdd=0
    for _,x in trades: eq+=slice_cap*x; pk=max(pk,eq); mdd=max(mdd,(pk-eq)/pk)
    return (roe,mdd*100)
def main():
    p=pd.read_parquet("/tmp/reeval_pool_mtm.parquet").dropna(subset=["mtm_dd"])
    clean=p[~((p.mtm_dd>=70)|((p.mtm_dd>=60)&(p.mtm_dd<70)&(p.mtm_ret<=-40)))].sort_values("skill",ascending=False)
    cand=list(clean.head(350).index)
    SP=int(pd.Timestamp("2026-05-23",tz="UTC").timestamp()*1000); TE=int(pd.Timestamp("2026-06-23T23:59:59",tz="UTC").timestamp()*1000)
    TR0=int(pd.Timestamp("2025-12-01",tz="UTC").timestamp()*1000)
    wn={}; tredge={}
    for i,w in enumerate(cand):
        try: lots=S.load_wallet_opens_closes(w,TR0,TE)
        except Exception: continue
        wn[w]=nets(lots,SP,TE); tn=nets(lots,TR0,SP)
        if tn: tredge[w]=np.mean([x for _,x in tn])*1e4
        if i%50==0: print(f"  ...{i}/{len(cand)}")
        time.sleep(0.16)
    pos=[w for w in cand if tredge.get(w,-1)>0]  # train-copy-positive
    print(f"\ncandidates {len(wn)} | train-copy-positive {len(pos)}")
    # FRONTIER: greedy add by train edge (proxy), record ROE+maxDD at each size; find max ROE at maxDD<=10
    ranked=sorted(pos,key=lambda w:-tredge.get(w,0))
    print(f"\n{'n':>4}{'ROE%':>9}{'maxDD%':>8}")
    best=None
    for n in [10,15,20,25,30,40,50,75,100,len(ranked)]:
        if n>len(ranked): continue
        roe,mdd=port(ranked[:n],wn)
        print(f"{n:>4}{roe:>9.1f}{mdd:>8.1f}")
        if mdd<=10 and (best is None or roe>best[1]): best=(n,roe,mdd)
    # also: ALL copy-positive (max diversification)
    roe,mdd=port(pos,wn); print(f"{'ALL':>4}{roe:>9.1f}{mdd:>8.1f}  (all {len(pos)} copy-positive)")
    if mdd<=10 and (best is None or roe>best[1]): best=(len(pos),roe,mdd)
    print(f"\nMAX ROE at maxDD<=10%: {best}" if best else "\nNo subset reached <=10% DD in the greedy scan -- needs market-neutral/short balancing")
if __name__=="__main__": main()
