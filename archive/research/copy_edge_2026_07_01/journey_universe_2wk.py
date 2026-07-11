import pyarrow.parquet as pq, numpy as np, pandas as pd, sys, gc
sys.path.insert(0,'research/v15')
from execution_model import fee_rt, slip_oneway, set_slip_default_bps
set_slip_default_bps(4.7); FEE=fee_rt(maker=False)
_slipcache={}
def rt_cost(coin):
    v=_slipcache.get(coin)
    if v is None: v=(FEE+2.0*slip_oneway(coin))*1e4; _slipcache[coin]=v
    return v
def ms(d): return int(pd.Timestamp(d,tz="UTC").timestamp()*1000)
BOUNDS=[ms("2025-12-01")+i*14*86400_000 for i in range(14)]  # 13 two-week windows
MINHOLD=2*60_000
uni=set(l.strip().lower() for l in open("app/data/v15/m01_nonerroring_wallets.txt") if l.strip() and not l.startswith("#"))
cols=["wallet","coin","ts","signed_size","mark","action_type","journey_id","is_liquidation"]
pf=pq.ParquetFile("app/data/v15/m02_actions.parquet")
# incremental per-journey accumulators: jid -> [w, coin, n_ent, ew, sum_e, xw, sum_x, min_mk, max_mk, t0, t1, sign]
JA={}
WID={}  # wallet -> int id
nrows=0
for b in pf.iter_batches(batch_size=1_000_000, columns=cols):
    d=b.to_pydict(); W=d["wallet"];CO=d["coin"];TS=d["ts"];SS=d["signed_size"];MK=d["mark"];AT=d["action_type"];JID=d["journey_id"];LQ=d["is_liquidation"]
    for i in range(len(W)):
        w=W[i]
        if w not in uni or LQ[i]: continue
        jid=JID[i]; mk=MK[i]; ss=SS[i]
        if jid is None or mk is None or ss is None or mk<=1e-9 or ss==0: continue
        ts=int(TS[i]); ent=(AT[i]=="ENTRY"); a=abs(ss)
        wid=WID.get(w)
        if wid is None: wid=len(WID); WID[w]=wid
        key=wid*10_000_000+jid
        r=JA.get(key)
        if r is None:
            JA[key]=[w,CO[i],(1 if ent else 0),(a if ent else 0.0),(a*mk if ent else 0.0),(0.0 if ent else a),(0.0 if ent else a*mk),mk,mk,ts,ts,(1 if ss>0 else -1) if ent else 0]
        else:
            if ent: r[2]+=1; r[3]+=a; r[4]+=a*mk
            else: r[5]+=a; r[6]+=a*mk
            if mk<r[7]: r[7]=mk
            if mk>r[8]: r[8]=mk
            if ts<r[9]: r[9]=ts
            if ts>r[10]: r[10]=ts
            if r[11]==0 and ent: r[11]=(1 if ss>0 else -1)
    nrows+=len(W)
print(f"streamed rows~{nrows}, journeys={len(JA)}")
def win(t):
    for i in range(len(BOUNDS)-1):
        if BOUNDS[i]<=t<BOUNDS[i+1]: return i
    return None
# per (wallet,window): journey pnl list + mae list + entcount
from collections import defaultdict
WW=defaultdict(lambda: [[],[],[]])  # (w,win)->[pnls,maes,ents]
for jid,r in JA.items():
    w,coin,nent,ew,sume,xw,sumx,mn,mx,t0,t1,sign=r
    if ew<=0 or xw<=0 or sign==0: continue
    if t1-t0<MINHOLD: continue
    if mn<=1e-9 or mx/mn>20: continue
    wi=win(t0)
    if wi is None: continue
    vwe=sume/ew; vwx=sumx/xw
    jp=((vwx-vwe)/vwe if sign>0 else (vwe-vwx)/vwe)*1e4 - rt_cost(coin)
    mae=max(((mn-vwe)/vwe if sign>0 else (vwe-mx)/vwe)*1e4, -10000)
    k=(w,wi); WW[k][0].append(jp); WW[k][1].append(mae); WW[k][2].append(nent)
del JA; gc.collect()
# per wallet: window medians
rows=[]
byw=defaultdict(dict)
for (w,wi),(pnls,maes,ents) in WW.items():
    if len(pnls)<8: continue
    byw[w][wi]=(float(np.median(pnls)),float(np.median(maes)),float(np.mean(ents)),len(pnls))
for w,wins in byw.items():
    if len(wins)<10: continue
    posm=sum(1 for wi,(jp,mae,ne,n) in wins.items() if jp>0)
    cleanm=sum(1 for wi,(jp,mae,ne,n) in wins.items() if mae>-800)
    avgjp=np.mean([jp for jp,_,_,_ in wins.values()]); minn=min(n for _,_,_,n in wins.values())
    rows.append((w,len(wins),posm,cleanm,round(avgjp,1),minn))
df=pd.DataFrame(rows,columns=["w","wins","posmed","cleanwins","avgjp","minn"])
sel=df[(df.posmed>=0.7*df.wins)&(df.cleanwins>=0.7*df.wins)&(df.minn>=8)&(df.avgjp>0)].sort_values("avgjp",ascending=False)
print(f"\nUNIVERSE keepers 2wk-windows/2min-floor (pos-med & clean in >=70% of >=10 windows, n>=8): {len(sel)}")
for _,r in sel.head(30).iterrows():
    print(f"  {r.w} avgJP={r.avgjp:+.0f}bps posmed={int(r.posmed)}/{int(r.wins)} clean={int(r.cleanwins)} minN={int(r.minn)}")
sel.to_csv("/tmp/universe_clean_keepers_2wk.csv",index=False)
print(f"\nsaved {len(sel)} -> /tmp/universe_clean_keepers_2wk.csv")
