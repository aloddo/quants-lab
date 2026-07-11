import pyarrow.parquet as pq, numpy as np, pandas as pd, sys
sys.path.insert(0,'research/v15')
from execution_model import fee_rt, slip_oneway, set_slip_default_bps
set_slip_default_bps(4.7); FEE=fee_rt(maker=False)
_sc={}
def rt_cost(coin):
    v=_sc.get(coin)
    if v is None: v=(FEE+2.0*slip_oneway(coin))*1e4; _sc[coin]=v
    return v
MINHOLD=2*60_000
uni=set(l.strip().lower() for l in open("app/data/v15/m01_nonerroring_wallets.txt") if l.strip() and not l.startswith("#"))
cols=["wallet","coin","ts","signed_size","mark","action_type","journey_id","is_liquidation"]
pf=pq.ParquetFile("app/data/v15/m02_actions.parquet")
JA={}; WID={}
for b in pf.iter_batches(batch_size=1_000_000, columns=cols):
    d=b.to_pydict();Wc=d["wallet"];Cc=d["coin"];Tc=d["ts"];Sc=d["signed_size"];Mc=d["mark"];Ac=d["action_type"];Jc=d["journey_id"];Lc=d["is_liquidation"]
    for i in range(len(Wc)):
        w=Wc[i]
        if w not in uni or Lc[i]: continue
        jid=Jc[i]; mk=Mc[i]; ss=Sc[i]
        if jid is None or mk is None or ss is None or mk<=1e-9 or ss==0: continue
        wid=WID.setdefault(w,len(WID)); key=wid*10_000_000+jid; ent=(Ac[i]=="ENTRY"); a=abs(ss); ts=int(Tc[i])
        r=JA.get(key)
        if r is None: JA[key]=[w,Cc[i],(1 if ss>0 else -1) if ent else 0,(a if ent else 0.0),(a*mk if ent else 0.0),(0.0 if ent else a),(0.0 if ent else a*mk),mk,mk,ts,ts]
        else:
            if ent: r[3]+=a; r[4]+=a*mk; r[2]=r[2] or (1 if ss>0 else -1)
            else: r[5]+=a; r[6]+=a*mk
            if mk<r[7]:r[7]=mk
            if mk>r[8]:r[8]=mk
            if ts>r[10]:r[10]=ts
            if ts<r[9]:r[9]=ts
print(f"journeys={len(JA)}")
from collections import defaultdict
WC=defaultdict(lambda:{'L':[],'S':[],'Lmae':[],'Smae':[]})  # (wallet,coin)
for k,r in JA.items():
    w,coin,sign,ew,sume,xw,sumx,mn,mx,t0,t1=r
    if ew<=0 or xw<=0 or sign==0 or t1-t0<MINHOLD or mn<=1e-9 or mx/mn>20: continue
    vwe=sume/ew; vwx=sumx/xw
    jp=((vwx-vwe)/vwe if sign>0 else (vwe-vwx)/vwe)*1e4 - rt_cost(coin)
    mae=max(((mn-vwe)/vwe if sign>0 else (vwe-mx)/vwe)*1e4,-10000)
    s='L' if sign>0 else 'S'; WC[(w,coin)][s].append(jp); WC[(w,coin)][s+'mae'].append(mae)
del JA
rows=[]
for (w,coin),d in WC.items():
    nl,ns=len(d['L']),len(d['S'])
    if nl<30 or ns<30: continue    # BOTH sides actively traded
    lm,sm=float(np.median(d['L'])),float(np.median(d['S']))
    lmae=float(np.median(d['Lmae'])); smae=float(np.median(d['Smae']))
    if lm>0 and sm>0 and lmae>-800 and smae>-800:   # profitable + clean BOTH sides = skill, not beta
        rows.append((w,coin,nl,ns,round(lm,0),round(sm,0),round((nl+ns)/192,1)))
df=pd.DataFrame(rows,columns=["w","coin","nL","nS","medL","medS","perday"]).sort_values("perday",ascending=False)
print(f"\n=== BETA-NEUTRAL skilled wallet-coin pairs (both sides profitable+clean, n>=30 each): {len(df)} ===")
print(f"distinct wallets: {df.w.nunique()} | distinct coins: {df.coin.nunique()}")
print(f"coin spread: {df.coin.value_counts().head(10).to_dict()}")
for _,r in df.head(30).iterrows():
    print(f"  {r.w[:12]} {r.coin:>8} L{int(r.nL)}({r.medL:+.0f}) S{int(r.nS)}({r.medS:+.0f}) {r.perday:.1f}/day")
df.to_csv("/tmp/beta_neutral_skilled.csv",index=False)
print(f"saved -> /tmp/beta_neutral_skilled.csv")
