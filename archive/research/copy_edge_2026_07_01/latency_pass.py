import pyarrow.parquet as pq, numpy as np, pandas as pd, sys
sys.path.insert(0,'research/v15')
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, slip_oneway, set_slip_default_bps, set_latency_ms
set_slip_default_bps(4.7); set_latency_ms(2000); FEE=fee_rt(maker=False); LAT=2000
_sc={}
def rt_cost(coin):
    v=_sc.get(coin)
    if v is None: v=(FEE+2.0*slip_oneway(coin))*1e4; _sc[coin]=v
    return v
W=set(l.split(',')[0].lower() for l in open("/tmp/beta_neutral_skilled.csv").read().splitlines()[1:] if l.strip())
cols=["wallet","coin","ts","signed_size","action_type","journey_id","is_liquidation"]
pf=pq.ParquetFile("app/data/v15/m02_actions.parquet")
JA={}; WID={}
for b in pf.iter_batches(batch_size=1_000_000, columns=cols):
    d=b.to_pydict()
    for i in range(len(d["wallet"])):
        w=d["wallet"][i]
        if w not in W or d["is_liquidation"][i]: continue
        jid=d["journey_id"][i]; ss=d["signed_size"][i]
        if jid is None or ss is None or ss==0: continue
        wid=WID.setdefault(w,len(WID)); key=wid*10_000_000+jid; ent=(d["action_type"][i]=="ENTRY"); ts=int(d["ts"][i])
        r=JA.get(key)
        # store: w, coin, sign, first_entry_ts, first_close_ts(=min close ts), has_close
        if r is None: JA[key]=[w,d["coin"][i],(1 if ss>0 else -1) if ent else 0, ts if ent else None, None if ent else ts]
        else:
            if ent:
                if r[3] is None or ts<r[3]: r[3]=ts
                if r[2]==0: r[2]=(1 if ss>0 else -1)
            else:
                if r[4] is None or ts<r[4]: r[4]=ts
print(f"journeys={len(JA)}")
from collections import defaultdict
WC=defaultdict(lambda:{'L':[],'S':[]})
for k,r in JA.items():
    w,coin,sign,et,xt=r
    if sign==0 or et is None or xt is None or xt-et<120000: continue
    ep=S.mark_at(coin,et+LAT); xp=S.mark_at(coin,xt+LAT)
    if not ep or not xp or ep<=0 or xp<=0: continue
    jp=((xp-ep)/ep if sign>0 else (ep-xp)/ep)*1e4 - rt_cost(coin)
    WC[(w,coin)]['L' if sign>0 else 'S'].append(jp)
rows=[]
for (w,coin),d in WC.items():
    nl,ns=len(d['L']),len(d['S'])
    if nl<30 or ns<30: continue
    lm,sm=float(np.median(d['L'])),float(np.median(d['S']))
    if lm>0 and sm>0:
        rows.append((w,coin,nl,ns,round(lm),round(sm),round((nl+ns)/192,1)))
df=pd.DataFrame(rows,columns=["w","coin","nL","nS","medL","medS","perday"]).sort_values("perday",ascending=False)
print(f"\n=== SURVIVES 2s LATENCY (two-sided positive after latency+cost): {len(df)} wallet-coin pairs ===")
print(f"distinct wallets: {df.w.nunique()} | coins: {df.coin.value_counts().head(8).to_dict()}")
print(f"aggregate trades/day (top 30): {df.head(30).perday.sum():.0f}")
for _,r in df.head(25).iterrows():
    print(f"  {r.w[:12]} {r.coin:>8} L{int(r.nL)}({int(r.medL):+}) S{int(r.nS)}({int(r.medS):+}) {r.perday}/day")
df.to_csv("/tmp/final_copyable_latency.csv",index=False)
print("saved -> /tmp/final_copyable_latency.csv")
