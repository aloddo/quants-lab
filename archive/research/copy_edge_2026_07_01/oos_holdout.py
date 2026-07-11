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
def ms(d): return int(pd.Timestamp(d,tz="UTC").timestamp()*1000)
SPLIT=ms("2026-04-01"); START=ms("2025-12-01"); END=ms("2026-06-11")
uni=set(l.strip().lower() for l in open("app/data/v15/m01_nonerroring_wallets.txt") if l.strip() and not l.startswith("#"))
cols=["wallet","coin","ts","signed_size","action_type","journey_id","is_liquidation"]
pf=pq.ParquetFile("app/data/v15/m02_actions.parquet")
JA={}; WID={}
for b in pf.iter_batches(batch_size=1_000_000, columns=cols):
    d=b.to_pydict()
    for i in range(len(d["wallet"])):
        w=d["wallet"][i]
        if w not in uni or d["is_liquidation"][i]: continue
        jid=d["journey_id"][i]; ss=d["signed_size"][i]
        if jid is None or ss is None or ss==0: continue
        wid=WID.setdefault(w,len(WID)); key=wid*10_000_000+jid; ent=(d["action_type"][i]=="ENTRY"); ts=int(d["ts"][i])
        r=JA.get(key)
        if r is None: JA[key]=[w,d["coin"][i],(1 if ss>0 else -1) if ent else 0, ts if ent else None, None if ent else ts]
        else:
            if ent:
                if r[3] is None or ts<r[3]: r[3]=ts
                if r[2]==0: r[2]=(1 if ss>0 else -1)
            else:
                if r[4] is None or ts<r[4]: r[4]=ts
print(f"journeys={len(JA)}")
from collections import defaultdict
# per (w,coin): train/test x long/short PnL lists
D=defaultdict(lambda:{'trL':[],'trS':[],'teL':[],'teS':[]})
for k,r in JA.items():
    w,coin,sign,et,xt=r
    if sign==0 or et is None or xt is None or xt-et<120000: continue
    ep=S.mark_at(coin,et+LAT); xp=S.mark_at(coin,xt+LAT)
    if not ep or not xp or ep<=0 or xp<=0: continue
    jp=((xp-ep)/ep if sign>0 else (ep-xp)/ep)*1e4 - rt_cost(coin)
    per='tr' if et<SPLIT else 'te'; side='L' if sign>0 else 'S'
    D[(w,coin)][per+side].append(jp)
del JA
# SELECT on TRAIN (two-sided positive, n>=20 each), EVALUATE on TEST
sel=0; test_two_sided_pos=0; rows=[]
for (w,coin),d in D.items():
    if len(d['trL'])<20 or len(d['trS'])<20: continue
    if np.median(d['trL'])<=0 or np.median(d['trS'])<=0: continue
    sel+=1
    if len(d['teL'])<10 or len(d['teS'])<10: continue
    tel,tes=float(np.median(d['teL'])),float(np.median(d['teS']))
    rows.append((w,coin,round(np.median(d['trL'])),round(np.median(d['trS'])),round(tel),round(tes),len(d['teL']),len(d['teS'])))
    if tel>0 and tes>0: test_two_sided_pos+=1
df=pd.DataFrame(rows,columns=["w","coin","trL","trS","teL","teS","nteL","nteS"])
print(f"\nselected on TRAIN (Dec-Apr, two-sided+): {sel}")
print(f"of those w/ enough TEST data ({len(df)}): still two-sided-positive OOS (Apr-Jun): {test_two_sided_pos} ({100*test_two_sided_pos/max(len(df),1):.0f}%)")
print(f"TEST long median across selected: {df.teL.median():+.1f}bps | TEST short median: {df.teS.median():+.1f}bps")
print(f"vs random 2-sided-persist expectation ~25%")
print("\nsurvivors (train L/S -> TEST L/S):")
surv=df[(df.teL>0)&(df.teS>0)].sort_values("teL",ascending=False)
for _,r in surv.head(20).iterrows():
    print(f"  {r.w[:12]} {r.coin:>8} tr[{int(r.trL):+},{int(r.trS):+}] -> TEST[{int(r.teL):+},{int(r.teS):+}] n={int(r.nteL)}/{int(r.nteS)}")
surv.to_csv("/tmp/oos_survivors.csv",index=False)
print(f"saved {len(surv)} OOS survivors -> /tmp/oos_survivors.csv")
