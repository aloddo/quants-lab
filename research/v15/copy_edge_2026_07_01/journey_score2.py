import pyarrow.parquet as pq, numpy as np, pandas as pd, sys
sys.path.insert(0,'research/v15')
from execution_model import fee_rt, slip_oneway, set_slip_default_bps
set_slip_default_bps(4.7); FEE=fee_rt(maker=False)
def rt_cost(coin): return (FEE + 2.0*slip_oneway(coin))*1e4
def ms(d): return int(pd.Timestamp(d,tz="UTC").timestamp()*1000)
BOUNDS=[ms("2025-12-01"),ms("2026-01-08"),ms("2026-02-15"),ms("2026-03-25"),ms("2026-05-02"),ms("2026-06-11")]
def win(t):
    for i in range(5):
        if BOUNDS[i]<=t<BOUNDS[i+1]: return i
    return None
MINHOLD=15*60_000
wallets=set(l.split(',')[0] for l in open('/tmp/robust_copyable_real_slip.csv').read().splitlines()[1:] if l.strip())
cols=["wallet","coin","ts","signed_size","mark","action_type","journey_id","is_liquidation"]
pf=pq.ParquetFile("app/data/v15/m02_actions.parquet")
from collections import defaultdict
J=defaultdict(list)
for b in pf.iter_batches(batch_size=1_000_000, columns=cols):
    d=b.to_pydict()
    for i in range(len(d["wallet"])):
        w=d["wallet"][i]
        if w not in wallets or d["is_liquidation"][i]: continue
        jid=d["journey_id"][i]; mk=d["mark"][i]; ss=d["signed_size"][i]
        if jid is None or mk is None or ss is None or mk<=1e-9 or ss==0: continue
        J[(w,jid)].append((int(d["ts"][i]),d["coin"][i],float(ss),float(mk),d["action_type"][i]=="ENTRY"))
rows=[]
for (w,jid),fills in J.items():
    fills.sort()
    dur=fills[-1][0]-fills[0][0]
    if dur<MINHOLD: continue                       # hold-floor
    ent=[(sz,mk) for ts,c,sz,mk,e in fills if e]; ex=[(sz,mk) for ts,c,sz,mk,e in fills if not e]
    if not ent or not ex: continue
    coin=fills[0][1]; wi=win(fills[0][0])
    if wi is None: continue
    marks=[mk for _,_,_,mk,_ in fills]
    if max(marks)/min(marks)>20: continue          # mark sanity (drop corrupt/near-zero mark journeys)
    is_long=ent[0][0]>0
    ew=sum(abs(s) for s,_ in ent); xw=sum(abs(s) for s,_ in ex)
    vwe=sum(abs(s)*m for s,m in ent)/ew; vwx=sum(abs(s)*m for s,m in ex)/xw
    jp=((vwx-vwe)/vwe if is_long else (vwe-vwx)/vwe)*1e4 - rt_cost(coin)
    mae=((min(marks)-vwe)/vwe if is_long else (vwe-max(marks))/vwe)*1e4
    rows.append((w,wi,jp,max(mae,-10000),len(ent)))
df=pd.DataFrame(rows,columns=["w","win","jp","mae","nent"])
print(f"clean journeys (hold>=15min, mark-sane): {len(df)}")
print("\nper-wallet JOURNEY median (size-weighted), add-count, MAE -- CLEAN:")
print(f"{'wallet':14} {'n':>4} {'JP_med':>7} {'MAE_med':>8} {'adds':>5}  verdict")
keep=[]
for w,g in df.groupby("w"):
    if len(g)<15: continue
    jpm=g.jp.median(); maem=g.mae.median(); adds=g.nent.mean()
    clean = jpm>0 and adds<=3.0 and maem>-800
    if clean: keep.append((w,jpm,adds,maem,len(g)))
    print(f"  {w[:12]} {len(g):>4} {jpm:>+7.0f} {maem:>+8.0f} {adds:>5.1f}  {'CLEAN-KEEP' if clean else 'drop'}")
print(f"\n=== CLEAN KEEPERS (journey+ , adds<=3, shallow MAE): {len(keep)} ===")
for w,jpm,adds,maem,n in sorted(keep,key=lambda x:-x[1]):
    print(f"  {w} JPmed={jpm:+.0f}bps adds={adds:.1f} MAE={maem:+.0f} n={n}")
