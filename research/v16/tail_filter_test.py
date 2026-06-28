"""
TAIL-ELIGIBILITY FILTER test (codex path on Alberto's let-winners-run insight). Do GENERAL pre-move features
(known at entry) predict which copied trades become tail winners, so we give ONLY those the let-it-run
treatment and cut the bleed on the rest? FROZEN features (anti-overfit, NOT coin-specific):
  f1 CLUSTERING: # distinct good wallets entering this coin+side in prior 60min (consensus/activity expansion)
  f2 REL_STRENGTH: coin prior-24h return - BTC prior-24h return (already outperforming the market)
  f3 MOMENTUM: coin prior-4h return
  f4 REGIME: BTC prior-24h return (up vs down market)
Decision rule (pre-registered): a filter PASSES if, OOS, the filtered (tail-eligible) subset has higher mean
net AND improved EX-TOP-1% mean (less bleed) AND retains >= half the top-tail winners, vs unfiltered.
Exit = let-winners-run (cut -15%, trail 30%, 7d). Canonical cost.
"""
import numpy as np, pandas as pd
import sys; sys.path.insert(0,'research/v16'); sys.path.insert(0,'research/v15')
import decoupled_exit_portfolio as D, execution_model as EX
MS_MIN=60_000; MS_HR=3600_000; LAT=1860; MAXH=7*24*60
HARD=-0.15; ARM=0.10; TRAIL=0.30
btc=D.marks('BTC')
def coin_ret(coin,a,b):
    m=D.marks(coin)
    if m is None: return np.nan
    mins,px=m; i0=np.searchsorted(mins,a,'right')-1; i1=np.searchsorted(mins,b,'right')-1
    if i0<0 or i1<=i0 or i1>=len(px) or px[i0]<=0: return np.nan
    return (px[i1]-px[i0])/px[i0]
def asym(coin,ets,sign):
    m=D.marks(coin)
    if m is None: return None
    mins,px=m; e=int(ets)+LAT; i0=np.searchsorted(mins,e,'right')-1
    if i0<0 or px[i0]<=0 or mins[i0]>=e+MAXH*MS_MIN: return None
    i1=min(np.searchsorted(mins,e+MAXH*MS_MIN,'right'),len(px)); ep=px[i0]; r=(px[i0:i1]-ep)/ep*sign
    peak=-1e9
    for k in range(1,len(r)):
        peak=max(peak,r[k])
        if r[k]<=HARD: return r[k]
        if peak>=ARM and r[k]<=peak-TRAIL: return r[k]
    return r[-1]
j=pd.read_parquet('app/data/v15/m02_journeys.parquet',columns=['wallet','coin','side','entry_ts','max_position_notional','net_realized_pnl']).sort_values('entry_ts')
LIQ=set(['BTC','ETH','SOL','HYPE','XRP','DOGE','AVAX','LINK','LTC','ADA','DOT','SUI','TAO','ARB','APT','SEI','NEAR','AAVE','INJ','UNI','PEPE','WLD','FARTCOIN','TON','TRX','FIL','ATOM','OP','ENA','VVV'])
j=j[(j.coin.isin(LIQ))&(j.max_position_notional>200)].copy()
j['cum']=j.groupby('wallet').net_realized_pnl.transform(lambda s:s.shift(1).expanding(min_periods=5).sum())
g=j[j.cum>0].copy()
# f1 clustering: distinct good wallets per coin+side prior 60min
clu=[]
for (c,s),grp in g.groupby(['coin','side']):
    grp=grp.sort_values('entry_ts'); ts=grp.entry_ts.values; wl=grp.wallet.values; n=len(grp); dc=np.zeros(n,int)
    for i in range(n):
        lo=np.searchsorted(ts,ts[i]-60*MS_MIN,'left'); dc[i]=len(set(wl[lo:i+1]))-1
    clu.append(pd.DataFrame({'idx':grp.index,'f1':dc}))
g=g.join(pd.concat(clu).set_index('idx'))
sub=g.sample(min(12000,len(g)),random_state=4)
rows=[]
for r in sub.itertuples(index=False):
    sgn=1.0 if r.side=='long' else -1.0; ex=asym(r.coin,r.entry_ts,sgn)
    if ex is None: continue
    e=int(r.entry_ts)
    c24=coin_ret(r.coin,e-24*MS_HR,e); b24=coin_ret('BTC',e-24*MS_HR,e); c4=coin_ret(r.coin,e-4*MS_HR,e)
    net=ex-EX.fee_rt(False)-2*EX.slip_oneway(r.coin)
    rows.append(dict(ts=r.entry_ts,coin=r.coin,net=net,f1=r.f1,
        f2=(c24-b24) if (np.isfinite(c24) and np.isfinite(b24)) else np.nan,
        f3=c4 if np.isfinite(c4) else np.nan, f4=b24 if np.isfinite(b24) else np.nan))
df=pd.DataFrame(rows).dropna()
from scipy.stats import spearmanr
cut=df.ts.quantile(0.7); tr,te=df[df.ts<=cut],df[df.ts>cut]
def extop(a):
    a=np.sort(a); k=max(1,len(a)//100); return a[:-k].mean()
print(f'TAIL-FILTER test, n={len(df)} | baseline mean {df.net.mean()*1e4:+.1f}bps ex-top1% {extop(df.net.values)*1e4:+.1f}bps tail(>+50%) {(df.net>0.5).sum()}')
for f in ['f1','f2','f3','f4']:
    print(f'  {f}: spearman(feat,net) ALL {spearmanr(df[f],df.net).correlation:+.3f} OOS {spearmanr(te[f],te.net).correlation:+.3f}')
    # top-tercile by feature (tail-eligible) vs rest, OOS
    thr=df[f].quantile(0.66); hi=te[te[f]>=thr]; lo=te[te[f]<thr]
    if len(hi)>50:
        print(f'      OOS hi-{f} (n{len(hi)}): mean {hi.net.mean()*1e4:+.1f}bps ex-top1% {extop(hi.net.values)*1e4:+.1f}bps tail {(hi.net>0.5).sum()} | lo: mean {lo.net.mean()*1e4:+.1f}bps ex-top1% {extop(lo.net.values)*1e4:+.1f}')
