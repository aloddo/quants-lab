import sys; sys.path.insert(0,'research/v16'); sys.path.insert(0,'research/v15')
import decoupled_exit_portfolio as D, execution_model as EX, numpy as np, pandas as pd
MS_MIN=60_000; LAT=1860; MAXH=7*24*60; HARD=-0.15; ARM=0.10; TRAIL=0.30
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
clu=[]
for (c,s),grp in g.groupby(['coin','side']):
    grp=grp.sort_values('entry_ts'); ts=grp.entry_ts.values; wl=grp.wallet.values; n=len(grp); dc=np.zeros(n,int)
    for i in range(n):
        lo=np.searchsorted(ts,ts[i]-60*MS_MIN,'left'); dc[i]=len(set(wl[lo:i+1]))-1
    clu.append(pd.DataFrame({'idx':grp.index,'f1':dc}))
g=g.join(pd.concat(clu).set_index('idx'))
hi=g[g.f1>=g.f1.quantile(0.66)].copy()
hi=hi[hi.coin!='HYPE']  # FROZEN: ex-HYPE (no HYPE-specific decision)
sub=hi.sample(min(10000,len(hi)),random_state=7)
rows=[]
for r in sub.itertuples(index=False):
    sgn=1.0 if r.side=='long' else -1.0; ex=asym(r.coin,r.entry_ts,sgn)
    if ex is None: continue
    rows.append(dict(coin=r.coin,ts=r.entry_ts,net=ex-EX.fee_rt(False)-2*EX.slip_oneway(r.coin)))
df=pd.DataFrame(rows).sort_values('ts')
def extop(a): a=np.sort(a); k=max(1,len(a)//100); return a[:-k].mean()
print('FROZEN WALK-FORWARD (ex-HYPE hi-clustering + let-winners-run), 5 time folds:')
df['fold']=pd.qcut(df.ts.rank(method='first'),5,labels=False)
for f in range(5):
    d=df[df.fold==f]; print('  fold%d (n%d): mean %+.0fbps med %+.0fbps pos %.3f ex-top1%% %+.0fbps' % (f,len(d),d.net.mean()*1e4,d.net.median()*1e4,(d.net>0).mean(),extop(d.net.values)*1e4))
# leave-one-coin-out (drop each big contributor)
print('  LEAVE-ONE-COIN-OUT (drop top contributors):')
cby=df.groupby('coin').net.sum().sort_values()
for c in cby.tail(3).index:
    d=df[df.coin!=c]; print('    ex-%s (n%d): mean %+.0fbps med %+.0fbps pos %.3f' % (c,len(d),d.net.mean()*1e4,d.net.median()*1e4,(d.net>0).mean()))
print('  ALL: mean %+.0fbps med %+.0fbps pos %.3f ex-top1%% %+.0f | coins +: %d/%d' % (df.net.mean()*1e4,df.net.median()*1e4,(df.net>0).mean(),extop(df.net.values)*1e4,(cby>0).sum(),df.coin.nunique()))
