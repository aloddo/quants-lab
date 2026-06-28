import sys; sys.path.insert(0,'research/v16'); sys.path.insert(0,'research/v15')
import decoupled_exit_portfolio as D, execution_model as EX, numpy as np, pandas as pd
MS_MIN=60_000; MS_HR=3600_000; LAT=1860; MAXH=7*24*60; HARD=-0.15; ARM=0.10; TRAIL=0.30
btc=D.marks('BTC')
def asym(coin,ets,sign):
    m=D.marks(coin)
    if m is None: return None
    mins,px=m; e=int(ets)+LAT; i0=np.searchsorted(mins,e,'right')-1
    if i0<0 or px[i0]<=0 or mins[i0]>=e+MAXH*MS_MIN: return None
    i1=min(np.searchsorted(mins,e+MAXH*MS_MIN,'right'),len(px)); ep=px[i0]; r=(px[i0:i1]-ep)/ep*sign
    peak=-1e9
    for k in range(1,len(r)):
        peak=max(peak,r[k])
        if r[k]<=HARD: return r[k], int(mins[i0+k]-e)
        if peak>=ARM and r[k]<=peak-TRAIL: return r[k], int(mins[i0+k]-e)
    return r[-1], int(mins[i1-1]-e)
def bret(a,b):
    bm,bp=btc; i0=np.searchsorted(bm,a,'right')-1; i1=np.searchsorted(bm,b,'right')-1
    if i0<0 or i1<=i0 or i1>=len(bp) or bp[i0]<=0: return 0.0
    return (bp[i1]-bp[i0])/bp[i0]
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
hi=g[g.f1>=g.f1.quantile(0.66)]   # HIGH-CLUSTERING subset (tail-eligible filter)
sub=hi.sample(min(8000,len(hi)),random_state=5)
rows=[]
for r in sub.itertuples(index=False):
    sgn=1.0 if r.side=='long' else -1.0; res=asym(r.coin,r.entry_ts,sgn)
    if res is None: continue
    raw,off=res; e=int(r.entry_ts)+LAT; cost=EX.fee_rt(False)+2*EX.slip_oneway(r.coin)
    rows.append(dict(coin=r.coin,ts=r.entry_ts,net=raw-cost,mneut=raw-sgn*bret(e,e+off)-cost))
df=pd.DataFrame(rows)
def extop(a): a=np.sort(a); k=max(1,len(a)//100); return a[:-k].mean()
cut=df.ts.quantile(0.7)
print('HIGH-CLUSTERING + let-winners-run controls, n=%d' % len(df))
print('  RAW: ALL mean %+.0fbps ex-top1%% %+.0fbps | OOS mean %+.0fbps ex-top1%% %+.0f' % (df.net.mean()*1e4, extop(df.net.values)*1e4, df[df.ts>cut].net.mean()*1e4, extop(df[df.ts>cut].net.values)*1e4))
print('  MKT-NEUTRAL: ALL mean %+.0fbps ex-top1%% %+.0fbps med %+.0f | OOS mean %+.0fbps ex-top1%% %+.0f' % (df.mneut.mean()*1e4, extop(df.mneut.values)*1e4, df.mneut.median()*1e4, df[df.ts>cut].mneut.mean()*1e4, extop(df[df.ts>cut].mneut.values)*1e4))
cby=df.groupby('coin').net.sum().sort_values(); tot=df.net.sum()
print('  CONCENTRATION: total %+.0f%% | top coin %s %+.0f%% (%.0f%%) | top-3 coins %.0f%% | n coins %d' % (tot*100, cby.idxmax(), cby.max()*100, cby.max()/tot*100 if tot else 0, cby.tail(3).sum()/tot*100 if tot else 0, df.coin.nunique()))
print('  %% positive %.3f | median net %+.0fbps' % ((df.net>0).mean(), df.net.median()*1e4))
