import sys; sys.path.insert(0,'research/v16'); sys.path.insert(0,'research/v15')
import decoupled_exit_portfolio as D, execution_model as EX, numpy as np, pandas as pd
MS_MIN=60_000; MS_HR=3600_000; LAT=1860; MAXH=7*24*60; HARD=-0.15; ARM=0.10; TRAIL=0.30
def cret(coin,a,b):
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
clu=[]
for (c,s),grp in g.groupby(['coin','side']):
    grp=grp.sort_values('entry_ts'); ts=grp.entry_ts.values; wl=grp.wallet.values; n=len(grp); dc=np.zeros(n,int)
    for i in range(n):
        lo=np.searchsorted(ts,ts[i]-60*MS_MIN,'left'); dc[i]=len(set(wl[lo:i+1]))-1
    clu.append(pd.DataFrame({'idx':grp.index,'f1':dc}))
g=g.join(pd.concat(clu).set_index('idx'))
hi=g[g.f1>=g.f1.quantile(0.66)]
sub=hi.sample(min(9000,len(hi)),random_state=6)
rows=[]
for r in sub.itertuples(index=False):
    sgn=1.0 if r.side=='long' else -1.0; ex=asym(r.coin,r.entry_ts,sgn)
    if ex is None: continue
    e=int(r.entry_ts); pre60=cret(r.coin,e-60*MS_MIN,e)  # pre-entry move (signed by trade direction)
    if not np.isfinite(pre60): continue
    rows.append(dict(coin=r.coin,net=ex-EX.fee_rt(False)-2*EX.slip_oneway(r.coin),pre60=pre60*sgn))
df=pd.DataFrame(rows)
df=df[df.coin!='HYPE']  # ex-HYPE (decisive)
df['preq']=pd.qcut(df.pre60.rank(method='first'),5,labels=False)
print('MOMENTUM CONTROL (ex-HYPE hi-clustering): net by PRE-ENTRY 60m move quintile (q0=already fell, q4=already pumped)')
q=df.groupby('preq').agg(pre=('pre60','mean'),net_mean=('net','mean'),net_med=('net','median'),pos=('net',lambda s:(s>0).mean()),n=('net','size'))
for i,row in q.iterrows():
    print('  preq%d (pre60 %+.1f%%): net mean %+.0fbps med %+.0fbps pos %.3f n %d' % (i,row.pre*100,row.net_mean*1e4,row.net_med*1e4,row.pos,row.n))
print('  => is the edge ONLY in q4 (already pumped=momentum) or broad (real copy info)?')
