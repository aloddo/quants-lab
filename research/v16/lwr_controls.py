import sys; sys.path.insert(0,'research/v16'); sys.path.insert(0,'research/v15')
import decoupled_exit_portfolio as D, execution_model as EX, numpy as np, pandas as pd
MS_MIN=60_000; LAT=1860; MAXH=7*24*60
HARD=-0.15; ARM=0.10; TRAIL=0.30   # deployable wide-stop config
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
def btc_ret(a,b):
    bm,bp=btc; i0=np.searchsorted(bm,a,'right')-1; i1=np.searchsorted(bm,b,'right')-1
    if i0<0 or i1<=i0 or i1>=len(bp) or bp[i0]<=0: return 0.0
    return (bp[i1]-bp[i0])/bp[i0]
j=pd.read_parquet('app/data/v15/m02_journeys.parquet',columns=['wallet','coin','side','entry_ts','max_position_notional','net_realized_pnl']).sort_values('entry_ts')
LIQ=set(['BTC','ETH','SOL','HYPE','XRP','DOGE','AVAX','LINK','LTC','ADA','DOT','SUI','TAO','ARB','APT','SEI','NEAR','AAVE','INJ','UNI','PEPE','WLD','FARTCOIN','TON','TRX','FIL','ATOM','OP','ENA','VVV'])
j=j[(j.coin.isin(LIQ))&(j.max_position_notional>200)].copy()
j['cum']=j.groupby('wallet').net_realized_pnl.transform(lambda s:s.shift(1).expanding(min_periods=5).sum())
sub=j[j.cum>0].sample(15000,random_state=3)
rows=[]
for r in sub.itertuples(index=False):
    sgn=1.0 if r.side=='long' else -1.0; res=asym(r.coin,r.entry_ts,sgn)
    if res is None: continue
    raw,off=res; cost=EX.fee_rt(False)+2*EX.slip_oneway(r.coin)
    e=int(r.entry_ts)+LAT; bret=btc_ret(e,e+off)
    rows.append(dict(coin=r.coin,ts=r.entry_ts,net=raw-cost,mneut=raw-sgn*bret-cost,off_h=off/3600000))
df=pd.DataFrame(rows)
cut=df.ts.quantile(0.7)
print('LET-WINNERS-RUN controls (stop15/trail30), n=%d, median hold %.1fh' % (len(df), df.off_h.median()))
print('  RAW directional: ALL mean %+.1fbps | OOS mean %+.1fbps' % (df.net.mean()*1e4, df[df.ts>cut].net.mean()*1e4))
print('  MARKET-NEUTRAL : ALL mean %+.1fbps median %+.1f | OOS mean %+.1fbps' % (df.mneut.mean()*1e4, df.mneut.median()*1e4, df[df.ts>cut].mneut.mean()*1e4))
s=df.sort_values('net'); top5=s.net.tail(5).sum(); tot=df.net.sum()
print('  CONCENTRATION: total %+.0f%% | top-5 trades %+.0f%% (%.0f%% of total) | ex-top-1%% mean %+.1fbps' % (tot*100, top5*100, top5/tot*100 if tot else 0, s.net.iloc[:-150].mean()*1e4))
df['day']=(df.ts//86400000); byday=df.groupby('day').net.sum().sort_values(); cby=df.groupby('coin').net.sum()
print('  top-5 DAYS net %+.0f%% of total %+.0f; days %d; top coin %s %+.0f%%' % (byday.tail(5).sum()*100, tot*100, df.day.nunique(), cby.idxmax(), cby.max()*100))
