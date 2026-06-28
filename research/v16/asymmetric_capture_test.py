"""
ASYMMETRIC CAPTURE test (Alberto: the 9bps mean is a lie; catch the fat-tail winners, cut losers fast).
Copy GOOD-wallet entries; HARD-STOP losers at -8%; let WINNERS RUN with a wide trailing stop for up to 7 days.
Measure the actual PnL DISTRIBUTION (not the mean) -- does cutting losers + running winners capture the
fat right tail and flip the book positive? Canonical execution_model cost (one RT). De-trended secondary.
"""
import numpy as np, pandas as pd
import sys; sys.path.insert(0, "research/v16"); sys.path.insert(0, "research/v15")
import decoupled_exit_portfolio as D, execution_model as EX

MS_MIN=60_000; LAT=1860
HARD_STOP=-0.08            # cut losers fast
TP_ARM=0.10               # arm trailing once +10%
TRAIL=0.20                # let winners run: exit on 20% pullback from peak
MAX_HOLD_MIN=7*24*60      # 7 days

def asym_exit(coin, entry_ts, sign):
    m=D.marks(coin)
    if m is None: return None
    mins,px=m; e=int(entry_ts)+LAT
    i0=np.searchsorted(mins,e,"right")-1
    if i0<0 or px[i0]<=0: return None
    end=e+MAX_HOLD_MIN*MS_MIN
    if mins[i0]>=end: return None
    i1=min(np.searchsorted(mins,end,"right"), len(px))
    ep=px[i0]; path=px[i0:i1]
    r=(path-ep)/ep*sign
    peak=-1e9
    for k in range(1,len(r)):
        peak=max(peak,r[k])
        if r[k]<=HARD_STOP: return r[k]
        if peak>=TP_ARM and r[k]<=peak-TRAIL: return r[k]
    return r[-1]

def main():
    j=pd.read_parquet("app/data/v15/m02_journeys.parquet",
                      columns=["wallet","coin","side","entry_ts","max_position_notional","net_realized_pnl"]).sort_values("entry_ts")
    LIQ=set(["BTC","ETH","SOL","HYPE","XRP","DOGE","AVAX","LINK","LTC","ADA","DOT","SUI","TAO","ARB","APT",
             "SEI","NEAR","AAVE","INJ","UNI","PEPE","WLD","FARTCOIN","TON","TRX","FIL","ATOM","OP","ENA","VVV"])
    j=j[(j.coin.isin(LIQ))&(j.max_position_notional>200)].copy()
    j["cum"]=j.groupby("wallet").net_realized_pnl.transform(lambda s:s.shift(1).expanding(min_periods=5).sum())
    j=j[j.cum>0]    # GOOD wallets
    # sample for speed (the path loop is per-trade)
    sub=j.sample(min(40000,len(j)),random_state=1)
    rows=[]
    for r in sub.itertuples(index=False):
        sgn=1.0 if r.side=="long" else -1.0
        ex=asym_exit(r.coin,r.entry_ts,sgn)
        if ex is None: continue
        net=ex - EX.fee_rt(False) - 2*EX.slip_oneway(r.coin)
        rows.append(net)
    a=np.array(rows)
    print(f"ASYMMETRIC CAPTURE (cut -8%, trail 20% from +10% peak, 7d max): n={len(a)} good-wallet liquid trades")
    print(f"  MEAN {a.mean()*1e4:+.1f}bps | MEDIAN {np.median(a)*1e4:+.1f}bps | %positive {(a>0).mean():.3f}")
    print(f"  percentiles: p5 {np.percentile(a,5)*100:+.1f}% p25 {np.percentile(a,25)*100:+.1f}% p50 {np.percentile(a,50)*100:+.1f}% p75 {np.percentile(a,75)*100:+.1f}% p90 {np.percentile(a,90)*100:+.1f}% p95 {np.percentile(a,95)*100:+.1f}% p99 {np.percentile(a,99)*100:+.1f}% max {a.max()*100:+.1f}%")
    print(f"  SUM (total return, 1 unit/trade): {a.sum()*100:+.0f}% over {len(a)} trades")
    print(f"  winners>+50%: {(a>0.5).sum()} ({(a>0.5).mean()*100:.1f}%) contributing {a[a>0.5].sum()*100:+.0f}%; losers<-8%: {(a<-0.08).sum()} contributing {a[a<-0.08].sum()*100:+.0f}%")
    print(f"  => is it the WINNERS carrying it? mean-with-winners {a.mean()*1e4:+.0f}bps vs mean-without-top1% {np.sort(a)[:-max(1,len(a)//100)].mean()*1e4:+.0f}bps")

if __name__=="__main__":
    main()
