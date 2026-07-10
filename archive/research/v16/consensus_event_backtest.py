"""
CONSENSUS-EVENT copy backtest (Alberto: "it has to be copy"; codex top architecture). Trade the consensus
EPISODE as ONE position, not per-trade. When >=TRIGGER distinct GOOD wallets enter a coin+side within a
rolling window, open ONE position; HOLD through the episode while good wallets keep piling in; EXIT on decay.
ONE round-trip cost per episode (the cost-wall escape). Canonical execution_model slip. De-trended + OOS.
"""
import numpy as np, pandas as pd
import sys; sys.path.insert(0, "research/v16"); sys.path.insert(0, "research/v15")
import decoupled_exit_portfolio as D, execution_model as EX

LIQUID = ["BTC","ETH","SOL","HYPE","XRP","DOGE","AVAX","LINK","LTC","ADA","DOT","SUI","TAO","ARB","APT",
          "SEI","NEAR","AAVE","INJ","UNI","PEPE","WLD","FARTCOIN","TON","TRX","FIL","ATOM","OP"]
MS_MIN=60_000; MS_HR=3600_000; LAT=1860
WIN=60*MS_MIN          # rolling consensus window 60min
TRIGGER=8              # distinct good wallets to open an episode
DECAY_GAP=4*MS_HR      # episode ends if no new good-wallet entry for 4h
MAX_HOLD=48*MS_HR

def px(coin, ts):
    m=D.marks(coin)
    if m is None: return np.nan
    mins,p=m; i=np.searchsorted(mins,ts,"right")-1
    return p[i] if 0<=i<len(p) and p[i]>0 else np.nan

def episodes(grp):
    """grp: good-wallet entries for one coin+side, sorted by ts. Yield (start_ts, end_ts)."""
    ts=grp.entry_ts.values; wl=grp.wallet.values; n=len(ts); out=[]
    i=0
    while i<n:
        # rolling distinct count ending at i
        lo=np.searchsorted(ts,ts[i]-WIN,"left")
        if len(set(wl[lo:i+1]))>=TRIGGER:
            start=ts[i]; last=ts[i]; j=i+1
            while j<n and ts[j]-last<=DECAY_GAP and ts[j]-start<=MAX_HOLD:
                last=ts[j]; j+=1
            end=min(last+DECAY_GAP, start+MAX_HOLD)
            out.append((start,end)); i=j
        else:
            i+=1
    return out

def main():
    j=pd.read_parquet("app/data/v15/m02_journeys.parquet",
                      columns=["wallet","coin","side","entry_ts","max_position_notional","net_realized_pnl"]).sort_values("entry_ts")
    j=j[(j.coin.isin(LIQUID))&(j.max_position_notional>200)].copy()
    j["cum"]=j.groupby("wallet").net_realized_pnl.transform(lambda s:s.shift(1).expanding(min_periods=5).sum())
    j=j[j.cum>0]   # GOOD wallets only (causal positive trailing PnL)
    rows=[]
    for (coin,side),grp in j.groupby(["coin","side"]):
        sgn=1.0 if side=="long" else -1.0
        for st,en in episodes(grp.sort_values("entry_ts")):
            ep=px(coin,st+LAT); xp=px(coin,en+LAT)
            if not (np.isfinite(ep) and np.isfinite(xp) and ep>0): continue
            gross=(xp-ep)/ep*sgn
            net=gross - EX.fee_rt(False) - 2*EX.slip_oneway(coin)   # ONE round trip per episode
            bt0=px("BTC",st+LAT); bt1=px("BTC",en+LAT)
            mneut=net-((bt1-bt0)/bt0*sgn if (np.isfinite(bt0) and np.isfinite(bt1) and bt0>0) else 0)
            rows.append(dict(coin=coin,side=side,st=st,hold_h=(en-st)/MS_HR,net=net,mneut=mneut))
    r=pd.DataFrame(rows)
    print(f"consensus episodes: {len(r)} | median hold {r.hold_h.median():.1f}h | trigger {TRIGGER} wallets/{WIN//MS_MIN}min")
    cut=r.st.quantile(0.7)
    for lbl,sub in [("ALL",r),("OOS(last30%)",r[r.st>cut])]:
        print(f"  {lbl} (n={len(sub)}): episode NET directional mean {sub.net.mean()*1e4:+6.1f}bps median {sub.net.median()*1e4:+6.1f} pos {(sub.net>0).mean():.3f} "
              f"| MKT-NEUTRAL mean {sub.mneut.mean()*1e4:+6.1f}bps med {sub.mneut.median()*1e4:+6.1f} pos {(sub.mneut>0).mean():.3f}")
    # annualized-ish: episodes per ~6mo, mean net per episode
    print(f"  total episodes/6mo {len(r)} -> ~{len(r)/6:.0f}/mo; sum NET directional {r.net.sum()*100:+.1f}% of 1-unit-per-episode over 6mo")

if __name__=="__main__":
    main()
