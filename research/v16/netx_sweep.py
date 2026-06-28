#!/usr/bin/env python
"""netx_sweep.py -- validate raising the NETX cap to unlock copy rate (Alberto 9929: low copy rate = netx-throttled).
Reuses the codex-validated cap_level_sweep_v2 replay. Fix gross=4.0; sweep netx in {2.5,3.0,3.5,4.0}. Measure
copies TAKEN (copy rate), $/mo, peak gross, maxDD. Shows the copy-rate lift and drawdown cost of raising netx."""
import json, numpy as np, pandas as pd
from pymongo import MongoClient
EQ0=475.0; RT=11.0; GROSS=4.0; BACKSTOP=5.0; N=150.0
NETX_GRID=[2.5,3.0,3.5,4.0]
def load_candles(coins):
    db=MongoClient("mongodb://localhost:27017").quants_lab; out={}
    for c in coins:
        rows=list(db.hyperliquid_candles_1h.find({"coin":c},{"timestamp_utc":1,"close":1,"_id":0}).sort("timestamp_utc",1))
        if len(rows)>20:
            df=pd.DataFrame(rows); out[c]=(df.timestamp_utc.to_numpy(),df.close.to_numpy())
    return out
def mark(cand,coin,t):
    if coin not in cand: return None
    ts,cl=cand[coin]; i=np.searchsorted(ts,t); return cl[min(i,len(cl)-1)] if len(ts) else None
def main():
    sk=set(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    j=pd.read_parquet("app/data/v15/m02_journeys.parquet",columns=["wallet","coin","side","entry_ts","exit_ts","net_realized_pnl","max_position_notional"])
    j=j[(j.wallet.isin(sk))&(~j.coin.str.startswith("xyz:"))&(j.max_position_notional>10)].copy()
    j["ret"]=j.net_realized_pnl/j.max_position_notional; j=j[j.ret.between(-1.0,2.0)].copy()
    j["sgn"]=j.side.str.lower().map(lambda s:1.0 if "long" in str(s) else -1.0)
    j["t_en"]=j.entry_ts.astype("float64"); j["t_ex"]=j.exit_ts.astype("float64")
    j=j[j.t_ex>j.t_en].dropna(subset=["t_en","t_ex"]).sort_values("t_en").reset_index(drop=True)
    cand=load_candles([c for c in j.coin.value_counts().index if j.coin.value_counts()[c]>=30]); j=j[j.coin.isin(cand)].reset_index(drop=True)
    ndays=(j.t_en.max()-j.t_en.min())/86400e3
    print(f"journeys {len(j)} over {ndays:.0f}d ({(j.sgn>0).mean()*100:.0f}% long). gross={GROSS} fixed; sweep NETX.\n")
    def replay(netx):
        realized=0.0; open_pos=[]; taken=0; avail=0; eq_curve=[]; peak_netx=0.0; trips=0
        for r in j.itertuples():
            now=r.t_en; still=[]
            for ex,c,sg,epx,p in open_pos:
                if ex<=now: realized+=p
                else: still.append((ex,c,sg,epx,p))
            open_pos=still
            mtm=0.0; net_ntl=0.0; gross_ntl=0.0; marks=[]
            for ex,c,sg,epx,p in open_pos:
                mpx=mark(cand,c,now); upl=sg*(mpx-epx)/epx*N if (mpx and epx) else 0.0
                mtm+=upl; net_ntl+=sg*N; gross_ntl+=N; marks.append(upl)
            eq=EQ0+realized+mtm; eq_curve.append(eq)
            if gross_ntl>BACKSTOP*max(eq,1) and open_pos:
                trips+=1; order=np.argsort(marks); keep=list(open_pos); target=GROSS*max(eq,1); g=gross_ntl
                for idx in order:
                    if g<=target: break
                    ex,c,sg,epx,p=open_pos[idx]; realized+=p; g-=N; keep[idx]=None
                open_pos=[pp for pp in keep if pp is not None]; net_ntl=sum(sg*N for _,_,sg,_,_ in open_pos); gross_ntl=g
            avail+=1; new_signed=r.sgn*N
            if abs(net_ntl+new_signed)>netx*eq and abs(net_ntl+new_signed)>abs(net_ntl): continue
            if gross_ntl+N>GROSS*max(eq,1): continue
            epx=mark(cand,r.coin,now); pnl=(r.ret-RT/1e4)*N
            open_pos.append((r.t_ex,r.coin,r.sgn,epx if epx else 1,pnl)); taken+=1
            peak_netx=max(peak_netx,abs(net_ntl+new_signed)/max(eq,1))
        for ex,c,sg,epx,p in open_pos: realized+=p
        ec=np.array(eq_curve); peak=np.maximum.accumulate(ec); maxdd=((peak-ec)/peak).max()*100 if len(ec) else 0
        return dict(taken=taken,avail=avail,cover=taken/max(avail,1)*100,usd_mo=realized/ndays*30.4,peak_netx=peak_netx,maxdd=maxdd,trips=trips)
    base=replay(2.5)
    print(f"{'netx':>6}{'copies':>8}{'cover%':>8}{'$/mo':>8}{'vs2.5':>7}{'pkNetx':>8}{'maxDD%':>8}")
    for nx in NETX_GRID:
        d=replay(nx); dv=(d['usd_mo']/base['usd_mo']-1)*100 if base['usd_mo'] else 0
        print(f"{nx:>6.1f}{d['taken']:>8}{d['cover']:>7.0f}%{d['usd_mo']:>8.0f}{dv:>+6.0f}%{d['peak_netx']:>7.2f}x{d['maxdd']:>8.1f}")
    print("\nREAD: higher netx -> more copies (cover%) + $/mo (unlocks copy rate Alberto flagged) at higher peak-netx")
    print("+ maxDD (the accepted directional risk). Pick the netx that maximizes $/mo at a drawdown Alberto tolerates. codex next.")
if __name__=="__main__": main()
