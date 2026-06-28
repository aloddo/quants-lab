#!/usr/bin/env python3
"""Balanced cohort STEP 2-4: WALK-FORWARD regime validation (codex #1 concern: is balanced low-DD real
across regimes or just the down-window?). Per-(wallet,side,month) copy edge on robust candidates; for each
test month select per-side edge from PRIOR months, build balanced (equal long/short) book, measure
round-trip portfolio ROE + maxDD per fold (regime-labeled). KEY: does balanced hold in the Apr BULL fold?
NOTE: round-trip DD = FIRST-PASS approximation (codex: MTM via m07/m09 is the final gate). V15 marks+exec."""
import sys
from pathlib import Path
from collections import defaultdict
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, slip_oneway
FEE_T=fee_rt(maker=False); CAP=500.0/1e4; LAT=2000
REGIME={'2025-12':'CHOP','2026-01':'BEAR','2026-02':'BEAR','2026-03':'CHOP','2026-04':'BULL','2026-05':'CHOP'}
def port(legs):
    if not legs: return (0,0,0)
    sc=1.0/len(legs); trades=[]; wr=[]
    for n in legs: wr.append(sum(x for _,x in n)); trades+=n
    if not trades: return (0,0,0)
    roe=sum(sc*x for x in wr)*100
    trades.sort(key=lambda x:x[0]); eq=1.;pk=1.;mdd=0
    for _,x in trades: eq+=sc*x; pk=max(pk,eq); mdd=max(mdd,(pk-eq)/pk)
    return (roe,mdd*100,len(trades))
def main():
    edge=pd.read_parquet("/tmp/balanced_perside_edge.parquet")
    cand=set(edge[edge.min_sample_ok].wallet)
    print(f"robust candidates: {len(cand)} wallets. Computing per-(wallet,side,month) copy edge + legs ...")
    cols=["wallet","coin","side","entry_ts","exit_ts","max_position_notional"]
    j=pd.read_parquet("app/data/v15/m02_journeys.parquet",columns=cols)
    j=j.dropna(subset=["entry_ts","exit_ts"]); j=j[(j.max_position_notional>10)&(j.wallet.isin(cand))&(j.exit_ts>j.entry_ts)]
    j["is_long"]=j.side.str.lower().str.contains("long"); j["mon"]=pd.to_datetime(j.entry_ts,unit="ms").dt.strftime("%Y-%m")
    j=j[j.mon.isin(REGIME)]
    # per-journey net + (wallet,side,month) leg store
    legs=defaultdict(list)  # (wallet,is_long,mon)->[(ts,net)]
    for coin,g in j.groupby("coin"):
        for r in g.itertuples():
            ent=S.mark_at(coin,int(r.entry_ts)+LAT); ex=S.mark_at(coin,int(r.exit_ts)+LAT)
            if ent and ex and ent>0:
                og=(ex-ent)/ent if r.is_long else (ent-ex)/ent
                legs[(r.wallet,r.is_long,r.mon)].append((int(r.entry_ts),max(-CAP,min(CAP,og))-FEE_T-slip_oneway(coin)*2))
    months=[m for m in ["2025-12","2026-01","2026-02","2026-03","2026-04","2026-05"]]
    print(f"\n{'fold':>9}{'regime':>7}{'LONGonly':>20}{'BALANCED(L+S)':>22}")
    print(f"{'':>16}{'ROE%':>9}{'DD%':>7}{'ROE%':>11}{'DD%':>7}{'nL/nS':>9}")
    for ti in range(1,len(months)):
        tm=months[ti]; prior=months[:ti]
        # per-side edge from PRIOR months
        def pe(is_long):
            agg=defaultdict(list)
            for (w,sd,mo),ls in legs.items():
                if mo in prior and sd==is_long: agg[w]+=[x for _,x in ls]
            return {w:np.mean(v) for w,v in agg.items() if len(v)>=10 and np.mean(v)>0}
        long_e=pe(True); short_e=pe(False)
        Ls=sorted(long_e,key=lambda w:-long_e[w])[:30]; Ss=sorted(short_e,key=lambda w:-short_e[w])[:30]
        # test-month legs
        def leg(w,is_long): return legs.get((w,is_long,tm),[])
        long_only=[leg(w,True) for w in Ls]
        balanced=[leg(w,True) for w in Ls[:20]]+[leg(w,False) for w in Ss[:20]]   # ~balanced count
        lo=port(long_only); ba=port(balanced)
        print(f"{tm:>9}{REGIME[tm]:>7}{lo[0]:>9.1f}{lo[1]:>7.1f}{ba[0]:>11.1f}{ba[1]:>7.1f}{f'{len(Ls)}/{len(Ss)}':>9}")
    print("\nKEY: does BALANCED hold low DD + positive ROE in the Apr BULL fold (not just bears)? If yes -> not")
    print("just down-tape beta. (Round-trip DD = first-pass; MTM via m07/m09 is the codex final gate.)")
if __name__=="__main__": main()
