#!/usr/bin/env python3
"""Balanced cohort STEP 1 (codex-approved spec): per-(wallet,side) SHRUNK copy edge on the WHOLE m02
universe, offline (mark_at copy prices + execution_model), with min-sample gates + empirical-Bayes
shrinkage. Identifies robust long-edge and short-edge leader sets (kills small-sample noise). V15-native."""
import sys, json
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, slip_oneway
FEE_T=fee_rt(maker=False); CAP=500.0/1e4; LAT=2000
TRAIN_END="2026-05-23"   # whole history up to asof
MIN_RT=30; MIN_DAYS=10
def main():
    cols=["wallet","coin","side","entry_ts","exit_ts","max_position_notional"]
    j=pd.read_parquet("app/data/v15/m02_journeys.parquet",columns=cols)
    j=j[j.max_position_notional>10].copy()
    j=j.dropna(subset=["entry_ts","exit_ts"]); j=j[j.exit_ts>j.entry_ts]
    j["t"]=pd.to_datetime(j.entry_ts,unit="ms"); j=j[j.t<=pd.Timestamp(TRAIN_END)]
    j["is_long"]=j.side.str.lower().str.contains("long")
    print(f"universe {j.wallet.nunique()} wallets, {len(j)} journeys (<= {TRAIN_END}); computing per-side copy edge ...")
    # copy edge per journey via marks (fidelity), net fee+slip
    rows=[]; coins=j.coin.unique()
    # group by coin to batch mark lookups
    for coin, g in j.groupby("coin"):
        for r in g.itertuples():
            ent=S.mark_at(coin, int(r.entry_ts)+LAT); ex=S.mark_at(coin, int(r.exit_ts)+LAT)
            if ent and ex and ent>0:
                og=(ex-ent)/ent if r.is_long else (ent-ex)/ent
                net=max(-CAP,min(CAP,og))-FEE_T-slip_oneway(coin)*2
                rows.append((r.wallet, r.is_long, net, int(r.entry_ts)//86400000))
    d=pd.DataFrame(rows,columns=["wallet","is_long","net","day"])
    # universe mean edge (for shrinkage prior)
    uni_mean=d.net.mean()
    out=[]
    for (w,side), g in d.groupby(["wallet","is_long"]):
        n=len(g); ndays=g.day.nunique()
        if n<15 or ndays<5: continue   # hard floor
        raw=g.net.mean()*1e4
        # empirical-Bayes shrinkage: shrink toward universe mean by sample size (k pseudo-obs)
        k=30.0; shrunk=(n*raw + k*uni_mean*1e4)/(n+k)
        std=g.net.std()*1e4 if n>1 else 0
        lcb=shrunk - 1.0*std/np.sqrt(n)   # lower-confidence-bound after costs
        passes = (n>=MIN_RT and ndays>=MIN_DAYS)
        out.append({"wallet":w,"side":"LONG" if side else "SHORT","n":n,"ndays":ndays,
                    "raw_bps":raw,"shrunk_bps":shrunk,"lcb_bps":lcb,"min_sample_ok":passes})
    o=pd.DataFrame(out)
    o.to_parquet("/tmp/balanced_perside_edge.parquet")
    print(f"\nper-(wallet,side) records: {len(o)} | min-sample-ok: {int(o.min_sample_ok.sum())}")
    for side in ["LONG","SHORT"]:
        s=o[(o.side==side)&(o.min_sample_ok)]
        pos=s[s.lcb_bps>0]   # positive after shrinkage + lower-confidence
        print(f"  {side}: {len(s)} min-sample-ok | LCB-positive (robust edge): {len(pos)} | median shrunk {s.shrunk_bps.median():+.0f}bps")
    print("saved /tmp/balanced_perside_edge.parquet")
if __name__=="__main__": main()
