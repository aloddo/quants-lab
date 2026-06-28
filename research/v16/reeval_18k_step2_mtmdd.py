#!/usr/bin/env python
"""Re-eval STEP 2: mark-to-market DD for the martingale-clean pool (HL portfolio account equity incl
unrealized). Gates the near-ruin tail. Saves pool + MTM DD."""
import json, urllib.request, time
import numpy as np, pandas as pd
def post(b):
    r=urllib.request.Request("https://api.hyperliquid.xyz/info",data=json.dumps(b).encode(),headers={"Content-Type":"application/json"})
    return json.load(urllib.request.urlopen(r,timeout=20))
def mtm(w,tries=2):
    for k in range(tries):
        try:
            d=dict(post({"type":"portfolio","user":w})); a=np.array([float(v) for _,v in d.get("month",{}).get("accountValueHistory",[])]); a=a[a>0]
            if len(a)<5: return None
            return (((np.maximum.accumulate(a)-a)/np.maximum.accumulate(a)).max()*100, (a[-1]/a[0]-1)*100)
        except Exception: time.sleep(0.5)
    return None
def main():
    pool=pd.read_parquet("/tmp/reeval_clean_pool.parquet")
    ws=list(pool.index)
    print(f"pulling mark-to-market DD for {len(ws)} martingale-clean wallets ...")
    dd={}; ret={}; fail=0
    for i,w in enumerate(ws):
        r=mtm(w)
        if r: dd[w],ret[w]=r
        else: fail+=1
        if i%400==0: print(f"  ...{i}/{len(ws)} (fail {fail})")
        time.sleep(0.04)
    pool["mtm_dd"]=pool.index.map(dd); pool["mtm_ret"]=pool.index.map(ret)
    p=pool.dropna(subset=["mtm_dd"])
    nearruin=((p.mtm_dd>=70)|((p.mtm_dd>=60)&(p.mtm_dd<70)&(p.mtm_ret<=-40)))
    p_clean=p[~nearruin]
    print(f"\nwith MTM DD: {len(p)} | near-ruin gated: {int(nearruin.sum())} | MTM-CLEAN: {len(p_clean)}")
    print(f"MTM-clean pool MTM DD: median {p_clean.mtm_dd.median():.0f}% | >50%DD remaining: {(p_clean.mtm_dd>50).sum()}")
    pool.to_parquet("/tmp/reeval_pool_mtm.parquet")
    print("saved /tmp/reeval_pool_mtm.parquet")
if __name__=="__main__": main()
