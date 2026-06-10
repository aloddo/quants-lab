"""Per-wallet cent-reconciliation harness: max|segment residual| after the recipe (exact marks +
whatever funding/ledger is cached). Reports each wallet's worst segment so we can drive all -> 0."""
import sys; sys.path.insert(0,"research/v15")
import pandas as pd, numpy as np, v15_m01_equity_reconstruct as m01
S=int(pd.Timestamp('2025-12-01',tz='UTC').timestamp()*1000); E=int(pd.Timestamp('2026-05-23',tz='UTC').timestamp()*1000+86_399_999)
adf=pd.read_parquet(m01.ANCHOR_PARQUET)
def maxresid(w):
    w=w.lower(); anchor=m01.load_wallet_anchor(w,adf)
    if anchor is None: return None,"no_anchor"
    avh=m01.get_portfolio_perp(w); wa=[(t,v) for t,v in avh if v>0.01 and S<=t<=E]
    if len(wa)<2: return None,"no_anchors"
    we=min(E,anchor.fetched_ms); le=max(we,int(anchor.fetched_ms))
    fills=m01.load_wallet_fills(w,S,le); fu=m01.load_wallet_funding(w,S,le); ld=m01.load_wallet_ledger(w,S,le)
    stream=[(f["time"],"fill",f) for f in fills]+[(int(x["time"]),"ledger",x) for x in ld]+[(int(x["time"]),"funding",x) for x in fu]; stream.sort(key=lambda x:x[0])
    wa=[(t,v) for t,v in wa if S<=t<=we]
    res=[]; exotic=any(":" in f["coin"] for f in fills)
    for i in range(1,len(wa)):
        a_t,a_v=wa[i-1]; b_t,b_v=wa[i]
        if b_t<=a_t: continue
        wr=m01.compute_eq_at(stream,fills,anchor,w,b_t,a_t,a_v)
        if wr.recon_incomplete: res.append(("inc",b_v*0.01)); continue
        res.append((i,wr.equity-b_v))
    if not res: return None,"no_segs"
    mx=max(abs(r[1]) for r in res); med=np.median([abs(r[1]) for r in res])
    return (mx,med,len(res),exotic),None
print(f"{'wallet':14s} {'maxResid$':>10s} {'medResid$':>10s} {'segs':>5s} exotic?")
for w in [l.strip() for l in open("app/data/v15/m01_validation_wallets.txt") if l.strip().startswith("0x")]:
    r,err=maxresid(w)
    if err: print(f"{w[:12]:14s} {err}"); continue
    mx,med,n,ex=r
    print(f"{w[:12]:14s} {mx:>10.2f} {med:>10.2f} {n:>5d} {'EXOTIC' if ex else 'main-only'}")
