"""M1 cent-reconciliation DIAGNOSTIC (read-only; does NOT modify M1). For each consecutive HL
anchor pair, decompose the reconstruction residual = recon_equity(b) - HL_anchor(b) into components
so we can see WHICH captured/uncaptured flow leaks cents. Reuses M1's own functions for faithfulness.
Usage: python /tmp/m1_residual_decomp.py 0xWALLET [0xWALLET2 ...]"""
import sys, os
sys.path.insert(0, "research/v15")
import pandas as pd, numpy as np
import v15_m01_equity_reconstruct as m01

START="2025-12-01"; END="2026-05-23"
start_ms=int(pd.Timestamp(START,tz="UTC").timestamp()*1000)
end_ms=int(pd.Timestamp(END,tz="UTC").timestamp()*1000+86_399_999)
anchor_df=pd.read_parquet(m01.ANCHOR_PARQUET)

def decomp(wallet):
    wl=wallet.lower()
    anchor=m01.load_wallet_anchor(wallet, anchor_df)
    if anchor is None: print(f"{wallet}: no anchor row"); return
    avh=m01.get_portfolio_perp(wallet)
    valid=[(t,v) for t,v in avh if v>0.01]
    walk_end=min(end_ms, anchor.fetched_ms); load_end=max(walk_end,int(anchor.fetched_ms))
    fills=m01.load_wallet_fills(wallet,start_ms,load_end)
    funding=m01.load_wallet_funding(wallet,start_ms,load_end)
    ledger=m01.load_wallet_ledger(wallet,start_ms,load_end)
    stream=[(f["time"],"fill",f) for f in fills]+[(int(e["time"]),"ledger",e) for e in ledger]+[(int(e["time"]),"funding",e) for e in funding]
    stream.sort(key=lambda x:x[0])
    wa=[(t,v) for t,v in valid if start_ms<=t<=walk_end]
    dexes=sorted({m01.coin_dex(f["coin"]) for f in fills}|{m01.coin_dex(c) for c in anchor.positions})
    print(f"\n===== {wallet} | dexes={dexes} | anchors={len(wa)} | fills={len(fills)} ledger={len(ledger)} funding={len(funding)} =====")
    print(f"{'seg':>3} {'dHL$':>12} {'fillcash':>11} {'funding':>9} {'ledgcash':>10} {'dPosVal':>11} {'residual$':>12} {'resid%':>7} {'unmk':>4} ledger_types_in_seg")
    for i in range(1,len(wa)):
        a_t,a_v=wa[i-1]; b_t,b_v=wa[i]
        if b_t<=a_t: continue
        wr=m01.compute_eq_at(stream,fills,anchor,wl,b_t,a_t,a_v)  # faithful recon at b from a
        # component sums in (a_t,b_t]
        fillcash=funding_c=ledgcash=0.0; ltypes={}
        for ts,typ,ev in stream:
            if ts<=a_t or ts>b_t: continue
            if typ=="fill": fillcash+=m01.fill_cash_delta(ev)
            elif typ=="funding": funding_c+=m01.funding_cash_delta(ev)
            elif typ=="ledger":
                ld=m01.ledger_cash_delta(ev,wl); ledgcash+=ld.cash
                t=ev.get("delta",{}).get("type","?"); ltypes[t]=ltypes.get(t,0.0)+ld.cash
        wr_a=m01.compute_eq_at(stream,fills,anchor,wl,a_t,a_t,a_v)
        dpos=wr.position_value-wr_a.position_value
        residual=wr.equity-b_v
        resid_pct=residual/b_v*100 if abs(b_v)>0.01 else float('nan')
        lt=",".join(f"{k}:{v:+.0f}" for k,v in sorted(ltypes.items(),key=lambda x:-abs(x[1]))[:4])
        flag=" INCOMPLETE" if wr.recon_incomplete else ""
        print(f"{i:>3} {b_v-a_v:>12,.0f} {fillcash:>11,.0f} {funding_c:>9,.0f} {ledgcash:>10,.0f} {dpos:>11,.0f} {residual:>12,.2f} {resid_pct:>6.1f}% {wr.n_unmarkable:>4} {lt}{flag}")

for w in sys.argv[1:]:
    decomp(w)
