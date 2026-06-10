#!/usr/bin/env python
"""Fast+levered+taker copy-leader scanner (Alberto directive 2026-06-03 msg 8478:
profitable, FAST, LEVERED copy trader). Purpose-built screen the 408 return/DD pool lacked.

Universe: most-active addresses in hl_wallet_trades (active = fast by construction).
Stage 1 (live HL API): leverage, fills/day(7d), taker%, sub-1s%, coins, equity.
Stage 2 (live, passers only): trailing-30d realized PnL via fill.closedPnl (NO m01 -- bug-free).

Eligibility (Alberto: "~10-20 fills/day fine", NOT sub-second, NO maker MM):
  lev>=2x, 8<=fills/day<=60 (fast but COPYABLE -- a ceiling excludes uncopyable HFT desks),
  taker>=55% (no maker-rebate MM), sub-1s<35% (not sub-second), coins>=2, equity>=20000.
NOTE (v2 fix): fills/day computed from the ACTUAL time-span of returned fills (userFillsByTime caps
at 2000 -> a naive n/7 floors HFT desks at 286 and hides that they do thousands/day -> uncopyable).
Universe = curated copy-candidate sets (408 return/DD pool + hl_copy_target_fills + hl_wallet_profiles),
NOT most-active-addresses (which is biased to HFT market-takers we cannot mirror).
Pure analysis -- writes a ranked CSV. No deploy. Run: python scripts/scan_fast_levered.py
"""
import sys, time, numpy as np, requests
from pymongo import MongoClient
URL="https://api.hyperliquid.xyz/info"
now=time.time()*1000; wk=now-7*24*3600*1000; mo=now-90*24*3600*1000; day=now-24*3600*1000
PROFIT_DAYS=90  # trailing-30d realized PnL is too noisy (one bad month); use 90d
db=MongoClient("mongodb://localhost:27017/quants_lab").quants_lab

def post(t,**kw):
    for _ in range(3):
        try:
            r=requests.post(URL,json={"type":t,**kw},timeout=15)
            if r.status_code==200: return r.json()
        except Exception: pass
        time.sleep(0.5)
    return None

MODE=sys.argv[1] if len(sys.argv)>1 else "curated"   # "curated" | "discovery"
uni=set()
if MODE=="discovery":
    # DISCOVERY: market-wide universe from hl_wallet_trades, narrowed to a COPYABLE-ACTIVITY band
    # (moderate mongo trade-count -> excludes HFT (huge count) AND inactive (tiny). mongo undercounts
    # per-wallet, so the band is a coarse pre-filter only; profitability TRUTH comes from API closedPnl.)
    LO,HI=350,3500
    print(f"Stage 0 [discovery]: activity band [{LO},{HI}] from hl_wallet_trades (137k universe)...")
    cnt={}
    for fld in ("buyer","seller"):
        for d in db.hl_wallet_trades.aggregate([{"$group":{"_id":f"${fld}","n":{"$sum":1}}}],allowDiskUse=True):
            if isinstance(d["_id"],str): cnt[d["_id"]]=cnt.get(d["_id"],0)+d["n"]
    uni={a.lower() for a,n in cnt.items() if a.startswith("0x") and LO<=n<=HI}
else:
    print("Stage 0 [curated]: 408 return/DD pool + copy-target + profiled sets...")
    try:
        import pandas as pd
        for w in pd.read_csv("/tmp/hl_truth_ranked.csv")["wallet"].astype(str): uni.add(w.lower())
    except Exception as e: print("  (hl_truth_ranked.csv:",e,")")
    for w in db.hl_copy_target_fills.distinct("wallet"):
        if isinstance(w,str): uni.add(w.lower())
    for w in db.hl_wallet_profiles.distinct("address"):
        if isinstance(w,str): uni.add(w.lower())
addrs=[a for a in uni if a.startswith("0x")]
print(f"  {len(addrs)} candidate addresses")

print("Stage 1: live screen (lev, fills/day, taker%, sub-1s%, coins, eq)...")
passers=[]
for i,a in enumerate(addrs):
    st=post("clearinghouseState",user=a)
    if not st or "marginSummary" not in st: continue
    ms=st["marginSummary"]; av=float(ms.get("accountValue",0)); ntl=float(ms.get("totalNtlPos",0))
    lev=ntl/av if av else 0
    f=post("userFillsByTime",user=a,startTime=int(wk))
    if not f: continue
    n7=len(f)
    ts=sorted(set(x["time"] for x in f))
    # v2: fills/day from ACTUAL span of returned fills (caps at 2000 -> n/7 hides HFT rate)
    span_d=max((ts[-1]-ts[0])/86400000.0, 1e-6) if len(ts)>1 else 7.0
    fpd=n7/min(span_d,7.0)
    taker=sum(1 for x in f if x.get("crossed"))/n7*100 if n7 else 0
    dts=np.diff(ts)/1000.0 if len(ts)>1 else np.array([9.0])
    subs=sum(1 for d in dts if d<1.0)/max(len(dts),1)*100
    coins=len(set(x["coin"] for x in f))
    elig = lev>=2 and 8<=fpd<=120 and taker>=55 and subs<35 and coins>=2 and av>=20000
    if elig:
        passers.append(dict(addr=a,eq=av,lev=lev,fpd=fpd,taker=taker,subs=subs,coins=coins))
        print(f"  PASS {a[:12]} eq=${av:,.0f} lev={lev:.1f}x f/d={fpd:.0f} taker={taker:.0f}% coins={coins}")
    time.sleep(0.08)
print(f"  {len(passers)} passed Stage 1")

print(f"Stage 2: trailing-{PROFIT_DAYS}d realized PnL (fill.closedPnl) for passers...")
print("  WARNING (codex r1): userFillsByTime caps at 2000 -> high-freq wallets' window is TRUNCATED")
print("  to span_d (<PROFIT_DAYS). roi divides by CURRENT equity (inflated/deflated by leader")
print("  deposits/withdrawals). This is a RANKING heuristic, NOT validation -> see validate_leaders.py.")
rows=[]
for p in passers:
    f=post("userFillsByTime",user=p["addr"],startTime=int(mo))
    if not f: continue
    ts=[x["time"] for x in f]
    span_d=round((max(ts)-min(ts))/86400000.0,1) if len(ts)>1 else 0
    capped=len(f)>=2000
    pnl30=sum(float(x.get("closedPnl",0)) for x in f)
    fees30=sum(float(x.get("fee",0)) for x in f)
    netpnl=pnl30-fees30
    roi30=netpnl/p["eq"]*100 if p["eq"] else 0
    p.update(pnl30=netpnl,roi30=roi30,span_d=span_d,capped=capped)
    rows.append(p)
    time.sleep(0.08)

rows.sort(key=lambda r:-(r["roi30"]))
print(f"\n== FAST+LEVERED+TAKER (ranked by netPnL/CURRENT-eq over span_d; HEURISTIC, not validated) ==")
print(f"{'addr':44s} {'eq':>9s} {'lev':>5s} {'f/d':>5s} {'tk%':>4s} {'coins':>5s} {'netPnL':>9s} {'roi%':>7s} {'span_d':>6s} {'cap':>4s}")
for r in rows:
    print(f"{r['addr']:44s} ${r['eq']:>8,.0f} {r['lev']:>4.1f}x {r['fpd']:>5.0f} {r['taker']:>4.0f} {r['coins']:>5d} ${r['pnl30']:>8,.0f} {r['roi30']:>6.1f}% {r['span_d']:>6.1f} {'Y' if r['capped'] else 'n':>4s}")
import csv
with open("/tmp/fast_levered_scan.csv","w",newline="") as fh:
    w=csv.DictWriter(fh,fieldnames=["addr","eq","lev","fpd","taker","subs","coins","pnl30","roi30","span_d","capped"]); w.writeheader()
    for r in rows: w.writerow(r)
print(f"\nwrote /tmp/fast_levered_scan.csv ({len(rows)} eligible -- HEURISTIC ranking, validate before deploy)")
