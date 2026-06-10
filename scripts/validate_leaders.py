"""Rigorous profitability validation of discovery-scan candidates before shortlisting a copy-5.
Fixes the raw-scan ROI noise: buckets realized closedPnl by ACTUAL day (not the 2000-cap window),
reports clean 7d/30d realized, day-level consistency (win-day %), current OPEN uPnL (unrealized risk
the realized number hides), and worst single day. Pure analysis, no deploy."""
import requests, time, numpy as np
from collections import defaultdict
URL="https://api.hyperliquid.xyz/info"; now=time.time()*1000
def post(t,**kw):
    for _ in range(4):
        try:
            r=requests.post(URL,json={"type":t,**kw},timeout=30)
            if r.status_code==200: return r.json()
        except Exception: pass
        time.sleep(1)
    return None
# candidates: read ALL scan-eligible from /tmp/fast_levered_scan.csv (codex r2: hardcoding omitted
# 01c0255d -> the stated final pair was not fully covered). Reading the CSV guarantees every eligible
# wallet is validated. Falls back to a small hardcoded set if the CSV is absent.
import csv, os, sys
CSV=sys.argv[1] if len(sys.argv)>1 else "/tmp/fast_levered_scan.csv"
C={}
if os.path.exists(CSV):
    for row in csv.DictReader(open(CSV)):
        a=row["addr"].lower()
        C[a]=f"{a[:8]} {float(row.get('lev',0)):.1f}x {float(row.get('fpd',0)):.0f}/d {float(row.get('taker',0)):.0f}tk {row.get('coins','?')}c"
else:
    C={"0x456f1049c0f2ec990091bbd3f30af62aca3fcdf1":"456f10 (fallback)",
       "0x01c0255d63e287cb0006328de58aa2e60d147bee":"01c025 (fallback)"}
print(f"validating {len(C)} candidates from {CSV if os.path.exists(CSV) else 'fallback set'}")
print(f"{'wallet':12s} {'eq':>9s} {'lev':>5s} {'open_uPnL':>10s} {'r7d':>9s} {'r30d':>10s} {'winday%':>7s} {'worstday':>9s}  note")
rows=[]
for a,note in C.items():
    st=post("clearinghouseState",user=a)
    if not st: print(f"{a[:12]} CH-FAIL"); continue
    ms=st["marginSummary"]; eq=float(ms["accountValue"]); lev=float(ms["totalNtlPos"])/eq if eq else 0
    upnl=sum(float(p["position"].get("unrealizedPnl",0)) for p in st.get("assetPositions",[]))
    f=post("userFillsByTime",user=a,startTime=int(now-30*24*3600*1000))
    if not f: print(f"{a[:12]} FILLS-FAIL"); continue
    byday=defaultdict(float)
    for x in f:
        d=int(x["time"]//86400000); byday[d]+=float(x.get("closedPnl",0))-float(x.get("fee",0))
    days=sorted(byday)
    # clean windows by actual timestamp
    r7=sum(v for x in f for v in [float(x.get("closedPnl",0))-float(x.get("fee",0))] if x["time"]>=now-7*24*3600*1000)
    r30=sum(byday.values())
    wins=sum(1 for d in days if byday[d]>0); winpct=wins/len(days)*100 if days else 0
    worst=min(byday.values()) if byday else 0
    rows.append((a,eq,lev,upnl,r7,r30,winpct,worst,note,len(days)))
    print(f"{a[:12]} ${eq:>8,.0f} {lev:>4.1f}x ${upnl:>9,.0f} ${r7:>8,.0f} ${r30:>9,.0f} {winpct:>6.0f}% ${worst:>8,.0f}  {note}")
    time.sleep(0.1)
print("\n== robust shortlist (r30d>0, open_uPnL not deeply negative, winday%>=45, span>=15d) ==")
for a,eq,lev,upnl,r7,r30,winpct,worst,note,nd in sorted(rows,key=lambda r:-r[5]):
    ok = r30>0 and upnl>-0.3*eq and winpct>=45 and nd>=15 and r7>-0.15*eq
    print(f"  [{'KEEP' if ok else 'drop'}] {a} r30d=${r30:,.0f} uPnL=${upnl:,.0f} winday={winpct:.0f}% lev={lev:.1f}x eq=${eq:,.0f} days={nd}")
