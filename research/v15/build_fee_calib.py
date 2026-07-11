import glob, json
import pyarrow.parquet as pq
import numpy as np
from collections import defaultdict

S3="app/data/hl_s3_fills_v2_by_wallet"
files=sorted(glob.glob(f"{S3}/*.parquet"))[:800]
# per-market taker all-in one-way fee fraction samples
mk=defaultdict(list)
main_samp=[]
nf=0
for fp in files:
    try: t=pq.read_table(fp).to_pydict()
    except: continue
    k=t.keys()
    if not all(x in k for x in ("coin","price","size","fee","crossed")): continue
    bf=t.get("builderFee",[0]*len(t["coin"])); df=t.get("deployerFee",[0]*len(t["coin"]))
    for i in range(len(t["coin"])):
        if t["crossed"][i] is not True: continue  # taker only
        try:
            c=str(t["coin"][i]); px=float(t["price"][i]); sz=abs(float(t["size"][i]))
            fee=float(t["fee"][i]); bfi=float(bf[i] or 0); dfi=float(df[i] or 0)
        except: continue
        if px<=0 or sz<=0: continue
        ow=(fee+bfi+dfi)/(sz*px)  # one-way fraction all-in
        if not np.isfinite(ow) or ow<0: continue
        if ":" in c: mk[c].append(ow)
        else: main_samp.append(ow)
    nf+=1

per_market={}
MIN_N=50
for c,v in mk.items():
    if len(v)>=MIN_N:
        per_market[c]=round(float(np.median(v)),8)
# summary
main_med=float(np.median(main_samp)) if main_samp else float('nan')
xyz=[c for c in per_market if c.startswith("xyz:")]
oth=[c for c in per_market if not c.startswith("xyz:")]
print(f"files={nf} main_taker_ow_median={main_med*1e4:.3f}bps (RT {2*main_med*1e4:.2f})")
print(f"builder markets with >= {MIN_N} taker fills: {len(per_market)} ({len(xyz)} xyz, {len(oth)} other)")
allv=np.array([per_market[c] for c in per_market])
if len(allv):
    print(f"per_market one-way bps: median={np.median(allv)*1e4:.3f} p10={np.percentile(allv,10)*1e4:.3f} p90={np.percentile(allv,90)*1e4:.3f} max={allv.max()*1e4:.3f}")
# recommended fallback for UNKNOWN builder markets: conservative = p75 of known builder (not median-optimistic, not 2x-absurd)
if len(allv):
    fb_ow=float(np.percentile(allv,75))
    print(f"recommended hip3 fallback (p75 known-builder one-way): {fb_ow*1e4:.3f}bps ow -> RT {2*fb_ow*1e4:.2f}bps (vs current 2x=18bps)")
print("sample per_market (first 12):")
for c in sorted(per_market)[:12]:
    print(f"  {c}: {per_market[c]:.8f} ({per_market[c]*1e4:.2f}bps ow)")
json.dump({"per_market":per_market,"main_taker_ow":round(main_med,8)}, open("/tmp/fee_calib_proposed.json","w"), indent=2)
print(f"\nwrote /tmp/fee_calib_proposed.json ({len(per_market)} markets)")
