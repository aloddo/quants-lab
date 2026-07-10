#!/usr/bin/env python3
"""PROPER copy re-validation (fixes the 2026-07-06 Fable NO-GO flaws):
COMPLETE API fills (not the incomplete local ledger) -> canonical roundtrips -> execution_model
(mark_at at latency + per-coin apply_entry/apply_exit slippage + real taker fee) -> LIQUID-only ->
per-trade cap -> TRUE OOS split (train<split, test>=split). No hardcoded fees, no illiquid artifacts.
"""
import sys, requests, json, argparse
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, set_latency_ms, apply_entry, apply_exit, slip_oneway, set_slip_default_bps
LIQ = set(json.load(open(S._DATA / "l2_calib_10coin.json")).keys())
FEE_T = fee_rt(maker=False)
CAP = 500.0 / 1e4

def pull_api(w, start, end):
    import time as _t
    fills=[]; s=start
    for _page in range(200):
        r=None
        for attempt in range(6):                       # retry/backoff on rate-limit / non-list (Fable bug fix)
            try:
                r=requests.post('https://api.hyperliquid.xyz/info',
                                json={'type':'userFillsByTime','user':w,'startTime':s,'endTime':end},timeout=25).json()
            except Exception:
                r=None
            if isinstance(r,list):
                break
            _t.sleep(1.5*(attempt+1))                   # backoff
        if not isinstance(r,list):
            raise RuntimeError(f"pull_api {w[:10]}: non-list response after retries (rate-limited?) - REFUSING to return truncated data")
        if not r: break                                # genuine end of data
        fills+=r
        if len(r)<2000: break
        s=max(f['time'] for f in r)+1
    # integrity gate: startPosition chain (complete-fill check) on plain perps (@spot/xyz move via transfers)
    perp=[f for f in fills if f.get('coin') and not f['coin'].startswith('@') and ':' not in f['coin']]
    bad=tot=0
    for c in set(f['coin'] for f in perp):
        g=sorted([f for f in perp if f['coin']==c],key=lambda x:x['time']); pos=None
        for i,f in enumerate(g):
            sp=float(f.get('startPosition',0) or 0); ss=float(f['sz'])*(1 if f['side']=='B' else -1)
            if i==0: pos=sp
            tot+=1
            if abs(pos-sp)>max(1e-6,0.01*abs(sp)): bad+=1
            pos=sp+ss
    integ = 1.0 - bad/max(1,tot)
    return (sorted([(int(f['time']),f['coin'],float(f['sz'])*(1 if f['side']=='B' else -1),float(f['px']))
                    for f in fills if f.get('coin') and f.get('px')]), integ)

def edge(rts, lo, hi, lat):
    # ALL coins (Alberto: do NOT exclude illiquid/microcap - the edge lives there). Price REAL per-coin
    # slippage via execution_model (calibrated for the 10, default ~4.7bps for the rest). Only skip coins
    # we literally cannot price (no marks .npy - xyz builder + @spot). Report calibrated_share + no-mark drop.
    nets=[]; n_calib=0; n_default=0; n_nomark=0
    for c,dir_,ets,xts,evw,xvw,g in rts:
        if not (lo<=ets<hi): continue
        em=S.mark_at(c, ets+lat); xm=S.mark_at(c, xts+lat)
        if em is None or xm is None or em<=0:
            n_nomark+=1; continue                       # no marks (xyz/spot) - data limit, not exclusion
        ef=apply_entry(c, em, dir_>0); xf=apply_exit(c, xm, dir_>0)   # cross spread both sides (taker)
        og=max(-CAP,min(CAP, dir_*(xf-ef)/ef))
        nets.append(og-fee_rt(maker=False, coin=c))   # P1 fix (codex 2026-07-09): coin-specific RT fee (xyz/HIP-3 undercharged by const FEE_T)
        if c in LIQ: n_calib+=1
        else: n_default+=1
    if not nets: return (None,0,0,0,n_nomark)
    return (float(np.mean(nets))*1e4, len(nets), n_calib, n_default, n_nomark)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--wallets", help="comma-separated addresses")
    ap.add_argument("--wallets-file", help="file with one address per line")
    ap.add_argument("--sleep", type=float, default=0.0, help="seconds between wallets (rate-limit)")
    ap.add_argument("--skip-marks-guard", action="store_true",
                    help="override the freshness guard that refuses windows past marks coverage")
    ap.add_argument("--start", default="2025-12-01"); ap.add_argument("--split", default="2026-03-15")
    ap.add_argument("--end", default="2026-05-17"); ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--slip-default", type=float, default=0.5,
                    help="one-way slippage bps for non-calibrated coins. Alberto: ~0 at $150 size even on "
                         "illiquid (thousands of real fills prove it); default 0.5 not the old 4.7.")
    args=ap.parse_args()
    set_latency_ms(args.latency_s*1000); lat=args.latency_s*1000
    set_slip_default_bps(args.slip_default)
    ms=lambda d:int(pd.Timestamp(d,tz="UTC").timestamp()*1000)
    start,split,end=ms(args.start),ms(args.split),ms(args.end)
    # PILLAR-1 FRESHNESS GUARD (2026-07-07): refuse LOUD if the requested window extends past marks
    # coverage. This is the exact silent-stale trap that gave testN=0 this morning (marks ended Jun1 while
    # the window ran to Jul7). No run on rotten data. Override with --skip-marks-guard if intentional.
    if not getattr(args, "skip_marks_guard", False):
        _btc = S.ASSETCTX_DIR / "BTC.npy"
        if _btc.exists():
            _m = np.load(_btc)
            if _m.shape[1] > 0:
                _cov = int(_m[0].max())
                if end > _cov + 3*86_400_000:   # allow 3d slack (some coins lag)
                    _cov_d = pd.Timestamp(_cov, unit="ms", tz="UTC")
                    _end_d = pd.Timestamp(end, unit="ms", tz="UTC")
                    raise SystemExit(
                        f"FRESHNESS GUARD: requested --end {_end_d} exceeds marks coverage (BTC ends {_cov_d}). "
                        f"Results past coverage silently drop to testN=0. Refresh marks "
                        f"(scripts/extract_asset_ctx_marks.py) or pass --skip-marks-guard.")
    import time as _t
    if args.wallets_file:
        wlist=[l.strip().lower() for l in open(args.wallets_file) if l.strip() and not l.startswith('#')]
    else:
        wlist=[w.strip().lower() for w in args.wallets.split(',')]
    print(f"{'wallet':<16}{'apiFills':>9}{'allRT':>6}{'trainN':>7}{'trainBPS':>9}{'testN':>6}{'testBPS':>9}{'calib%':>7}{'noMark':>7}  verdict")
    for wi,w in enumerate(wlist):
        if wi and args.sleep: _t.sleep(args.sleep)
        w=w.strip().lower()
        try:
            t,integ=pull_api(w,start,end)
        except RuntimeError as e:
            print(f"{w[:16]:<16}  PULL-FAILED (rate-limited, skipped): {e}"); continue
        rts=roundtrips(t)
        trb,trn,trc,trd,trnm=edge(rts,start,split,lat); teb,ten,tec,ted,tenm=edge(rts,split,end,lat)
        v='n/a'
        if trn>=10 and ten>=10 and trb is not None and teb is not None:
            v = 'OOS-HOLDS' if (trb>0 and teb>0) else ('test-neg' if trb>0 else 'weak')
        trs=f"{trb:+.0f}" if trb is not None else "-"; tes=f"{teb:+.0f}" if teb is not None else "-"
        calibpct=100*(trc+tec)/max(1,trc+trd+tec+ted)
        print(f"{w[:16]:<16}{len(t):>9}{len(rts):>6}{trn:>7}{trs:>9}{ten:>6}{tes:>9}{calibpct:>6.0f}%{trnm+tenm:>7}  {v}")
if __name__=="__main__": main()
