#!/usr/bin/env python3
"""Multi-window copyable-wallet HUNT -- LEADER-ACTUAL-EXIT (Alberto 2026-07-01: no imposed hold bias).

Copies the leader's REAL round-trip: enter at mark at (leader_entry+latency), exit at mark at the
wallet's OWN matching close (FIFO per coin)+latency. NO fixed hold. Splits Dec1..Jun11 into 5 windows,
scores each wallet's copy-edge per window (gross + net of taker RT), finds wallets positive across
windows, and judges picks (chosen on early windows) on the untouched latest window. Lists survivors by
address.

Run: python research/v15/copy_multiwindow_hunter.py
"""
from __future__ import annotations
import sys
from collections import deque
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import os
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, set_latency_ms, slip_oneway, set_slip_default_bps
# CODEX-REQUESTED slip-default sensitivity: illiquid tail coins are underpriced by the 4.7bps default.
set_slip_default_bps(float(os.environ.get("SLIP_DEFAULT_BPS", "4.7")))

LAT_MS = 2000
FEE_TAKER = fee_rt(maker=False)       # 8.64bps RT -- what we actually pay entering/exiting market
def rt_cost(coin):                    # full realistic RT: taker fee + 2x one-way slippage (entry+exit)
    return FEE_TAKER + 2.0 * slip_oneway(coin)
MINF = 6                              # min matched round-trips per window to trust a wallet's edge
MAX_HOLD_MS = 14 * 24 * 3600 * 1000   # ignore lots the leader never closes within 14d (force-drop)
MIN_HOLD_MS = 15 * 60 * 1000          # drop sub-15min round-trips (1-min mark can't price them; we can't mirror at 2s lag)

def ms(d): return int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)
BOUNDS = [ms("2025-12-01"), ms("2026-01-08"), ms("2026-02-15"), ms("2026-03-25"), ms("2026-05-02"), ms("2026-06-11")]
WINDOWS = list(zip(BOUNDS[:-1], BOUNDS[1:]))

def build_roundtrips(we, lo_ms, hi_ms):
    """Global FIFO round-trips keyed by (coin, side). Each = leader open -> matching close (its own
    first close via FIFO). Returns list of (open_ts, net_bps). Fixed-$ copier: equal weight per open.
    Carries inventory across window boundaries (no per-window slicing) -> no boundary mis-pair."""
    ev = we.slice_dicts(lo_ms, hi_ms)
    lots = {}      # (coin, is_long) -> deque of (entry_ts, entry_mk)
    trips = []
    for f in ev:
        coin = f["coin"]; is_long = f["is_long"]
        if f["is_open"]:
            pt = f["ts"] + LAT_MS
            mk = S.mark_at(coin, pt)
            if mk and mk > 0:
                lots.setdefault((coin, is_long), deque()).append((pt, mk))
        else:
            # a close on the position side: closing a long is is_long=False in m02, so the lot side is NOT is_long
            side = not is_long
            dq = lots.get((coin, side))
            if dq:
                ent_ts, ent_mk = dq.popleft()
                xt = f["ts"] + LAT_MS
                hold = xt - ent_ts
                if hold > MAX_HOLD_MS or hold < MIN_HOLD_MS:   # drop never-closed AND un-copyable fast scalps
                    continue
                ex = S.mark_at(coin, xt)
                if ex and ex > 0:
                    g = (ex - ent_mk) / ent_mk if side else (ent_mk - ex) / ent_mk
                    trips.append((ent_ts, (g - rt_cost(coin)) * 1e4))   # net bps, per-coin cost
    return trips

def window_stats(trips, s, e):
    """Stats over round-trips whose OPEN falls in [s,e). mean / trimmed(drop top+bottom 1%) / median."""
    v = np.array([nb for (ts, nb) in trips if s <= ts < e])
    if len(v) < 1:
        return None
    n = len(v)
    if n >= 20:
        lo, hi = np.percentile(v, 1), np.percentile(v, 99)
        trimmed = float(v[(v >= lo) & (v <= hi)].mean())
    else:
        trimmed = float(np.sort(v)[:-max(1, n // 100) or None].mean()) if n > 2 else float(v.mean())
    return {"n": n, "net_bps": float(v.mean()), "trim_bps": trimmed,
            "med_bps": float(np.median(v)), "win": float((v > 0).mean() * 100)}

def main():
    set_latency_ms(LAT_MS)
    uni = [l.strip().lower() for l in open("app/data/v15/m01_universe_20k_wallets.txt") if l.strip() and not l.startswith("#")]
    print(f"loading {len(uni)} wallets, {len(WINDOWS)} windows, LEADER-ACTUAL-EXIT, net taker {FEE_TAKER*1e4:.2f}bps ...")
    wf = S.load_events_from_m02(set(uni), BOUNDS[0] - MAX_HOLD_MS, BOUNDS[-1])
    rows = []
    for w, we in wf.items():
        trips = build_roundtrips(we, BOUNDS[0] - MAX_HOLD_MS, BOUNDS[-1])
        rec = {"wallet": w}
        for i, (s, e) in enumerate(WINDOWS):
            r = window_stats(trips, s, e)
            if not r or r["n"] < MINF:
                rec[f"nb{i}"] = np.nan; rec[f"tb{i}"] = np.nan; rec[f"md{i}"] = np.nan; rec[f"n{i}"] = (r["n"] if r else 0)
            else:
                rec[f"nb{i}"] = r["net_bps"]; rec[f"tb{i}"] = r["trim_bps"]; rec[f"md{i}"] = r["med_bps"]; rec[f"n{i}"] = r["n"]
        rows.append(rec)
    df = pd.DataFrame(rows)
    df.to_parquet("app/data/v15/copy_multiwindow_slip50.parquet")
    nbcols = [f"nb{i}" for i in range(len(WINDOWS))]; tbcols = [f"tb{i}" for i in range(len(WINDOWS))]
    ncols = [f"n{i}" for i in range(len(WINDOWS))]
    full = df.dropna(subset=nbcols)
    print(f"\nwallets with >={MINF} round-trips ALL 5 windows: {len(full)} (FIXED: coin+side FIFO, carried inventory)")
    if not len(full):
        print("none qualify"); return
    # WALK-FORWARD, frozen rule (net+ all sel windows, n>=60, avg>=10bps), test next window -- on MEAN and TRIMMED
    print("\nWALK-FORWARD frozen rule (net+ all sel, n>=60, avg>=10bps), OOS on next window:")
    for metric, cols in [("MEAN", nbcols), ("TRIMMED(drop top+bot 1%)", tbcols)]:
        print(f" [{metric}]")
        for si, ti in [([0,1],2),([0,1,2],3),([0,1,2,3],4)]:
            sc=[cols[i] for i in si]; sn=[ncols[i] for i in si]
            d=df.dropna(subset=sc+[cols[ti]])
            pk=d[(d[sc]>0).all(axis=1)&(d[sn].min(axis=1)>=60)&(d[sc].mean(axis=1)>=10)]
            if len(pk): print(f"   sel w{si}->test w{ti}: picked={len(pk):<3} OOS {metric[:4]} {pk[cols[ti]].mean():+6.1f}bps %pos {100*(pk[cols[ti]]>0).mean():.0f}%")
    # winner-concentration falsification: does the DEPLOYABLE set survive on TRIMMED + MEDIAN?
    dep = full[(full[nbcols]>0).all(axis=1)&(full[ncols].min(axis=1)>=60)]
    print(f"\nDEPLOYABLE (net+ all 5, n>=60): {len(dep)} wallets -- MEAN vs TRIMMED vs MEDIAN per wallet:")
    for _,r in dep.sort_values('n0',ascending=False).iterrows():
        surv = "SURVIVES" if all(r[f"tb{i}"]>0 for i in range(5)) and all(r[f"md{i}"]>0 for i in range(5)) else "FAILS-trim/med"
        print(f"  {r.wallet} mean={[round(r[f'nb{i}']) for i in range(5)]} trim={[round(r[f'tb{i}']) for i in range(5)]} med={[round(r[f'md{i}']) for i in range(5)]} -> {surv}")
    print("\nsaved app/data/v15/copy_multiwindow_slip50.parquet")

if __name__ == "__main__":
    main()
