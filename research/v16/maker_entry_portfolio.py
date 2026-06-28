"""
Maker-entry copy portfolio (the last copy lever; Alberto "keep squeezing"). Copy the markout-selected slow
cohort but ENTER AS A MAKER (post a passive limit at the leader's entry price) instead of crossing as a taker.
Earn the spread + maker fee instead of paying the taker cost wall (the structural reason copy fails).

Fill model (close-only marks -> approximate): post limit at leader price P at entry_ts; FILL if a 1m close
crosses P in the maker-favorable direction within FILL_WINDOW (long: close<=P; short: close>=P); else MISS
(captures adverse selection -- we miss the trades that immediately ran the leader's way). On fill, hold and
exit on OUR clock (+4h / trailing-TP / -8% stop from fill). Cost = maker entry fee + taker exit fee + exit slip.
Compares maker vs the taker decoupled-exit baseline. Prereg uses the same bar family as decoupled-exit-prereg.
"""
import numpy as np, pandas as pd
from pathlib import Path
import sys; sys.path.insert(0, "research/v15"); sys.path.insert(0, "research/v16")
import execution_model as EX
import decoupled_exit_portfolio as D

FILL_WINDOW_MS = 30 * D.MS_MIN
MAKER_FEE_OW = EX.fee_rt(maker=True) / 2.0     # maker entry
TAKER_FEE_OW = EX.fee_rt(maker=False) / 2.0    # taker exit

def maker_trade(coin, entry_ts, side):
    """Returns net return if our passive limit fills, else None (missed). Our-clock exit from fill time."""
    m = D.marks(coin)
    if m is None: return None
    mins, px = m
    t_post = entry_ts            # post at leader's entry ts (no latency edge needed as maker)
    i0 = np.searchsorted(mins, t_post, "right") - 1
    if i0 < 0: return None
    P = px[i0]
    if not np.isfinite(P) or P <= 0: return None
    # fill window
    iw = np.searchsorted(mins, t_post + FILL_WINDOW_MS, "right")
    is_long = side == "long"
    fill_i = None
    for k in range(i0+1, min(iw, len(px))):
        if (is_long and px[k] <= P) or ((not is_long) and px[k] >= P):
            fill_i = k; break
    if fill_i is None: return None    # MISSED (adverse selection: price ran the leader's way)
    t_fill = int(mins[fill_i])
    # our-clock exit from fill
    de = D.decoupled_exit(coin, t_fill - D.LAT_MS, side)  # decoupled_exit adds LAT_MS; we already filled, offset
    if de is None: return None
    _, exit_mark = de
    gross = (exit_mark - P) / P * (1.0 if is_long else -1.0)
    return gross - MAKER_FEE_OW - TAKER_FEE_OW - EX.slip_oneway(coin)   # entry maker (no slip), exit taker+slip

def run_maker_fold(entries, max_conc, cooldown_ms=15*D.MS_MIN):
    open_pos=[]; coin_open={}; contribs=[]; w=1.0/max_conc; n_signal=0; n_fill=0
    for r in entries.itertuples(index=False):
        t=r.entry_ts; n_signal+=1
        open_pos=[x for x in open_pos if x>t]
        if len(open_pos)>=max_conc: continue
        if coin_open.get(r.coin,0)>t: continue
        nr = maker_trade(r.coin, t, r.side)
        if nr is None: continue
        n_fill+=1
        ex=t+FILL_WINDOW_MS+D.H_MAX_MS
        open_pos.append(ex); coin_open[r.coin]=ex+cooldown_ms
        contribs.append(w*nr)
    roe=float(np.sum(contribs)) if contribs else 0.0
    return roe, n_fill, n_signal

def main():
    sl=pd.read_parquet(D.WS/"markout_cohort_shortlist.parquet")
    folds=pd.read_parquet(D.WS/"m03_folds.parquet")[["fold_id","test_start","test_end_excl"]]
    j=pd.read_parquet("app/data/v15/m02_journeys.parquet",columns=["wallet","coin","side","entry_ts","max_position_notional"])
    j=j[j.max_position_notional>200]
    print(f"{'fold':>4} {'conc':>4} {'MAKER_roe':>10} {'fill%':>6} {'nfill':>6}")
    rows=[]
    for conc in [5,8]:
        for f in folds.itertuples(index=False):
            t0=pd.Timestamp(f.test_start).value//10**6; t1=pd.Timestamp(f.test_end_excl).value//10**6
            cw=set(sl[sl.fold_id==f.fold_id].primary_wallet)
            coh=j[(j.entry_ts>=t0)&(j.entry_ts<t1)&(j.wallet.isin(cw))][["entry_ts","coin","side"]].sort_values("entry_ts")
            roe,nf,ns=run_maker_fold(coh,conc)
            fr=nf/ns if ns else 0
            rows.append(dict(fold=f.fold_id,conc=conc,roe=roe,fillrate=fr,nfill=nf))
            print(f"{f.fold_id:>4} {conc:>4} {roe*100:>+9.2f}% {fr*100:>5.1f}% {nf:>6}")
    R=pd.DataFrame(rows)
    print("\n=== MAKER-ENTRY vs pre-registered bar (OOS folds>=16) ===")
    for conc in [5,8]:
        o=R[(R.conc==conc)&(R.fold>=16)]; a=R[R.conc==conc]
        print(f"  conc={conc}: OOS median {o.roe.median()*100:+.2f}% mean {o.roe.mean()*100:+.2f}% | "
              f"OOS pos {(o.roe>0).mean():.2f} | ALL median {a.roe.median()*100:+.2f}% | mean fill {a.fillrate.mean()*100:.0f}%")

if __name__=="__main__":
    main()
