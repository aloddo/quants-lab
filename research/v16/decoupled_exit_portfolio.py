"""
Decoupled-exit copy portfolio backtest (codex H2; Alberto "keep squeezing"). Prereg:
copy-rebuild/2026-06-28-decoupled-exit-prereg.

Copy ONLY the markout-selected slow traders' ENTRY; exit on OUR clock (+4h time / trailing-TP / -8% stop),
NEVER mirror the leader's late exit. Per-coin netting + concurrency cap. Canonical per-coin costs via
execution_model. Portfolio walk-forward, frozen OOS holdout (folds>=16).
"""
import numpy as np, pandas as pd, urllib.parse as ul
from pathlib import Path
import sys; sys.path.insert(0, "research/v15")
import execution_model as EX

WS = Path("app/data/v15/weekly_spike")
MARK_DIRS = [Path("app/data/v15/assetctx_marks"), Path("app/data/v15/builder_marks")]
MS_MIN = 60_000; LAT_MS = 1860
H_MAX_MS = 240 * MS_MIN          # +4h time stop
TP_ARM = 0.005; TP_TRAIL = 0.003 # trailing TP: arm at +0.5% peak, exit on -0.3% pullback
HARD_STOP = -0.08
EX.set_latency_ms(LAT_MS)

_mk = {}
def marks(coin):
    if coin in _mk: return _mk[coin]
    for d in MARK_DIRS:
        for nm in (f"{coin}.npy", f"{ul.quote(coin, safe='')}.npy"):
            p = d / nm
            if p.exists():
                a = np.load(p, mmap_mode="r")
                _mk[coin] = (np.asarray(a[0], "int64"), np.asarray(a[1], "float64")); return _mk[coin]
    _mk[coin] = None; return None

def px_path(coin, t0, t1):
    m = marks(coin)
    if m is None: return None
    mins, px = m
    i0 = np.searchsorted(mins, t0, "right") - 1
    i1 = np.searchsorted(mins, t1, "right")
    if i0 < 0 or i1 <= i0: return None
    return mins[i0:i1], px[i0:i1]

def decoupled_exit(coin, entry_ts, side):
    """return (entry_mark, exit_mark) using OUR exit rule on the 1m path; None if uncovered."""
    pp = px_path(coin, entry_ts + LAT_MS, entry_ts + LAT_MS + H_MAX_MS)
    if pp is None: return None
    _, px = pp
    e = px[0]
    if not np.isfinite(e) or e <= 0: return None
    sgn = 1.0 if side == "long" else -1.0
    r = (px - e) / e * sgn
    peak = -1e9
    for k in range(1, len(r)):
        peak = max(peak, r[k])
        if r[k] <= HARD_STOP: return e, px[k]                       # hard stop
        if peak >= TP_ARM and r[k] <= peak - TP_TRAIL: return e, px[k]  # trailing TP
    return e, px[-1]                                                 # +4h time stop

def net_return(coin, entry_mark, exit_mark, side):
    is_long = side == "long"
    ef = EX.apply_entry(coin, entry_mark, is_long)
    xf = EX.apply_exit(coin, exit_mark, is_long)
    return EX.gross_return(ef, xf, is_long) - EX.fee_rt(maker=False)

def run_portfolio(entries, max_conc, cooldown_ms=15*MS_MIN, mirror=False):
    """entries: DataFrame[entry_ts, coin, side] sorted by entry_ts. Returns fold net ROE + maxDD.
    mirror=False -> our decoupled exit. Equal sizing = 1/max_conc per slot."""
    open_pos = []   # (exit_ts, )
    coin_open_until = {}
    contribs = []   # (entry_ts, exit_ts, weight*net_r)
    w = 1.0 / max_conc
    for r in entries.itertuples(index=False):
        t = r.entry_ts
        open_pos = [x for x in open_pos if x > t]                 # free finished slots
        if len(open_pos) >= max_conc: continue                   # concurrency full
        if coin_open_until.get(r.coin, 0) > t: continue          # per-coin netting + cooldown
        de = decoupled_exit(r.coin, t, r.side)
        if de is None: continue
        em, xm = de
        nr = net_return(r.coin, em, xm, r.side)
        # exit ts approx: we don't track exact; use +4h cap for slot/cooldown accounting
        ex_ts = t + LAT_MS + H_MAX_MS
        open_pos.append(ex_ts)
        coin_open_until[r.coin] = ex_ts + cooldown_ms
        contribs.append((t, ex_ts, w * nr))
    if not contribs: return 0.0, 0.0, 0
    df = pd.DataFrame(contribs, columns=["t","x","c"]).sort_values("t")
    eq = 1.0 + df.c.cumsum()                                     # equity path (closed-trade approx)
    roe = float(df.c.sum()); dd = float((eq.cummax() - eq).max())
    return roe, dd, len(df)

def main():
    sl = pd.read_parquet(WS/"markout_cohort_shortlist.parquet")
    folds = pd.read_parquet(WS/"m03_folds.parquet")[["fold_id","test_start","test_end_excl"]]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet",
                        columns=["wallet","coin","side","entry_ts","max_position_notional"])
    j = j[j.max_position_notional > 200]
    rng = np.random.default_rng(5)
    all_wallets = j.wallet.unique()
    print(f"{'fold':>4} {'conc':>4} {'COHORT_roe':>11} {'COHORT_dd':>10} {'n':>5} {'RANDOM_roe':>11}")
    rows = []
    for conc in [5, 8, 12]:
        for f in folds.itertuples(index=False):
            t0 = pd.Timestamp(f.test_start).value//10**6; t1 = pd.Timestamp(f.test_end_excl).value//10**6
            cw = set(sl[sl.fold_id==f.fold_id].primary_wallet)
            ent = j[(j.entry_ts>=t0)&(j.entry_ts<t1)]
            coh = ent[ent.wallet.isin(cw)][["entry_ts","coin","side"]].sort_values("entry_ts")
            roe, dd, n = run_portfolio(coh, conc)
            # random baseline: same # wallets, random
            rw = set(rng.choice(all_wallets, size=len(cw), replace=False))
            rnd = ent[ent.wallet.isin(rw)][["entry_ts","coin","side"]].sort_values("entry_ts")
            r_roe, _, _ = run_portfolio(rnd, conc)
            rows.append(dict(fold=f.fold_id, conc=conc, roe=roe, dd=dd, n=n, rnd=r_roe))
            print(f"{f.fold_id:>4} {conc:>4} {roe*100:>+10.2f}% {dd*100:>9.2f}% {n:>5} {r_roe*100:>+10.2f}%")
    R = pd.DataFrame(rows)
    print("\n=== PRE-REGISTERED BAR (OOS folds>=16) ===")
    for conc in [5,8,12]:
        o = R[(R.conc==conc)&(R.fold>=16)]
        a = R[R.conc==conc]
        print(f"  conc={conc}: OOS median {o.roe.median()*100:+.2f}% mean {o.roe.mean()*100:+.2f}% | "
              f"OOS rand median {o.rnd.median()*100:+.2f}% | medDD {a.dd.median()*100:.2f}% | "
              f"OOS pos {(o.roe>0).mean():.2f} | ALL median {a.roe.median()*100:+.2f}%")
    print(f"\n  calibrated slip share: {EX.calibrated_share()}")

if __name__ == "__main__":
    main()
