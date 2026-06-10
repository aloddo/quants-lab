#!/usr/bin/env python3
"""Hardened robustness v2 -- addresses codex round-1 findings #3 (weak null) and #4 (unmatched control).

Adds two STRONGER nulls beyond the uniform-time shuffle:
  - SAME-DAY shuffle: random entry bar on the SAME coin + SAME UTC calendar day (matches vol/trend/
    regime/time-of-day state -- isolates intraday timing).
  - POOLED-TIMESTAMP shuffle: entry times drawn from the pooled set of ALL real entries (informed+control)
    on the SAME coin+side (compares "this informed wallet's timing" vs "some active trader's timing on the
    same coin in the same window" -- the opportunity-set null codex asked for).
  - FREQ-MATCHED control: control wallets nearest-matched to the informed set on events_per_day + active_days
    (not just net_edge<=0 by notional).
If REAL still beats the SAME-DAY and POOLED nulls, the edge is genuine copyable entry timing.
Seeded, deterministic.
"""
from __future__ import annotations
import time
import threading
import concurrent.futures as cf
import numpy as np
import pandas as pd
import scipy.stats as ss

from research.v15.entry_copy_overlay_sim import (
    _candles, _load_feat, _base_active, copyable_universe, overlay_trade,
    wallet_forward_entries, select_informed, Risk, FEE_RT, SLIP, FWD_START)

CONFIGS = [
    ("stop5 arm1 tr0.6 h6",  Risk(stop=0.05, arm=0.01, trail=0.006, max_hold_h=6)),
    ("stop3 arm1 tr0.6 h24", Risk(stop=0.03, arm=0.01, trail=0.006, max_hold_h=24)),
]
N_SHUFFLE = 5
DAY_MS = 86400_000


def freq_matched_control(feat_path, informed, max_freq, top_k):
    """Control = active major-traders with net_edge<=0, nearest-matched to informed on (events_per_day,
    active_days). Avoids the 'sorted by notional' mismatch."""
    f = _load_feat(feat_path)
    base = _base_active(f, max_freq)
    pool = base[base["net_edge_bps"] <= 0].copy()
    if pool.empty:
        return pool.head(0)
    inf_med = informed[["events_per_day", "active_days"]].median()
    # z-distance to informed median on the two activity axes
    for c in ("events_per_day", "active_days"):
        sd = pool[c].std(ddof=0) or 1.0
        pool[c + "_z"] = (pool[c] - inf_med[c]) / sd
    pool["dist"] = np.hypot(pool["events_per_day_z"], pool["active_days_z"])
    return pool.sort_values("dist").head(top_k)


def main():
    feat = "app/data/wallet_alpha/wallet_features.parquet"
    informed, _ctl_old = select_informed(feat, max_freq=10, min_tstat=1.0, min_winrate=0.50, top_k=80)
    control = freq_matched_control(feat, informed, max_freq=10, top_k=80)
    universe = copyable_universe()
    since = int(pd.Timestamp(FWD_START, tz="UTC").timestamp() * 1000)
    now = int(time.time() * 1000)
    cache, clock = {}, threading.Lock()
    rng = np.random.default_rng(42)

    def cc(coin):
        with clock:
            if coin in cache:
                return cache[coin]
        df = _candles(coin, since, now)
        with clock:
            cache[coin] = df
        return df

    def real_trades(wallets):
        def one(w):
            out = []
            for e in wallet_forward_entries(w, since, now, universe):
                cdf = cc(e["coin"])
                if cdf.empty:
                    continue
                fwd = cdf[cdf["t"] >= e["ts"] + 1000]
                if fwd.empty or len(fwd) < 2:
                    continue
                epx = fwd["c"].iloc[0]
                epx = epx * (1 + SLIP) if e["side"] == "Buy" else epx * (1 - SLIP)
                out.append({"side": e["side"], "epx": epx, "path": fwd.iloc[1:].reset_index(drop=True),
                            "coin": e["coin"], "ts": e["ts"]})
            return out
        with cf.ThreadPoolExecutor(max_workers=4) as ex:
            return [t for sub in ex.map(one, wallets) for t in sub]

    print("collecting informed + control real entries...")
    inf = real_trades(informed["wallet"].tolist())
    ctl = real_trades(control["wallet"].tolist())
    print(f"informed={len(inf)} control={len(ctl)} (freq-matched)\n")

    # pooled entry-timestamp set per (coin, side) from ALL real entries (informed+control)
    pool_ts: dict = {}
    for t in inf + ctl:
        pool_ts.setdefault((t["coin"], t["side"]), []).append(t["ts"])

    def entry_from_bar(coin, side, bar_idx, cdf):
        epx = cdf["c"].iloc[bar_idx]
        epx = epx * (1 + SLIP) if side == "Buy" else epx * (1 - SLIP)
        return side, epx, cdf.iloc[bar_idx + 1:].reset_index(drop=True)

    # build the three shuffled sets (lists of (side, epx, path))
    uni, sameday, pooled = [], [], []
    for t in inf:
        coin, side, ts = t["coin"], t["side"], t["ts"]
        cdf = cache.get(coin)
        if cdf is None or len(cdf) < 4:
            continue
        n = len(cdf)
        day0 = (ts // DAY_MS) * DAY_MS
        same_idx = cdf.index[(cdf["t"] >= day0) & (cdf["t"] < day0 + DAY_MS)].tolist()
        same_idx = [i for i in same_idx if i < n - 2]
        ts_pool = [x for x in pool_ts.get((coin, side), []) if x != ts]
        for _ in range(N_SHUFFLE):
            i = int(rng.integers(0, n - 2))
            uni.append(entry_from_bar(coin, side, i, cdf))
            if same_idx:
                j = int(rng.choice(same_idx))
                sameday.append(entry_from_bar(coin, side, j, cdf))
            if ts_pool:
                pt = int(rng.choice(ts_pool)) + 1000
                fwd = cdf[cdf["t"] >= pt]
                if len(fwd) >= 2:
                    k = cdf.index[cdf["t"] >= pt][0]
                    if k < n - 1:
                        pooled.append(entry_from_bar(coin, side, k, cdf))
    print(f"nulls: uniform={len(uni)} same-day={len(sameday)} pooled-ts={len(pooled)}\n")

    ctl_tuples = [(t["side"], t["epx"], t["path"]) for t in ctl]

    def ev(trades, r):
        return np.array([overlay_trade(s, p, path, r)[0] - SLIP - FEE_RT for s, p, path in trades])

    def ev_real(trades, r):
        return np.array([overlay_trade(t["side"], t["epx"], t["path"], r)[0] - SLIP - FEE_RT for t in trades])

    def mw(a, b):
        return ss.mannwhitneyu(a, b, alternative="greater")[1] if len(a) >= 10 and len(b) >= 10 else float("nan")

    print("=== HARDENED NULLS: REAL vs uniform / SAME-DAY / POOLED-TS / freq-matched CONTROL ===\n")
    for name, r in CONFIGS:
        R = ev_real(inf, r)
        U, SD, PL, P = ev(uni, r), ev(sameday, r), ev(pooled, r), ev(ctl_tuples, r)
        print(f"[{name}]")
        print(f"  REAL        {R.mean()*1e4:7.1f} bps  win {100*(R>0).mean():3.0f}%  n={len(R)}")
        print(f"  uniform-shuf{U.mean()*1e4:7.1f} bps  n={len(U)}   p(REAL>)={mw(R,U):.4f}")
        print(f"  SAME-DAY    {SD.mean()*1e4:7.1f} bps  n={len(SD)}   p(REAL>)={mw(R,SD):.4f}")
        print(f"  POOLED-TS   {PL.mean()*1e4:7.1f} bps  n={len(PL)}   p(REAL>)={mw(R,PL):.4f}")
        print(f"  CONTROL(fm) {P.mean()*1e4:7.1f} bps  win {100*(P>0).mean():3.0f}%  n={len(P)}   p(REAL>)={mw(R,P):.4f}\n")


if __name__ == "__main__":
    main()
