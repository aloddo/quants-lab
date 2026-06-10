#!/usr/bin/env python3
"""Risk-overlay SWEEP for entry-copy. Fetch informed+control entries and the needed candle paths ONCE,
then grid over risk configs (and a fixed-horizon exit variant) in memory. The question: is there ANY
risk config where the in-sample-informed set both (a) nets positive AND (b) significantly beats the
no-edge control, OOS? If none does, entry-copy on majors has no capturable edge for us.
"""
from __future__ import annotations
import time
import concurrent.futures as cf
import numpy as np
import pandas as pd
import scipy.stats as ss

from research.v15.entry_copy_overlay_sim import (
    _candles, copyable_universe, overlay_trade, wallet_forward_entries,
    select_informed, Risk, FEE_RT, SLIP, FWD_START)


def collect(wallets, since, now, universe, cache, clock):
    """Return list of (coin, side, entry_px, path_df) for all copyable entries."""
    import threading
    def cc(coin):
        with clock:
            if coin in cache:
                return cache[coin]
        df = _candles(coin, since, now)
        with clock:
            cache[coin] = df
        return df
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
            out.append((e["side"], epx, fwd.iloc[1:].reset_index(drop=True)))
        return out
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        return [t for sub in ex.map(one, wallets) for t in sub]


def fixed_horizon_ret(side, epx, path, hours):
    """Exit at +hours (no stop/TP). Captures raw signal follow-through."""
    if path.empty:
        return 0.0
    tgt = path["t"].iloc[0] + hours * 3600 * 1000
    seg = path[path["t"] <= tgt]
    cl = (seg["c"].iloc[-1] if len(seg) else path["c"].iloc[0])
    g = (cl - epx) / epx if side == "Buy" else (epx - cl) / epx
    return g - SLIP - FEE_RT


def eval_overlay(trades, r):
    a = np.array([overlay_trade(s, p, path, r)[0] - SLIP - FEE_RT for s, p, path in trades])
    return a


def eval_fixed(trades, hours):
    return np.array([fixed_horizon_ret(s, p, path, hours) for s, p, path in trades])


def main():
    import threading
    informed, control = select_informed("app/data/wallet_alpha/wallet_features.parquet",
                                         max_freq=10, min_tstat=1.0, min_winrate=0.50, top_k=80)
    universe = copyable_universe()
    since = int(pd.Timestamp(FWD_START, tz="UTC").timestamp() * 1000)
    now = int(time.time() * 1000)
    cache, clock = {}, threading.Lock()
    print(f"informed={len(informed)} control={len(control)} universe={len(universe)} coins; collecting entries+paths...")
    inf = collect(informed["wallet"].tolist(), since, now, universe, cache, clock)
    ctl = collect(control["wallet"].tolist(), since, now, universe, cache, clock)
    print(f"informed copyable trades={len(inf)}  control copyable trades={len(ctl)}\n")

    def line(tag, ia, ca):
        p = ss.mannwhitneyu(ia, ca, alternative="greater")[1] if len(ia) >= 10 and len(ca) >= 10 else np.nan
        print(f"{tag:38s} INF {ia.mean()*1e4:7.1f}bps (med {np.median(ia)*1e4:6.1f}, win {100*(ia>0).mean():3.0f}%, n{len(ia)})  "
              f"CTL {ca.mean()*1e4:7.1f}bps (win {100*(ca>0).mean():3.0f}%)  delta {(ia.mean()-ca.mean())*1e4:6.1f}  p={p:.3f}")

    print("=== OVERLAY GRID (stop / arm / trail / max_hold_h) -- net bps/trade ===")
    for stop in (0.015, 0.02, 0.03, 0.05):
        for arm, trail in ((0.01, 0.006), (0.015, 0.008), (0.02, 0.012), (0.03, 0.015)):
            for hold in (6, 24):
                r = Risk(stop=stop, arm=arm, trail=trail, max_hold_h=hold)
                line(f"stop{stop:.3f} arm{arm:.3f} tr{trail:.3f} h{hold}",
                     eval_overlay(inf, r), eval_overlay(ctl, r))

    print("\n=== FIXED-HORIZON exit (no stop, capture raw follow-through) -- net bps/trade ===")
    for h in (0.25, 0.5, 1, 2, 4, 8, 24):
        line(f"fixed +{h}h", eval_fixed(inf, h), eval_fixed(ctl, h))


if __name__ == "__main__":
    main()
