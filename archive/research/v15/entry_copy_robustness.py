#!/usr/bin/env python3
"""Decisive robustness check for the entry-copy overlay edge.

The overlay sweep showed: informed-selected wallets, copied on majors with a wide-stop/fast-TP overlay,
net positive AND beat a no-edge control by +20-44 bps/trade (p<0.001), OOS.

The KILLER confound: is that edge ENTRY-TIMING alpha (copyable -- we replicate WHEN they enter) or merely
COIN-SELECTION (they traded coins that happened to rip; copying their timing wouldn't capture that, and it
may be survivorship)? Test: compare informed REAL entries vs informed entries with SHUFFLED entry times on
the SAME coin+side (random bar in the window). If real >> shuffled, the timing carries alpha. If real ~=
shuffled, it is coin-selection, not copyable timing.

Also re-runs the no-edge control for reference. Deterministic shuffle (seeded) for reproducibility.
"""
from __future__ import annotations
import time
import threading
import concurrent.futures as cf
import numpy as np
import pandas as pd
import scipy.stats as ss

from research.v15.entry_copy_overlay_sim import (
    _candles, copyable_universe, overlay_trade, wallet_forward_entries,
    select_informed, Risk, FEE_RT, SLIP, FWD_START)

CONFIGS = [
    ("stop3 arm1 tr0.6 h24", Risk(stop=0.03, arm=0.01, trail=0.006, max_hold_h=24)),
    ("stop5 arm1 tr0.6 h6",  Risk(stop=0.05, arm=0.01, trail=0.006, max_hold_h=6)),
    ("stop3 arm1.5 tr0.8 h24", Risk(stop=0.03, arm=0.015, trail=0.008, max_hold_h=24)),
]
N_SHUFFLE = 5  # random-time replicas per real entry


def main():
    informed, control = select_informed("app/data/wallet_alpha/wallet_features.parquet",
                                         max_freq=10, min_tstat=1.0, min_winrate=0.50, top_k=80)
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
        """(side, entry_px, path) at the REAL entry bar."""
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
                out.append((e["side"], epx, fwd.iloc[1:].reset_index(drop=True), e["coin"]))
            return out
        with cf.ThreadPoolExecutor(max_workers=4) as ex:
            return [t for sub in ex.map(one, wallets) for t in sub]

    print("collecting informed real entries...")
    inf = real_trades(informed["wallet"].tolist())
    print("collecting control real entries...")
    ctl = real_trades(control["wallet"].tolist())
    print(f"informed trades={len(inf)} control trades={len(ctl)}\n")

    # Build shuffled-time replicas of informed: same coin+side, random entry bar in the window.
    shuf = []
    for side, _epx, _path, coin in inf:
        cdf = cache.get(coin)
        if cdf is None or len(cdf) < 4:
            continue
        for _ in range(N_SHUFFLE):
            i = int(rng.integers(0, len(cdf) - 2))
            epx = cdf["c"].iloc[i]
            epx = epx * (1 + SLIP) if side == "Buy" else epx * (1 - SLIP)
            shuf.append((side, epx, cdf.iloc[i + 1:].reset_index(drop=True), coin))
    print(f"informed-shuffled replicas={len(shuf)} ({N_SHUFFLE} per real entry, same coin+side, random time)\n")

    def ev(trades, r):
        return np.array([overlay_trade(s, p, path, r)[0] - SLIP - FEE_RT for s, p, path, _c in trades])

    print("=== TIMING-ALPHA TEST: informed REAL vs informed SHUFFLED-TIME vs no-edge CONTROL ===")
    print("(if REAL >> SHUFFLED -> entry timing is copyable alpha; if REAL ~= SHUFFLED -> coin-selection only)\n")
    for name, r in CONFIGS:
        ir = ev(inf, r); ishuf = ev(shuf, r); cr = ev(ctl, r)
        p_shuf = ss.mannwhitneyu(ir, ishuf, alternative="greater")[1]
        p_ctl = ss.mannwhitneyu(ir, cr, alternative="greater")[1]
        print(f"[{name}]")
        print(f"  REAL     {ir.mean()*1e4:7.1f} bps/trade  win {100*(ir>0).mean():3.0f}%  n={len(ir)}")
        print(f"  SHUFFLED {ishuf.mean()*1e4:7.1f} bps/trade  win {100*(ishuf>0).mean():3.0f}%  n={len(ishuf)}")
        print(f"  CONTROL  {cr.mean()*1e4:7.1f} bps/trade  win {100*(cr>0).mean():3.0f}%  n={len(cr)}")
        print(f"  REAL>SHUFFLED delta {(ir.mean()-ishuf.mean())*1e4:6.1f} bps  p={p_shuf:.4f}   |   "
              f"REAL>CONTROL delta {(ir.mean()-cr.mean())*1e4:6.1f} bps  p={p_ctl:.4f}\n")


if __name__ == "__main__":
    main()
