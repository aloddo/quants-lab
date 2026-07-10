#!/usr/bin/env python3
"""Beta-neutral SELECTION test -- strips the overlay geometry to isolate whether informed wallets pick the
right coin+SIDE+day. The OOS window (2026-05-28..now) was a CRASH (BTC -18%, ETH -23%, SOL -25%), so a
long-biased read would lose ~20%; positive direction-adjusted returns here are NOT beta.

Metric: direction-adjusted RAW coin return at fixed horizon h (NO stop/TP):
  dr = (exit-entry)/entry if Buy else (entry-exit)/entry,  net of fees+slippage.
Compares informed REAL vs freq-matched no-edge CONTROL vs SAME-DAY shuffle (same coin+side+day, random time).
Also reports the SHORT fraction per group (did informed correctly lean short into the crash?).
"""
from __future__ import annotations
import time, threading
import concurrent.futures as cf
import numpy as np, pandas as pd, scipy.stats as ss
from research.v15.entry_copy_overlay_sim import (
    _candles, copyable_universe, wallet_forward_entries, select_informed, FEE_RT, SLIP, FWD_START)
from research.v15.entry_copy_robustness2 import freq_matched_control

DAY_MS = 86400_000
HORIZONS_H = [1, 4, 12, 24]


def main():
    feat = "app/data/wallet_alpha/wallet_features.parquet"
    informed, _ = select_informed(feat, max_freq=10, min_tstat=1.0, min_winrate=0.50, top_k=80)
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

    def collect(wallets):
        def one(w):
            out = []
            for e in wallet_forward_entries(w, since, now, universe):
                cdf = cc(e["coin"])
                if cdf.empty:
                    continue
                out.append((e["coin"], e["side"], e["ts"]))
            return out
        with cf.ThreadPoolExecutor(max_workers=4) as ex:
            return [t for sub in ex.map(one, wallets) for t in sub]

    print("collecting...")
    inf = collect(informed["wallet"].tolist())
    ctl = collect(control["wallet"].tolist())
    print(f"informed entries={len(inf)} control={len(ctl)}")
    print(f"SHORT fraction: informed {np.mean([s=='Sell' for _c,s,_t in inf])*100:.0f}%  "
          f"control {np.mean([s=='Sell' for _c,s,_t in ctl])*100:.0f}%\n")

    def dadj_ret(coin, side, ts, h):
        cdf = cache.get(coin)
        if cdf is None or len(cdf) < 2:
            return None
        fwd = cdf[cdf["t"] >= ts + 1000]
        if len(fwd) < 2:
            return None
        epx = fwd["c"].iloc[0]
        epx = epx * (1 + SLIP) if side == "Buy" else epx * (1 - SLIP)
        tgt = fwd["t"].iloc[0] + h * 3600 * 1000
        seg = fwd[fwd["t"] <= tgt]
        xpx = seg["c"].iloc[-1] if len(seg) else fwd["c"].iloc[-1]
        xpx = xpx * (1 - SLIP) if side == "Buy" else xpx * (1 + SLIP)
        g = (xpx - epx) / epx if side == "Buy" else (epx - xpx) / epx
        return g - FEE_RT

    def grp(entries, h, shuffle_sameday=False):
        out = []
        for coin, side, ts in entries:
            if shuffle_sameday:
                cdf = cache.get(coin)
                if cdf is None:
                    continue
                day0 = (ts // DAY_MS) * DAY_MS
                idx = cdf.index[(cdf["t"] >= day0) & (cdf["t"] < day0 + DAY_MS)].tolist()
                if not idx:
                    continue
                ts = int(cdf["t"].loc[int(rng.choice(idx))])
            r = dadj_ret(coin, side, ts, h)
            if r is not None:
                out.append(r)
        return np.array(out)

    print("=== BETA-NEUTRAL direction-adjusted RAW coin return (no overlay), net fees+slip, in a -20% crash ===")
    print(f"{'h':>4} | {'INFORMED':>20} | {'CONTROL(fm)':>16} | {'SAME-DAY-shuf':>16} | p(INF>CTL) p(INF>SD)")
    for h in HORIZONS_H:
        R = grp(inf, h); C = grp(ctl, h); S = grp(inf, h, shuffle_sameday=True)
        p_c = ss.mannwhitneyu(R, C, alternative="greater")[1] if len(R) >= 10 and len(C) >= 10 else float("nan")
        p_s = ss.mannwhitneyu(R, S, alternative="greater")[1] if len(R) >= 10 and len(S) >= 10 else float("nan")
        print(f"{h:>4}h| {R.mean()*1e4:7.1f}bps win{100*(R>0).mean():3.0f}% n{len(R):<5}| "
              f"{C.mean()*1e4:7.1f}bps n{len(C):<5}| {S.mean()*1e4:7.1f}bps n{len(S):<5}| "
              f"{p_c:.4f}    {p_s:.4f}")


if __name__ == "__main__":
    main()
