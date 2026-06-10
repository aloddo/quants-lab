#!/usr/bin/env python3
"""Minute/hour post-fill DRIFT diagnostic (codex-specified last copy stone; research, bot PAUSED).

Codex: "worth one run, but only as a latency-decay diagnostic." All prior tests used 4h+ holds; this checks
whether informed wallets' entries have tradable drift at MINUTE/HOUR horizons after our 1-2s arrival delay.
Spec: +2s arrival, copy same direction at first executable 1m fill-VWAP after delay, horizons 1m/5m/15m/60m,
no stop/TP, costed (8.64bps + 2bps slip), OOS second half, fixed informed pool (markout-selected).
PASS only if top/informed ABSOLUTE net return is positive after costs at 1m or 5m with decay at longer
horizons. KILL if positive-spread-but-negative-absolute like every prior test. Do not iterate.
"""
from __future__ import annotations
import glob, os
import numpy as np, pandas as pd
from research.v15.entry_copy_overlay_sim import select_informed

FEE_RT = 0.000864; SLIP = 0.0002; MIN_MS = 60_000; DELAY_MS = 2_000
LO, HI = 20251201, 20260527
HORIZONS = {"1m": 1, "5m": 5, "15m": 15, "60m": 60}


def main():
    informed, _ = select_informed("app/data/wallet_alpha/wallet_features.parquet",
                                  max_freq=10, min_tstat=1.0, min_winrate=0.50, top_k=80)
    pool = set(informed["wallet"])
    files = sorted(glob.glob("app/data/hl_s3_fills_v2/*.parquet"))
    files = [f for f in files if os.path.basename(f)[:-8].isdigit() and LO <= int(os.path.basename(f)[:-8]) <= HI]
    files = files[len(files) // 2:]   # OOS second half
    print(f"OOS files: {len(files)} ({os.path.basename(files[0])}..{os.path.basename(files[-1])}); pool={len(pool)}")

    # pass A: informed pool's OPEN entries (coin, side, t) + their coin set
    entries, coins = [], set()
    for f in files:
        df = pd.read_parquet(f, columns=["wallet", "coin", "time", "dir"])
        df = df[(df["wallet"].isin(pool)) & (~df["coin"].astype(str).str.contains(":")) &
                (df["dir"].isin(["Open Long", "Open Short"]))]
        if df.empty:
            continue
        for c, t, d in zip(df["coin"], df["time"], df["dir"]):
            entries.append((c, 1 if d == "Open Long" else -1, int(t)))
            coins.add(c)
    print(f"informed OOS entries on majors: {len(entries)} across {len(coins)} coins")

    # pass B: 1m VWAP for just those coins over OOS
    price = {}
    for f in files:
        df = pd.read_parquet(f, columns=["coin", "price", "size", "time"])
        df = df[df["coin"].isin(coins)]
        if df.empty:
            continue
        df["b"] = (df["time"] // MIN_MS) * MIN_MS
        df["sz"] = pd.to_numeric(df["size"], errors="coerce").fillna(0.0)
        df["pv"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0) * df["sz"]
        a = df.groupby(["coin", "b"]).agg(pv=("pv", "sum"), sz=("sz", "sum")).reset_index()
        for c, b, pv, sz in zip(a["coin"], a["b"], a["pv"], a["sz"]):
            price.setdefault(c, {})[b] = pv / max(sz, 1e-9)
    print(f"1m price built for {len(price)} coins")

    res = {k: [] for k in HORIZONS}
    for c, side, t in entries:
        pm = price.get(c)
        if not pm:
            continue
        b0 = ((t + DELAY_MS) // MIN_MS) * MIN_MS
        p0 = pm.get(b0)
        if not p0 or p0 <= 0:
            continue
        for name, mins in HORIZONS.items():
            p1 = pm.get(b0 + mins * MIN_MS)
            if p1 and p1 > 0:
                res[name].append(side * (p1 / p0 - 1.0) - FEE_RT - SLIP)

    print("\n=== MINUTE/HOUR POST-FILL DRIFT (informed entries, +2s arrival, costed, OOS) ===")
    print(f"{'horizon':>8} {'n':>7} {'net bps':>9} {'win%':>6} {'gross bps':>10}")
    pass_flag = False
    for name in HORIZONS:
        a = np.array(res[name])
        if len(a) < 30:
            print(f"{name:>8} {len(a):>7} (insufficient)")
            continue
        gross = a + FEE_RT + SLIP
        print(f"{name:>8} {len(a):>7} {a.mean()*1e4:>9.1f} {100*(a>0).mean():>6.0f} {gross.mean()*1e4:>10.1f}")
        if name in ("1m", "5m") and a.mean() > 0:
            pass_flag = True
    print(f"\nDECISION (codex pre-registered): {'PASS -- positive absolute net drift at 1m/5m -> investigate' if pass_flag else 'KILL -- no positive absolute post-fill drift after costs (copy closed end-to-end)'}")


if __name__ == "__main__":
    main()
