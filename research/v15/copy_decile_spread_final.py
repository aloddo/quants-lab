#!/usr/bin/env python3
"""PRE-REGISTERED final closing test for HL public-fill copy (codex-specified).

Codex sign: stop copy and redirect, but run EXACTLY ONE final test first -- a top-decile-minus-rest
portfolio spread on the UNTOUCHED SECOND HALF, at the already-selected cell (14d lookback, 2d horizon), with
costs/slippage included and wallet-cluster awareness, plus subperiod stability. Decision rule (pre-registered,
NO tuning, NO further grids): if the top-decile copy return is not clearly positive, stable across subperiods,
and executable after costs, KILL HL public-fill copy.

PRE-REGISTERED PARAMS (locked, do not change): L=14d, H=2d, per-entry hold 4h, cost = 8.64bps + 2bps slip,
top-decile = top 10% by trailing-14d informedness among wallets with >=8 entries in the trailing window,
rebalance every 2 days, SECOND-HALF rebalances only.
"""
from __future__ import annotations
import glob, os
import numpy as np, pandas as pd, scipy.stats as ss

FEE_RT = 0.000864; SLIP = 0.0002; BUCKET_MS = 900_000; HOLD = 16
LO, HI = 20251201, 20260527; DAY = 86_400_000
L, H = 14, 2; MINW = 8; REBAL_STEP = 2; DECILE = 0.10


def main():
    files = sorted(glob.glob("app/data/hl_s3_fills_v2/*.parquet"))
    files = [f for f in files if os.path.basename(f)[:-8].isdigit() and LO <= int(os.path.basename(f)[:-8]) <= HI]
    # pass1 price
    pv = []
    for f in files:
        df = pd.read_parquet(f, columns=["coin", "price", "size", "time"])
        df = df[~df["coin"].astype(str).str.contains(":")]
        if df.empty:
            continue
        df["b"] = (df["time"] // BUCKET_MS) * BUCKET_MS
        df["sz"] = pd.to_numeric(df["size"], errors="coerce").fillna(0.0)
        df["pvv"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0) * df["sz"]
        pv.append(df.groupby(["coin", "b"]).agg(pvv=("pvv", "sum"), sz=("sz", "sum")).reset_index())
    pa = pd.concat(pv, ignore_index=True).groupby(["coin", "b"]).agg(pvv=("pvv", "sum"), sz=("sz", "sum")).reset_index()
    pa["vwap"] = pa["pvv"] / pa["sz"].clip(lower=1e-9)
    price = pa.set_index(["coin", "b"])["vwap"]
    # pass2 wallet-day
    rows = []
    for f in files:
        di = int((pd.Timestamp(os.path.basename(f)[:-8], tz="UTC").value // 1_000_000) // DAY)
        df = pd.read_parquet(f, columns=["wallet", "coin", "price", "time", "dir"])
        df = df[(~df["coin"].astype(str).str.contains(":")) & (df["dir"].isin(["Open Long", "Open Short"]))]
        if df.empty:
            continue
        df["b"] = (df["time"] // BUCKET_MS) * BUCKET_MS
        df["side"] = np.where(df["dir"].values == "Open Long", 1.0, -1.0)
        p0 = price.reindex(pd.MultiIndex.from_arrays([df["coin"], df["b"]])).to_numpy()
        p1 = price.reindex(pd.MultiIndex.from_arrays([df["coin"], df["b"] + HOLD * BUCKET_MS])).to_numpy()
        ok = np.isfinite(p0) & np.isfinite(p1) & (p0 > 0) & (p1 > 0)
        if not ok.any():
            continue
        ret = df["side"].to_numpy()[ok] * (p1[ok] / p0[ok] - 1.0) - FEE_RT - SLIP
        sub = pd.DataFrame({"wallet": df["wallet"].to_numpy()[ok], "ret": ret})
        agg = sub.groupby("wallet")["ret"].agg(["mean", "count"]).reset_index()
        for w, m, n in zip(agg["wallet"], agg["mean"], agg["count"]):
            rows.append((w, di, m, int(n)))
    wd = pd.DataFrame(rows, columns=["wallet", "day", "mret", "n"])
    d0, d1 = int(wd["day"].min()), int(wd["day"].max()); nd = d1 - d0 + 1
    wals = wd["wallet"].unique(); wi = {w: i for i, w in enumerate(wals)}
    SUM = np.zeros((len(wals), nd)); CNT = np.zeros((len(wals), nd))
    np.add.at(SUM, (wd["wallet"].map(wi).to_numpy(), wd["day"].to_numpy() - d0), (wd["mret"] * wd["n"]).to_numpy())
    np.add.at(CNT, (wd["wallet"].map(wi).to_numpy(), wd["day"].to_numpy() - d0), wd["n"].to_numpy())
    cS = np.concatenate([np.zeros((len(wals), 1)), np.cumsum(SUM, 1)], 1)
    cN = np.concatenate([np.zeros((len(wals), 1)), np.cumsum(CNT, 1)], 1)

    def win(t, lo, hi):
        a = max(0, min(nd, t + lo - d0)); b = max(0, min(nd, t + hi - d0))
        s = cS[:, b] - cS[:, a]; n = cN[:, b] - cN[:, a]
        with np.errstate(invalid="ignore", divide="ignore"):
            return np.where(n > 0, s / n, np.nan), n

    rebals = [t for t in range(d0, d1 + 1) if (t - L) >= d0 and (t + H) <= d1][::REBAL_STEP]
    second = rebals[len(rebals) // 2:]   # untouched OOS half (selection of the cell used first half)
    dec_ret, rest_ret, spread, prev_sel = [], [], [], None
    turnover = []
    for t in second:
        ti, ni = win(t, -L, 0); no_m, no_n = win(t, 0, H)
        elig = (ni >= MINW) & np.isfinite(ti)
        meas = (no_n >= MINW) & np.isfinite(no_m)
        idx = np.where(elig)[0]
        if len(idx) < 30:
            continue
        thr = np.quantile(ti[idx], 1 - DECILE)
        sel = set(np.where(elig & (ti >= thr))[0].tolist())
        rest = set(idx.tolist()) - sel
        sel_m = [no_m[i] for i in sel if meas[i]]
        rest_m = [no_m[i] for i in rest if meas[i]]
        if len(sel_m) < 5 or len(rest_m) < 5:
            continue
        dec_ret.append(np.mean(sel_m)); rest_ret.append(np.mean(rest_m))
        spread.append(np.mean(sel_m) - np.mean(rest_m))
        if prev_sel is not None and (len(sel) + len(prev_sel)):
            turnover.append(1 - len(sel & prev_sel) / max(len(sel), 1))
        prev_sel = sel
    dec_ret, rest_ret, spread = map(np.array, (dec_ret, rest_ret, spread))
    print("=== PRE-REGISTERED CLOSING TEST (codex-specified): top-decile copy, 14d/2d, OOS 2nd half, costed ===")
    print(f"rebalances (OOS half): {len(dec_ret)} | avg decile turnover/rebalance: {np.mean(turnover)*100:.0f}%")
    t1, p1 = ss.ttest_1samp(dec_ret, 0.0)
    t2, p2 = ss.ttest_1samp(spread, 0.0)
    print(f"TOP-DECILE copy return (deployable): mean {dec_ret.mean()*1e4:.1f} bps/period  t={t1:.2f} p={p1:.4f}  win {100*(dec_ret>0).mean():.0f}%")
    print(f"REST return:                          mean {rest_ret.mean()*1e4:.1f} bps")
    print(f"SPREAD (decile - rest):               mean {spread.mean()*1e4:.1f} bps  t={t2:.2f} p={p2:.4f}")
    half = len(dec_ret) // 2
    print(f"STABILITY (decile return): subperiod A {dec_ret[:half].mean()*1e4:.1f} bps | B {dec_ret[half:].mean()*1e4:.1f} bps")
    ok = (p1 < 0.05 and dec_ret.mean() > 0 and dec_ret[:half].mean() > 0 and dec_ret[half:].mean() > 0)
    print(f"\nDECISION (pre-registered rule): {'PASS -- decile copy positive, significant, stable after costs -> investigate live' if ok else 'FAIL -- KILL HL public-fill copy (not positive/stable/executable)'}")


if __name__ == "__main__":
    main()
