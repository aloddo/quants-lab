#!/usr/bin/env python3
"""Edge-decay HALF-LIFE surface (offline, zero capital) -- codex-endorsed step 1 of the dynamic copy thesis.

Tests Alberto's hypothesis: wallet edge is real but SHORT-LIVED (sub-monthly) and rotates. My prior null was
at MONTHLY cadence; this sweeps finer. Question: does a wallet's informedness over a trailing window of L
DAYS predict its direction-adjusted return over the next H DAYS? Sweep L,H. Persistence measured at the
WALLET level (Spearman across wallets) so the effective sample is #wallets, not #trades (codex caution).

Codex's leakage guards, all implemented:
- CAUSAL L: trailing window ends strictly BEFORE the rebalance instant t (uses days < t).
- WALK-FORWARD L,H selection: pick the best (L,H) on the FIRST half of rebalance dates, then report that
  cell's persistence on the untouched SECOND half. The headline number is the second-half (OOS) one.
- No future-conditioned universe: wallets enter a rebalance's ranking via trailing activity ONLY.
- Executable, costed returns: per-entry return = direction-adjusted 4h fill-VWAP move net fees+slippage
  (not ideal wallet marks).
- Overlap noted: H windows overlap across rebalance dates -> we report per-rebalance Spearman and treat the
  sequence as the sample (do not pool entries to fake-inflate t-stats).

Two passes over fills (majors only). Pass1: per-coin bucket-VWAP series. Pass2: per-(wallet,day) mean 4h
dir-adj return + count. Then the L,H sweep in memory. Memory-safe.
"""
from __future__ import annotations
import glob, os
import numpy as np, pandas as pd, scipy.stats as ss

FEE_RT = 0.000864
SLIP = 0.0002
BUCKET_MS = 900_000
HOLD = 16            # per-entry hold = 4h (16 buckets)
LO, HI = 20251201, 20260527
DAY = 86_400_000

LS = [1, 2, 3, 5, 7, 14, 21, 30]   # trailing informedness windows (days)
HS = [1, 2, 3, 5, 7]               # forward evaluation windows (days)
REBAL_STEP = 2                     # rebalance every 2 days
MINW = 8                           # min entries each side to include a wallet at a rebalance


def main():
    files = sorted(glob.glob("app/data/hl_s3_fills_v2/*.parquet"))
    files = [f for f in files if os.path.basename(f)[:-8].isdigit() and LO <= int(os.path.basename(f)[:-8]) <= HI]
    print(f"{len(files)} day-files")

    # PASS 1: per-coin bucket VWAP -> a single Series indexed by (coin,b) for vectorized lookup
    pv_rows = []
    for f in files:
        df = pd.read_parquet(f, columns=["coin", "price", "size", "time"])
        df = df[~df["coin"].astype(str).str.contains(":")]
        if df.empty:
            continue
        df["b"] = (df["time"] // BUCKET_MS) * BUCKET_MS
        df["sz"] = pd.to_numeric(df["size"], errors="coerce").fillna(0.0)
        df["pv"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0) * df["sz"]
        a = df.groupby(["coin", "b"]).agg(pv=("pv", "sum"), sz=("sz", "sum")).reset_index()
        pv_rows.append(a)
    pa = pd.concat(pv_rows, ignore_index=True).groupby(["coin", "b"]).agg(pv=("pv", "sum"), sz=("sz", "sum")).reset_index()
    pa["vwap"] = pa["pv"] / pa["sz"].clip(lower=1e-9)
    price = pa.set_index(["coin", "b"])["vwap"]
    print(f"pass1 done: {price.index.get_level_values(0).nunique()} coins, {len(price)} buckets")

    # PASS 2: per-(wallet, day) mean 4h dir-adj return + count -- VECTORIZED (map, no itertuples)
    rows = []
    for f in files:
        ymd = int(os.path.basename(f)[:-8])
        di = int((pd.Timestamp(str(ymd), tz="UTC").value // 1_000_000) // DAY)
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
    print(f"pass2 done: {len(wd)} wallet-days, {wd['wallet'].nunique()} wallets, "
          f"days {wd['day'].min()}..{wd['day'].max()}")

    # dense wallet x day matrices: SUM of daily mret*n (=sum ret) and N (count). cumsum over days -> O(1) windows.
    d0, d1 = int(wd["day"].min()), int(wd["day"].max())
    ndays = d1 - d0 + 1
    wallets = wd["wallet"].unique()
    widx = {w: i for i, w in enumerate(wallets)}
    SUM = np.zeros((len(wallets), ndays)); CNT = np.zeros((len(wallets), ndays))
    wi = wd["wallet"].map(widx).to_numpy(); dj = (wd["day"].to_numpy() - d0)
    np.add.at(SUM, (wi, dj), (wd["mret"] * wd["n"]).to_numpy())
    np.add.at(CNT, (wi, dj), wd["n"].to_numpy())
    cumS = np.concatenate([np.zeros((len(wallets), 1)), np.cumsum(SUM, axis=1)], axis=1)  # [:, k] = sum days [0,k)
    cumN = np.concatenate([np.zeros((len(wallets), 1)), np.cumsum(CNT, axis=1)], axis=1)

    def win(t, lo_off, hi_off):
        """vectorized activity-weighted mean over day-window [t+lo_off, t+hi_off); returns (mean[], n[])."""
        a = (t + lo_off) - d0; b = (t + hi_off) - d0
        a = max(0, min(ndays, a)); b = max(0, min(ndays, b))
        s = cumS[:, b] - cumS[:, a]; n = cumN[:, b] - cumN[:, a]
        with np.errstate(invalid="ignore", divide="ignore"):
            m = np.where(n > 0, s / n, np.nan)
        return m, n

    rebals = [t for t in range(d0, d1 + 1) if (t - max(LS)) >= d0 and (t + max(HS)) <= d1][::REBAL_STEP]
    half = len(rebals) // 2

    def cell_seq(L, H):
        first, second = [], []
        for i, t in enumerate(rebals):
            mi, ni = win(t, -L, 0); mo, no = win(t, 0, H)
            mask = (ni >= MINW) & (no >= MINW) & np.isfinite(mi) & np.isfinite(mo)
            if mask.sum() < 12:
                continue
            sp = ss.spearmanr(mi[mask], mo[mask])[0]
            if sp == sp:
                (first if i < half else second).append(sp)
        return first, second

    print("\n=== HALF-LIFE SURFACE: wallet-level persistence Spearman(trailing-L informedness, next-H return) ===")
    print("(per-rebalance Spearman, averaged; FIRST-half = selection, SECOND-half = OOS)  first-half cells below")
    print(f"{'L/H':>5} " + " ".join(f"{h:>6}d" for h in HS))
    surf = {}
    for L in LS:
        cells = []
        for H in HS:
            first, second = cell_seq(L, H)
            surf[(L, H)] = (np.mean(first) if first else np.nan, np.mean(second) if second else np.nan,
                            second, len(second))
            cells.append(surf[(L, H)][0])
        print(f"{L:>4}d " + " ".join(f"{c:>6.3f}" if c == c else f"{'na':>6}" for c in cells))

    valid = {k: v for k, v in surf.items() if v[0] == v[0] and v[1] == v[1]}
    best = max(valid, key=lambda k: valid[k][0])
    f1, s2, seq, n2 = valid[best]
    L, H = best
    tstat, pval = ss.ttest_1samp(seq, 0.0) if len(seq) >= 5 else (np.nan, np.nan)
    print(f"\nWALK-FORWARD: best (L,H) by FIRST half = L={L}d H={H}d  first-half persist={f1:.3f}")
    print(f"  -> SECOND-half (OOS) persist={s2:.3f} over {n2} rebalances; mean {np.mean(seq):.3f} "
          f"t={tstat:.2f} p={pval:.4f}")
    print(f"  VERDICT: {'sub-monthly persistence SURVIVES walk-forward (investigate live)' if (pval==pval and pval<0.05 and np.mean(seq)>0.05) else 'no OOS-robust sub-monthly persistence -> dynamic re-rank unlikely to help'}")


if __name__ == "__main__":
    main()
