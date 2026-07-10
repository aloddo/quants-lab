#!/usr/bin/env python3
"""SPRINT: permutation null for the KNET (net-consensus) signal (codex spec 2026-06-11).

Observed: knet = k_same - k_opp at entry is monotone with overlay net bps in both folds
(knet<0 ~ 0bps; knet>=5 ~ +51-54bps mean). Suspicion: knet>0 may just proxy 'trendy period'
(time clustering), not wallet-specific information.

Null construction (codex): within each (fold, coin, side, UTC-day) cell, SHUFFLE the entry
timestamps across that cell's trades (preserving the cell's trade count, coins, sides, days,
and each trade's own outcome), then RECOMPUTE knet against the UNSHUFFLED position intervals
of the other wallets. If the shuffled-knet edge spread matches the observed spread, knet is
time-structure, not signal. 200 permutations -> null distribution of (mean[knet>=5] -
mean[knet<0]).

NOTE on mechanics: outcome (ov_bps) stays attached to the TRADE; only its entry timestamp used
for the knet recomputation is shuffled within the cell. Position intervals of OTHER wallets
stay real, so the null preserves the herd structure but breaks the alignment between THIS
trade's timing-within-day and the herd state.

Run: python research/v16/knet_null_test.py     (~1-2 min, pure pandas/numpy)
"""
from __future__ import annotations
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path("/Users/hermes/quants-lab")
RNG = np.random.default_rng(7)


def knet_for(df, entry_col):
    """Vectorized-ish knet: for each trade, net same-side minus opp-side OTHER-wallet positions
    covering the (possibly shuffled) entry ts. Interval set = REAL (entry_ts, exit_ts)."""
    out = np.zeros(len(df), dtype=np.int32)
    for (fold, coin), g in df.groupby(["fold", "coin"]):
        E = g.entry_ts.values          # real intervals
        X = g.exit_ts.values
        D = g["dir"].values
        W = g.wallet_id.values
        T = g[entry_col].values        # query timestamps (real or shuffled)
        idx = g.index.values
        for j in range(len(g)):
            live = (E <= T[j]) & (X > T[j]) & (W != W[j])
            out[idx[j]] = int(np.sum(live & (D == D[j])) - np.sum(live & (D != D[j])))
    return out


def spread(df, knet):
    hi = df.ov_bps.values[knet >= 5]
    lo = df.ov_bps.values[knet < 0]
    if len(hi) < 30 or len(lo) < 30:
        return np.nan
    return hi.mean() - lo.mean()


def main():
    df = pd.read_parquet(_REPO / "app" / "data" / "v16" / "sprint_trades_enriched.parquet")
    df = df.reset_index(drop=True)
    df["wallet_id"] = pd.factorize(df.wallet)[0]
    df["day"] = (df.entry_ts // 86_400_000).astype(np.int64)

    base_knet = knet_for(df, "entry_ts")
    # sanity: matches stored k_same-k_opp
    stored = (df.k_same - df.k_opp).values
    agree = (base_knet == stored).mean()
    print(f"recomputed knet agreement with stored: {agree*100:.1f}%")
    obs = spread(df, base_knet)
    print(f"OBSERVED spread mean(knet>=5) - mean(knet<0): {obs:+.2f} bps")
    for fold, g in df.groupby("fold"):
        k = base_knet[g.index.values]
        print(f"  {fold}: spread {spread(g, k):+.2f}")

    n_perm = 200
    null = []
    cells = df.groupby(["fold", "coin", "dir", "day"]).indices
    shuf_col = df.entry_ts.values.copy()
    for p in range(n_perm):
        s = df.entry_ts.values.copy()
        for _, idxs in cells.items():
            if len(idxs) > 1:
                s[idxs] = RNG.permutation(s[idxs])
        df["_shuf_ts"] = s
        k = knet_for(df, "_shuf_ts")
        null.append(spread(df, k))
        if (p + 1) % 50 == 0:
            print(f"  perm {p+1}/{n_perm}: null spread so far mean {np.nanmean(null):+.2f} "
                  f"p95 {np.nanpercentile(null, 95):+.2f}", flush=True)
    null = np.array(null)
    p_val = float((null >= obs).mean())
    print(f"\nNULL: mean {np.nanmean(null):+.2f} | p95 {np.nanpercentile(null,95):+.2f} | "
          f"p99 {np.nanpercentile(null,99):+.2f}")
    print(f"OBSERVED {obs:+.2f} | permutation p-value = {p_val:.4f}")
    print("VERDICT:", "SIGNAL (survives null)" if p_val < 0.01 else
          "AMBIGUOUS" if p_val < 0.05 else "TIME-STRUCTURE ARTIFACT (killed)")


if __name__ == "__main__":
    main()
