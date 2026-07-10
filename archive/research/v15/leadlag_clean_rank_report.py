#!/usr/bin/env python3
"""Lead-lag clean-rank VERDICT report -- offline aggregation of the sim output against codex's 5
pass criteria. Reads the two parquets emitted by leadlag_clean_rank_sim.py (per-decision aggregate +
per-wallet forward attribution) and prints a PASS/FAIL verdict. Pure pandas, tiny, no fanout.

Codex's 5 criteria for a DEPLOYABLE copy-momentum edge (from the prior decisive test):
  1. top-decile forward edge net of cost > 0
  2. (top - matched-null) >= +15 bps
  3. bootstrap 5th pct of (top - null) > 0
  4. (top - decision-anchored beta) > 0   [skill above just being in the coins -> not trend-following]
  5. concentration: top-1 wallet < 20% and top-10 wallets < 50% of positive excess

Run: python research/v15/leadlag_clean_rank_report.py --in app/data/v15/leadlag_clean_rank.parquet
"""
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def bootstrap_pctile(x: np.ndarray, q: float = 5.0, n_boot: int = 5000, seed: int = 7) -> float:
    if len(x) == 0:
        return float("nan")
    rng = np.random.default_rng(seed)
    means = np.array([rng.choice(x, size=len(x), replace=True).mean() for _ in range(n_boot)])
    return float(np.percentile(means, q))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", default="app/data/v15/leadlag_clean_rank.parquet")
    args = ap.parse_args()
    p = Path(args.inp)
    byw_p = Path(str(p).replace(".parquet", "_bywallet.parquet"))
    df = pd.read_parquet(p)
    n_dec = len(df)
    print(f"=== lead-lag clean-rank verdict ({n_dec} decision hours) ===")
    if n_dec == 0:
        print("NO DECISIONS -> inconclusive (widen window/candidates).")
        return

    tmn = df["top_minus_null_bps"].dropna().to_numpy()
    tmb = df["top_minus_beta_bps"].dropna().to_numpy()
    top = df["top_fwd_edge_bps"].dropna().to_numpy()

    c1_top = float(np.mean(top)) if len(top) else float("nan")
    c2_tmn = float(np.mean(tmn)) if len(tmn) else float("nan")
    c3_boot = bootstrap_pctile(tmn, 5.0)
    c4_tmb = float(np.mean(tmb)) if len(tmb) else float("nan")

    # concentration of POSITIVE excess (top wallet forward edge above its own decision mean), per wallet
    conc1 = conc10 = float("nan")
    if byw_p.exists():
        bw = pd.read_parquet(byw_p)
        # excess = wallet fwd edge minus the per-decision top mean (its contribution above the cohort)
        dmean = bw.groupby("decision_ts")["fwd_edge_bps"].transform("mean")
        bw = bw.assign(excess=bw["fwd_edge_bps"] - dmean)
        pos = bw[bw["excess"] > 0]
        if len(pos):
            by_w = pos.groupby("wallet")["excess"].sum().sort_values(ascending=False)
            tot = by_w.sum()
            conc1 = float(by_w.iloc[0] / tot) if tot > 0 else float("nan")
            conc10 = float(by_w.iloc[:10].sum() / tot) if tot > 0 else float("nan")

    crit = [
        ("1. top fwd edge > 0",            c1_top, c1_top > 0),
        ("2. (top - null) >= +15bps",      c2_tmn, c2_tmn >= 15.0),
        ("3. bootstrap 5pct(top-null) > 0", c3_boot, c3_boot > 0),
        ("4. (top - beta) > 0 [skill]",    c4_tmb, c4_tmb > 0),
        ("5a. top-1 wallet < 20% excess",  conc1 * 100 if conc1 == conc1 else float("nan"),
            (conc1 < 0.20) if conc1 == conc1 else False),
        ("5b. top-10 wallets < 50% excess", conc10 * 100 if conc10 == conc10 else float("nan"),
            (conc10 < 0.50) if conc10 == conc10 else False),
    ]
    print(f"{'criterion':36s} {'value':>10s}  verdict")
    n_pass = 0
    for name, val, ok in crit:
        n_pass += int(bool(ok))
        print(f"{name:36s} {val:>10.2f}  {'PASS' if ok else 'FAIL'}")
    print(f"\nmean top_fwd={c1_top:.2f}bps  mean null={df['null_fwd_edge_bps'].dropna().mean():.2f}bps  "
          f"mean beta={df['beta_edge_bps'].dropna().mean():.2f}bps")
    print(f"mean top/null sym-side overlap={df['top_null_symside_overlap'].mean():.2f}")
    all_pass = all(ok for _, _, ok in crit)
    print(f"\nVERDICT: {'DEPLOYABLE EDGE (all pass)' if all_pass else f'KILL / no edge ({n_pass}/6 pass)'}")
    print("NOTE: survivorship-biased universe -> a positive here still needs a point-in-time-eligible "
          "rerun before deploy; a negative is a conservative kill.")


if __name__ == "__main__":
    main()
