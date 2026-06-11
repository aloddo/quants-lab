#!/usr/bin/env python3
"""SPRINT: codex round-2 DECISIVE knet test -- copy edge minus decision-anchored beta benchmark.

Benchmark per trade: enter SAME coin, side, timestamp (+2s latency) with the SAME execution
costs, but exit leader-INDEPENDENTLY at entry+72h (standardized horizon). bench_bps strips the
leader's exit selection; what remains is the market's direction conditional on (coin, side, t).

Codex pass gates (both folds):
  knet>=5 residual (copy - bench) >= +10bps ; knet<0 residual <= 0 ;
  residual spread (knet>=5 minus knet<0) >= +12bps ;
  no single coin contributes > 40% of the residual uplift.
If bench reproduces the bucket spread -> knet is a RISK-STATE filter (gate ok, no sizing).

Run: python research/v16/knet_beta_residual.py   (~2 min)
"""
from __future__ import annotations
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path("/Users/hermes/quants-lab")
sys.path.insert(0, str(_REPO / "research" / "v15"))
sys.path.insert(0, str(_REPO / "research" / "v16"))

import leadlag_clean_rank_sim as S
from execution_model import fee_rt, apply_entry, apply_exit, set_latency_ms
from select_cohort import CAP

FEE_T = fee_rt(maker=False)
LAT = 2_000
H72 = 72 * 3_600_000


def bench_net(coin, dir_, ets):
    m0 = S.mark_at(coin, ets + LAT)
    m1 = S.mark_at(coin, ets + LAT + H72)
    if m0 is None or m1 is None or m0 <= 0:
        return None
    e = apply_entry(coin, m0, dir_ > 0)
    x = apply_exit(coin, m1, dir_ > 0)
    g = max(-CAP, min(CAP, dir_ * (x - e) / e))
    return g * 1e4 - FEE_T * 1e4


def main():
    import os
    _md = os.environ.get("V16_SPRINT_MARKS_DIR")
    if _md:
        S.ASSETCTX_DIR = Path(_md)
        print(f"marks dir override: {_md}")
    set_latency_ms(LAT)
    df = pd.read_parquet(_REPO / "app" / "data" / "v16" / "sprint_trades_enriched.parquet")
    df["knet"] = df.k_same - df.k_opp
    df["bucket"] = np.select([df.knet < 0, df.knet >= 5], ["neg", "5p"], default="mid")
    b = np.full(len(df), np.nan)
    for i, (c, d_, t) in enumerate(zip(df.coin.values, df["dir"].values, df.entry_ts.values)):
        v = bench_net(c, int(d_), int(t))
        if v is not None:
            b[i] = v
    df["bench_bps"] = b
    df = df.dropna(subset=["bench_bps"]).copy()
    df["residual"] = df.ov_bps - df.bench_bps

    gates = []
    for fold, g in df.groupby("fold"):
        print(f"\n=== {fold} (n={len(g)}) ===")
        tab = g.groupby("bucket").agg(n=("residual", "count"),
                                      copy_mean=("ov_bps", "mean"),
                                      bench_mean=("bench_bps", "mean"),
                                      resid_mean=("residual", "mean")).round(2)
        print(tab.to_string())
        r5 = g[g.bucket == "5p"].residual.mean()
        rn = g[g.bucket == "neg"].residual.mean()
        spread_resid = r5 - rn
        bench_spread = g[g.bucket == "5p"].bench_bps.mean() - g[g.bucket == "neg"].bench_bps.mean()
        copy_spread = g[g.bucket == "5p"].ov_bps.mean() - g[g.bucket == "neg"].ov_bps.mean()
        print(f"  copy spread {copy_spread:+.1f} = bench(risk-state) {bench_spread:+.1f} "
              f"+ residual(alpha) {spread_resid:+.1f}")
        # coin concentration of residual uplift among knet>=5
        g5 = g[g.bucket == "5p"]
        up = g5.groupby("coin").residual.sum()
        tot = up[up > 0].sum()
        conc = (up.max() / tot * 100) if tot > 0 else float("nan")
        print(f"  knet>=5: resid {r5:+.1f} | knet<0: resid {rn:+.1f} | resid spread {spread_resid:+.1f} "
              f"| max-coin share of positive resid uplift {conc:.0f}% ({up.idxmax()})")
        gates.append({"fold": fold, "r5": r5, "rn": rn, "spread": spread_resid, "conc": conc})

    print("\n=== CODEX GATES ===")
    ok = True
    for gt in gates:
        g1 = gt["r5"] >= 10.0
        g2 = gt["rn"] <= 0.0
        g3 = gt["spread"] >= 12.0
        g4 = gt["conc"] <= 40.0
        ok &= (g1 and g2 and g3 and g4)
        print(f"  {gt['fold']}: r5>=+10 {'PASS' if g1 else 'FAIL'}({gt['r5']:+.1f}) | "
              f"rneg<=0 {'PASS' if g2 else 'FAIL'}({gt['rn']:+.1f}) | "
              f"spread>=+12 {'PASS' if g3 else 'FAIL'}({gt['spread']:+.1f}) | "
              f"coinconc<=40% {'PASS' if g4 else 'FAIL'}({gt['conc']:.0f}%)")
    print("VERDICT:", "ALPHA (knet usable as gate AND informative)" if ok else
          "RISK-STATE FILTER (gate ok, do NOT size on it)" )
    df.to_parquet(_REPO / "app" / "data" / "v16" / "knet_residual.parquet")


if __name__ == "__main__":
    main()
