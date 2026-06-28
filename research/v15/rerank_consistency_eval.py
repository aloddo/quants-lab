#!/usr/bin/env python3
"""rerank_consistency_eval.py -- V15-NATIVE selection + k-sweep eval (Alberto 9867 'b' + 9869).

Thin DRIVER on research/v15/v15_forward_select.forward_backtest (the canonical "plug a selection rule,
grade walk-forward through M9" harness). Does NOT modify the harness. Tests, all through M9 chained OOS
(execution_model slippage/fees + capital ledger + $10 min-notional feasibility at b0):

  HYP-1 selection rule:   live SKILL rank   vs   consistency-tilted ranks
  HYP-2 # of leaders:     k_select in {5,10,20,40}   (copy-only-the-best vs broad)
  HYP-3 diversification:  M9 min-notional decides which picks are fundable at b0=$500, so the k-sweep
                          directly shows whether more-smaller beats fewer-bigger AT OUR CAPITAL.

Selection rules operate ONLY on trailing features known at each fold (no look-ahead; harness-enforced):
  trail_pos_frac (consistency = frac of prior OOS folds positive), trail_dd (mean prior maxDD),
  trail_mean (mean prior OOS roe), trail_n, trail_elig (count prior eligible folds), pre_roe/pre_dd
  (pretest as-of fold k). Fold 1 has no trailing -> fall back to pretest (pre_roe, -pre_dd).

Live SKILL rank = z(win)+z(sharpe)+z(-maxdd); in V15 features the no-return analogue (biweekly frozen
rule) = z(pos_frac)+z(-dd). Consistency variants add weight to pos_frac / eligibility persistence.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v15/rerank_consistency_eval.py --ks 5,10,20,40
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import v15_forward_select as FS  # noqa: E402


def z(x: pd.Series) -> pd.Series:
    x = x.astype(float)
    return (x - x.mean()) / (x.std() + 1e-9)


def _fold1_fallback(f: pd.DataFrame) -> pd.Series:
    """Pretest-based score for the no-trailing case (trail_n==0)."""
    return z(f["pre_roe"].fillna(f["pre_roe"].median())) + z(-f["pre_dd"].fillna(f["pre_dd"].median()))


def make_score(kind: str):
    def score(f: pd.DataFrame) -> pd.Series:
        has_trail = f["trail_n"] > 0
        pf = z(f["trail_pos_frac"].fillna(f["trail_pos_frac"].median()))
        dd = z(-f["trail_dd"].fillna(f["trail_dd"].median()))
        mn = z(f["trail_mean"].fillna(f["trail_mean"].median()))
        eligfrac = z((f["trail_elig"] / f["trail_n"].clip(lower=1)).fillna(0))
        if kind == "skill":           # live no-return analogue
            s = pf + dd
        elif kind == "skill+mean":    # add the (deliberately-excluded-live) return term
            s = pf + dd + mn
        elif kind == "consistency":   # emphasize consistency (pos_frac)
            s = 2.0 * pf + dd
        elif kind == "consistency+elig":  # consistency + eligibility persistence
            s = pf + dd + eligfrac
        elif kind == "pretest":       # DENSE pretest-quality rank (in-sample-as-of-k, ~250/fold pool)
            return z(f["pre_roe"].fillna(f["pre_roe"].median())) + z(-f["pre_dd"].fillna(f["pre_dd"].median()))
        elif kind == "pretest_ddonly":   # quality via drawdown only (no return term, live-philosophy)
            return z(-f["pre_dd"].fillna(f["pre_dd"].median()))
        else:
            raise ValueError(kind)
        # fold-1 / no-trailing rows: use pretest fallback so they aren't all tied at the median
        fb = _fold1_fallback(f)
        return s.where(has_trail, fb)
    return score


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ks", default="5,10,20,40")
    ap.add_argument("--rules", default="skill,skill+mean,consistency,consistency+elig")
    ap.add_argument("--b0", type=float, default=500.0)
    ap.add_argument("--min-trail", type=int, default=1)
    args = ap.parse_args()
    ks = [int(x) for x in args.ks.split(",")]
    rules = args.rules.split(",")

    def pct(v):
        return v * 100 if (v == v and abs(v) <= 5) else v

    print(f"V15-native selection+k sweep | b0=${args.b0:.0f} | rules={rules} | ks={ks}")
    print(f"{'rule':>18}{'k':>5}{'chainROE%':>11}{'maxDD%':>9}{'calmar':>8}"
          f"{'+folds':>8}{'topshr%':>9}{'sel/fld':>9}")
    rows = []
    for rule in rules:
        sf = make_score(rule)
        for k in ks:
            try:
                res = FS.forward_backtest(sf, k_select=k, b0=args.b0, min_trail=args.min_trail)
            except Exception as e:
                print(f"{rule:>18}{k:>5}   ERROR {type(e).__name__}: {str(e)[:60]}")
                continue
            roe = pct(res.get("chained_roe", float("nan")))
            mdd = pct(res.get("max_chained_dd", float("nan")))
            calmar = res.get("chained_calmar", float("nan"))
            posf = res.get("n_positive_folds", float("nan"))
            topshr = pct(res.get("top_entity_pnl_share", float("nan")))
            sels = res.get("selections", {})
            avgsel = np.mean(list(sels.values())) if sels else 0
            rows.append({"rule": rule, "k": k, "roe": roe, "mdd": mdd, "calmar": calmar,
                         "posf": posf, "topshr": topshr, "avgsel": avgsel, "raw": res})
            print(f"{rule:>18}{k:>5}{roe:>11.2f}{mdd:>9.2f}{calmar:>8.2f}"
                  f"{posf:>8.0f}{topshr:>9.1f}{avgsel:>9.1f}")
    print("\nraw keys sample:", list(rows[0]["raw"].keys()) if rows else "none")
    print("\nREAD: highest chained_roe at acceptable maxDD wins. If consistency-tilted >= skill across k ->")
    print("  consistency dimension has V15-native support. If low-k >> high-k -> 'copy only the best' wins;")
    print("  if high-k >= low-k -> broad/diversified wins (more-smaller at our capital). codex-review next.")


if __name__ == "__main__":
    main()
