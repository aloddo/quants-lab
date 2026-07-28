#!/usr/bin/env python3
"""Score the LIVE COPY CANDIDATES against the measured profile panel.

Answers, per wallet Alberto is actually considering:
  - where it sits on every profile attribute (percentile vs the sampled universe)
  - what copying it ACTUALLY returned out of sample, after our costs
  - how many held-out folds it was measurable in, and in how many it was positive

Keyed on primary_wallet (an address). NEVER on entity_id, which is a positional
index that is not stable across folds
(findings/quant/2026-07-28-entity-id-positional-index-collides-across-folds).

Costs are already inside r_i (calibrated slippage + real HL fees + 4s latency), so
test_mean_r > 0 IS profitable after costs. Nothing further is subtracted.

Usage:
  python research/v15/copy_candidate_report.py --dir app/data/v15/funnel20k_20260728
"""
from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import numpy as np
import pandas as pd

ATTRS = ["pre_mean_r", "pre_median_r", "pre_win_rate", "median_hold_h", "mae_p90", "liq_rate",
         "clean_close_rate", "ls_balance", "mean_giveback", "mean_time_underwater",
         "mean_underwater_add", "pos_per_day", "n_coins", "median_peak_notional"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True)
    ap.add_argument("--configs", default="config/copy_trader_*_20260726.json")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    d = Path(args.dir)
    out_dir = Path(args.out) if args.out else d

    panel = pd.read_parquet(d / "profile_panel.parquet")
    if "primary_wallet" not in panel.columns:
        raise SystemExit("panel has no primary_wallet - rerun copy_wallet_profile.py")
    panel["pw"] = panel["primary_wallet"].astype(str).str.lower()

    # which config(s) nominated each wallet, and why
    cand: dict[str, list] = {}
    reasons: dict[str, str] = {}
    for f in sorted(glob.glob(args.configs)):
        nm = Path(f).name.replace("copy_trader_", "").replace("_20260726.json", "")
        for w, meta in json.load(open(f))["wallets"].items():
            cand.setdefault(w.lower(), []).append(nm)
            reasons.setdefault(w.lower(), (meta or {}).get("selection", ""))

    # UNIVERSE baseline: what copying an unselected wallet does, after costs
    base = {
        "cells": int(len(panel)),
        "wallets": int(panel["pw"].nunique()),
        "mean_r": float(panel["test_mean_r"].mean()),
        "median_r": float(panel["test_mean_r"].median()),
        "frac_cells_positive": float((panel["test_mean_r"] > 0).mean()),
        "exposure_r": float(panel["test_sum_pnl"].sum() / panel["test_sum_peak"].sum()),
    }
    print("=== UNIVERSE BASELINE (copy an unselected wallet, after our costs) ===")
    print(json.dumps(base, indent=2))

    # per-wallet aggregate across its held-out folds
    g = panel.groupby("pw")
    per = pd.DataFrame({
        "folds": g["fold_id"].nunique(),
        "test_n": g["test_n"].sum(),
        "test_mean_r": g["test_mean_r"].mean(),
        "folds_positive": g["test_mean_r"].apply(lambda s: int((s > 0).sum())),
        "exposure_r": g.apply(lambda x: float(x["test_sum_pnl"].sum() / x["test_sum_peak"].sum())
                              if x["test_sum_peak"].sum() else np.nan, include_groups=False),
    })
    for a in ATTRS:
        if a in panel.columns:
            per[a] = g[a].mean()

    # percentile of each attribute vs the sampled universe (so "is this wallet unusual" is readable)
    pct = per.copy()
    for a in ATTRS:
        if a in per.columns:
            pct[a + "_pct"] = per[a].rank(pct=True) * 100.0

    rows = []
    for w, srcs in sorted(cand.items()):
        if w not in per.index:
            rows.append({"wallet": w, "configs": ",".join(srcs), "status": "NOT MEASURABLE",
                         "note": "no rankable seat in any fold (eligibility/recency gates)"})
            continue
        r = per.loc[w]
        row = {"wallet": w, "configs": ",".join(srcs), "status": "measured",
               "folds": int(r["folds"]), "test_n": int(r["test_n"]),
               "test_mean_r": float(r["test_mean_r"]),
               "folds_positive": int(r["folds_positive"]),
               "exposure_r": float(r["exposure_r"]),
               "beats_baseline": bool(r["test_mean_r"] > base["mean_r"]),
               "profitable": bool(r["test_mean_r"] > 0)}
        for a in ATTRS:
            if a in per.columns:
                row[a] = float(r[a])
                row[a + "_pct"] = float(pct.loc[w, a + "_pct"])
        row["why_selected"] = reasons.get(w, "")[:160]
        rows.append(row)

    rep = pd.DataFrame(rows)
    rep.to_csv(out_dir / "candidate_report.csv", index=False)
    (out_dir / "candidate_baseline.json").write_text(json.dumps(base, indent=2))

    show = [c for c in ["wallet", "configs", "status", "folds", "test_n", "test_mean_r",
                        "exposure_r", "folds_positive", "profitable", "beats_baseline",
                        "pre_mean_r", "liq_rate", "mae_p90", "median_hold_h", "ls_balance"]
            if c in rep.columns]
    print("\n=== LIVE CANDIDATES, OUT OF SAMPLE, AFTER COSTS ===")
    print(rep[show].to_string(index=False))
    if "profitable" in rep.columns:
        m = rep[rep.status == "measured"]
        print(f"\nmeasured: {len(m)}/{len(rep)} | profitable OOS: {int(m['profitable'].sum())} "
              f"| beating the universe baseline: {int(m['beats_baseline'].sum())}")
    print(f"\nwrote {out_dir/'candidate_report.csv'}")


if __name__ == "__main__":
    main()
