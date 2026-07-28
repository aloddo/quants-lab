#!/usr/bin/env python3
"""WHICH WALLET PROFILE COPIES PROFITABLY AFTER OUR COSTS.

Alberto 2026-07-28: "copy trading IS THE ONLY STRATEGY - I don't give a fuck about stat sig - I just
want the right wallet profile to copy live."

So this does NOT emit a pass/fail verdict. It emits: for each observable wallet ATTRIBUTE, what the
copied after-cost return looks like across the attribute's range, ranked by EFFECT SIZE, with
fold-consistency instead of p-values.

CAUSAL DESIGN (the thing that killed the 2026-07-27 screen, not repeated here):
  attributes for fold k are measured ONLY in fold k's PRETEST window [train_start, test_start)
  the outcome for fold k is measured ONLY in fold k's TEST window [test_start, test_end)
  -> a wallet is never scored on the same positions used to describe it, in any fold.
  Pooling is across (entity, fold) cells, so a wallet contributes once per fold, not once overall.

OUTCOME UNITS: r_i = realized_pnl_after_cost / peak_notional, emitted per position by the M7 engine.
Costs (calibrated slippage + real HL fees + 4s latency) are ALREADY INSIDE r_i. So mean r_i > 0 means
profitable AFTER our costs. No further cost subtraction is applied here, and none should be.

Two outcome bases are reported because they answer different questions:
  mean_r      = equal-weighted per position  -> "is the average trade profitable to copy"
  exposure_r  = Σrealized_after_cost / Σpeak -> "is the DOLLAR-weighted book profitable to copy"
Exposure-weighted is the one that pays the bills; equal-weighted is the one that generalizes.

Usage:
  python research/v15/copy_wallet_profile.py --dir app/data/v15/funnel_20260728
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [profile] %(message)s", stream=sys.stdout)
log = logging.getLogger("profile")

MIN_POS_PRETEST = 5     # positions needed in the pretest window to describe a wallet at all
MIN_POS_TEST = 3        # positions needed in the test window to score it
N_BUCKETS = 5


def _read_parts(p: Path) -> pd.DataFrame | None:
    """ShardedParquetWriter may emit either one file or a .parts dir."""
    if p.exists():
        return pd.read_parquet(p)
    parts = sorted(p.parent.glob(p.stem + "*.parquet"))
    parts = [x for x in parts if x != p]
    if not parts:
        d = Path(str(p) + ".parts")
        if d.is_dir():
            parts = sorted(d.glob("*.parquet"))
    if not parts:
        return None
    return pd.concat([pd.read_parquet(x) for x in parts], ignore_index=True)


def describe(pos: pd.DataFrame) -> pd.DataFrame:
    """Per (entity_id, fold_id) ATTRIBUTES from the PRETEST window."""
    pos = pos.copy()
    for c in ("r_i", "mae", "mfe", "mfe_giveback", "underwater_add_ratio", "peak_notional",
              "realized_pnl_after_cost", "n_addon", "n_trim", "time_underwater"):
        if c in pos.columns:
            pos[c] = pd.to_numeric(pos[c], errors="coerce")
    if {"entry_ts", "exit_ts"}.issubset(pos.columns):
        pos["hold_h"] = (pd.to_numeric(pos["exit_ts"], errors="coerce")
                         - pd.to_numeric(pos["entry_ts"], errors="coerce")) / 3_600_000.0
    else:
        pos["hold_h"] = np.nan

    g = pos.groupby(["entity_id", "fold_id"], observed=True)
    out = pd.DataFrame({
        "n_pos": g.size(),
        # --- edge shape ---
        "pre_mean_r": g["r_i"].mean(),
        "pre_median_r": g["r_i"].median(),
        "pre_win_rate": g["r_i"].apply(lambda s: float((s > 0).mean())),
        "pre_r_std": g["r_i"].std(),
        # --- holding behaviour ---
        "median_hold_h": g["hold_h"].median(),
        # --- risk shape (Alberto 2026-07-24: MAE/MFE are crucial) ---
        "mean_mae": g["mae"].mean() if "mae" in pos.columns else np.nan,
        "mean_mfe": g["mfe"].mean() if "mfe" in pos.columns else np.nan,
        "mean_giveback": g["mfe_giveback"].mean() if "mfe_giveback" in pos.columns else np.nan,
        "mean_time_underwater": (g["time_underwater"].mean()
                                 if "time_underwater" in pos.columns else np.nan),
        # --- scaling behaviour (DCA-whale detector) ---
        "mean_underwater_add": (g["underwater_add_ratio"].mean()
                                if "underwater_add_ratio" in pos.columns else np.nan),
        "addon_rate": g["n_addon"].mean() if "n_addon" in pos.columns else np.nan,
        "trim_rate": g["n_trim"].mean() if "n_trim" in pos.columns else np.nan,
        # --- size / concentration ---
        "median_peak_notional": g["peak_notional"].median(),
        "n_coins": g["coin"].nunique() if "coin" in pos.columns else np.nan,
    }).reset_index()
    # trades per day over the wallet's own active span in the window
    span = g["entry_ts"].agg(["min", "max"])
    days = ((pd.to_numeric(span["max"], errors="coerce")
             - pd.to_numeric(span["min"], errors="coerce")) / 86_400_000.0).clip(lower=1.0)
    out["pos_per_day"] = (out["n_pos"].to_numpy() / days.to_numpy())
    return out


def score(pos: pd.DataFrame) -> pd.DataFrame:
    """Per (entity_id, fold_id) OUTCOME from the TEST window."""
    pos = pos.copy()
    for c in ("r_i", "peak_notional", "realized_pnl_after_cost"):
        pos[c] = pd.to_numeric(pos[c], errors="coerce")
    g = pos.groupby(["entity_id", "fold_id"], observed=True)
    out = pd.DataFrame({
        "test_n": g.size(),
        "test_mean_r": g["r_i"].mean(),
        "test_sum_pnl": g["realized_pnl_after_cost"].sum(),
        "test_sum_peak": g["peak_notional"].sum(),
    }).reset_index()
    out["test_exposure_r"] = out["test_sum_pnl"] / out["test_sum_peak"].replace(0, np.nan)
    return out


ATTRS = ["pre_mean_r", "pre_median_r", "pre_win_rate", "pre_r_std", "median_hold_h",
         "mean_mae", "mean_mfe", "mean_giveback", "mean_time_underwater",
         "mean_underwater_add", "addon_rate", "trim_rate",
         "median_peak_notional", "n_coins", "pos_per_day", "n_pos"]


def profile(panel: pd.DataFrame, outcome: str) -> pd.DataFrame:
    """For each attribute: bucket WITHIN FOLD (so regime differences between folds cannot create or
    hide a gradient), then pool. Report the top-minus-bottom spread and how many folds it holds in."""
    rows = []
    for a in ATTRS:
        if a not in panel.columns or panel[a].notna().sum() < 50:
            continue
        d = panel[[a, outcome, "fold_id", "entity_id"]].dropna()
        if len(d) < 50:
            continue
        # within-fold quantile buckets
        def _bucket(s):
            try:
                return pd.qcut(s, N_BUCKETS, labels=False, duplicates="drop")
            except Exception:
                return pd.Series(np.nan, index=s.index)
        d = d.assign(b=d.groupby("fold_id", observed=True)[a].transform(_bucket)).dropna(subset=["b"])
        if d.empty:
            continue
        bmax = d["b"].max()
        pooled = d.groupby("b", observed=True)[outcome].mean()
        if bmax not in pooled.index or 0 not in pooled.index:
            continue
        top, bot = float(pooled.loc[bmax]), float(pooled.loc[0])
        # fold consistency: in how many folds does the top bucket beat the bottom
        per_fold = d.groupby(["fold_id", "b"], observed=True)[outcome].mean().unstack()
        ok = 0
        tot = 0
        if bmax in per_fold.columns and 0 in per_fold.columns:
            cmp = per_fold[[0, bmax]].dropna()
            tot = len(cmp)
            ok = int((cmp[bmax] > cmp[0]).sum())
        rows.append({
            "attribute": a,
            "bottom_bucket_r": bot,
            "top_bucket_r": top,
            "spread": top - bot,
            "abs_spread": abs(top - bot),
            "direction": "higher_is_better" if top > bot else "lower_is_better",
            "folds_consistent": ok,
            "folds_total": tot,
            "consistency": (ok / tot) if tot else np.nan,
            "top_bucket_positive": top > 0,
            "n_cells": len(d),
        })
    return pd.DataFrame(rows).sort_values("abs_spread", ascending=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", required=True, help="funnel run dir (holds m07_pretest/ and m07_test/)")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    d = Path(args.dir)
    out_dir = Path(args.out) if args.out else d

    pre = _read_parts(d / "m07_pretest" / "m07_positions.parquet")
    tst = _read_parts(d / "m07_test" / "m07_positions.parquet")
    if pre is None or tst is None:
        raise SystemExit(f"missing m07_positions (pretest={pre is not None}, test={tst is not None})")
    log.info("pretest positions %d | test positions %d", len(pre), len(tst))

    A = describe(pre)
    B = score(tst)
    A = A[A["n_pos"] >= MIN_POS_PRETEST]
    B = B[B["test_n"] >= MIN_POS_TEST]
    panel = A.merge(B, on=["entity_id", "fold_id"], how="inner")
    log.info("panel cells (entity,fold) with pretest>=%d and test>=%d: %d  (entities %d, folds %s)",
             MIN_POS_PRETEST, MIN_POS_TEST, len(panel), panel["entity_id"].nunique(),
             sorted(panel["fold_id"].unique().tolist()))
    if panel.empty:
        raise SystemExit("empty panel - nothing to profile")

    panel.to_parquet(out_dir / "profile_panel.parquet", index=False)

    # BASELINE: what does copying an unselected wallet do, after costs?
    base = {
        "cells": int(len(panel)),
        "mean_r_all": float(panel["test_mean_r"].mean()),
        "median_r_all": float(panel["test_mean_r"].median()),
        "frac_cells_positive": float((panel["test_mean_r"] > 0).mean()),
        "exposure_r_all": float(panel["test_sum_pnl"].sum() / panel["test_sum_peak"].sum()),
    }
    log.info("BASELINE (copy an unselected wallet): %s", json.dumps(base, indent=2))

    results = {}
    for outcome in ("test_mean_r", "test_exposure_r"):
        p = profile(panel, outcome)
        p.to_csv(out_dir / f"profile_attributes_{outcome}.csv", index=False)
        results[outcome] = p
        log.info("\n=== ATTRIBUTE PROFILE vs %s (ranked by effect size) ===\n%s",
                 outcome, p.to_string(index=False))

    (out_dir / "profile_baseline.json").write_text(json.dumps(base, indent=2))
    log.info("wrote profile_panel.parquet + profile_attributes_*.csv + profile_baseline.json -> %s",
             out_dir)


if __name__ == "__main__":
    main()
