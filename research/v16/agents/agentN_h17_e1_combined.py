#!/usr/bin/env python
"""Agent N -- H17 x E1 COMBINED forward (OOS) backtest.

Agent M validated E1 (anti-crowd, low k_opp entry) and H17 (low crossed-share
cohort) independently. This measures the JOINT effect of stacking both filters
on the 18-day forward holdout (2026-05-23..06-10, forward_trades.parquet) -- the
week-2 re-rank candidate (cohort selection H17 x entry filter E1).

DEPLOYABLE H17: crossed_share for each of the 80 forward cohort wallets is
computed from leader fills STRICTLY BEFORE the forward window (90-day train
window 2026-02-22..2026-05-22), i.e. only past data -> no look-ahead. T1
(lowest crossed-share tercile) = H17-favored cohort.

E1: k_opp <= forward-local bottom tercile (native field in forward table).

Memory-safe: fills read ONE DAY FILE AT A TIME, columns [wallet, crossed],
filtered to the 80 cohort wallets, reduced to per-wallet counters immediately.
No giant concats.

Run:
  /Users/hermes/miniforge3/envs/quants-lab/bin/python research/v16/agents/agentN_h17_e1_combined.py
"""
from __future__ import annotations
import glob
import os
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent.parent
sys.path.insert(0, str(_REPO / "research" / "v15"))
sys.path.insert(0, str(_REPO / "research" / "v16"))
try:
    from _streaming_io import install_memory_guard
except Exception:
    def install_memory_guard(**kw):
        pass
from sprint_analysis import wallet_median  # exact reuse

FWD_PQ = _REPO / "app" / "data" / "v16" / "forward_trades.parquet"
FILLS_DIR = _REPO / "app" / "data" / "hl_s3_fills_v2"
REPORT = "/tmp/agentN_h17_e1_combined.md"
TRAIN_LO, TRAIN_HI = "20260222", "20260523"  # [lo, hi) -- strictly before forward start
SPREAD_MIN = 5.0
MIN_BUCKET_TRADES = 50

md: list[str] = []
def emit(s=""):
    md.append(s); print(s)
def fmt(x, nd=1):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return "--"
    return f"{x:.{nd}f}"


def cell(df, mask, col="ov_bps"):
    a = df[mask]
    if len(a) == 0:
        return dict(n=0, mean=float("nan"), win=float("nan"), wmed=float("nan"))
    return dict(n=len(a), mean=a[col].mean(), win=100.0 * (a[col] > 0).mean(),
                wmed=wallet_median(a, col))


def main():
    install_memory_guard()
    fwd = pd.read_parquet(FWD_PQ)
    assert fwd["ov_bps"].notna().all()
    wallets = set(fwd.wallet.unique())
    span = (fwd.entry_ts.max() - fwd.entry_ts.min()) / 86_400_000.0

    # ---- deployable H17: crossed_share from PRE-forward leader fills ----
    files = sorted(glob.glob(str(FILLS_DIR / "*.parquet")))
    train_files = [f for f in files if TRAIN_LO <= Path(f).stem < TRAIN_HI]
    tot, cro = Counter(), Counter()
    for f in train_files:
        d = pd.read_parquet(f, columns=["wallet", "crossed"])
        d = d[d.wallet.isin(wallets)]
        if len(d) == 0:
            continue
        g = d.groupby("wallet")["crossed"].agg(["size", "sum"])
        for w, row in g.iterrows():
            tot[w] += int(row["size"]); cro[w] += int(row["sum"])
    share = {w: (cro[w] / tot[w]) for w in tot if tot[w] >= 50}  # need >=50 fills to rank
    cs = pd.Series(share, name="crossed_share")
    print(f"train window {TRAIN_LO}..{TRAIN_HI} ({len(train_files)} files); "
          f"{len(cs)}/{len(wallets)} wallets rankable (>=50 fills)", file=sys.stderr)

    t1_thr = cs.quantile(1/3)               # H17 favored = lowest tercile
    h17_wallets = set(cs[cs <= t1_thr].index)
    fwd["h17"] = fwd.wallet.isin(h17_wallets)
    # wallets without a crossed_share rank are excluded from the H17 cohort (conservative)

    # ---- E1: k_opp bottom tercile (forward-local) ----
    k_opp_thr = fwd["k_opp"].quantile(1/3)
    fwd["e1"] = fwd["k_opp"] <= k_opp_thr

    base = cell(fwd, pd.Series(True, index=fwd.index))
    h17_only = cell(fwd, fwd["h17"])
    e1_only = cell(fwd, fwd["e1"])
    joint = cell(fwd, fwd["h17"] & fwd["e1"])
    # complement of joint = everything not in both
    rest = cell(fwd, ~(fwd["h17"] & fwd["e1"]))

    emit("# Agent N -- H17 x E1 COMBINED forward (OOS) backtest")
    emit(f"Holdout: {FWD_PQ.name} ({len(fwd)} gated trades, {fwd.wallet.nunique()} wallets, "
         f"{fmt(span)}d, 2026-05-23..06-10). Metric = pooled mean ov_bps (shipped overlay net, "
         "execution_model-priced). H17 cohort from PRE-forward fills (no look-ahead).")
    emit(f"H17 cohort = {len(h17_wallets)} wallets (crossed_share <= {fmt(t1_thr,3)} = bottom "
         f"tercile of {len(cs)} rankable). E1 = k_opp <= {fmt(k_opp_thr)} (bottom tercile).")
    emit("")
    emit("| bucket | n | mean ov_bps | win% | wallet-median | spread vs base |")
    emit("|---|---|---|---|---|---|")
    for label, c in [("BASELINE (all)", base), ("H17 only (low crossed cohort)", h17_only),
                     ("E1 only (anti-crowd entry)", e1_only),
                     ("H17 x E1 (JOINT)", joint), ("rest (not joint)", rest)]:
        sp = c["mean"] - base["mean"] if np.isfinite(c["mean"]) else float("nan")
        emit(f"| {label} | {c['n']} | {fmt(c['mean'])} | {fmt(c['win'])} | "
             f"{fmt(c['wmed'])} | {fmt(sp)} |")
    emit("")
    joint_spread_vs_rest = (joint["mean"] - rest["mean"]) if np.isfinite(joint["mean"]) else float("nan")
    holds = (np.isfinite(joint_spread_vs_rest) and joint_spread_vs_rest >= SPREAD_MIN
             and joint["n"] >= MIN_BUCKET_TRADES)
    emit(f"JOINT vs rest spread = {fmt(joint_spread_vs_rest)} bps (n_joint={joint['n']}). "
         f"Gate (>=5bps, n>=50): **{'HOLDS' if holds else 'FAILS'}**.")
    emit("")
    # incremental value: does stacking H17 add over E1 alone, and vice versa?
    e1_in_h17 = cell(fwd, fwd["h17"] & fwd["e1"])
    e1_out_h17 = cell(fwd, (~fwd["h17"]) & fwd["e1"])
    emit("## Incremental value of stacking")
    emit(f"- E1 entries INSIDE H17 cohort: mean {fmt(e1_in_h17['mean'])} bps (n={e1_in_h17['n']}, "
         f"win {fmt(e1_in_h17['win'])}%).")
    emit(f"- E1 entries OUTSIDE H17 cohort: mean {fmt(e1_out_h17['mean'])} bps (n={e1_out_h17['n']}, "
         f"win {fmt(e1_out_h17['win'])}%).")
    add = (e1_in_h17["mean"] - e1_out_h17["mean"]) if np.isfinite(e1_in_h17["mean"]) and np.isfinite(e1_out_h17["mean"]) else float("nan")
    emit(f"- H17 incremental lift on E1 entries = {fmt(add)} bps. "
         f"{'H17 adds signal on top of E1.' if np.isfinite(add) and add >= 3 else 'H17 adds little/none on top of E1 (E1 may already capture it).'}")
    emit("")
    emit("**Caveat:** single 18d OOS window; joint bucket is the thinnest cell (intersection). "
         "Needs Alberto GO + codex + live confirmation before any cohort change.")

    Path(REPORT).write_text("\n".join(md))
    print(f"\nwrote {REPORT}", file=sys.stderr)


if __name__ == "__main__":
    main()
