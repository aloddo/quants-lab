#!/usr/bin/env python3
"""Agent M -- OOS forward validation of agent L's 5 robust copy-trading edges.

Validates the 5 LOO-confirmed, mutually-independent edges agent L found IN-SAMPLE
(fold table) on the OUT-OF-SAMPLE 18-day forward holdout (2026-05-23..06-10):

  1. CONSENSUS / ANTI-CROWD  (L21 favored = k_opp bottom-tercile = LOW opposite crowding)
  2. BURST-SIZE              (L25 favored = burst_n >= 5)
  3. FADE-THE-4H-MOVE        (L28 favored = pre4h_signed bottom-tercile = most adverse / fade)
  4. ALT-COIN TILT           (L34/L37 favored = alt = NOT BTC/ETH/SOL/BNB)
  5. TRAIN_N ABOVE-MEDIAN    (L11 favored = train_n >= cohort-median)

Each favored bucket is FIXED by agent L's in-sample direction. NOT re-optimized on the
forward. Pre-registered OOS gate (mirrors L's favored-bucket-vs-rest): favored bucket
vs the rest must show pooled spread >= 5 bps AND >= 50 trades in the favored bucket.

burst_* and pre4h_signed are NOT in the forward table -> enriched here by reusing the
EXACT add_burst / add_prereturns functions from research/v16/sprint_analysis.py, against
the SAME sprint marks dir the forward table itself was priced with
(app/data/v15/assetctx_marks_sprint, covers through 2026-06-11). Read-only on all data.

Run:
  V16_SPRINT_MARKS_DIR=app/data/v15/assetctx_marks_sprint \
  /Users/hermes/miniforge3/envs/quants-lab/bin/python research/v16/agents/agentM_5edge_oos.py
"""
from __future__ import annotations
import os, sys
from pathlib import Path

import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_REPO = _HERE.parent.parent.parent
sys.path.insert(0, str(_REPO / "research" / "v15"))
sys.path.insert(0, str(_REPO / "research" / "v16"))

import leadlag_clean_rank_sim as S
# point marks at the sprint dir BEFORE importing sprint_analysis enrich fns use mark_at
_MD = os.environ.get("V16_SPRINT_MARKS_DIR", str(_REPO / "app" / "data" / "v15" / "assetctx_marks_sprint"))
S.ASSETCTX_DIR = Path(_MD)
from sprint_analysis import add_burst, add_prereturns, wallet_median  # exact enrichment reuse

FWD_PQ = _REPO / "app" / "data" / "v16" / "forward_trades.parquet"
REPORT = "/tmp/agentM_5edge_oos.md"
SPREAD_MIN = 5.0
MIN_BUCKET_TRADES = 50

md: list[str] = []
def emit(s=""):
    md.append(s); print(s)
def fmt(x, nd=1):
    if x is None or (isinstance(x, float) and not np.isfinite(x)):
        return "--"
    return f"{x:.{nd}f}"


def favored_vs_rest(df, mask, col="ov_bps"):
    """Pooled spread (favored minus rest), wallet-median spread, n, win%."""
    a, b = df[mask], df[~mask]
    if len(a) == 0 or len(b) == 0:
        return dict(spread=float("nan"), wmed=float("nan"), n=len(a),
                    mean_fav=float("nan"), mean_rest=float("nan"), win=float("nan"))
    return dict(
        spread=a[col].mean() - b[col].mean(),
        wmed=wallet_median(a, col) - wallet_median(b, col),
        n=len(a),
        mean_fav=a[col].mean(),
        mean_rest=b[col].mean(),
        win=100.0 * (a[col] > 0).mean(),
    )


def main():
    fwd = pd.read_parquet(FWD_PQ)
    assert fwd["ov_bps"].notna().all()
    n0 = len(fwd)
    span_days = (fwd.entry_ts.max() - fwd.entry_ts.min()) / 86_400_000.0

    # ---- enrich the forward table (reuse sprint_analysis fns verbatim) ----
    print(f"marks dir for enrichment: {S.ASSETCTX_DIR}", file=sys.stderr)
    fwd = add_burst(fwd)               # burst_n, burst_before, burst_pos, burst_bucket
    fwd = add_prereturns(fwd)          # pre5m/1h/4h (+_signed); uses S.mark_at on sprint marks
    pre4h_cov = fwd["pre4h_signed"].notna().mean()

    base_mean = fwd["ov_bps"].mean()
    base_wmed = wallet_median(fwd, "ov_bps")

    emit("# Agent M -- OOS forward validation of agent L's 5 robust edges")
    emit(f"OOS holdout: {FWD_PQ} ({n0} gated forward trades, {fwd.wallet.nunique()} wallets, "
         f"{fmt(span_days)}d window 2026-05-23..06-10). This is OUT-OF-SAMPLE vs agent L's "
         "fold1/fold2 (those end 2026-05-17 / 2026-05-23). Edge metric = pooled mean ov_bps "
         "(the SHIPPED overlay net, priced through canonical execution_model).")
    emit(f"Forward baseline (all trades): mean {fmt(base_mean)} bps | wallet-median "
         f"{fmt(base_wmed)} bps | win {fmt(100*(fwd.ov_bps>0).mean())}%.")
    emit("")
    emit("Enrichment of the forward table (fields missing from forward_trades.parquet):")
    emit("- **burst_n / burst_bucket**: rebuilt with `sprint_analysis.add_burst` verbatim "
         "(same-coin+side cohort entries within +/-30min; groups by fold/coin/dir -- the "
         "single 'forward' fold). Pure pandas on the trade table, no marks.")
    emit(f"- **pre4h_signed**: rebuilt with `sprint_analysis.add_prereturns` verbatim "
         f"(4h pre-entry signed return via S.mark_at), against the SAME sprint marks the "
         f"forward table was priced with: `{S.ASSETCTX_DIR.name}` (covers through 2026-06-11, "
         f"the default assetctx_marks only reaches 2026-06-01). Coverage on forward rows: "
         f"{fmt(100*pre4h_cov)}%.")
    emit("- consensus (k_same/k_opp/k30_same), train_n, train_taker, rank, coin, dir: native "
         "in the forward table (computed by forward_test.py with the same conventions).")
    emit("")
    emit("Pre-registered OOS gate (favored bucket FIXED by L's in-sample direction, NOT "
         "re-optimized on forward): favored-bucket-vs-rest pooled spread >= 5 bps AND >= 50 "
         "trades in the favored bucket. Reporting spread, n, win%, wmed-spread, HOLDS/FAILS.")

    # ---- in-sample reference (from agent L, both folds) ----
    # edge -> (label, in-sample fold1 spread, fold2 spread, favored-mask builder)
    k_opp_thr = fwd["k_opp"].quantile(1/3)            # forward-local tercile threshold
    pre4h_thr = fwd["pre4h_signed"].quantile(1/3)
    trainn_med = fwd.groupby("wallet")["train_n"].first().median()
    LARGE = ["BTC", "ETH", "SOL", "BNB"]

    edges = [
        dict(eid="E1", name="CONSENSUS / ANTI-CROWD (k_opp bottom-tercile = low opposite crowding)",
             src="L21", is1=42.2, is2=21.1,
             mask=fwd["k_opp"] <= k_opp_thr,
             favdesc=f"k_opp <= {fmt(k_opp_thr)} (fewest opposing leaders)"),
        dict(eid="E2", name="BURST-SIZE (burst_n >= 5)",
             src="L25", is1=105.9, is2=43.7,
             mask=fwd["burst_n"] >= 5,
             favdesc="burst_n >= 5 (entry inside a 5+ leader burst)"),
        dict(eid="E3", name="FADE-THE-4H-MOVE (pre4h_signed bottom-tercile = most adverse)",
             src="L28", is1=27.2, is2=15.9,
             mask=fwd["pre4h_signed"] <= pre4h_thr,
             favdesc=f"pre4h_signed <= {fmt(pre4h_thr)} bps (entering against prior 4h move)"),
        dict(eid="E4", name="ALT-COIN TILT (not BTC/ETH/SOL/BNB)",
             src="L34/L37", is1=14.1, is2=17.3,
             mask=~fwd["coin"].isin(LARGE),
             favdesc="alt coin (not BTC/ETH/SOL/BNB)"),
        dict(eid="E5", name="TRAIN_N ABOVE-MEDIAN (train_n >= cohort-median)",
             src="L11", is1=14.6, is2=9.8,
             mask=fwd["train_n"] >= trainn_med,
             favdesc=f"train_n >= {fmt(trainn_med)} (median)"),
    ]

    emit("\n# Per-edge OOS results\n")
    emit("| edge | src | favored bucket | IS fold1 | IS fold2 | OOS spread | OOS n(fav) | "
         "OOS win% | OOS wmed-spread | verdict |")
    emit("|---|---|---|---|---|---|---|---|---|---|")
    survivors, failures = [], []
    detail_blocks = []
    for e in edges:
        # E3 mask may contain NaN (no pre4h coverage) -> treat NaN as not-favored, and
        # restrict the analysis to rows with a defined pre4h for a fair comparison.
        mask = e["mask"]
        df_use = fwd
        if e["eid"] == "E3":
            cov = fwd["pre4h_signed"].notna()
            df_use = fwd[cov]
            mask = (df_use["pre4h_signed"] <= pre4h_thr)
        else:
            mask = mask.reindex(df_use.index)
        r = favored_vs_rest(df_use, mask.astype(bool))
        holds = (np.isfinite(r["spread"]) and r["spread"] >= SPREAD_MIN
                 and r["n"] >= MIN_BUCKET_TRADES)
        verdict = "HOLDS" if holds else "FAILS"
        (survivors if holds else failures).append(e["eid"])
        emit(f"| {e['eid']} {e['name'].split('(')[0].strip()} | {e['src']} | {e['favdesc']} | "
             f"{fmt(e['is1'])} | {fmt(e['is2'])} | {fmt(r['spread'])} | {r['n']} | "
             f"{fmt(r['win'])} | {fmt(r['wmed'])} | **{verdict}** |")
        detail_blocks.append((e, r, holds, len(df_use)))

    # ---- detailed per-edge narrative ----
    emit("\n# Detail per edge\n")
    for e, r, holds, nuse in detail_blocks:
        emit(f"## {e['eid']} {e['name']}  [{e['src']}]")
        emit(f"- In-sample (agent L): fold1 spread = {fmt(e['is1'])} bps, fold2 spread = "
             f"{fmt(e['is2'])} bps. Favored bucket fixed: {e['favdesc']}.")
        emit(f"- OOS forward: favored mean = {fmt(r['mean_fav'])} bps (n={r['n']}), rest mean = "
             f"{fmt(r['mean_rest'])} bps. **spread = {fmt(r['spread'])} bps**; "
             f"wmed-spread = {fmt(r['wmed'])} bps; favored win% = {fmt(r['win'])}.")
        if e["eid"] == "E3":
            emit(f"- (analysis on {nuse} forward rows with defined pre4h_signed)")
        gate = []
        gate.append(f"spread>=5: {'OK' if np.isfinite(r['spread']) and r['spread']>=5 else 'NO'} "
                    f"({fmt(r['spread'])})")
        gate.append(f"n(fav)>=50: {'OK' if r['n']>=50 else 'NO'} ({r['n']})")
        emit(f"- Gate: {' | '.join(gate)} -> **{'HOLDS OOS' if holds else 'FAILS OOS'}**")
        emit("")

    emit("# Verdict\n")
    emit(f"- **OOS-CONFIRMED (held the forward gate): {survivors}**")
    emit(f"- **FAILED OOS (drop): {failures}**")
    emit("")
    emit("**Honest caveat:** the forward is a SINGLE 18-day window (n=977 gated trades, 80 "
         "wallets), with some favored buckets (notably burst_n>=5) thin. Survivors are "
         "STRONGER candidates -- they held a clean out-of-sample test that the in-sample "
         "folds could not provide -- but a single forward window is not proof of a durable "
         "edge. Before any live weighting they still need (a) live confirmation and (b) "
         "re-pricing through execution_model.py at live latency/slippage. Failures are a "
         "valuable KILL: they prevent overfitting the in-sample folds into the live cohort.")
    emit("")
    week2 = ", ".join(survivors) if survivors else "(none)"
    emit(f"OOS-CONFIRMED edges = {survivors} | failed = {failures} | week-2 filters = [{week2}]")

    Path(REPORT).write_text("\n".join(md) + "\n")
    print(f"\n[done] -> {REPORT}", file=sys.stderr)
    print(f"[done] survivors={survivors} failed={failures}", file=sys.stderr)


if __name__ == "__main__":
    main()
