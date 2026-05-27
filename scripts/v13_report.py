#!/usr/bin/env python3
"""V13 Script 5/5 (v3): Strategy report.

Per projects/quant/v13 Section 6.3 + 6.8.

v3 verdict logic (Alberto decision 2026-05-24: "let's go with the spec"):

Spec Section 6.3 has TWO tiers, NOT strict per-fold-per-row:

  AGGREGATE OOS (criteria 1-6, pooled across all 8 test windows):
    1. Aggregate Sharpe > 1.5
    2. Aggregate random-portfolio percentile rank >= 95th on BOTH Sharpe + PnL
    3. Aggregate net PnL beats USDC/BTC/ETH/HYPE/HL_index/alt/momentum/V12
    4. Aggregate fee drag < 30% of gross PnL
    5. Returns survive K-aware top-N removal (still profitable across folds)
    6. Latency sensitivity (worst poll Sharpe >= 0.7 * best poll Sharpe)

  FOLD-LEVEL (F1-F4, partial failures allowed):
    F1. Profitable folds >= 6 of 8 (generalized: >= ceil(0.75 * n_folds))
    F2. Most recent fold profitable
    F3. NO fold max DD > 25%
    F4. NO fold worst single-day < -10%

Spec rationale (verbatim): "15-day test windows are noisy enough that demanding
every fold pass every threshold over-rejects on noise. The aggregate gate plus
the 6-of-8 fold-level threshold accepts strategies that work most of the time
but allows one or two weak folds without auto-failing."

Aggregate computation REQUIRES walk_forward's per-fold daily-returns and full
random-sample outputs (see walk_forward --daily-returns-output and
--random-samples-output). mean-of-Sharpes != pooled Sharpe.

Pending enforcement (unchanged from v2):
- deployment_blocked_by_near_liq forces FAIL until margin-ratio data or override
- Ablation completeness issues force FAIL
- Price interval = "1h" forces FAIL via cadence-not-validated blocker

Inputs:
    --walk-forward-results <path>    walk_forward_results.parquet (per-fold scalars)
    --daily-returns <path>           walk_forward_daily_returns.parquet (per-(fold, day))
    --random-samples <path>          walk_forward_random_samples.parquet (per-(fold, trial))
    --wallet-metrics <path>          wallet_metrics.parquet (latest fold)
    --ablation-results <path>        ablation_results.parquet
    --price-interval                 "1m" or "1h"
    --output <path>                  app/data/v13/strategy_report.md
"""
from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "strategy_report.md"

# Section 6.3 pass/fail thresholds (per-fold).
PASS_THRESHOLDS = {
    "sharpe_min": 1.5,
    "max_dd_max_pct": 25.0,
    "worst_day_max_pct_abs": 10.0,
    "random_sharpe_pct_min": 95.0,
    "random_pnl_pct_min": 95.0,
    "fee_drag_max_frac": 0.30,
    "robust_remove_min_sharpe": 0.0,
    "latency_loss_max_frac": 0.30,
}


def _safe_float(x) -> float | None:
    """Convert to float; return None for NaN / None / pd.NA / unparseable."""
    if x is None:
        return None
    try:
        # pd.isna catches NaN, pd.NA, NaT, None uniformly.
        if pd.isna(x):
            return None
    except (TypeError, ValueError):
        pass
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v != v:
        return None
    return v


def _is_strict_true(x) -> bool:
    """True if and only if value is literally Python True or numpy True.
    Rejects 1, 1.0, "True", and all non-boolean values."""
    if x is None:
        return False
    try:
        if pd.isna(x):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(x, bool):
        return x is True
    if isinstance(x, np.bool_):
        return bool(x) is True
    return False


def _evaluate_fold_caps(row: dict) -> dict:
    """Section 6.3 FOLD-LEVEL caps (F3 + F4 only): NO fold may exceed these.

    Per spec 6.3 the per-fold checks are F3 max-DD<25 and F4 worst-day<10
    enforced as HARD CAPS across folds. F1 (profitable count) and F2
    (latest profitable) are evaluated separately at the corpus level. The
    aggregate criteria 1-6 are evaluated by _evaluate_aggregate on pooled
    daily returns + random samples.

    A row with non-ok status is auto-fail on caps (cannot verify).

    NaN = missing = FAIL.
    """
    fails: list[str] = []
    if row.get("status") != "ok":
        fails.append(f"fold_status:{row.get('status')}")
        return {"fold": row.get("fold"), "all_pass": False, "fails": fails}

    # F3: max DD <= 25%
    max_dd = _safe_float(row.get("test_max_dd_pct"))
    if max_dd is None:
        fails.append("max_dd_missing")
    elif max_dd > PASS_THRESHOLDS["max_dd_max_pct"]:
        fails.append(f"max_dd_exceeded({max_dd:.1f}>{PASS_THRESHOLDS['max_dd_max_pct']})")

    # F4: worst single day <= 10% (absolute)
    worst_day_raw = _safe_float(row.get("test_worst_day_pct"))
    if worst_day_raw is None:
        fails.append("worst_day_missing")
    elif abs(worst_day_raw) > PASS_THRESHOLDS["worst_day_max_pct_abs"]:
        fails.append(f"worst_day_exceeded({abs(worst_day_raw):.1f}>{PASS_THRESHOLDS['worst_day_max_pct_abs']})")

    # Latest-fold dedicated check (F2): latest fold profitable.
    if _is_strict_true(row.get("is_latest_fold")):
        if not _is_strict_true(row.get("latest_fold_profitable")):
            fails.append("latest_fold_not_profitable")

    # Row 9: latency sensitivity (cadence sweep must remain robust).
    # ENFORCED via ablation #5 (latency_cadence) at the ablation-completeness
    # layer below. Walk_forward emits latency variants as ablation rows.

    return {"fold": row.get("fold"), "all_pass": len(fails) == 0, "fails": fails}


def _evaluate_aggregate(wf_sorted: pd.DataFrame,
                        daily_returns_df: pd.DataFrame | None,
                        random_samples_df: pd.DataFrame | None) -> dict:
    """Spec 6.3 AGGREGATE OOS evaluation (criteria 1-6, pooled across all
    test windows).

    Returns dict with per-criterion pass/fail + value + missing reasons. The
    overall aggregate verdict is the AND of criteria 1, 2 (Sharpe + PnL gates),
    and 4. Criteria 3 (beats benchmarks), 5 (top-N removal), 6 (latency) are
    enforced through ablation-completeness (see main).

    Missing pool inputs are reported as MISSING and FAIL. Criterion 4 falls
    back to mean of per-fold fee_drag (currently a 0.30 placeholder in
    walk_forward; flagged as APPROXIMATED when not derived from pooled fees).
    """
    out: dict = {
        "aggregate_sharpe": None,
        "aggregate_sharpe_pass": False,
        "aggregate_random_sharpe_p95": None,
        "aggregate_random_pnl_p95": None,
        "aggregate_sharpe_pct_rank": None,
        "aggregate_pnl_pct_rank": None,
        "aggregate_random_pass": False,
        "aggregate_fee_drag": None,
        "aggregate_fee_drag_pass": False,
        "aggregate_fee_drag_source": "missing",
        "aggregate_net_pnl_pct": None,
        "missing": [],
    }

    # ── Aggregate Sharpe (criterion 1) ─────────────────────────────────
    if daily_returns_df is None or daily_returns_df.empty:
        out["missing"].append("daily_returns")
    else:
        # Pool daily returns across all 8 folds in chronological order. We
        # treat each fold's test window as a contiguous block (per spec
        # rationale: each test is independent OOS evaluation). Compute one
        # aggregate Sharpe on the pooled series.
        pooled = daily_returns_df.sort_values(["fold", "day_idx"])["daily_return"].astype(float)
        pooled_clean = pooled.replace([np.inf, -np.inf], np.nan).dropna()
        if pooled_clean.empty or pooled_clean.std() == 0:
            out["missing"].append("aggregate_sharpe_undefined")
        else:
            agg_sh = float(pooled_clean.mean() / pooled_clean.std() * math.sqrt(365))
            out["aggregate_sharpe"] = agg_sh
            out["aggregate_sharpe_pass"] = agg_sh >= PASS_THRESHOLDS["sharpe_min"]
            # Aggregate net PnL: compound the pooled daily returns.
            out["aggregate_net_pnl_pct"] = float(((1.0 + pooled_clean).prod() - 1.0) * 100.0)

    # ── Aggregate random p95 (criterion 2) ────────────────────────────
    if random_samples_df is None or random_samples_df.empty:
        out["missing"].append("random_samples")
    elif out["aggregate_sharpe"] is None or out["aggregate_net_pnl_pct"] is None:
        # Cannot compare without an aggregate Sharpe + PnL value.
        out["missing"].append("aggregate_sharpe_for_random_rank")
    else:
        # Pool random sharpes + pnls across all folds. The aggregate gate
        # is: strategy aggregate Sharpe + aggregate PnL must both rank >=
        # 95th vs the pooled random distribution. We approximate the pooled
        # random distribution as a per-fold compounded series per trial
        # index, falling back to raw concatenation when trial indices
        # disagree across folds.
        rs = random_samples_df.copy()
        # Try per-trial compounding: for each (trial), sum (or compound) the
        # random pnls across folds; for Sharpe we use trial-by-trial mean of
        # per-fold sharpes (no pooled daily series for randoms is emitted).
        per_trial = rs.groupby("trial").agg({"random_sharpe": "mean", "random_pnl_pct": "sum"})
        if per_trial.empty:
            out["missing"].append("random_samples_empty")
        else:
            sh_p95 = float(per_trial["random_sharpe"].quantile(0.95))
            pn_p95 = float(per_trial["random_pnl_pct"].quantile(0.95))
            out["aggregate_random_sharpe_p95"] = sh_p95
            out["aggregate_random_pnl_p95"] = pn_p95
            # Rank: percentile of strategy aggregate vs the per-trial distribution.
            sh_sorted = per_trial["random_sharpe"].sort_values().values
            pn_sorted = per_trial["random_pnl_pct"].sort_values().values
            sh_rank = float(np.searchsorted(sh_sorted, out["aggregate_sharpe"]) / max(1, len(sh_sorted)) * 100)
            pn_rank = float(np.searchsorted(pn_sorted, out["aggregate_net_pnl_pct"]) / max(1, len(pn_sorted)) * 100)
            out["aggregate_sharpe_pct_rank"] = sh_rank
            out["aggregate_pnl_pct_rank"] = pn_rank
            out["aggregate_random_pass"] = (sh_rank >= PASS_THRESHOLDS["random_sharpe_pct_min"]
                                            and pn_rank >= PASS_THRESHOLDS["random_pnl_pct_min"])

    # ── Aggregate fee drag (criterion 4) ──────────────────────────────
    # Pooled fee_drag = sum(fees + slippage across all folds) / sum(gross_pnl
    # across all folds). Walk_forward v3 emits per-fold test_fees_usd +
    # test_slippage_usd + test_gross_pnl_usd accumulated from the simulator's
    # per-minute fees_minute / slippage_minute. Falls back to mean of per-fold
    # fee_drag values when the new cost columns are absent (older walk_forward
    # outputs). When pooled gross PnL is non-positive the gate is undefined and
    # set to 1.0 (conservative FAIL).
    has_cost_columns = all(c in wf_sorted.columns for c in (
        "test_fees_usd", "test_slippage_usd", "test_gross_pnl_usd"
    ))
    if has_cost_columns:
        fees_total = float(pd.to_numeric(wf_sorted["test_fees_usd"], errors="coerce").fillna(0).sum())
        slip_total = float(pd.to_numeric(wf_sorted["test_slippage_usd"], errors="coerce").fillna(0).sum())
        gross_total = float(pd.to_numeric(wf_sorted["test_gross_pnl_usd"], errors="coerce").fillna(0).sum())
        if gross_total > 1e-9:
            out["aggregate_fee_drag"] = (fees_total + slip_total) / gross_total
        else:
            out["aggregate_fee_drag"] = 1.0  # gross non-positive -> conservative FAIL
        out["aggregate_fee_drag_pass"] = out["aggregate_fee_drag"] < PASS_THRESHOLDS["fee_drag_max_frac"]
        out["aggregate_fee_drag_source"] = "pooled_real"
    else:
        fee_drag_col = wf_sorted.get("fee_drag")
        if fee_drag_col is not None and not fee_drag_col.dropna().empty:
            out["aggregate_fee_drag"] = float(fee_drag_col.dropna().mean())
            out["aggregate_fee_drag_pass"] = out["aggregate_fee_drag"] < PASS_THRESHOLDS["fee_drag_max_frac"]
            out["aggregate_fee_drag_source"] = "mean_of_per_fold_legacy"
        else:
            out["missing"].append("fee_drag")

    return out


def _evaluate_fold_corpus(fold_evals: list, wf_sorted: pd.DataFrame) -> dict:
    """Spec 6.3 FOLD-LEVEL corpus checks: F1 (>=6 of 8 profitable), F2
    (latest profitable -- captured in fold caps), F3+F4 (per-fold caps,
    NO fold may exceed -- captured in fold caps).

    F1: count folds whose test_net_return_pct > 0. Generalize 6-of-8 to
    >= ceil(0.75 * n_folds) so non-default fold counts behave sensibly.
    """
    out: dict = {
        "n_folds_attempted": len(fold_evals),
        "n_folds_profitable": 0,
        "f1_threshold": 0,
        "f1_pass": False,
        "f3_f4_pass": False,
        "f3_f4_violators": [],
    }
    if not fold_evals:
        return out

    # F1: profitable folds count.
    if "test_net_return_pct" in wf_sorted.columns:
        n_prof = int((wf_sorted["test_net_return_pct"].astype(float) > 0).sum())
    else:
        n_prof = 0
    out["n_folds_profitable"] = n_prof
    n_attempted = len(fold_evals)
    threshold = max(1, math.ceil(n_attempted * 0.75))
    out["f1_threshold"] = threshold
    out["f1_pass"] = n_prof >= threshold

    # F3 + F4: no fold may violate. fold_evals carry per-fold cap fails.
    violators = [(f["fold"], f["fails"]) for f in fold_evals if not f["all_pass"]]
    out["f3_f4_violators"] = violators
    out["f3_f4_pass"] = len(violators) == 0

    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--walk-forward-results", required=True)
    ap.add_argument("--daily-returns", required=False,
                    help="walk_forward_daily_returns.parquet from walk_forward "
                         "--daily-returns-output. Required for spec 6.3 "
                         "aggregate Sharpe (criteria 1).")
    ap.add_argument("--random-samples", required=False,
                    help="walk_forward_random_samples.parquet from walk_forward "
                         "--random-samples-output. Required for spec 6.3 "
                         "aggregate random p95 (criteria 2).")
    ap.add_argument("--wallet-metrics", required=False)
    ap.add_argument("--ablation-results", required=False)
    ap.add_argument("--price-interval", default="1m", choices=["1m", "1h"],
                    help="Production default is 1m (S3-reconstructed). 1h "
                         "is a fallback that forces overall FAIL via the "
                         "cadence-not-validated pending blocker.")
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    wf = pd.read_parquet(args.walk_forward_results)
    if wf.empty:
        print("walk_forward_results is empty; cannot generate report")
        return

    # Sort by fold index (walk_forward emits folds in chronological order, so
    # fold_idx == chronological position). This handles failed folds cleanly:
    # they sit at their natural position in the sequence regardless of whether
    # test_end was populated.
    if "fold" in wf.columns:
        wf_sorted = wf.sort_values("fold", kind="stable").reset_index(drop=True)
    else:
        wf_sorted = wf.reset_index(drop=True)
    wf_sorted["is_latest_fold"] = False
    if len(wf_sorted) > 0:
        wf_sorted.loc[wf_sorted.index[-1], "is_latest_fold"] = True

    # Per-fold CAP-only evaluation (F3 + F4 only, plus F2 latest-profitable
    # tagged on the latest-fold row). The strict per-fold-per-row evaluation
    # is REMOVED per Alberto decision 2026-05-24 to match spec 6.3 (which
    # warns "demanding every fold pass every threshold over-rejects on
    # noise"). Aggregate criteria 1-6 are evaluated separately on pooled
    # inputs by _evaluate_aggregate.
    fold_evals = [_evaluate_fold_caps(r._asdict()) for r in wf_sorted.itertuples(index=False)]
    n_fold_attempted = len(fold_evals)

    # Latest-fold profitable (F2) is checked via the cap-evaluator's
    # latest_fold_not_profitable fail string on the latest-fold row.
    latest_fold_eval = fold_evals[-1] if fold_evals else None
    latest_fold_pass = bool(latest_fold_eval and latest_fold_eval["all_pass"])

    # Load aggregate-evaluation inputs (per-fold daily returns, random samples).
    daily_returns_df = None
    random_samples_df = None
    if args.daily_returns and Path(args.daily_returns).exists():
        try:
            daily_returns_df = pd.read_parquet(args.daily_returns)
        except Exception as e:
            print(f"WARN: failed to read --daily-returns: {e}")
    if args.random_samples and Path(args.random_samples).exists():
        try:
            random_samples_df = pd.read_parquet(args.random_samples)
        except Exception as e:
            print(f"WARN: failed to read --random-samples: {e}")

    agg = _evaluate_aggregate(wf_sorted, daily_returns_df, random_samples_df)
    fold_corpus = _evaluate_fold_corpus(fold_evals, wf_sorted)

    # Aggregate verdict (criteria 1, 2, 4 evaluated here; 3/5/6 via ablation).
    aggregate_pass = (agg["aggregate_sharpe_pass"]
                      and agg["aggregate_random_pass"]
                      and agg["aggregate_fee_drag_pass"])
    fold_level_pass = fold_corpus["f1_pass"] and fold_corpus["f3_f4_pass"]
    n_fold_pass = fold_corpus["n_folds_profitable"]

    # Pending-analyses: ablations + near-liq + price-interval approximation.
    pending_reasons: list[str] = []

    # 9 ablations: enforce completeness. Must have:
    # - file present + non-empty + correct schema
    # - every WALK-FORWARD fold has all 9 ablation experiments
    # - every status == "pass"
    abl_path = Path(args.ablation_results) if args.ablation_results else None
    ABLATION_NAMES = {"top_vs_random", "top_vs_beta", "K_sensitivity",
                      "remove_top_1_5_10", "latency_cadence",
                      "fees_multiplier", "slippage_multiplier",
                      "consensus_off_soft_hard40", "weighting_equal_vs_score"}
    expected_folds = set(wf_sorted["fold"].tolist()) if "fold" in wf_sorted.columns else set()
    ablation_issues: list[str] = []
    if not abl_path or not abl_path.exists():
        ablation_issues.append("file_missing")
    else:
        abl = pd.read_parquet(abl_path)
        if abl.empty:
            ablation_issues.append("file_empty")
        else:
            required_cols = {"fold", "experiment", "status"}
            if not required_cols.issubset(set(abl.columns)):
                ablation_issues.append(f"schema_missing_cols:{required_cols - set(abl.columns)}")
            else:
                # Cross-reference: every walk-forward fold must appear; no extras allowed.
                abl_folds = set(abl["fold"].unique().tolist())
                missing_folds = expected_folds - abl_folds
                extra_folds = abl_folds - expected_folds
                if missing_folds:
                    ablation_issues.append(f"folds_with_no_ablations:{sorted(missing_folds)}")
                if extra_folds:
                    ablation_issues.append(f"unexpected_extra_folds:{sorted(extra_folds)}")
                # Duplicate (fold, experiment, variant) check. Each experiment
                # legitimately emits multiple variant rows (K=5, K=10, etc.),
                # so we dedup on the full triple, not just (fold, experiment).
                dup_keys = ["fold", "experiment", "variant"] if "variant" in abl.columns else ["fold", "experiment"]
                dup_mask = abl.duplicated(subset=dup_keys, keep=False)
                if dup_mask.any():
                    dups = abl[dup_mask][dup_keys].drop_duplicates().values.tolist()
                    ablation_issues.append(f"duplicate_rows:{dups[:5]}")
                # Extra (non-canonical) experiment names anywhere.
                all_exp_names = set(abl["experiment"].unique().tolist())
                extra_exps = all_exp_names - ABLATION_NAMES
                if extra_exps:
                    ablation_issues.append(f"unexpected_exp_names:{sorted(extra_exps)}")
                # Per-fold completeness + per-row pass status (scan ALL rows, not just expected folds).
                for fi in expected_folds:
                    grp = abl[abl["fold"] == fi]
                    have_exps = set(grp["experiment"].unique())
                    missing_exps = ABLATION_NAMES - have_exps
                    if missing_exps:
                        ablation_issues.append(f"fold{fi}_missing_exps:{sorted(missing_exps)}")
                # Scan ALL rows for status != "pass". Fail-closed on NaN:
                # use ~eq("pass").fillna(False), then invert via "not eq".
                non_pass_mask = ~abl["status"].eq("pass").fillna(False)
                non_pass = abl[non_pass_mask]
                if not non_pass.empty:
                    for _, r in non_pass.iterrows():
                        st = r["status"]
                        ablation_issues.append(f"fold{r['fold']}_exp_{r['experiment']}_status:{st if not pd.isna(st) else 'NA'}")
    if ablation_issues:
        pending_reasons.append(f"ablations_incomplete:{len(ablation_issues)}_issues")

    # deployment_blocked_by_near_liq: OR across BOTH walk_forward + wallet_metrics.
    near_liq_blocked = False
    if "deployment_blocked_by_near_liq" in wf_sorted.columns:
        near_liq_blocked = near_liq_blocked or bool(wf_sorted["deployment_blocked_by_near_liq"].any())
    if args.wallet_metrics and Path(args.wallet_metrics).exists():
        wm = pd.read_parquet(args.wallet_metrics)
        if "deployment_blocked_by_near_liq" in wm.columns:
            near_liq_blocked = near_liq_blocked or bool(wm["deployment_blocked_by_near_liq"].any())
    if near_liq_blocked:
        pending_reasons.append("near_liq_partial_coverage")

    # Price interval approximation: 1h cannot validate sub-hour rules.
    if args.price_interval == "1h":
        pending_reasons.append("price_interval_1h_cadence_unvalidated")

    # Overall verdict per spec 6.3 two-tier structure.
    overall_pass = aggregate_pass and fold_level_pass and latest_fold_pass and len(pending_reasons) == 0

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        f.write(f"# V13 Backtest Report\n\n")
        f.write(f"Walk-forward source: `{args.walk_forward_results}`\n\n")
        f.write(f"Folds attempted: {n_fold_attempted}\n")
        f.write(f"Folds profitable (test net PnL > 0): {n_fold_pass} of {n_fold_attempted} (F1 threshold: >= {fold_corpus['f1_threshold']})\n\n")

        f.write("## Verdict (spec Section 6.3)\n\n")
        f.write(f"**Overall: {'PASS' if overall_pass else 'FAIL'}**\n\n")

        # Aggregate tier (criteria 1-6 across all test windows POOLED).
        f.write("### Aggregate OOS (criteria 1-6, pooled across all test windows)\n\n")
        def _fmt(v, digits=2):
            if v is None:
                return "MISSING"
            try:
                return f"{float(v):.{digits}f}"
            except Exception:
                return str(v)
        f.write(f"- 1. Aggregate Sharpe = {_fmt(agg['aggregate_sharpe'])} (threshold > {PASS_THRESHOLDS['sharpe_min']}): {'PASS' if agg['aggregate_sharpe_pass'] else 'FAIL'}\n")
        f.write(f"- 2. Aggregate random p95 rank: Sharpe = {_fmt(agg['aggregate_sharpe_pct_rank'])} pct, PnL = {_fmt(agg['aggregate_pnl_pct_rank'])} pct (both must >= {PASS_THRESHOLDS['random_sharpe_pct_min']}): {'PASS' if agg['aggregate_random_pass'] else 'FAIL'}\n")
        f.write(f"- 3. Aggregate net PnL beats USDC/BTC/ETH/HYPE/HL_index/alt/momentum/V12: enforced via ablation #2 (top_vs_beta). Aggregate net PnL = {_fmt(agg['aggregate_net_pnl_pct'])} %. See ablation completeness section.\n")
        f.write(f"- 4. Aggregate fee drag = {_fmt(agg['aggregate_fee_drag'])} (threshold < {PASS_THRESHOLDS['fee_drag_max_frac']}): {'PASS' if agg['aggregate_fee_drag_pass'] else 'FAIL'}")
        if agg["aggregate_fee_drag_source"] == "mean_of_per_fold_legacy":
            f.write(" -- LEGACY (per-fold cost columns absent; using mean of per-fold scalar fee_drag from older walk_forward; re-run walk_forward for true pooled aggregate)")
        elif agg["aggregate_fee_drag_source"] == "pooled_real":
            f.write(" -- POOLED REAL (sum_folds(fees + slip) / sum_folds(gross_pnl) from simulator-tracked per-minute costs)")
        f.write("\n")
        f.write(f"- 5. Top-N removal still profitable: enforced via ablation #4 (remove_top_1_5_10). See ablation completeness section.\n")
        f.write(f"- 6. Latency sensitivity (worst poll Sharpe >= 0.7 * best poll Sharpe): enforced via ablation #5 (latency_cadence). See ablation completeness section.\n")
        if agg["missing"]:
            f.write(f"- MISSING aggregate inputs: {agg['missing']}\n")
        f.write("\n")

        # Fold-level tier (F1-F4, partial failures allowed within bounds).
        f.write("### Fold-level (F1-F4, per-fold caps and corpus checks)\n\n")
        f.write(f"- F1. Profitable folds: {fold_corpus['n_folds_profitable']} of {fold_corpus['n_folds_attempted']} (threshold >= {fold_corpus['f1_threshold']}, generalizes spec 6-of-8): {'PASS' if fold_corpus['f1_pass'] else 'FAIL'}\n")
        f.write(f"- F2. Latest fold profitable + caps: {'PASS' if latest_fold_pass else 'FAIL'}\n")
        if fold_corpus["f3_f4_violators"]:
            f.write(f"- F3+F4. Per-fold caps (max DD <= 25%, worst day <= 10%): FAIL on {len(fold_corpus['f3_f4_violators'])} fold(s):\n")
            for fold, fails in fold_corpus["f3_f4_violators"]:
                f.write(f"  - fold {fold}: {', '.join(fails)}\n")
        else:
            f.write(f"- F3+F4. Per-fold caps (max DD <= 25%, worst day <= 10%): PASS (no fold violators)\n")
        f.write("\n")

        if not overall_pass:
            f.write("### FAIL reasons summary\n\n")
            if not aggregate_pass:
                f.write(f"- aggregate_tier: ")
                agg_fails = []
                if not agg["aggregate_sharpe_pass"]:
                    agg_fails.append("sharpe")
                if not agg["aggregate_random_pass"]:
                    agg_fails.append("random_p95")
                if not agg["aggregate_fee_drag_pass"]:
                    agg_fails.append("fee_drag")
                f.write(f"{', '.join(agg_fails)}\n")
            if not fold_corpus["f1_pass"]:
                f.write(f"- F1 profitable-folds: {fold_corpus['n_folds_profitable']}/{fold_corpus['n_folds_attempted']} < {fold_corpus['f1_threshold']}\n")
            if not fold_corpus["f3_f4_pass"]:
                f.write(f"- F3/F4: {len(fold_corpus['f3_f4_violators'])} fold(s) violate per-fold caps\n")
            if not latest_fold_pass:
                fails = latest_fold_eval["fails"] if latest_fold_eval else ["no_folds"]
                f.write(f"- F2 latest-fold: {', '.join(fails)}\n")
            for p in pending_reasons:
                f.write(f"- pending: {p}\n")
            f.write("\n")

        # Six questions of Section 6.8 in fixed order.
        f.write("## The Six Questions (Section 6.8)\n\n")

        # Q1: Do top wallets outperform random wallets OOS?
        f.write("### 1. Do top wallets outperform random wallets out of sample?\n\n")
        if not wf_sorted.empty:
            successful = wf_sorted[wf_sorted["status"] == "ok"]
            if not successful.empty:
                if "random_sharpe_pct_rank" not in successful.columns or "random_pnl_pct_rank" not in successful.columns:
                    f.write("INCOMPLETE: random_sharpe_pct_rank or random_pnl_pct_rank column missing -- FAIL.\n\n")
                else:
                    ranks = successful["random_sharpe_pct_rank"].dropna().tolist()
                    pnl_ranks = successful["random_pnl_pct_rank"].dropna().tolist()
                    if ranks and pnl_ranks and len(ranks) == len(successful) and len(pnl_ranks) == len(successful):
                        f.write(f"Per-fold Sharpe percentile vs 1000 random portfolios: {[round(r,1) for r in ranks]}\n\n")
                        f.write(f"Per-fold net-PnL percentile: {[round(r,1) for r in pnl_ranks]}\n\n")
                        answer = "YES" if all(r >= 95 for r in ranks) and all(r >= 95 for r in pnl_ranks) else "NO"
                        f.write(f"**Answer: {answer}** (both Sharpe + net-PnL must be >=95th in every fold)\n\n")
                    else:
                        f.write(f"INCOMPLETE: Sharpe ranks ({len(ranks)} of {len(successful)}) + PnL ranks ({len(pnl_ranks)} of {len(successful)}) -- FAIL.\n\n")
            else:
                f.write("No successful folds -- FAIL.\n\n")
        else:
            f.write("Empty results -- FAIL.\n\n")

        # Q2: Does the edge survive fees, slippage, and latency?
        f.write("### 2. Does the edge survive fees, slippage, and latency?\n\n")
        if "price_interval_1h_cadence_unvalidated" in pending_reasons:
            f.write("Pending: backtest ran at 1h granularity. Section 6.7 ablations 5/6/7 (latency 30s/1m/5m, fees 1x/1.5x/2x, slippage 0/real/punitive) cannot be validated at this granularity. **FAIL until 1m backfill or spec amendment.**\n\n")
        elif ablation_issues:
            f.write(f"Pending: {len(ablation_issues)} ablation completeness issues. **FAIL until full ablation suite runs.**\n\n")
        else:
            f.write("All 9 ablations passed (see ablation_results.parquet).\n\n")

        # Q3: Persistence across folds?
        f.write("### 3. Is performance persistent across folds?\n\n")
        if not wf_sorted.empty:
            successful = wf_sorted[wf_sorted["status"] == "ok"]
            if not successful.empty:
                sharpes = successful["test_sharpe"].dropna().tolist()
                f.write(f"Per-fold test Sharpe: {[round(s,2) for s in sharpes]}\n\n")
                latest_row = wf_sorted.iloc[-1]
                latest_te = latest_row.get("test_end", "?")
                latest_sh = latest_row.get("test_sharpe", "NA")
                latest_prof_raw = latest_row.get("latest_fold_profitable")
                if _is_strict_true(latest_prof_raw):
                    latest_prof_str = "yes"
                else:
                    try:
                        if pd.isna(latest_prof_raw) or latest_prof_raw is None:
                            latest_prof_str = "unknown"
                        elif _is_strict_true(latest_prof_raw):
                            latest_prof_str = "yes"
                        else:
                            latest_prof_str = "no"
                    except Exception:
                        latest_prof_str = "unknown"
                f.write(f"Latest fold (by fold idx={int(latest_row.get('fold'))}, test_end={latest_te}): Sharpe = {latest_sh}, profitable = {latest_prof_str}, all-Section-6.3-rows-pass = {latest_fold_pass}\n\n")
            else:
                f.write("No successful folds.\n\n")

        # Q4: Alpha independent of beta?
        f.write("### 4. Is alpha independent from BTC, ETH, perp index, and alt basket beta?\n\n")
        f.write("Multi-factor regression of test returns on (BTC, ETH, HL index, alt basket, momentum) is part of ablation #2 (top_vs_beta).\n")
        if ablation_issues:
            f.write("**Status: pending ablation implementation -> FAIL.**\n\n")
        else:
            f.write("See ablation_results.parquet.\n\n")

        # Q5: Edge diversified or concentrated?
        f.write("### 5. Is the edge diversified or concentrated in a few wallets?\n\n")
        if not wf_sorted.empty:
            successful = wf_sorted[wf_sorted["status"] == "ok"]
            if not successful.empty:
                for k in [1, 5, 10]:
                    col = f"remove_top{k}_sharpe"
                    if col in successful.columns:
                        vals = successful[col].dropna().tolist()
                        f.write(f"- Remove top {k}: per-fold Sharpe = {[round(v,2) for v in vals]}\n")
                f.write("\n")
                # Require: all 3 K-remove columns present AND all values across
                # all successful folds are non-NaN AND strictly positive.
                ks_present = [k for k in [1, 5, 10] if f"remove_top{k}_sharpe" in successful.columns]
                if len(ks_present) < 3:
                    f.write(f"**Answer: INCOMPLETE** (missing top-K removal columns: {set([1,5,10]) - set(ks_present)}) -- FAIL.\n\n")
                else:
                    all_data_complete = True
                    all_pos = True
                    for k in [1, 5, 10]:
                        col = f"remove_top{k}_sharpe"
                        vals = successful[col]
                        if vals.isna().any():
                            all_data_complete = False
                            break
                        if not (vals > 0).all():
                            all_pos = False
                    if not all_data_complete:
                        f.write("**Answer: INCOMPLETE** (NaN in remove-top columns) -- FAIL.\n\n")
                    else:
                        f.write(f"**Answer: {'diversified' if all_pos else 'concentrated or fragile'}**\n\n")

        # Q6: Which wallet traits predict future copyable PnL?
        f.write("### 6. Which wallet traits predict future copyable PnL?\n\n")
        if args.wallet_metrics and Path(args.wallet_metrics).exists():
            wm = pd.read_parquet(args.wallet_metrics)
            elig = wm[wm["eligible"]] if "eligible" in wm.columns else pd.DataFrame()
            if not elig.empty:
                target = "wallet_score"
                trait_cols = [
                    "sharpe_pct", "sortino_pct", "max_dd_pct", "journey_win_rate",
                    "profit_factor", "median_journey_bps", "median_holding_hours",
                    "turnover_per_day", "pnl_concentration_coin", "pnl_concentration_day",
                    "btc_eth_r2", "hl_index_beta", "fee_drag",
                ]
                rows = []
                for c in trait_cols:
                    if c in elig.columns:
                        try:
                            r = elig[[c, target]].corr().iloc[0, 1]
                            rows.append((c, r))
                        except Exception:
                            pass
                rows.sort(key=lambda x: abs(x[1] or 0), reverse=True)
                f.write("Pearson correlation of each trait with `wallet_score` (eligible only):\n\n")
                f.write("| Trait | Correlation |\n|-------|------------:|\n")
                for c, r in rows:
                    f.write(f"| {c} | {r:+.3f} |\n")
                f.write("\n")
            else:
                f.write("No eligible wallets in metrics.\n\n")
        else:
            f.write("(wallet_metrics file not provided)\n\n")

        # Per-fold detail.
        f.write("## Per-Fold Detail\n\n")
        f.write("| Fold | Status | Train | Test | K | Gross | Val Sh | Test Sh | DD% | Net% | %rank_sh | %rank_pnl | Latest? | Fails |\n")
        f.write("|-----:|--------|-------|------|--:|------:|-------:|--------:|----:|-----:|---------:|----------:|---------|-------|\n")
        for i, row in wf_sorted.iterrows():
            ev = fold_evals[i]
            fails_str = ",".join(ev["fails"][:3]) + ("..." if len(ev["fails"]) > 3 else "")
            f.write(
                f"| {int(row['fold'])} | {row.get('status','?')} | "
                f"{row.get('train_start','?')}..{row.get('train_end','?')} | "
                f"{row.get('test_start','?')}..{row.get('test_end','?')} | "
                f"{row.get('best_K','-')} | {row.get('best_gross','-')} | "
                f"{row.get('val_sharpe','-')} | {row.get('test_sharpe','-')} | "
                f"{row.get('test_max_dd_pct','-')} | {row.get('test_net_return_pct','-')} | "
                f"{row.get('random_sharpe_pct_rank','-')} | {row.get('random_pnl_pct_rank','-')} | "
                f"{'YES' if row.get('is_latest_fold') else ''} | {fails_str} |\n"
            )
        f.write("\n")

        # Pending block.
        if pending_reasons:
            f.write("## Pending / Deployment Blockers\n\n")
            for p in pending_reasons:
                if p == "near_liq_partial_coverage":
                    f.write("- **Near-liquidation analysis pending margin-ratio collection.** survival_factor is PARTIAL_COVERAGE; "
                            "Section 5 of remediation plan v2 forces overall FAIL until margin-ratio data exists or Alberto explicitly overrides.\n")
                elif p == "price_interval_1h_cadence_unvalidated":
                    f.write("- **Backtest ran at 1h granularity.** Spec poll cadences (30s/1m/5m/10m) and staleness rule (15m) "
                            "cannot be validated at this granularity. 1m backfill required for full validation.\n")
                elif p.startswith("ablations_incomplete"):
                    f.write(f"- **{p}**: Section 6.7 ablation suite has incomplete coverage for one or more folds. See ablation_results.parquet for the missing experiment/variant rows.\n")
                else:
                    f.write(f"- {p}\n")
            f.write("\n")

        f.write("---\n")
        f.write("Report generated by `scripts/v13_report.py` v3 (spec 6.3 two-tier verdict + pooled aggregate fee_drag).\n")

    print(f"Wrote report to {out}")
    print(f"Overall verdict: {'PASS' if overall_pass else 'FAIL'}")


if __name__ == "__main__":
    main()
