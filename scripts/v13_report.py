#!/usr/bin/env python3
"""V13 Script 5/5 (v2): Strategy report.

Per projects/quant/v13 Section 6.3 + 6.8 + remediation plan v2.

v2 fixes (from codex r1 #21-#23 + R3 partial-coverage enforcement):

#21 PER-FOLD PER-ROW evaluation. Final verdict PASSes only if EVERY criterion
    passes on EVERY fold. No mean-across-folds masking.

#22 LATEST FOLD = sort by test_end, include failed folds. The actual latest
    attempted fold must be successful AND profitable.

#23 PENDING ANALYSES force overall FAIL. If any of the 6 questions cannot
    be answered with data, the report verdict is FAIL.

Additional R3 gotcha enforcement:
- deployment_blocked_by_near_liq propagated from wallet_metrics: forces FAIL
  until margin-ratio data exists or Alberto explicitly overrides.
- Ablation `not_implemented` rows from walk_forward: each missing ablation
  forces FAIL.
- Price interval = "1h" with APPROXIMATED label propagation: latency-
  sensitive criteria (poll cadence, staleness, cooldown) cannot pass at
  1h granularity.

Inputs:
    --walk-forward-results <path>    walk_forward_results.parquet
    --wallet-metrics <path>          wallet_metrics.parquet (latest fold)
    --ablation-results <path>        ablation_results.parquet
    --price-interval                 "1m" or "1h" (matches walk-forward run)
    --output <path>                  app/data/v13/strategy_report.md
"""
from __future__ import annotations

import argparse
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


def _evaluate_fold(row: dict) -> dict:
    """Per-fold per-row Section 6.3 evaluation. NaN = missing = FAIL."""
    fails: list[str] = []
    if row.get("status") != "ok":
        fails.append(f"fold_status:{row.get('status')}")
        return {"fold": row.get("fold"), "all_pass": False, "fails": fails}

    # Row 1: Sharpe >= 1.5
    sharpe = _safe_float(row.get("test_sharpe"))
    if sharpe is None:
        fails.append("sharpe_missing")
    elif sharpe < PASS_THRESHOLDS["sharpe_min"]:
        fails.append(f"sharpe_below_min({sharpe:.2f}<{PASS_THRESHOLDS['sharpe_min']})")

    # Row 2: max DD <= 25%
    max_dd = _safe_float(row.get("test_max_dd_pct"))
    if max_dd is None:
        fails.append("max_dd_missing")
    elif max_dd > PASS_THRESHOLDS["max_dd_max_pct"]:
        fails.append(f"max_dd_exceeded({max_dd:.1f}>{PASS_THRESHOLDS['max_dd_max_pct']})")

    # Row 3: worst single day <= 10%
    worst_day_raw = _safe_float(row.get("test_worst_day_pct"))
    if worst_day_raw is None:
        fails.append("worst_day_missing")
    elif abs(worst_day_raw) > PASS_THRESHOLDS["worst_day_max_pct_abs"]:
        fails.append(f"worst_day_exceeded({abs(worst_day_raw):.1f}>{PASS_THRESHOLDS['worst_day_max_pct_abs']})")

    # Row 4: beats benchmarks (USDC, BTC, ETH, HYPE, perp index, alt basket, momentum, V12)
    # ENFORCED via ablation #2 (top_vs_beta) at the ablation-completeness layer
    # below, not as a per-fold row column. Walk_forward emits this as ablation
    # rows; if any benchmark beat fails, ablation_issues will surface it.

    # Row 5: random Sharpe + net PnL percentile >= 95
    sharpe_rank = _safe_float(row.get("random_sharpe_pct_rank"))
    if sharpe_rank is None:
        fails.append("random_sharpe_rank_missing")
    elif sharpe_rank < PASS_THRESHOLDS["random_sharpe_pct_min"]:
        fails.append(f"sharpe_pct_rank_below({sharpe_rank:.1f}<{PASS_THRESHOLDS['random_sharpe_pct_min']})")
    pnl_rank = _safe_float(row.get("random_pnl_pct_rank"))
    if pnl_rank is None:
        fails.append("random_pnl_rank_missing")
    elif pnl_rank < PASS_THRESHOLDS["random_pnl_pct_min"]:
        fails.append(f"pnl_pct_rank_below({pnl_rank:.1f}<{PASS_THRESHOLDS['random_pnl_pct_min']})")

    # Row 6: fee_drag <= 30% (the per-fold fee drag column needs to come from
    # walk_forward; if absent we FAIL to be safe).
    fee_drag = _safe_float(row.get("fee_drag"))
    if fee_drag is None:
        fails.append("fee_drag_missing")
    elif fee_drag > PASS_THRESHOLDS["fee_drag_max_frac"]:
        fails.append(f"fee_drag_exceeded({fee_drag:.2f}>{PASS_THRESHOLDS['fee_drag_max_frac']})")

    # Row 7: top-1/5/10 removal must remain profitable
    for k in [1, 5, 10]:
        col = f"remove_top{k}_sharpe"
        val = _safe_float(row.get(col))
        if val is None:
            fails.append(f"robust_{col}_missing")
        elif val <= PASS_THRESHOLDS["robust_remove_min_sharpe"]:
            fails.append(f"robust_{col}_failed({val:.2f})")

    # Row 8: latest fold profitable (only enforced on the latest-fold row).
    if _is_strict_true(row.get("is_latest_fold")):
        if not _is_strict_true(row.get("latest_fold_profitable")):
            fails.append("latest_fold_not_profitable")

    # Row 9: latency sensitivity (cadence sweep must remain robust).
    # ENFORCED via ablation #5 (latency_cadence) at the ablation-completeness
    # layer below. Walk_forward emits latency variants as ablation rows.

    return {"fold": row.get("fold"), "all_pass": len(fails) == 0, "fails": fails}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--walk-forward-results", required=True)
    ap.add_argument("--wallet-metrics", required=False)
    ap.add_argument("--ablation-results", required=False)
    ap.add_argument("--price-interval", default="1h", choices=["1m", "1h"])
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

    # Per-fold per-row evaluation.
    fold_evals = [_evaluate_fold(r._asdict()) for r in wf_sorted.itertuples(index=False)]
    n_fold_pass = sum(1 for f in fold_evals if f["all_pass"])
    n_fold_attempted = len(fold_evals)
    fold_level_pass = (n_fold_pass == n_fold_attempted) and (n_fold_attempted > 0)

    # Latest-fold dedicated check.
    latest_fold_eval = fold_evals[-1] if fold_evals else None
    latest_fold_pass = bool(latest_fold_eval and latest_fold_eval["all_pass"])

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

    # Overall verdict.
    overall_pass = fold_level_pass and latest_fold_pass and len(pending_reasons) == 0

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w") as f:
        f.write(f"# V13 Backtest Report\n\n")
        f.write(f"Walk-forward source: `{args.walk_forward_results}`\n\n")
        f.write(f"Folds attempted: {n_fold_attempted}\n")
        f.write(f"Folds passing all rows: {n_fold_pass}\n\n")

        f.write("## Verdict\n\n")
        f.write(f"**Overall: {'PASS' if overall_pass else 'FAIL'}**\n\n")
        if not overall_pass:
            f.write("Reasons FAIL:\n")
            if not fold_level_pass:
                f.write(f"- per_fold_per_row: only {n_fold_pass}/{n_fold_attempted} folds passed all Section 6.3 rows\n")
            if not latest_fold_pass:
                fails = latest_fold_eval["fails"] if latest_fold_eval else ["no_folds"]
                f.write(f"- latest_fold_not_clear: {', '.join(fails)}\n")
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
                elif p.startswith("ablations_unimplemented"):
                    f.write(f"- **{p}**: Section 6.7 ablation suite is stubbed. Full implementation required before deploy.\n")
                else:
                    f.write(f"- {p}\n")
            f.write("\n")

        f.write("---\n")
        f.write("Report generated by `scripts/v13_report.py` v2.\n")

    print(f"Wrote report to {out}")
    print(f"Overall verdict: {'PASS' if overall_pass else 'FAIL'}")


if __name__ == "__main__":
    main()
