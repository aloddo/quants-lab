#!/usr/bin/env python3
"""V13 Module 03 v2 — Eligibility Gates V2 (Alberto-locked 2026-05-30 voice msgs).

Spec changes from M03 v1:

REMOVED gates:
- median_holding > 5 min
- pnl_concentration_day < 50%
- active_in_last_7d
- median_train_equity >= $1000
- liquidation_events == 0  (per Alberto voice 7891: "happens to the best of us")

KEPT:
- active_days >= 15
- trade_count >= 30

CHANGED:
- max_dd: 50% -> 80%
- after_fee_pnl: now includes FUNDING (uses M02 net_realized_pnl_usd which has funding_net_usd subtracted)

ADDED:
- source_6m_roe_flow_adjusted >= 50% (time-weighted return adjusted for deposits/withdrawals)
- HFT exclusion: any active day with n_fills_day >= 30 AND median_inter_fill_seconds_day < 5.0

REPORTING:
- per-fold waterfall JSONL (count after each filter)
- summary CSV across folds
- 3-tier pool flag: critical/insufficient/thin
- carry source_equity_base diagnostic for M04 weighting

Approach (practical — avoids re-running 3h equity reconstruct):
- Universe: pull from M01 equity parquet (~20K wallets covered)
- For each wallet: compute time-weighted source ROE from M01 equity series
  using ledger_nonfunding_cum to back out deposits/withdrawals
- Count active days + trade_count from M02 journeys
- Compute funding-aware after_fee_pnl using M02 net_realized_pnl_usd sum
- HFT detection deferred to v2.1 (requires raw fills; for now, approximate
  via journey n_fills_total / duration_hours peak)

Usage:
  python scripts/v13_m03_v2.py \
    --equity-parquet app/data/v13/equity_universe_20k.parquet \
    --journeys-glob 'app/data/v13/journey_chunks/chunk_*.parquet' \
    --window-start 2025-12-01 --window-end 2026-05-14 \
    --output-eligible /tmp/v13_m03_v2_eligible.txt \
    --output-waterfall /tmp/v13_m03_v2_waterfall.json \
    --output-metrics-parquet /tmp/v13_m03_v2_metrics.parquet
"""
from __future__ import annotations

import argparse
import glob
import json
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [m03_v2] %(message)s")
logger = logging.getLogger("m03_v2")


def time_weighted_return_flow_adjusted(eq_df: pd.DataFrame) -> dict:
    """Compute time-weighted return adjusted for cashflows (deposits/withdrawals).

    eq_df columns expected:
      - date (datetime/date)
      - perp_account_value_usd
      - spot_usdc_today
      - ledger_nonfunding_cum (cumulative external deposits/withdrawals, excludes funding)

    Returns:
      {
        "source_6m_roe_flow_adjusted": float (0.5 = +50%),
        "max_dd_pct": float (0.8 = 80%),
        "n_days": int,
        "starting_equity": float,
        "ending_equity": float,
        "total_flows_usd": float (gross deposits - withdrawals),
        "diagnostic_simple_roe": float (start-to-end, ignoring flows),
      }
    """
    d = eq_df.sort_values("date").reset_index(drop=True)
    if len(d) < 2:
        return {
            "source_6m_roe_flow_adjusted": 0.0,
            "max_dd_pct": 0.0,
            "n_days": len(d),
            "starting_equity": 0.0,
            "ending_equity": 0.0,
            "total_flows_usd": 0.0,
            "diagnostic_simple_roe": 0.0,
        }

    # Total equity = perp + spot
    d["total_equity"] = d["perp_account_value_usd"].fillna(0) + d["spot_usdc_today"].fillna(0)

    # Daily flow = ledger_nonfunding_cum diff (this is cashflows from ext deposits/withdrawals)
    d["flow_today"] = d["ledger_nonfunding_cum"].diff().fillna(0.0)

    # Daily return adjusted: r_t = (eq_t - eq_{t-1} - flow_t) / eq_{t-1}
    # Where eq_{t-1} is the starting equity for the day (before today's flow).
    d["eq_prev"] = d["total_equity"].shift(1)
    # Guard division — when eq_prev is 0 or very small, return is undefined (treat as 0)
    safe_denom = d["eq_prev"].where(d["eq_prev"] > 100, np.nan)  # need >$100 base to compute return
    d["return_adj"] = (d["total_equity"] - d["eq_prev"] - d["flow_today"]) / safe_denom
    # Filter inf, NaN
    d["return_adj"] = d["return_adj"].replace([np.inf, -np.inf], np.nan)
    valid_returns = d["return_adj"].dropna()

    if len(valid_returns) == 0:
        roe = 0.0
        max_dd = 0.0
    else:
        roe = float((1 + valid_returns).prod() - 1)
        # Cumulative drawdown on the return series
        cum = (1 + valid_returns).cumprod()
        peak = cum.cummax()
        dd_series = (cum - peak) / peak
        max_dd = float(abs(dd_series.min())) if len(dd_series) > 0 else 0.0

    starting_eq = float(d["total_equity"].iloc[0]) if len(d) > 0 else 0.0
    ending_eq = float(d["total_equity"].iloc[-1]) if len(d) > 0 else 0.0
    total_flows = float(d["ledger_nonfunding_cum"].iloc[-1] - d["ledger_nonfunding_cum"].iloc[0]) if "ledger_nonfunding_cum" in d.columns else 0.0
    simple_roe = (ending_eq - starting_eq) / starting_eq if starting_eq > 0 else 0.0

    return {
        "source_6m_roe_flow_adjusted": roe,
        "max_dd_pct": max_dd,
        "n_days": len(d),
        "starting_equity": starting_eq,
        "ending_equity": ending_eq,
        "total_flows_usd": total_flows,
        "diagnostic_simple_roe": float(simple_roe),
    }


def compute_journey_metrics(jr_df: pd.DataFrame, window_start_ms: int, window_end_ms: int) -> dict:
    """Compute journey-based metrics: active_days, trade_count, funding-aware after_fee_pnl, median holding."""
    if len(jr_df) == 0:
        return {
            "active_days": 0, "trade_count": 0,
            "net_realized_pnl_usd": 0.0,
            "n_journeys": 0,
            "median_holding_hours": 0.0,
            "peak_fills_per_hour": 0.0,
        }
    # Filter journeys EXITING in window
    j = jr_df[(jr_df["exit_ts"] >= window_start_ms) & (jr_df["exit_ts"] <= window_end_ms)].copy()
    if len(j) == 0:
        return {
            "active_days": 0, "trade_count": 0,
            "net_realized_pnl_usd": 0.0,
            "n_journeys": 0,
            "median_holding_hours": 0.0,
            "peak_fills_per_hour": 0.0,
        }
    j["entry_dt"] = pd.to_datetime(j["entry_ts"], unit="ms", utc=True)
    j["exit_dt"] = pd.to_datetime(j["exit_ts"], unit="ms", utc=True)
    active_dates = set()
    for _, row in j.iterrows():
        start_d = max(row["entry_dt"].date(), datetime.fromtimestamp(window_start_ms/1000, tz=timezone.utc).date())
        end_d = min(row["exit_dt"].date(), datetime.fromtimestamp(window_end_ms/1000, tz=timezone.utc).date())
        if start_d <= end_d:
            cur = start_d
            from datetime import timedelta as td
            while cur <= end_d:
                active_dates.add(cur)
                cur += td(days=1)
    trade_count = int(j["n_fills_total"].sum())
    net_pnl = float(j["net_realized_pnl_usd"].sum())
    n_journeys = len(j)
    # Median holding hours (Alberto-revised HFT filter: median_holding > 1 min replaces fills/hour HFT)
    median_holding_hours = float(j["duration_hours"].median()) if len(j) > 0 else 0.0
    # Peak fills/hour kept as diagnostic only
    j["fills_per_hour"] = j["n_fills_total"] / j["duration_hours"].replace(0, np.nan)
    peak_fph = float(j["fills_per_hour"].max()) if not j["fills_per_hour"].isna().all() else 0.0

    return {
        "active_days": len(active_dates),
        "trade_count": trade_count,
        "net_realized_pnl_usd": net_pnl,
        "n_journeys": n_journeys,
        "median_holding_hours": median_holding_hours,
        "peak_fills_per_hour": peak_fph,
    }


def apply_v2_gates(metrics: dict) -> tuple[bool, list[str]]:
    """Apply M03 v2 gates. Returns (eligible, list_of_failures).

    Alberto-revised 2026-05-30 msg 7896: HFT filter is now median_holding > 1 minute
    (replaces the fills/hour-based HFT detection — simpler and aligned with original spec
    which had median_holding > 5min). Cleaner, looser, removes the same HFT class.
    """
    failures = []
    if metrics["active_days"] < 15:
        failures.append(f"active_days {metrics['active_days']} < 15")
    if metrics["trade_count"] < 30:
        failures.append(f"trade_count {metrics['trade_count']} < 30")
    if metrics["max_dd_pct"] > 0.80:
        failures.append(f"max_dd {metrics['max_dd_pct']:.1%} > 80%")
    if metrics["source_6m_roe_flow_adjusted"] < 0.50:
        failures.append(f"source_6m_roe {metrics['source_6m_roe_flow_adjusted']:.1%} < 50%")
    if metrics["net_realized_pnl_usd"] <= 0:
        failures.append(f"after_fee_pnl (incl funding) {metrics['net_realized_pnl_usd']:.1f} <= 0")
    # Alberto-revised HFT filter (msg 7896): median holding > 1 minute
    if metrics["median_holding_hours"] * 60 <= 1.0:
        failures.append(f"median_holding {metrics['median_holding_hours']*60:.2f}min <= 1min (HFT)")
    return (len(failures) == 0, failures)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--equity-parquet", required=True)
    ap.add_argument("--journeys-glob", required=True)
    ap.add_argument("--window-start", default="2025-12-01")
    ap.add_argument("--window-end", default="2026-05-14")
    ap.add_argument("--output-eligible", required=True)
    ap.add_argument("--output-waterfall", required=True)
    ap.add_argument("--output-metrics-parquet", required=True)
    args = ap.parse_args()

    window_start = date.fromisoformat(args.window_start)
    window_end = date.fromisoformat(args.window_end)
    window_start_ms = int(datetime.combine(window_start, datetime.min.time(), tzinfo=timezone.utc).timestamp() * 1000)
    window_end_ms = int(datetime.combine(window_end, datetime.max.time(), tzinfo=timezone.utc).timestamp() * 1000)

    # Load equity (M01)
    logger.info(f"Loading equity from {args.equity_parquet}...")
    eq = pd.read_parquet(args.equity_parquet)
    logger.info(f"  {len(eq):,} rows, {eq['wallet'].nunique()} wallets")

    # Filter equity to window
    eq["date"] = pd.to_datetime(eq["date"]).dt.date
    eq_window = eq[(eq["date"] >= window_start) & (eq["date"] <= window_end)].copy()
    logger.info(f"  {len(eq_window):,} rows in window {window_start} → {window_end}")

    # Load journeys
    chunks = sorted(glob.glob(args.journeys_glob))
    logger.info(f"Loading {len(chunks)} journey chunks...")
    jr_all = pd.concat([pd.read_parquet(c) for c in chunks], ignore_index=True)
    logger.info(f"  {len(jr_all):,} journeys total")

    # Group by wallet for both
    eq_by_wallet = {w: g for w, g in eq_window.groupby("wallet")}
    jr_by_wallet = {w: g for w, g in jr_all.groupby("wallet")}

    wallets = set(eq_by_wallet.keys()) | set(jr_by_wallet.keys())
    logger.info(f"Total wallets to evaluate: {len(wallets)}")

    metrics_rows = []
    waterfall = {
        "fold": "single_window_2025-12-01_to_2026-05-14",
        "filters": [],
    }
    universe_count = len(wallets)
    waterfall["universe"] = universe_count
    waterfall["filters"].append({"filter": "universe", "count_in": None, "count_out": universe_count})

    # Pre-screen: must have BOTH equity AND journeys
    pre_screen = set()
    for w in wallets:
        if w in eq_by_wallet and w in jr_by_wallet:
            pre_screen.add(w)
    logger.info(f"After pre-screen (has equity+journey): {len(pre_screen)}")
    waterfall["filters"].append({"filter": "has_equity_and_journeys", "count_in": universe_count, "count_out": len(pre_screen)})

    # Evaluate each wallet
    for i, wallet in enumerate(pre_screen):
        if i % 500 == 0:
            logger.info(f"  evaluating {i}/{len(pre_screen)}...")
        eq_df = eq_by_wallet[wallet]
        jr_df = jr_by_wallet[wallet]
        # Equity-based metrics
        eq_m = time_weighted_return_flow_adjusted(eq_df)
        # Journey-based metrics
        jr_m = compute_journey_metrics(jr_df, window_start_ms, window_end_ms)
        # Combine
        m = {"wallet": wallet, **eq_m, **jr_m}
        # Apply gates
        eligible, failures = apply_v2_gates(m)
        m["eligible"] = eligible
        m["fail_reasons"] = "|".join(failures) if failures else ""
        metrics_rows.append(m)

    df = pd.DataFrame(metrics_rows)

    # Sequential waterfall (each filter applied in order, tracking dropouts)
    filters_order = [
        ("active_days_ge_15", lambda d: d["active_days"] >= 15),
        ("trade_count_ge_30", lambda d: d["trade_count"] >= 30),
        ("max_dd_le_80pct", lambda d: d["max_dd_pct"] <= 0.80),
        ("source_6m_roe_ge_50pct", lambda d: d["source_6m_roe_flow_adjusted"] >= 0.50),
        ("after_fee_pnl_gt_0", lambda d: d["net_realized_pnl_usd"] > 0),
        ("median_holding_gt_1min", lambda d: d["median_holding_hours"] * 60 > 1.0),
    ]
    cur_df = df.copy()
    for fname, predicate in filters_order:
        before = len(cur_df)
        mask = predicate(cur_df)
        median_before = {
            "max_dd_pct": float(cur_df["max_dd_pct"].median()),
            "source_6m_roe": float(cur_df["source_6m_roe_flow_adjusted"].median()),
        }
        cur_df = cur_df[mask]
        after = len(cur_df)
        median_after = {
            "max_dd_pct": float(cur_df["max_dd_pct"].median()) if after > 0 else None,
            "source_6m_roe": float(cur_df["source_6m_roe_flow_adjusted"].median()) if after > 0 else None,
        }
        waterfall["filters"].append({
            "filter": fname,
            "count_in": before, "count_out": after, "dropped": before - after,
            "dropped_pct": round((before - after) / before * 100, 2) if before > 0 else 0,
            "median_max_dd_before": median_before["max_dd_pct"],
            "median_roe_before": median_before["source_6m_roe"],
            "median_max_dd_after": median_after["max_dd_pct"],
            "median_roe_after": median_after["source_6m_roe"],
        })
        logger.info(f"  filter {fname}: {before} → {after} (dropped {before-after}, {(before-after)/max(before,1)*100:.1f}%)")

    final_eligible = cur_df["wallet"].tolist()
    waterfall["final_eligible_count"] = len(final_eligible)

    # 3-tier pool flag (assume K_target = 5 default)
    K_target = 5
    pool_flag = "ok"
    if len(final_eligible) < 5 * K_target:
        pool_flag = "critical"
    elif len(final_eligible) < 10 * K_target:
        pool_flag = "insufficient"
    elif len(final_eligible) < 20 * K_target:
        pool_flag = "thin"
    waterfall["pool_flag"] = pool_flag
    waterfall["K_target_assumed"] = K_target

    # Save
    Path(args.output_eligible).write_text("\n".join(final_eligible))
    Path(args.output_waterfall).write_text(json.dumps(waterfall, indent=2, default=str))
    df.to_parquet(args.output_metrics_parquet)
    logger.info(f"WROTE eligible: {args.output_eligible} ({len(final_eligible)} wallets)")
    logger.info(f"WROTE waterfall: {args.output_waterfall}")
    logger.info(f"WROTE metrics: {args.output_metrics_parquet}")
    logger.info(f"FINAL POOL: {len(final_eligible)} wallets, flag={pool_flag}")


if __name__ == "__main__":
    main()
