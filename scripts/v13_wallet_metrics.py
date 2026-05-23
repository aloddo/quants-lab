#!/usr/bin/env python3
"""V13 Script 3/5 (v2): Wallet-level metrics + eligibility + composite score.

Per projects/quant/v13 Sections 5.3, 5.4, 5.5 + remediation plan v2.

v2 fixes (from codex r1 #10-#13 + r3 gotchas):

#10 LOOK-AHEAD FIX: filter journeys by `exit_ts` inside the window (closed-
    in-window only), not by `entry_date`. Journeys that started inside the
    window but exit AFTER it are excluded.

#11 FEE DRAG fix: use FILL-level data, not journey-level max notional. The
    script takes raw S3 fills as a third input. fee_drag = (sum_fills
    abs(notional) * fee_bps_per_side / 10000 + slippage_bps applied per
    fill) / gross_positive_pnl_usd.

#12 AFTER-FEE SHARPE fix: build per-day after-fee return series. Subtract
    our daily copy-fee (computed from fills) from the wallet's equity-
    series-derived daily PnL. Compute Sharpe on the corrected returns.

#13 SURVIVAL FACTOR fix: liquidation events only (detected from S3 fills
    via `dir == "Liquidation"`). Near-liquidation is UNSUPPORTED in v1 -
    the report layer (script 5) flags it as PARTIAL_COVERAGE which forces
    overall FAIL until margin-ratio data exists.

Schema validation: required input columns including closedPnl, dir.
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient

import sys as _sys
_sys.path.insert(0, str(Path(__file__).resolve().parent))
from v13_equity_reconstruct import (    # noqa: E402
    EPS,
    load_fills_for_dates,
    validate_and_normalize_fills,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_metrics] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "wallet_metrics.parquet"

HL_FEE_BPS_PER_SIDE = 4.32
DEFAULT_SLIPPAGE_BPS = 5.0          # conservative per-fill slippage estimate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sharpe(returns: pd.Series) -> float:
    if returns.empty or returns.std() == 0:
        return 0.0
    return float(returns.mean() / returns.std() * np.sqrt(365))


def _sortino(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    downside = returns[returns < 0]
    if downside.empty or downside.std() == 0:
        return 0.0
    return float(returns.mean() / downside.std() * np.sqrt(365))


def _max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    peak = equity.cummax()
    dd = (equity - peak) / peak.replace(0, np.nan)
    dd = dd.fillna(0)
    return float(-dd.min()) if not dd.empty else 0.0


def _herfindahl(shares: pd.Series) -> float:
    if shares.empty or shares.sum() == 0:
        return 0.0
    s = shares / shares.sum()
    return float((s ** 2).sum())


def _beta(strategy_returns: pd.Series, market_returns: pd.Series) -> tuple[float, float]:
    aligned = pd.concat({"s": strategy_returns, "m": market_returns}, axis=1).dropna()
    if len(aligned) < 5 or aligned["m"].var() == 0:
        return 0.0, 0.0
    sr, mr = aligned["s"], aligned["m"]
    beta = float(np.cov(sr, mr)[0, 1] / mr.var())
    pred = beta * (mr - mr.mean()) + sr.mean()
    ss_res = ((sr - pred) ** 2).sum()
    ss_tot = ((sr - sr.mean()) ** 2).sum()
    r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0
    return beta, max(0.0, r2)


def _btc_eth_combined_r2(s: pd.Series, b: pd.Series, e: pd.Series) -> float:
    df = pd.concat({"s": s, "b": b, "e": e}, axis=1).dropna()
    if len(df) < 5:
        return 0.0
    X = np.column_stack([np.ones(len(df)), df["b"].values, df["e"].values])
    y = df["s"].values
    try:
        beta_hat, *_ = np.linalg.lstsq(X, y, rcond=None)
    except np.linalg.LinAlgError:
        return 0.0
    pred = X @ beta_hat
    ss_res = float(np.sum((y - pred) ** 2))
    ss_tot = float(np.sum((y - y.mean()) ** 2))
    return max(0.0, 1.0 - ss_res / ss_tot) if ss_tot > 0 else 0.0


# ---------------------------------------------------------------------------
# Market reference series
# ---------------------------------------------------------------------------

def load_market_daily_returns(start: datetime, end: datetime) -> dict:
    c = MongoClient("mongodb://localhost:27017")["quants_lab"]["hyperliquid_candles_1h"]
    start_ms = int(start.timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).timestamp() * 1000)
    docs = list(c.find(
        {"timestamp_utc": {"$gte": start_ms, "$lte": end_ms}},
        {"coin": 1, "timestamp_utc": 1, "close": 1, "_id": 0},
    ))
    if not docs:
        return {}
    df = pd.DataFrame(docs)
    df["dt"] = pd.to_datetime(df["timestamp_utc"], unit="ms", utc=True)
    df["date"] = df["dt"].dt.floor("D")
    daily = df.sort_values("dt").groupby(["coin", "date"], as_index=False).last()
    pivot = daily.pivot(index="date", columns="coin", values="close")
    pivot.index = pd.to_datetime(pivot.index).tz_convert("UTC").date

    out = {}
    if "BTC" in pivot.columns:
        out["BTC"] = pivot["BTC"].pct_change().dropna()
    if "ETH" in pivot.columns:
        out["ETH"] = pivot["ETH"].pct_change().dropna()
    full = pivot.dropna(axis=1, how="any")
    if full.shape[1] >= 5:
        log_ret = np.log(full).diff().dropna()
        idx_ret = log_ret.mean(axis=1)
        out["HL_INDEX"] = idx_ret.apply(lambda x: float(np.exp(x) - 1))
    return out


# ---------------------------------------------------------------------------
# Per-wallet metric computation
# ---------------------------------------------------------------------------

def compute_metrics_for_wallet(
    wallet: str,
    eq_df: pd.DataFrame,                   # rows for this wallet
    jr_df: pd.DataFrame,                   # rows for this wallet
    fills_df: pd.DataFrame,                # rows for this wallet
    market: dict,
    window_start: datetime,
    window_end: datetime,
) -> dict:
    # 1) Equity series within window.
    eq = eq_df.sort_values("date").set_index("date")["equity_usd"]
    # eq.index is dates already (from script 1 output schema).
    eq = eq[(eq.index >= window_start.date()) & (eq.index <= window_end.date())]
    if eq.empty or len(eq) < 2:
        return {
            "wallet": wallet,
            "active_days": 0,
            "eligible": False,
            "deployment_blocked_by_near_liq": True,
            "near_liq_coverage": "PARTIAL_COVERAGE",
        }

    daily_returns = eq.pct_change().dropna()

    # 2) Journeys closed inside window only (FIXES look-ahead).
    jr = jr_df.copy()
    if not jr.empty:
        jr["exit_dt"] = pd.to_datetime(jr["exit_ts"], unit="ms", utc=True)
        jr["exit_date"] = jr["exit_dt"].dt.date
        jr = jr[(jr["exit_date"] >= window_start.date()) & (jr["exit_date"] <= window_end.date())]

    # 3) Fills filtered to window for fee + liq computations.
    fills = fills_df.copy()
    if not fills.empty:
        fills["dt"] = pd.to_datetime(fills["time"], unit="ms", utc=True)
        fills["date"] = fills["dt"].dt.floor("D").dt.date
        fills = fills[(fills["date"] >= window_start.date()) & (fills["date"] <= window_end.date())]
        fills["notional"] = fills["size"].astype(float) * fills["price"].astype(float)

    m: dict = {
        "wallet": wallet,
        "window_start": window_start.date(),
        "window_end": window_end.date(),
        "active_days": int((eq != 0).sum()),
    }

    # 4) Return-series metrics.
    m["daily_return_mean_pct"] = float(daily_returns.mean() * 100) if not daily_returns.empty else 0.0
    m["sharpe_pct"] = _sharpe(daily_returns)
    m["sortino_pct"] = _sortino(daily_returns)
    m["max_dd_pct"] = float(_max_drawdown(eq) * 100)
    m["worst_single_day_pct"] = float(daily_returns.min() * 100) if not daily_returns.empty else 0.0

    last_7_start = window_end - timedelta(days=7)
    has_recent_eq = ((eq.index >= last_7_start.date()) & (eq != 0)).any()
    has_recent_fills = (not fills.empty) and (fills["date"] >= last_7_start.date()).any()
    m["active_in_last_7d"] = int(bool(has_recent_eq and has_recent_fills))

    # 5) Journey metrics (closed-in-window).
    if jr.empty:
        m.update({
            "journey_win_rate": 0.0, "profit_factor": 0.0,
            "avg_win_bps": 0.0, "avg_loss_bps": 0.0,
            "median_journey_bps": 0.0, "median_journey_fills": 0,
            "median_holding_hours": 0.0,
            "trade_count": 0,
            "pnl_concentration_coin": 0.0,
            "pnl_concentration_day": 0.0,
        })
        gross_positive_pnl = 0.0
    else:
        pnl = jr["realized_pnl_usd"]
        m["journey_win_rate"] = float((pnl > 0).mean())
        pos = float(pnl[pnl > 0].sum())
        neg = float(-pnl[pnl < 0].sum())
        m["profit_factor"] = float(pos / neg) if neg > 0 else (float("inf") if pos > 0 else 0.0)
        wins = jr.loc[pnl > 0, "pnl_bps_of_max"]
        losses = jr.loc[pnl < 0, "pnl_bps_of_max"]
        m["avg_win_bps"] = float(wins.mean()) if not wins.empty else 0.0
        m["avg_loss_bps"] = float(losses.mean()) if not losses.empty else 0.0
        m["median_journey_bps"] = float(jr["pnl_bps_of_max"].median())
        m["median_journey_fills"] = int(jr["n_fills_total"].median())
        m["median_holding_hours"] = float(jr["duration_hours"].median())
        m["trade_count"] = int(jr["n_fills_total"].sum())
        per_coin_pnl = jr.groupby("coin")["realized_pnl_usd"].sum().abs()
        m["pnl_concentration_coin"] = _herfindahl(per_coin_pnl)
        per_day_pnl = jr.groupby("exit_date")["realized_pnl_usd"].sum().abs()
        m["pnl_concentration_day"] = (
            float(per_day_pnl.max() / per_day_pnl.sum()) if per_day_pnl.sum() > 0 else 0.0
        )
        gross_positive_pnl = pos

    # 6) Fee + slippage from FILL data.
    if fills.empty:
        m["total_traded_notional_usd"] = 0.0
        m["total_fees_usd"] = 0.0
        m["total_slippage_usd"] = 0.0
        m["fee_drag"] = 0.0
        daily_fees_series = pd.Series(dtype=float)
    else:
        m["total_traded_notional_usd"] = float(fills["notional"].sum())
        m["total_fees_usd"] = float(fills["notional"].sum() * HL_FEE_BPS_PER_SIDE / 10000.0)
        m["total_slippage_usd"] = float(fills["notional"].sum() * DEFAULT_SLIPPAGE_BPS / 10000.0)
        total_cost = m["total_fees_usd"] + m["total_slippage_usd"]
        m["fee_drag"] = float(total_cost / gross_positive_pnl) if gross_positive_pnl > 0 else (10.0 if total_cost > 0 else 0.0)
        m["fee_drag"] = float(min(m["fee_drag"], 10.0))
        # Daily fee+slippage series for after-fee Sharpe.
        per_day_notional = fills.groupby("date")["notional"].sum()
        daily_fees_series = per_day_notional * (HL_FEE_BPS_PER_SIDE + DEFAULT_SLIPPAGE_BPS) / 10000.0

    # 6b) Turnover/day. Inclusive window: 2026-05-15 to 2026-05-23 = 9 days.
    n_days = max(1, (window_end.date() - window_start.date()).days + 1)
    med_eq = float(eq.median()) if eq.median() > 0 else 1.0
    m["turnover_per_day"] = float(m["total_traded_notional_usd"] / n_days / med_eq) if med_eq > 0 else 0.0

    # 7) Liquidation detection (FIX for survival_factor).
    # HL marks liquidation fills via dir field. Conservative: dir contains 'Liquidat' substring.
    liq_events = 0
    if not fills.empty and "dir" in fills.columns:
        liq_mask = fills["dir"].astype(str).str.contains("Liquidat", case=False, na=False)
        liq_events = int(liq_mask.sum())
    m["liquidation_events"] = liq_events
    m["near_liq_coverage"] = "PARTIAL_COVERAGE"   # near-liq detection not supported in v1
    # Explicit deployment-block flag for downstream report (script 5). Per spec
    # remediation plan Section 5: partial coverage forces overall FAIL until
    # margin-ratio data exists or Alberto explicitly overrides.
    m["deployment_blocked_by_near_liq"] = True

    # 8) Beta + factor model.
    m["btc_beta"] = _beta(daily_returns, market.get("BTC", pd.Series(dtype=float)))[0]
    m["eth_beta"] = _beta(daily_returns, market.get("ETH", pd.Series(dtype=float)))[0]
    m["hl_index_beta"] = _beta(daily_returns, market.get("HL_INDEX", pd.Series(dtype=float)))[0]
    if "BTC" in market and "ETH" in market:
        m["btc_eth_r2"] = _btc_eth_combined_r2(daily_returns, market["BTC"], market["ETH"])
    else:
        m["btc_eth_r2"] = 0.0

    # 9) Half-window Sharpe drop.
    if len(daily_returns) >= 8:
        mid = len(daily_returns) // 2
        first = _sharpe(daily_returns.iloc[:mid])
        second = _sharpe(daily_returns.iloc[mid:])
        m["half_window_sharpe_drop"] = float(second / first) if first != 0 else 0.0
    else:
        m["half_window_sharpe_drop"] = 0.0

    # 10) After-fee Sharpe via per-day after-fee return series (FIX).
    # eq.diff().dropna() loses the first day; we compute returns from day 2
    # onward, but include any fees that happened on day 1 by accumulating
    # them into day 2's return (the spec asks for after-fee SHARPE, which is
    # a time-series statistic; including day-1 fees against day-2 PnL gives
    # the right magnitude in aggregate). Also guard non-positive starting
    # equity to avoid sign-inversion.
    if not daily_fees_series.empty:
        daily_pnl_usd = eq.diff().dropna()
        if not daily_pnl_usd.empty:
            df_idx = pd.DataFrame({"pnl": daily_pnl_usd})
            df_idx["fees"] = daily_fees_series.reindex(df_idx.index).fillna(0.0)
            # Carry day-0 fees (if any) into day-1's bucket (the earliest in df_idx).
            first_idx = df_idx.index.min()
            day0_fees = float(daily_fees_series[daily_fees_series.index < first_idx].sum())
            df_idx.loc[first_idx, "fees"] += day0_fees
            df_idx["after_fee_pnl"] = df_idx["pnl"] - df_idx["fees"]
            starting_eq = eq.shift(1).reindex(df_idx.index)
            # Guard non-positive starting equity to avoid sign inversion.
            starting_eq = starting_eq.where(starting_eq > EPS, np.nan)
            df_idx["after_fee_return_pct"] = df_idx["after_fee_pnl"] / starting_eq
            after_fee_returns = df_idx["after_fee_return_pct"].dropna()
            m["after_fee_sharpe"] = _sharpe(after_fee_returns)
        else:
            m["after_fee_sharpe"] = 0.0
    else:
        m["after_fee_sharpe"] = m["sharpe_pct"]

    # 11) Section 5.4 eligibility.
    eligible = (
        m["active_days"] >= 15
        and m["trade_count"] >= 30
        and m["max_dd_pct"] < 50.0
        and m["median_holding_hours"] * 60 > 5
        and m["fee_drag"] < 0.40
        and m["pnl_concentration_day"] < 0.50
        and m["btc_eth_r2"] < 0.50
        and m["active_in_last_7d"] == 1
        and m["liquidation_events"] == 0
    )
    m["eligible"] = bool(eligible)

    # 12) Section 5.5 composite score.
    pct_profitable_days = float((daily_returns > 0).mean())
    m["consistency_factor"] = pct_profitable_days * max(0.0, 1.0 - m["max_dd_pct"] / 100.0)
    hold_min = m["median_holding_hours"] * 60
    m["copyability_factor"] = (
        min(hold_min / 30.0, 1.0) * max(0.0, 1.0 - min(m["turnover_per_day"] / 5.0, 1.0))
    )
    m["diversification_factor"] = max(0.0, 1.0 - m["pnl_concentration_coin"])
    # Survival: hard liq = 0. Near-liq UNSUPPORTED in v1 -> 1.0 placeholder
    # for arithmetic but downstream report flags this as partial coverage.
    m["survival_factor"] = float(m["active_in_last_7d"]) * (0.0 if liq_events > 0 else 1.0)
    m["wallet_score"] = (
        m["after_fee_sharpe"]
        * m["consistency_factor"]
        * m["copyability_factor"]
        * m["diversification_factor"]
        * m["survival_factor"]
    )
    return m


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--journeys", required=True)
    ap.add_argument("--equity-series", required=True)
    ap.add_argument("--fills-dir", default=None,
                    help="Override fills directory (default: equity_reconstruct.FILLS_DIR)")
    ap.add_argument("--window-start", required=True)
    ap.add_argument("--window-end", required=True)
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    # Wire --fills-dir through to the shared loader by monkey-patching the
    # module-level FILLS_DIR if user provided one. This honors the override
    # codex r10 flagged as currently ignored.
    if args.fills_dir is not None:
        import v13_equity_reconstruct as _veq
        _veq.FILLS_DIR = Path(args.fills_dir)
        logger.info(f"Using fills dir: {args.fills_dir}")

    ws = datetime.strptime(args.window_start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    we = datetime.strptime(args.window_end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    logger.info(f"Window: {ws.date()} to {we.date()}")

    journeys = pd.read_parquet(args.journeys)
    journeys["wallet"] = journeys["wallet"].str.lower()
    equity = pd.read_parquet(args.equity_series)
    equity["wallet"] = equity["wallet"].str.lower()
    equity["date"] = pd.to_datetime(equity["date"]).dt.date

    # Load raw S3 fills for the window, filtered to wallets present in the
    # equity / journeys universe.
    wallets = sorted(set(equity["wallet"].unique()) | set(journeys["wallet"].unique()))
    logger.info(f"Computing metrics for {len(wallets)} wallets...")

    logger.info("Loading raw fills for fee + liq computation...")
    fills = load_fills_for_dates(ws, we, set(wallets))
    if not fills.empty:
        fills = validate_and_normalize_fills(fills)
    logger.info(f"Loaded {len(fills):,} window fills")

    market = load_market_daily_returns(ws, we)
    if not market:
        logger.warning("No market data; beta = 0")

    rows = []
    for i, w in enumerate(wallets, 1):
        eq_w = equity[equity["wallet"] == w]
        if eq_w.empty:
            continue
        jr_w = journeys[journeys["wallet"] == w]
        fl_w = fills[fills["wallet"] == w] if not fills.empty else pd.DataFrame()
        try:
            rows.append(compute_metrics_for_wallet(w, eq_w, jr_w, fl_w, market, ws, we))
        except Exception as e:
            logger.exception(f"metrics failed for {w[:10]}: {e}")
        if i % 100 == 0 or i == len(wallets):
            logger.info(f"  {i}/{len(wallets)} processed")

    if not rows:
        logger.error("Zero rows.")
        return

    df = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(df):,} rows to {out_path}")

    n_elig = int(df["eligible"].sum())
    n_liq = int((df["liquidation_events"] > 0).sum())
    logger.info(f"Eligible: {n_elig} / {len(df)} ({100*n_elig/len(df):.1f}%)")
    logger.info(f"Wallets with liquidation event in window: {n_liq}")
    if n_elig > 0:
        top = df[df["eligible"]].sort_values("wallet_score", ascending=False).head(10)
        logger.info("Top 10 eligible by wallet_score:")
        for _, r in top.iterrows():
            logger.info(
                f"  {r['wallet'][:10]}.. score={r['wallet_score']:>8.3f} "
                f"after_fee_sharpe={r['after_fee_sharpe']:>5.2f} hold_h={r['median_holding_hours']:>5.1f} "
                f"trades={r['trade_count']:>5} fee_drag={r['fee_drag']:.2f}"
            )


if __name__ == "__main__":
    main()
