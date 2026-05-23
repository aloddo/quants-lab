#!/usr/bin/env python3
"""V13 Script 3/5: Wallet-level metrics + eligibility + composite score.

Per projects/quant/v13 Sections 5.3, 5.4, 5.5.

Inputs:
    --journeys <path>       wallet_journeys.parquet from script 2
    --equity-series <path>  wallet_equity_series.parquet from script 1
    --window-start YYYY-MM-DD   Start of metric window (inclusive)
    --window-end YYYY-MM-DD     End of metric window (inclusive)
    --output <path>         Default: app/data/v13/wallet_metrics.parquet

Outputs (parquet, one row per wallet):
    wallet, window_start, window_end,
    -- 5.3 metrics --
    daily_return_mean_pct, sharpe_pct, sortino_pct, max_dd_pct,
    worst_single_day_pct, journey_win_rate, profit_factor,
    avg_win_bps, avg_loss_bps, median_journey_bps,
    median_journey_fills, turnover_per_day, median_holding_hours,
    btc_beta, eth_beta, btc_eth_r2, hl_index_beta,
    trade_count, pnl_concentration_coin, pnl_concentration_day,
    half_window_sharpe_drop, active_days, active_in_last_7d,
    -- 5.4 eligibility --
    eligible,
    -- 5.5 composite score --
    after_fee_sharpe, consistency_factor, copyability_factor,
    diversification_factor, survival_factor, wallet_score

Usage:
    python scripts/v13_wallet_metrics.py \\
        --journeys app/data/v13/wallet_journeys.parquet \\
        --equity-series app/data/v13/wallet_equity_series.parquet \\
        --window-start 2026-04-01 --window-end 2026-04-30
"""
from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_metrics] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "wallet_metrics.parquet"

# HL round-trip taker fee in bps, per side (entry or exit).
HL_FEE_BPS_PER_SIDE = 4.32


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _daily_returns(equity_series: pd.Series) -> pd.Series:
    """Daily % returns; equity_series indexed by date, ascending."""
    return equity_series.pct_change().dropna()


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
    """Max peak-to-trough drawdown as a positive fraction (0..1)."""
    if equity.empty:
        return 0.0
    peak = equity.cummax()
    dd = (equity - peak) / peak.replace(0, np.nan)
    dd = dd.fillna(0)
    return float(-dd.min()) if not dd.empty else 0.0


def _herfindahl(shares: pd.Series) -> float:
    """Herfindahl index over shares (which sum to 1)."""
    if shares.empty or shares.sum() == 0:
        return 0.0
    s = shares / shares.sum()
    return float((s ** 2).sum())


def _beta(strategy_returns: pd.Series, market_returns: pd.Series) -> tuple[float, float]:
    """OLS beta and R^2 of strategy on market. Returns (beta, R^2)."""
    s = strategy_returns.align(market_returns, join="inner")
    sr, mr = s[0], s[1]
    if len(sr) < 5 or mr.var() == 0:
        return 0.0, 0.0
    beta = float(np.cov(sr, mr)[0, 1] / mr.var())
    pred = beta * (mr - mr.mean()) + sr.mean()
    ss_res = ((sr - pred) ** 2).sum()
    ss_tot = ((sr - sr.mean()) ** 2).sum()
    r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0
    return beta, max(0.0, r2)


def _btc_eth_combined_r2(strategy_returns: pd.Series, btc: pd.Series, eth: pd.Series) -> float:
    """R^2 of OLS regression on (BTC, ETH) jointly."""
    df = pd.concat({"s": strategy_returns, "b": btc, "e": eth}, axis=1).dropna()
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
# Market reference series (BTC, ETH, HL perp index)
# ---------------------------------------------------------------------------

def load_market_daily_returns(start: datetime, end: datetime) -> dict:
    """Return {'BTC': series, 'ETH': series, 'HL_INDEX': series} of daily % returns."""
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
    # HL index = equal-weight basket of all coins with full coverage
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
    eq_df: pd.DataFrame,
    jr_df: pd.DataFrame,
    market: dict,
    window_start: datetime,
    window_end: datetime,
) -> dict:
    """Returns metrics dict for one wallet over the window."""
    # 1) Equity series within window.
    eq = eq_df.sort_values("date").set_index("date")["equity_usd"]
    eq.index = pd.to_datetime(eq.index).date if not isinstance(eq.index[0], type(window_start.date())) else eq.index
    eq = eq[(eq.index >= window_start.date()) & (eq.index <= window_end.date())]
    if eq.empty or len(eq) < 2:
        return {"wallet": wallet, "active_days": 0}

    returns = _daily_returns(eq)
    # 2) Journey window.
    jr = jr_df.copy()
    jr["entry_dt"] = pd.to_datetime(jr["entry_ts"], unit="ms", utc=True)
    jr["entry_date"] = jr["entry_dt"].dt.date
    jr = jr[(jr["entry_date"] >= window_start.date()) & (jr["entry_date"] <= window_end.date())]

    # 3) Metrics.
    m: dict = {"wallet": wallet, "window_start": window_start.date(), "window_end": window_end.date()}

    m["daily_return_mean_pct"] = float(returns.mean() * 100) if not returns.empty else 0.0
    m["sharpe_pct"] = _sharpe(returns)
    m["sortino_pct"] = _sortino(returns)
    m["max_dd_pct"] = float(_max_drawdown(eq) * 100)
    m["worst_single_day_pct"] = float(returns.min() * 100) if not returns.empty else 0.0
    m["active_days"] = int((eq != 0).sum())

    last_7_start = window_end - timedelta(days=7)
    m["active_in_last_7d"] = int(
        ((eq.index >= last_7_start.date()) & (eq != 0)).any() and not jr[jr["entry_date"] >= last_7_start.date()].empty
    )

    if jr.empty:
        m.update({
            "journey_win_rate": 0.0, "profit_factor": 0.0,
            "avg_win_bps": 0.0, "avg_loss_bps": 0.0,
            "median_journey_bps": 0.0, "median_journey_fills": 0,
            "turnover_per_day": 0.0, "median_holding_hours": 0.0,
            "trade_count": 0, "pnl_concentration_coin": 0.0,
            "pnl_concentration_day": 0.0,
        })
    else:
        pnl = jr["realized_pnl_usd"]
        m["journey_win_rate"] = float((pnl > 0).mean())
        pos = pnl[pnl > 0].sum()
        neg = -pnl[pnl < 0].sum()
        m["profit_factor"] = float(pos / neg) if neg > 0 else (float("inf") if pos > 0 else 0.0)
        wins = jr.loc[pnl > 0, "pnl_bps_of_max"]
        losses = jr.loc[pnl < 0, "pnl_bps_of_max"]
        m["avg_win_bps"] = float(wins.mean()) if not wins.empty else 0.0
        m["avg_loss_bps"] = float(losses.mean()) if not losses.empty else 0.0
        m["median_journey_bps"] = float(jr["pnl_bps_of_max"].median())
        m["median_journey_fills"] = int(jr["n_fills_total"].median())
        # turnover = sum of max notional per journey / window_days / median equity in window
        med_eq = float(eq.median()) if eq.median() > 0 else 1.0
        n_days = max(1, (window_end.date() - window_start.date()).days)
        m["turnover_per_day"] = float(jr["max_position_notional_usd"].sum() / n_days / med_eq)
        m["median_holding_hours"] = float(jr["duration_hours"].median())
        m["trade_count"] = int(jr["n_fills_total"].sum())

        per_coin_pnl = jr.groupby("coin")["realized_pnl_usd"].sum().abs()
        m["pnl_concentration_coin"] = _herfindahl(per_coin_pnl)
        per_day_pnl = jr.groupby("entry_date")["realized_pnl_usd"].sum().abs()
        m["pnl_concentration_day"] = (
            float(per_day_pnl.max() / per_day_pnl.sum()) if per_day_pnl.sum() > 0 else 0.0
        )

    # 4) Beta vs BTC, ETH, HL_INDEX.
    if "BTC" in market:
        m["btc_beta"], _ = _beta(returns, market["BTC"])
    else:
        m["btc_beta"] = 0.0
    if "ETH" in market:
        m["eth_beta"], _ = _beta(returns, market["ETH"])
    else:
        m["eth_beta"] = 0.0
    if "BTC" in market and "ETH" in market:
        m["btc_eth_r2"] = _btc_eth_combined_r2(returns, market["BTC"], market["ETH"])
    else:
        m["btc_eth_r2"] = 0.0
    if "HL_INDEX" in market:
        m["hl_index_beta"], _ = _beta(returns, market["HL_INDEX"])
    else:
        m["hl_index_beta"] = 0.0

    # 5) Half-window Sharpe drop.
    if len(returns) >= 8:
        mid = len(returns) // 2
        first_half = _sharpe(returns.iloc[:mid])
        second_half = _sharpe(returns.iloc[mid:])
        m["half_window_sharpe_drop"] = float(second_half / first_half) if first_half != 0 else 0.0
    else:
        m["half_window_sharpe_drop"] = 0.0

    # 6) Section 5.4 eligibility.
    # fee_drag = total fees paid / gross realized PnL (both in USD).
    # Per-journey fee: round-trip = 2 * fee_per_side, applied to max notional.
    # If a journey has many addons / trims, real fee is higher; this is a
    # conservative-low estimate for v1.
    fee_drag = 0.0
    if not jr.empty:
        total_fees_usd = float(jr["max_position_notional_usd"].sum()) * 2.0 * HL_FEE_BPS_PER_SIDE / 10000.0
        gross_pnl_usd = float(jr.loc[jr["realized_pnl_usd"] > 0, "realized_pnl_usd"].sum())
        if gross_pnl_usd > 0:
            fee_drag = total_fees_usd / gross_pnl_usd
    m["fee_drag"] = float(min(fee_drag, 10.0))    # capped sentinel

    eligible = (
        m["active_days"] >= 15
        and m["trade_count"] >= 30
        and m["max_dd_pct"] < 50.0
        and m["median_holding_hours"] * 60 > 5
        and m["fee_drag"] < 0.40
        and m["pnl_concentration_day"] < 0.50
        and m["btc_eth_r2"] < 0.50
        and m["active_in_last_7d"] == 1
    )
    m["eligible"] = bool(eligible)

    # 7) Section 5.5 composite wallet score.
    gross_sharpe = m["sharpe_pct"]
    # after-fee approximation: deduct (fee_drag * gross_pnl_bps) from Sharpe by scaling
    after_fee_sharpe = gross_sharpe * max(0.0, 1.0 - m["fee_drag"])
    m["after_fee_sharpe"] = after_fee_sharpe

    pct_profitable_days = float((returns > 0).mean())
    m["consistency_factor"] = pct_profitable_days * max(0.0, 1.0 - m["max_dd_pct"] / 100.0)

    hold_min = m["median_holding_hours"] * 60
    m["copyability_factor"] = (
        min(hold_min / 30.0, 1.0) * max(0.0, 1.0 - min(m["turnover_per_day"] / 5.0, 1.0))
    )

    m["diversification_factor"] = max(0.0, 1.0 - m["pnl_concentration_coin"])

    near_liq_proxy = 0    # v1: not computed (needs margin_ratio history)
    m["survival_factor"] = m["active_in_last_7d"] * max(0.0, 1.0 - near_liq_proxy / 5.0)

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
    ap.add_argument("--window-start", required=True)
    ap.add_argument("--window-end", required=True)
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    window_start = datetime.strptime(args.window_start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    window_end = datetime.strptime(args.window_end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    logger.info(f"Window: {window_start.date()} to {window_end.date()}")
    logger.info("Loading inputs...")
    journeys = pd.read_parquet(args.journeys)
    equity = pd.read_parquet(args.equity_series)
    equity["date"] = pd.to_datetime(equity["date"]).dt.date
    equity["wallet"] = equity["wallet"].str.lower()
    journeys["wallet"] = journeys["wallet"].str.lower()

    logger.info("Loading market reference (BTC, ETH, HL_INDEX)...")
    market = load_market_daily_returns(window_start, window_end)
    if not market:
        logger.warning("No market data loaded; beta metrics will be zero.")

    wallets = sorted(set(equity["wallet"].unique()) | set(journeys["wallet"].unique()))
    logger.info(f"Computing metrics for {len(wallets)} wallets...")

    rows = []
    for i, w in enumerate(wallets, 1):
        eq_w = equity[equity["wallet"] == w]
        jr_w = journeys[journeys["wallet"] == w]
        if eq_w.empty:
            continue
        try:
            row = compute_metrics_for_wallet(w, eq_w, jr_w, market, window_start, window_end)
            rows.append(row)
        except Exception as e:
            logger.exception(f"metric compute failed for {w[:10]}: {e}")
        if i % 100 == 0 or i == len(wallets):
            logger.info(f"  {i}/{len(wallets)} processed")

    if not rows:
        logger.error("Zero wallets produced metrics.")
        return

    df = pd.DataFrame(rows)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(df):,} rows to {out_path}")

    # Summary.
    n_elig = int(df["eligible"].sum())
    logger.info(f"Eligible wallets: {n_elig} / {len(df)} ({100*n_elig/len(df):.1f}%)")
    if n_elig > 0:
        top = df[df["eligible"]].sort_values("wallet_score", ascending=False).head(10)
        logger.info("Top 10 by wallet_score (eligible only):")
        for _, r in top.iterrows():
            logger.info(
                f"  {r['wallet'][:10]}.. score={r['wallet_score']:>8.3f} "
                f"sharpe={r['sharpe_pct']:>5.2f} hold_h={r['median_holding_hours']:>5.1f} "
                f"trades={r['trade_count']:>5} maxdd={r['max_dd_pct']:>4.1f}% "
                f"btc_eth_r2={r['btc_eth_r2']:.2f}"
            )


if __name__ == "__main__":
    main()
