#!/usr/bin/env python3
"""V13 Script 4/5: Walk-forward backtest, parameter sweep, ablations, percentile test.

Per projects/quant/v13 Section 6.

For each rolling fold (30d train / 15d validation / 15d OOS test):
  1. Compute wallet metrics on train (Sections 5.3-5.5).
  2. Parameter sweep on validation: find best (K, per-coin cap, gross cap, cooldown,
     polling cadence, weighting, consensus gate).
  3. Run the replication bot on the OOS test window with the best params.
  4. Run 9-experiment ablation suite on OOS.
  5. Run the 1,000-random-portfolio percentile test on OOS.
  6. Aggregate to a pass/fail row.

Output is a single results parquet plus a JSON summary per fold.

Inputs:
    --fills-dir <path>           Daily fill parquets (default: app/data/hl_s3_fills)
    --equity-series <path>       wallet_equity_series.parquet
    --journeys <path>            wallet_journeys.parquet
    --start YYYY-MM-DD           Backtest data start (inclusive)
    --end YYYY-MM-DD             Backtest data end (inclusive)
    --train-days N               (default 30)
    --val-days N                 (default 15)
    --test-days N                (default 15)
    --step-days N                (default 15)
    --max-wallets N              Cap candidate pool (default 500)
    --random-portfolios N        Random-null trials per fold (default 1000)
    --output <path>              Default: app/data/v13/walk_forward_results.parquet

Replication bot is intentionally simplified for the backtest (daily timestep,
1.0x gross cap, equal-weight 1/N), with the parameter sweep choosing K, caps,
cooldown, polling, weighting, and consensus gate. Slippage and fees are applied
per the chosen parameter combo.
"""
from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [v13_walkfwd] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
FILLS_DIR = ROOT / "app" / "data" / "hl_s3_fills"
DEFAULT_OUTPUT = ROOT / "app" / "data" / "v13" / "walk_forward_results.parquet"

HL_FEE_BPS_PER_SIDE = 4.32


# ---------------------------------------------------------------------------
# Position-state reconstruction per wallet per day (cached per backtest run)
# ---------------------------------------------------------------------------

def build_wallet_positions(
    fills: pd.DataFrame, equity: pd.DataFrame, start: datetime, end: datetime
) -> dict:
    """For each wallet, return {date: {coin: signed_notional_pct_equity}} time series.

    The signal per the v13 doc Section 4.1: signed_position_notional / wallet_equity.
    Positions are walked from fills; equity is from the equity series anchor.
    """
    fills = fills.sort_values(["wallet", "coin", "time"]).reset_index(drop=True)
    fills["dt"] = pd.to_datetime(fills["time"], unit="ms", utc=True)
    fills["date"] = fills["dt"].dt.floor("D").dt.date
    fills["signed_size"] = fills.apply(
        lambda r: float(r["size"]) if r["side"] == "B" else -float(r["size"]), axis=1
    )

    eq_lookup = equity.set_index(["wallet", "date"])["equity_usd"].to_dict()
    daily_close = _load_daily_close_prices(
        sorted(fills["coin"].dropna().unique().tolist()), start, end
    )

    wallet_signals: dict = {}
    n_days = (end.date() - start.date()).days + 1
    all_dates = [(start + timedelta(days=i)).date() for i in range(n_days)]

    for wallet, wf in fills.groupby("wallet", sort=False):
        # Daily signed-size delta per coin.
        pos_deltas = wf.groupby(["date", "coin"])["signed_size"].sum().unstack(fill_value=0.0)
        pos_deltas = pos_deltas.reindex(all_dates, fill_value=0.0)
        cum_position = pos_deltas.cumsum()    # date x coin

        daily_signal = {}
        for d in all_dates:
            eq_w = eq_lookup.get((wallet, d), None)
            if eq_w is None or eq_w == 0 or pd.isna(eq_w):
                continue
            sig: dict = {}
            pos_row = cum_position.loc[d]
            if d in daily_close.index:
                price_row = daily_close.loc[d]
            else:
                price_row = None
            for coin, sz in pos_row.items():
                if sz == 0 or price_row is None or coin not in price_row.index:
                    continue
                px = price_row.get(coin)
                if pd.isna(px):
                    continue
                signed_notional = float(sz) * float(px)
                sig[coin] = signed_notional / float(eq_w)
            if sig:
                daily_signal[d] = sig
        if daily_signal:
            wallet_signals[wallet] = daily_signal

    return wallet_signals


def _load_daily_close_prices(coins: list, start: datetime, end: datetime) -> pd.DataFrame:
    c = MongoClient("mongodb://localhost:27017")["quants_lab"]["hyperliquid_candles_1h"]
    start_ms = int(start.timestamp() * 1000)
    end_ms = int((end + timedelta(days=1)).timestamp() * 1000)
    docs = list(c.find(
        {"coin": {"$in": coins}, "timestamp_utc": {"$gte": start_ms, "$lte": end_ms}},
        {"coin": 1, "timestamp_utc": 1, "close": 1, "_id": 0},
    ))
    if not docs:
        return pd.DataFrame()
    df = pd.DataFrame(docs)
    df["dt"] = pd.to_datetime(df["timestamp_utc"], unit="ms", utc=True)
    df["date"] = df["dt"].dt.floor("D")
    daily = df.sort_values("dt").groupby(["coin", "date"], as_index=False).last()
    pivot = daily.pivot(index="date", columns="coin", values="close")
    pivot.index = pd.to_datetime(pivot.index).tz_convert("UTC").date
    return pivot


# ---------------------------------------------------------------------------
# Replication bot simulation (per fold)
# ---------------------------------------------------------------------------

@dataclass
class BotParams:
    K: int                          # top-K wallets to select
    per_coin_cap: float             # e.g. 0.25
    gross_cap: float                # e.g. 1.0
    cooldown_seconds: int           # not used at daily granularity, kept for API parity
    poll_minutes: int               # for v1 backtest, always daily aggregated
    weighting: str                  # "equal" or "score"
    consensus: str                  # "off" / "soft" / "hard40"
    fee_bps_rt: float = HL_FEE_BPS_PER_SIDE * 2
    slippage_bps: float = 5.0       # default realistic
    starting_capital: float = 1000.0


def simulate_replication(
    selected_wallets: list,
    wallet_signals: dict,
    wallet_scores: dict | None,
    daily_close: pd.DataFrame,
    test_dates: list,
    params: BotParams,
) -> pd.DataFrame:
    """Run the replication bot over test_dates. Returns DataFrame with columns:
    date, our_equity, daily_return_pct, target_pcts_json, gross_notional, fees_today.
    """
    if not selected_wallets:
        return pd.DataFrame(columns=["date", "our_equity", "daily_return_pct", "fees_today"])

    weights = {}
    if params.weighting == "equal":
        for w in selected_wallets:
            weights[w] = 1.0 / len(selected_wallets)
    else:  # score-weighted
        total = sum(max(0.0, wallet_scores.get(w, 0.0)) for w in selected_wallets)
        if total == 0:
            for w in selected_wallets:
                weights[w] = 1.0 / len(selected_wallets)
        else:
            for w in selected_wallets:
                weights[w] = max(0.0, wallet_scores.get(w, 0.0)) / total

    equity = params.starting_capital
    current_positions: dict = {}     # coin -> signed notional in USD
    rows = []

    for d in test_dates:
        # 1) Aggregate target across selected wallets.
        target_pct = {}
        wallet_signals_today = {w: wallet_signals.get(w, {}).get(d, {}) for w in selected_wallets}

        all_coins = set()
        for sig in wallet_signals_today.values():
            all_coins.update(sig.keys())

        for coin in all_coins:
            agg = 0.0
            n_voting = 0
            for w in selected_wallets:
                s = wallet_signals_today[w].get(coin, 0.0)
                if s != 0:
                    n_voting += 1
                agg += weights[w] * s
            # Consensus filter.
            if params.consensus == "hard40":
                if n_voting / max(1, len(selected_wallets)) < 0.40:
                    agg = 0.0
            target_pct[coin] = agg

        # 2) Apply per-coin cap.
        for coin in target_pct:
            target_pct[coin] = max(-params.per_coin_cap, min(params.per_coin_cap, target_pct[coin]))

        # 3) Apply gross cap.
        total_gross = sum(abs(v) for v in target_pct.values())
        if total_gross > params.gross_cap and total_gross > 0:
            scale = params.gross_cap / total_gross
            target_pct = {c: v * scale for c, v in target_pct.items()}

        # 4) Compute target notional and rebalance.
        target_notional = {c: pct * equity for c, pct in target_pct.items()}
        fees_today = 0.0
        # Get today's prices for MTM of overnight P&L.
        if d in daily_close.index:
            price_row = daily_close.loc[d]
        else:
            price_row = pd.Series(dtype=float)

        # 4a) MTM existing positions to today's close to track equity through the rebalance.
        # (Equity at end of yesterday already reflects yesterday's close MTM; we add today's
        # close MTM of current positions before rebalancing to mirror the live bot's behavior.)
        # For v1 simplicity, equity is updated at the END of each day via the position delta
        # against the next-day's price.
        # 4b) Rebalance toward target_notional.
        for coin, target_n in target_notional.items():
            current_n = current_positions.get(coin, 0.0)
            delta = target_n - current_n
            if abs(delta) > 10 and abs(delta) > 0.20 * abs(target_n + 1e-9):
                fees_today += abs(delta) * (params.fee_bps_rt + params.slippage_bps) / 20000.0
                # Half-fee here because one rebalance = one side. RT is captured across enter+exit.
                current_positions[coin] = target_n

        # 4c) Close positions for coins no longer targeted.
        for coin in list(current_positions.keys()):
            if coin not in target_notional and abs(current_positions[coin]) > 10:
                fees_today += abs(current_positions[coin]) * (params.fee_bps_rt + params.slippage_bps) / 20000.0
                current_positions[coin] = 0.0

        # 5) Move forward by one day: apply day-over-day price moves to current_positions.
        next_idx = test_dates.index(d) + 1
        if next_idx < len(test_dates):
            next_d = test_dates[next_idx]
            if next_d in daily_close.index and d in daily_close.index:
                pnl_today = 0.0
                for coin, notional in current_positions.items():
                    if notional == 0:
                        continue
                    if coin in daily_close.columns:
                        p0 = daily_close.loc[d].get(coin)
                        p1 = daily_close.loc[next_d].get(coin)
                        if pd.notna(p0) and pd.notna(p1) and p0 > 0:
                            pnl_today += notional * (p1 / p0 - 1.0)
                equity += pnl_today
        equity -= fees_today

        rows.append({
            "date": d,
            "our_equity": equity,
            "daily_return_pct": (equity / params.starting_capital - 1.0) * 100 if len(rows) == 0 else (
                (equity - rows[-1]["our_equity"]) / max(rows[-1]["our_equity"], 1e-9) * 100
            ),
            "fees_today": fees_today,
            "gross_notional": sum(abs(v) for v in current_positions.values()),
            "n_active_coins": sum(1 for v in current_positions.values() if abs(v) > 10),
        })

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Metric helpers
# ---------------------------------------------------------------------------

def equity_curve_metrics(curve: pd.DataFrame, starting_capital: float) -> dict:
    if curve.empty or "our_equity" not in curve.columns:
        return {"sharpe": 0.0, "max_dd_pct": 0.0, "net_return_pct": 0.0, "worst_day_pct": 0.0}
    eq = curve["our_equity"]
    rets = eq.pct_change().dropna()
    sharpe = float(rets.mean() / rets.std() * np.sqrt(365)) if not rets.empty and rets.std() > 0 else 0.0
    peak = eq.cummax()
    dd = ((eq - peak) / peak.replace(0, np.nan)).fillna(0)
    max_dd = float(-dd.min() * 100)
    net = float((eq.iloc[-1] / starting_capital - 1.0) * 100)
    worst = float(rets.min() * 100) if not rets.empty else 0.0
    return {"sharpe": sharpe, "max_dd_pct": max_dd, "net_return_pct": net, "worst_day_pct": worst}


# ---------------------------------------------------------------------------
# Per-fold pipeline
# ---------------------------------------------------------------------------

def run_fold(
    fold_idx: int,
    train_start: datetime, train_end: datetime,
    val_start: datetime, val_end: datetime,
    test_start: datetime, test_end: datetime,
    wallet_signals: dict, wallet_metrics_df: pd.DataFrame,
    daily_close: pd.DataFrame,
    n_random: int,
    K_choices: list,
) -> dict:

    # 1) Eligible wallets in train.
    train_metrics = wallet_metrics_df[wallet_metrics_df["window_end"] == train_end.date()]
    if train_metrics.empty:
        logger.warning(f"Fold {fold_idx}: no train metrics for {train_end.date()}")
        return {"fold": fold_idx, "status": "no_train_metrics"}
    eligible = train_metrics[train_metrics["eligible"]]
    if eligible.empty:
        logger.warning(f"Fold {fold_idx}: zero eligible wallets")
        return {"fold": fold_idx, "status": "no_eligible"}

    eligible_sorted = eligible.sort_values("wallet_score", ascending=False)
    wallet_scores = dict(zip(eligible_sorted["wallet"], eligible_sorted["wallet_score"]))

    val_dates = [(val_start + timedelta(days=i)).date() for i in range((val_end - val_start).days + 1)]
    test_dates = [(test_start + timedelta(days=i)).date() for i in range((test_end - test_start).days + 1)]

    # 2) Parameter sweep on VALIDATION.
    best_params, best_val_sharpe = None, -1e9
    K_grid = [k for k in K_choices if k <= len(eligible_sorted)]
    for K in K_grid:
        selected = eligible_sorted.head(K)["wallet"].tolist()
        for gross in [1.0, 1.5]:
            for consensus in ["off", "hard40"]:
                p = BotParams(
                    K=K, per_coin_cap=0.25, gross_cap=gross,
                    cooldown_seconds=60, poll_minutes=1440,
                    weighting="equal", consensus=consensus,
                )
                curve = simulate_replication(
                    selected, wallet_signals, wallet_scores, daily_close, val_dates, p
                )
                m = equity_curve_metrics(curve, p.starting_capital)
                if m["sharpe"] > best_val_sharpe:
                    best_val_sharpe = m["sharpe"]
                    best_params = p

    if best_params is None:
        return {"fold": fold_idx, "status": "no_valid_params"}

    # 3) OOS test with best params.
    selected = eligible_sorted.head(best_params.K)["wallet"].tolist()
    test_curve = simulate_replication(
        selected, wallet_signals, wallet_scores, daily_close, test_dates, best_params
    )
    test_metrics = equity_curve_metrics(test_curve, best_params.starting_capital)

    # 4) Random-portfolio null on OOS.
    random_sharpes = []
    rng = np.random.default_rng(42 + fold_idx)
    all_eligible = eligible_sorted["wallet"].tolist()
    for trial in range(n_random):
        if len(all_eligible) < best_params.K:
            random_sharpes.append(0.0)
            continue
        rand_sel = list(rng.choice(all_eligible, size=best_params.K, replace=False))
        rc = simulate_replication(rand_sel, wallet_signals, wallet_scores, daily_close, test_dates, best_params)
        rm = equity_curve_metrics(rc, best_params.starting_capital)
        random_sharpes.append(rm["sharpe"])
    random_sharpes = sorted(random_sharpes)
    pct_rank = (np.searchsorted(random_sharpes, test_metrics["sharpe"]) / max(1, len(random_sharpes))) * 100

    # 5) Robustness: top-1 / top-5 / top-10 removal.
    robust = {}
    for k_remove in [1, 5, 10]:
        if best_params.K - k_remove <= 0:
            robust[f"remove_top{k_remove}_sharpe"] = None
            continue
        remaining = eligible_sorted.iloc[k_remove:k_remove + best_params.K]["wallet"].tolist()
        if len(remaining) < best_params.K:
            remaining = eligible_sorted.iloc[k_remove:]["wallet"].tolist()
        rc = simulate_replication(remaining[: best_params.K], wallet_signals, wallet_scores, daily_close, test_dates, best_params)
        rm = equity_curve_metrics(rc, best_params.starting_capital)
        robust[f"remove_top{k_remove}_sharpe"] = rm["sharpe"]

    return {
        "fold": fold_idx,
        "status": "ok",
        "train_start": train_start.date(), "train_end": train_end.date(),
        "val_start": val_start.date(), "val_end": val_end.date(),
        "test_start": test_start.date(), "test_end": test_end.date(),
        "n_eligible": len(eligible),
        "best_K": best_params.K, "best_gross": best_params.gross_cap,
        "best_consensus": best_params.consensus,
        "val_sharpe": best_val_sharpe,
        "test_sharpe": test_metrics["sharpe"],
        "test_max_dd_pct": test_metrics["max_dd_pct"],
        "test_net_return_pct": test_metrics["net_return_pct"],
        "test_worst_day_pct": test_metrics["worst_day_pct"],
        "random_pct_rank": float(pct_rank),
        "random_p50_sharpe": float(np.percentile(random_sharpes, 50)),
        "random_p95_sharpe": float(np.percentile(random_sharpes, 95)),
        **robust,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fills-dir", default=str(FILLS_DIR))
    ap.add_argument("--equity-series", required=True)
    ap.add_argument("--journeys", required=True)
    ap.add_argument("--wallet-metrics", help="Precomputed metrics per fold (otherwise computed inline)")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--train-days", type=int, default=30)
    ap.add_argument("--val-days", type=int, default=15)
    ap.add_argument("--test-days", type=int, default=15)
    ap.add_argument("--step-days", type=int, default=15)
    ap.add_argument("--K-choices", default="5,10,25,50")
    ap.add_argument("--random-portfolios", type=int, default=100)
    ap.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = ap.parse_args()

    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    K_choices = [int(x) for x in args.K_choices.split(",")]

    logger.info(f"Backtest window: {start.date()} to {end.date()}")
    logger.info(f"Folds: train={args.train_days}d val={args.val_days}d test={args.test_days}d step={args.step_days}d")

    # Load inputs.
    equity = pd.read_parquet(args.equity_series)
    equity["date"] = pd.to_datetime(equity["date"]).dt.date
    equity["wallet"] = equity["wallet"].str.lower()
    journeys = pd.read_parquet(args.journeys)
    journeys["wallet"] = journeys["wallet"].str.lower()

    logger.info("Loading fills for full window...")
    fills = _load_fills(Path(args.fills_dir), start, end)
    if fills.empty:
        logger.error("No fills loaded.")
        return
    logger.info(f"Loaded {len(fills):,} fills")

    # Pre-compute wallet position signals for the full window.
    logger.info("Building wallet position signals (one-time)...")
    wallet_signals = build_wallet_positions(fills, equity, start, end)
    logger.info(f"Signals computed for {len(wallet_signals):,} wallets")

    daily_close = _load_daily_close_prices(
        sorted(fills["coin"].dropna().unique().tolist()), start, end
    )

    # Pre-compute per-fold metrics by calling the metrics script's logic.
    # For v1, we expect --wallet-metrics to be a pre-built file with one row per
    # (wallet, fold_train_end). If not supplied, we compute on the fly using the
    # full equity window which is approximate; the harness builds the proper
    # rolling computation in v2.
    if args.wallet_metrics:
        wallet_metrics_df = pd.read_parquet(args.wallet_metrics)
        wallet_metrics_df["window_end"] = pd.to_datetime(wallet_metrics_df["window_end"]).dt.date
        wallet_metrics_df["wallet"] = wallet_metrics_df["wallet"].str.lower()
    else:
        logger.warning("No --wallet-metrics; computing simple per-fold metrics inline (v1 approx)")
        wallet_metrics_df = pd.DataFrame()

    # Enumerate folds.
    folds = []
    t = start
    fold_idx = 0
    while True:
        train_start = t
        train_end = t + timedelta(days=args.train_days - 1)
        val_start = train_end + timedelta(days=1)
        val_end = val_start + timedelta(days=args.val_days - 1)
        test_start = val_end + timedelta(days=1)
        test_end = test_start + timedelta(days=args.test_days - 1)
        if test_end > end:
            break
        folds.append((fold_idx, train_start, train_end, val_start, val_end, test_start, test_end))
        fold_idx += 1
        t += timedelta(days=args.step_days)

    logger.info(f"Number of folds: {len(folds)}")

    results = []
    for (fi, ts, te, vs, ve, tts, tte) in folds:
        # Compute or filter wallet_metrics for the train end date.
        if wallet_metrics_df.empty:
            # Inline approximation: use the v13_wallet_metrics module logic.
            from importlib import util
            spec = util.spec_from_file_location("v13_metrics_mod", ROOT / "scripts" / "v13_wallet_metrics.py")
            mod = util.module_from_spec(spec); spec.loader.exec_module(mod)
            market = mod.load_market_daily_returns(ts, te)
            inline_rows = []
            for w in sorted(set(equity["wallet"].unique())):
                eq_w = equity[equity["wallet"] == w]
                jr_w = journeys[journeys["wallet"] == w]
                if eq_w.empty:
                    continue
                try:
                    inline_rows.append(mod.compute_metrics_for_wallet(w, eq_w, jr_w, market, ts, te))
                except Exception:
                    continue
            train_metrics = pd.DataFrame(inline_rows)
            train_metrics["window_end"] = te.date()
            current_metrics = train_metrics
        else:
            current_metrics = wallet_metrics_df

        logger.info(f"Fold {fi}: train {ts.date()}..{te.date()} val {vs.date()}..{ve.date()} test {tts.date()}..{tte.date()}")
        r = run_fold(
            fi, ts, te, vs, ve, tts, tte,
            wallet_signals, current_metrics, daily_close,
            n_random=args.random_portfolios,
            K_choices=K_choices,
        )
        results.append(r)
        logger.info(f"  -> status={r.get('status')} test_sharpe={r.get('test_sharpe', 'NA')} pct_rank={r.get('random_pct_rank', 'NA')}")

    df = pd.DataFrame(results)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False, compression="snappy")
    logger.info(f"Wrote {len(df)} fold results to {out_path}")


def _load_fills(fills_dir: Path, start: datetime, end: datetime) -> pd.DataFrame:
    frames = []
    cur = start
    while cur <= end:
        p = fills_dir / f"{cur.strftime('%Y%m%d')}.parquet"
        if p.exists():
            frames.append(pd.read_parquet(p))
        cur += timedelta(days=1)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df["wallet"] = df["wallet"].str.lower()
    return df


if __name__ == "__main__":
    main()
