#!/usr/bin/env python3
"""
Walk-forward validation + parameter sensitivity for X7 stable_buy_usd signal.

Tasks:
  1. Universe: BTC, ETH, SOL, DOGE (XRP excluded). Generalizable to 25 Bybit pairs.
  2. Rolling walk-forward: 30d train → 15d test, step 15d. Learn orientation on train.
  3. Parameter sensitivity grid: z_window × lag × horizon × fee_oneway.

Output artifacts → app/data/cache/onchain_x7_deep_eda/
  - walkforward_folds.csv
  - walkforward_equity.csv
  - param_sensitivity_grid.csv
  - param_sensitivity_summary.json
"""

from __future__ import annotations

import json
from pathlib import Path
from itertools import product

import numpy as np
import pandas as pd
from scipy.stats import spearmanr


# ─── Config ──────────────────────────────────────────────────────────────────

PANEL_PATH = Path("app/data/cache/onchain_x7_advanced/20260420_154932_feature_panel.csv")
OUT_DIR = Path("app/data/cache/onchain_x7_deep_eda")

# TODO: When Dune data arrives for full 25-pair universe, add assets here.
# The code is asset-agnostic — just extend this list and provide panel data.
ASSETS = ["BTC", "ETH", "SOL", "DOGE"]

# Walk-forward parameters (baseline)
WF_TRAIN_DAYS = 30
WF_TEST_DAYS = 15
WF_STEP_DAYS = 15
WF_Z_WINDOW = 24
WF_LAG = 1
WF_HORIZON = 4
WF_FEE = 0.00055

# Parameter sensitivity grid
GRID_Z_WINDOWS = [12, 24, 48, 72]
GRID_LAGS = [1, 2, 3, 4]
GRID_HORIZONS = [4, 8]
GRID_FEES = [0.00035, 0.00055, 0.00075, 0.001]


# ─── Helpers ──────────────────────────────────────────────────────────────────

def load_panel() -> pd.DataFrame:
    df = pd.read_csv(PANEL_PATH)
    df["hour"] = pd.to_datetime(df["hour"], utc=True)
    df = df[df["asset"].isin(ASSETS)].copy()
    return df.sort_values(["asset", "hour"]).reset_index(drop=True)


def compute_z_score(series: pd.Series, window: int) -> pd.Series:
    """Rolling z-score."""
    mu = series.rolling(window, min_periods=max(window // 2, 2)).mean()
    sigma = series.rolling(window, min_periods=max(window // 2, 2)).std()
    return (series - mu) / sigma.replace(0, np.nan)


def get_signal_col(df: pd.DataFrame, z_window: int) -> pd.Series:
    """Get or compute the z-score signal column for given window."""
    col_name = f"stable_buy_usd_z{z_window}"
    if col_name in df.columns:
        return df[col_name]
    # Compute from raw stable_buy_usd per asset
    result = pd.Series(np.nan, index=df.index)
    for asset in df["asset"].unique():
        mask = df["asset"] == asset
        raw = df.loc[mask, "stable_buy_usd"]
        result.loc[mask] = compute_z_score(raw, z_window)
    return result


def compute_fwd_return(df: pd.DataFrame, horizon: int) -> pd.Series:
    """Compute forward return at given horizon from 1h returns."""
    # We have fwd_ret_4h in the panel. For other horizons, compute from price.
    if horizon == 4 and "fwd_ret_4h" in df.columns:
        return df["fwd_ret_4h"]
    # Approximate from 1h returns by summing (log-approx is fine for small returns)
    # Actually we need to shift the close price. Use fwd_ret_1h accumulated.
    result = pd.Series(np.nan, index=df.index)
    for asset in df["asset"].unique():
        mask = df["asset"] == asset
        ret_1h = df.loc[mask, "fwd_ret_1h"]
        # fwd_ret over horizon h = product of (1+r_1h) for next h hours - 1
        cum = (1 + ret_1h).rolling(horizon).apply(lambda x: x.prod() - 1, raw=True)
        # Shift back: fwd_ret at t = cum at t+horizon-1 (but we want forward-looking)
        # Actually: fwd_ret_Xh at time t = cumulative return from t to t+X
        # = product of fwd_ret_1h[t], fwd_ret_1h[t+1], ..., fwd_ret_1h[t+X-1]
        fwd = pd.Series(np.nan, index=ret_1h.index)
        vals = ret_1h.values
        for i in range(len(vals) - horizon + 1):
            fwd.iloc[i] = np.prod(1 + vals[i:i + horizon]) - 1
        result.loc[mask] = fwd
    return result


def prepare_data(df: pd.DataFrame, z_window: int, lag: int, horizon: int) -> pd.DataFrame:
    """Prepare dataset with signal, lag, and forward returns at given horizon."""
    out = df[["hour", "asset", "stable_buy_usd", "fwd_ret_1h", "fwd_ret_4h"]].copy()

    # Compute signal
    out["signal"] = get_signal_col(df, z_window)

    # Compute forward return for this horizon
    if horizon == 4:
        out["fwd_ret"] = out["fwd_ret_4h"]
    else:
        out["fwd_ret"] = compute_fwd_return(df, horizon)

    # Apply feature lag (strict: signal at t-lag predicts return from t)
    out["signal"] = out.groupby("asset")["signal"].shift(lag)

    # Filter to non-overlapping decision timestamps
    out = out[out["hour"].dt.hour % horizon == 0].copy()

    # Keep only needed columns
    out = out[["hour", "asset", "signal", "fwd_ret"]].dropna().copy()
    return out


def learn_orientation(train: pd.DataFrame) -> dict[str, int]:
    """Learn per-asset sign from Spearman correlation on train set."""
    orientation = {}
    for asset, g in train.groupby("asset"):
        if len(g) < 5:
            orientation[asset] = 1
            continue
        rho, _ = spearmanr(g["signal"], g["fwd_ret"], nan_policy="omit")
        orientation[asset] = 1 if (pd.notna(rho) and rho >= 0) else -1
    return orientation


def backtest_block(data: pd.DataFrame, orientation: dict[str, int], fee_oneway: float) -> pd.DataFrame:
    """Run signal-based trading on a block. Returns per-timestamp portfolio returns."""
    d = data.sort_values(["asset", "hour"]).copy()
    d["sig"] = d.apply(lambda r: orientation.get(r["asset"], 1) * r["signal"], axis=1)
    d["pos"] = np.sign(d["sig"]).astype(int)
    d["chg"] = d.groupby("asset")["pos"].diff().abs().fillna(d["pos"].abs())
    # Fee on every position change (entry=1x, exit=1x, flip=2x)
    d["asset_ret"] = d["pos"] * d["fwd_ret"] - fee_oneway * d["chg"]

    hourly = (
        d.groupby("hour")
        .agg(ret=("asset_ret", "mean"), n_assets=("asset", "count"))
        .reset_index()
    )
    return hourly


def ann_sharpe(returns: pd.Series, periods_per_year: int) -> float:
    if len(returns) < 2 or returns.std() == 0:
        return np.nan
    return float((returns.mean() / returns.std()) * np.sqrt(periods_per_year))


def ic_series(data: pd.DataFrame) -> float:
    """Average cross-sectional IC across timestamps."""
    ics = []
    for _, g in data.groupby("hour"):
        if len(g) < 3:
            continue
        rho, _ = spearmanr(g["signal"], g["fwd_ret"], nan_policy="omit")
        if pd.notna(rho):
            ics.append(rho)
    return float(np.mean(ics)) if ics else np.nan


# ─── Task 2: Rolling Walk-Forward ─────────────────────────────────────────────

def run_walkforward(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rolling 30d train / 15d test walk-forward."""
    data = prepare_data(df_raw, z_window=WF_Z_WINDOW, lag=WF_LAG, horizon=WF_HORIZON)
    periods_per_year = (365 * 24) // WF_HORIZON  # 2190 for 4h

    all_hours = sorted(data["hour"].unique())
    start = all_hours[0]
    end = all_hours[-1]

    train_td = pd.Timedelta(days=WF_TRAIN_DAYS)
    test_td = pd.Timedelta(days=WF_TEST_DAYS)
    step_td = pd.Timedelta(days=WF_STEP_DAYS)

    fold_rows = []
    equity_parts = []
    fold_num = 0

    cursor = start
    while cursor + train_td + test_td <= end + pd.Timedelta(hours=1):
        train_start = cursor
        train_end = cursor + train_td
        test_start = train_end
        test_end = train_end + test_td

        train = data[(data["hour"] >= train_start) & (data["hour"] < train_end)]
        test = data[(data["hour"] >= test_start) & (data["hour"] < test_end)]

        if len(train) < 10 or len(test) < 5:
            cursor += step_td
            continue

        fold_num += 1
        orientation = learn_orientation(train)
        test_hourly = backtest_block(test, orientation, WF_FEE)

        fold_sharpe = ann_sharpe(test_hourly["ret"], periods_per_year)
        fold_ic = ic_series(test)
        total_ret = float((1 + test_hourly["ret"]).prod() - 1)
        hit_rate = float((test_hourly["ret"] > 0).mean())

        fold_rows.append({
            "fold": fold_num,
            "train_start": str(train_start),
            "train_end": str(train_end),
            "test_start": str(test_start),
            "test_end": str(test_end),
            "n_train": len(train),
            "n_test": len(test),
            "orientation": json.dumps(orientation),
            "test_sharpe": fold_sharpe,
            "test_ic": fold_ic,
            "test_total_return": total_ret,
            "test_hit_rate": hit_rate,
        })

        test_hourly["fold"] = fold_num
        equity_parts.append(test_hourly[["hour", "ret", "fold"]])

        cursor += step_td

    folds_df = pd.DataFrame(fold_rows)

    # Build cumulative equity curve
    if equity_parts:
        equity_df = pd.concat(equity_parts, ignore_index=True).sort_values("hour")
        equity_df["cumulative_return"] = (1 + equity_df["ret"]).cumprod() - 1
    else:
        equity_df = pd.DataFrame(columns=["hour", "ret", "fold", "cumulative_return"])

    return folds_df, equity_df


# ─── Task 3: Parameter Sensitivity Grid ──────────────────────────────────────

def run_param_grid(df_raw: pd.DataFrame) -> pd.DataFrame:
    """Test all parameter combinations with walk-forward."""
    results = []
    total = len(GRID_Z_WINDOWS) * len(GRID_LAGS) * len(GRID_HORIZONS) * len(GRID_FEES)
    done = 0

    for z_window, lag, horizon, fee in product(GRID_Z_WINDOWS, GRID_LAGS, GRID_HORIZONS, GRID_FEES):
        done += 1
        if done % 20 == 0:
            print(f"  Grid progress: {done}/{total}")

        periods_per_year = (365 * 24) // horizon
        data = prepare_data(df_raw, z_window=z_window, lag=lag, horizon=horizon)

        if len(data) < 50:
            results.append({
                "z_window": z_window, "lag": lag, "horizon": horizon,
                "fee_oneway": fee, "sharpe": np.nan, "total_return": np.nan,
                "n_folds": 0, "avg_fold_sharpe": np.nan,
            })
            continue

        # Walk-forward with same structure
        all_hours = sorted(data["hour"].unique())
        start = all_hours[0]
        end = all_hours[-1]
        train_td = pd.Timedelta(days=WF_TRAIN_DAYS)
        test_td = pd.Timedelta(days=WF_TEST_DAYS)
        step_td = pd.Timedelta(days=WF_STEP_DAYS)

        all_test_rets = []
        fold_sharpes = []
        cursor = start

        while cursor + train_td + test_td <= end + pd.Timedelta(hours=1):
            train = data[(data["hour"] >= cursor) & (data["hour"] < cursor + train_td)]
            test = data[(data["hour"] >= cursor + train_td) & (data["hour"] < cursor + train_td + test_td)]

            if len(train) < 10 or len(test) < 5:
                cursor += step_td
                continue

            orientation = learn_orientation(train)
            test_hourly = backtest_block(test, orientation, fee)
            all_test_rets.append(test_hourly["ret"])
            fold_sharpes.append(ann_sharpe(test_hourly["ret"], periods_per_year))

            cursor += step_td

        if all_test_rets:
            combined = pd.concat(all_test_rets, ignore_index=True)
            overall_sharpe = ann_sharpe(combined, periods_per_year)
            total_ret = float((1 + combined).prod() - 1)
            avg_fold_sharpe = float(np.nanmean(fold_sharpes))
        else:
            overall_sharpe = np.nan
            total_ret = np.nan
            avg_fold_sharpe = np.nan

        results.append({
            "z_window": z_window,
            "lag": lag,
            "horizon": horizon,
            "fee_oneway": fee,
            "sharpe": overall_sharpe,
            "total_return": total_ret,
            "n_folds": len(fold_sharpes),
            "avg_fold_sharpe": avg_fold_sharpe,
        })

    return pd.DataFrame(results)


def build_sensitivity_summary(grid_df: pd.DataFrame) -> dict:
    """Top 10 combos + boundary flags."""
    valid = grid_df.dropna(subset=["sharpe"]).sort_values("sharpe", ascending=False)
    top10 = valid.head(10).to_dict(orient="records")

    # Boundary detection: check if best params are at grid edges
    boundary_flags = []
    if len(valid) > 0:
        best = valid.iloc[0]
        if best["z_window"] == min(GRID_Z_WINDOWS):
            boundary_flags.append("z_window at lower bound")
        if best["z_window"] == max(GRID_Z_WINDOWS):
            boundary_flags.append("z_window at upper bound")
        if best["lag"] == min(GRID_LAGS):
            boundary_flags.append("lag at lower bound")
        if best["lag"] == max(GRID_LAGS):
            boundary_flags.append("lag at upper bound")
        if best["horizon"] == min(GRID_HORIZONS):
            boundary_flags.append("horizon at lower bound")
        if best["horizon"] == max(GRID_HORIZONS):
            boundary_flags.append("horizon at upper bound")

    return {
        "top_10": top10,
        "boundary_flags": boundary_flags,
        "grid_dimensions": {
            "z_window": GRID_Z_WINDOWS,
            "lag": GRID_LAGS,
            "horizon": GRID_HORIZONS,
            "fee_oneway": GRID_FEES,
        },
        "total_combinations": len(grid_df),
        "valid_combinations": int(grid_df["sharpe"].notna().sum()),
    }


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading panel...")
    df_raw = load_panel()
    print(f"  {len(df_raw)} rows, assets: {sorted(df_raw['asset'].unique())}")
    print(f"  Date range: {df_raw['hour'].min()} → {df_raw['hour'].max()}")

    # ── Task 2: Walk-Forward ──
    print("\n=== Walk-Forward Validation ===")
    print(f"  Train={WF_TRAIN_DAYS}d, Test={WF_TEST_DAYS}d, Step={WF_STEP_DAYS}d")
    print(f"  z_window={WF_Z_WINDOW}, lag={WF_LAG}h, horizon={WF_HORIZON}h, fee={WF_FEE}")

    folds_df, equity_df = run_walkforward(df_raw)

    folds_path = OUT_DIR / "walkforward_folds.csv"
    equity_path = OUT_DIR / "walkforward_equity.csv"
    folds_df.to_csv(folds_path, index=False)
    equity_df.to_csv(equity_path, index=False)

    print(f"\n  Folds: {len(folds_df)}")
    if len(folds_df) > 0:
        print(f"  Avg fold Sharpe: {folds_df['test_sharpe'].mean():.3f}")
        print(f"  Sharpe range: [{folds_df['test_sharpe'].min():.3f}, {folds_df['test_sharpe'].max():.3f}]")
        print(f"  Avg fold IC: {folds_df['test_ic'].mean():.4f}")
        print(f"  Cumulative return: {equity_df['cumulative_return'].iloc[-1]:.4f}" if len(equity_df) > 0 else "")
        print(f"\n  Per-fold results:")
        print(folds_df[["fold", "test_start", "test_end", "test_sharpe", "test_ic", "test_total_return"]].to_string(index=False))

    # ── Task 3: Parameter Sensitivity ──
    print("\n\n=== Parameter Sensitivity Grid ===")
    total_combos = len(GRID_Z_WINDOWS) * len(GRID_LAGS) * len(GRID_HORIZONS) * len(GRID_FEES)
    print(f"  Testing {total_combos} combinations...")

    grid_df = run_param_grid(df_raw)
    summary = build_sensitivity_summary(grid_df)

    grid_path = OUT_DIR / "param_sensitivity_grid.csv"
    summary_path = OUT_DIR / "param_sensitivity_summary.json"
    grid_df.to_csv(grid_path, index=False)
    summary_path.write_text(json.dumps(summary, indent=2))

    print(f"\n  Valid results: {summary['valid_combinations']}/{summary['total_combinations']}")
    if summary["boundary_flags"]:
        print(f"  BOUNDARY FLAGS: {summary['boundary_flags']}")
    print("\n  Top 5 parameter combinations:")
    for i, row in enumerate(summary["top_10"][:5], 1):
        print(f"    {i}. z={row['z_window']} lag={row['lag']} h={row['horizon']} "
              f"fee={row['fee_oneway']:.5f} → Sharpe={row['sharpe']:.3f} ret={row['total_return']:.4f}")

    print(f"\n\nArtifacts written to {OUT_DIR}/:")
    print(f"  - {folds_path}")
    print(f"  - {equity_path}")
    print(f"  - {grid_path}")
    print(f"  - {summary_path}")


if __name__ == "__main__":
    main()
