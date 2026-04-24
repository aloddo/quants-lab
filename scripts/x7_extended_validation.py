#!/usr/bin/env python3
"""
X7 Extended Validation — 365-day walk-forward with boundary exploration.

Uses extended Dune data (365d) to run proper 6+ fold walk-forward and
test z_window=96/120 to resolve boundary flag.

Dune query IDs:
  DEX stablecoin buy flow: 7345369
  CEX net flow: 7345371
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

# === CONFIG ===
OUTPUT_DIR = Path("app/data/cache/onchain_x7_extended")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CANDLE_DIR = Path("app/data/cache/candles")

DEX_QUERY_ID = 7345369
CEX_QUERY_ID = 7345371

# Focus assets (ETH/SOL confirmed, BTC/DOGE exploratory)
PRIMARY_ASSETS = ["ETH", "SOL"]
SECONDARY_ASSETS = ["BTC", "DOGE", "AVAX", "LINK", "ADA", "ARB", "OP", "SUI"]
ALL_ASSETS = PRIMARY_ASSETS + SECONDARY_ASSETS

# Extended z-score windows (resolve boundary at 72)
Z_WINDOWS = [24, 48, 72, 96, 120]
LAGS = [2, 3, 4]  # Based on deep EDA: lag 3 optimal
HORIZONS = {"4h": 4}
FEE_ONEWAY = 0.00055  # conservative default

# Walk-forward config
TRAIN_DAYS = 60  # longer train for extended data
TEST_DAYS = 30
STEP_DAYS = 30


def fetch_dune_results(query_id: int, cache_name: str) -> pd.DataFrame:
    """Fetch from Dune API with caching."""
    cache_path = OUTPUT_DIR / f"{cache_name}.csv"
    if cache_path.exists():
        print(f"  Using cached {cache_path}")
        df = pd.read_csv(cache_path)
        df["hour"] = pd.to_datetime(df["hour"], utc=True)
        return df

    from dune_client.client import DuneClient
    from dune_client.query import QueryBase

    api_key = os.environ.get("DUNE_API_KEY", "4lS0uYbOdht78ZzWK0R63NEccj9Z9JLD")
    client = DuneClient(api_key)
    query = QueryBase(query_id=query_id)

    print(f"  Fetching Dune query {query_id}...")
    results = client.get_latest_result(query)
    rows = results.get_rows()
    df = pd.DataFrame(rows)
    df["hour"] = pd.to_datetime(df["hour"], utc=True)
    df.to_csv(cache_path, index=False)
    print(f"  Fetched {len(df)} rows, saved to {cache_path}")
    return df


def load_candle_returns(assets: list[str]) -> pd.DataFrame:
    """Load hourly candle data and compute forward returns."""
    all_rets = []
    for asset in assets:
        patterns = [
            f"bybit_perpetual|{asset}-USDT|1h.parquet",
            f"binance_spot|{asset}-USDT|1h.parquet",
        ]
        found = None
        for p in patterns:
            path = CANDLE_DIR / p
            if path.exists():
                found = path
                break
        if found is None:
            matches = list(CANDLE_DIR.glob(f"*{asset}*1h*"))
            if matches:
                found = matches[0]
        if found is None:
            print(f"  WARNING: No candle file for {asset}")
            continue

        candles = pd.read_parquet(found)
        candles["hour"] = pd.to_datetime(candles["timestamp"], unit="s", utc=True).dt.floor("h")
        candles = candles.sort_values("hour").drop_duplicates("hour").reset_index(drop=True)

        for hz_name, hz_hours in HORIZONS.items():
            candles[f"fwd_ret_{hz_name}"] = candles["close"].pct_change(hz_hours).shift(-hz_hours)

        candles["asset"] = asset
        all_rets.append(candles[["hour", "asset"] + [f"fwd_ret_{h}" for h in HORIZONS.keys()]])

    return pd.concat(all_rets, ignore_index=True)


def compute_z_features(df: pd.DataFrame, raw_col: str, z_windows: list[int]) -> pd.DataFrame:
    """Compute rolling z-scores for a raw feature column."""
    out = df.copy()
    for w in z_windows:
        col_name = f"{raw_col}_z{w}"
        out[col_name] = out.groupby("asset")[raw_col].transform(
            lambda x: (x - x.rolling(w, min_periods=max(w // 2, 6)).mean()) /
                      x.rolling(w, min_periods=max(w // 2, 6)).std().clip(lower=1e-10)
        )
    return out


def walk_forward(panel: pd.DataFrame, signal_col: str, lag: int, horizon: int,
                 fee: float, assets: list[str]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rolling walk-forward with orientation learning."""
    panel = panel[panel["asset"].isin(assets)].copy()
    panel = panel.sort_values(["asset", "hour"]).reset_index(drop=True)

    # Apply lag
    panel["signal_lagged"] = panel.groupby("asset")[signal_col].shift(lag)

    # Filter to decision timestamps (every `horizon` hours)
    panel["hour_of_day"] = panel["hour"].dt.hour
    panel = panel[panel["hour"].dt.hour % horizon == 0].copy()

    ret_col = f"fwd_ret_4h"
    panel = panel.dropna(subset=["signal_lagged", ret_col])

    hours = sorted(panel["hour"].unique())
    train_hours = TRAIN_DAYS * 24
    test_hours = TEST_DAYS * 24
    step_hours = STEP_DAYS * 24

    folds = []
    equity_records = []
    cum_ret = 1.0

    fold_idx = 0
    start = 0
    while start + train_hours + test_hours <= len(hours):
        train_end_hour = hours[start + train_hours - 1]
        test_start_hour = hours[start + train_hours]
        test_end_idx = min(start + train_hours + test_hours - 1, len(hours) - 1)
        test_end_hour = hours[test_end_idx]

        train_mask = (panel["hour"] >= hours[start]) & (panel["hour"] <= train_end_hour)
        test_mask = (panel["hour"] >= test_start_hour) & (panel["hour"] <= test_end_hour)

        train = panel[train_mask]
        test = panel[test_mask]

        if len(train) < 20 or len(test) < 10:
            start += step_hours // (24 // horizon)
            continue

        # Learn orientation per asset on train
        orientation = {}
        for asset in assets:
            asset_train = train[train["asset"] == asset]
            if len(asset_train) < 5:
                orientation[asset] = 1
                continue
            corr = asset_train["signal_lagged"].corr(asset_train[ret_col])
            orientation[asset] = 1 if corr >= 0 else -1

        # Evaluate on test
        test_rets = []
        for _, row in test.iterrows():
            asset = row["asset"]
            sig = row["signal_lagged"]
            direction = np.sign(sig) * orientation.get(asset, 1)
            raw_ret = row[ret_col]
            # Apply fee on every trade (assume position change each period)
            net_ret = direction * raw_ret - 2 * fee
            test_rets.append(net_ret)
            cum_ret *= (1 + net_ret)
            equity_records.append({
                "fold": fold_idx,
                "hour": row["hour"],
                "asset": asset,
                "ret": net_ret,
                "cum_ret": cum_ret,
            })

        test_rets = np.array(test_rets)
        if len(test_rets) > 0 and test_rets.std() > 0:
            periods_per_year = (365 * 24) / horizon
            sharpe = (test_rets.mean() / test_rets.std()) * np.sqrt(periods_per_year)
        else:
            sharpe = 0

        folds.append({
            "fold": fold_idx,
            "train_start": str(hours[start]),
            "train_end": str(train_end_hour),
            "test_start": str(test_start_hour),
            "test_end": str(test_end_hour),
            "n_train": int(len(train)),
            "n_test": int(len(test)),
            "orientation": json.dumps(orientation),
            "test_sharpe": float(sharpe),
            "test_return": float((1 + test_rets).prod() - 1) if len(test_rets) > 0 else 0,
            "test_hit_rate": float((test_rets > 0).mean()) if len(test_rets) > 0 else 0,
        })

        fold_idx += 1
        start += step_hours // (24 // horizon)

    return pd.DataFrame(folds), pd.DataFrame(equity_records)


def main():
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    print(f"=== X7 Extended Validation — {ts} ===\n")

    # Step 1: Fetch extended data
    print("[1/5] Fetching extended Dune data...")
    dex_df = fetch_dune_results(DEX_QUERY_ID, "dex_365d_raw")
    cex_df = fetch_dune_results(CEX_QUERY_ID, "cex_365d_raw")

    print(f"\nDEX data: {len(dex_df)} rows, {dex_df['asset'].nunique()} assets")
    print(f"  Range: {dex_df['hour'].min()} to {dex_df['hour'].max()}")
    print(f"CEX data: {len(cex_df)} rows, {cex_df['asset'].nunique()} assets")
    print(f"  Range: {cex_df['hour'].min()} to {cex_df['hour'].max()}")

    # Step 2: Compute features
    print("\n[2/5] Computing z-score features...")
    # Ensure numeric
    for col in ["stable_buy_usd", "buy_usd", "sell_usd", "dex_flow_imbalance"]:
        if col in dex_df.columns:
            dex_df[col] = pd.to_numeric(dex_df[col], errors="coerce").fillna(0)

    dex_features = compute_z_features(dex_df, "stable_buy_usd", Z_WINDOWS)
    dex_features = compute_z_features(dex_features, "buy_usd", Z_WINDOWS)
    dex_features = compute_z_features(dex_features, "dex_flow_imbalance", Z_WINDOWS)

    # CEX features
    for col in ["inflow_usd", "outflow_usd", "net_flow_usd"]:
        if col in cex_df.columns:
            cex_df[col] = pd.to_numeric(cex_df[col], errors="coerce").fillna(0)
    cex_features = compute_z_features(cex_df, "inflow_usd", Z_WINDOWS)
    cex_features = compute_z_features(cex_features, "net_flow_usd", Z_WINDOWS)

    # Step 3: Load candles and merge
    print("\n[3/5] Loading candle returns...")
    returns_df = load_candle_returns(ALL_ASSETS)
    print(f"  {returns_df.shape[0]} return rows for {returns_df['asset'].nunique()} assets")

    # Merge DEX features with returns
    panel = dex_features.merge(returns_df, on=["hour", "asset"], how="inner")
    print(f"  DEX panel: {panel.shape[0]} rows, {panel['asset'].nunique()} assets")
    print(f"  Range: {panel['hour'].min()} to {panel['hour'].max()}")

    # Also merge CEX (keep separate for now)
    cex_panel = cex_features.merge(returns_df, on=["hour", "asset"], how="inner")
    print(f"  CEX panel: {cex_panel.shape[0]} rows")

    if len(panel) < 100:
        print("\nERROR: Not enough merged data. Dune queries may still be running.")
        print("Re-run this script after queries complete.")
        return

    # Step 4: Walk-forward with extended z_windows
    print("\n[4/5] Running walk-forward across parameter grid...")
    results = []

    for z_win in Z_WINDOWS:
        signal_col = f"stable_buy_usd_z{z_win}"
        if signal_col not in panel.columns:
            continue
        for lag in LAGS:
            folds_df, _ = walk_forward(
                panel, signal_col, lag=lag, horizon=4,
                fee=FEE_ONEWAY, assets=PRIMARY_ASSETS
            )
            if folds_df.empty:
                continue
            avg_sharpe = folds_df["test_sharpe"].mean()
            total_ret = (1 + folds_df["test_return"]).prod() - 1
            pct_positive = (folds_df["test_sharpe"] > 0).mean()

            results.append({
                "signal": signal_col,
                "z_window": z_win,
                "lag": lag,
                "n_folds": len(folds_df),
                "avg_sharpe": float(avg_sharpe),
                "std_sharpe": float(folds_df["test_sharpe"].std()),
                "total_return": float(total_ret),
                "pct_folds_positive": float(pct_positive),
                "min_fold_sharpe": float(folds_df["test_sharpe"].min()),
                "max_fold_sharpe": float(folds_df["test_sharpe"].max()),
            })
            print(f"  z{z_win} lag{lag}: {len(folds_df)} folds, "
                  f"avg_sharpe={avg_sharpe:.2f}, "
                  f"pct_pos={pct_positive:.0%}, "
                  f"total_ret={total_ret:.1%}")

    grid_df = pd.DataFrame(results).sort_values("avg_sharpe", ascending=False)

    # Step 5: Best config detailed walk-forward
    if not grid_df.empty:
        best = grid_df.iloc[0]
        print(f"\n[5/5] Best config: {best['signal']}, lag={best['lag']}")
        print(f"  Avg Sharpe: {best['avg_sharpe']:.2f}")
        print(f"  Total Return: {best['total_return']:.1%}")
        print(f"  Folds positive: {best['pct_folds_positive']:.0%}")
        print(f"  Sharpe range: [{best['min_fold_sharpe']:.2f}, {best['max_fold_sharpe']:.2f}]")

        # Run detailed walk-forward for best config on all assets
        best_signal = best["signal"]
        best_lag = int(best["lag"])

        print(f"\n  Running on ALL assets ({len(ALL_ASSETS)})...")
        all_folds, all_equity = walk_forward(
            panel, best_signal, lag=best_lag, horizon=4,
            fee=FEE_ONEWAY, assets=ALL_ASSETS
        )
        if not all_folds.empty:
            print(f"  All-asset: {len(all_folds)} folds, "
                  f"avg_sharpe={all_folds['test_sharpe'].mean():.2f}, "
                  f"total_ret={(1 + all_folds['test_return']).prod() - 1:.1%}")
            all_folds.to_csv(OUTPUT_DIR / f"{ts}_walkforward_all_assets.csv", index=False)
            all_equity.to_csv(OUTPUT_DIR / f"{ts}_equity_all_assets.csv", index=False)

    # Save grid results
    grid_df.to_csv(OUTPUT_DIR / f"{ts}_param_grid.csv", index=False)

    # Summary
    summary = {
        "timestamp": ts,
        "dex_panel_shape": list(panel.shape),
        "cex_panel_shape": list(cex_panel.shape),
        "date_range": [str(panel["hour"].min()), str(panel["hour"].max())],
        "primary_assets": PRIMARY_ASSETS,
        "z_windows_tested": Z_WINDOWS,
        "lags_tested": LAGS,
        "top_5_configs": grid_df.head(5).to_dict(orient="records") if not grid_df.empty else [],
        "boundary_resolved": not grid_df.empty and grid_df.iloc[0]["z_window"] < max(Z_WINDOWS),
    }
    with open(OUTPUT_DIR / f"{ts}_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n=== Done. Outputs in {OUTPUT_DIR}/{ts}_* ===")


if __name__ == "__main__":
    main()
