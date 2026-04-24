#!/usr/bin/env python3
"""
X7 CEX Flow Signal EDA — Pull data from Dune and correlate with forward returns.

Dune query ID: 7345314 (X7 CEX Net Flow v2)
Uses dune-client to fetch full results, then aligns with local candle data.
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
DUNE_QUERY_ID = 7345314
OUTPUT_DIR = Path("app/data/cache/onchain_x7_cex_flow")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CANDLE_DIR = Path("app/data/cache/candles")

ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK",
          "DOT", "UNI", "ARB", "OP", "BNB", "MATIC", "LTC", "BCH"]

Z_WINDOWS = [12, 24, 48, 168]  # hours
LAGS = [1, 2, 4]
HORIZONS = {"4h": 4, "8h": 8, "24h": 24}


def fetch_dune_data() -> pd.DataFrame:
    """Fetch full CEX flow data from Dune API."""
    try:
        from dune_client.client import DuneClient
        from dune_client.query import QueryBase
    except ImportError:
        print("Installing dune-client...")
        import subprocess
        subprocess.run(["/Users/hermes/miniforge3/envs/quants-lab/bin/pip",
                       "install", "dune-client"], check=True, capture_output=True)
        from dune_client.client import DuneClient
        from dune_client.query import QueryBase

    api_key = os.environ.get("DUNE_API_KEY")
    if not api_key:
        # Try loading from .env
        env_path = Path("/Users/hermes/quants-lab/.env")
        if env_path.exists():
            for line in env_path.read_text().splitlines():
                if line.startswith("DUNE_API_KEY="):
                    api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break

    if not api_key:
        raise ValueError("DUNE_API_KEY not found in env or .env file")

    print(f"Fetching Dune query {DUNE_QUERY_ID}...")
    client = DuneClient(api_key)
    query = QueryBase(query_id=DUNE_QUERY_ID)
    results = client.get_latest_result(query)

    rows = results.get_rows()
    df = pd.DataFrame(rows)
    print(f"  Fetched {len(df)} rows")
    return df


def load_candle_returns() -> pd.DataFrame:
    """Load hourly candle data and compute forward returns."""
    all_rets = []
    for asset in ASSETS:
        # Try common patterns (pipe-separated filenames)
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
            # Search for any matching file
            matches = list(CANDLE_DIR.glob(f"*{asset}*1h*"))
            if matches:
                found = matches[0]

        if found is None:
            print(f"  WARNING: No candle file for {asset}")
            continue

        candles = pd.read_parquet(found)
        # Timestamp is Unix epoch in seconds
        if "timestamp" in candles.columns:
            candles["hour"] = pd.to_datetime(candles["timestamp"], unit="s", utc=True)
        elif "open_time" in candles.columns:
            candles["hour"] = pd.to_datetime(candles["open_time"], unit="s", utc=True)
        else:
            candles["hour"] = pd.to_datetime(candles.index, utc=True)

        # Floor to hour to ensure clean merge
        candles["hour"] = candles["hour"].dt.floor("h")
        candles = candles.sort_values("hour").drop_duplicates("hour").reset_index(drop=True)

        # Forward returns
        close_col = "close" if "close" in candles.columns else candles.columns[candles.columns.str.contains("close", case=False)][0]
        for hz_name, hz_hours in HORIZONS.items():
            candles[f"fwd_ret_{hz_name}"] = candles[close_col].pct_change(hz_hours).shift(-hz_hours)

        candles["asset"] = asset
        all_rets.append(candles[["hour", "asset"] + [f"fwd_ret_{h}" for h in HORIZONS.keys()]])

    return pd.concat(all_rets, ignore_index=True)


def compute_features(flow_df: pd.DataFrame) -> pd.DataFrame:
    """Compute z-score and derived features from CEX flow data."""
    df = flow_df.copy()

    # Base features
    df["net_flow_ratio"] = df["net_flow_usd"] / (df["inflow_usd"] + df["outflow_usd"] + 1)
    df["large_net_flow"] = df["large_inflow_usd"] - df["large_outflow_usd"]
    df["flow_count_ratio"] = df["inflow_count"] / (df["inflow_count"] + df["outflow_count"] + 1)
    df["binance_net_flow"] = df["binance_inflow_usd"] - df["binance_outflow_usd"]

    base_features = ["net_flow_usd", "net_flow_ratio", "large_net_flow",
                     "inflow_usd", "outflow_usd", "flow_count_ratio", "binance_net_flow"]

    # Z-scores per asset
    for feat in base_features:
        for window in Z_WINDOWS:
            col_name = f"{feat}_z{window}"
            df[col_name] = df.groupby("asset")[feat].transform(
                lambda x: (x - x.rolling(window, min_periods=max(window // 2, 6)).mean()) /
                          x.rolling(window, min_periods=max(window // 2, 6)).std().clip(lower=1e-10)
            )

    return df


def run_ic_analysis(panel: pd.DataFrame) -> pd.DataFrame:
    """Compute IC for all CEX flow features vs forward returns."""
    feature_cols = [c for c in panel.columns if c not in
                    {"hour", "asset"} and not c.startswith("fwd_ret_")]

    results = []
    for feat in feature_cols:
        for lag in LAGS:
            lagged = panel.groupby("asset")[feat].shift(lag)
            for hz_name in HORIZONS.keys():
                ret_col = f"fwd_ret_{hz_name}"
                mask = lagged.notna() & panel[ret_col].notna()
                n = mask.sum()
                if n < 100:
                    continue
                rho, p = spearmanr(lagged[mask], panel[ret_col][mask])
                results.append({
                    "feature": feat,
                    "lag_hours": lag,
                    "horizon": hz_name,
                    "n": int(n),
                    "rho": float(rho),
                    "p_value": float(p),
                    "significant": p < 0.01,
                })

    return pd.DataFrame(results).sort_values(["horizon", "p_value"])


def run_per_asset_ic(panel: pd.DataFrame, top_features: list[str]) -> pd.DataFrame:
    """Per-asset IC for top features."""
    results = []
    for feat in top_features:
        if feat not in panel.columns:
            continue
        for asset in panel["asset"].unique():
            asset_df = panel[panel["asset"] == asset].copy()
            lagged = asset_df[feat].shift(1)
            for hz_name in HORIZONS.keys():
                ret_col = f"fwd_ret_{hz_name}"
                mask = lagged.notna() & asset_df[ret_col].notna()
                n = mask.sum()
                if n < 30:
                    continue
                rho, p = spearmanr(lagged[mask], asset_df[ret_col][mask])
                results.append({
                    "feature": feat,
                    "asset": asset,
                    "horizon": hz_name,
                    "n": int(n),
                    "rho": float(rho),
                    "p_value": float(p),
                })
    return pd.DataFrame(results)


def main():
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    print(f"=== X7 CEX Flow EDA — {ts} ===\n")

    # Step 1: Fetch Dune data
    cache_path = OUTPUT_DIR / "cex_flow_raw.csv"
    if cache_path.exists():
        print(f"Using cached CEX flow data from {cache_path}")
        flow_df = pd.read_csv(cache_path)
        flow_df["hour"] = pd.to_datetime(flow_df["hour"], utc=True)
    else:
        flow_df = fetch_dune_data()
        flow_df["hour"] = pd.to_datetime(flow_df["hour"], utc=True)
        flow_df.to_csv(cache_path, index=False)
        print(f"  Saved to {cache_path}")

    print(f"\nCEX flow data: {flow_df.shape[0]} rows, {flow_df['asset'].nunique()} assets")
    print(f"Date range: {flow_df['hour'].min()} to {flow_df['hour'].max()}")

    # Step 2: Compute features
    print("\nComputing z-score features...")
    features_df = compute_features(flow_df)
    feat_cols = [c for c in features_df.columns if "_z" in c or c in
                 ["net_flow_ratio", "large_net_flow", "flow_count_ratio", "binance_net_flow"]]
    print(f"  {len(feat_cols)} feature columns generated")

    # Step 3: Load candle returns
    print("\nLoading candle returns...")
    returns_df = load_candle_returns()
    print(f"  {returns_df.shape[0]} return rows for {returns_df['asset'].nunique()} assets")

    # Step 4: Merge
    panel = features_df.merge(returns_df, on=["hour", "asset"], how="inner")
    print(f"\nMerged panel: {panel.shape[0]} rows, {panel['asset'].nunique()} assets")
    print(f"Date range: {panel['hour'].min()} to {panel['hour'].max()}")

    # Step 5: IC analysis
    print("\n[1/2] Running IC analysis...")
    ic_results = run_ic_analysis(panel)

    # Show top signals
    print("\n=== TOP CEX FLOW SIGNALS (4h horizon, p < 0.01) ===")
    top_4h = ic_results[(ic_results["horizon"] == "4h") & (ic_results["significant"])].head(20)
    for _, r in top_4h.iterrows():
        print(f"  {r['feature']:<35} lag={r['lag_hours']}h  rho={r['rho']:+.4f}  p={r['p_value']:.2e}  N={r['n']}")

    print("\n=== TOP CEX FLOW SIGNALS (24h horizon, p < 0.01) ===")
    top_24h = ic_results[(ic_results["horizon"] == "24h") & (ic_results["significant"])].head(20)
    for _, r in top_24h.iterrows():
        print(f"  {r['feature']:<35} lag={r['lag_hours']}h  rho={r['rho']:+.4f}  p={r['p_value']:.2e}  N={r['n']}")

    # Step 6: Per-asset breakdown for top features
    top_feats = ic_results[ic_results["significant"]].groupby("feature")["rho"].apply(
        lambda x: abs(x).max()).nlargest(10).index.tolist()

    if top_feats:
        print(f"\n[2/2] Per-asset IC for top {len(top_feats)} features...")
        per_asset = run_per_asset_ic(panel, top_feats)

        # Show universality
        sig_4h = per_asset[(per_asset["horizon"] == "4h") & (per_asset["p_value"] < 0.05)]
        for feat in top_feats[:5]:
            sub = sig_4h[sig_4h["feature"] == feat]
            if not sub.empty:
                assets_str = ", ".join(f"{r['asset']}:{r['rho']:+.3f}" for _, r in sub.iterrows())
                print(f"  {feat}: {assets_str}")
    else:
        per_asset = pd.DataFrame()

    # Save results
    ic_results.to_csv(OUTPUT_DIR / f"{ts}_cex_flow_ic.csv", index=False)
    panel.to_csv(OUTPUT_DIR / f"{ts}_cex_flow_panel.csv", index=False)
    if not per_asset.empty:
        per_asset.to_csv(OUTPUT_DIR / f"{ts}_cex_flow_per_asset.csv", index=False)

    summary = {
        "timestamp": ts,
        "dune_query_id": DUNE_QUERY_ID,
        "panel_shape": list(panel.shape),
        "assets": sorted(panel["asset"].unique().tolist()),
        "date_range": [str(panel["hour"].min()), str(panel["hour"].max())],
        "top_signals_4h": top_4h.head(10).to_dict(orient="records") if len(top_4h) > 0 else [],
        "top_signals_24h": top_24h.head(10).to_dict(orient="records") if len(top_24h) > 0 else [],
    }
    with open(OUTPUT_DIR / f"{ts}_cex_flow_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n=== Done. Outputs in {OUTPUT_DIR}/{ts}_* ===")


if __name__ == "__main__":
    main()
