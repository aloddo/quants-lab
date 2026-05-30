#!/usr/bin/env python3
"""
X7 Deep EDA — Comprehensive signal candidate analysis.

Goes beyond basic IC screening:
1. Leak-free lagged IC with multiple lag values (1h, 2h, 4h)
2. Rolling IC stability (is the signal persistent or episodic?)
3. IC decay curve (how fast does predictive power fade?)
4. Regime conditioning (high-vol vs low-vol, trend vs chop)
5. Per-asset breakdown with cross-sectional vs time-series decomposition
6. Monotonic quintile returns (does signal strength map linearly to returns?)
7. Signal autocorrelation (is it too persistent = stale, or too noisy?)
8. Turnover-adjusted IC (penalize signals that flip too fast to trade)
9. Top candidate shortlist with composite scoring

Output: structured JSON + CSV artifacts for each analysis.
"""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd
from scipy.stats import spearmanr, pearsonr

# === CONFIG ===
PANEL_PATH = Path("app/data/cache/onchain_x7_advanced/20260420_154932_feature_panel.csv")
OUTPUT_DIR = Path("app/data/cache/onchain_x7_deep_eda")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE"]
HORIZONS = {"1h": "fwd_ret_1h", "4h": "fwd_ret_4h", "24h": "fwd_ret_24h"}
LAGS = [1, 2, 4]  # hours of feature lag

# Top candidates from univariate IC screen (p < 0.001, rho > 0.03)
TOP_FEATURES = [
    "stablecoin_conversion_imbalance_z24",
    "stablecoin_conversion_imbalance_z168",
    "stablecoin_conversion_imbalance",
    "stablecoin_conversion_imbalance_d1",
    "dex_flow_imbalance_z24",
    "stable_buy_usd_z168",
    "dex_flow_imbalance",
    "dex_flow_imbalance_z168",
    "dex_flow_x_vol",
    "stable_buy_usd_z24",
    "realized_vol_24h_z24",
    "dex_flow_imbalance_d1",
    "buy_usd_z168",
    "buy_usd_z24",
    "stable_buy_usd_d1",
]

# Additional interesting features for deeper look
SECONDARY_FEATURES = [
    "whale_100k_count_z24",
    "whale_100k_usd_z24",
    "transfer_count_z168",
    "dex_breadth_imbalance_z24",
    "sell_usd_d1",
    "transfer_p90_usd_z24",
]

ALL_CANDIDATES = TOP_FEATURES + SECONDARY_FEATURES


def load_panel() -> pd.DataFrame:
    df = pd.read_csv(PANEL_PATH)
    df["hour"] = pd.to_datetime(df["hour"], utc=True)
    df = df.sort_values(["asset", "hour"]).reset_index(drop=True)
    return df


def apply_lag(df: pd.DataFrame, feature: str, lag: int) -> pd.Series:
    """Lag feature by N hours per asset (strict causal)."""
    return df.groupby("asset")[feature].shift(lag)


# === ANALYSIS 1: Lagged IC at multiple lags ===
def analysis_lagged_ic(df: pd.DataFrame) -> pd.DataFrame:
    """Compute IC (Spearman rho) for each feature × horizon × lag combination."""
    print("\n[1/9] Computing lagged IC matrix...")
    results = []
    for feat in ALL_CANDIDATES:
        if feat not in df.columns:
            continue
        for lag in LAGS:
            lagged = apply_lag(df, feat, lag)
            for hz_name, hz_col in HORIZONS.items():
                mask = lagged.notna() & df[hz_col].notna()
                n = mask.sum()
                if n < 100:
                    continue
                rho, p = spearmanr(lagged[mask], df[hz_col][mask])
                results.append({
                    "feature": feat,
                    "lag_hours": lag,
                    "horizon": hz_name,
                    "n": int(n),
                    "rho": float(rho),
                    "p_value": float(p),
                    "significant": p < 0.01,
                })
    out = pd.DataFrame(results)
    out = out.sort_values(["horizon", "rho"], ascending=[True, False])
    print(f"   {len(out)} IC measurements computed")
    return out


# === ANALYSIS 2: Rolling IC stability ===
def analysis_rolling_ic(df: pd.DataFrame, window_days: int = 14) -> pd.DataFrame:
    """Compute rolling IC in windows to detect episodic vs persistent signals."""
    print("\n[2/9] Computing rolling IC stability (14-day windows)...")
    window_hours = window_days * 24
    results = []

    # Focus on top 5 features at 4h horizon with lag=1
    top5 = ["stable_buy_usd_z24", "stablecoin_conversion_imbalance_z24",
             "dex_flow_imbalance_z24", "dex_flow_imbalance", "stable_buy_usd_z168"]

    hours_sorted = sorted(df["hour"].unique())

    for feat in top5:
        if feat not in df.columns:
            continue
        lagged = apply_lag(df, feat, lag=1)
        df_tmp = df.copy()
        df_tmp["_lagged_feat"] = lagged

        # Slide window
        for i in range(0, len(hours_sorted) - window_hours, window_hours // 2):
            w_start = hours_sorted[i]
            w_end = hours_sorted[min(i + window_hours, len(hours_sorted) - 1)]
            mask = (df_tmp["hour"] >= w_start) & (df_tmp["hour"] < w_end)
            chunk = df_tmp[mask]
            valid = chunk["_lagged_feat"].notna() & chunk["fwd_ret_4h"].notna()
            n = valid.sum()
            if n < 30:
                continue
            rho, p = spearmanr(chunk.loc[valid, "_lagged_feat"], chunk.loc[valid, "fwd_ret_4h"])
            results.append({
                "feature": feat,
                "window_start": str(w_start),
                "window_end": str(w_end),
                "n": int(n),
                "rho": float(rho),
                "p_value": float(p),
            })

    out = pd.DataFrame(results)
    if not out.empty:
        # Compute stability metrics per feature
        stability = out.groupby("feature").agg(
            mean_rho=("rho", "mean"),
            std_rho=("rho", "std"),
            pct_positive=("rho", lambda x: (x > 0).mean()),
            pct_significant=("p_value", lambda x: (x < 0.05).mean()),
            n_windows=("rho", "count"),
        ).reset_index()
        stability["ic_ir"] = stability["mean_rho"] / stability["std_rho"]  # IC information ratio
        print(f"   Stability summary:")
        for _, row in stability.iterrows():
            print(f"   {row['feature']}: mean_IC={row['mean_rho']:.4f}, "
                  f"IC_IR={row['ic_ir']:.2f}, "
                  f"pct_pos={row['pct_positive']:.0%}, "
                  f"pct_sig={row['pct_significant']:.0%}")
    return out


# === ANALYSIS 3: IC decay curve ===
def analysis_ic_decay(df: pd.DataFrame) -> pd.DataFrame:
    """How does IC decay as we increase the lag? Tells us signal half-life."""
    print("\n[3/9] Computing IC decay curves...")
    decay_lags = [1, 2, 3, 4, 6, 8, 12, 24]
    top5 = ["stable_buy_usd_z24", "stablecoin_conversion_imbalance_z24",
             "dex_flow_imbalance_z24", "buy_usd_z24", "stable_buy_usd_z168"]
    results = []
    for feat in top5:
        if feat not in df.columns:
            continue
        for lag in decay_lags:
            lagged = apply_lag(df, feat, lag)
            # Use 4h return for consistency
            mask = lagged.notna() & df["fwd_ret_4h"].notna()
            n = mask.sum()
            if n < 100:
                continue
            rho, p = spearmanr(lagged[mask], df["fwd_ret_4h"][mask])
            results.append({
                "feature": feat,
                "lag_hours": lag,
                "rho": float(rho),
                "p_value": float(p),
                "n": int(n),
            })
    out = pd.DataFrame(results)
    if not out.empty:
        print("   IC decay (4h fwd return):")
        for feat in top5:
            sub = out[out["feature"] == feat].sort_values("lag_hours")
            if sub.empty:
                continue
            decay_str = " → ".join(f"L{r['lag_hours']}:{r['rho']:.4f}" for _, r in sub.iterrows())
            print(f"   {feat}: {decay_str}")
    return out


# === ANALYSIS 4: Regime conditioning ===
def analysis_regime(df: pd.DataFrame) -> pd.DataFrame:
    """Split IC by vol regime and trend regime."""
    print("\n[4/9] Computing regime-conditioned IC...")
    results = []
    top5 = ["stable_buy_usd_z24", "stablecoin_conversion_imbalance_z24",
             "dex_flow_imbalance_z24", "dex_flow_imbalance", "buy_usd_z24"]

    for asset in ASSETS:
        asset_df = df[df["asset"] == asset].copy()
        if len(asset_df) < 200:
            continue

        # Vol regime: realized_vol_24h above/below median
        vol_med = asset_df["realized_vol_24h"].median()
        asset_df["vol_regime"] = np.where(asset_df["realized_vol_24h"] > vol_med, "high_vol", "low_vol")

        # Trend regime: sign of 24h trailing return
        asset_df["trailing_24h"] = asset_df["fwd_ret_24h"].shift(24)  # past 24h performance
        # Use rolling 24h return as trend proxy
        asset_df["trend_regime"] = np.where(
            asset_df["fwd_ret_1h"].rolling(24).sum().shift(1) > 0, "uptrend", "downtrend"
        )

        for feat in top5:
            if feat not in asset_df.columns:
                continue
            lagged = asset_df[feat].shift(1)

            for regime_col in ["vol_regime", "trend_regime"]:
                for regime_val in asset_df[regime_col].dropna().unique():
                    mask = (asset_df[regime_col] == regime_val) & lagged.notna() & asset_df["fwd_ret_4h"].notna()
                    n = mask.sum()
                    if n < 50:
                        continue
                    rho, p = spearmanr(lagged[mask], asset_df["fwd_ret_4h"][mask])
                    results.append({
                        "feature": feat,
                        "asset": asset,
                        "regime_type": regime_col,
                        "regime_value": regime_val,
                        "n": int(n),
                        "rho": float(rho),
                        "p_value": float(p),
                    })

    out = pd.DataFrame(results)
    if not out.empty:
        print("   Key regime findings (4h, lag=1):")
        # Show biggest regime divergences
        pivot = out.groupby(["feature", "regime_type", "regime_value"])["rho"].mean()
        for feat in top5:
            sub = pivot.get(feat, pd.Series())
            if not sub.empty:
                print(f"   {feat}:")
                for idx, val in sub.items():
                    print(f"     {idx[0]}={idx[1]}: rho={val:.4f}")
    return out


# === ANALYSIS 5: Per-asset IC decomposition ===
def analysis_per_asset(df: pd.DataFrame) -> pd.DataFrame:
    """IC per asset — is signal universal or asset-specific?"""
    print("\n[5/9] Computing per-asset IC breakdown...")
    results = []
    for feat in ALL_CANDIDATES:
        if feat not in df.columns:
            continue
        for asset in ASSETS:
            asset_df = df[df["asset"] == asset]
            lagged = asset_df[feat].shift(1)  # lag 1h within asset (already sorted)
            for hz_name, hz_col in HORIZONS.items():
                mask = lagged.notna() & asset_df[hz_col].notna()
                n = mask.sum()
                if n < 50:
                    continue
                rho, p = spearmanr(lagged[mask], asset_df[hz_col][mask])
                results.append({
                    "feature": feat,
                    "asset": asset,
                    "horizon": hz_name,
                    "n": int(n),
                    "rho": float(rho),
                    "p_value": float(p),
                })

    out = pd.DataFrame(results)
    if not out.empty:
        # Show features that work across >= 3 assets at 4h
        hz4 = out[(out["horizon"] == "4h") & (out["p_value"] < 0.05)]
        feat_counts = hz4.groupby("feature")["asset"].nunique()
        universal = feat_counts[feat_counts >= 3].index.tolist()
        print(f"   Features significant (p<0.05) on >= 3 assets at 4h: {len(universal)}")
        for f in universal[:8]:
            sub = hz4[hz4["feature"] == f][["asset", "rho", "p_value"]]
            assets_str = ", ".join(f"{r['asset']}:{r['rho']:.3f}" for _, r in sub.iterrows())
            print(f"   {f}: {assets_str}")
    return out


# === ANALYSIS 6: Quintile monotonicity ===
def analysis_quintiles(df: pd.DataFrame) -> pd.DataFrame:
    """Do quintile portfolios show monotonic returns?"""
    print("\n[6/9] Computing quintile return monotonicity...")
    results = []
    top5 = ["stable_buy_usd_z24", "stablecoin_conversion_imbalance_z24",
             "dex_flow_imbalance_z24", "dex_flow_imbalance", "buy_usd_z24"]

    for feat in top5:
        if feat not in df.columns:
            continue
        lagged = apply_lag(df, feat, 1)
        tmp = df[["hour", "asset", "fwd_ret_4h"]].copy()
        tmp["signal"] = lagged
        tmp = tmp.dropna(subset=["signal", "fwd_ret_4h"])

        if len(tmp) < 500:
            continue

        # Cross-sectional quintiles per hour
        tmp["quintile"] = tmp.groupby("hour")["signal"].transform(
            lambda x: pd.qcut(x, 5, labels=[1, 2, 3, 4, 5], duplicates="drop") if len(x) >= 5 else np.nan
        )
        tmp = tmp.dropna(subset=["quintile"])
        tmp["quintile"] = tmp["quintile"].astype(int)

        q_rets = tmp.groupby("quintile")["fwd_ret_4h"].mean()
        spread = q_rets.get(5, 0) - q_rets.get(1, 0)
        # Monotonicity: Spearman correlation of quintile rank vs mean return
        if len(q_rets) == 5:
            mono_rho, _ = spearmanr([1, 2, 3, 4, 5], q_rets.values)
        else:
            mono_rho = np.nan

        for q, ret in q_rets.items():
            results.append({
                "feature": feat,
                "quintile": int(q),
                "mean_4h_ret": float(ret),
                "n": int((tmp["quintile"] == q).sum()),
            })

        print(f"   {feat}: Q1={q_rets.get(1, 0)*100:.3f}% → Q5={q_rets.get(5, 0)*100:.3f}% "
              f"| spread={spread*100:.3f}% | mono={mono_rho:.2f}")

    return pd.DataFrame(results)


# === ANALYSIS 7: Signal autocorrelation ===
def analysis_autocorrelation(df: pd.DataFrame) -> pd.DataFrame:
    """Is the signal too persistent (stale) or too noisy (untradeable)?"""
    print("\n[7/9] Computing signal autocorrelation...")
    results = []
    for feat in ALL_CANDIDATES[:10]:
        if feat not in df.columns:
            continue
        for asset in ASSETS:
            series = df[df["asset"] == asset][feat].dropna()
            if len(series) < 100:
                continue
            for ac_lag in [1, 4, 12, 24]:
                if len(series) <= ac_lag:
                    continue
                ac = float(series.autocorr(lag=ac_lag))
                results.append({
                    "feature": feat,
                    "asset": asset,
                    "ac_lag": ac_lag,
                    "autocorrelation": ac,
                })

    out = pd.DataFrame(results)
    if not out.empty:
        # Summarize: avg autocorrelation across assets
        summary = out.groupby(["feature", "ac_lag"])["autocorrelation"].mean().unstack("ac_lag")
        print("   Avg autocorrelation by feature (AC1, AC4, AC12, AC24):")
        for feat in summary.index[:8]:
            row = summary.loc[feat]
            print(f"   {feat}: " + " | ".join(f"AC{int(c)}={v:.3f}" for c, v in row.items() if pd.notna(v)))
    return out


# === ANALYSIS 8: Turnover-adjusted IC ===
def analysis_turnover_adjusted(df: pd.DataFrame) -> pd.DataFrame:
    """Penalize signals that flip direction too frequently."""
    print("\n[8/9] Computing turnover-adjusted signal quality...")
    results = []
    top5 = ["stable_buy_usd_z24", "stablecoin_conversion_imbalance_z24",
             "dex_flow_imbalance_z24", "dex_flow_imbalance", "buy_usd_z24"]

    for feat in top5:
        if feat not in df.columns:
            continue
        for asset in ASSETS:
            asset_df = df[df["asset"] == asset].copy()
            lagged = asset_df[feat].shift(1)
            signal_sign = np.sign(lagged)
            # Turnover = fraction of periods where signal flips sign
            flips = (signal_sign.diff().abs() > 0).sum()
            total = signal_sign.notna().sum()
            turnover = flips / max(total, 1)

            # IC
            mask = lagged.notna() & asset_df["fwd_ret_4h"].notna()
            n = mask.sum()
            if n < 50:
                continue
            rho, p = spearmanr(lagged[mask], asset_df["fwd_ret_4h"][mask])

            # Turnover-adjusted IC: penalize high turnover
            # Assume 7.5bps cost per flip, ~0.5% avg move at 4h
            cost_drag = turnover * 0.00075 / 0.005  # as fraction of expected return
            adj_ic = rho * (1 - cost_drag)

            results.append({
                "feature": feat,
                "asset": asset,
                "raw_ic": float(rho),
                "turnover": float(turnover),
                "cost_drag_pct": float(cost_drag * 100),
                "adjusted_ic": float(adj_ic),
                "n": int(n),
            })

    out = pd.DataFrame(results)
    if not out.empty:
        summary = out.groupby("feature")[["raw_ic", "turnover", "adjusted_ic"]].mean()
        print("   Feature | Raw IC | Turnover | Adj IC")
        for feat, row in summary.iterrows():
            print(f"   {feat}: {row['raw_ic']:.4f} | {row['turnover']:.2%} | {row['adjusted_ic']:.4f}")
    return out


# === ANALYSIS 9: Composite scoring and final shortlist ===
def analysis_composite_score(
    lagged_ic: pd.DataFrame,
    rolling_ic: pd.DataFrame,
    per_asset: pd.DataFrame,
    turnover_adj: pd.DataFrame,
) -> pd.DataFrame:
    """Score each signal candidate on multiple dimensions."""
    print("\n[9/9] Computing composite scores...")

    top5 = ["stable_buy_usd_z24", "stablecoin_conversion_imbalance_z24",
             "dex_flow_imbalance_z24", "dex_flow_imbalance", "buy_usd_z24",
             "stable_buy_usd_z168", "dex_flow_imbalance_z168"]

    scores = []
    for feat in top5:
        # 1. Raw IC at lag=1, 4h horizon
        ic_row = lagged_ic[(lagged_ic["feature"] == feat) &
                           (lagged_ic["lag_hours"] == 1) &
                           (lagged_ic["horizon"] == "4h")]
        raw_ic = ic_row["rho"].values[0] if len(ic_row) > 0 else 0

        # 2. IC stability (from rolling)
        if not rolling_ic.empty:
            stab = rolling_ic[rolling_ic["feature"] == feat]
            if not stab.empty:
                ic_ir = stab["rho"].mean() / max(stab["rho"].std(), 0.001)
                pct_pos = (stab["rho"] > 0).mean()
            else:
                ic_ir = 0
                pct_pos = 0
        else:
            ic_ir = 0
            pct_pos = 0

        # 3. Universality (how many assets show significance)
        asset_sig = per_asset[(per_asset["feature"] == feat) &
                              (per_asset["horizon"] == "4h") &
                              (per_asset["p_value"] < 0.05)]
        n_assets_sig = len(asset_sig)

        # 4. Turnover-adjusted IC
        ta = turnover_adj[turnover_adj["feature"] == feat]
        adj_ic = ta["adjusted_ic"].mean() if not ta.empty else raw_ic
        avg_turnover = ta["turnover"].mean() if not ta.empty else 0.5

        # Composite: weighted sum
        composite = (
            0.30 * min(abs(raw_ic) / 0.06, 1.0) +  # raw IC strength (cap at 0.06)
            0.25 * min(ic_ir / 1.5, 1.0) +  # IC stability
            0.20 * (n_assets_sig / 5.0) +  # universality
            0.15 * min(abs(adj_ic) / 0.05, 1.0) +  # cost-adjusted IC
            0.10 * (1 - avg_turnover)  # low turnover bonus
        )

        scores.append({
            "feature": feat,
            "raw_ic_4h": raw_ic,
            "ic_ir": ic_ir,
            "pct_windows_positive": pct_pos,
            "n_assets_significant": n_assets_sig,
            "adjusted_ic": adj_ic,
            "avg_turnover": avg_turnover,
            "composite_score": composite,
        })

    out = pd.DataFrame(scores).sort_values("composite_score", ascending=False)
    print("\n   === FINAL RANKING ===")
    print(f"   {'Feature':<45} {'IC':>6} {'IC_IR':>6} {'Assets':>6} {'Adj_IC':>7} {'Score':>6}")
    for _, r in out.iterrows():
        print(f"   {r['feature']:<45} {r['raw_ic_4h']:>6.4f} {r['ic_ir']:>6.2f} "
              f"{r['n_assets_significant']:>6} {r['adjusted_ic']:>7.4f} {r['composite_score']:>6.3f}")
    return out


def main():
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    print(f"=== X7 Deep EDA — {ts} ===")
    print(f"Panel: {PANEL_PATH}")

    df = load_panel()
    print(f"Loaded: {df.shape[0]} rows, {df.shape[1]} cols, {df['asset'].nunique()} assets")
    print(f"Date range: {df['hour'].min()} to {df['hour'].max()}")

    # Run all analyses
    lagged_ic = analysis_lagged_ic(df)
    rolling_ic = analysis_rolling_ic(df)
    ic_decay = analysis_ic_decay(df)
    regime = analysis_regime(df)
    per_asset = analysis_per_asset(df)
    quintiles = analysis_quintiles(df)
    autocorr = analysis_autocorrelation(df)
    turnover_adj = analysis_turnover_adjusted(df)
    composite = analysis_composite_score(lagged_ic, rolling_ic, per_asset, turnover_adj)

    # Save all outputs
    prefix = f"{ts}_deep_eda"
    lagged_ic.to_csv(OUTPUT_DIR / f"{prefix}_lagged_ic.csv", index=False)
    rolling_ic.to_csv(OUTPUT_DIR / f"{prefix}_rolling_ic.csv", index=False)
    ic_decay.to_csv(OUTPUT_DIR / f"{prefix}_ic_decay.csv", index=False)
    regime.to_csv(OUTPUT_DIR / f"{prefix}_regime.csv", index=False)
    per_asset.to_csv(OUTPUT_DIR / f"{prefix}_per_asset.csv", index=False)
    quintiles.to_csv(OUTPUT_DIR / f"{prefix}_quintiles.csv", index=False)
    autocorr.to_csv(OUTPUT_DIR / f"{prefix}_autocorrelation.csv", index=False)
    turnover_adj.to_csv(OUTPUT_DIR / f"{prefix}_turnover_adjusted.csv", index=False)
    composite.to_csv(OUTPUT_DIR / f"{prefix}_composite_scores.csv", index=False)

    # Summary JSON
    summary = {
        "timestamp": ts,
        "panel_shape": list(df.shape),
        "date_range": [str(df["hour"].min()), str(df["hour"].max())],
        "top_candidates": composite.head(5).to_dict(orient="records"),
        "analyses_run": [
            "lagged_ic", "rolling_ic", "ic_decay", "regime",
            "per_asset", "quintiles", "autocorrelation", "turnover_adjusted", "composite"
        ],
    }
    with open(OUTPUT_DIR / f"{prefix}_summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    print(f"\n=== Done. Outputs in {OUTPUT_DIR}/{prefix}_* ===")
    return composite


if __name__ == "__main__":
    main()
