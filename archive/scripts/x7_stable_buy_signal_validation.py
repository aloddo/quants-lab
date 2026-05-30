#!/usr/bin/env python3
"""
Validation script for lagged stable-buy flow signal on 4h horizon.

Signal:
  stable_buy_usd_z24 (lagged by 1h) with per-asset orientation learned on train split.
Execution:
  Bybit perp directional, rebalanced every 4 hours, equal-weight across assets.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr


def load_panel(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    df["hour"] = pd.to_datetime(df["hour"], utc=True)
    return df.sort_values(["asset", "hour"]).copy()


def apply_feature_lag(df: pd.DataFrame, lag_hours: int = 1) -> pd.DataFrame:
    out = df.copy()
    lag_cols = [c for c in out.columns if c not in {"hour", "asset", "fwd_ret_1h", "fwd_ret_4h", "fwd_ret_24h"}]
    for c in lag_cols:
        out[c] = out.groupby("asset")[c].shift(lag_hours)
    return out


def summarize_returns(hourly: pd.DataFrame) -> dict[str, float]:
    if hourly["ret"].std() > 0:
        sharpe = float((hourly["ret"].mean() / hourly["ret"].std()) * np.sqrt(6 * 365))
    else:
        sharpe = np.nan
    return {
        "hours": int(len(hourly)),
        "total_return": float((1.0 + hourly["ret"]).prod() - 1.0),
        "mean_ret_4h": float(hourly["ret"].mean()),
        "std_ret_4h": float(hourly["ret"].std()),
        "ann_sharpe": sharpe,
        "hit_rate": float((hourly["ret"] > 0).mean()),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="Validate lagged stable-buy flow signal.")
    p.add_argument("--panel", required=True, help="Feature panel CSV path.")
    p.add_argument("--out-dir", default="app/data/cache/onchain_x7_advanced")
    p.add_argument("--fee-oneway", type=float, default=0.00055, help="Fee/slippage per 1x turnover unit.")
    p.add_argument("--lag-hours", type=int, default=1)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--permutations", type=int, default=500)
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = load_panel(Path(args.panel))
    df = apply_feature_lag(df, lag_hours=args.lag_hours)

    # avoid overlap for 4h horizon
    df = df[df["hour"].dt.hour % 4 == 0].copy()
    df = df[["hour", "asset", "stable_buy_usd_z24", "fwd_ret_4h"]].dropna()

    hours = sorted(df["hour"].unique())
    split = hours[int(len(hours) * 0.5)]
    train = df[df["hour"] < split].copy()
    test = df[df["hour"] >= split].copy()

    # learn per-asset orientation from train
    orientation = {}
    for asset, g in train.groupby("asset"):
        rho, _ = spearmanr(g["stable_buy_usd_z24"], g["fwd_ret_4h"], nan_policy="omit")
        orientation[asset] = 1 if (pd.notna(rho) and rho >= 0) else -1

    def run_block(data: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        d = data.sort_values(["asset", "hour"]).copy()
        d["sig"] = d.apply(lambda r: orientation[r["asset"]] * r["stable_buy_usd_z24"], axis=1)
        d["pos"] = np.sign(d["sig"]).astype(int)
        d["chg"] = d.groupby("asset")["pos"].diff().abs().fillna(d["pos"].abs())
        d["asset_ret"] = d["pos"] * d["fwd_ret_4h"] - args.fee_oneway * d["chg"]

        hr = (
            d.groupby("hour")
            .agg(
                ret=("asset_ret", "mean"),
                gross_turnover=("chg", "sum"),
                n_assets=("asset", "count"),
            )
            .reset_index()
        )
        return d, hr

    train_asset, train_hourly = run_block(train)
    test_asset, test_hourly = run_block(test)

    # permutation test on test split
    rng = np.random.default_rng(args.seed)
    perm_means = []
    base = test_asset[["hour", "asset", "pos", "chg", "fwd_ret_4h"]].copy()
    for _ in range(args.permutations):
        perm = base.copy()
        shuffled = []
        for _, g in perm.groupby("asset"):
            arr = g["fwd_ret_4h"].to_numpy().copy()
            rng.shuffle(arr)
            shuffled.append(pd.Series(arr, index=g.index))
        perm["fwd_ret_perm"] = pd.concat(shuffled).sort_index()
        perm["asset_ret"] = perm["pos"] * perm["fwd_ret_perm"] - args.fee_oneway * perm["chg"]
        perm_hour = perm.groupby("hour")["asset_ret"].mean().mean()
        perm_means.append(perm_hour)
    perm_means = np.array(perm_means)
    obs_mean = test_hourly["ret"].mean()
    p_value_perm = float((np.sum(perm_means >= obs_mean) + 1) / (len(perm_means) + 1))

    # per-asset diagnostics on test
    per_asset_rows = []
    for asset, g in test_asset.groupby("asset"):
        if g["asset_ret"].std() > 0:
            sh = float((g["asset_ret"].mean() / g["asset_ret"].std()) * np.sqrt(6 * 365))
        else:
            sh = np.nan
        per_asset_rows.append(
            {
                "asset": asset,
                "n": int(len(g)),
                "total_return": float((1.0 + g["asset_ret"]).prod() - 1.0),
                "ann_sharpe": sh,
                "hit_rate": float((g["asset_ret"] > 0).mean()),
                "avg_turnover": float(g["chg"].mean()),
            }
        )
    per_asset = pd.DataFrame(per_asset_rows).sort_values("ann_sharpe", ascending=False)

    summary = {
        "signal": "stable_buy_usd_z24 (lagged 1h)",
        "horizon": "4h",
        "split_rule": "50/50 chronological split over 4h decision timestamps",
        "fee_oneway": args.fee_oneway,
        "orientation": orientation,
        "train_metrics": summarize_returns(train_hourly),
        "test_metrics": summarize_returns(test_hourly),
        "test_permutation_p_value": p_value_perm,
        "test_perm_mean_avg": float(perm_means.mean()),
        "test_perm_mean_std": float(perm_means.std()),
    }

    stamp = pd.Timestamp.now(tz="UTC").strftime("%Y%m%d_%H%M%S")
    summary_path = out_dir / f"{stamp}_stable_buy_validation_summary.json"
    train_path = out_dir / f"{stamp}_stable_buy_train_hourly.csv"
    test_path = out_dir / f"{stamp}_stable_buy_test_hourly.csv"
    per_asset_path = out_dir / f"{stamp}_stable_buy_test_per_asset.csv"

    summary_path.write_text(json.dumps(summary, indent=2))
    train_hourly.to_csv(train_path, index=False)
    test_hourly.to_csv(test_path, index=False)
    per_asset.to_csv(per_asset_path, index=False)

    print(json.dumps(summary, indent=2))
    print("\nPer-asset test diagnostics:")
    print(per_asset.to_string(index=False))
    print("\nArtifacts:")
    print(f"- {summary_path}")
    print(f"- {train_path}")
    print(f"- {test_path}")
    print(f"- {per_asset_path}")


if __name__ == "__main__":
    main()
