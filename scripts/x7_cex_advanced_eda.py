#!/usr/bin/env python3
"""
Advanced CEX subsecond EDA for practical exploitability.

What this adds beyond basic lead-lag scans:
1) Multi-signal tail-event sweep with fees and latency delay.
2) Cross-asset spillover (leader -> follower transfer test).
3) Purged walk-forward micro model (XGBoost) with strict feature lag.
4) On-chain regime gating overlay from X7 stable_buy signal.
5) Permutation significance test for the best deployable config.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

try:
    from xgboost import XGBRegressor
except Exception:  # pragma: no cover
    XGBRegressor = None


ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE"]
FEATURES = [
    "lead1",
    "lead2",
    "lead3",
    "ofi_diff",
    "spread_z",
    "vol_ratio",
    "rv_1m",
    "lead_x_spread",
    "lead_x_ofi",
]


@dataclass
class ModelConfig:
    train_days: int = 4
    test_days: int = 1
    horizon_5s: int = 2  # 10s target
    cost_bps: float = 0.5
    q: float = 0.9
    purge_bars: int = 6


def load_onchain_gate(panel_path: Path) -> dict[str, pd.DataFrame]:
    panel = pd.read_csv(panel_path)
    panel["hour"] = pd.to_datetime(panel["hour"], utc=True)
    panel = panel.sort_values(["asset", "hour"]).copy()
    panel["gate"] = panel.groupby("asset")["stable_buy_usd_z24"].shift(1)
    out: dict[str, pd.DataFrame] = {}
    for asset in ASSETS:
        out[asset] = panel.loc[panel["asset"] == asset, ["hour", "gate"]].dropna().copy()
    return out


def load_asset(asset: str, root: Path, gate_map: dict[str, pd.DataFrame]) -> pd.DataFrame:
    symbol = f"{asset}USDT"
    path = root / symbol / f"{symbol}_5s_merged_14d.parquet"
    if not path.exists():
        return pd.DataFrame()

    df = pd.read_parquet(path).reset_index()
    ts_col = "timestamp" if "timestamp" in df.columns else df.columns[0]
    df["ts"] = pd.to_datetime(df[ts_col], utc=True)

    numeric = [
        "bn_vwap",
        "bb_vwap",
        "spread_vwap_bps",
        "bn_buy_count",
        "bb_buy_count",
        "bn_trade_count",
        "bb_trade_count",
        "bn_volume_usd",
        "bb_volume_usd",
    ]
    for c in numeric:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(subset=["bn_vwap", "bb_vwap"]).sort_values("ts").copy()
    if len(df) < 20_000:
        return pd.DataFrame()

    df["bn_ret1"] = np.log(df["bn_vwap"] / df["bn_vwap"].shift(1))
    df["bb_ret1"] = np.log(df["bb_vwap"] / df["bb_vwap"].shift(1))
    df["lead1"] = df["bn_ret1"] - df["bb_ret1"]
    df["lead2"] = np.log(df["bn_vwap"] / df["bn_vwap"].shift(2)) - np.log(df["bb_vwap"] / df["bb_vwap"].shift(2))
    df["lead3"] = np.log(df["bn_vwap"] / df["bn_vwap"].shift(3)) - np.log(df["bb_vwap"] / df["bb_vwap"].shift(3))

    df["ofi_bn"] = 2.0 * (df["bn_buy_count"] / df["bn_trade_count"].clip(lower=1.0)) - 1.0
    df["ofi_bb"] = 2.0 * (df["bb_buy_count"] / df["bb_trade_count"].clip(lower=1.0)) - 1.0
    df["ofi_diff"] = df["ofi_bn"] - df["ofi_bb"]

    roll = 120  # 10 minutes on 5s bars
    spread = df["spread_vwap_bps"]
    mu = spread.rolling(roll, min_periods=roll // 2).mean()
    sd = spread.rolling(roll, min_periods=roll // 2).std().replace(0, np.nan)
    df["spread_z"] = (spread - mu) / sd

    df["vol_ratio"] = np.log1p(df["bn_volume_usd"]) - np.log1p(df["bb_volume_usd"])
    df["rv_1m"] = df["bb_ret1"].rolling(12, min_periods=6).std() * np.sqrt(12)
    df["lead_x_spread"] = df["lead1"] * df["spread_z"]
    df["lead_x_ofi"] = df["lead1"] * df["ofi_diff"]

    # strict causal merge: on-chain gate is hourly and already shifted by +1h
    df["hour"] = df["ts"].dt.floor("h")
    g = gate_map.get(asset, pd.DataFrame(columns=["hour", "gate"]))
    df = df.merge(g, on="hour", how="left")
    df["asset"] = asset

    keep = ["asset", "ts", "hour", "bb_vwap", "gate"] + FEATURES
    return df[keep].dropna(subset=["bb_vwap"]).copy()


def add_forward_returns(df: pd.DataFrame, horizons: list[int], latencies: list[int]) -> pd.DataFrame:
    out = df.copy()
    for h in horizons:
        for l in latencies:
            c = f"ret_h{h}_l{l}"
            out[c] = np.log(out["bb_vwap"].shift(-(h + l)) / out["bb_vwap"].shift(-l)) * 10000.0
    return out


def tail_sweep(
    df: pd.DataFrame,
    asset: str,
    signals: list[str],
    horizons: list[int],
    latencies: list[int],
    q_values: list[float],
    costs: list[float],
) -> pd.DataFrame:
    rows = []
    for sig in signals:
        if sig not in df.columns:
            continue
        s = df[sig].copy()
        for h in horizons:
            for l in latencies:
                rcol = f"ret_h{h}_l{l}"
                d = df[[sig, rcol, "gate"]].dropna().copy()
                if len(d) < 500:
                    continue
                for q in q_values:
                    hi = d[sig].quantile(q)
                    lo = d[sig].quantile(1 - q)
                    pos = np.where(d[sig] >= hi, 1, np.where(d[sig] <= lo, -1, 0))
                    trade_mask = pos != 0
                    trade_rate = float(np.mean(trade_mask))
                    if trade_rate < 0.005:
                        continue
                    gross = pos * d[rcol].to_numpy()
                    tr_gross = gross[trade_mask]
                    rho, p_ic = spearmanr(d[sig], d[rcol], nan_policy="omit")
                    for cost in costs:
                        net = gross - np.abs(pos) * cost
                        tr_net = net[trade_mask]
                        rows.append(
                            {
                                "asset": asset,
                                "signal": sig,
                                "h_5s": h,
                                "latency_5s": l,
                                "q": q,
                                "cost_bps": cost,
                                "n": int(len(d)),
                                "trade_rate": trade_rate,
                                "gross_bps": float(np.mean(gross)),
                                "net_bps": float(np.mean(net)),
                                "trade_gross_bps": float(np.mean(tr_gross)) if len(tr_gross) else np.nan,
                                "trade_net_bps": float(np.mean(tr_net)) if len(tr_net) else np.nan,
                                "win_trade": float(np.mean(tr_net > 0)) if len(tr_net) else np.nan,
                                "ic_spearman": float(rho) if pd.notna(rho) else np.nan,
                                "ic_p_value": float(p_ic) if pd.notna(p_ic) else np.nan,
                                "gated_net_bps": float(np.mean(net[d["gate"].to_numpy() > 1.0]))
                                if np.any(d["gate"].to_numpy() > 1.0)
                                else np.nan,
                            }
                        )
    return pd.DataFrame(rows)


def spillover_test(data: dict[str, pd.DataFrame], cost_bps: float = 0.5, q: float = 0.99, h: int = 2) -> pd.DataFrame:
    rows = []
    leader = "BTC"
    if leader not in data or data[leader].empty:
        return pd.DataFrame(rows)

    lead = data[leader][["ts", "lead1"]].dropna().copy()
    for follower, fdf in data.items():
        if follower == leader or fdf.empty:
            continue
        d = fdf[["ts", "bb_vwap"]].copy()
        d[f"ret_h{h}"] = np.log(d["bb_vwap"].shift(-h) / d["bb_vwap"]) * 10000.0
        m = d.merge(lead, on="ts", how="inner").dropna(subset=["lead1", f"ret_h{h}"])
        if len(m) < 1000:
            continue
        hi = m["lead1"].quantile(q)
        lo = m["lead1"].quantile(1 - q)
        pos = np.where(m["lead1"] >= hi, 1, np.where(m["lead1"] <= lo, -1, 0))
        gross = pos * m[f"ret_h{h}"].to_numpy()
        net = gross - np.abs(pos) * cost_bps
        mask = pos != 0
        rho, p = spearmanr(m["lead1"], m[f"ret_h{h}"], nan_policy="omit")
        rows.append(
            {
                "leader": leader,
                "follower": follower,
                "n": int(len(m)),
                "trade_rate": float(np.mean(mask)),
                "gross_bps": float(np.mean(gross)),
                "net_bps": float(np.mean(net)),
                "trade_net_bps": float(np.mean(net[mask])) if np.any(mask) else np.nan,
                "win_trade": float(np.mean(net[mask] > 0)) if np.any(mask) else np.nan,
                "ic_spearman": float(rho) if pd.notna(rho) else np.nan,
                "ic_p_value": float(p) if pd.notna(p) else np.nan,
            }
        )
    return pd.DataFrame(rows)


def walkforward_model(df: pd.DataFrame, cfg: ModelConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    # strict causality: feature at t-1 predicts ret from t -> t+h
    d = df[["asset", "ts", "hour", "gate", "bb_vwap"] + FEATURES].copy()
    for c in FEATURES:
        d[c] = d[c].shift(1)

    h = cfg.horizon_5s
    d["y"] = np.log(d["bb_vwap"].shift(-h) / d["bb_vwap"]) * 10000.0

    # non-overlap for target horizon
    d = d.iloc[::h].copy()
    d["date"] = d["ts"].dt.date
    d = d.dropna(subset=FEATURES + ["y"]).copy()

    days = sorted(d["date"].unique())
    if len(days) < cfg.train_days + cfg.test_days + 1:
        return pd.DataFrame(), pd.DataFrame()

    if XGBRegressor is None:
        return pd.DataFrame(), pd.DataFrame()

    fold_rows = []
    trade_rows = []
    fold = 0

    for i in range(cfg.train_days, len(days) - cfg.test_days + 1):
        tr_days = days[i - cfg.train_days : i]
        te_days = days[i : i + cfg.test_days]
        if not te_days:
            continue
        train = d[d["date"].isin(tr_days)].copy()
        test = d[d["date"].isin(te_days)].copy()
        if train.empty or test.empty:
            continue

        # purge from train tail to avoid near-boundary leakage
        cutoff = pd.Timestamp(str(te_days[0]), tz="UTC")
        train = train[train["ts"] <= cutoff - pd.Timedelta(seconds=cfg.purge_bars * 5)].copy()
        if len(train) < 2000 or len(test) < 500:
            continue

        xgb = XGBRegressor(
            n_estimators=260,
            max_depth=4,
            learning_rate=0.05,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.0,
            reg_lambda=1.0,
            objective="reg:squarederror",
            random_state=42,
            n_jobs=4,
        )
        xgb.fit(train[FEATURES], train["y"])
        train_pred = xgb.predict(train[FEATURES])
        test_pred = xgb.predict(test[FEATURES])
        thr = float(np.quantile(np.abs(train_pred), cfg.q))

        pos = np.where(np.abs(test_pred) >= thr, np.sign(test_pred), 0.0)
        net = pos * test["y"].to_numpy() - np.abs(pos) * cfg.cost_bps

        gate_arr = test["gate"].to_numpy()
        gated_pos = np.where(gate_arr > 1.0, pos, 0.0)
        gated_net = gated_pos * test["y"].to_numpy() - np.abs(gated_pos) * cfg.cost_bps

        fold += 1
        mask = pos != 0
        gmask = gated_pos != 0
        fold_rows.append(
            {
                "asset": str(test["asset"].iloc[0]),
                "fold": fold,
                "train_start": str(min(tr_days)),
                "train_end": str(max(tr_days)),
                "test_start": str(min(te_days)),
                "test_end": str(max(te_days)),
                "n_train": int(len(train)),
                "n_test": int(len(test)),
                "trade_rate": float(np.mean(mask)),
                "gross_bps": float(np.mean(pos * test["y"].to_numpy())),
                "net_bps": float(np.mean(net)),
                "trade_net_bps": float(np.mean(net[mask])) if np.any(mask) else np.nan,
                "win_trade": float(np.mean(net[mask] > 0)) if np.any(mask) else np.nan,
                "gated_trade_rate": float(np.mean(gmask)),
                "gated_net_bps": float(np.mean(gated_net)),
                "gated_trade_net_bps": float(np.mean(gated_net[gmask])) if np.any(gmask) else np.nan,
                "gated_win_trade": float(np.mean(gated_net[gmask] > 0)) if np.any(gmask) else np.nan,
            }
        )

        trade = test[["ts", "asset", "y", "gate"]].copy()
        trade["pred"] = test_pred
        trade["pos"] = pos
        trade["net"] = net
        trade["gated_pos"] = gated_pos
        trade["gated_net"] = gated_net
        trade_rows.append(trade)

    return pd.DataFrame(fold_rows), (pd.concat(trade_rows, ignore_index=True) if trade_rows else pd.DataFrame())


def permutation_significance(trades: pd.DataFrame, n_perm: int = 300) -> dict[str, float]:
    if trades.empty:
        return {"n_perm": n_perm, "obs_net_bps": np.nan, "perm_p_value": np.nan}

    # Use gated strategy as stricter deployable series.
    d = trades.dropna(subset=["gated_net"]).copy()
    if len(d) < 1000:
        return {"n_perm": n_perm, "obs_net_bps": np.nan, "perm_p_value": np.nan}
    d["date"] = pd.to_datetime(d["ts"], utc=True).dt.date

    obs = float(d["gated_net"].mean())
    by_day = [g["gated_net"].to_numpy() for _, g in d.groupby("date")]
    if len(by_day) < 4:
        return {"n_perm": n_perm, "obs_net_bps": obs, "perm_p_value": np.nan}

    rng = np.random.default_rng(42)
    perm_vals = []
    for _ in range(n_perm):
        idx = rng.permutation(len(by_day))
        s = np.concatenate([by_day[i] for i in idx])
        perm_vals.append(float(np.mean(s)))
    p_val = float((1.0 + np.sum(np.array(perm_vals) >= obs)) / (n_perm + 1.0))
    return {"n_perm": n_perm, "obs_net_bps": obs, "perm_p_value": p_val}


def main() -> None:
    p = argparse.ArgumentParser(description="Advanced subsecond CEX EDA.")
    p.add_argument("--arb-root", default="app/data/cache/arb_trades")
    p.add_argument("--panel", default="app/data/cache/onchain_x7_advanced/20260420_154932_feature_panel.csv")
    p.add_argument("--out-prefix", default="app/data/cache/x7_cex_advanced_eda_20260420")
    args = p.parse_args()

    gate_map = load_onchain_gate(Path(args.panel))
    root = Path(args.arb_root)
    out_prefix = Path(args.out_prefix)

    data: dict[str, pd.DataFrame] = {}
    for a in ASSETS:
        df = load_asset(a, root, gate_map)
        if not df.empty:
            data[a] = add_forward_returns(df, horizons=[2, 6, 12], latencies=[0, 1, 2, 3, 6])

    all_tail = []
    model_fold = []
    model_trades = []
    for a, df in data.items():
        tail = tail_sweep(
            df=df,
            asset=a,
            signals=["lead1", "lead2", "ofi_diff", "lead_x_spread", "lead_x_ofi"],
            horizons=[2, 6, 12],
            latencies=[0, 1, 2, 3, 6],
            q_values=[0.9, 0.95, 0.97, 0.98, 0.99],
            costs=[0.25, 0.5, 1.0],
        )
        if not tail.empty:
            all_tail.append(tail)

        cfg = ModelConfig()
        fold_df, trades_df = walkforward_model(df, cfg)
        if not fold_df.empty:
            model_fold.append(fold_df)
        if not trades_df.empty:
            model_trades.append(trades_df)

    tail_df = pd.concat(all_tail, ignore_index=True) if all_tail else pd.DataFrame()
    spill_df = spillover_test(data, cost_bps=0.5, q=0.99, h=2)
    fold_df = pd.concat(model_fold, ignore_index=True) if model_fold else pd.DataFrame()
    trades_df = pd.concat(model_trades, ignore_index=True) if model_trades else pd.DataFrame()

    # latency decay from best event per (asset, signal, h, cost)
    latency_decay = pd.DataFrame()
    if not tail_df.empty:
        keys = ["asset", "signal", "h_5s", "cost_bps", "latency_5s"]
        best_q = (
            tail_df.sort_values("net_bps", ascending=False)
            .groupby(keys, as_index=False)
            .head(1)
            .copy()
        )
        latency_decay = (
            best_q.groupby(["asset", "signal", "h_5s", "cost_bps", "latency_5s"], as_index=False)["net_bps"]
            .mean()
            .sort_values(["asset", "signal", "h_5s", "cost_bps", "latency_5s"])
        )

    model_summary = pd.DataFrame()
    perm_rows = []
    if not fold_df.empty:
        model_summary = (
            fold_df.groupby("asset", as_index=False)
            .agg(
                folds=("fold", "count"),
                net_bps=("net_bps", "mean"),
                trade_net_bps=("trade_net_bps", "mean"),
                win_trade=("win_trade", "mean"),
                trade_rate=("trade_rate", "mean"),
                gated_net_bps=("gated_net_bps", "mean"),
                gated_trade_net_bps=("gated_trade_net_bps", "mean"),
                gated_win_trade=("gated_win_trade", "mean"),
                gated_trade_rate=("gated_trade_rate", "mean"),
            )
            .sort_values("gated_net_bps", ascending=False)
        )
        if not trades_df.empty:
            for asset, g in trades_df.groupby("asset"):
                perm = permutation_significance(g, n_perm=300)
                perm["asset"] = asset
                perm_rows.append(perm)
    perm_df = pd.DataFrame(perm_rows)

    tail_path = f"{out_prefix}_tail_sweep.csv"
    spill_path = f"{out_prefix}_spillover.csv"
    lat_path = f"{out_prefix}_latency_decay.csv"
    fold_path = f"{out_prefix}_model_wf_folds.csv"
    trades_path = f"{out_prefix}_model_wf_trades.csv"
    msum_path = f"{out_prefix}_model_summary.csv"
    perm_path = f"{out_prefix}_perm.csv"

    if not tail_df.empty:
        tail_df.to_csv(tail_path, index=False)
    if not spill_df.empty:
        spill_df.to_csv(spill_path, index=False)
    if not latency_decay.empty:
        latency_decay.to_csv(lat_path, index=False)
    if not fold_df.empty:
        fold_df.to_csv(fold_path, index=False)
    if not trades_df.empty:
        trades_df.to_csv(trades_path, index=False)
    if not model_summary.empty:
        model_summary.to_csv(msum_path, index=False)
    if not perm_df.empty:
        perm_df.to_csv(perm_path, index=False)

    summary = {
        "tail_sweep_file": tail_path if not tail_df.empty else None,
        "spillover_file": spill_path if not spill_df.empty else None,
        "latency_decay_file": lat_path if not latency_decay.empty else None,
        "model_folds_file": fold_path if not fold_df.empty else None,
        "model_summary_file": msum_path if not model_summary.empty else None,
        "perm_file": perm_path if not perm_df.empty else None,
        "counts": {
            "assets_loaded": len(data),
            "tail_rows": int(len(tail_df)),
            "spill_rows": int(len(spill_df)),
            "wf_folds": int(len(fold_df)),
            "wf_trades_rows": int(len(trades_df)),
        },
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
