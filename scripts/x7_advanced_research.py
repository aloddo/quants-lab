#!/usr/bin/env python3
"""
X7 advanced on-chain research pipeline.

What it does:
1) Pull richer hourly on-chain feature panels from Dune for BTC/ETH/SOL/XRP/DOGE
   over last 90 days.
2) Align with Bybit perp hourly forward returns from local parquet.
3) Run univariate Spearman IC screens.
4) Run a walk-forward predictive ensemble (XGBoost + Ridge) and simulate
   a practical top2/bottom2 portfolio with turnover costs.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import requests
from scipy.stats import spearmanr
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler
from xgboost import XGBRegressor


ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE"]
HORIZONS = [1, 4, 24]
STABLECOINS = ["USDT", "USDC", "DAI", "FDUSD", "TUSD", "USDE", "USDD", "PYUSD", "LUSD", "FRAX"]

ASSET_SYMBOLS = {
    "BTC": ["BTC", "WBTC", "TBTC", "BTCB"],
    "ETH": ["ETH", "WETH"],
    "SOL": ["SOL", "WSOL"],
    "XRP": ["XRP", "WXRP"],
    "DOGE": ["DOGE", "WDOGE"],
}


@dataclass
class QuerySpec:
    query_id: str
    hypothesis: str
    sql_builder: Callable[[], str]


def _all_symbols(mapping: dict[str, list[str]]) -> list[str]:
    out: set[str] = set()
    for vals in mapping.values():
        out.update(v.upper() for v in vals)
    return sorted(out)


def _build_case_expr(column: str) -> str:
    lines = ["CASE"]
    for asset, symbols in ASSET_SYMBOLS.items():
        symbols_sql = ", ".join(f"'{s}'" for s in symbols)
        lines.append(f"    WHEN upper({column}) IN ({symbols_sql}) THEN '{asset}'")
    lines.append("    ELSE NULL")
    lines.append("END")
    return "\n".join(lines)


def build_dex_feature_sql() -> str:
    asset_symbols = ", ".join(f"'{s}'" for s in _all_symbols(ASSET_SYMBOLS))
    stable_symbols = ", ".join(f"'{s}'" for s in STABLECOINS)
    asset_bought_case = _build_case_expr("token_bought_symbol")
    asset_sold_case = _build_case_expr("token_sold_symbol")
    return f"""
WITH base AS (
    SELECT
        date_trunc('hour', block_time) AS hour,
        tx_from,
        blockchain,
        amount_usd,
        upper(token_bought_symbol) AS token_bought_symbol,
        upper(token_sold_symbol) AS token_sold_symbol,
        {asset_bought_case} AS asset_bought,
        {asset_sold_case} AS asset_sold
    FROM dex.trades
    WHERE block_time >= now() - interval '90' day
      AND amount_usd IS NOT NULL
      AND (
          upper(token_bought_symbol) IN ({asset_symbols})
          OR upper(token_sold_symbol) IN ({asset_symbols})
      )
),
oriented AS (
    SELECT
        hour,
        asset_bought AS asset,
        'buy' AS side,
        tx_from,
        blockchain,
        amount_usd,
        CASE WHEN token_sold_symbol IN ({stable_symbols}) THEN 1 ELSE 0 END AS stable_counterparty
    FROM base
    WHERE asset_bought IS NOT NULL

    UNION ALL

    SELECT
        hour,
        asset_sold AS asset,
        'sell' AS side,
        tx_from,
        blockchain,
        amount_usd,
        CASE WHEN token_bought_symbol IN ({stable_symbols}) THEN 1 ELSE 0 END AS stable_counterparty
    FROM base
    WHERE asset_sold IS NOT NULL
),
agg AS (
    SELECT
        hour,
        asset,
        sum(CASE WHEN side='buy' THEN amount_usd ELSE 0 END) AS buy_usd,
        sum(CASE WHEN side='sell' THEN amount_usd ELSE 0 END) AS sell_usd,
        sum(CASE WHEN side='buy' AND amount_usd >= 100000 THEN amount_usd ELSE 0 END) AS buy_usd_large,
        sum(CASE WHEN side='sell' AND amount_usd >= 100000 THEN amount_usd ELSE 0 END) AS sell_usd_large,
        sum(CASE WHEN side='buy' AND stable_counterparty=1 THEN amount_usd ELSE 0 END) AS stable_buy_usd,
        sum(CASE WHEN side='sell' AND stable_counterparty=1 THEN amount_usd ELSE 0 END) AS stable_sell_usd,
        approx_distinct(CASE WHEN side='buy' THEN tx_from END) AS buy_trader_count,
        approx_distinct(CASE WHEN side='sell' THEN tx_from END) AS sell_trader_count,
        approx_distinct(tx_from) AS total_trader_count,
        approx_distinct(blockchain) AS chain_count
    FROM oriented
    GROUP BY 1,2
)
SELECT
    hour,
    asset,
    buy_usd,
    sell_usd,
    buy_usd_large,
    sell_usd_large,
    stable_buy_usd,
    stable_sell_usd,
    buy_trader_count,
    sell_trader_count,
    total_trader_count,
    chain_count,
    CASE
        WHEN buy_usd + sell_usd = 0 THEN 0
        ELSE CAST(buy_usd - sell_usd AS double) / CAST(buy_usd + sell_usd AS double)
    END AS dex_flow_imbalance,
    CASE
        WHEN buy_usd_large + sell_usd_large = 0 THEN 0
        ELSE CAST(buy_usd_large - sell_usd_large AS double) / CAST(buy_usd_large + sell_usd_large AS double)
    END AS dex_large_flow_imbalance,
    CASE
        WHEN buy_trader_count + sell_trader_count = 0 THEN 0
        ELSE CAST(buy_trader_count - sell_trader_count AS double) / CAST(buy_trader_count + sell_trader_count AS double)
    END AS dex_breadth_imbalance,
    CASE
        WHEN stable_buy_usd + stable_sell_usd = 0 THEN 0
        ELSE CAST(stable_buy_usd - stable_sell_usd AS double) / CAST(stable_buy_usd + stable_sell_usd AS double)
    END AS stablecoin_conversion_imbalance,
    CASE
        WHEN buy_usd + sell_usd = 0 THEN 0
        ELSE CAST(stable_buy_usd + stable_sell_usd AS double) / CAST(buy_usd + sell_usd AS double)
    END AS stablecoin_pair_share
FROM agg
ORDER BY 1,2
"""


def build_transfer_feature_sql() -> str:
    asset_symbols = ", ".join(f"'{s}'" for s in _all_symbols(ASSET_SYMBOLS))
    asset_case = _build_case_expr("symbol")
    return f"""
WITH base AS (
    SELECT
        date_trunc('hour', block_time) AS hour,
        {asset_case} AS asset,
        blockchain,
        amount_usd,
        "from" AS sender,
        "to" AS receiver
    FROM tokens.transfers
    WHERE block_time >= now() - interval '90' day
      AND amount_usd IS NOT NULL
      AND upper(symbol) IN ({asset_symbols})
),
agg AS (
    SELECT
        hour,
        asset,
        sum(amount_usd) AS transfer_usd_sum,
        count(*) AS transfer_count,
        approx_distinct(sender) AS transfer_unique_senders,
        approx_distinct(receiver) AS transfer_unique_receivers,
        approx_distinct(blockchain) AS transfer_chain_count,
        sum(CASE WHEN amount_usd >= 100000 THEN amount_usd ELSE 0 END) AS whale_100k_usd,
        sum(CASE WHEN amount_usd >= 1000000 THEN amount_usd ELSE 0 END) AS whale_1m_usd,
        count_if(amount_usd >= 100000) AS whale_100k_count,
        count_if(amount_usd >= 1000000) AS whale_1m_count,
        approx_percentile(amount_usd, 0.9) AS transfer_p90_usd
    FROM base
    WHERE asset IS NOT NULL
    GROUP BY 1,2
)
SELECT
    hour,
    asset,
    transfer_usd_sum,
    transfer_count,
    transfer_unique_senders,
    transfer_unique_receivers,
    transfer_chain_count,
    whale_100k_usd,
    whale_1m_usd,
    whale_100k_count,
    whale_1m_count,
    transfer_p90_usd,
    ln(1 + whale_100k_usd) AS whale_100k_log,
    ln(1 + whale_1m_usd) AS whale_1m_log,
    CASE WHEN transfer_usd_sum = 0 THEN 0 ELSE CAST(whale_100k_usd AS double) / CAST(transfer_usd_sum AS double) END AS whale_100k_share,
    CASE WHEN transfer_usd_sum = 0 THEN 0 ELSE CAST(whale_1m_usd AS double) / CAST(transfer_usd_sum AS double) END AS whale_1m_share
FROM agg
ORDER BY 1,2
"""


def build_chain_dispersion_sql() -> str:
    asset_symbols = ", ".join(f"'{s}'" for s in _all_symbols(ASSET_SYMBOLS))
    bought_case = _build_case_expr("token_bought_symbol")
    sold_case = _build_case_expr("token_sold_symbol")
    return f"""
WITH base AS (
    SELECT
        date_trunc('hour', block_time) AS hour,
        blockchain,
        amount_usd,
        {bought_case} AS asset_bought,
        {sold_case} AS asset_sold
    FROM dex.trades
    WHERE block_time >= now() - interval '90' day
      AND amount_usd IS NOT NULL
      AND (
          upper(token_bought_symbol) IN ({asset_symbols})
          OR upper(token_sold_symbol) IN ({asset_symbols})
      )
),
oriented AS (
    SELECT hour, blockchain, asset_bought AS asset, amount_usd
    FROM base
    WHERE asset_bought IS NOT NULL

    UNION ALL

    SELECT hour, blockchain, asset_sold AS asset, amount_usd
    FROM base
    WHERE asset_sold IS NOT NULL
),
chain_hour AS (
    SELECT hour, asset, blockchain, sum(amount_usd) AS chain_usd
    FROM oriented
    GROUP BY 1,2,3
),
totals AS (
    SELECT hour, asset, sum(chain_usd) AS total_usd
    FROM chain_hour
    GROUP BY 1,2
),
shares AS (
    SELECT
        c.hour,
        c.asset,
        c.blockchain,
        c.chain_usd,
        t.total_usd,
        CASE WHEN t.total_usd = 0 THEN 0 ELSE CAST(c.chain_usd AS double) / CAST(t.total_usd AS double) END AS chain_share
    FROM chain_hour c
    JOIN totals t
      ON c.hour=t.hour AND c.asset=t.asset
)
SELECT
    hour,
    asset,
    count(*) AS dex_chain_count,
    sum(chain_share * chain_share) AS dex_chain_hhi,
    max(chain_share) AS dex_max_chain_share
FROM shares
GROUP BY 1,2
ORDER BY 1,2
"""


QUERIES = [
    QuerySpec(
        query_id="x7_adv_q1_dex_features",
        hypothesis="Flow, large-flow, breadth and stablecoin conversion structure from DEX prints.",
        sql_builder=build_dex_feature_sql,
    ),
    QuerySpec(
        query_id="x7_adv_q2_transfer_features",
        hypothesis="Whale-intensity and transfer-structure features from token transfer graph.",
        sql_builder=build_transfer_feature_sql,
    ),
    QuerySpec(
        query_id="x7_adv_q3_chain_dispersion",
        hypothesis="Cross-chain concentration/dispersion of DEX activity as a conviction proxy.",
        sql_builder=build_chain_dispersion_sql,
    ),
]


def load_dune_api_key(env_file: Path) -> str:
    text = env_file.read_text()
    m = re.search(r"DUNE_API_KEY=([A-Za-z0-9_-]+)", text)
    if not m:
        raise RuntimeError(f"DUNE_API_KEY not found in {env_file}")
    return m.group(1)


def run_dune_sql(sql: str, api_key: str, max_wait_seconds: int = 900) -> tuple[str, pd.DataFrame]:
    base = "https://api.dune.com/api/v1"
    headers = {"X-Dune-Api-Key": api_key, "Content-Type": "application/json"}

    r = requests.post(
        f"{base}/sql/execute",
        headers=headers,
        json={"sql": sql, "performance": "medium"},
        timeout=45,
    )
    r.raise_for_status()
    execution_id = r.json()["execution_id"]

    deadline = time.time() + max_wait_seconds
    state = None
    payload = {}
    while time.time() < deadline:
        s = requests.get(f"{base}/execution/{execution_id}/status", headers=headers, timeout=45)
        s.raise_for_status()
        payload = s.json()
        state = payload.get("state")
        if state in {"QUERY_STATE_COMPLETED", "QUERY_STATE_FAILED", "QUERY_STATE_CANCELLED", "QUERY_STATE_EXPIRED"}:
            break
        time.sleep(2)

    if state != "QUERY_STATE_COMPLETED":
        raise RuntimeError(f"Execution {execution_id} failed state={state} payload={json.dumps(payload)[:1000]}")

    res = requests.get(f"{base}/execution/{execution_id}/results", headers=headers, timeout=60)
    res.raise_for_status()
    data = res.json()
    df = pd.DataFrame(data.get("result", {}).get("rows", []))
    return execution_id, df


def load_price_panel(candles_dir: Path) -> pd.DataFrame:
    frames = []
    for asset in ASSETS:
        path = candles_dir / f"bybit_perpetual|{asset}-USDT|1h.parquet"
        df = pd.read_parquet(path)[["timestamp", "close"]].copy()
        df["hour"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.floor("h")
        df = df.sort_values("hour").drop_duplicates("hour")
        df["asset"] = asset
        for h in HORIZONS:
            df[f"fwd_ret_{h}h"] = df["close"].shift(-h) / df["close"] - 1.0
        # 24h realized vol regime proxy
        df["ret_1h"] = np.log(df["close"] / df["close"].shift(1))
        df["realized_vol_24h"] = df["ret_1h"].rolling(24).std()
        frames.append(df[["hour", "asset", "realized_vol_24h"] + [f"fwd_ret_{h}h" for h in HORIZONS]])
    out = pd.concat(frames, ignore_index=True)
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=90) - pd.Timedelta(hours=max(HORIZONS))
    return out[out["hour"] >= cutoff].copy()


def prep_query_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["hour"] = pd.to_datetime(out["hour"], utc=True)
    out["asset"] = out["asset"].astype(str).str.upper()
    out = out[out["asset"].isin(ASSETS)].sort_values(["asset", "hour"])
    for c in out.columns:
        if c not in {"hour", "asset"}:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def add_feature_engineering(panel: pd.DataFrame) -> pd.DataFrame:
    df = panel.sort_values(["asset", "hour"]).copy()
    feature_cols = [c for c in df.columns if c not in {"hour", "asset"} and not c.startswith("fwd_ret_")]
    for c in feature_cols:
        # short and long z-score dynamics
        g = df.groupby("asset")[c]
        mean24 = g.transform(lambda x: x.rolling(24, min_periods=8).mean())
        std24 = g.transform(lambda x: x.rolling(24, min_periods=8).std())
        mean168 = g.transform(lambda x: x.rolling(168, min_periods=24).mean())
        std168 = g.transform(lambda x: x.rolling(168, min_periods=24).std())
        df[f"{c}_z24"] = (df[c] - mean24) / std24.replace(0, np.nan)
        df[f"{c}_z168"] = (df[c] - mean168) / std168.replace(0, np.nan)
        df[f"{c}_d1"] = g.diff(1)
    # volatility interaction with core flow signal if present
    if "dex_flow_imbalance" in df.columns and "realized_vol_24h" in df.columns:
        df["dex_flow_x_vol"] = df["dex_flow_imbalance"] * df["realized_vol_24h"].fillna(0)
    return df


def apply_feature_lag(df: pd.DataFrame, lag_hours: int = 1) -> pd.DataFrame:
    if lag_hours <= 0:
        return df
    out = df.sort_values(["asset", "hour"]).copy()
    lag_cols = [
        c
        for c in out.columns
        if c not in {"hour", "asset", "fwd_ret_1h", "fwd_ret_4h", "fwd_ret_24h"}
    ]
    for c in lag_cols:
        out[c] = out.groupby("asset")[c].shift(lag_hours)
    return out


def univariate_screen(df: pd.DataFrame) -> pd.DataFrame:
    feature_cols = [
        c
        for c in df.columns
        if c not in {"hour", "asset"} and not c.startswith("fwd_ret_")
    ]
    rows = []
    for feat in feature_cols:
        for h in HORIZONS:
            ycol = f"fwd_ret_{h}h"
            sub = df[[feat, ycol]].dropna()
            n = len(sub)
            if n >= 20 and sub[feat].nunique() > 1:
                rho, p = spearmanr(sub[feat], sub[ycol], nan_policy="omit")
            else:
                rho, p = np.nan, np.nan
            rows.append(
                {
                    "feature": feat,
                    "horizon": f"{h}h",
                    "n": n,
                    "rho": rho,
                    "p_value": p,
                    "significant": bool((n > 100) and (pd.notna(p)) and (p < 0.05)),
                }
            )
    out = pd.DataFrame(rows).sort_values(["horizon", "p_value"], ascending=[True, True])
    return out


def _select_feature_columns(df: pd.DataFrame) -> list[str]:
    cols = [
        c
        for c in df.columns
        if c not in {"hour", "asset", "fwd_ret_1h", "fwd_ret_4h", "fwd_ret_24h"}
    ]
    # drop obviously sparse raw columns if too many NaNs
    keep = []
    for c in cols:
        if df[c].notna().mean() >= 0.35:
            keep.append(c)
    return keep


def walk_forward_model(df: pd.DataFrame, target_col: str = "fwd_ret_1h") -> tuple[pd.DataFrame, dict[str, float]]:
    data = df.sort_values("hour").copy()
    feat_cols = _select_feature_columns(data)
    data[feat_cols] = data[feat_cols].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    data = data.dropna(subset=[target_col]).copy()

    hours = sorted(data["hour"].unique())
    min_train_hours = 24 * 35  # 35 days
    retrain_every = 24  # daily retraining

    preds = []
    scaler = StandardScaler()
    ridge = Ridge(alpha=2.0, random_state=42)
    xgb = XGBRegressor(
        n_estimators=220,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.9,
        colsample_bytree=0.8,
        reg_alpha=0.0,
        reg_lambda=2.0,
        objective="reg:squarederror",
        random_state=42,
        n_jobs=4,
    )

    last_fit_idx = -10**9
    trained = False

    for i, h in enumerate(hours):
        if i < min_train_hours:
            continue
        train_hours = hours[:i]
        test_hours = [h]

        if (i - last_fit_idx) >= retrain_every or not trained:
            train_df = data[data["hour"].isin(train_hours)]
            X_train = train_df[feat_cols].values
            y_train = train_df[target_col].values

            Xs = scaler.fit_transform(X_train)
            ridge.fit(Xs, y_train)
            xgb.fit(X_train, y_train)
            trained = True
            last_fit_idx = i

        test_df = data[data["hour"].isin(test_hours)].copy()
        if test_df.empty:
            continue
        X_test = test_df[feat_cols].values
        pred_ridge = ridge.predict(scaler.transform(X_test))
        pred_xgb = xgb.predict(X_test)
        test_df["pred"] = 0.35 * pred_ridge + 0.65 * pred_xgb
        preds.append(test_df[["hour", "asset", target_col, "pred"]])

    pred_df = pd.concat(preds, ignore_index=True).rename(columns={target_col: "realized_ret"})
    pred_df = pred_df.sort_values(["hour", "pred"], ascending=[True, False]).copy()

    # strategy: each hour long top2, short bottom2 equally weighted
    rows = []
    for h, g in pred_df.groupby("hour"):
        g = g.sort_values("pred", ascending=False)
        top = g.head(2)
        bot = g.tail(2)
        weights = {a: 0.0 for a in ASSETS}
        for a in top["asset"]:
            weights[a] += 0.5
        for a in bot["asset"]:
            weights[a] -= 0.5

        gross = 0.0
        for _, r in g.iterrows():
            gross += weights[r["asset"]] * r["realized_ret"]
        rows.append({"hour": h, "gross_ret": gross, **{f"w_{a}": weights[a] for a in ASSETS}})

    strat = pd.DataFrame(rows).sort_values("hour").reset_index(drop=True)

    # turnover and costs
    wcols = [f"w_{a}" for a in ASSETS]
    turnover = strat[wcols].diff().abs().sum(axis=1).fillna(0.0)
    # 6 bps one-way all-in per unit notional turnover
    cost_per_turn = 0.0006
    strat["turnover"] = turnover
    strat["cost"] = turnover * cost_per_turn
    strat["net_ret"] = strat["gross_ret"] - strat["cost"]
    strat["cum_net_ret"] = (1.0 + strat["net_ret"]).cumprod() - 1.0

    ret = strat["net_ret"].dropna()
    ann_factor = np.sqrt(24 * 365)
    sharpe = (ret.mean() / ret.std() * ann_factor) if ret.std() > 0 else np.nan
    hit_rate = float((ret > 0).mean()) if len(ret) else np.nan
    avg_turnover = float(strat["turnover"].mean()) if len(strat) else np.nan
    total_return = float(strat["cum_net_ret"].iloc[-1]) if len(strat) else np.nan

    # IC on panel predictions
    ics = []
    for _, g in pred_df.groupby("hour"):
        if g["pred"].nunique() > 1 and g["realized_ret"].nunique() > 1:
            ic, _ = spearmanr(g["pred"], g["realized_ret"], nan_policy="omit")
            ics.append(ic)
    ic_mean = float(np.nanmean(ics)) if ics else np.nan
    ic_std = float(np.nanstd(ics, ddof=1)) if len(ics) > 1 else np.nan
    ic_tstat = float(ic_mean / (ic_std / np.sqrt(len(ics)))) if len(ics) > 1 and ic_std > 0 else np.nan

    metrics = {
        "rows_scored": int(len(pred_df)),
        "hours_scored": int(pred_df["hour"].nunique()),
        "feature_count": int(len(feat_cols)),
        "ic_mean": ic_mean,
        "ic_tstat": ic_tstat,
        "net_hourly_sharpe_ann": float(sharpe) if pd.notna(sharpe) else np.nan,
        "hit_rate": hit_rate,
        "avg_turnover": avg_turnover,
        "total_net_return": total_return,
    }
    return strat, metrics


def main() -> None:
    p = argparse.ArgumentParser(description="Run advanced X7 research pipeline.")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--candles-dir", default="app/data/cache/candles")
    p.add_argument("--out-dir", default="app/data/cache/onchain_x7_advanced")
    p.add_argument("--lag-hours", type=int, default=1, help="Lag all predictive features by this many hours.")
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    api_key = load_dune_api_key(Path(args.env_file))
    price_df = load_price_panel(Path(args.candles_dir))

    metadata = []
    feature_frames = []
    for spec in QUERIES:
        sql = spec.sql_builder().strip()
        sql_path = out_dir / f"{run_ts}_{spec.query_id}.sql"
        sql_path.write_text(sql + "\n")

        ex_id, raw = run_dune_sql(sql, api_key)
        raw_path = out_dir / f"{run_ts}_{spec.query_id}_raw.csv"
        raw.to_csv(raw_path, index=False)
        clean = prep_query_frame(raw)
        feature_frames.append(clean)

        metadata.append(
            {
                "query_id": spec.query_id,
                "hypothesis": spec.hypothesis,
                "execution_id": ex_id,
                "raw_rows": int(len(raw)),
                "sql_file": str(sql_path),
                "raw_csv_file": str(raw_path),
            }
        )
        print(f"[{spec.query_id}] execution_id={ex_id} rows={len(raw)}")

    # outer-join all feature panels
    panel = price_df.copy()
    for f in feature_frames:
        panel = panel.merge(f, on=["hour", "asset"], how="left")

    panel = add_feature_engineering(panel)
    panel = apply_feature_lag(panel, lag_hours=args.lag_hours)
    panel_path = out_dir / f"{run_ts}_feature_panel.csv"
    panel.to_csv(panel_path, index=False)

    ic = univariate_screen(panel)
    ic_path = out_dir / f"{run_ts}_univariate_ic.csv"
    ic.to_csv(ic_path, index=False)

    strat, metrics = walk_forward_model(panel, target_col="fwd_ret_1h")
    strat_path = out_dir / f"{run_ts}_walkforward_strategy.csv"
    metrics_path = out_dir / f"{run_ts}_model_metrics.json"
    meta_path = out_dir / f"{run_ts}_metadata.json"

    strat.to_csv(strat_path, index=False)
    metrics_path.write_text(json.dumps(metrics, indent=2))
    meta_path.write_text(json.dumps(metadata, indent=2))

    sig = ic[(ic["significant"])].copy().sort_values(["horizon", "p_value"]).head(25)
    print("\nTop significant univariate features:")
    print(sig.to_string(index=False) if not sig.empty else "None")

    print("\nWalk-forward model metrics:")
    print(json.dumps(metrics, indent=2))

    print("\nArtifacts:")
    print(f"- {meta_path}")
    print(f"- {panel_path}")
    print(f"- {ic_path}")
    print(f"- {strat_path}")
    print(f"- {metrics_path}")


if __name__ == "__main__":
    main()
