#!/usr/bin/env python3
"""
X7 on-chain signal scan for Bybit directional perp trading.

Runs three Dune SQL hypotheses over the last 90 days (hourly, BTC/ETH/SOL/XRP/DOGE),
aligns against Bybit perp hourly prices from local parquet, and computes
Spearman rank correlations vs 1h/4h/24h forward returns.
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


ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE"]
HORIZONS = [1, 4, 24]

# Symbol aliases used in Dune tables.
ASSET_SYMBOLS = {
    "BTC": ["BTC", "WBTC", "TBTC", "BTCB"],
    "ETH": ["ETH", "WETH"],
    "SOL": ["SOL", "WSOL"],
    "XRP": ["XRP", "WXRP"],
    "DOGE": ["DOGE", "WDOGE"],
}


@dataclass
class SignalSpec:
    signal_id: str
    hypothesis: str
    source_table: str
    sql_builder: Callable[[], str]
    metric_column: str


def _build_case_expr(column: str, mapping: dict[str, list[str]]) -> str:
    lines = ["CASE"]
    for asset, symbols in mapping.items():
        symbol_list = ", ".join(f"'{s.upper()}'" for s in symbols)
        lines.append(f"    WHEN upper({column}) IN ({symbol_list}) THEN '{asset}'")
    lines.append("    ELSE NULL")
    lines.append("END")
    return "\n".join(lines)


def _all_symbols(mapping: dict[str, list[str]]) -> list[str]:
    out: set[str] = set()
    for symbols in mapping.values():
        out.update(s.upper() for s in symbols)
    return sorted(out)


def build_flow_imbalance_sql() -> str:
    symbols = ", ".join(f"'{s}'" for s in _all_symbols(ASSET_SYMBOLS))
    bought_case = _build_case_expr("token_bought_symbol", ASSET_SYMBOLS)
    sold_case = _build_case_expr("token_sold_symbol", ASSET_SYMBOLS)
    return f"""
WITH raw AS (
    SELECT
        date_trunc('hour', block_time) AS hour,
        {bought_case} AS asset_bought,
        {sold_case} AS asset_sold,
        amount_usd
    FROM dex.trades
    WHERE block_time >= now() - interval '90' day
      AND amount_usd IS NOT NULL
      AND (
          upper(token_bought_symbol) IN ({symbols})
          OR upper(token_sold_symbol) IN ({symbols})
      )
),
buy_side AS (
    SELECT
        hour,
        asset_bought AS asset,
        sum(amount_usd) AS buy_usd
    FROM raw
    WHERE asset_bought IS NOT NULL
    GROUP BY 1, 2
),
sell_side AS (
    SELECT
        hour,
        asset_sold AS asset,
        sum(amount_usd) AS sell_usd
    FROM raw
    WHERE asset_sold IS NOT NULL
    GROUP BY 1, 2
)
SELECT
    coalesce(b.hour, s.hour) AS hour,
    coalesce(b.asset, s.asset) AS asset,
    coalesce(b.buy_usd, 0) AS buy_usd,
    coalesce(s.sell_usd, 0) AS sell_usd,
    CASE
        WHEN coalesce(b.buy_usd, 0) + coalesce(s.sell_usd, 0) = 0 THEN 0
        ELSE (
            coalesce(b.buy_usd, 0) - coalesce(s.sell_usd, 0)
        ) / (
            coalesce(b.buy_usd, 0) + coalesce(s.sell_usd, 0)
        )
    END AS metric_dex_flow_imbalance
FROM buy_side b
FULL OUTER JOIN sell_side s
    ON b.hour = s.hour AND b.asset = s.asset
WHERE coalesce(b.asset, s.asset) IS NOT NULL
ORDER BY 1, 2
"""


def build_breadth_imbalance_sql() -> str:
    symbols = ", ".join(f"'{s}'" for s in _all_symbols(ASSET_SYMBOLS))
    bought_case = _build_case_expr("token_bought_symbol", ASSET_SYMBOLS)
    sold_case = _build_case_expr("token_sold_symbol", ASSET_SYMBOLS)
    return f"""
WITH raw AS (
    SELECT
        date_trunc('hour', block_time) AS hour,
        {bought_case} AS asset_bought,
        {sold_case} AS asset_sold,
        tx_from
    FROM dex.trades
    WHERE block_time >= now() - interval '90' day
      AND (
          upper(token_bought_symbol) IN ({symbols})
          OR upper(token_sold_symbol) IN ({symbols})
      )
),
buy_side AS (
    SELECT
        hour,
        asset_bought AS asset,
        approx_distinct(tx_from) AS buy_trader_count
    FROM raw
    WHERE asset_bought IS NOT NULL
    GROUP BY 1, 2
),
sell_side AS (
    SELECT
        hour,
        asset_sold AS asset,
        approx_distinct(tx_from) AS sell_trader_count
    FROM raw
    WHERE asset_sold IS NOT NULL
    GROUP BY 1, 2
)
SELECT
    coalesce(b.hour, s.hour) AS hour,
    coalesce(b.asset, s.asset) AS asset,
    coalesce(b.buy_trader_count, 0) AS buy_trader_count,
    coalesce(s.sell_trader_count, 0) AS sell_trader_count,
    CASE
        WHEN coalesce(b.buy_trader_count, 0) + coalesce(s.sell_trader_count, 0) = 0 THEN 0
        ELSE cast(
            coalesce(b.buy_trader_count, 0) - coalesce(s.sell_trader_count, 0)
            AS double
        ) / cast(
            coalesce(b.buy_trader_count, 0) + coalesce(s.sell_trader_count, 0)
            AS double
        )
    END AS metric_dex_breadth_imbalance
FROM buy_side b
FULL OUTER JOIN sell_side s
    ON b.hour = s.hour AND b.asset = s.asset
WHERE coalesce(b.asset, s.asset) IS NOT NULL
ORDER BY 1, 2
"""


def build_whale_transfer_sql() -> str:
    symbols = ", ".join(f"'{s}'" for s in _all_symbols(ASSET_SYMBOLS))
    symbol_case = _build_case_expr("symbol", ASSET_SYMBOLS)
    return f"""
WITH raw AS (
    SELECT
        date_trunc('hour', block_time) AS hour,
        {symbol_case} AS asset,
        amount_usd
    FROM tokens.transfers
    WHERE block_time >= now() - interval '90' day
      AND amount_usd IS NOT NULL
      AND amount_usd >= 100000
      AND upper(symbol) IN ({symbols})
)
SELECT
    hour,
    asset,
    count(*) AS whale_transfer_count,
    sum(amount_usd) AS whale_transfer_usd,
    avg(amount_usd) AS whale_avg_transfer_usd,
    ln(1 + sum(amount_usd)) AS metric_whale_transfer_intensity
FROM raw
WHERE asset IS NOT NULL
GROUP BY 1, 2
ORDER BY 1, 2
"""


SIGNALS: list[SignalSpec] = [
    SignalSpec(
        signal_id="x7_h1_dex_flow_imbalance",
        hypothesis=(
            "DEX net aggressor flow imbalance (buy USD - sell USD) predicts short-horizon "
            "directional perp returns."
        ),
        source_table="dex.trades",
        sql_builder=build_flow_imbalance_sql,
        metric_column="metric_dex_flow_imbalance",
    ),
    SignalSpec(
        signal_id="x7_h2_dex_breadth_imbalance",
        hypothesis=(
            "DEX trader breadth imbalance (unique buyer traders - unique seller traders) "
            "predicts directional continuation/mean-reversion."
        ),
        source_table="dex.trades",
        sql_builder=build_breadth_imbalance_sql,
        metric_column="metric_dex_breadth_imbalance",
    ),
    SignalSpec(
        signal_id="x7_h3_whale_transfer_intensity",
        hypothesis=(
            "Whale transfer intensity (>= $100k transfer USD, log-scaled) captures latent "
            "positioning pressure that leads perp returns."
        ),
        source_table="tokens.transfers",
        sql_builder=build_whale_transfer_sql,
        metric_column="metric_whale_transfer_intensity",
    ),
]


def load_dune_api_key(env_file: Path) -> str:
    text = env_file.read_text()
    match = re.search(r"DUNE_API_KEY=([A-Za-z0-9_-]+)", text)
    if not match:
        raise RuntimeError(f"DUNE_API_KEY not found in {env_file}")
    return match.group(1)


def run_dune_sql(sql: str, api_key: str, poll_seconds: int = 2, max_wait_seconds: int = 900) -> tuple[str, pd.DataFrame]:
    base = "https://api.dune.com/api/v1"
    headers = {"X-Dune-Api-Key": api_key, "Content-Type": "application/json"}

    execute = requests.post(
        f"{base}/sql/execute",
        headers=headers,
        json={"sql": sql, "performance": "medium"},
        timeout=45,
    )
    execute.raise_for_status()
    execution_id = execute.json()["execution_id"]

    deadline = time.time() + max_wait_seconds
    final_state = None
    status_payload: dict[str, object] = {}
    while time.time() < deadline:
        status_resp = requests.get(f"{base}/execution/{execution_id}/status", headers=headers, timeout=45)
        status_resp.raise_for_status()
        status_payload = status_resp.json()
        state = status_payload.get("state")
        if state in {
            "QUERY_STATE_COMPLETED",
            "QUERY_STATE_FAILED",
            "QUERY_STATE_CANCELLED",
            "QUERY_STATE_EXPIRED",
        }:
            final_state = state
            break
        time.sleep(poll_seconds)

    if final_state != "QUERY_STATE_COMPLETED":
        raise RuntimeError(
            f"Dune execution {execution_id} ended with state={final_state}. "
            f"Payload={json.dumps(status_payload)[:1000]}"
        )

    results_resp = requests.get(f"{base}/execution/{execution_id}/results", headers=headers, timeout=60)
    results_resp.raise_for_status()
    payload = results_resp.json()
    rows = payload.get("result", {}).get("rows", [])
    return execution_id, pd.DataFrame(rows)


def load_hourly_prices(candles_dir: Path) -> pd.DataFrame:
    frames = []
    for asset in ASSETS:
        path = candles_dir / f"bybit_perpetual|{asset}-USDT|1h.parquet"
        if not path.exists():
            raise FileNotFoundError(f"Missing candle file: {path}")
        df = pd.read_parquet(path)[["timestamp", "close"]].copy()
        df["hour"] = pd.to_datetime(df["timestamp"], unit="s", utc=True).dt.floor("h")
        df = df.sort_values("hour").drop_duplicates("hour")
        df["asset"] = asset
        for h in HORIZONS:
            df[f"fwd_ret_{h}h"] = df["close"].shift(-h) / df["close"] - 1.0
        frames.append(df[["hour", "asset"] + [f"fwd_ret_{h}h" for h in HORIZONS]])
    out = pd.concat(frames, ignore_index=True)
    return out[out["hour"] >= (pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=90) - pd.Timedelta(hours=max(HORIZONS)))]


def prepare_signal_df(raw: pd.DataFrame, metric_col: str) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["hour", "asset", "metric"])
    df = raw.copy()
    df["hour"] = pd.to_datetime(df["hour"], utc=True)
    df["asset"] = df["asset"].astype(str).str.upper()
    df = df[df["asset"].isin(ASSETS)]
    df = df.sort_values(["asset", "hour"])
    df = df[["hour", "asset", metric_col]].rename(columns={metric_col: "metric"})
    df["metric"] = pd.to_numeric(df["metric"], errors="coerce")
    return df.dropna(subset=["metric"])


def evaluate_signal(signal_df: pd.DataFrame, price_df: pd.DataFrame, signal_id: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    merged = price_df.merge(signal_df, on=["hour", "asset"], how="left")
    merged["metric"] = merged["metric"].fillna(0.0)

    pooled_rows = []
    by_asset_rows = []

    for h in HORIZONS:
        ret_col = f"fwd_ret_{h}h"
        valid = merged[["metric", ret_col, "asset"]].dropna()
        n = len(valid)
        if n >= 2:
            rho, pval = spearmanr(valid["metric"], valid[ret_col], nan_policy="omit")
        else:
            rho, pval = np.nan, np.nan
        pooled_rows.append(
            {
                "signal_id": signal_id,
                "horizon": f"{h}h",
                "scope": "pooled",
                "asset": "ALL",
                "n": n,
                "rho": rho,
                "p_value": pval,
                "significant": bool((n > 100) and (pval < 0.05)) if pd.notna(pval) else False,
            }
        )

        for asset in ASSETS:
            sub = valid[valid["asset"] == asset]
            n_a = len(sub)
            if n_a >= 2:
                rho_a, pval_a = spearmanr(sub["metric"], sub[ret_col], nan_policy="omit")
            else:
                rho_a, pval_a = np.nan, np.nan
            by_asset_rows.append(
                {
                    "signal_id": signal_id,
                    "horizon": f"{h}h",
                    "scope": "asset",
                    "asset": asset,
                    "n": n_a,
                    "rho": rho_a,
                    "p_value": pval_a,
                    "significant": bool((n_a > 100) and (pval_a < 0.05)) if pd.notna(pval_a) else False,
                }
            )

    return pd.DataFrame(pooled_rows), pd.DataFrame(by_asset_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run X7 on-chain signal hypotheses against forward returns.")
    parser.add_argument("--env-file", default=".env", help="Path to .env with DUNE_API_KEY.")
    parser.add_argument(
        "--candles-dir",
        default="app/data/cache/candles",
        help="Path to candle parquet cache.",
    )
    parser.add_argument(
        "--out-dir",
        default="app/data/cache/onchain_x7",
        help="Directory for SQL/results artifacts.",
    )
    args = parser.parse_args()

    env_file = Path(args.env_file)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    api_key = load_dune_api_key(env_file)
    price_df = load_hourly_prices(Path(args.candles_dir))

    all_pooled = []
    all_asset = []
    metadata = []

    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    for spec in SIGNALS:
        sql = spec.sql_builder().strip()
        sql_path = out_dir / f"{run_ts}_{spec.signal_id}.sql"
        sql_path.write_text(sql + "\n")

        execution_id, raw_df = run_dune_sql(sql, api_key)
        raw_csv_path = out_dir / f"{run_ts}_{spec.signal_id}_raw.csv"
        raw_df.to_csv(raw_csv_path, index=False)

        signal_df = prepare_signal_df(raw_df, spec.metric_column)
        pooled_df, asset_df = evaluate_signal(signal_df, price_df, spec.signal_id)
        all_pooled.append(pooled_df)
        all_asset.append(asset_df)

        metadata.append(
            {
                "signal_id": spec.signal_id,
                "hypothesis": spec.hypothesis,
                "source_table": spec.source_table,
                "metric_column": spec.metric_column,
                "execution_id": execution_id,
                "raw_rows": int(len(raw_df)),
                "aligned_rows": int(len(signal_df)),
                "sql_file": str(sql_path),
                "raw_csv_file": str(raw_csv_path),
            }
        )

        print(f"[{spec.signal_id}] execution_id={execution_id} raw_rows={len(raw_df)} aligned_rows={len(signal_df)}")

    pooled = pd.concat(all_pooled, ignore_index=True)
    by_asset = pd.concat(all_asset, ignore_index=True)

    pooled_path = out_dir / f"{run_ts}_pooled_correlations.csv"
    by_asset_path = out_dir / f"{run_ts}_asset_correlations.csv"
    meta_path = out_dir / f"{run_ts}_metadata.json"

    pooled.to_csv(pooled_path, index=False)
    by_asset.to_csv(by_asset_path, index=False)
    meta_path.write_text(json.dumps(metadata, indent=2))

    significant = pooled[(pooled["significant"])].sort_values(["horizon", "p_value", "rho"], ascending=[True, True, False])
    if significant.empty:
        print("\nNo pooled signal/horizon pair met p < 0.05 and N > 100.")
    else:
        print("\nSignificant pooled results (p < 0.05, N > 100):")
        print(significant.to_string(index=False))

    print("\nArtifacts:")
    print(f"- {pooled_path}")
    print(f"- {by_asset_path}")
    print(f"- {meta_path}")


if __name__ == "__main__":
    main()
