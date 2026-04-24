#!/usr/bin/env python3
"""
X7 structural (non-flow-first) on-chain research pipeline.

Purpose:
- Focus on structural supply/demand state transitions rather than simple net flow.
- Pull 90-day hourly structural signals from Dune.
- Align with Bybit perp hourly returns from local parquet candles.
- Run robust EDA: lagged IC matrix, per-asset IC, BH-FDR, monotonic quintiles.
- Run walk-forward directional portfolio on a composite structural score.

Core structural hypotheses:
1) Supply elasticity shocks (mint/burn/sink) alter effective float.
2) Holder topology concentration (HHI/top-holder share) shifts market fragility.
3) Dormant supply reactivation + CEX float transfer changes latent sell pressure.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import requests
from scipy.stats import spearmanr, ttest_1samp


ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE"]
HORIZONS = [1, 4, 24]
LAGS = [1, 2, 4]

ASSET_SYMBOLS = {
    "BTC": ["BTC", "WBTC", "TBTC", "BTCB"],
    "ETH": ["ETH", "WETH"],
    "SOL": ["SOL", "WSOL"],
    "XRP": ["XRP", "WXRP"],
    "DOGE": ["DOGE", "WDOGE"],
}

ZERO_ADDR = "0x0000000000000000000000000000000000000000"
DEAD_ADDRS = [
    ZERO_ADDR,
    "0x000000000000000000000000000000000000dead",
    "0xdead000000000000000042069420694206942069",
    "0x000000000000000000000000000000000000dEaD".lower(),
]


@dataclass
class QuerySpec:
    query_id: str
    hypothesis: str
    sql_builder: Callable[[], str]


def _all_symbols() -> list[str]:
    out: set[str] = set()
    for vals in ASSET_SYMBOLS.values():
        out.update(v.upper() for v in vals)
    return sorted(out)


def _build_asset_case(column: str) -> str:
    lines = ["CASE"]
    for asset, symbols in ASSET_SYMBOLS.items():
        symbols_sql = ", ".join(f"'{s.upper()}'" for s in symbols)
        lines.append(f"    WHEN upper({column}) IN ({symbols_sql}) THEN '{asset}'")
    lines.append("    ELSE NULL")
    lines.append("END")
    return "\n".join(lines)


def build_supply_elasticity_sql() -> str:
    symbols = ", ".join(f"'{s}'" for s in _all_symbols())
    asset_case = _build_asset_case("symbol")
    dead_list = ", ".join(f"'{a.lower()}'" for a in DEAD_ADDRS)
    return f"""
WITH base AS (
    SELECT
        date_trunc('hour', block_time) AS hour,
        {asset_case} AS asset,
        amount_usd,
        lower(cast("from" AS varchar)) AS sender,
        lower(cast("to" AS varchar)) AS receiver
    FROM tokens.transfers
    WHERE block_time >= now() - interval '90' day
      AND amount_usd IS NOT NULL
      AND upper(symbol) IN ({symbols})
),
agg AS (
    SELECT
        hour,
        asset,
        sum(amount_usd) AS transfer_usd,
        count(*) AS transfer_count,
        sum(CASE WHEN sender = '{ZERO_ADDR.lower()}' THEN amount_usd ELSE 0 END) AS minted_usd,
        sum(CASE WHEN receiver IN ({dead_list}) THEN amount_usd ELSE 0 END) AS burned_or_sunk_usd,
        sum(CASE WHEN receiver = '{ZERO_ADDR.lower()}' THEN amount_usd ELSE 0 END) AS burned_to_zero_usd,
        sum(CASE WHEN receiver = '{'0x000000000000000000000000000000000000dead'}' THEN amount_usd ELSE 0 END) AS burned_to_dead_usd,
        count_if(sender = '{ZERO_ADDR.lower()}') AS mint_txn_count,
        count_if(receiver IN ({dead_list})) AS burn_or_sink_txn_count
    FROM base
    WHERE asset IS NOT NULL
    GROUP BY 1,2
)
SELECT
    hour,
    asset,
    transfer_usd,
    transfer_count,
    minted_usd,
    burned_or_sunk_usd,
    burned_to_zero_usd,
    burned_to_dead_usd,
    mint_txn_count,
    burn_or_sink_txn_count,
    minted_usd - burned_or_sunk_usd AS net_issuance_usd,
    CASE
        WHEN transfer_usd = 0 THEN 0
        ELSE CAST(minted_usd - burned_or_sunk_usd AS double) / CAST(transfer_usd AS double)
    END AS net_issuance_ratio,
    CASE
        WHEN minted_usd + burned_or_sunk_usd = 0 THEN 0
        ELSE CAST(burned_or_sunk_usd AS double) / CAST(minted_usd + burned_or_sunk_usd AS double)
    END AS sink_vs_issuance_share,
    ln(1 + minted_usd) - ln(1 + burned_or_sunk_usd) AS issuance_log_imbalance
FROM agg
ORDER BY 1,2
"""


def build_holder_topology_sql() -> str:
    symbols = ", ".join(f"'{s}'" for s in _all_symbols())
    asset_case = _build_asset_case("symbol")
    return f"""
WITH base AS (
    SELECT
        date_trunc('hour', block_time) AS hour,
        {asset_case} AS asset,
        amount_usd,
        lower(cast("from" AS varchar)) AS sender,
        lower(cast("to" AS varchar)) AS receiver
    FROM tokens.transfers
    WHERE block_time >= now() - interval '90' day
      AND amount_usd IS NOT NULL
      AND upper(symbol) IN ({symbols})
),
participant_side AS (
    SELECT hour, asset, sender AS address, sum(amount_usd) AS usd
    FROM base
    WHERE asset IS NOT NULL AND sender IS NOT NULL
    GROUP BY 1,2,3

    UNION ALL

    SELECT hour, asset, receiver AS address, sum(amount_usd) AS usd
    FROM base
    WHERE asset IS NOT NULL AND receiver IS NOT NULL
    GROUP BY 1,2,3
),
participant AS (
    SELECT hour, asset, address, sum(usd) AS participant_usd
    FROM participant_side
    GROUP BY 1,2,3
),
ranked AS (
    SELECT
        hour,
        asset,
        address,
        participant_usd,
        sum(participant_usd) OVER (PARTITION BY hour, asset) AS total_usd,
        row_number() OVER (PARTITION BY hour, asset ORDER BY participant_usd DESC) AS rn
    FROM participant
)
SELECT
    hour,
    asset,
    count(*) AS participant_count,
    max(total_usd) AS participant_total_usd,
    CASE
        WHEN max(total_usd) = 0 THEN 0
        ELSE sum(power(CAST(participant_usd AS double) / CAST(total_usd AS double), 2))
    END AS participant_hhi,
    CASE
        WHEN max(total_usd) = 0 THEN 0
        ELSE CAST(max(CASE WHEN rn = 1 THEN participant_usd ELSE 0 END) AS double) / CAST(max(total_usd) AS double)
    END AS top1_participant_share,
    CASE
        WHEN max(total_usd) = 0 THEN 0
        ELSE CAST(sum(CASE WHEN rn <= 5 THEN participant_usd ELSE 0 END) AS double) / CAST(max(total_usd) AS double)
    END AS top5_participant_share,
    CASE
        WHEN max(total_usd) = 0 THEN 0
        ELSE CAST(sum(CASE WHEN rn <= 10 THEN participant_usd ELSE 0 END) AS double) / CAST(max(total_usd) AS double)
    END AS top10_participant_share
FROM ranked
GROUP BY 1,2
ORDER BY 1,2
"""


def build_dormancy_cex_sql() -> str:
    symbols = ", ".join(f"'{s}'" for s in _all_symbols())
    asset_case = _build_asset_case("symbol")
    # NOTE: Original sender-dormancy window query timed out at 30 minutes on free tier.
    # This lighter variant keeps the structural CEX-float and sender/receiver topology
    # while remaining executable in practical runtime/credit budgets.
    return f"""
WITH base AS (
    SELECT
        date_trunc('hour', block_time) AS hour,
        {asset_case} AS asset,
        amount_usd,
        lower(cast("from" AS varchar)) AS sender,
        lower(cast("to" AS varchar)) AS receiver
    FROM tokens.transfers
    WHERE block_time >= now() - interval '90' day
      AND amount_usd IS NOT NULL
      AND upper(symbol) IN ({symbols})
),
cex_labels AS (
    SELECT DISTINCT lower(cast(address AS varchar)) AS cex_address
    FROM labels.cex
),
agg AS (
    SELECT
        b.hour,
        b.asset,
        sum(b.amount_usd) AS transfer_usd,
        approx_distinct(b.sender) AS unique_senders,
        approx_distinct(b.receiver) AS unique_receivers,
        sum(CASE WHEN tc.cex_address IS NOT NULL AND fc.cex_address IS NULL THEN b.amount_usd ELSE 0 END) AS cex_inflow_usd,
        sum(CASE WHEN fc.cex_address IS NOT NULL AND tc.cex_address IS NULL THEN b.amount_usd ELSE 0 END) AS cex_outflow_usd,
        sum(CASE WHEN tc.cex_address IS NOT NULL OR fc.cex_address IS NOT NULL THEN b.amount_usd ELSE 0 END) AS cex_touched_usd
    FROM base b
    LEFT JOIN cex_labels fc
        ON b.sender = fc.cex_address
    LEFT JOIN cex_labels tc
        ON b.receiver = tc.cex_address
    WHERE b.asset IS NOT NULL
    GROUP BY 1,2
)
SELECT
    hour,
    asset,
    transfer_usd,
    unique_senders,
    unique_receivers,
    cex_inflow_usd,
    cex_outflow_usd,
    cex_touched_usd,
    cex_inflow_usd - cex_outflow_usd AS cex_net_flow_usd,
    CASE
        WHEN transfer_usd = 0 THEN 0
        ELSE CAST(cex_inflow_usd - cex_outflow_usd AS double) / CAST(transfer_usd AS double)
    END AS cex_net_flow_ratio,
    CASE
        WHEN transfer_usd = 0 THEN 0
        ELSE CAST(cex_touched_usd AS double) / CAST(transfer_usd AS double)
    END AS cex_touch_share,
    CASE
        WHEN unique_receivers = 0 THEN 0
        ELSE CAST(unique_senders AS double) / CAST(unique_receivers AS double)
    END AS sender_receiver_ratio
FROM agg
ORDER BY 1,2
"""


QUERIES = [
    QuerySpec(
        query_id="x7_structural_supply_elasticity",
        hypothesis="Net issuance and sink shocks alter effective float and directional skew.",
        sql_builder=build_supply_elasticity_sql,
    ),
    QuerySpec(
        query_id="x7_structural_holder_topology",
        hypothesis="Participant concentration (HHI/top-share) captures fragility/crowding structure.",
        sql_builder=build_holder_topology_sql,
    ),
    QuerySpec(
        query_id="x7_structural_cex_sender_structure",
        hypothesis="CEX-float transfer and sender/receiver topology capture exchange-available supply regime.",
        sql_builder=build_dormancy_cex_sql,
    ),
]


def load_dune_api_key(env_file: Path) -> str:
    text = env_file.read_text()
    m = re.search(r"DUNE_API_KEY=([A-Za-z0-9_-]+)", text)
    if not m:
        raise RuntimeError(f"DUNE_API_KEY not found in {env_file}")
    return m.group(1)


def _request_with_retry(
    method: str,
    url: str,
    headers: dict[str, str],
    json_payload: dict | None = None,
    timeout: int = 45,
    max_attempts: int = 9,
) -> requests.Response:
    last_err: Exception | None = None
    for attempt in range(max_attempts):
        try:
            resp = requests.request(method, url, headers=headers, json=json_payload, timeout=timeout)
            if resp.status_code in {429, 500, 502, 503, 504}:
                sleep_s = min(60.0, (2.0**attempt) + random.uniform(0.0, 1.0))
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:  # pragma: no cover
            last_err = e
            sleep_s = min(60.0, (2.0**attempt) + random.uniform(0.0, 1.0))
            time.sleep(sleep_s)
    raise RuntimeError(f"HTTP request failed after retries: {method} {url} err={last_err}")


def run_dune_sql(
    sql: str,
    api_key: str,
    poll_seconds: int = 6,
    max_wait_seconds: int = 1800,
) -> tuple[str, pd.DataFrame]:
    base = "https://api.dune.com/api/v1"
    headers = {"X-Dune-Api-Key": api_key, "Content-Type": "application/json"}

    execute = _request_with_retry(
        "POST",
        f"{base}/sql/execute",
        headers=headers,
        json_payload={"sql": sql, "performance": "medium"},
        timeout=60,
    )
    execution_id = execute.json()["execution_id"]

    deadline = time.time() + max_wait_seconds
    final_state = None
    payload: dict = {}
    while time.time() < deadline:
        status = _request_with_retry(
            "GET",
            f"{base}/execution/{execution_id}/status",
            headers=headers,
            timeout=60,
        )
        payload = status.json()
        final_state = payload.get("state")
        if final_state in {
            "QUERY_STATE_COMPLETED",
            "QUERY_STATE_FAILED",
            "QUERY_STATE_CANCELLED",
            "QUERY_STATE_EXPIRED",
        }:
            break
        time.sleep(poll_seconds)

    if final_state != "QUERY_STATE_COMPLETED":
        raise RuntimeError(
            f"Dune execution {execution_id} ended with state={final_state} payload={json.dumps(payload)[:1200]}"
        )

    results = _request_with_retry(
        "GET",
        f"{base}/execution/{execution_id}/results",
        headers=headers,
        timeout=90,
    )
    data = results.json()
    rows = data.get("result", {}).get("rows", [])
    return execution_id, pd.DataFrame(rows)


def load_price_panel(candles_dir: Path, lookback_days: int = 90) -> pd.DataFrame:
    frames = []
    for asset in ASSETS:
        path = candles_dir / f"bybit_perpetual|{asset}-USDT|1h.parquet"
        if not path.exists():
            continue
        c = pd.read_parquet(path)[["timestamp", "close"]].copy()
        c["hour"] = pd.to_datetime(c["timestamp"], unit="s", utc=True).dt.floor("h")
        c = c.sort_values("hour").drop_duplicates("hour")
        c["asset"] = asset
        for h in HORIZONS:
            c[f"fwd_ret_{h}h"] = c["close"].shift(-h) / c["close"] - 1.0
        c["ret_1h"] = np.log(c["close"] / c["close"].shift(1))
        c["realized_vol_24h"] = c["ret_1h"].rolling(24).std()
        frames.append(c[["hour", "asset", "realized_vol_24h"] + [f"fwd_ret_{h}h" for h in HORIZONS]])
    out = pd.concat(frames, ignore_index=True)
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=lookback_days) - pd.Timedelta(hours=max(HORIZONS))
    return out[out["hour"] >= cutoff].copy()


def prep_query_frame(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["hour"] = pd.to_datetime(out["hour"], utc=True)
    out["asset"] = out["asset"].astype(str).str.upper()
    out = out[out["asset"].isin(ASSETS)].sort_values(["asset", "hour"]).copy()
    for c in out.columns:
        if c not in {"hour", "asset"}:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    return out


def add_feature_engineering(panel: pd.DataFrame) -> pd.DataFrame:
    out = panel.sort_values(["asset", "hour"]).copy()
    raw_features = [
        c
        for c in out.columns
        if c not in {"hour", "asset"} and not c.startswith("fwd_ret_")
    ]
    for c in raw_features:
        g = out.groupby("asset")[c]
        mu24 = g.transform(lambda x: x.rolling(24, min_periods=8).mean())
        sd24 = g.transform(lambda x: x.rolling(24, min_periods=8).std())
        mu72 = g.transform(lambda x: x.rolling(72, min_periods=18).mean())
        sd72 = g.transform(lambda x: x.rolling(72, min_periods=18).std())
        mu168 = g.transform(lambda x: x.rolling(168, min_periods=24).mean())
        sd168 = g.transform(lambda x: x.rolling(168, min_periods=24).std())
        out[f"{c}_z24"] = (out[c] - mu24) / sd24.replace(0, np.nan)
        out[f"{c}_z72"] = (out[c] - mu72) / sd72.replace(0, np.nan)
        out[f"{c}_z168"] = (out[c] - mu168) / sd168.replace(0, np.nan)
        out[f"{c}_d1"] = g.diff(1)
        out[f"{c}_d6"] = g.diff(6)

    # targeted structure interactions
    if "participant_hhi" in out.columns and "dormant_30d_share" in out.columns:
        out["fragility_x_dormancy"] = out["participant_hhi"] * out["dormant_30d_share"]
    if "net_issuance_ratio" in out.columns and "cex_net_flow_ratio" in out.columns:
        out["issuance_x_cex"] = out["net_issuance_ratio"] * out["cex_net_flow_ratio"]
    if "top10_participant_share" in out.columns and "realized_vol_24h" in out.columns:
        out["top10_x_vol"] = out["top10_participant_share"] * out["realized_vol_24h"]

    return out


def _feature_columns(df: pd.DataFrame) -> list[str]:
    cols = [
        c
        for c in df.columns
        if c not in {"hour", "asset"} and not c.startswith("fwd_ret_")
    ]
    keep = [c for c in cols if df[c].notna().mean() >= 0.25]
    return keep


def benjamini_hochberg_qvalues(p_values: pd.Series) -> pd.Series:
    p = p_values.copy()
    q = pd.Series(np.nan, index=p.index, dtype=float)
    valid = p.dropna()
    if valid.empty:
        return q
    n = len(valid)
    order = valid.sort_values().index
    ranked = valid.loc[order]
    bh = ranked * n / np.arange(1, n + 1)
    bh_rev = np.minimum.accumulate(bh.iloc[::-1])[::-1]
    q.loc[order] = bh_rev.values
    return q


def ic_scan(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    features = _feature_columns(df)
    pooled_rows = []
    asset_rows = []

    for feat in features:
        for lag in LAGS:
            lagged = df.groupby("asset")[feat].shift(lag)
            for h in HORIZONS:
                ycol = f"fwd_ret_{h}h"
                sub = df[["asset", ycol]].copy()
                sub["x"] = lagged
                sub = sub.dropna(subset=["x", ycol])
                n = len(sub)
                if n >= 30 and sub["x"].nunique() > 1 and sub[ycol].nunique() > 1:
                    rho, p = spearmanr(sub["x"], sub[ycol], nan_policy="omit")
                else:
                    rho, p = np.nan, np.nan
                pooled_rows.append(
                    {
                        "feature": feat,
                        "lag_h": lag,
                        "horizon_h": h,
                        "n": int(n),
                        "rho": float(rho) if pd.notna(rho) else np.nan,
                        "p_value": float(p) if pd.notna(p) else np.nan,
                    }
                )

                for asset, g in sub.groupby("asset"):
                    n_a = len(g)
                    if n_a >= 30 and g["x"].nunique() > 1 and g[ycol].nunique() > 1:
                        rho_a, p_a = spearmanr(g["x"], g[ycol], nan_policy="omit")
                    else:
                        rho_a, p_a = np.nan, np.nan
                    asset_rows.append(
                        {
                            "feature": feat,
                            "asset": asset,
                            "lag_h": lag,
                            "horizon_h": h,
                            "n": int(n_a),
                            "rho": float(rho_a) if pd.notna(rho_a) else np.nan,
                            "p_value": float(p_a) if pd.notna(p_a) else np.nan,
                        }
                    )

    pooled = pd.DataFrame(pooled_rows)
    pooled["q_value"] = benjamini_hochberg_qvalues(pooled["p_value"])
    pooled["significant"] = (pooled["n"] > 100) & (pooled["p_value"] < 0.05)
    pooled["fdr_significant"] = (pooled["n"] > 100) & (pooled["q_value"] < 0.10)

    per_asset = pd.DataFrame(asset_rows)
    per_asset["q_value"] = benjamini_hochberg_qvalues(per_asset["p_value"])
    per_asset["significant"] = (per_asset["n"] > 100) & (per_asset["p_value"] < 0.05)
    per_asset["fdr_significant"] = (per_asset["n"] > 100) & (per_asset["q_value"] < 0.10)
    return pooled, per_asset


def top_features_for_model(pooled_ic: pd.DataFrame, top_k: int = 16) -> pd.DataFrame:
    score = pooled_ic.copy()
    score = score[score["n"] > 120].copy()
    if score.empty:
        return pd.DataFrame()
    # prioritize 4h and 24h structural predictability, then abs rho and q-value
    score["horizon_weight"] = np.where(score["horizon_h"] == 4, 1.2, np.where(score["horizon_h"] == 24, 1.0, 0.8))
    score["alpha_score"] = score["horizon_weight"] * score["rho"].abs() * np.log1p(score["n"])
    score = score.sort_values(["fdr_significant", "significant", "alpha_score", "q_value"], ascending=[False, False, False, True])
    best = score.groupby("feature", as_index=False).head(1).head(top_k).copy()
    return best


def monotonic_quintile_test(panel: pd.DataFrame, feature_list: list[str], horizon_h: int = 4, lag_h: int = 1) -> pd.DataFrame:
    rows = []
    ycol = f"fwd_ret_{horizon_h}h"
    for feat in feature_list:
        if feat not in panel.columns:
            continue
        d = panel[["asset", feat, ycol]].copy()
        d["x"] = d.groupby("asset")[feat].shift(lag_h)
        d = d.dropna(subset=["x", ycol]).copy()
        for asset, g in d.groupby("asset"):
            if len(g) < 120 or g["x"].nunique() < 12:
                continue
            g = g.sort_values("x").copy()
            g["q"] = pd.qcut(g["x"], 5, labels=False, duplicates="drop") + 1
            qret = g.groupby("q")[ycol].mean().reset_index()
            if qret["q"].nunique() < 4:
                continue
            rho, p = spearmanr(qret["q"], qret[ycol], nan_policy="omit")
            rows.append(
                {
                    "feature": feat,
                    "asset": asset,
                    "horizon_h": horizon_h,
                    "lag_h": lag_h,
                    "n": int(len(g)),
                    "q1_ret": float(qret.loc[qret["q"] == qret["q"].min(), ycol].iloc[0]),
                    "q5_ret": float(qret.loc[qret["q"] == qret["q"].max(), ycol].iloc[0]),
                    "q5_minus_q1": float(qret.loc[qret["q"] == qret["q"].max(), ycol].iloc[0] - qret.loc[qret["q"] == qret["q"].min(), ycol].iloc[0]),
                    "mono_rho": float(rho) if pd.notna(rho) else np.nan,
                    "mono_p": float(p) if pd.notna(p) else np.nan,
                }
            )
    out = pd.DataFrame(rows)
    if not out.empty:
        out["mono_q"] = benjamini_hochberg_qvalues(out["mono_p"])
        out["monotonic_sig"] = (out["n"] > 100) & (out["mono_p"] < 0.05)
    return out


def walk_forward_structural(
    panel: pd.DataFrame,
    selected: pd.DataFrame,
    horizon_h: int = 4,
    train_days: int = 45,
    test_days: int = 15,
    fee_oneway: float = 0.00055,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    if selected.empty:
        return pd.DataFrame(), pd.DataFrame(), {}

    selected_feats = selected["feature"].tolist()
    data = panel[["hour", "asset", f"fwd_ret_{horizon_h}h"] + selected_feats].copy()
    data = data.sort_values(["asset", "hour"]).copy()
    # strict causality: lag all selected features by +1h
    for f in selected_feats:
        data[f] = data.groupby("asset")[f].shift(1)

    # non-overlap decisions
    data = data[data["hour"].dt.hour % horizon_h == 0].copy()
    ycol = f"fwd_ret_{horizon_h}h"
    data = data.dropna(subset=[ycol]).copy()

    # robust normalize per asset rolling
    for f in selected_feats:
        g = data.groupby("asset")[f]
        mu = g.transform(lambda x: x.rolling(72, min_periods=24).mean())
        sd = g.transform(lambda x: x.rolling(72, min_periods=24).std()).replace(0, np.nan)
        data[f"{f}_z"] = (data[f] - mu) / sd

    zcols = [f"{f}_z" for f in selected_feats]
    data = data.dropna(subset=zcols + [ycol]).copy()
    if data.empty:
        return pd.DataFrame(), pd.DataFrame(), {}

    hours = sorted(data["hour"].unique())
    train_td = pd.Timedelta(days=train_days)
    test_td = pd.Timedelta(days=test_days)
    step_td = pd.Timedelta(days=test_days)

    fold_rows = []
    hourly_rows = []
    cur = hours[0]
    fold = 0

    while cur + train_td + test_td <= hours[-1] + pd.Timedelta(hours=1):
        tr_start = cur
        tr_end = cur + train_td
        te_start = tr_end
        te_end = tr_end + test_td

        tr = data[(data["hour"] >= tr_start) & (data["hour"] < tr_end)].copy()
        te = data[(data["hour"] >= te_start) & (data["hour"] < te_end)].copy()
        if len(tr) < 400 or len(te) < 80:
            cur += step_td
            continue

        # learn feature orientation from train Spearman to 4h target
        orient = {}
        for f in selected_feats:
            sub = tr[[f, ycol]].dropna()
            if len(sub) < 120 or sub[f].nunique() < 10:
                orient[f] = 0.0
                continue
            rho, _ = spearmanr(sub[f], sub[ycol], nan_policy="omit")
            orient[f] = float(np.sign(rho)) if pd.notna(rho) else 0.0

        # weighted structural score
        weights = selected.set_index("feature")["rho"].abs().to_dict()
        te["score"] = 0.0
        for f in selected_feats:
            w = float(weights.get(f, 0.0))
            te["score"] += orient.get(f, 0.0) * w * te[f"{f}_z"].fillna(0.0)

        # portfolio: long top2, short bottom2 each decision hour
        hourly = []
        for h, g in te.groupby("hour"):
            g = g.sort_values("score", ascending=False)
            top = g.head(2)
            bot = g.tail(2)
            ret = 0.5 * top[ycol].sum() - 0.5 * bot[ycol].sum()
            # turnover proxy: assume rebalance every step for selected legs
            cost = 4 * fee_oneway  # 2 longs + 2 shorts, one-way each leg
            net = ret - cost
            hourly.append(
                {
                    "hour": h,
                    "fold": fold,
                    "gross_ret": float(ret),
                    "net_ret": float(net),
                    "top_assets": ",".join(top["asset"].tolist()),
                    "bot_assets": ",".join(bot["asset"].tolist()),
                }
            )

        hdf = pd.DataFrame(hourly).sort_values("hour")
        if hdf.empty:
            cur += step_td
            continue

        fold += 1
        ann_factor = np.sqrt((365 * 24) / horizon_h)
        s = hdf["net_ret"].std()
        sharpe = float((hdf["net_ret"].mean() / s) * ann_factor) if s > 0 else np.nan
        t_stat, p_val = ttest_1samp(hdf["net_ret"], 0.0) if len(hdf) > 3 else (np.nan, np.nan)

        fold_rows.append(
            {
                "fold": fold,
                "train_start": str(tr_start),
                "train_end": str(tr_end),
                "test_start": str(te_start),
                "test_end": str(te_end),
                "n_train": int(len(tr)),
                "n_test": int(len(te)),
                "hours_test": int(hdf["hour"].nunique()),
                "mean_net_ret": float(hdf["net_ret"].mean()),
                "sharpe_ann": sharpe,
                "ttest_p": float(p_val) if pd.notna(p_val) else np.nan,
                "total_net_return": float((1 + hdf["net_ret"]).prod() - 1),
                "hit_rate": float((hdf["net_ret"] > 0).mean()),
                "orientation": json.dumps(orient),
            }
        )
        hourly_rows.append(hdf)
        cur += step_td

    folds = pd.DataFrame(fold_rows)
    hourly = pd.concat(hourly_rows, ignore_index=True) if hourly_rows else pd.DataFrame()
    if not hourly.empty:
        hourly = hourly.sort_values("hour")
        hourly["cum_net_ret"] = (1 + hourly["net_ret"]).cumprod() - 1

    metrics = {}
    if not folds.empty and not hourly.empty:
        ann_factor = np.sqrt((365 * 24) / horizon_h)
        s_all = hourly["net_ret"].std()
        metrics = {
            "folds": int(len(folds)),
            "selected_features": selected_feats,
            "hours_scored": int(hourly["hour"].nunique()),
            "mean_fold_sharpe": float(folds["sharpe_ann"].mean()),
            "median_fold_sharpe": float(folds["sharpe_ann"].median()),
            "pct_folds_positive": float((folds["sharpe_ann"] > 0).mean()),
            "global_mean_net_ret": float(hourly["net_ret"].mean()),
            "global_sharpe_ann": float((hourly["net_ret"].mean() / s_all) * ann_factor) if s_all > 0 else np.nan,
            "global_total_return": float(hourly["cum_net_ret"].iloc[-1]),
            "global_hit_rate": float((hourly["net_ret"] > 0).mean()),
        }
    return folds, hourly, metrics


def main() -> None:
    p = argparse.ArgumentParser(description="Structural non-flow on-chain research for X7.")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--candles-dir", default="app/data/cache/candles")
    p.add_argument("--out-dir", default="app/data/cache/onchain_x7_structural")
    p.add_argument("--use-cache", action="store_true", default=False)
    p.add_argument("--max-wait-seconds", type=int, default=1800)
    args = p.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    api_key = load_dune_api_key(Path(args.env_file))
    price = load_price_panel(Path(args.candles_dir), lookback_days=90)

    metadata = []
    frames = []
    for spec in QUERIES:
        sql = spec.sql_builder().strip()
        sql_path = out_dir / f"{run_ts}_{spec.query_id}.sql"
        raw_path = out_dir / f"{run_ts}_{spec.query_id}_raw.csv"
        sql_path.write_text(sql + "\n")

        if args.use_cache and raw_path.exists():
            raw = pd.read_csv(raw_path)
            ex_id = "cached"
        else:
            ex_id, raw = run_dune_sql(sql, api_key, poll_seconds=6, max_wait_seconds=args.max_wait_seconds)
            raw.to_csv(raw_path, index=False)
        clean = prep_query_frame(raw)
        frames.append(clean)
        metadata.append(
            {
                "query_id": spec.query_id,
                "hypothesis": spec.hypothesis,
                "execution_id": ex_id,
                "raw_rows": int(len(raw)),
                "raw_csv": str(raw_path),
                "sql_file": str(sql_path),
            }
        )
        print(f"[{spec.query_id}] execution_id={ex_id} rows={len(raw)}")

    panel = price.copy()
    for f in frames:
        panel = panel.merge(f, on=["hour", "asset"], how="left")
    panel = add_feature_engineering(panel)
    panel_path = out_dir / f"{run_ts}_feature_panel.csv"
    panel.to_csv(panel_path, index=False)

    pooled_ic, asset_ic = ic_scan(panel)
    pooled_ic_path = out_dir / f"{run_ts}_pooled_ic.csv"
    asset_ic_path = out_dir / f"{run_ts}_asset_ic.csv"
    pooled_ic.to_csv(pooled_ic_path, index=False)
    asset_ic.to_csv(asset_ic_path, index=False)

    selected = top_features_for_model(pooled_ic, top_k=18)
    selected_path = out_dir / f"{run_ts}_selected_features.csv"
    selected.to_csv(selected_path, index=False)

    top_for_quintile = selected["feature"].tolist()[:10] if not selected.empty else []
    quintiles = monotonic_quintile_test(panel, top_for_quintile, horizon_h=4, lag_h=1)
    quintile_path = out_dir / f"{run_ts}_quintile_tests.csv"
    quintiles.to_csv(quintile_path, index=False)

    folds, hourly, wf_metrics = walk_forward_structural(
        panel=panel,
        selected=selected,
        horizon_h=4,
        train_days=45,
        test_days=15,
        fee_oneway=0.00055,
    )
    folds_path = out_dir / f"{run_ts}_walkforward_folds.csv"
    hourly_path = out_dir / f"{run_ts}_walkforward_hourly.csv"
    folds.to_csv(folds_path, index=False)
    hourly.to_csv(hourly_path, index=False)

    meta_path = out_dir / f"{run_ts}_metadata.json"
    metrics_path = out_dir / f"{run_ts}_summary_metrics.json"

    structural_findings = []
    if not pooled_ic.empty:
        sig = pooled_ic[(pooled_ic["n"] > 100) & (pooled_ic["p_value"] < 0.05)].copy()
        if not sig.empty:
            top_sig = (
                sig.sort_values(["horizon_h", "p_value", "rho"], ascending=[True, True, False])
                .head(40)[["feature", "lag_h", "horizon_h", "n", "rho", "p_value", "q_value"]]
            )
            structural_findings = top_sig.to_dict(orient="records")

    summary = {
        "run_ts": run_ts,
        "queries": metadata,
        "panel_rows": int(len(panel)),
        "panel_assets": sorted(panel["asset"].dropna().unique().tolist()),
        "pooled_significant_count": int(((pooled_ic["n"] > 100) & (pooled_ic["p_value"] < 0.05)).sum()),
        "pooled_fdr_significant_count": int(((pooled_ic["n"] > 100) & (pooled_ic["q_value"] < 0.10)).sum()),
        "selected_feature_count": int(len(selected)),
        "walkforward_metrics": wf_metrics,
        "top_findings": structural_findings[:20],
        "artifacts": {
            "panel_csv": str(panel_path),
            "pooled_ic_csv": str(pooled_ic_path),
            "asset_ic_csv": str(asset_ic_path),
            "selected_csv": str(selected_path),
            "quintile_csv": str(quintile_path),
            "walkforward_folds_csv": str(folds_path),
            "walkforward_hourly_csv": str(hourly_path),
        },
    }

    meta_path.write_text(json.dumps(metadata, indent=2))
    metrics_path.write_text(json.dumps(summary, indent=2))

    print("\nTop pooled IC rows (p<0.05, N>100):")
    sig = pooled_ic[(pooled_ic["n"] > 100) & (pooled_ic["p_value"] < 0.05)].sort_values("p_value")
    if sig.empty:
        print("None")
    else:
        print(sig.head(30)[["feature", "lag_h", "horizon_h", "n", "rho", "p_value", "q_value"]].to_string(index=False))

    print("\nWalk-forward metrics:")
    print(json.dumps(wf_metrics, indent=2))

    print("\nArtifacts:")
    print(f"- {meta_path}")
    print(f"- {metrics_path}")
    print(f"- {panel_path}")
    print(f"- {pooled_ic_path}")
    print(f"- {asset_ic_path}")
    print(f"- {selected_path}")
    print(f"- {quintile_path}")
    print(f"- {folds_path}")
    print(f"- {hourly_path}")


if __name__ == "__main__":
    main()
