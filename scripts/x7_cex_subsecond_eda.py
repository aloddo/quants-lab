#!/usr/bin/env python3
"""
X7 CEX microstructure EDA for fast execution exploitation.

Focus:
1) Verify true market-data cadence in quote snapshot collections.
2) Evaluate Binance->Bybit lead-lag impulse alpha on 5s merged data.
3) Run cost sensitivity and on-chain regime gating.
4) Produce hour/day regime diagnostics for deployment windows.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from pymongo import MongoClient


ASSETS = ["BTC", "ETH", "SOL", "XRP", "DOGE"]


def to_ms(ts: object) -> float:
    if isinstance(ts, datetime):
        t = pd.Timestamp(ts)
        if t.tzinfo is None:
            t = t.tz_localize("UTC")
        else:
            t = t.tz_convert("UTC")
        return float(t.value // 10**6)
    if isinstance(ts, (int, float)):
        if ts > 1e12:
            return float(ts)
        return float(ts * 1000.0)
    return np.nan


def profile_snapshot_cadence(db) -> pd.DataFrame:
    collections = [
        "arb_quote_snapshots",
        "arb_bb_spot_perp_snapshots",
        "arb_bn_usdc_bb_perp_snapshots",
        "arb_bv_eur_bb_perp_snapshots",
    ]
    rows = []
    for col in collections:
        proj = {"_id": 0, "timestamp": 1, "symbol": 1, "symbol_bb": 1, "symbol_bn": 1, "symbol_bv": 1}
        docs = list(db[col].find({}, proj).sort("timestamp", -1).limit(300000))
        if not docs:
            continue
        df = pd.DataFrame(docs)
        sym_col = "symbol"
        if sym_col not in df.columns:
            for cand in ["symbol_bb", "symbol_bn", "symbol_bv"]:
                if cand in df.columns:
                    sym_col = cand
                    break
        df["symbol"] = df[sym_col].astype(str)
        df["t_ms"] = df["timestamp"].map(to_ms)
        df = df.dropna(subset=["t_ms"])
        for sym, g in df.groupby("symbol"):
            if len(g) < 300:
                continue
            d = g.sort_values("t_ms")["t_ms"].diff().dropna()
            rows.append(
                {
                    "collection": col,
                    "symbol": sym,
                    "n": int(len(g)),
                    "delta_ms_p50": float(d.quantile(0.5)),
                    "delta_ms_p90": float(d.quantile(0.9)),
                    "delta_ms_p99": float(d.quantile(0.99)),
                }
            )
    out = pd.DataFrame(rows).sort_values(["collection", "n"], ascending=[True, False])
    return out


def load_onchain_regime(panel_path: Path) -> dict[str, pd.DataFrame]:
    panel = pd.read_csv(panel_path)
    panel["hour"] = pd.to_datetime(panel["hour"], utc=True)
    panel = panel.sort_values(["asset", "hour"]).copy()
    lag_cols = [c for c in panel.columns if c not in {"hour", "asset", "fwd_ret_1h", "fwd_ret_4h", "fwd_ret_24h"}]
    for c in lag_cols:
        panel[c] = panel.groupby("asset")[c].shift(1)

    out = {}
    for a in ASSETS:
        d = panel[panel["asset"] == a][["hour", "stable_buy_usd_z24"]].dropna().copy()
        out[a] = d
    return out


def run_leadlag_for_asset(asset: str, candles_root: Path, onchain_hourly: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    symbol = f"{asset}USDT"
    p = candles_root / symbol / f"{symbol}_5s_merged_14d.parquet"
    if not p.exists():
        return pd.DataFrame(), pd.DataFrame()

    df = pd.read_parquet(p).reset_index().rename(columns={"timestamp": "ts"})
    for c in ["bn_vwap", "bb_vwap"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["bn_vwap", "bb_vwap"]).copy()
    if len(df) < 10000:
        return pd.DataFrame(), pd.DataFrame()

    df["lead_impulse"] = np.log(df["bn_vwap"] / df["bn_vwap"].shift(1)) - np.log(df["bb_vwap"] / df["bb_vwap"].shift(1))
    df["fwd_bb_ret_10s_bps"] = np.log(df["bb_vwap"].shift(-2) / df["bb_vwap"]) * 10000.0
    df["fwd_bb_ret_30s_bps"] = np.log(df["bb_vwap"].shift(-6) / df["bb_vwap"]) * 10000.0
    df["hour"] = pd.to_datetime(df["ts"], utc=True).dt.floor("h")
    df = df.merge(onchain_hourly, on="hour", how="left")
    df = df.dropna(subset=["lead_impulse", "fwd_bb_ret_10s_bps", "stable_buy_usd_z24"]).copy()

    q_values = [0.9, 0.95, 0.97, 0.98, 0.99, 0.995]
    cost_values = [0.25, 0.5, 1.0]

    sweep_rows = []
    for q in q_values:
        hi = df["lead_impulse"].quantile(q)
        lo = df["lead_impulse"].quantile(1 - q)
        for h, ret_col in [(2, "fwd_bb_ret_10s_bps"), (6, "fwd_bb_ret_30s_bps")]:
            d = df[["lead_impulse", ret_col, "stable_buy_usd_z24"]].dropna().copy()
            pos = np.where(d["lead_impulse"] >= hi, 1, np.where(d["lead_impulse"] <= lo, -1, 0))
            trade_rate = float(np.mean(pos != 0))
            if trade_rate < 0.005:
                continue
            gross_all = pos * d[ret_col].to_numpy()
            gross_bps = float(np.mean(gross_all))
            trade_gross_bps = float(np.mean(gross_all[pos != 0])) if np.any(pos != 0) else np.nan
            win_trade = float(np.mean(gross_all[pos != 0] > 0)) if np.any(pos != 0) else np.nan
            for cost in cost_values:
                net = gross_all - np.abs(pos) * cost
                sweep_rows.append(
                    {
                        "asset": asset,
                        "h_5s": h,
                        "q": q,
                        "cost_bps": cost,
                        "trade_rate": trade_rate,
                        "gross_bps": gross_bps,
                        "net_bps": float(np.mean(net)),
                        "trade_gross_bps": trade_gross_bps,
                        "win_trade": win_trade,
                    }
                )

    # fixed operating point diagnostics (q=0.95, h=2, cost=0.5)
    d = df[["ts", "lead_impulse", "fwd_bb_ret_10s_bps", "stable_buy_usd_z24"]].dropna().copy()
    hi = d["lead_impulse"].quantile(0.95)
    lo = d["lead_impulse"].quantile(0.05)
    d["pos"] = np.where(d["lead_impulse"] >= hi, 1, np.where(d["lead_impulse"] <= lo, -1, 0))
    d["net"] = d["pos"] * d["fwd_bb_ret_10s_bps"] - np.abs(d["pos"]) * 0.5
    d["hour"] = pd.to_datetime(d["ts"], utc=True).dt.hour
    d["dow"] = pd.to_datetime(d["ts"], utc=True).dt.dayofweek
    d["regime"] = pd.cut(d["stable_buy_usd_z24"], bins=[-1e9, -1, 1, 1e9], labels=["neg", "neutral", "pos"])

    regime_rows = []
    for rg, g in d.groupby("regime"):
        tr = g[g["pos"] != 0]
        regime_rows.append(
            {
                "asset": asset,
                "regime": str(rg),
                "n": int(len(g)),
                "trade_rate": float((g["pos"] != 0).mean()),
                "net_bps": float(g["net"].mean()),
                "trade_net_bps": float(tr["net"].mean()) if len(tr) else np.nan,
                "win_trade": float((tr["net"] > 0).mean()) if len(tr) else np.nan,
            }
        )

    for h, g in d.groupby("hour"):
        tr = g[g["pos"] != 0]
        regime_rows.append(
            {
                "asset": asset,
                "regime": f"hour_{h:02d}",
                "n": int(len(g)),
                "trade_rate": float((g["pos"] != 0).mean()),
                "net_bps": float(g["net"].mean()),
                "trade_net_bps": float(tr["net"].mean()) if len(tr) else np.nan,
                "win_trade": float((tr["net"] > 0).mean()) if len(tr) else np.nan,
            }
        )

    for w, g in d.groupby("dow"):
        tr = g[g["pos"] != 0]
        regime_rows.append(
            {
                "asset": asset,
                "regime": f"dow_{w}",
                "n": int(len(g)),
                "trade_rate": float((g["pos"] != 0).mean()),
                "net_bps": float(g["net"].mean()),
                "trade_net_bps": float(tr["net"].mean()) if len(tr) else np.nan,
                "win_trade": float((tr["net"] > 0).mean()) if len(tr) else np.nan,
            }
        )

    return pd.DataFrame(sweep_rows), pd.DataFrame(regime_rows)


def main() -> None:
    p = argparse.ArgumentParser(description="CEX fast-execution EDA.")
    p.add_argument("--mongo-uri", default="mongodb://localhost:27017/quants_lab")
    p.add_argument("--panel", default="app/data/cache/onchain_x7_advanced/20260420_154932_feature_panel.csv")
    p.add_argument("--arb-trades-root", default="app/data/cache/arb_trades")
    p.add_argument("--out-prefix", default="app/data/cache/x7_cex_fast_eda_20260420")
    args = p.parse_args()

    client = MongoClient(args.mongo_uri)
    db = client["quants_lab"]

    cadence = profile_snapshot_cadence(db)
    onchain = load_onchain_regime(Path(args.panel))

    sweep_frames = []
    regime_frames = []
    for a in ASSETS:
        sw, rg = run_leadlag_for_asset(a, Path(args.arb_trades_root), onchain.get(a, pd.DataFrame(columns=["hour", "stable_buy_usd_z24"])))
        if not sw.empty:
            sweep_frames.append(sw)
        if not rg.empty:
            regime_frames.append(rg)

    sweep = pd.concat(sweep_frames, ignore_index=True) if sweep_frames else pd.DataFrame()
    regime = pd.concat(regime_frames, ignore_index=True) if regime_frames else pd.DataFrame()

    out_prefix = Path(args.out_prefix)
    cadence_path = f"{out_prefix}_cadence.csv"
    cadence.to_csv(cadence_path, index=False)

    if not sweep.empty:
        sweep_path = f"{out_prefix}_leadlag_sweep.csv"
        sweep.to_csv(sweep_path, index=False)
    else:
        sweep_path = None

    if not regime.empty:
        regime_path = f"{out_prefix}_regime.csv"
        regime.to_csv(regime_path, index=False)
    else:
        regime_path = None

    summary = {
        "cadence_file": cadence_path,
        "leadlag_file": sweep_path,
        "regime_file": regime_path,
        "notes": [
            "Cadence table reflects observed data update frequency in stored snapshots.",
            "Lead-lag sweep reports expected bps per 5s sample after cost assumptions.",
            "Regime table includes on-chain regime bins and session slices for q=0.95, h=10s, cost=0.5bps.",
        ],
    }

    print(json.dumps(summary, indent=2))
    if not cadence.empty:
        print("\nSnapshot cadence head:")
        print(cadence.head(20).to_string(index=False))
    if not sweep.empty:
        print("\nTop lead-lag configs by net_bps:")
        print(sweep.sort_values("net_bps", ascending=False).head(20).to_string(index=False))
    if not regime.empty:
        print("\nTop positive regime rows:")
        print(regime.sort_values("net_bps", ascending=False).head(20).to_string(index=False))


if __name__ == "__main__":
    main()
