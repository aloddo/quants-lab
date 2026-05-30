#!/usr/bin/env python3
"""
X11 Hyperliquid Signal EDA + Hypothesis Sweep

Focus:
  - Hyperliquid-native funding/premium signals
  - Cross-venue carry spread (HL vs Bybit/Binance)
  - Directional crowding/liquidation signals
  - Market-making skew/inventory signals (short-horizon mean reversion)

Inputs:
  - Parquet: app/data/cache/candles/hyperliquid_perpetual|PAIR|1h.parquet
  - MongoDB: hyperliquid_funding_rates, bybit_funding_rates, binance_funding_rates,
             bybit_open_interest, coinalyze_liquidations

Outputs:
  - Sweep, enriched ranking, candidates, feature IC, quantile markouts in out-dir
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
from pymongo import MongoClient


@dataclass(frozen=True)
class HypothesisConfig:
    family: str
    strategy_type: str
    z1: float
    z2: float
    z3: float
    hold_h: int


@dataclass
class EvalStats:
    pair: str
    n_trades: int
    trade_bps_mean: float
    pf: float
    win_rate: float
    total_bps: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="X11 Hyperliquid signal EDA")
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab"))
    p.add_argument("--candles-dir", default="app/data/cache/candles")
    p.add_argument("--interval", default="1h", choices=["1m", "5m", "15m", "1h"])
    p.add_argument("--out-dir", default="app/data/cache/x11_hyperliquid")
    p.add_argument("--start", default="2025-04-21", help="UTC date inclusive (YYYY-MM-DD)")
    p.add_argument("--end", default="2026-04-21", help="UTC date inclusive (YYYY-MM-DD)")
    p.add_argument("--max-pairs", type=int, default=24)
    p.add_argument("--min-bars", type=int, default=2500)
    p.add_argument("--train-frac", type=float, default=0.65)
    p.add_argument("--cost-bps-oneway", type=float, default=3.0)
    p.add_argument("--min-train-trades", type=int, default=20)
    p.add_argument("--min-test-trades", type=int, default=10)
    p.add_argument("--min-selected-pairs", type=int, default=4)
    p.add_argument("--top-k-wf", type=int, default=70)
    return p.parse_args()


def _db_name_from_uri(uri: str) -> str:
    if "/" not in uri:
        return "quants_lab"
    tail = uri.rsplit("/", 1)[-1]
    return tail or "quants_lab"


def rolling_z(s: pd.Series, window: int, min_periods: int | None = None) -> pd.Series:
    mp = min_periods if min_periods is not None else max(10, window // 3)
    mu = s.rolling(window, min_periods=mp).mean()
    sd = s.rolling(window, min_periods=mp).std().replace(0, np.nan)
    return (s - mu) / sd


def interval_to_minutes(interval: str) -> int:
    return {"1m": 1, "5m": 5, "15m": 15, "1h": 60}[interval]


def interval_to_pandas_freq(interval: str) -> str:
    return {"1m": "1min", "5m": "5min", "15m": "15min", "1h": "1h"}[interval]


def bars_for_hours(hours: float, interval_minutes: int) -> int:
    return max(1, int(round(hours * 60.0 / interval_minutes)))


def compute_pf(arr: np.ndarray) -> float:
    if arr.size == 0:
        return np.nan
    gp = arr[arr > 0].sum()
    gl = -arr[arr < 0].sum()
    if gl <= 0:
        return np.nan
    return float(gp / gl)


def split_idx(n: int, train_frac: float) -> int:
    return int(max(80, min(n - 80, round(n * train_frac))))


def make_trades_from_signal(
    close: np.ndarray,
    sig: np.ndarray,
    hold: int,
    cost_bps_oneway: float,
    bybit_funding: np.ndarray | None = None,
    include_carry: bool = False,
) -> np.ndarray:
    n = len(close)
    if n <= hold + 2:
        return np.array([], dtype=float)
    out: List[float] = []
    i = 0
    while i < n - hold - 1:
        side = sig[i]
        if side == 0:
            i += 1
            continue
        entry_i = i + 1
        exit_i = i + hold
        ret = side * ((close[exit_i] - close[entry_i]) / close[entry_i]) * 1e4 - 2.0 * cost_bps_oneway
        if include_carry and bybit_funding is not None:
            fr_slice = bybit_funding[entry_i : exit_i + 1]
            if fr_slice.size > 0 and np.isfinite(fr_slice).any():
                avg_rate = float(np.nanmean(fr_slice))
                carry_bps = (-side) * avg_rate * 1e4 * (hold / 8.0)
                ret += carry_bps
        out.append(float(ret))
        i = exit_i + 1
    return np.asarray(out, dtype=float)


def build_universe(db, candles_dir: Path, interval: str) -> List[str]:
    hl_pairs = {
        p.name.split("|")[1]
        for p in candles_dir.glob(f"hyperliquid_perpetual|*-USDT|{interval}.parquet")
    }
    bybit_f = set(db["bybit_funding_rates"].distinct("pair"))
    bin_f = set(db["binance_funding_rates"].distinct("pair"))
    hl_f = set(db["hyperliquid_funding_rates"].distinct("pair"))
    oi = set(db["bybit_open_interest"].distinct("pair"))
    liq = set(db["coinalyze_liquidations"].distinct("pair"))
    return sorted(hl_pairs & bybit_f & bin_f & hl_f & oi & liq)


def load_pair_panel(
    pair: str,
    db,
    candles_dir: Path,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    interval: str,
) -> pd.DataFrame | None:
    path = candles_dir / f"hyperliquid_perpetual|{pair}|{interval}.parquet"
    if not path.exists():
        return None

    interval_minutes = interval_to_minutes(interval)
    freq = interval_to_pandas_freq(interval)
    b = lambda h: bars_for_hours(h, interval_minutes)

    warmup_start = start_ts - pd.Timedelta(days=45)
    start_s = int(warmup_start.timestamp())
    end_s = int(end_ts.timestamp())
    start_ms = int(warmup_start.timestamp() * 1000)
    end_ms = int(end_ts.timestamp() * 1000)

    try:
        px = pd.read_parquet(path, columns=["timestamp", "open", "high", "low", "close", "volume"])
    except Exception:
        return None
    px = px[(px["timestamp"] >= start_s) & (px["timestamp"] <= end_s)].copy()
    if len(px) < 1500:
        return None
    px["ts"] = pd.to_datetime(px["timestamp"], unit="s", utc=True)
    px = px.set_index("ts").sort_index().rename(
        columns={"open": "hl_open", "high": "hl_high", "low": "hl_low", "close": "hl_close", "volume": "hl_vol"}
    )

    hl_docs = list(
        db["hyperliquid_funding_rates"]
        .find(
            {"pair": pair, "timestamp_utc": {"$gte": start_ms, "$lte": end_ms}},
            {"_id": 0, "timestamp_utc": 1, "funding_rate": 1, "premium": 1},
        )
        .sort("timestamp_utc", 1)
    )
    by_docs = list(
        db["bybit_funding_rates"]
        .find(
            {"pair": pair, "timestamp_utc": {"$gte": start_ms, "$lte": end_ms}},
            {"_id": 0, "timestamp_utc": 1, "funding_rate": 1},
        )
        .sort("timestamp_utc", 1)
    )
    bn_docs = list(
        db["binance_funding_rates"]
        .find(
            {"pair": pair, "timestamp_utc": {"$gte": start_ms, "$lte": end_ms}},
            {"_id": 0, "timestamp_utc": 1, "funding_rate": 1},
        )
        .sort("timestamp_utc", 1)
    )
    oi_docs = list(
        db["bybit_open_interest"]
        .find(
            {"pair": pair, "timestamp_utc": {"$gte": start_ms, "$lte": end_ms}},
            {"_id": 0, "timestamp_utc": 1, "oi_value": 1},
        )
        .sort("timestamp_utc", 1)
    )
    liq_docs = list(
        db["coinalyze_liquidations"]
        .find(
            {"pair": pair, "timestamp_utc": {"$gte": start_ms, "$lte": end_ms}},
            {"_id": 0, "timestamp_utc": 1, "long_liquidations_usd": 1, "short_liquidations_usd": 1, "total_liquidations_usd": 1},
        )
        .sort("timestamp_utc", 1)
    )

    if len(hl_docs) < 500 or len(by_docs) < 120 or len(bn_docs) < 120 or len(oi_docs) < 600 or len(liq_docs) < 600:
        return None

    hlf = pd.DataFrame(hl_docs)
    hlf["ts"] = pd.to_datetime(hlf["timestamp_utc"], unit="ms", utc=True)
    hlf = hlf.set_index("ts")[["funding_rate", "premium"]].sort_index()
    hlf = hlf.rename(columns={"funding_rate": "hl_funding", "premium": "hl_premium"})
    hlf = hlf.resample(freq).last().ffill()

    by = pd.DataFrame(by_docs)
    by["ts"] = pd.to_datetime(by["timestamp_utc"], unit="ms", utc=True)
    by = by.set_index("ts")[["funding_rate"]].sort_index().rename(columns={"funding_rate": "bybit_funding"})
    by = by.resample(freq).last().ffill()

    bn = pd.DataFrame(bn_docs)
    bn["ts"] = pd.to_datetime(bn["timestamp_utc"], unit="ms", utc=True)
    bn = bn.set_index("ts")[["funding_rate"]].sort_index().rename(columns={"funding_rate": "binance_funding"})
    bn = bn.resample(freq).last().ffill()

    oi = pd.DataFrame(oi_docs)
    oi["ts"] = pd.to_datetime(oi["timestamp_utc"], unit="ms", utc=True)
    oi = oi.set_index("ts")[["oi_value"]].sort_index().resample(freq).last().ffill()

    liq = pd.DataFrame(liq_docs)
    liq["ts"] = pd.to_datetime(liq["timestamp_utc"], unit="ms", utc=True)
    liq = liq.set_index("ts")[["long_liquidations_usd", "short_liquidations_usd", "total_liquidations_usd"]].sort_index()
    liq = liq.resample(freq).last().fillna(0.0)

    df = px[["hl_open", "hl_high", "hl_low", "hl_close", "hl_vol"]].join(hlf, how="left")
    df = df.join(by, how="left").join(bn, how="left").join(oi, how="left").join(liq, how="left")
    df["hl_funding"] = df["hl_funding"].ffill()
    df["hl_premium"] = df["hl_premium"].ffill()
    df["bybit_funding"] = df["bybit_funding"].ffill()
    df["binance_funding"] = df["binance_funding"].ffill()

    # Core features
    df["ret_1h"] = df["hl_close"].pct_change(b(1))
    df["ret_4h"] = df["hl_close"].pct_change(b(4))
    df["ret_8h"] = df["hl_close"].pct_change(b(8))
    df["ret_24h"] = df["hl_close"].pct_change(b(24))
    df["ret_1h_z"] = rolling_z(df["ret_1h"], b(72), min_periods=max(10, b(24)))
    df["ret_8h_z"] = rolling_z(df["ret_8h"], b(96), min_periods=max(10, b(32)))

    df["hl_funding_z"] = rolling_z(df["hl_funding"], b(72), min_periods=max(10, b(24)))
    df["hl_premium_z"] = rolling_z(df["hl_premium"], b(72), min_periods=max(10, b(24)))

    df["hl_cum8"] = df["hl_funding"].rolling(b(8), min_periods=max(2, b(4))).sum()
    df["spread_hl_bybit"] = df["hl_cum8"] - df["bybit_funding"]
    df["spread_hl_cex"] = df["hl_cum8"] - (df["bybit_funding"] + df["binance_funding"]) / 2.0
    df["spread_hl_bybit_z"] = rolling_z(df["spread_hl_bybit"], b(90), min_periods=max(10, b(30)))
    df["spread_hl_cex_z"] = rolling_z(df["spread_hl_cex"], b(90), min_periods=max(10, b(30)))

    oi_log = np.log(df["oi_value"].replace(0, np.nan)).ffill()
    df["oi_delta"] = oi_log.diff(1)
    df["oi_z"] = rolling_z(df["oi_delta"], b(72), min_periods=max(10, b(24)))

    liq_total = df["total_liquidations_usd"].fillna(0.0)
    liq_long = df["long_liquidations_usd"].fillna(0.0)
    liq_short = df["short_liquidations_usd"].fillna(0.0)
    df["liq_total_z"] = rolling_z(np.log1p(liq_total), b(72), min_periods=max(10, b(24)))
    df["liq_imb"] = (liq_long - liq_short) / (liq_total + 1.0)
    df["liq_imb_z"] = rolling_z(df["liq_imb"], b(72), min_periods=max(10, b(24)))

    df["vol_z"] = rolling_z(np.log1p(df["hl_vol"].clip(lower=0.0)), b(96), min_periods=max(10, b(32)))

    df = df[(df.index >= start_ts) & (df.index <= end_ts)].copy()
    fill_cols = [
        "ret_1h_z",
        "ret_8h_z",
        "hl_funding_z",
        "hl_premium_z",
        "spread_hl_bybit_z",
        "spread_hl_cex_z",
        "oi_z",
        "liq_total_z",
        "liq_imb_z",
        "vol_z",
    ]
    for c in fill_cols:
        df[c] = df[c].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    df = df.dropna(subset=["hl_close", "hl_funding", "bybit_funding", "binance_funding", "oi_value"])
    if len(df) < 1500:
        return None
    df["pair"] = pair
    return df


def rank_pairs_by_liquidity(panels: Dict[str, pd.DataFrame], max_pairs: int) -> List[str]:
    rows = []
    for pair, df in panels.items():
        notional = (df["hl_close"] * df["hl_vol"]).replace([np.inf, -np.inf], np.nan).dropna()
        if len(notional) < 300:
            continue
        rows.append((pair, float(notional.median())))
    rows = sorted(rows, key=lambda x: x[1], reverse=True)
    return [r[0] for r in rows[:max_pairs]]


def build_signal(df: pd.DataFrame, cfg: HypothesisConfig) -> np.ndarray:
    spread_cex_z = df["spread_hl_cex_z"].to_numpy()
    spread_by_z = df["spread_hl_bybit_z"].to_numpy()
    hlfz = df["hl_funding_z"].to_numpy()
    hpz = df["hl_premium_z"].to_numpy()
    oiz = df["oi_z"].to_numpy()
    ltz = df["liq_total_z"].to_numpy()
    liz = df["liq_imb_z"].to_numpy()
    r1z = df["ret_1h_z"].to_numpy()
    r8z = df["ret_8h_z"].to_numpy()
    volz = df["vol_z"].to_numpy()

    sig = np.zeros(len(df), dtype=np.int8)
    z1, z2, z3 = cfg.z1, cfg.z2, cfg.z3
    fam = cfg.family

    if fam == "arb_hl_cex_carry_revert":
        cond = (np.abs(spread_cex_z) >= z1) & (np.abs(hlfz) >= z2)
        sig[cond] = -np.sign(spread_cex_z[cond]).astype(np.int8)

    elif fam == "arb_hl_bybit_spread_revert":
        cond = (np.abs(spread_by_z) >= z1) & (np.abs(hpz) >= z2)
        sig[cond] = -np.sign(spread_by_z[cond]).astype(np.int8)

    elif fam == "dir_crowding_revert":
        cond = (np.abs(hlfz) >= z1) & (oiz >= z2) & (np.sign(hlfz) == np.sign(r8z))
        sig[cond] = -np.sign(hlfz[cond]).astype(np.int8)

    elif fam == "dir_liq_exhaust_revert":
        cond = (ltz >= z1) & (np.abs(liz) >= z2) & (np.abs(hlfz) >= z3)
        sig[cond] = -np.sign(liz[cond]).astype(np.int8)

    elif fam == "dir_breakout_follow":
        cond = (oiz >= z1) & (np.abs(r8z) >= z2) & (volz >= z3)
        sig[cond] = np.sign(r8z[cond]).astype(np.int8)

    elif fam == "mm_skew_revert":
        cond = (np.abs(hpz) >= z1) & (ltz <= z2) & (volz <= z3)
        sig[cond] = -np.sign(hpz[cond]).astype(np.int8)

    elif fam == "mm_inventory_fade":
        cond = (np.abs(r1z) >= z1) & (np.abs(hlfz) >= z2) & (ltz <= z3)
        sig[cond] = -np.sign(r1z[cond]).astype(np.int8)

    elif fam == "any_composite_contra":
        score = 0.9 * spread_cex_z + 0.7 * hpz + 0.5 * liz + 0.4 * hlfz
        cond = (np.abs(score) >= z1) & (oiz >= z2)
        sig[cond] = -np.sign(score[cond]).astype(np.int8)

    else:
        raise ValueError(f"Unknown family: {fam}")

    sig[sig == -0] = 0
    return sig


def make_config_grid() -> List[HypothesisConfig]:
    out: List[HypothesisConfig] = []

    for fam in ["arb_hl_cex_carry_revert", "arb_hl_bybit_spread_revert"]:
        for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.5, 1.0, 1.5], [8, 12, 16]):
            out.append(HypothesisConfig(fam, "arb", z1, z2, 0.0, h))

    for fam in ["dir_crowding_revert"]:
        for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.0, 0.5, 1.0], [4, 8, 12]):
            out.append(HypothesisConfig(fam, "directional", z1, z2, 0.0, h))

    for fam in ["dir_liq_exhaust_revert"]:
        for z1, z2, z3, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.5, 1.0, 1.5], [0.5, 1.0, 1.5], [4, 8, 12]):
            out.append(HypothesisConfig(fam, "directional", z1, z2, z3, h))

    for fam in ["dir_breakout_follow"]:
        for z1, z2, z3, h in itertools.product([0.5, 1.0, 1.5, 2.0], [0.5, 1.0, 1.5], [0.0, 0.5, 1.0], [4, 8, 12]):
            out.append(HypothesisConfig(fam, "directional", z1, z2, z3, h))

    for fam in ["mm_skew_revert", "mm_inventory_fade"]:
        for z1, z2, z3, h in itertools.product([0.8, 1.0, 1.3, 1.6], [0.0, 0.5, 1.0], [0.5, 1.0, 1.5], [1, 2, 3]):
            out.append(HypothesisConfig(fam, "mm", z1, z2, z3, h))

    for fam in ["any_composite_contra"]:
        for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.0, 0.5, 1.0], [4, 8, 12]):
            out.append(HypothesisConfig(fam, "any", z1, z2, 0.0, h))

    return out


def eval_config_no_leakage(
    panels: Dict[str, pd.DataFrame],
    cfg: HypothesisConfig,
    interval_minutes: int,
    train_frac: float,
    cost_bps: float,
    min_train_trades: int,
    min_test_trades: int,
    min_selected_pairs: int,
) -> dict | None:
    tr_rows: List[EvalStats] = []
    te_rows: List[EvalStats] = []
    include_carry = cfg.strategy_type == "arb"
    hold_bars = bars_for_hours(cfg.hold_h, interval_minutes)

    for pair, df in panels.items():
        if len(df) < hold_bars + 140:
            continue
        sig = build_signal(df, cfg)
        cut = split_idx(len(df), train_frac)

        tr_close = df["hl_close"].to_numpy()[:cut]
        te_close = df["hl_close"].to_numpy()[cut:]
        tr_sig = sig[:cut]
        te_sig = sig[cut:]
        tr_by = df["bybit_funding"].to_numpy()[:cut]
        te_by = df["bybit_funding"].to_numpy()[cut:]

        tr = make_trades_from_signal(tr_close, tr_sig, hold_bars, cost_bps, bybit_funding=tr_by, include_carry=include_carry)
        te = make_trades_from_signal(te_close, te_sig, hold_bars, cost_bps, bybit_funding=te_by, include_carry=include_carry)
        if tr.size < 8 or te.size < min_test_trades:
            continue

        tr_rows.append(
            EvalStats(
                pair=pair,
                n_trades=int(tr.size),
                trade_bps_mean=float(np.mean(tr)),
                pf=compute_pf(tr),
                win_rate=float((tr > 0).mean()),
                total_bps=float(tr.sum()),
            )
        )
        te_rows.append(
            EvalStats(
                pair=pair,
                n_trades=int(te.size),
                trade_bps_mean=float(np.mean(te)),
                pf=compute_pf(te),
                win_rate=float((te > 0).mean()),
                total_bps=float(te.sum()),
            )
        )

    if not tr_rows:
        return None

    trdf = pd.DataFrame([asdict(x) for x in tr_rows])
    tedf = pd.DataFrame([asdict(x) for x in te_rows])

    selected = trdf[(trdf["trade_bps_mean"] > 0.0) & (trdf["pf"] >= 1.04) & (trdf["n_trades"] >= min_train_trades)]
    if len(selected) < min_selected_pairs:
        return None

    sel_pairs = set(selected["pair"])
    tr_sel = trdf[trdf["pair"].isin(sel_pairs)].copy()
    te_sel = tedf[tedf["pair"].isin(sel_pairs)].copy()
    if tr_sel.empty or te_sel.empty:
        return None

    tr_mean = float((tr_sel["trade_bps_mean"] * tr_sel["n_trades"]).sum() / tr_sel["n_trades"].sum())
    te_mean = float((te_sel["trade_bps_mean"] * te_sel["n_trades"]).sum() / te_sel["n_trades"].sum())
    tr_wr = float((tr_sel["win_rate"] * tr_sel["n_trades"]).sum() / tr_sel["n_trades"].sum())
    te_wr = float((te_sel["win_rate"] * te_sel["n_trades"]).sum() / te_sel["n_trades"].sum())
    total_test_bps = float(te_sel["total_bps"].sum())
    top_pair_share = float(abs(te_sel["total_bps"].max() / total_test_bps)) if total_test_bps != 0 else np.nan
    test_pf_median = float(te_sel["pf"].replace([np.inf, -np.inf], np.nan).dropna().median())
    test_pf_mean = float(te_sel["pf"].replace([np.inf, -np.inf], np.nan).dropna().mean())

    return {
        "strategy_type": cfg.strategy_type,
        "family": cfg.family,
        "z1": cfg.z1,
        "z2": cfg.z2,
        "z3": cfg.z3,
        "hold_h": cfg.hold_h,
        "n_pairs_evaluated": int(len(trdf)),
        "n_selected_pairs": int(len(sel_pairs)),
        "selected_pairs": ",".join(sorted(sel_pairs)),
        "train_trade_bps": tr_mean,
        "train_wr": tr_wr,
        "train_trades": int(tr_sel["n_trades"].sum()),
        "test_trade_bps": te_mean,
        "test_wr": te_wr,
        "test_trades": int(te_sel["n_trades"].sum()),
        "test_total_bps": total_test_bps,
        "test_positive_pair_share": float((te_sel["trade_bps_mean"] > 0).mean()),
        "test_pf_pair_median": test_pf_median,
        "test_pf_pair_mean": test_pf_mean,
        "top_pair_share": top_pair_share,
    }


def walk_forward_score(
    panels: Dict[str, pd.DataFrame],
    cfg: HypothesisConfig,
    selected_pairs: List[str],
    interval_minutes: int,
    cost_bps: float,
    train_days: int = 60,
    test_days: int = 30,
    step_days: int = 15,
) -> dict:
    pair_panels = [panels[p] for p in selected_pairs if p in panels]
    if not pair_panels:
        return {"wf_windows": 0, "wf_positive_share": 0.0, "wf_test_bps_mean": np.nan}

    t0 = min(df.index.min() for df in pair_panels)
    t1 = max(df.index.max() for df in pair_panels)
    if (t1 - t0) < pd.Timedelta(days=train_days + test_days + 5):
        return {"wf_windows": 0, "wf_positive_share": 0.0, "wf_test_bps_mean": np.nan}

    include_carry = cfg.strategy_type == "arb"
    hold_bars = bars_for_hours(cfg.hold_h, interval_minutes)
    bars_per_hour = max(1, 60 // interval_minutes)
    cur = t0
    wins = []
    while True:
        tr_end = cur + pd.Timedelta(days=train_days)
        te_end = tr_end + pd.Timedelta(days=test_days)
        if te_end > t1:
            break

        all_trades = []
        active_pairs = 0
        for p in selected_pairs:
            df = panels.get(p)
            if df is None:
                continue
            win = df[(df.index >= cur) & (df.index < te_end)]
            if len(win) < hold_bars + 80:
                continue
            active_pairs += 1
            sig = build_signal(win, cfg)
            split_point = int((tr_end - cur) / pd.Timedelta(hours=1)) * bars_per_hour
            split_point = max(40, min(len(win) - 40, split_point))
            te_close = win["hl_close"].to_numpy()[split_point:]
            te_sig = sig[split_point:]
            te_by = win["bybit_funding"].to_numpy()[split_point:]
            tr = make_trades_from_signal(
                te_close,
                te_sig,
                hold_bars,
                cost_bps,
                bybit_funding=te_by,
                include_carry=include_carry,
            )
            if tr.size > 0:
                all_trades.append(tr)

        if all_trades and active_pairs >= 2:
            cat = np.concatenate(all_trades)
            wins.append(float(np.mean(cat)))
        cur = cur + pd.Timedelta(days=step_days)

    if not wins:
        return {"wf_windows": 0, "wf_positive_share": 0.0, "wf_test_bps_mean": np.nan}

    arr = np.asarray(wins, dtype=float)
    return {
        "wf_windows": int(arr.size),
        "wf_positive_share": float((arr > 0).mean()),
        "wf_test_bps_mean": float(arr.mean()),
    }


def cost_sensitivity(
    panels: Dict[str, pd.DataFrame],
    cfg: HypothesisConfig,
    selected_pairs: List[str],
    interval_minutes: int,
    train_frac: float,
    costs: List[float],
) -> dict:
    include_carry = cfg.strategy_type == "arb"
    hold_bars = bars_for_hours(cfg.hold_h, interval_minutes)
    out = {}
    for c in costs:
        total_ret = 0.0
        total_n = 0
        for p in selected_pairs:
            df = panels.get(p)
            if df is None or len(df) < hold_bars + 120:
                continue
            sig = build_signal(df, cfg)
            cut = split_idx(len(df), train_frac)
            te_close = df["hl_close"].to_numpy()[cut:]
            te_sig = sig[cut:]
            te_by = df["bybit_funding"].to_numpy()[cut:]
            tr = make_trades_from_signal(te_close, te_sig, hold_bars, c, bybit_funding=te_by, include_carry=include_carry)
            if tr.size:
                total_ret += float(tr.sum())
                total_n += int(tr.size)
        out[f"test_trade_bps_cost_{int(c)}"] = (total_ret / total_n) if total_n > 0 else np.nan
        out[f"test_trades_cost_{int(c)}"] = total_n
    return out


def run_feature_ic_eda(panels: Dict[str, pd.DataFrame], interval_minutes: int) -> pd.DataFrame:
    feature_cols = [
        "spread_hl_cex_z",
        "spread_hl_bybit_z",
        "hl_funding_z",
        "hl_premium_z",
        "oi_z",
        "liq_total_z",
        "liq_imb_z",
        "vol_z",
    ]
    horizons = [1, 4, 8]
    rows = []
    for pair, df in panels.items():
        local = df.copy()
        for h in horizons:
            local[f"fwd_ret_{h}h"] = local["hl_close"].shift(-bars_for_hours(h, interval_minutes)) / local["hl_close"] - 1.0
        for f in feature_cols:
            for h in horizons:
                y = local[f"fwd_ret_{h}h"]
                x = local[f]
                idx = x.notna() & y.notna()
                if idx.sum() < 200:
                    continue
                corr = float(x[idx].corr(y[idx]))
                rows.append({"pair": pair, "feature": f, "horizon_h": h, "ic": corr, "n": int(idx.sum())})
    if not rows:
        return pd.DataFrame()
    raw = pd.DataFrame(rows)
    agg = (
        raw.groupby(["feature", "horizon_h"], as_index=False)
        .agg(ic_mean=("ic", "mean"), ic_median=("ic", "median"), ic_std=("ic", "std"), n_pairs=("pair", "nunique"))
        .sort_values(["horizon_h", "ic_mean"], ascending=[True, False])
        .reset_index(drop=True)
    )
    return agg


def run_quantile_markout_eda(panels: Dict[str, pd.DataFrame], interval_minutes: int) -> pd.DataFrame:
    features = ["spread_hl_cex_z", "hl_premium_z", "hl_funding_z", "liq_imb_z"]
    frames = []
    for pair, df in panels.items():
        tmp = df.copy()
        tmp["pair"] = pair
        tmp["fwd_ret_1h_bps"] = (tmp["hl_close"].shift(-bars_for_hours(1, interval_minutes)) / tmp["hl_close"] - 1.0) * 1e4
        tmp["fwd_ret_4h_bps"] = (tmp["hl_close"].shift(-bars_for_hours(4, interval_minutes)) / tmp["hl_close"] - 1.0) * 1e4
        frames.append(tmp[["pair", "fwd_ret_1h_bps", "fwd_ret_4h_bps"] + features])
    if not frames:
        return pd.DataFrame()
    all_df = pd.concat(frames, ignore_index=True)
    rows = []
    for f in features:
        x = all_df[f].replace([np.inf, -np.inf], np.nan)
        y1 = all_df["fwd_ret_1h_bps"]
        y4 = all_df["fwd_ret_4h_bps"]
        valid = x.notna() & y1.notna() & y4.notna()
        if valid.sum() < 500:
            continue
        bucket = pd.qcut(x[valid], q=10, labels=False, duplicates="drop")
        grp = pd.DataFrame(
            {
                "bucket": bucket.astype(int),
                "fwd_ret_1h_bps": y1[valid].values,
                "fwd_ret_4h_bps": y4[valid].values,
            }
        ).groupby("bucket", as_index=False).agg(
            mean_1h_bps=("fwd_ret_1h_bps", "mean"),
            mean_4h_bps=("fwd_ret_4h_bps", "mean"),
            n=("fwd_ret_1h_bps", "size"),
        )
        grp["feature"] = f
        rows.append(grp)
    if not rows:
        return pd.DataFrame()
    return pd.concat(rows, ignore_index=True)[["feature", "bucket", "mean_1h_bps", "mean_4h_bps", "n"]]


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    candles_dir = Path(args.candles_dir)
    interval_minutes = interval_to_minutes(args.interval)

    start_ts = pd.Timestamp(args.start, tz="UTC")
    end_ts = pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(hours=23)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    client = MongoClient(args.mongo_uri)
    db = client[_db_name_from_uri(args.mongo_uri)]

    print(f"[1/7] Build Hyperliquid universe ({args.interval})")
    universe = build_universe(db, candles_dir, interval=args.interval)
    print(f"  base universe: {len(universe)} pairs")
    if not universe:
        raise SystemExit("No universe available. Ensure Hyperliquid candle parquet + Mongo data exist.")

    print("[2/7] Load pair panels")
    panels: Dict[str, pd.DataFrame] = {}
    for p in universe:
        panel = load_pair_panel(p, db, candles_dir, start_ts, end_ts, interval=args.interval)
        if panel is not None and len(panel) >= args.min_bars:
            panels[p] = panel
    print(f"  usable panels: {len(panels)}")
    if not panels:
        raise SystemExit("No usable panels.")

    keep = rank_pairs_by_liquidity(panels, args.max_pairs)
    panels = {p: panels[p] for p in keep if p in panels}
    print(f"  final research universe: {len(panels)}")

    print("[3/7] Run feature EDA (IC + quantile markouts)")
    ic_df = run_feature_ic_eda(panels, interval_minutes=interval_minutes)
    q_df = run_quantile_markout_eda(panels, interval_minutes=interval_minutes)

    print("[4/7] Build hypothesis grid")
    cfgs = make_config_grid()
    print(f"  configs: {len(cfgs)}")

    print("[5/7] Run no-leakage sweep")
    rows = []
    for i, cfg in enumerate(cfgs, start=1):
        res = eval_config_no_leakage(
            panels=panels,
            cfg=cfg,
            interval_minutes=interval_minutes,
            train_frac=args.train_frac,
            cost_bps=args.cost_bps_oneway,
            min_train_trades=args.min_train_trades,
            min_test_trades=args.min_test_trades,
            min_selected_pairs=args.min_selected_pairs,
        )
        if res is not None:
            rows.append(res)
        if i % 100 == 0:
            print(f"  processed {i}/{len(cfgs)} configs, passing={len(rows)}")
    if not rows:
        raise SystemExit("No passing configs.")

    df = pd.DataFrame(rows)
    # Composite rank: emphasize OOS edge + diversification + robustness
    df["rank_score"] = (
        2.0 * df["test_trade_bps"].fillna(-1e9)
        + 18.0 * df["test_pf_pair_median"].fillna(-1e9)
        + 5.0 * df["test_positive_pair_share"].fillna(0.0)
        - 10.0 * df["top_pair_share"].fillna(1.0)
    )
    df = df.sort_values("rank_score", ascending=False).reset_index(drop=True)

    raw_path = out_dir / f"{stamp}_x11_raw.csv"
    df.to_csv(raw_path, index=False)

    print("[6/7] Walk-forward + cost checks on top configs")
    enriched = []
    for _, r in df.head(min(args.top_k_wf, len(df))).iterrows():
        cfg = HypothesisConfig(
            family=str(r["family"]),
            strategy_type=str(r["strategy_type"]),
            z1=float(r["z1"]),
            z2=float(r["z2"]),
            z3=float(r["z3"]),
            hold_h=int(r["hold_h"]),
        )
        pairs = [x for x in str(r["selected_pairs"]).split(",") if x]
        wf = walk_forward_score(
            panels=panels,
            cfg=cfg,
            selected_pairs=pairs,
            interval_minutes=interval_minutes,
            cost_bps=args.cost_bps_oneway,
            train_days=60,
            test_days=30,
            step_days=15,
        )
        cs = cost_sensitivity(
            panels=panels,
            cfg=cfg,
            selected_pairs=pairs,
            interval_minutes=interval_minutes,
            train_frac=args.train_frac,
            costs=[3.0, 4.0, 6.0],
        )
        row = r.to_dict()
        row.update(wf)
        row.update(cs)
        enriched.append(row)
    top_df = pd.DataFrame(enriched)

    # Candidate thresholds
    cand = top_df[
        (top_df["test_trade_bps"] > 2.0)
        & (top_df["test_pf_pair_median"] >= 1.08)
        & (top_df["test_positive_pair_share"] >= 0.52)
        & (top_df["top_pair_share"] <= 0.72)
        & (top_df["wf_windows"] >= 4)
        & (top_df["wf_positive_share"] >= 0.50)
        & (top_df["test_trade_bps_cost_6"] > -0.5)
    ].copy()
    if not cand.empty:
        cand = cand.sort_values(
            ["rank_score", "test_trade_bps", "wf_positive_share"],
            ascending=[False, False, False],
        )

    top_path = out_dir / f"{stamp}_x11_top_enriched.csv"
    cand_path = out_dir / f"{stamp}_x11_candidates.csv"
    ic_path = out_dir / f"{stamp}_x11_feature_ic.csv"
    q_path = out_dir / f"{stamp}_x11_quantile_markouts.csv"
    meta_path = out_dir / f"{stamp}_x11_meta.json"

    top_df.to_csv(top_path, index=False)
    cand.to_csv(cand_path, index=False)
    ic_df.to_csv(ic_path, index=False)
    q_df.to_csv(q_path, index=False)

    with meta_path.open("w") as f:
        json.dump(
            {
                "timestamp_utc": stamp,
                "start": args.start,
                "end": args.end,
                "interval": args.interval,
                "train_frac": args.train_frac,
                "cost_bps_oneway": args.cost_bps_oneway,
                "n_universe_pairs": int(len(panels)),
                "universe_pairs": sorted(panels.keys()),
                "n_configs": int(len(cfgs)),
                "n_pass_initial": int(len(df)),
                "n_top_enriched": int(len(top_df)),
                "n_candidates": int(len(cand)),
                "strategy_type_counts": (
                    cand["strategy_type"].value_counts().to_dict() if not cand.empty else {}
                ),
            },
            f,
            indent=2,
        )

    print("[7/7] Done")
    print("\n=== SUMMARY ===")
    print(f"Passing configs: {len(df)}")
    print(f"Top enriched: {len(top_df)}")
    print(f"Candidates: {len(cand)}")
    if not cand.empty:
        cols = [
            "strategy_type",
            "family",
            "z1",
            "z2",
            "z3",
            "hold_h",
            "n_selected_pairs",
            "test_trade_bps",
            "test_pf_pair_median",
            "test_positive_pair_share",
            "top_pair_share",
            "wf_positive_share",
            "test_trade_bps_cost_6",
        ]
        print("\nTop candidates:")
        print(cand[cols].head(20).to_string(index=False))
    print("\nArtifacts:")
    print(raw_path)
    print(top_path)
    print(cand_path)
    print(ic_path)
    print(q_path)
    print(meta_path)


if __name__ == "__main__":
    main()
