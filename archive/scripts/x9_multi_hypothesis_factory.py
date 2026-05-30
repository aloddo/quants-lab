#!/usr/bin/env python3
"""
X9 Multi-Hypothesis Factory (fresh discovery pass)

Purpose:
  Run a broad, no-leakage hypothesis sweep over hourly crypto derivatives features
  to surface strategy candidates for deeper research.

Data used (local):
  - Bybit perp 1h candles (price/volume)
  - Binance spot 1h candles (for perp-spot premium)
  - Bybit funding rates
  - Binance funding rates
  - Bybit open interest
  - Coinalyze liquidations (1h)
  - Fear & Greed index

Method:
  1) Build per-pair 1h feature panels for a common time range.
  2) Evaluate hundreds of strategy configs across multiple families.
  3) Select pairs on TRAIN only (no leakage).
  4) Score on TEST and rank by OOS quality.
  5) Run walk-forward + cost sensitivity on top candidates.
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from pymongo import MongoClient
from scipy import stats


# -------------------- Config dataclasses --------------------


@dataclass(frozen=True)
class HypothesisConfig:
    family: str
    z1: float
    z2: float
    z3: float
    hold_hours: int


@dataclass
class EvalStats:
    pair: str
    n_trades: int
    trade_bps_mean: float
    pf: float
    win_rate: float
    total_bps: float


# -------------------- Utilities --------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="X9 multi-hypothesis discovery")
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab"))
    p.add_argument("--candles-dir", default="app/data/cache/candles")
    p.add_argument("--out-dir", default="app/data/cache/x9_hypothesis_factory")
    p.add_argument("--start", default="2025-04-21", help="UTC date inclusive (YYYY-MM-DD)")
    p.add_argument("--end", default="2026-04-21", help="UTC date inclusive (YYYY-MM-DD)")
    p.add_argument("--train-frac", type=float, default=0.65)
    p.add_argument("--cost-bps-oneway", type=float, default=4.0)
    p.add_argument("--max-pairs", type=int, default=36)
    p.add_argument("--min-bars", type=int, default=2000)
    p.add_argument("--min-train-trades", type=int, default=20)
    p.add_argument("--min-test-trades", type=int, default=8)
    p.add_argument("--min-selected-pairs", type=int, default=4)
    p.add_argument("--top-k-wf", type=int, default=60)
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


def compute_pf(arr: np.ndarray) -> float:
    if arr.size == 0:
        return np.nan
    pos = arr[arr > 0].sum()
    neg = -arr[arr < 0].sum()
    if neg <= 0:
        return np.nan
    return float(pos / neg)


def ttest_p(arr: np.ndarray) -> float:
    if arr.size < 12:
        return float("nan")
    return float(stats.ttest_1samp(arr, 0.0, nan_policy="omit").pvalue)


def compute_streak_from_events(event_values: pd.Series) -> pd.Series:
    streak = []
    cur = 0
    prev = 0
    for v in event_values.astype(float).fillna(0.0).to_numpy():
        s = int(np.sign(v))
        if s == 0:
            cur = 0
        elif s == prev:
            cur += 1
        else:
            cur = 1
        streak.append(cur)
        if s != 0:
            prev = s
    return pd.Series(streak, index=event_values.index, dtype=float)


def make_trades_from_signal(close: np.ndarray, sig: np.ndarray, hold: int, cost_bps_oneway: float) -> np.ndarray:
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
        out.append(float(ret))
        i = exit_i + 1
    return np.asarray(out, dtype=float)


# -------------------- Data loading --------------------


def build_base_universe(db, candles_dir: Path) -> List[str]:
    bybit = {p.name.split("|")[1] for p in candles_dir.glob("bybit_perpetual|*-USDT|1h.parquet")}
    bin_spot = {p.name.split("|")[1] for p in candles_dir.glob("binance_spot|*-USDT|1h.parquet")}
    bybit_f = set(db["bybit_funding_rates"].distinct("pair"))
    bybit_oi = set(db["bybit_open_interest"].distinct("pair"))
    liq = set(db["coinalyze_liquidations"].distinct("pair"))
    bin_f = set(db["binance_funding_rates"].distinct("pair"))
    rich = sorted(bybit & bin_spot & bybit_f & bybit_oi & liq & bin_f)
    return rich


def load_fear_greed_series(db, start_ms: int, end_ms: int) -> pd.Series:
    docs = list(
        db["fear_greed_index"]
        .find({"timestamp_utc": {"$gte": start_ms - 40 * 24 * 3600 * 1000, "$lte": end_ms}}, {"_id": 0, "timestamp_utc": 1, "value": 1})
        .sort("timestamp_utc", 1)
    )
    if not docs:
        return pd.Series(dtype=float)
    df = pd.DataFrame(docs)
    df["ts"] = pd.to_datetime(df["timestamp_utc"], unit="ms", utc=True)
    s = df.set_index("ts")["value"].astype(float).sort_index()
    # Normalize to [-1, +1]
    return (s - 50.0) / 50.0


def load_pair_panel(
    pair: str,
    db,
    candles_dir: Path,
    start_ts: pd.Timestamp,
    end_ts: pd.Timestamp,
    fg_series: pd.Series,
) -> pd.DataFrame | None:
    bybit_path = candles_dir / f"bybit_perpetual|{pair}|1h.parquet"
    bin_spot_path = candles_dir / f"binance_spot|{pair}|1h.parquet"
    if not bybit_path.exists() or not bin_spot_path.exists():
        return None

    warmup_start = start_ts - pd.Timedelta(days=45)
    start_s = int(warmup_start.timestamp())
    end_s = int(end_ts.timestamp())
    start_ms = int(warmup_start.timestamp() * 1000)
    end_ms = int(end_ts.timestamp() * 1000)

    try:
        perp = pd.read_parquet(bybit_path, columns=["timestamp", "close", "volume"])  # type: ignore[arg-type]
        spot = pd.read_parquet(bin_spot_path, columns=["timestamp", "close", "volume"])  # type: ignore[arg-type]
    except Exception:
        return None

    perp = perp[(perp["timestamp"] >= start_s) & (perp["timestamp"] <= end_s)].copy()
    spot = spot[(spot["timestamp"] >= start_s) & (spot["timestamp"] <= end_s)].copy()
    if len(perp) < 1800 or len(spot) < 1800:
        return None

    perp["ts"] = pd.to_datetime(perp["timestamp"], unit="s", utc=True)
    spot["ts"] = pd.to_datetime(spot["timestamp"], unit="s", utc=True)
    perp = perp.set_index("ts").sort_index().rename(columns={"close": "perp_close", "volume": "perp_vol"})
    spot = spot.set_index("ts").sort_index().rename(columns={"close": "spot_close", "volume": "spot_vol"})

    # Bybit funding (event-level)
    f_by = list(
        db["bybit_funding_rates"]
        .find({"pair": pair, "timestamp_utc": {"$gte": start_ms, "$lte": end_ms}}, {"_id": 0, "timestamp_utc": 1, "funding_rate": 1})
        .sort("timestamp_utc", 1)
    )
    if len(f_by) < 100:
        return None
    f_by_df = pd.DataFrame(f_by)
    f_by_df["ts"] = pd.to_datetime(f_by_df["timestamp_utc"], unit="ms", utc=True)
    f_by_df = f_by_df.set_index("ts")[["funding_rate"]].sort_index().rename(columns={"funding_rate": "bybit_funding"})

    # Binance funding (event-level)
    f_bn = list(
        db["binance_funding_rates"]
        .find({"pair": pair, "timestamp_utc": {"$gte": start_ms, "$lte": end_ms}}, {"_id": 0, "timestamp_utc": 1, "funding_rate": 1})
        .sort("timestamp_utc", 1)
    )
    if len(f_bn) < 100:
        return None
    f_bn_df = pd.DataFrame(f_bn)
    f_bn_df["ts"] = pd.to_datetime(f_bn_df["timestamp_utc"], unit="ms", utc=True)
    f_bn_df = f_bn_df.set_index("ts")[["funding_rate"]].sort_index().rename(columns={"funding_rate": "binance_funding"})

    # Bybit OI (1h-ish)
    oi_docs = list(
        db["bybit_open_interest"]
        .find({"pair": pair, "timestamp_utc": {"$gte": start_ms, "$lte": end_ms}}, {"_id": 0, "timestamp_utc": 1, "oi_value": 1})
        .sort("timestamp_utc", 1)
    )
    if len(oi_docs) < 500:
        return None
    oi_df = pd.DataFrame(oi_docs)
    oi_df["ts"] = pd.to_datetime(oi_df["timestamp_utc"], unit="ms", utc=True)
    oi_df = oi_df.set_index("ts")[["oi_value"]].sort_index().resample("1h").last().ffill()

    # Coinalyze liquidations (1h)
    liq_docs = list(
        db["coinalyze_liquidations"]
        .find({"pair": pair, "timestamp_utc": {"$gte": start_ms, "$lte": end_ms}},
              {"_id": 0, "timestamp_utc": 1, "long_liquidations_usd": 1, "short_liquidations_usd": 1, "total_liquidations_usd": 1})
        .sort("timestamp_utc", 1)
    )
    if len(liq_docs) < 500:
        return None
    liq = pd.DataFrame(liq_docs)
    liq["ts"] = pd.to_datetime(liq["timestamp_utc"], unit="ms", utc=True)
    liq = liq.set_index("ts")[["long_liquidations_usd", "short_liquidations_usd", "total_liquidations_usd"]].sort_index()
    liq = liq.resample("1h").last().fillna(0.0)

    # Master index from perp bars
    df = perp[["perp_close", "perp_vol"]].join(spot[["spot_close", "spot_vol"]], how="inner")

    # Merge event-level funding onto hourly bars (resample avoids timestamp alignment drift)
    fr = pd.DataFrame(index=df.index)
    by_1h = f_by_df.resample("1h").last().ffill()
    bn_1h = f_bn_df.resample("1h").last().ffill()
    fr["bybit_funding"] = by_1h["bybit_funding"].reindex(fr.index, method="ffill")
    fr["binance_funding"] = bn_1h["binance_funding"].reindex(fr.index, method="ffill")

    # Event features from bybit funding events
    by_event = f_by_df.copy()
    by_event["fr_z_event"] = rolling_z(by_event["bybit_funding"], 30, min_periods=15)
    by_event["fr_streak_event"] = compute_streak_from_events(np.sign(by_event["bybit_funding"]))

    fr["fr_z"] = np.nan
    fr["fr_streak"] = np.nan
    aligned = by_event[["fr_z_event", "fr_streak_event"]].reindex(fr.index, method="ffill")
    fr["fr_z"] = aligned["fr_z_event"]
    fr["fr_streak"] = aligned["fr_streak_event"]

    df = df.join(fr, how="left")
    df = df.join(oi_df, how="left")
    df = df.join(liq, how="left")

    # Fear-greed (daily) -> hourly ffill
    if not fg_series.empty:
        fg_hourly = fg_series.reindex(df.index, method="ffill")
        df["fg_norm"] = fg_hourly
    else:
        df["fg_norm"] = 0.0

    # Core features
    df["ret_1h"] = df["perp_close"].pct_change(1)
    df["ret_4h"] = df["perp_close"].pct_change(4)
    df["ret_8h"] = df["perp_close"].pct_change(8)
    df["ret_24h"] = df["perp_close"].pct_change(24)
    df["ret_8h_z"] = rolling_z(df["ret_8h"], 72, min_periods=24)

    df["premium"] = (df["perp_close"] / df["spot_close"]) - 1.0
    df["premium_z"] = rolling_z(df["premium"], 72, min_periods=24)

    df["funding_spread"] = df["bybit_funding"] - df["binance_funding"]
    df["funding_spread_z"] = rolling_z(df["funding_spread"], 60, min_periods=20)

    oi_log = np.log(df["oi_value"].replace(0, np.nan)).ffill()
    df["oi_delta"] = oi_log.diff(1)
    df["oi_z"] = rolling_z(df["oi_delta"], 72, min_periods=24)

    liq_total = df["total_liquidations_usd"].fillna(0.0)
    liq_long = df["long_liquidations_usd"].fillna(0.0)
    liq_short = df["short_liquidations_usd"].fillna(0.0)
    df["liq_total_z"] = rolling_z(np.log1p(liq_total), 72, min_periods=24)
    df["liq_imb"] = (liq_long - liq_short) / (liq_total + 1.0)
    df["liq_imb_z"] = rolling_z(df["liq_imb"], 72, min_periods=24)

    df["vol_z"] = rolling_z(np.log1p(df["perp_vol"].clip(lower=0.0)), 96, min_periods=32)

    df = df[(df.index >= start_ts) & (df.index <= end_ts)].copy()
    # Neutral-fill sparse z-score warmup / flat-variance gaps.
    for col in ["fr_z", "ret_8h_z", "premium_z", "funding_spread_z", "oi_z", "liq_total_z", "liq_imb_z", "vol_z", "fg_norm"]:
        df[col] = df[col].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df["fr_streak"] = df["fr_streak"].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    df = df.dropna(subset=[
        "perp_close", "spot_close", "bybit_funding", "binance_funding", "oi_value"
    ])

    if len(df) < 1800:
        return None

    df["pair"] = pair
    return df


def rank_pairs_by_liquidity(panels: Dict[str, pd.DataFrame], max_pairs: int) -> List[str]:
    rows = []
    for pair, df in panels.items():
        notional = (df["perp_close"] * df["perp_vol"]).replace([np.inf, -np.inf], np.nan).dropna()
        if len(notional) < 300:
            continue
        rows.append((pair, float(notional.median()), float(notional.mean())))
    ranked = sorted(rows, key=lambda x: x[1], reverse=True)
    keep = [r[0] for r in ranked[:max_pairs]]
    return keep


# -------------------- Signal families --------------------


def build_signal(df: pd.DataFrame, cfg: HypothesisConfig) -> np.ndarray:
    fr = df["bybit_funding"].to_numpy()
    frz = df["fr_z"].to_numpy()
    streak = df["fr_streak"].to_numpy()
    oiz = df["oi_z"].to_numpy()
    pz = df["premium_z"].to_numpy()
    fsz = df["funding_spread_z"].to_numpy()
    ltotz = df["liq_total_z"].to_numpy()
    limbz = df["liq_imb_z"].to_numpy()
    r8z = df["ret_8h_z"].to_numpy()
    volz = df["vol_z"].to_numpy()
    fg = df["fg_norm"].to_numpy()

    n = len(df)
    sig = np.zeros(n, dtype=np.int8)

    fam = cfg.family
    z1, z2, z3 = cfg.z1, cfg.z2, cfg.z3

    if fam == "funding_revert":
        cond = (np.abs(frz) >= z1) & (streak >= 2) & (oiz >= z2)
        sig[cond] = -np.sign(fr[cond]).astype(np.int8)

    elif fam == "funding_cont":
        cond = (np.abs(frz) >= z1) & (streak >= 2) & (oiz >= z2)
        sig[cond] = np.sign(fr[cond]).astype(np.int8)

    elif fam == "premium_revert":
        cond = (np.abs(pz) >= z1) & (ltotz <= z2)
        sig[cond] = -np.sign(pz[cond]).astype(np.int8)

    elif fam == "premium_cont":
        cond = (np.abs(pz) >= z1) & (np.abs(frz) >= z2) & (np.sign(pz) == np.sign(frz))
        sig[cond] = np.sign(pz[cond]).astype(np.int8)

    elif fam == "liq_exhaust_revert":
        cond = (ltotz >= z1) & (np.abs(limbz) >= z2)
        sig[cond] = -np.sign(limbz[cond]).astype(np.int8)

    elif fam == "liq_follow":
        cond = (ltotz >= z1) & (np.abs(limbz) >= z2) & (np.sign(limbz) == np.sign(r8z))
        sig[cond] = np.sign(limbz[cond]).astype(np.int8)

    elif fam == "funding_spread_revert":
        cond = (np.abs(fsz) >= z1) & (np.abs(pz) >= z2)
        sig[cond] = -np.sign(fsz[cond]).astype(np.int8)

    elif fam == "funding_spread_cont":
        cond = (np.abs(fsz) >= z1) & (np.abs(pz) >= z2) & (np.sign(fsz) == np.sign(pz))
        sig[cond] = np.sign(fsz[cond]).astype(np.int8)

    elif fam == "oi_price_div_revert":
        cond = (oiz >= z1) & (np.abs(r8z) >= z2)
        sig[cond] = -np.sign(r8z[cond]).astype(np.int8)

    elif fam == "oi_breakout_follow":
        cond = (oiz >= z1) & (np.abs(r8z) >= z2) & (volz >= z3)
        sig[cond] = np.sign(r8z[cond]).astype(np.int8)

    elif fam == "sentiment_contra":
        cond = (np.abs(fg) >= z1) & (np.abs(frz) >= z2)
        # high greed -> short bias, high fear -> long bias
        sig[cond] = -np.sign(fg[cond]).astype(np.int8)

    elif fam == "composite_contra":
        score = frz + 0.7 * pz + 0.5 * limbz
        cond = (np.abs(score) >= z1) & (oiz >= z2) & (np.abs(fsz) >= z3)
        sig[cond] = -np.sign(score[cond]).astype(np.int8)

    else:
        raise ValueError(f"Unknown family: {fam}")

    sig[sig == -0] = 0
    return sig


def make_config_grid() -> List[HypothesisConfig]:
    cfgs: List[HypothesisConfig] = []

    for fam in ["funding_revert", "funding_cont"]:
        for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [-0.5, 0.0, 0.5, 1.0], [4, 6, 8, 12]):
            cfgs.append(HypothesisConfig(fam, z1, z2, 0.0, h))

    for fam in ["premium_revert", "premium_cont"]:
        for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.0, 0.5, 1.0], [2, 4, 6, 8]):
            cfgs.append(HypothesisConfig(fam, z1, z2, 0.0, h))

    for fam in ["liq_exhaust_revert", "liq_follow"]:
        for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5, 3.0], [0.5, 1.0, 1.5], [2, 4, 6, 8, 12]):
            cfgs.append(HypothesisConfig(fam, z1, z2, 0.0, h))

    for fam in ["funding_spread_revert", "funding_spread_cont"]:
        for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.5, 1.0, 1.5], [2, 4, 6, 8]):
            cfgs.append(HypothesisConfig(fam, z1, z2, 0.0, h))

    for fam in ["oi_price_div_revert"]:
        for z1, z2, h in itertools.product([0.5, 1.0, 1.5, 2.0], [0.5, 1.0, 1.5], [3, 6, 9, 12]):
            cfgs.append(HypothesisConfig(fam, z1, z2, 0.0, h))

    for fam in ["oi_breakout_follow"]:
        for z1, z2, z3, h in itertools.product([0.5, 1.0, 1.5, 2.0], [0.5, 1.0, 1.5], [0.0, 0.5, 1.0], [3, 6, 9, 12]):
            cfgs.append(HypothesisConfig(fam, z1, z2, z3, h))

    for fam in ["sentiment_contra"]:
        for z1, z2, h in itertools.product([0.3, 0.5, 0.7, 0.9], [1.0, 1.5, 2.0], [4, 8, 12, 24]):
            cfgs.append(HypothesisConfig(fam, z1, z2, 0.0, h))

    for fam in ["composite_contra"]:
        for z1, z2, z3, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.0, 0.5, 1.0], [0.5, 1.0, 1.5], [4, 8, 12]):
            cfgs.append(HypothesisConfig(fam, z1, z2, z3, h))

    return cfgs


# -------------------- Evaluation --------------------


def split_idx(n: int, train_frac: float) -> int:
    return int(max(50, min(n - 50, round(n * train_frac))))


def eval_config_no_leakage(
    panels: Dict[str, pd.DataFrame],
    cfg: HypothesisConfig,
    train_frac: float,
    cost_bps: float,
    min_train_trades: int,
    min_test_trades: int,
    min_selected_pairs: int,
) -> dict | None:
    pair_train: List[EvalStats] = []
    pair_test: List[EvalStats] = []

    for pair, df in panels.items():
        n = len(df)
        if n < cfg.hold_hours + 120:
            continue
        sig = build_signal(df, cfg)
        cut = split_idx(n, train_frac)

        tr_close = df["perp_close"].to_numpy()[:cut]
        te_close = df["perp_close"].to_numpy()[cut:]
        tr_sig = sig[:cut]
        te_sig = sig[cut:]

        tr = make_trades_from_signal(tr_close, tr_sig, cfg.hold_hours, cost_bps)
        te = make_trades_from_signal(te_close, te_sig, cfg.hold_hours, cost_bps)

        if tr.size < 8 or te.size < min_test_trades:
            continue

        pair_train.append(
            EvalStats(
                pair=pair,
                n_trades=int(tr.size),
                trade_bps_mean=float(np.mean(tr)),
                pf=compute_pf(tr),
                win_rate=float((tr > 0).mean()),
                total_bps=float(tr.sum()),
            )
        )
        pair_test.append(
            EvalStats(
                pair=pair,
                n_trades=int(te.size),
                trade_bps_mean=float(np.mean(te)),
                pf=compute_pf(te),
                win_rate=float((te > 0).mean()),
                total_bps=float(te.sum()),
            )
        )

    if not pair_train:
        return None

    train_df = pd.DataFrame([asdict(x) for x in pair_train])
    test_df = pd.DataFrame([asdict(x) for x in pair_test])

    # Pair selection on train only
    selected = train_df[(train_df["trade_bps_mean"] > 0.0) & (train_df["pf"] >= 1.05) & (train_df["n_trades"] >= min_train_trades)]
    if len(selected) < min_selected_pairs:
        return None

    sel_pairs = set(selected["pair"])
    te_sel = test_df[test_df["pair"].isin(sel_pairs)].copy()
    tr_sel = train_df[train_df["pair"].isin(sel_pairs)].copy()

    if te_sel.empty or tr_sel.empty:
        return None

    # Recompute aggregate PF from pooled per-pair mean*count approximation
    # (exact pooled trade-level PF would require carrying all trades; this is a ranking pass)
    # For ranking robustness, also require strong pair-wise PF share.
    test_mean = float((te_sel["trade_bps_mean"] * te_sel["n_trades"]).sum() / te_sel["n_trades"].sum())
    train_mean = float((tr_sel["trade_bps_mean"] * tr_sel["n_trades"]).sum() / tr_sel["n_trades"].sum())
    test_wr = float((te_sel["win_rate"] * te_sel["n_trades"]).sum() / te_sel["n_trades"].sum())
    train_wr = float((tr_sel["win_rate"] * tr_sel["n_trades"]).sum() / tr_sel["n_trades"].sum())

    # Pair-level concentration proxy
    total_test_bps = float(te_sel["total_bps"].sum())
    if total_test_bps == 0:
        top_pair_share = float("nan")
    else:
        top_pair_share = float(abs(te_sel["total_bps"].max() / total_test_bps))

    # Pair-wise PF quality proxy
    test_pf_median = float(te_sel["pf"].replace([np.inf, -np.inf], np.nan).dropna().median()) if not te_sel.empty else np.nan
    test_pf_mean = float(te_sel["pf"].replace([np.inf, -np.inf], np.nan).dropna().mean()) if not te_sel.empty else np.nan

    return {
        "family": cfg.family,
        "z1": cfg.z1,
        "z2": cfg.z2,
        "z3": cfg.z3,
        "hold_hours": cfg.hold_hours,
        "n_pairs_evaluated": int(len(train_df)),
        "n_selected_pairs": int(len(sel_pairs)),
        "selected_pairs": ",".join(sorted(sel_pairs)),
        "train_trade_bps": train_mean,
        "train_wr": train_wr,
        "train_trades": int(tr_sel["n_trades"].sum()),
        "test_trade_bps": test_mean,
        "test_wr": test_wr,
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
    cost_bps: float,
    train_days: int = 60,
    test_days: int = 30,
    step_days: int = 15,
) -> dict:
    # Use global index range; require enough active pairs per window.
    pair_panels = [panels[p] for p in selected_pairs if p in panels]
    if not pair_panels:
        return {"wf_windows": 0, "wf_positive_share": 0.0, "wf_test_bps_mean": np.nan}

    t0 = min(df.index.min() for df in pair_panels)
    t1 = max(df.index.max() for df in pair_panels)
    if (t1 - t0) < pd.Timedelta(days=train_days + test_days + 5):
        return {"wf_windows": 0, "wf_positive_share": 0.0, "wf_test_bps_mean": np.nan}

    cur = t0
    wins = []
    while True:
        tr_end = cur + pd.Timedelta(days=train_days)
        te_end = tr_end + pd.Timedelta(days=test_days)
        if te_end > t1:
            break

        all_test_trades = []
        active_pairs = 0
        for p in selected_pairs:
            df = panels.get(p)
            if df is None:
                continue
            window = df[(df.index >= cur) & (df.index < te_end)]
            if len(window) < cfg.hold_hours + 50:
                continue
            active_pairs += 1
            sig = build_signal(window, cfg)
            split_point = int((tr_end - cur) / pd.Timedelta(hours=1))
            split_point = max(20, min(len(window) - 20, split_point))
            test_close = window["perp_close"].to_numpy()[split_point:]
            test_sig = sig[split_point:]
            tr = make_trades_from_signal(test_close, test_sig, cfg.hold_hours, cost_bps)
            if tr.size > 0:
                all_test_trades.append(tr)

        if all_test_trades and active_pairs >= 2:
            cat = np.concatenate(all_test_trades)
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
    train_frac: float,
    costs: List[float],
) -> dict:
    out = {}
    for c in costs:
        total_ret = 0.0
        total_n = 0
        for p in selected_pairs:
            df = panels.get(p)
            if df is None or len(df) < cfg.hold_hours + 120:
                continue
            sig = build_signal(df, cfg)
            cut = split_idx(len(df), train_frac)
            te_close = df["perp_close"].to_numpy()[cut:]
            te_sig = sig[cut:]
            tr = make_trades_from_signal(te_close, te_sig, cfg.hold_hours, c)
            if tr.size:
                total_ret += float(tr.sum())
                total_n += int(tr.size)
        out[f"test_trade_bps_cost_{int(c)}"] = (total_ret / total_n) if total_n > 0 else np.nan
        out[f"test_trades_cost_{int(c)}"] = total_n
    return out


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    candles_dir = Path(args.candles_dir)

    start_ts = pd.Timestamp(args.start, tz="UTC")
    end_ts = pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(hours=23)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    client = MongoClient(args.mongo_uri)
    db = client[_db_name_from_uri(args.mongo_uri)]

    print(f"[1/6] Building universe and loading fear-greed...")
    universe = build_base_universe(db, candles_dir)
    print(f"  Base rich universe: {len(universe)} pairs")

    fg = load_fear_greed_series(db, int(start_ts.timestamp() * 1000), int(end_ts.timestamp() * 1000))

    print(f"[2/6] Loading pair panels ({args.start} -> {args.end})...")
    panels: Dict[str, pd.DataFrame] = {}
    for pair in universe:
        panel = load_pair_panel(pair, db, candles_dir, start_ts, end_ts, fg)
        if panel is None:
            continue
        if len(panel) >= args.min_bars:
            panels[pair] = panel
    print(f"  Loaded usable panels: {len(panels)}")
    if not panels:
        raise SystemExit("No usable panels.")

    print(f"[3/6] Ranking by liquidity and capping to top {args.max_pairs}...")
    keep_pairs = rank_pairs_by_liquidity(panels, args.max_pairs)
    panels = {p: panels[p] for p in keep_pairs if p in panels}
    print(f"  Final research universe: {len(panels)} pairs")

    print("[4/6] Building hypothesis grid...")
    cfgs = make_config_grid()
    print(f"  Configs: {len(cfgs)}")

    print("[5/6] Running no-leakage sweep...")
    rows = []
    for i, cfg in enumerate(cfgs, start=1):
        res = eval_config_no_leakage(
            panels=panels,
            cfg=cfg,
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
        raise SystemExit("No configs passed initial thresholds.")

    df = pd.DataFrame(rows)

    # Composite rank: prefer OOS quality, diversification, lower concentration
    df["rank_score"] = (
        2.0 * df["test_trade_bps"].fillna(-1e9)
        + 20.0 * df["test_pf_pair_median"].fillna(-1e9)
        + 5.0 * df["test_positive_pair_share"].fillna(0.0)
        - 10.0 * df["top_pair_share"].fillna(1.0)
    )
    df = df.sort_values("rank_score", ascending=False).reset_index(drop=True)

    raw_path = out_dir / f"{stamp}_sweep_raw.csv"
    df.to_csv(raw_path, index=False)

    print("[6/6] Walk-forward + cost sensitivity on top configs...")
    top_n = min(args.top_k_wf, len(df))
    enriched_rows = []
    for _, r in df.head(top_n).iterrows():
        cfg = HypothesisConfig(
            family=str(r["family"]),
            z1=float(r["z1"]),
            z2=float(r["z2"]),
            z3=float(r["z3"]),
            hold_hours=int(r["hold_hours"]),
        )
        pairs = [x for x in str(r["selected_pairs"]).split(",") if x]
        wf = walk_forward_score(
            panels=panels,
            cfg=cfg,
            selected_pairs=pairs,
            cost_bps=args.cost_bps_oneway,
            train_days=60,
            test_days=30,
            step_days=15,
        )
        cs = cost_sensitivity(
            panels=panels,
            cfg=cfg,
            selected_pairs=pairs,
            train_frac=args.train_frac,
            costs=[4.0, 6.0, 8.0],
        )
        row = r.to_dict()
        row.update(wf)
        row.update(cs)
        enriched_rows.append(row)

    top_df = pd.DataFrame(enriched_rows)

    # Hard candidate filters
    cand = top_df[
        (top_df["test_trade_bps"] > 5.0)
        & (top_df["test_pf_pair_median"] >= 1.10)
        & (top_df["test_positive_pair_share"] >= 0.55)
        & (top_df["top_pair_share"] <= 0.65)
        & (top_df["wf_windows"] >= 4)
        & (top_df["wf_positive_share"] >= 0.50)
        & (top_df["test_trade_bps_cost_6"] > 0.0)
    ].copy()

    if not cand.empty:
        cand = cand.sort_values(
            ["test_trade_bps", "wf_positive_share", "test_pf_pair_median"],
            ascending=[False, False, False],
        )

    top_path = out_dir / f"{stamp}_top_enriched.csv"
    cand_path = out_dir / f"{stamp}_candidates.csv"
    cfg_path = out_dir / f"{stamp}_run_meta.json"

    top_df.to_csv(top_path, index=False)
    cand.to_csv(cand_path, index=False)

    with cfg_path.open("w") as f:
        json.dump(
            {
                "timestamp_utc": stamp,
                "start": args.start,
                "end": args.end,
                "train_frac": args.train_frac,
                "cost_bps_oneway": args.cost_bps_oneway,
                "universe_pairs": list(sorted(panels.keys())),
                "n_universe_pairs": len(panels),
                "n_configs": len(cfgs),
                "n_pass_initial": int(len(df)),
                "n_top_enriched": int(len(top_df)),
                "n_candidates": int(len(cand)),
            },
            f,
            indent=2,
        )

    print("\n=== SUMMARY ===")
    print(f"Initial passing configs: {len(df)}")
    print(f"Top enriched rows: {len(top_df)}")
    print(f"Final candidates: {len(cand)}")
    if not cand.empty:
        cols = [
            "family", "z1", "z2", "z3", "hold_hours", "n_selected_pairs",
            "test_trade_bps", "test_pf_pair_median", "test_positive_pair_share",
            "top_pair_share", "wf_positive_share", "test_trade_bps_cost_6",
        ]
        print("\nTop candidates:")
        print(cand[cols].head(20).to_string(index=False))

    print("\nArtifacts:")
    print(raw_path)
    print(top_path)
    print(cand_path)
    print(cfg_path)


if __name__ == "__main__":
    main()
