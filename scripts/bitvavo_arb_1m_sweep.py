#!/usr/bin/env python3
"""
Bitvavo EUR vs Bybit perp arb sweep (minute-capable).

This script evaluates spread-capture hypotheses on aligned candles:
- Bitvavo spot BASE-EUR (converted to USD via synthetic EURUSD)
- Bybit perpetual BASE-USDT
- Optional context features from Bybit funding, OI and Coinalyze liquidations

Unlike directional tests, PnL is computed on spread convergence/widening directly.
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
from pymongo import MongoClient


CANDLES_DIR = Path("app/data/cache/candles")
OUT_DIR_DEFAULT = Path("app/data/cache/bitvavo_arb_sweep")


@dataclass(frozen=True)
class Config:
    family: str
    z1: float
    z2: float
    z3: float
    hold_bars: int


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sweep Bitvavo EUR <> Bybit perp arb hypotheses")
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab"))
    p.add_argument("--start", default="2026-01-21")
    p.add_argument("--end", default="2026-04-21")
    p.add_argument("--interval", default="1m", choices=["1m", "5m", "15m", "1h"])
    p.add_argument("--max-pairs", type=int, default=24)
    p.add_argument("--train-frac", type=float, default=0.65)
    p.add_argument("--cost-bps", type=float, default=18.0, help="Roundtrip cost in bps")
    p.add_argument("--min-bars", type=int, default=15000)
    p.add_argument("--min-train-trades", type=int, default=80)
    p.add_argument("--min-test-trades", type=int, default=40)
    p.add_argument("--min-selected-pairs", type=int, default=4)
    p.add_argument("--max-stale-bars", type=int, default=3, help="Max allowed Bitvavo stale bars at entry")
    p.add_argument("--min-bv-coverage", type=float, default=0.0, help="Min share of bars with fresh Bitvavo prints")
    p.add_argument("--top-k-wf", type=int, default=120)
    p.add_argument("--out-dir", default=str(OUT_DIR_DEFAULT))
    return p.parse_args()


def db_name_from_uri(uri: str) -> str:
    if "/" not in uri:
        return "quants_lab"
    return uri.rsplit("/", 1)[-1] or "quants_lab"


def interval_delta(interval: str) -> pd.Timedelta:
    return {
        "1m": pd.Timedelta(minutes=1),
        "5m": pd.Timedelta(minutes=5),
        "15m": pd.Timedelta(minutes=15),
        "1h": pd.Timedelta(hours=1),
    }[interval]


def bars_per_hour(interval: str) -> int:
    return int(round(pd.Timedelta(hours=1) / interval_delta(interval)))


def zscore(s: pd.Series, w: int, mp: int) -> pd.Series:
    mu = s.rolling(w, min_periods=mp).mean()
    sd = s.rolling(w, min_periods=mp).std().replace(0, np.nan)
    return (s - mu) / sd


def streak_from_sign(vals: pd.Series) -> pd.Series:
    out = []
    cur = 0
    prev = 0
    for v in vals.astype(float).fillna(0.0).to_numpy():
        s = int(np.sign(v))
        if s == 0:
            cur = 0
        elif s == prev:
            cur += 1
        else:
            cur = 1
        out.append(cur)
        if s != 0:
            prev = s
    return pd.Series(out, index=vals.index, dtype=float)


def compute_stale_bars(is_trade: np.ndarray) -> np.ndarray:
    out = np.zeros(len(is_trade), dtype=np.int32)
    last = -1_000_000_000
    for i, v in enumerate(is_trade):
        if v:
            last = i
            out[i] = 0
        else:
            out[i] = i - last if last > -1_000_000 else 1_000_000
    return out


def discover_bases(interval: str) -> List[str]:
    bv = set()
    for p in CANDLES_DIR.glob(f"bitvavo_spot|*-EUR|{interval}.parquet"):
        pair = p.name.split("|")[1]
        if pair.endswith("-EUR"):
            bv.add(pair[:-4])

    out = []
    for base in sorted(bv):
        if (CANDLES_DIR / f"bybit_perpetual|{base}-USDT|{interval}.parquet").exists():
            out.append(base)
    return out


def load_eurusd_series(start: pd.Timestamp, end: pd.Timestamp, interval: str) -> pd.Series:
    bv_path = CANDLES_DIR / f"bitvavo_spot|BTC-EUR|{interval}.parquet"
    bb_path = CANDLES_DIR / f"bybit_perpetual|BTC-USDT|{interval}.parquet"
    if not (bv_path.exists() and bb_path.exists()):
        return pd.Series(dtype=float)

    try:
        bv = pd.read_parquet(bv_path, columns=["timestamp", "close"])
        bb = pd.read_parquet(bb_path, columns=["timestamp", "close"])
    except Exception:
        return pd.Series(dtype=float)

    bv["ts"] = pd.to_datetime(bv["timestamp"], unit="s", utc=True)
    bb["ts"] = pd.to_datetime(bb["timestamp"], unit="s", utc=True)
    bv = bv.set_index("ts").sort_index().rename(columns={"close": "btc_eur"})
    bb = bb.set_index("ts").sort_index().rename(columns={"close": "btc_usdt"})

    warm = start - pd.Timedelta(days=10)
    bb = bb[(bb.index >= warm) & (bb.index <= end)]
    bv = bv[(bv.index >= warm) & (bv.index <= end)]
    if bb.empty or bv.empty:
        return pd.Series(dtype=float)

    df = bb[["btc_usdt"]].join(bv[["btc_eur"]], how="left")
    df["btc_eur"] = df["btc_eur"].ffill()
    df["eurusd"] = (df["btc_usdt"] / df["btc_eur"]).replace([np.inf, -np.inf], np.nan).ffill().bfill()
    return df["eurusd"]


def load_panel(
    base: str,
    db,
    start: pd.Timestamp,
    end: pd.Timestamp,
    interval: str,
    eurusd: pd.Series,
) -> pd.DataFrame | None:
    bv_path = CANDLES_DIR / f"bitvavo_spot|{base}-EUR|{interval}.parquet"
    bb_path = CANDLES_DIR / f"bybit_perpetual|{base}-USDT|{interval}.parquet"
    if not (bv_path.exists() and bb_path.exists()):
        return None

    warm = start - pd.Timedelta(days=10)
    s = int(warm.timestamp())
    e = int(end.timestamp())
    sm = int(warm.timestamp() * 1000)
    em = int(end.timestamp() * 1000)

    try:
        bv = pd.read_parquet(bv_path, columns=["timestamp", "close", "volume"])
        bb = pd.read_parquet(bb_path, columns=["timestamp", "close", "volume"])
    except Exception:
        return None

    bv = bv[(bv["timestamp"] >= s) & (bv["timestamp"] <= e)].copy()
    bb = bb[(bb["timestamp"] >= s) & (bb["timestamp"] <= e)].copy()
    if len(bb) < 1000 or len(bv) < 100:
        return None

    bv["ts"] = pd.to_datetime(bv["timestamp"], unit="s", utc=True)
    bb["ts"] = pd.to_datetime(bb["timestamp"], unit="s", utc=True)
    bv = bv.set_index("ts").sort_index().rename(columns={"close": "bv_close_eur", "volume": "bv_vol"})
    bb = bb.set_index("ts").sort_index().rename(columns={"close": "bb_close_usdt", "volume": "bb_vol"})

    df = bb[["bb_close_usdt", "bb_vol"]].join(bv[["bv_close_eur", "bv_vol"]], how="left")
    if df.empty:
        return None

    trade_mask = df["bv_close_eur"].notna().to_numpy()
    df["bv_trade_bar"] = trade_mask.astype(float)
    df["bv_stale_bars"] = compute_stale_bars(trade_mask)
    df["bv_close_eur"] = df["bv_close_eur"].ffill()
    df["bv_vol"] = df["bv_vol"].fillna(0.0)
    df = df[df["bv_close_eur"].notna()].copy()
    if len(df) < 1000:
        return None

    fx = eurusd.reindex(df.index, method="ffill")
    df["eurusd"] = fx
    df["bv_close_usd"] = df["bv_close_eur"] * df["eurusd"]
    df["spread_bps"] = (df["bb_close_usdt"] / df["bv_close_usd"] - 1.0) * 1e4

    bph = bars_per_hour(interval)
    w_short = max(2 * bph, 60)
    w_mid = max(8 * bph, 240)
    mp_short = max(bph, 30)
    mp_mid = max(4 * bph, 120)

    df["spread_z"] = zscore(df["spread_bps"], w_mid, mp_mid)
    df["spread_mom_1h"] = df["spread_bps"].diff(bph)
    df["spread_mom_z"] = zscore(df["spread_mom_1h"], w_short, mp_short)
    df["spread_abs_z"] = zscore(df["spread_bps"].abs(), w_mid, mp_mid)
    df["ret_1h"] = df["bb_close_usdt"].pct_change(bph)
    df["ret_1h_z"] = zscore(df["ret_1h"], w_short, mp_short)
    df["fx_ret_1h"] = df["eurusd"].pct_change(bph)
    df["fx_ret_z"] = zscore(df["fx_ret_1h"], w_short, mp_short)

    fdocs = list(
        db["bybit_funding_rates"]
        .find({"pair": f"{base}-USDT", "timestamp_utc": {"$gte": sm, "$lte": em}}, {"_id": 0, "timestamp_utc": 1, "funding_rate": 1})
        .sort("timestamp_utc", 1)
    )
    if fdocs:
        fdf = pd.DataFrame(fdocs)
        fdf["ts"] = pd.to_datetime(fdf["timestamp_utc"], unit="ms", utc=True)
        fdf = fdf.set_index("ts")[["funding_rate"]].sort_index()
        fund_1h = fdf.resample("1h").last().ffill()
        df["funding"] = fund_1h["funding_rate"].reindex(df.index, method="ffill").fillna(0.0)

        fev = fdf.copy()
        fev["funding_z_ev"] = zscore(fev["funding_rate"], 40, 20)
        fev["funding_streak_ev"] = streak_from_sign(np.sign(fev["funding_rate"]))
        aligned = fev[["funding_z_ev", "funding_streak_ev"]].reindex(df.index, method="ffill")
        df["funding_z"] = aligned["funding_z_ev"].fillna(0.0)
        df["funding_streak"] = aligned["funding_streak_ev"].fillna(0.0)
    else:
        df["funding"] = 0.0
        df["funding_z"] = 0.0
        df["funding_streak"] = 0.0

    odocs = list(
        db["bybit_open_interest"]
        .find({"pair": f"{base}-USDT", "timestamp_utc": {"$gte": sm, "$lte": em}}, {"_id": 0, "timestamp_utc": 1, "oi_value": 1})
        .sort("timestamp_utc", 1)
    )
    if odocs:
        odf = pd.DataFrame(odocs)
        odf["ts"] = pd.to_datetime(odf["timestamp_utc"], unit="ms", utc=True)
        odf = odf.set_index("ts")[["oi_value"]].sort_index().resample("1h").last().ffill()
        df = df.join(odf, how="left")
        oi_log = np.log(df["oi_value"].replace(0, np.nan)).ffill()
        df["oi_delta"] = oi_log.diff(bph)
        df["oi_z"] = zscore(df["oi_delta"], w_short, mp_short).fillna(0.0)
    else:
        df["oi_value"] = np.nan
        df["oi_z"] = 0.0

    ldocs = list(
        db["coinalyze_liquidations"]
        .find(
            {"pair": f"{base}-USDT", "timestamp_utc": {"$gte": sm, "$lte": em}},
            {"_id": 0, "timestamp_utc": 1, "long_liquidations_usd": 1, "short_liquidations_usd": 1, "total_liquidations_usd": 1},
        )
        .sort("timestamp_utc", 1)
    )
    if ldocs:
        ldf = pd.DataFrame(ldocs)
        ldf["ts"] = pd.to_datetime(ldf["timestamp_utc"], unit="ms", utc=True)
        ldf = (
            ldf.set_index("ts")[["long_liquidations_usd", "short_liquidations_usd", "total_liquidations_usd"]]
            .sort_index()
            .resample("1h")
            .last()
            .fillna(0.0)
        )
        df = df.join(ldf, how="left")
    else:
        df["long_liquidations_usd"] = 0.0
        df["short_liquidations_usd"] = 0.0
        df["total_liquidations_usd"] = 0.0

    lt = df["total_liquidations_usd"].fillna(0.0)
    ll = df["long_liquidations_usd"].fillna(0.0)
    ls = df["short_liquidations_usd"].fillna(0.0)
    df["liq_total_z"] = zscore(np.log1p(lt), w_short, mp_short).fillna(0.0)
    df["liq_imb"] = (ll - ls) / (lt + 1.0)
    df["liq_imb_z"] = zscore(df["liq_imb"], w_short, mp_short).fillna(0.0)

    h = df.index.hour
    df["is_eu_session"] = ((h >= 7) & (h < 16)).astype(float)
    df["is_us_session"] = ((h >= 13) & (h < 22)).astype(float)

    df = df[(df.index >= start) & (df.index <= end)].copy()
    for col in [
        "spread_z",
        "spread_mom_z",
        "spread_abs_z",
        "ret_1h_z",
        "fx_ret_z",
        "funding_z",
        "funding_streak",
        "oi_z",
        "liq_total_z",
        "liq_imb_z",
    ]:
        df[col] = df[col].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    df = df.dropna(subset=["spread_bps", "bb_close_usdt", "bv_close_usd"])
    if len(df) < 1000:
        return None

    df["pair"] = f"{base}-USDT"
    return df


def signal(df: pd.DataFrame, cfg: Config, max_stale_bars: int) -> np.ndarray:
    sp = df["spread_bps"].to_numpy()
    spz = df["spread_z"].to_numpy()
    smz = df["spread_mom_z"].to_numpy()
    fz = df["funding_z"].to_numpy()
    fs = df["funding_streak"].to_numpy()
    oiz = df["oi_z"].to_numpy()
    ltz = df["liq_total_z"].to_numpy()
    liz = df["liq_imb_z"].to_numpy()
    fxz = df["fx_ret_z"].to_numpy()
    is_eu = df["is_eu_session"].to_numpy()
    stale = df["bv_stale_bars"].to_numpy()

    n = len(df)
    sig = np.zeros(n, dtype=np.int8)
    f = cfg.family
    z1, z2, z3 = cfg.z1, cfg.z2, cfg.z3

    if f == "basis_revert":
        cond = (np.abs(spz) >= z1) & (np.abs(smz) <= z2)
        sig[cond] = np.sign(sp[cond]).astype(np.int8)

    elif f == "basis_cont":
        cond = (np.abs(spz) >= z1) & (np.sign(spz) == np.sign(smz)) & (np.abs(smz) >= z2)
        sig[cond] = -np.sign(sp[cond]).astype(np.int8)

    elif f == "funding_aligned_revert":
        cond = (np.abs(spz) >= z1) & (np.abs(fz) >= z2) & (np.sign(spz) == np.sign(fz)) & (fs >= 1)
        sig[cond] = np.sign(sp[cond]).astype(np.int8)

    elif f == "oi_liq_exhaust_revert":
        cond = (np.abs(spz) >= z1) & (oiz >= z2) & (ltz >= z3)
        sig[cond] = np.sign(sp[cond]).astype(np.int8)

    elif f == "fxshock_revert":
        cond = (np.abs(spz) >= z1) & (np.abs(fxz) >= z2)
        sig[cond] = np.sign(sp[cond]).astype(np.int8)

    elif f == "eu_session_revert":
        cond = (is_eu > 0) & (np.abs(spz) >= z1) & (np.abs(smz) <= z2)
        sig[cond] = np.sign(sp[cond]).astype(np.int8)

    elif f == "composite_revert":
        score = spz + 0.5 * fz + 0.5 * oiz + 0.35 * liz
        cond = (np.abs(score) >= z1) & (ltz >= z2) & (np.abs(fxz) >= z3)
        sig[cond] = np.sign(score[cond]).astype(np.int8)

    sig[stale > max_stale_bars] = 0
    return sig


def make_trades(spread: np.ndarray, sig: np.ndarray, hold_bars: int, cost_bps: float) -> np.ndarray:
    n = len(spread)
    if n <= hold_bars + 2:
        return np.array([], dtype=float)

    out: List[float] = []
    i = 0
    while i < n - hold_bars - 1:
        side = sig[i]
        if side == 0:
            i += 1
            continue
        e = i + 1
        x = i + hold_bars
        pnl = side * (spread[e] - spread[x]) - cost_bps
        out.append(float(pnl))
        i = x + 1
    return np.asarray(out, dtype=float)


def pf(arr: np.ndarray) -> float:
    if arr.size == 0:
        return np.nan
    gp = arr[arr > 0].sum()
    gl = -arr[arr < 0].sum()
    if gl <= 0:
        return np.nan
    return float(gp / gl)


def split_idx(n: int, frac: float) -> int:
    return int(max(300, min(n - 300, round(n * frac))))


def make_configs(interval: str) -> List[Config]:
    if interval == "1m":
        holds_main = [5, 10, 15, 20, 30, 45, 60, 90, 120]
        holds_slow = [15, 30, 45, 60, 90, 120, 180]
    elif interval == "5m":
        holds_main = [3, 6, 9, 12, 18, 24, 36]
        holds_slow = [6, 12, 18, 24, 36]
    elif interval == "15m":
        holds_main = [2, 4, 6, 8, 12, 16]
        holds_slow = [4, 8, 12, 16, 24]
    else:
        holds_main = [2, 4, 6, 8, 12, 18]
        holds_slow = [4, 8, 12, 18, 24]

    out: List[Config] = []

    for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5, 3.0], [0.5, 1.0, 1.5, 2.0], holds_main):
        out.append(Config("basis_revert", z1, z2, 0.0, h))

    for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.5, 1.0, 1.5], holds_main):
        out.append(Config("basis_cont", z1, z2, 0.0, h))

    for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.5, 1.0, 1.5], holds_slow):
        out.append(Config("funding_aligned_revert", z1, z2, 0.0, h))

    for z1, z2, z3, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.0, 0.5, 1.0], [0.5, 1.0, 1.5], holds_main):
        out.append(Config("oi_liq_exhaust_revert", z1, z2, z3, h))

    for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.5, 1.0, 1.5], holds_main):
        out.append(Config("fxshock_revert", z1, z2, 0.0, h))

    for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.5, 1.0, 1.5], holds_main):
        out.append(Config("eu_session_revert", z1, z2, 0.0, h))

    for z1, z2, z3, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.0, 0.5, 1.0], [0.5, 1.0, 1.5], holds_slow):
        out.append(Config("composite_revert", z1, z2, z3, h))

    return out


def eval_cfg(
    panels: Dict[str, pd.DataFrame],
    cfg: Config,
    train_frac: float,
    cost_bps: float,
    min_train_trades: int,
    min_test_trades: int,
    min_selected_pairs: int,
    max_stale_bars: int,
) -> dict | None:
    tr_rows = []
    te_rows = []

    for pair, df in panels.items():
        if len(df) < cfg.hold_bars + 400:
            continue

        s = signal(df, cfg, max_stale_bars=max_stale_bars)
        cut = split_idx(len(df), train_frac)

        tr = make_trades(df["spread_bps"].to_numpy()[:cut], s[:cut], cfg.hold_bars, cost_bps)
        te = make_trades(df["spread_bps"].to_numpy()[cut:], s[cut:], cfg.hold_bars, cost_bps)

        if tr.size < 10 or te.size < min_test_trades:
            continue

        tr_rows.append((pair, int(tr.size), float(tr.mean()), pf(tr), float((tr > 0).mean()), float(tr.sum())))
        te_rows.append((pair, int(te.size), float(te.mean()), pf(te), float((te > 0).mean()), float(te.sum())))

    if not tr_rows:
        return None

    trdf = pd.DataFrame(tr_rows, columns=["pair", "n", "mean", "pf", "wr", "sum"])
    tedf = pd.DataFrame(te_rows, columns=["pair", "n", "mean", "pf", "wr", "sum"])

    sel = trdf[(trdf["mean"] > 0) & (trdf["pf"] >= 1.05) & (trdf["n"] >= min_train_trades)]
    if len(sel) < min_selected_pairs:
        return None

    spairs = set(sel["pair"])
    tr = trdf[trdf["pair"].isin(spairs)].copy()
    te = tedf[tedf["pair"].isin(spairs)].copy()
    if te.empty:
        return None

    tr_mean = float((tr["mean"] * tr["n"]).sum() / tr["n"].sum())
    te_mean = float((te["mean"] * te["n"]).sum() / te["n"].sum())
    total_test = float(te["sum"].sum())
    top_share = float(abs(te["sum"].max() / total_test)) if total_test != 0 else np.nan

    return {
        "family": cfg.family,
        "z1": cfg.z1,
        "z2": cfg.z2,
        "z3": cfg.z3,
        "hold_bars": cfg.hold_bars,
        "n_selected_pairs": int(len(spairs)),
        "selected_pairs": ",".join(sorted(spairs)),
        "train_trade_bps": tr_mean,
        "train_trades": int(tr["n"].sum()),
        "test_trade_bps": te_mean,
        "test_trades": int(te["n"].sum()),
        "test_pf_pair_median": float(te["pf"].replace([np.inf, -np.inf], np.nan).dropna().median()),
        "test_positive_pair_share": float((te["mean"] > 0).mean()),
        "top_pair_share": top_share,
    }


def walk_forward(
    panels: Dict[str, pd.DataFrame],
    cfg: Config,
    pairs: List[str],
    cost_bps: float,
    max_stale_bars: int,
    interval: str,
) -> dict:
    ppanels = [panels[p] for p in pairs if p in panels]
    if not ppanels:
        return {"wf_windows": 0, "wf_positive_share": 0.0}

    t0 = min(x.index.min() for x in ppanels)
    t1 = max(x.index.max() for x in ppanels)

    if interval == "1m":
        train_d, test_d, step_d = 45, 15, 7
    elif interval == "5m":
        train_d, test_d, step_d = 60, 20, 10
    else:
        train_d, test_d, step_d = 75, 25, 12

    cur = t0
    wins = []

    while True:
        tr_end = cur + pd.Timedelta(days=train_d)
        te_end = tr_end + pd.Timedelta(days=test_d)
        if te_end > t1:
            break

        arrs = []
        active = 0
        for p in pairs:
            df = panels.get(p)
            if df is None:
                continue
            w = df[(df.index >= cur) & (df.index < te_end)]
            if len(w) < cfg.hold_bars + 500:
                continue
            active += 1

            s = signal(w, cfg, max_stale_bars=max_stale_bars)
            split = int(w.index.searchsorted(tr_end, side="left"))
            split = max(200, min(len(w) - 200, split))
            te = make_trades(w["spread_bps"].to_numpy()[split:], s[split:], cfg.hold_bars, cost_bps)
            if te.size:
                arrs.append(te)

        if arrs and active >= 2:
            cat = np.concatenate(arrs)
            wins.append(float(cat.mean()))

        cur += pd.Timedelta(days=step_d)

    if not wins:
        return {"wf_windows": 0, "wf_positive_share": 0.0}

    arr = np.asarray(wins, dtype=float)
    return {"wf_windows": int(arr.size), "wf_positive_share": float((arr > 0).mean())}


def cost_check(
    panels: Dict[str, pd.DataFrame],
    cfg: Config,
    pairs: List[str],
    train_frac: float,
    cost_bps: float,
    max_stale_bars: int,
) -> float:
    tot = 0.0
    n = 0
    for p in pairs:
        df = panels.get(p)
        if df is None:
            continue
        s = signal(df, cfg, max_stale_bars=max_stale_bars)
        cut = split_idx(len(df), train_frac)
        te = make_trades(df["spread_bps"].to_numpy()[cut:], s[cut:], cfg.hold_bars, cost_bps)
        if te.size:
            tot += float(te.sum())
            n += int(te.size)
    return float(tot / n) if n > 0 else np.nan


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    bar_dt = interval_delta(args.interval)
    start = pd.Timestamp(args.start, tz="UTC")
    end = pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(days=1) - bar_dt
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    client = MongoClient(args.mongo_uri)
    db = client[db_name_from_uri(args.mongo_uri)]

    print("[1/6] Discovering matched Bitvavo/Bybit pairs...")
    bases = discover_bases(args.interval)
    print(f"  Matched bases: {len(bases)}")
    if not bases:
        raise SystemExit("No matched Bitvavo/Bybit candle files for requested interval.")

    print("[2/6] Building synthetic EURUSD...")
    eurusd = load_eurusd_series(start, end, args.interval)
    if eurusd.empty:
        raise SystemExit("Could not build EURUSD synthetic series (need BTC-EUR and BTC-USDT candles).")

    print("[3/6] Building feature panels...")
    panels: Dict[str, pd.DataFrame] = {}
    for b in bases:
        p = load_panel(b, db, start, end, args.interval, eurusd)
        if p is not None and len(p) >= args.min_bars:
            panels[f"{b}-USDT"] = p

    if not panels:
        raise SystemExit("No usable panels after merge/quality filters.")

    rows = []
    for pair, p in panels.items():
        coverage = float(p["bv_trade_bar"].mean()) if "bv_trade_bar" in p.columns else 0.0
        if coverage < args.min_bv_coverage:
            continue
        notional = (p["bb_close_usdt"] * p["bb_vol"]).replace([np.inf, -np.inf], np.nan).dropna()
        if len(notional) < 200:
            continue
        rows.append((pair, float(notional.median()), coverage))
    keep = [x[0] for x in sorted(rows, key=lambda x: x[1], reverse=True)[: args.max_pairs]]
    panels = {p: panels[p] for p in keep if p in panels}
    print(f"  Final universe: {len(panels)} pairs")

    print("[4/6] Building config grid...")
    cfgs = make_configs(args.interval)
    print(f"  Configs: {len(cfgs)}")

    print("[5/6] Running no-leakage sweep...")
    out = []
    for i, cfg in enumerate(cfgs, 1):
        r = eval_cfg(
            panels=panels,
            cfg=cfg,
            train_frac=args.train_frac,
            cost_bps=args.cost_bps,
            min_train_trades=args.min_train_trades,
            min_test_trades=args.min_test_trades,
            min_selected_pairs=args.min_selected_pairs,
            max_stale_bars=args.max_stale_bars,
        )
        if r is not None:
            out.append(r)
        if i % 100 == 0:
            print(f"  {i}/{len(cfgs)} done, pass={len(out)}")

    if not out:
        raise SystemExit("No configs passed initial filters.")

    df = pd.DataFrame(out)
    df["rank_score"] = (
        2.0 * df["test_trade_bps"]
        + 20.0 * df["test_pf_pair_median"]
        + 5.0 * df["test_positive_pair_share"]
        - 8.0 * df["top_pair_share"]
        + 0.25 * df["n_selected_pairs"]
    )
    df = df.sort_values("rank_score", ascending=False).reset_index(drop=True)
    raw_path = out_dir / f"{stamp}_raw.csv"
    df.to_csv(raw_path, index=False)

    print("[6/6] Walk-forward + cost stress...")
    top_rows = []
    for _, r in df.head(min(args.top_k_wf, len(df))).iterrows():
        cfg = Config(str(r["family"]), float(r["z1"]), float(r["z2"]), float(r["z3"]), int(r["hold_bars"]))
        pairs = [x for x in str(r["selected_pairs"]).split(",") if x]

        wf = walk_forward(
            panels=panels,
            cfg=cfg,
            pairs=pairs,
            cost_bps=args.cost_bps,
            max_stale_bars=args.max_stale_bars,
            interval=args.interval,
        )
        c_up = cost_check(
            panels=panels,
            cfg=cfg,
            pairs=pairs,
            train_frac=args.train_frac,
            cost_bps=args.cost_bps + 6.0,
            max_stale_bars=args.max_stale_bars,
        )

        row = r.to_dict()
        row.update(wf)
        row["test_trade_bps_cost_up"] = c_up
        top_rows.append(row)

    top_df = pd.DataFrame(top_rows)
    top_path = out_dir / f"{stamp}_top.csv"
    top_df.to_csv(top_path, index=False)

    cand = top_df[
        (top_df["test_trade_bps"] > 1.5)
        & (top_df["test_pf_pair_median"] >= 1.08)
        & (top_df["test_positive_pair_share"] >= 0.52)
        & (top_df["top_pair_share"] <= 0.70)
        & (top_df["wf_windows"] >= 3)
        & (top_df["wf_positive_share"] >= 0.50)
        & (top_df["test_trade_bps_cost_up"] > 0.0)
    ].copy()
    cand = cand.sort_values(["test_trade_bps", "wf_positive_share"], ascending=[False, False])
    cand_path = out_dir / f"{stamp}_candidates.csv"
    cand.to_csv(cand_path, index=False)

    meta = {
        "timestamp_utc": stamp,
        "start": args.start,
        "end": args.end,
        "interval": args.interval,
        "n_universe_pairs": len(panels),
        "universe_pairs": sorted(panels.keys()),
        "n_configs": len(cfgs),
        "n_pass_initial": int(len(df)),
        "n_top": int(len(top_df)),
        "n_candidates": int(len(cand)),
        "cost_bps": args.cost_bps,
        "cost_up_bps": args.cost_bps + 6.0,
        "max_stale_bars": args.max_stale_bars,
        "min_bv_coverage": args.min_bv_coverage,
    }
    meta_path = out_dir / f"{stamp}_meta.json"
    with meta_path.open("w") as f:
        json.dump(meta, f, indent=2)

    print("\n=== SUMMARY ===")
    print(f"Initial pass: {len(df)}")
    print(f"Top enriched: {len(top_df)}")
    print(f"Candidates: {len(cand)}")
    if not cand.empty:
        cols = [
            "family",
            "z1",
            "z2",
            "z3",
            "hold_bars",
            "n_selected_pairs",
            "test_trade_bps",
            "test_pf_pair_median",
            "test_positive_pair_share",
            "top_pair_share",
            "wf_positive_share",
            "test_trade_bps_cost_up",
        ]
        print(cand[cols].head(20).to_string(index=False))

    print("\nArtifacts:")
    print(raw_path)
    print(top_path)
    print(cand_path)
    print(meta_path)


if __name__ == "__main__":
    main()
