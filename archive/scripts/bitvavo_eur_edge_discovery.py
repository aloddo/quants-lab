#!/usr/bin/env python3
"""
Bitvavo EUR Edge Discovery

Tests EUR-quote specific hypotheses using:
- Bitvavo EUR spot candles (local parquet)
- Bybit USDT perpetual candles (local parquet)
- Bybit funding / OI (Mongo)
- Coinalyze liquidations (Mongo)

Outputs ranked configs + candidates with no-leakage pair selection.
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
OUT_DIR_DEFAULT = Path("app/data/cache/bitvavo_eur_discovery")


@dataclass(frozen=True)
class Config:
    family: str
    z1: float
    z2: float
    z3: float
    hold_h: int


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Discover EUR-quote edges with Bitvavo + Bybit")
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab"))
    p.add_argument("--start", default="2024-04-21")
    p.add_argument("--end", default="2026-04-21")
    p.add_argument("--interval", default="1h", choices=["1h"])
    p.add_argument("--max-pairs", type=int, default=36)
    p.add_argument("--train-frac", type=float, default=0.65)
    p.add_argument("--cost-bps", type=float, default=4.0)
    p.add_argument("--min-bars", type=int, default=3000)
    p.add_argument("--min-train-trades", type=int, default=25)
    p.add_argument("--min-test-trades", type=int, default=10)
    p.add_argument("--min-selected-pairs", type=int, default=4)
    p.add_argument("--top-k-wf", type=int, default=100)
    p.add_argument("--out-dir", default=str(OUT_DIR_DEFAULT))
    return p.parse_args()


def db_name_from_uri(uri: str) -> str:
    if "/" not in uri:
        return "quants_lab"
    return uri.rsplit("/", 1)[-1] or "quants_lab"


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


def make_trades(close: np.ndarray, sig: np.ndarray, hold_h: int, cost_bps: float) -> np.ndarray:
    n = len(close)
    if n <= hold_h + 2:
        return np.array([], dtype=float)
    out: List[float] = []
    i = 0
    while i < n - hold_h - 1:
        side = sig[i]
        if side == 0:
            i += 1
            continue
        e = i + 1
        x = i + hold_h
        ret = side * ((close[x] - close[e]) / close[e]) * 1e4 - 2.0 * cost_bps
        out.append(float(ret))
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
    return int(max(80, min(n - 80, round(n * frac))))


def discover_pairs() -> List[str]:
    pairs = []
    for p in CANDLES_DIR.glob("bitvavo_spot|*-EUR|1h.parquet"):
        pair = p.name.split("|")[1]
        if pair.endswith("-EUR"):
            pairs.append(pair[:-4])
    # keep only if bybit perp exists
    out = []
    for base in sorted(set(pairs)):
        if (CANDLES_DIR / f"bybit_perpetual|{base}-USDT|1h.parquet").exists():
            out.append(base)
    return out


def load_eurusd_series(start: pd.Timestamp, end: pd.Timestamp) -> pd.Series:
    """
    EURUSD synthetic hourly:
      BTC-USDT(Bybit) / BTC-EUR(Bitvavo)
    """
    bv_path = CANDLES_DIR / "bitvavo_spot|BTC-EUR|1h.parquet"
    bb_path = CANDLES_DIR / "bybit_perpetual|BTC-USDT|1h.parquet"
    if not (bv_path.exists() and bb_path.exists()):
        return pd.Series(dtype=float)

    bv = pd.read_parquet(bv_path, columns=["timestamp", "close"])
    bb = pd.read_parquet(bb_path, columns=["timestamp", "close"])
    bv["ts"] = pd.to_datetime(bv["timestamp"], unit="s", utc=True)
    bb["ts"] = pd.to_datetime(bb["timestamp"], unit="s", utc=True)
    bv = bv.set_index("ts").sort_index().rename(columns={"close": "btc_eur"})
    bb = bb.set_index("ts").sort_index().rename(columns={"close": "btc_usdt"})

    df = bb[["btc_usdt"]].join(bv[["btc_eur"]], how="inner")
    df = df[(df.index >= start - pd.Timedelta(days=45)) & (df.index <= end)]
    df["eurusd"] = df["btc_usdt"] / df["btc_eur"]
    df["eurusd"] = df["eurusd"].replace([np.inf, -np.inf], np.nan).ffill().bfill()
    # slight smoothing to suppress single-venue micro glitches
    return df["eurusd"].rolling(3, min_periods=1).median()


def load_panel(
    base: str,
    db,
    start: pd.Timestamp,
    end: pd.Timestamp,
    eurusd: pd.Series,
) -> pd.DataFrame | None:
    bv_path = CANDLES_DIR / f"bitvavo_spot|{base}-EUR|1h.parquet"
    bb_path = CANDLES_DIR / f"bybit_perpetual|{base}-USDT|1h.parquet"
    if not (bv_path.exists() and bb_path.exists()):
        return None

    warm = start - pd.Timedelta(days=45)
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
    if len(bv) < 1200 or len(bb) < 1200:
        return None

    bv["ts"] = pd.to_datetime(bv["timestamp"], unit="s", utc=True)
    bb["ts"] = pd.to_datetime(bb["timestamp"], unit="s", utc=True)
    bv = bv.set_index("ts").sort_index().rename(columns={"close": "bv_close_eur", "volume": "bv_vol"})
    bb = bb.set_index("ts").sort_index().rename(columns={"close": "bb_close_usdt", "volume": "bb_vol"})

    df = bb[["bb_close_usdt", "bb_vol"]].join(bv[["bv_close_eur", "bv_vol"]], how="inner")
    if len(df) < 1200:
        return None

    # FX conversion EUR->USD
    fx = eurusd.reindex(df.index, method="ffill")
    df["eurusd"] = fx
    df["bv_close_usd"] = df["bv_close_eur"] * df["eurusd"]

    # Premium: Bybit perp vs Bitvavo spot (USD-converted)
    df["premium_bps"] = (df["bb_close_usdt"] / df["bv_close_usd"] - 1.0) * 1e4
    df["premium_z"] = zscore(df["premium_bps"], 72, 24)

    # Basic returns
    df["ret_8h"] = df["bb_close_usdt"].pct_change(8)
    df["ret_8h_z"] = zscore(df["ret_8h"], 72, 24)

    # FX shocks
    df["fx_ret_1h"] = df["eurusd"].pct_change(1)
    df["fx_ret_z"] = zscore(df["fx_ret_1h"], 72, 24)

    # Funding
    fdocs = list(
        db["bybit_funding_rates"]
        .find({"pair": f"{base}-USDT", "timestamp_utc": {"$gte": sm, "$lte": em}}, {"_id": 0, "timestamp_utc": 1, "funding_rate": 1})
        .sort("timestamp_utc", 1)
    )
    if len(fdocs) < 80:
        return None
    fdf = pd.DataFrame(fdocs)
    fdf["ts"] = pd.to_datetime(fdf["timestamp_utc"], unit="ms", utc=True)
    fdf = fdf.set_index("ts")[["funding_rate"]].sort_index().rename(columns={"funding_rate": "funding"})
    fund_1h = fdf.resample("1h").last().ffill()
    df["funding"] = fund_1h["funding"].reindex(df.index, method="ffill")

    fev = fdf.copy()
    fev["funding_z_ev"] = zscore(fev["funding"], 30, 15)
    fev["funding_streak_ev"] = streak_from_sign(np.sign(fev["funding"]))
    aligned = fev[["funding_z_ev", "funding_streak_ev"]].reindex(df.index, method="ffill")
    df["funding_z"] = aligned["funding_z_ev"]
    df["funding_streak"] = aligned["funding_streak_ev"]

    # OI
    odocs = list(
        db["bybit_open_interest"]
        .find({"pair": f"{base}-USDT", "timestamp_utc": {"$gte": sm, "$lte": em}}, {"_id": 0, "timestamp_utc": 1, "oi_value": 1})
        .sort("timestamp_utc", 1)
    )
    if len(odocs) < 500:
        return None
    odf = pd.DataFrame(odocs)
    odf["ts"] = pd.to_datetime(odf["timestamp_utc"], unit="ms", utc=True)
    odf = odf.set_index("ts")[["oi_value"]].sort_index().resample("1h").last().ffill()
    df = df.join(odf, how="left")

    oi_log = np.log(df["oi_value"].replace(0, np.nan)).ffill()
    df["oi_delta"] = oi_log.diff(1)
    df["oi_z"] = zscore(df["oi_delta"], 72, 24)

    # Liquidations (if missing for pair, set to 0 features)
    ldocs = list(
        db["coinalyze_liquidations"]
        .find({"pair": f"{base}-USDT", "timestamp_utc": {"$gte": sm, "$lte": em}},
              {"_id": 0, "timestamp_utc": 1, "long_liquidations_usd": 1, "short_liquidations_usd": 1, "total_liquidations_usd": 1})
        .sort("timestamp_utc", 1)
    )
    if ldocs:
        ldf = pd.DataFrame(ldocs)
        ldf["ts"] = pd.to_datetime(ldf["timestamp_utc"], unit="ms", utc=True)
        ldf = ldf.set_index("ts")[["long_liquidations_usd", "short_liquidations_usd", "total_liquidations_usd"]].sort_index().resample("1h").last().fillna(0.0)
        df = df.join(ldf, how="left")
    else:
        df["long_liquidations_usd"] = 0.0
        df["short_liquidations_usd"] = 0.0
        df["total_liquidations_usd"] = 0.0

    lt = df["total_liquidations_usd"].fillna(0.0)
    ll = df["long_liquidations_usd"].fillna(0.0)
    ls = df["short_liquidations_usd"].fillna(0.0)
    df["liq_total_z"] = zscore(np.log1p(lt), 72, 24)
    df["liq_imb"] = (ll - ls) / (lt + 1.0)
    df["liq_imb_z"] = zscore(df["liq_imb"], 72, 24)

    # Session regime features
    h = df.index.hour
    df["is_eu_session"] = ((h >= 7) & (h < 16)).astype(float)
    df["is_weekend"] = (df.index.dayofweek >= 5).astype(float)

    # Final cleanup
    df = df[(df.index >= start) & (df.index <= end)].copy()
    for col in [
        "premium_z", "funding_z", "funding_streak", "oi_z", "liq_total_z", "liq_imb_z",
        "ret_8h_z", "fx_ret_z",
    ]:
        df[col] = df[col].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    df = df.dropna(subset=["bb_close_usdt", "bv_close_usd", "funding", "oi_value"])
    if len(df) < 1200:
        return None

    df["pair"] = f"{base}-USDT"
    return df


def signal(df: pd.DataFrame, cfg: Config) -> np.ndarray:
    pz = df["premium_z"].to_numpy()
    fz = df["funding_z"].to_numpy()
    fs = df["funding_streak"].to_numpy()
    oiz = df["oi_z"].to_numpy()
    ltz = df["liq_total_z"].to_numpy()
    liz = df["liq_imb_z"].to_numpy()
    fxz = df["fx_ret_z"].to_numpy()
    is_eu = df["is_eu_session"].to_numpy()
    is_we = df["is_weekend"].to_numpy()

    n = len(df)
    sig = np.zeros(n, dtype=np.int8)
    f = cfg.family
    z1, z2, z3 = cfg.z1, cfg.z2, cfg.z3

    if f == "eur_basis_revert":
        cond = (np.abs(pz) >= z1) & (oiz >= z2)
        sig[cond] = -np.sign(pz[cond]).astype(np.int8)

    elif f == "eur_basis_cont":
        cond = (np.abs(pz) >= z1) & (np.sign(pz) == np.sign(fz)) & (fs >= 2) & (oiz >= z2)
        sig[cond] = np.sign(pz[cond]).astype(np.int8)

    elif f == "eur_funding_combo_revert":
        cond = (np.abs(fz) >= z1) & (np.abs(pz) >= z2) & (np.sign(fz) == np.sign(pz)) & (oiz >= z3)
        sig[cond] = -np.sign(fz[cond]).astype(np.int8)

    elif f == "eur_funding_combo_cont":
        cond = (np.abs(fz) >= z1) & (np.abs(pz) >= z2) & (np.sign(fz) == np.sign(pz)) & (oiz >= z3)
        sig[cond] = np.sign(fz[cond]).astype(np.int8)

    elif f == "eur_liq_exhaust_revert":
        cond = (ltz >= z1) & (np.abs(pz) >= z2)
        side = -np.sign(liz)
        # fallback side from premium when liq imbalance near zero
        side[(side == 0)] = -np.sign(pz[(side == 0)])
        sig[cond] = side[cond].astype(np.int8)

    elif f == "eur_session_revert":
        cond = (is_eu > 0) & (np.abs(pz) >= z1) & (oiz >= z2)
        sig[cond] = -np.sign(pz[cond]).astype(np.int8)

    elif f == "eur_weekend_revert":
        cond = (is_we > 0) & (np.abs(pz) >= z1)
        sig[cond] = -np.sign(pz[cond]).astype(np.int8)

    elif f == "eur_fxshock_revert":
        cond = (np.abs(fxz) >= z1) & (np.abs(pz) >= z2)
        sig[cond] = -np.sign(pz[cond]).astype(np.int8)

    elif f == "eur_composite_contra":
        score = pz + 0.7 * fz + 0.5 * liz
        cond = (np.abs(score) >= z1) & (oiz >= z2) & (np.abs(fxz) >= z3)
        sig[cond] = -np.sign(score[cond]).astype(np.int8)

    return sig


def make_configs() -> List[Config]:
    out: List[Config] = []

    for fam in ["eur_basis_revert", "eur_basis_cont"]:
        for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [-0.5, 0.0, 0.5, 1.0], [2, 4, 6, 8, 12]):
            out.append(Config(fam, z1, z2, 0.0, h))

    for fam in ["eur_funding_combo_revert", "eur_funding_combo_cont"]:
        for z1, z2, z3, h in itertools.product([1.0, 1.5, 2.0], [0.5, 1.0, 1.5], [0.0, 0.5, 1.0], [4, 8, 12]):
            out.append(Config(fam, z1, z2, z3, h))

    for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5, 3.0], [0.5, 1.0, 1.5], [2, 4, 6, 8, 12]):
        out.append(Config("eur_liq_exhaust_revert", z1, z2, 0.0, h))

    for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.0, 0.5, 1.0], [2, 4, 6, 8]):
        out.append(Config("eur_session_revert", z1, z2, 0.0, h))

    for z1, h in itertools.product([1.0, 1.5, 2.0, 2.5], [4, 8, 12, 24]):
        out.append(Config("eur_weekend_revert", z1, 0.0, 0.0, h))

    for z1, z2, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.5, 1.0, 1.5], [2, 4, 6, 8]):
        out.append(Config("eur_fxshock_revert", z1, z2, 0.0, h))

    for z1, z2, z3, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.0, 0.5, 1.0], [0.5, 1.0, 1.5], [4, 8, 12]):
        out.append(Config("eur_composite_contra", z1, z2, z3, h))

    return out


def eval_cfg(
    panels: Dict[str, pd.DataFrame],
    cfg: Config,
    train_frac: float,
    cost_bps: float,
    min_train_trades: int,
    min_test_trades: int,
    min_selected_pairs: int,
) -> dict | None:
    tr_rows = []
    te_rows = []

    for pair, df in panels.items():
        if len(df) < cfg.hold_h + 150:
            continue

        s = signal(df, cfg)
        cut = split_idx(len(df), train_frac)

        tr = make_trades(df["bb_close_usdt"].to_numpy()[:cut], s[:cut], cfg.hold_h, cost_bps)
        te = make_trades(df["bb_close_usdt"].to_numpy()[cut:], s[cut:], cfg.hold_h, cost_bps)

        if tr.size < 8 or te.size < min_test_trades:
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

    sp = set(sel["pair"])
    tr = trdf[trdf["pair"].isin(sp)].copy()
    te = tedf[tedf["pair"].isin(sp)].copy()
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
        "hold_h": cfg.hold_h,
        "n_selected_pairs": int(len(sp)),
        "selected_pairs": ",".join(sorted(sp)),
        "train_trade_bps": tr_mean,
        "train_trades": int(tr["n"].sum()),
        "test_trade_bps": te_mean,
        "test_trades": int(te["n"].sum()),
        "test_pf_pair_median": float(te["pf"].replace([np.inf, -np.inf], np.nan).dropna().median()),
        "test_positive_pair_share": float((te["mean"] > 0).mean()),
        "top_pair_share": top_share,
    }


def walk_forward(panels: Dict[str, pd.DataFrame], cfg: Config, pairs: List[str], cost_bps: float) -> dict:
    ppanels = [panels[p] for p in pairs if p in panels]
    if not ppanels:
        return {"wf_windows": 0, "wf_positive_share": 0.0}

    t0 = min(x.index.min() for x in ppanels)
    t1 = max(x.index.max() for x in ppanels)

    train_d, test_d, step_d = 60, 30, 15
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
            if len(w) < cfg.hold_h + 80:
                continue
            active += 1

            s = signal(w, cfg)
            split = int((tr_end - cur) / pd.Timedelta(hours=1))
            split = max(40, min(len(w) - 40, split))
            te = make_trades(w["bb_close_usdt"].to_numpy()[split:], s[split:], cfg.hold_h, cost_bps)
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


def cost_check(panels: Dict[str, pd.DataFrame], cfg: Config, pairs: List[str], train_frac: float, cost: float) -> float:
    tot = 0.0
    n = 0
    for p in pairs:
        df = panels.get(p)
        if df is None:
            continue
        s = signal(df, cfg)
        cut = split_idx(len(df), train_frac)
        te = make_trades(df["bb_close_usdt"].to_numpy()[cut:], s[cut:], cfg.hold_h, cost)
        if te.size:
            tot += float(te.sum())
            n += int(te.size)
    return float(tot / n) if n > 0 else np.nan


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    start = pd.Timestamp(args.start, tz="UTC")
    end = pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(hours=23)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    client = MongoClient(args.mongo_uri)
    db = client[db_name_from_uri(args.mongo_uri)]

    print("[1/6] Discovering EUR pairs...")
    bases = discover_pairs()
    print(f"  Matched Bitvavo/Bybit bases: {len(bases)}")
    if not bases:
        raise SystemExit("No Bitvavo EUR candle files found. Run backfill first.")

    print("[2/6] Building EURUSD synthetic series...")
    eurusd = load_eurusd_series(start, end)
    if eurusd.empty:
        raise SystemExit("Could not build EURUSD synthetic series (need BTC-EUR + BTC-USDT candles).")

    print("[3/6] Loading pair panels...")
    panels: Dict[str, pd.DataFrame] = {}
    for b in bases:
        p = load_panel(b, db, start, end, eurusd)
        if p is not None and len(p) >= args.min_bars:
            panels[f"{b}-USDT"] = p

    if not panels:
        raise SystemExit("No usable panels after feature merge.")

    # Liquidity rank by bybit notional
    rows = []
    for pair, p in panels.items():
        n = (p["bb_close_usdt"] * p["bb_vol"]).replace([np.inf, -np.inf], np.nan).dropna()
        if len(n) < 400:
            continue
        rows.append((pair, float(n.median())))
    keep = [x[0] for x in sorted(rows, key=lambda x: x[1], reverse=True)[: args.max_pairs]]
    panels = {p: panels[p] for p in keep if p in panels}
    print(f"  Final universe: {len(panels)} pairs")

    print("[4/6] Building config grid...")
    cfgs = make_configs()
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
        - 10.0 * df["top_pair_share"]
    )
    df = df.sort_values("rank_score", ascending=False).reset_index(drop=True)
    raw_path = out_dir / f"{stamp}_eur_raw.csv"
    df.to_csv(raw_path, index=False)

    print("[6/6] Walk-forward + cost checks...")
    top_rows = []
    for _, r in df.head(min(args.top_k_wf, len(df))).iterrows():
        cfg = Config(str(r["family"]), float(r["z1"]), float(r["z2"]), float(r["z3"]), int(r["hold_h"]))
        pairs = [x for x in str(r["selected_pairs"]).split(",") if x]

        wf = walk_forward(panels, cfg, pairs, args.cost_bps)
        c6 = cost_check(panels, cfg, pairs, args.train_frac, 6.0)

        row = r.to_dict()
        row.update(wf)
        row["test_trade_bps_cost_6"] = c6
        top_rows.append(row)

    top_df = pd.DataFrame(top_rows)
    top_path = out_dir / f"{stamp}_eur_top.csv"
    top_df.to_csv(top_path, index=False)

    cand = top_df[
        (top_df["test_trade_bps"] > 5.0)
        & (top_df["test_pf_pair_median"] >= 1.10)
        & (top_df["test_positive_pair_share"] >= 0.55)
        & (top_df["top_pair_share"] <= 0.65)
        & (top_df["wf_windows"] >= 4)
        & (top_df["wf_positive_share"] >= 0.50)
        & (top_df["test_trade_bps_cost_6"] > 0.0)
    ].copy()
    cand = cand.sort_values(["test_trade_bps", "wf_positive_share"], ascending=[False, False])

    cand_path = out_dir / f"{stamp}_eur_candidates.csv"
    cand.to_csv(cand_path, index=False)

    meta = {
        "timestamp_utc": stamp,
        "start": args.start,
        "end": args.end,
        "n_universe_pairs": len(panels),
        "universe_pairs": sorted(panels.keys()),
        "n_configs": len(cfgs),
        "n_pass_initial": int(len(df)),
        "n_top": int(len(top_df)),
        "n_candidates": int(len(cand)),
        "cost_bps": args.cost_bps,
    }
    meta_path = out_dir / f"{stamp}_eur_meta.json"
    with meta_path.open("w") as f:
        json.dump(meta, f, indent=2)

    print("\n=== SUMMARY ===")
    print(f"Initial pass: {len(df)}")
    print(f"Top enriched: {len(top_df)}")
    print(f"Candidates: {len(cand)}")
    if not cand.empty:
        cols = [
            "family", "z1", "z2", "z3", "hold_h", "n_selected_pairs",
            "test_trade_bps", "test_pf_pair_median", "test_positive_pair_share",
            "top_pair_share", "wf_positive_share", "test_trade_bps_cost_6",
        ]
        print(cand[cols].head(20).to_string(index=False))

    print("\nArtifacts:")
    print(raw_path)
    print(top_path)
    print(cand_path)
    print(meta_path)


if __name__ == "__main__":
    main()
