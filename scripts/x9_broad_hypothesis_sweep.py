#!/usr/bin/env python3
"""
X9 Broad Hypothesis Sweep (no spot dependency)

Uses only broadly available local data:
  - Bybit perp 1h candles
  - Bybit funding rates
  - Bybit open interest
  - Coinalyze liquidations
  - Fear & Greed

Goal: discover additional candidate signal families on a wider universe.
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


@dataclass(frozen=True)
class Config:
    family: str
    z1: float
    z2: float
    z3: float
    hold_h: int


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab"))
    p.add_argument("--candles-dir", default="app/data/cache/candles")
    p.add_argument("--out-dir", default="app/data/cache/x9_hypothesis_factory")
    p.add_argument("--start", default="2025-04-21")
    p.add_argument("--end", default="2026-04-21")
    p.add_argument("--max-pairs", type=int, default=40)
    p.add_argument("--train-frac", type=float, default=0.65)
    p.add_argument("--cost-bps", type=float, default=4.0)
    p.add_argument("--min-bars", type=int, default=3000)
    p.add_argument("--min-train-trades", type=int, default=25)
    p.add_argument("--min-test-trades", type=int, default=10)
    p.add_argument("--min-selected-pairs", type=int, default=6)
    p.add_argument("--top-k-wf", type=int, default=80)
    return p.parse_args()


def db_name(uri: str) -> str:
    return uri.rsplit("/", 1)[-1] if "/" in uri else "quants_lab"


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


def make_trades(close: np.ndarray, sig: np.ndarray, hold: int, cost_bps: float) -> np.ndarray:
    n = len(close)
    if n <= hold + 2:
        return np.array([], dtype=float)
    rets: List[float] = []
    i = 0
    while i < n - hold - 1:
        side = sig[i]
        if side == 0:
            i += 1
            continue
        e = i + 1
        x = i + hold
        r = side * ((close[x] - close[e]) / close[e]) * 1e4 - 2.0 * cost_bps
        rets.append(float(r))
        i = x + 1
    return np.asarray(rets, dtype=float)


def pf(arr: np.ndarray) -> float:
    if arr.size == 0:
        return np.nan
    gp = arr[arr > 0].sum()
    gl = -arr[arr < 0].sum()
    if gl <= 0:
        return np.nan
    return float(gp / gl)


def build_universe(db, candles_dir: Path) -> List[str]:
    bybit = {p.name.split("|")[1] for p in candles_dir.glob("bybit_perpetual|*-USDT|1h.parquet")}
    f = set(db.bybit_funding_rates.distinct("pair"))
    oi = set(db.bybit_open_interest.distinct("pair"))
    liq = set(db.coinalyze_liquidations.distinct("pair"))
    return sorted(bybit & f & oi & liq)


def load_fg(db, start_ms: int, end_ms: int) -> pd.Series:
    docs = list(db.fear_greed_index.find({"timestamp_utc": {"$gte": start_ms - 60 * 24 * 3600 * 1000, "$lte": end_ms}}, {"_id": 0, "timestamp_utc": 1, "value": 1}).sort("timestamp_utc", 1))
    if not docs:
        return pd.Series(dtype=float)
    fg = pd.DataFrame(docs)
    fg["ts"] = pd.to_datetime(fg["timestamp_utc"], unit="ms", utc=True)
    return (fg.set_index("ts")["value"].astype(float) - 50.0) / 50.0


def load_panel(pair: str, db, cdir: Path, start: pd.Timestamp, end: pd.Timestamp, fg: pd.Series) -> pd.DataFrame | None:
    ppath = cdir / f"bybit_perpetual|{pair}|1h.parquet"
    if not ppath.exists():
        return None

    wstart = start - pd.Timedelta(days=45)
    s = int(wstart.timestamp())
    e = int(end.timestamp())
    sm = int(wstart.timestamp() * 1000)
    em = int(end.timestamp() * 1000)

    try:
        px = pd.read_parquet(ppath, columns=["timestamp", "close", "volume"])
    except Exception:
        return None
    px = px[(px["timestamp"] >= s) & (px["timestamp"] <= e)].copy()
    if len(px) < 2500:
        return None

    px["ts"] = pd.to_datetime(px["timestamp"], unit="s", utc=True)
    px = px.set_index("ts").sort_index()[["close", "volume"]].rename(columns={"close": "px", "volume": "vol"})

    fdocs = list(db.bybit_funding_rates.find({"pair": pair, "timestamp_utc": {"$gte": sm, "$lte": em}}, {"_id": 0, "timestamp_utc": 1, "funding_rate": 1}).sort("timestamp_utc", 1))
    odocs = list(db.bybit_open_interest.find({"pair": pair, "timestamp_utc": {"$gte": sm, "$lte": em}}, {"_id": 0, "timestamp_utc": 1, "oi_value": 1}).sort("timestamp_utc", 1))
    ldocs = list(db.coinalyze_liquidations.find({"pair": pair, "timestamp_utc": {"$gte": sm, "$lte": em}}, {"_id": 0, "timestamp_utc": 1, "long_liquidations_usd": 1, "short_liquidations_usd": 1, "total_liquidations_usd": 1}).sort("timestamp_utc", 1))

    if len(fdocs) < 120 or len(odocs) < 800 or len(ldocs) < 800:
        return None

    fdf = pd.DataFrame(fdocs)
    fdf["ts"] = pd.to_datetime(fdf["timestamp_utc"], unit="ms", utc=True)
    fdf = fdf.set_index("ts")[["funding_rate"]].sort_index().rename(columns={"funding_rate": "funding"})

    odf = pd.DataFrame(odocs)
    odf["ts"] = pd.to_datetime(odf["timestamp_utc"], unit="ms", utc=True)
    odf = odf.set_index("ts")[["oi_value"]].sort_index().resample("1h").last().ffill()

    ldf = pd.DataFrame(ldocs)
    ldf["ts"] = pd.to_datetime(ldf["timestamp_utc"], unit="ms", utc=True)
    ldf = ldf.set_index("ts")[["long_liquidations_usd", "short_liquidations_usd", "total_liquidations_usd"]].sort_index().resample("1h").last().fillna(0.0)

    df = px.copy()
    fund_1h = fdf.resample("1h").last().ffill()
    df["funding"] = fund_1h["funding"].reindex(df.index, method="ffill")

    fev = fdf.copy()
    fev["fr_z_ev"] = zscore(fev["funding"], 30, 15)
    fev["fr_streak_ev"] = streak_from_sign(np.sign(fev["funding"]))
    aligned = fev[["fr_z_ev", "fr_streak_ev"]].reindex(df.index, method="ffill")
    df["fr_z"] = aligned["fr_z_ev"]
    df["fr_streak"] = aligned["fr_streak_ev"]

    df = df.join(odf, how="left").join(ldf, how="left")
    df["fg_norm"] = fg.reindex(df.index, method="ffill") if not fg.empty else 0.0

    df["ret_8h"] = df["px"].pct_change(8)
    df["ret_8h_z"] = zscore(df["ret_8h"], 72, 24)
    oi_log = np.log(df["oi_value"].replace(0, np.nan)).ffill()
    df["oi_delta"] = oi_log.diff(1)
    df["oi_z"] = zscore(df["oi_delta"], 72, 24)

    lt = df["total_liquidations_usd"].fillna(0.0)
    ll = df["long_liquidations_usd"].fillna(0.0)
    ls = df["short_liquidations_usd"].fillna(0.0)
    df["liq_total_z"] = zscore(np.log1p(lt), 72, 24)
    df["liq_imb"] = (ll - ls) / (lt + 1.0)
    df["liq_imb_z"] = zscore(df["liq_imb"], 72, 24)
    df["vol_z"] = zscore(np.log1p(df["vol"].clip(lower=0.0)), 96, 32)

    df = df[(df.index >= start) & (df.index <= end)].copy()
    for c in ["fr_z", "fr_streak", "oi_z", "liq_total_z", "liq_imb_z", "ret_8h_z", "vol_z", "fg_norm"]:
        df[c] = df[c].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    df = df.dropna(subset=["px", "funding", "oi_value"])
    if len(df) < 2500:
        return None
    return df


def signal(df: pd.DataFrame, cfg: Config) -> np.ndarray:
    fr = df["funding"].to_numpy()
    frz = df["fr_z"].to_numpy()
    stk = df["fr_streak"].to_numpy()
    oiz = df["oi_z"].to_numpy()
    ltz = df["liq_total_z"].to_numpy()
    liz = df["liq_imb_z"].to_numpy()
    r8z = df["ret_8h_z"].to_numpy()
    vz = df["vol_z"].to_numpy()
    fg = df["fg_norm"].to_numpy()

    sig = np.zeros(len(df), dtype=np.int8)
    f = cfg.family
    a, b, c = cfg.z1, cfg.z2, cfg.z3

    if f == "funding_revert":
        cond = (np.abs(frz) >= a) & (stk >= 2) & (oiz >= b)
        sig[cond] = -np.sign(fr[cond]).astype(np.int8)
    elif f == "funding_cont":
        cond = (np.abs(frz) >= a) & (stk >= 2) & (oiz >= b)
        sig[cond] = np.sign(fr[cond]).astype(np.int8)
    elif f == "liq_exhaust_revert":
        cond = (ltz >= a) & (np.abs(liz) >= b)
        sig[cond] = -np.sign(liz[cond]).astype(np.int8)
    elif f == "liq_follow":
        cond = (ltz >= a) & (np.abs(liz) >= b) & (np.sign(liz) == np.sign(r8z))
        sig[cond] = np.sign(liz[cond]).astype(np.int8)
    elif f == "oi_breakout_follow":
        cond = (oiz >= a) & (np.abs(r8z) >= b) & (vz >= c)
        sig[cond] = np.sign(r8z[cond]).astype(np.int8)
    elif f == "oi_price_div_revert":
        cond = (oiz >= a) & (np.abs(r8z) >= b)
        sig[cond] = -np.sign(r8z[cond]).astype(np.int8)
    elif f == "sentiment_contra":
        cond = (np.abs(fg) >= a) & (np.abs(frz) >= b)
        sig[cond] = -np.sign(fg[cond]).astype(np.int8)
    elif f == "composite_contra":
        score = frz + 0.8 * liz
        cond = (np.abs(score) >= a) & (oiz >= b) & (ltz >= c)
        sig[cond] = -np.sign(score[cond]).astype(np.int8)
    return sig


def configs() -> List[Config]:
    out: List[Config] = []
    for fam in ["funding_revert", "funding_cont"]:
        for a, b, h in itertools.product([1.0, 1.5, 2.0, 2.5], [-0.5, 0.0, 0.5, 1.0], [4, 6, 8, 12]):
            out.append(Config(fam, a, b, 0.0, h))
    for fam in ["liq_exhaust_revert", "liq_follow"]:
        for a, b, h in itertools.product([1.0, 1.5, 2.0, 2.5, 3.0], [0.5, 1.0, 1.5], [2, 4, 6, 8, 12]):
            out.append(Config(fam, a, b, 0.0, h))
    for fam in ["oi_breakout_follow"]:
        for a, b, c, h in itertools.product([0.5, 1.0, 1.5, 2.0], [0.5, 1.0, 1.5], [0.0, 0.5, 1.0], [3, 6, 9, 12]):
            out.append(Config(fam, a, b, c, h))
    for fam in ["oi_price_div_revert"]:
        for a, b, h in itertools.product([0.5, 1.0, 1.5, 2.0], [0.5, 1.0, 1.5], [3, 6, 9, 12]):
            out.append(Config(fam, a, b, 0.0, h))
    for fam in ["sentiment_contra"]:
        for a, b, h in itertools.product([0.3, 0.5, 0.7, 0.9], [1.0, 1.5, 2.0], [4, 8, 12, 24]):
            out.append(Config(fam, a, b, 0.0, h))
    for fam in ["composite_contra"]:
        for a, b, c, h in itertools.product([1.0, 1.5, 2.0, 2.5], [0.0, 0.5, 1.0], [0.0, 0.5, 1.0], [4, 8, 12]):
            out.append(Config(fam, a, b, c, h))
    return out


def split_idx(n: int, frac: float) -> int:
    return int(max(80, min(n - 80, round(n * frac))))


def eval_cfg(panels: Dict[str, pd.DataFrame], cfg: Config, train_frac: float, cost_bps: float, min_train: int, min_test: int, min_pairs: int):
    tr_rows = []
    te_rows = []
    for pair, df in panels.items():
        if len(df) < cfg.hold_h + 150:
            continue
        sig = signal(df, cfg)
        c = split_idx(len(df), train_frac)
        tr = make_trades(df["px"].to_numpy()[:c], sig[:c], cfg.hold_h, cost_bps)
        te = make_trades(df["px"].to_numpy()[c:], sig[c:], cfg.hold_h, cost_bps)
        if tr.size < 8 or te.size < min_test:
            continue
        tr_rows.append((pair, tr.size, float(tr.mean()), pf(tr), float((tr > 0).mean()), float(tr.sum())))
        te_rows.append((pair, te.size, float(te.mean()), pf(te), float((te > 0).mean()), float(te.sum())))

    if not tr_rows:
        return None
    trdf = pd.DataFrame(tr_rows, columns=["pair", "n", "mean", "pf", "wr", "sum"])
    tedf = pd.DataFrame(te_rows, columns=["pair", "n", "mean", "pf", "wr", "sum"])
    sel = trdf[(trdf["mean"] > 0) & (trdf["pf"] >= 1.05) & (trdf["n"] >= min_train)]
    if len(sel) < min_pairs:
        return None

    sp = set(sel["pair"])
    tr = trdf[trdf["pair"].isin(sp)].copy()
    te = tedf[tedf["pair"].isin(sp)].copy()
    if te.empty:
        return None

    te_mean = float((te["mean"] * te["n"]).sum() / te["n"].sum())
    tr_mean = float((tr["mean"] * tr["n"]).sum() / tr["n"].sum())
    top_share = float(abs(te["sum"].max() / te["sum"].sum())) if te["sum"].sum() != 0 else np.nan

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
    pair_panels = [panels[p] for p in pairs if p in panels]
    if not pair_panels:
        return {"wf_windows": 0, "wf_positive_share": 0.0}

    t0 = min(df.index.min() for df in pair_panels)
    t1 = max(df.index.max() for df in pair_panels)
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
            te = make_trades(w["px"].to_numpy()[split:], s[split:], cfg.hold_h, cost_bps)
            if te.size > 0:
                arrs.append(te)
        if arrs and active >= 2:
            cat = np.concatenate(arrs)
            wins.append(float(cat.mean()))
        cur += pd.Timedelta(days=step_d)

    if not wins:
        return {"wf_windows": 0, "wf_positive_share": 0.0}
    a = np.asarray(wins)
    return {"wf_windows": int(a.size), "wf_positive_share": float((a > 0).mean())}


def cost_check(panels: Dict[str, pd.DataFrame], cfg: Config, pairs: List[str], train_frac: float, cost: float) -> float:
    tot = 0.0
    n = 0
    for p in pairs:
        df = panels.get(p)
        if df is None:
            continue
        s = signal(df, cfg)
        c = split_idx(len(df), train_frac)
        te = make_trades(df["px"].to_numpy()[c:], s[c:], cfg.hold_h, cost)
        if te.size:
            tot += float(te.sum())
            n += int(te.size)
    return float(tot / n) if n > 0 else np.nan


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    start = pd.Timestamp(args.start, tz="UTC")
    end = pd.Timestamp(args.end, tz="UTC") + pd.Timedelta(hours=23)

    client = MongoClient(args.mongo_uri)
    db = client[db_name(args.mongo_uri)]

    print("[1/6] universe + fg")
    uni = build_universe(db, Path(args.candles_dir))
    fg = load_fg(db, int(start.timestamp() * 1000), int(end.timestamp() * 1000))
    print("  base universe", len(uni))

    print("[2/6] load panels")
    panels: Dict[str, pd.DataFrame] = {}
    for p in uni:
        df = load_panel(p, db, Path(args.candles_dir), start, end, fg)
        if df is not None and len(df) >= args.min_bars:
            panels[p] = df
    print("  loaded panels", len(panels))
    if not panels:
        raise SystemExit("No panels")

    rows = []
    for p, df in panels.items():
        notional = (df["px"] * df["vol"]).replace([np.inf, -np.inf], np.nan).dropna()
        if len(notional) < 400:
            continue
        rows.append((p, float(notional.median())))
    keep = [x[0] for x in sorted(rows, key=lambda x: x[1], reverse=True)[: args.max_pairs]]
    panels = {p: panels[p] for p in keep if p in panels}
    print("  final universe", len(panels))

    print("[3/6] grid")
    cfgs = configs()
    print("  configs", len(cfgs))

    print("[4/6] sweep")
    out = []
    for i, cfg in enumerate(cfgs, 1):
        r = eval_cfg(
            panels, cfg, args.train_frac, args.cost_bps,
            args.min_train_trades, args.min_test_trades, args.min_selected_pairs
        )
        if r is not None:
            out.append(r)
        if i % 100 == 0:
            print(f"  {i}/{len(cfgs)} done, pass={len(out)}")

    if not out:
        raise SystemExit("No passing configs")

    df = pd.DataFrame(out)
    df["rank_score"] = 2.0 * df["test_trade_bps"] + 20.0 * df["test_pf_pair_median"] + 5.0 * df["test_positive_pair_share"] - 10.0 * df["top_pair_share"]
    df = df.sort_values("rank_score", ascending=False).reset_index(drop=True)

    raw_path = out_dir / f"{stamp}_broad_raw.csv"
    df.to_csv(raw_path, index=False)

    print("[5/6] wf + cost on top")
    top = []
    for _, r in df.head(min(args.top_k_wf, len(df))).iterrows():
        cfg = Config(str(r["family"]), float(r["z1"]), float(r["z2"]), float(r["z3"]), int(r["hold_h"]))
        pairs = [x for x in str(r["selected_pairs"]).split(",") if x]
        wf = walk_forward(panels, cfg, pairs, args.cost_bps)
        c6 = cost_check(panels, cfg, pairs, args.train_frac, 6.0)
        row = r.to_dict()
        row.update(wf)
        row["test_trade_bps_cost_6"] = c6
        top.append(row)

    top_df = pd.DataFrame(top)
    top_path = out_dir / f"{stamp}_broad_top.csv"
    top_df.to_csv(top_path, index=False)

    cand = top_df[
        (top_df["test_trade_bps"] > 6.0)
        & (top_df["test_pf_pair_median"] >= 1.10)
        & (top_df["test_positive_pair_share"] >= 0.55)
        & (top_df["top_pair_share"] <= 0.60)
        & (top_df["wf_windows"] >= 4)
        & (top_df["wf_positive_share"] >= 0.50)
        & (top_df["test_trade_bps_cost_6"] > 0.0)
    ].copy()
    cand = cand.sort_values(["test_trade_bps", "wf_positive_share"], ascending=[False, False])

    cand_path = out_dir / f"{stamp}_broad_candidates.csv"
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
    }
    meta_path = out_dir / f"{stamp}_broad_meta.json"
    with meta_path.open("w") as f:
        json.dump(meta, f, indent=2)

    print("\n=== SUMMARY ===")
    print("initial pass", len(df), "top", len(top_df), "candidates", len(cand))
    if not cand.empty:
        cols = ["family", "z1", "z2", "z3", "hold_h", "n_selected_pairs", "test_trade_bps", "test_pf_pair_median", "test_positive_pair_share", "top_pair_share", "wf_positive_share", "test_trade_bps_cost_6"]
        print(cand[cols].head(20).to_string(index=False))

    print("\nArtifacts:")
    print(raw_path)
    print(top_path)
    print(cand_path)
    print(meta_path)


if __name__ == "__main__":
    main()
