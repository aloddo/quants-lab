#!/usr/bin/env python3
"""
Bitvavo (spot EUR) vs Bybit (USDT perp) queue-aware microstructure model.

Goal:
Estimate whether sub-minute cross-venue spread opportunities remain profitable
once we model maker fill uncertainty on Bitvavo and realistic execution costs.

Data source:
  Mongo collection: arb_bv_eur_bb_perp_snapshots (typically ~5s cadence)

Model outline:
1) Signal when taker-taker spread exceeds threshold (choose stronger direction).
2) Place maker order on Bitvavo side (buy at bid OR sell at ask).
3) Fill occurs only if future quote path implies queue consumption/touch.
4) Hedge perp taker on fill; unwind both legs after fixed hold horizon.
5) Score expected PnL using queue-priority factor q in [0,1].
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from pymongo import MongoClient


OUT_DIR_DEFAULT = Path("app/data/cache/arb_queue_model")


@dataclass(frozen=True)
class Config:
    entry_bps: float
    wait_steps: int
    hold_steps: int
    fill_mode: str
    queue_p: float
    cost_bps: float


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Queue-aware Bitvavo/Bybit arb microstructure model")
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab"))
    p.add_argument("--lookback-hours", type=int, default=20, help="Hours of recent snapshots to analyze")
    p.add_argument("--max-symbols", type=int, default=80, help="Top symbols by doc count")
    p.add_argument("--min-symbol-samples", type=int, default=6000)
    p.add_argument("--min-signals", type=int, default=30)
    p.add_argument("--max-price-drift-bps", type=float, default=250.0, help="Filter out obvious mapping/outlier rows")
    p.add_argument("--full-grid", action="store_true", help="Run exhaustive config grid (slower)")
    p.add_argument("--out-dir", default=str(OUT_DIR_DEFAULT))
    return p.parse_args()


def db_name_from_uri(uri: str) -> str:
    if "/" not in uri:
        return "quants_lab"
    return uri.rsplit("/", 1)[-1] or "quants_lab"


def choose_symbol_universe(coll, start_ts: datetime, max_symbols: int, min_samples: int) -> List[str]:
    pipe = [
        {"$match": {"timestamp": {"$gte": start_ts}}},
        {"$group": {"_id": "$symbol_bb", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": max_symbols * 3},
    ]
    rows = list(coll.aggregate(pipe, allowDiskUse=True))
    rows = [r for r in rows if r.get("_id") and int(r.get("n", 0)) >= min_samples]
    rows = rows[:max_symbols]
    return [str(r["_id"]) for r in rows]


def load_symbol_df(coll, symbol: str, start_ts: datetime, max_price_drift_bps: float) -> pd.DataFrame:
    proj = {
        "_id": 0,
        "timestamp": 1,
        "symbol_bb": 1,
        "bv_bid_usd": 1,
        "bv_ask_usd": 1,
        "bb_perp_bid": 1,
        "bb_perp_ask": 1,
        "spread_buy_bv_sell_bb": 1,
        "spread_buy_bb_sell_bv": 1,
    }
    docs = list(coll.find({"timestamp": {"$gte": start_ts}, "symbol_bb": symbol}, proj).sort("timestamp", 1))
    if len(docs) < 1000:
        return pd.DataFrame()

    df = pd.DataFrame(docs)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    for c in ["bv_bid_usd", "bv_ask_usd", "bb_perp_bid", "bb_perp_ask", "spread_buy_bv_sell_bb", "spread_buy_bb_sell_bv"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.dropna(
        subset=[
            "bv_bid_usd",
            "bv_ask_usd",
            "bb_perp_bid",
            "bb_perp_ask",
            "spread_buy_bv_sell_bb",
            "spread_buy_bb_sell_bv",
        ]
    ).copy()
    if df.empty:
        return df

    df = df[(df["bv_bid_usd"] > 0) & (df["bv_ask_usd"] > 0) & (df["bb_perp_bid"] > 0) & (df["bb_perp_ask"] > 0)].copy()
    if df.empty:
        return df

    # Basic quote sanity.
    df = df[(df["bv_ask_usd"] >= df["bv_bid_usd"]) & (df["bb_perp_ask"] >= df["bb_perp_bid"])].copy()
    if df.empty:
        return df

    # Remove obvious mismatches / broken mappings.
    mid_bv = 0.5 * (df["bv_bid_usd"] + df["bv_ask_usd"])
    mid_bb = 0.5 * (df["bb_perp_bid"] + df["bb_perp_ask"])
    rel = ((mid_bb - mid_bv) / mid_bv) * 1e4
    df = df[rel.abs() <= max_price_drift_bps].copy()
    if df.empty:
        return df

    return df.reset_index(drop=True)


def fills_buy_maker(entry_bid: float, bid_j: float, ask_j: float, mode: str) -> bool:
    # Buy maker on bid:
    # touch: ask crosses through entry bid
    # consume: bid level depleted below entry bid OR touch
    if mode == "touch":
        return ask_j <= entry_bid
    return (bid_j < entry_bid) or (ask_j <= entry_bid)


def fills_sell_maker(entry_ask: float, bid_j: float, ask_j: float, mode: str) -> bool:
    # Sell maker on ask:
    # touch: bid crosses through entry ask
    # consume: ask level depleted above entry ask OR touch
    if mode == "touch":
        return bid_j >= entry_ask
    return (ask_j > entry_ask) or (bid_j >= entry_ask)


def simulate_symbol(df: pd.DataFrame, cfg: Config) -> Dict[str, float]:
    bv_bid = df["bv_bid_usd"].to_numpy(dtype=float)
    bv_ask = df["bv_ask_usd"].to_numpy(dtype=float)
    bb_bid = df["bb_perp_bid"].to_numpy(dtype=float)
    bb_ask = df["bb_perp_ask"].to_numpy(dtype=float)
    s1 = df["spread_buy_bv_sell_bb"].to_numpy(dtype=float)  # direction 1 trigger basis
    s2 = df["spread_buy_bb_sell_bv"].to_numpy(dtype=float)  # direction 2 trigger basis

    n = len(df)
    if n <= cfg.wait_steps + cfg.hold_steps + 5:
        return {}

    n_signals = 0
    n_fill_events = 0
    sum_realized = 0.0
    realized: List[float] = []

    i = 0
    while i < n - cfg.wait_steps - cfg.hold_steps - 1:
        side = 0
        if s1[i] >= cfg.entry_bps or s2[i] >= cfg.entry_bps:
            side = 1 if s1[i] >= s2[i] else -1
        if side == 0:
            i += 1
            continue

        n_signals += 1

        fill_idx = -1
        if side == 1:
            entry_bv = bv_bid[i]
            j_end = min(n - cfg.hold_steps - 1, i + cfg.wait_steps)
            for j in range(i + 1, j_end + 1):
                if fills_buy_maker(entry_bv, bv_bid[j], bv_ask[j], cfg.fill_mode):
                    fill_idx = j
                    break
        else:
            entry_bv = bv_ask[i]
            j_end = min(n - cfg.hold_steps - 1, i + cfg.wait_steps)
            for j in range(i + 1, j_end + 1):
                if fills_sell_maker(entry_bv, bv_bid[j], bv_ask[j], cfg.fill_mode):
                    fill_idx = j
                    break

        if fill_idx < 0:
            i += 1
            continue

        n_fill_events += 1
        exit_idx = fill_idx + cfg.hold_steps

        if side == 1:
            # Open: buy BV maker @ entry_bv, sell BB taker @ bb_bid(fill)
            entry_basis = (bb_bid[fill_idx] - entry_bv) / entry_bv * 1e4
            # Close: sell BV taker @ bv_bid(exit), buy BB taker @ bb_ask(exit)
            exit_basis = (bb_ask[exit_idx] - bv_bid[exit_idx]) / bv_bid[exit_idx] * 1e4
        else:
            # Open: sell BV maker @ entry_bv, buy BB taker @ bb_ask(fill)
            entry_basis = (entry_bv - bb_ask[fill_idx]) / bb_ask[fill_idx] * 1e4
            # Close: buy BV taker @ bv_ask(exit), sell BB taker @ bb_bid(exit)
            exit_basis = (bv_ask[exit_idx] - bb_bid[exit_idx]) / bb_bid[exit_idx] * 1e4

        pnl = float(entry_basis - exit_basis - cfg.cost_bps)
        realized.append(pnl)
        # queue_p models queue-priority probability conditional on consumption event.
        sum_realized += cfg.queue_p * pnl

        i = exit_idx + 1

    if n_signals == 0:
        return {}

    arr = np.asarray(realized, dtype=float)
    mean_realized = float(arr.mean()) if arr.size else np.nan
    win_realized = float((arr > 0).mean()) if arr.size else np.nan
    gp = arr[arr > 0].sum() if arr.size else 0.0
    gl = -arr[arr < 0].sum() if arr.size else 0.0
    pf_realized = float(gp / gl) if gl > 0 else np.nan

    fill_event_rate = float(n_fill_events / n_signals)
    eff_fill_rate = float(cfg.queue_p * fill_event_rate)

    return {
        "n_signals": int(n_signals),
        "n_fill_events": int(n_fill_events),
        "expected_fills": float(cfg.queue_p * n_fill_events),
        "fill_event_rate": fill_event_rate,
        "effective_fill_rate": eff_fill_rate,
        "mean_realized_trade_bps": mean_realized,
        "pf_realized_trade": pf_realized,
        "win_realized_trade": win_realized,
        "expected_bps_per_signal": float(sum_realized / n_signals),
        "expected_bps_per_event_fill": float(cfg.queue_p * mean_realized) if np.isfinite(mean_realized) else np.nan,
        "expected_total_bps": float(sum_realized),
    }


def build_configs(full_grid: bool) -> List[Config]:
    out: List[Config] = []
    if full_grid:
        entry_grid = [8.0, 12.0, 16.0, 20.0, 24.0, 30.0]
        wait_grid = [3, 6, 12, 24]          # 15s, 30s, 60s, 120s
        hold_grid = [12, 24, 36, 60, 120]   # 60s, 120s, 180s, 300s, 600s
        queue_grid = [1.0, 0.75, 0.5, 0.3]
        cost_grid = [10.0, 14.0, 18.0, 22.0, 26.0]
    else:
        # Fast discovery grid (reliable in minutes; iterate from here).
        entry_grid = [12.0, 18.0, 24.0, 30.0]
        wait_grid = [6, 12, 24]
        hold_grid = [12, 24, 60]
        queue_grid = [1.0, 0.6, 0.3]
        cost_grid = [12.0, 18.0, 24.0]

    for entry_bps, wait_steps, hold_steps, fill_mode, queue_p, cost_bps in itertools.product(
        entry_grid,
        wait_grid,
        hold_grid,
        ["touch", "consume"],
        queue_grid,
        cost_grid,
    ):
        out.append(Config(entry_bps, wait_steps, hold_steps, fill_mode, queue_p, cost_bps))
    return out


def main() -> None:
    args = parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    start_ts = datetime.now(timezone.utc) - timedelta(hours=args.lookback_hours)
    client = MongoClient(args.mongo_uri)
    db = client[db_name_from_uri(args.mongo_uri)]
    coll = db["arb_bv_eur_bb_perp_snapshots"]

    print("[1/4] Selecting symbol universe...")
    symbols = choose_symbol_universe(
        coll=coll,
        start_ts=start_ts,
        max_symbols=args.max_symbols,
        min_samples=args.min_symbol_samples,
    )
    print(f"  symbols: {len(symbols)}")
    if not symbols:
        raise SystemExit("No symbols satisfy sample requirements.")

    print("[2/4] Loading snapshot panels...")
    panels: Dict[str, pd.DataFrame] = {}
    for s in symbols:
        df = load_symbol_df(coll=coll, symbol=s, start_ts=start_ts, max_price_drift_bps=args.max_price_drift_bps)
        if len(df) >= args.min_symbol_samples:
            panels[s] = df
    print(f"  usable panels: {len(panels)}")
    if not panels:
        raise SystemExit("No usable panels after sanity filtering.")

    print("[3/4] Sweeping queue model configs...")
    cfgs = build_configs(full_grid=args.full_grid)
    rows: List[dict] = []

    for si, (sym, df) in enumerate(panels.items(), 1):
        for ci, cfg in enumerate(cfgs, 1):
            m = simulate_symbol(df, cfg)
            if not m:
                continue
            if m["n_signals"] < args.min_signals:
                continue
            rows.append(
                {
                    "symbol": sym,
                    "entry_bps": cfg.entry_bps,
                    "wait_steps": cfg.wait_steps,
                    "hold_steps": cfg.hold_steps,
                    "fill_mode": cfg.fill_mode,
                    "queue_p": cfg.queue_p,
                    "cost_bps": cfg.cost_bps,
                    **m,
                }
            )
        if si % 10 == 0:
            print(f"  symbols processed: {si}/{len(panels)}")

    if not rows:
        raise SystemExit("No rows survived min_signals filter.")

    df = pd.DataFrame(rows)
    # Favor true expected edge per signal and robust realized fill-trade quality.
    df["rank_score"] = (
        1.5 * df["expected_bps_per_signal"]
        + 0.3 * df["effective_fill_rate"] * 100.0
        + 0.25 * df["mean_realized_trade_bps"].fillna(0.0)
        + 1.0 * df["pf_realized_trade"].fillna(0.0)
    )
    df = df.sort_values("rank_score", ascending=False).reset_index(drop=True)

    raw_path = out_dir / f"{stamp}_raw.csv"
    df.to_csv(raw_path, index=False)

    # Robust candidates.
    cand = df[
        (df["expected_bps_per_signal"] > 0.0)
        & (df["mean_realized_trade_bps"] > 0.0)
        & (df["pf_realized_trade"] >= 1.05)
        & (df["effective_fill_rate"] >= 0.03)
        & (df["n_signals"] >= max(args.min_signals, 50))
    ].copy()
    cand = cand.sort_values(["expected_bps_per_signal", "effective_fill_rate"], ascending=[False, False])
    cand_path = out_dir / f"{stamp}_candidates.csv"
    cand.to_csv(cand_path, index=False)

    meta = {
        "timestamp_utc": stamp,
        "lookback_hours": args.lookback_hours,
        "n_symbols_universe": len(symbols),
        "n_symbols_usable": len(panels),
        "n_configs": len(cfgs),
        "n_rows": int(len(df)),
        "n_candidates": int(len(cand)),
        "start_ts_utc": start_ts.isoformat(),
    }
    meta_path = out_dir / f"{stamp}_meta.json"
    with meta_path.open("w") as f:
        json.dump(meta, f, indent=2)

    print("[4/4] Done.\n")
    print("=== SUMMARY ===")
    print(f"rows: {len(df)}")
    print(f"candidates: {len(cand)}")
    if not cand.empty:
        cols = [
            "symbol",
            "entry_bps",
            "wait_steps",
            "hold_steps",
            "fill_mode",
            "queue_p",
            "cost_bps",
            "n_signals",
            "effective_fill_rate",
            "expected_bps_per_signal",
            "mean_realized_trade_bps",
            "pf_realized_trade",
        ]
        print(cand[cols].head(20).to_string(index=False))
    else:
        print("No robust candidates under current assumptions.")

    print("\nArtifacts:")
    print(raw_path)
    print(cand_path)
    print(meta_path)


if __name__ == "__main__":
    main()
