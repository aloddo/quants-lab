#!/usr/bin/env python3
"""
X9 Funding-Crowding Discovery (no-leakage holdout).

Research goal:
  Discover a Bybit-perp directional edge using only raw funding/OI + price data.

Method:
  1) Build per-pair funding event panels from:
       - Mongo: bybit_funding_rates, bybit_open_interest
       - Local cache: app/data/cache/candles/bybit_perpetual|PAIR|1h.parquet
  2) Engineer crowding features at funding timestamps:
       - funding z-score (rolling 30 events)
       - OI delta z-score (rolling 30 events)
       - funding sign streak length
  3) Sweep strategy configs.
  4) For each config, select pairs using TRAIN ONLY:
       - train_mean_trade_bps > 0
       - train_trades >= min_train_trades
  5) Evaluate selected-pair portfolio on TEST.

No test information is used in pair selection.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
from pymongo import MongoClient
from scipy import stats


@dataclass(frozen=True)
class StrategyConfig:
    family: str
    funding_z_threshold: float
    streak_min: int
    oi_z_min: float
    hold_events: int


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="X9 funding crowding discovery")
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab"))
    p.add_argument("--candles-dir", default="app/data/cache/candles")
    p.add_argument("--out-dir", default="app/data/cache/x9_funding_crowding")
    p.add_argument("--train-frac", type=float, default=0.70)
    p.add_argument("--cost-bps-oneway", type=float, default=4.0)
    p.add_argument("--min-bars", type=int, default=1500)
    p.add_argument("--min-events", type=int, default=180)
    p.add_argument("--min-train-trades", type=int, default=20)
    p.add_argument("--min-test-trades", type=int, default=5)
    p.add_argument("--min-selected-pairs", type=int, default=4)
    p.add_argument("--max-pairs", type=int, default=0, help="0 means no cap")
    return p.parse_args()


def _db_name_from_uri(uri: str) -> str:
    if "/" not in uri:
        return "quants_lab"
    return uri.rsplit("/", 1)[-1] or "quants_lab"


def compute_streak(sign_series: pd.Series) -> pd.Series:
    streak: list[int] = []
    cur = 0
    prev = 0
    for v in sign_series.astype(float).fillna(0.0).to_numpy():
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
    return pd.Series(streak, index=sign_series.index, dtype=float)


def load_pair_panel(
    pair: str,
    db,
    candles_dir: Path,
    min_bars: int,
    min_events: int,
) -> pd.DataFrame | None:
    candle_path = candles_dir / f"bybit_perpetual|{pair}|1h.parquet"
    if not candle_path.exists():
        return None

    try:
        candles = pd.read_parquet(candle_path, columns=["timestamp", "close"])
    except Exception:
        return None

    if len(candles) < min_bars:
        return None

    candles["ts"] = pd.to_datetime(candles["timestamp"], unit="s", utc=True)
    px = candles.set_index("ts")[["close"]].sort_index().rename(columns={"close": "px"})

    fdocs = list(
        db["bybit_funding_rates"]
        .find({"pair": pair}, {"_id": 0, "timestamp_utc": 1, "funding_rate": 1})
        .sort("timestamp_utc", 1)
    )
    odocs = list(
        db["bybit_open_interest"]
        .find({"pair": pair}, {"_id": 0, "timestamp_utc": 1, "oi_value": 1})
        .sort("timestamp_utc", 1)
    )

    if len(fdocs) < min_events or len(odocs) < min_events:
        return None

    fdf = pd.DataFrame(fdocs)
    odf = pd.DataFrame(odocs)
    if fdf.empty or odf.empty:
        return None

    fdf["ts"] = pd.to_datetime(fdf["timestamp_utc"], unit="ms", utc=True)
    odf["ts"] = pd.to_datetime(odf["timestamp_utc"], unit="ms", utc=True)
    fdf = fdf.set_index("ts")[["funding_rate"]].sort_index()
    odf = odf.set_index("ts")[["oi_value"]].sort_index()

    panel = fdf.join(odf.resample("1h").last().ffill(), how="left")
    panel["oi_value"] = panel["oi_value"].ffill()
    panel = panel.join(px, how="left")
    panel["px"] = panel["px"].ffill()
    panel = panel.dropna(subset=["funding_rate", "oi_value", "px"])
    if len(panel) < min_events:
        return None

    fr = panel["funding_rate"]
    fr_mu = fr.rolling(30, min_periods=15).mean()
    fr_sd = fr.rolling(30, min_periods=15).std().replace(0, np.nan)
    fr_z = (fr - fr_mu) / fr_sd

    oi_log = np.log(panel["oi_value"].replace(0, np.nan)).ffill()
    oi_delta = oi_log.diff(1)
    oi_mu = oi_delta.rolling(30, min_periods=15).mean()
    oi_sd = oi_delta.rolling(30, min_periods=15).std().replace(0, np.nan)
    oi_z = (oi_delta - oi_mu) / oi_sd

    panel = pd.DataFrame(
        {
            "pair": pair,
            "fr": fr,
            "fr_z": fr_z,
            "oi_z": oi_z,
            "streak": compute_streak(np.sign(fr)),
            "px": panel["px"],
        },
        index=panel.index,
    ).dropna()

    return panel if len(panel) >= min_events else None


def split_train_test(df: pd.DataFrame, train_frac: float) -> tuple[pd.DataFrame, pd.DataFrame]:
    t0 = df.index.min()
    t1 = df.index.max()
    split_ts = t0 + (t1 - t0) * train_frac
    return df[df.index < split_ts], df[df.index >= split_ts]


def make_signal(df: pd.DataFrame, cfg: StrategyConfig) -> np.ndarray:
    base = (
        (np.abs(df["fr_z"].to_numpy()) >= cfg.funding_z_threshold)
        & (df["streak"].to_numpy() >= cfg.streak_min)
        & (df["oi_z"].to_numpy() >= cfg.oi_z_min)
    )
    sign_fr = np.sign(df["fr"].to_numpy())

    if cfg.family == "crowd_revert":
        sig = -sign_fr
    elif cfg.family == "crowd_cont":
        sig = sign_fr
    else:
        raise ValueError(f"Unsupported family: {cfg.family}")

    return np.where(base, sig, 0).astype(int)


def make_trades(
    df: pd.DataFrame,
    sig: np.ndarray,
    hold_events: int,
    cost_bps_oneway: float,
) -> list[tuple[datetime.date, float]]:
    px = df["px"].to_numpy()
    idx = df.index
    n = len(df)
    out: list[tuple[datetime.date, float]] = []
    i = 0
    while i < n - hold_events - 1:
        side = sig[i]
        if side == 0:
            i += 1
            continue
        entry_i = i + 1
        exit_i = i + hold_events
        ret_bps = side * ((px[exit_i] - px[entry_i]) / px[entry_i]) * 1e4 - 2.0 * cost_bps_oneway
        out.append((idx[entry_i].date(), float(ret_bps)))
        i = exit_i + 1
    return out


def ttest_pvalue(sample: Iterable[float]) -> float:
    arr = np.asarray(list(sample), dtype=float)
    if arr.size < 12:
        return float("nan")
    return float(stats.ttest_1samp(arr, 0.0, nan_policy="omit").pvalue)


def evaluate_config_no_leakage(
    panels: dict[str, tuple[pd.DataFrame, pd.DataFrame]],
    cfg: StrategyConfig,
    cost_bps_oneway: float,
    min_train_trades: int,
    min_test_trades: int,
    min_selected_pairs: int,
) -> dict | None:
    per_pair = []
    train_rows = []
    test_rows = []

    for pair, (train_df, test_df) in panels.items():
        if len(train_df) <= cfg.hold_events + 5 or len(test_df) <= cfg.hold_events + 5:
            continue
        tr_sig = make_signal(train_df, cfg)
        te_sig = make_signal(test_df, cfg)
        tr_trades = make_trades(train_df, tr_sig, cfg.hold_events, cost_bps_oneway)
        te_trades = make_trades(test_df, te_sig, cfg.hold_events, cost_bps_oneway)
        if len(tr_trades) < 8 or len(te_trades) < min_test_trades:
            continue

        tr_mean = float(np.mean([x[1] for x in tr_trades]))
        te_mean = float(np.mean([x[1] for x in te_trades]))
        per_pair.append(
            {
                "pair": pair,
                "train_trade_bps": tr_mean,
                "test_trade_bps": te_mean,
                "train_trades": len(tr_trades),
                "test_trades": len(te_trades),
            }
        )
        train_rows.extend([(d, r, pair) for d, r in tr_trades])
        test_rows.extend([(d, r, pair) for d, r in te_trades])

    if not per_pair:
        return None

    pair_df = pd.DataFrame(per_pair)
    selected = pair_df[
        (pair_df["train_trade_bps"] > 0.0)
        & (pair_df["train_trades"] >= min_train_trades)
    ].copy()

    if len(selected) < min_selected_pairs:
        return None

    selected_pairs = set(selected["pair"])

    tr_df = pd.DataFrame(train_rows, columns=["day", "ret", "pair"])
    te_df = pd.DataFrame(test_rows, columns=["day", "ret", "pair"])
    tr_sel = tr_df[tr_df["pair"].isin(selected_pairs)].copy()
    te_sel = te_df[te_df["pair"].isin(selected_pairs)].copy()

    if tr_sel.empty or te_sel.empty:
        return None

    tr_daily = tr_sel.groupby("day")["ret"].sum()
    te_daily = te_sel.groupby("day")["ret"].sum()

    return {
        "config": asdict(cfg),
        "selected_pairs": sorted(selected_pairs),
        "pair_metrics": pair_df.sort_values("test_trade_bps", ascending=False).to_dict(orient="records"),
        "train_daily_bps": float(tr_daily.mean()),
        "train_trade_bps": float(tr_sel["ret"].mean()),
        "train_daily_p": ttest_pvalue(tr_daily.values),
        "train_trades": int(len(tr_sel)),
        "train_days": int(len(tr_daily)),
        "test_daily_bps": float(te_daily.mean()),
        "test_trade_bps": float(te_sel["ret"].mean()),
        "test_daily_p": ttest_pvalue(te_daily.values),
        "test_trades": int(len(te_sel)),
        "test_days": int(len(te_daily)),
        "test_pos_pair_share": float((selected["test_trade_bps"] > 0).mean()),
        "n_selected_pairs": int(len(selected_pairs)),
    }


def evaluate_fixed_pairs(
    panels: dict[str, tuple[pd.DataFrame, pd.DataFrame]],
    cfg: StrategyConfig,
    selected_pairs: list[str],
    cost_bps_oneway: float,
    hold_events_override: int | None = None,
) -> dict:
    hold_events = hold_events_override if hold_events_override is not None else cfg.hold_events
    run_cfg = StrategyConfig(
        family=cfg.family,
        funding_z_threshold=cfg.funding_z_threshold,
        streak_min=cfg.streak_min,
        oi_z_min=cfg.oi_z_min,
        hold_events=hold_events,
    )

    tr_rows = []
    te_rows = []
    for pair in selected_pairs:
        if pair not in panels:
            continue
        train_df, test_df = panels[pair]
        tr_sig = make_signal(train_df, run_cfg)
        te_sig = make_signal(test_df, run_cfg)
        tr_rows.extend([(pair, d, r) for d, r in make_trades(train_df, tr_sig, hold_events, cost_bps_oneway)])
        te_rows.extend([(pair, d, r) for d, r in make_trades(test_df, te_sig, hold_events, cost_bps_oneway)])

    tr_df = pd.DataFrame(tr_rows, columns=["pair", "day", "ret"])
    te_df = pd.DataFrame(te_rows, columns=["pair", "day", "ret"])

    tr_daily = tr_df.groupby("day")["ret"].sum() if not tr_df.empty else pd.Series(dtype=float)
    te_daily = te_df.groupby("day")["ret"].sum() if not te_df.empty else pd.Series(dtype=float)

    return {
        "family": run_cfg.family,
        "funding_z_threshold": run_cfg.funding_z_threshold,
        "streak_min": run_cfg.streak_min,
        "oi_z_min": run_cfg.oi_z_min,
        "hold_events": run_cfg.hold_events,
        "cost_bps_oneway": cost_bps_oneway,
        "train_daily_bps": float(tr_daily.mean()) if not tr_daily.empty else float("nan"),
        "train_trade_bps": float(tr_df["ret"].mean()) if not tr_df.empty else float("nan"),
        "train_daily_p": ttest_pvalue(tr_daily.values) if not tr_daily.empty else float("nan"),
        "train_trades": int(len(tr_df)),
        "test_daily_bps": float(te_daily.mean()) if not te_daily.empty else float("nan"),
        "test_trade_bps": float(te_df["ret"].mean()) if not te_df.empty else float("nan"),
        "test_daily_p": ttest_pvalue(te_daily.values) if not te_daily.empty else float("nan"),
        "test_trades": int(len(te_df)),
    }


def main() -> None:
    args = parse_args()
    candles_dir = Path(args.candles_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    client = MongoClient(args.mongo_uri)
    db = client[_db_name_from_uri(args.mongo_uri)]

    fund_pairs = set(db["bybit_funding_rates"].distinct("pair"))
    candle_pairs = {
        p.name.split("|")[1]
        for p in candles_dir.glob("bybit_perpetual|*-USDT|1h.parquet")
    }
    universe = sorted(fund_pairs & candle_pairs)
    if args.max_pairs > 0:
        universe = universe[: args.max_pairs]

    panels: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    for pair in universe:
        panel = load_pair_panel(
            pair=pair,
            db=db,
            candles_dir=candles_dir,
            min_bars=args.min_bars,
            min_events=args.min_events,
        )
        if panel is None:
            continue
        train_df, test_df = split_train_test(panel, args.train_frac)
        if len(train_df) < 80 or len(test_df) < 40:
            continue
        panels[pair] = (train_df, test_df)

    print(f"usable_pairs={len(panels)}")
    if not panels:
        raise SystemExit("No usable pair panels found.")

    cfgs = []
    for family in ["crowd_revert", "crowd_cont"]:
        for q in [1.0, 1.5, 2.0, 2.5]:
            for streak_min in [2, 3, 4, 5]:
                for oi_z_min in [0.0, 0.5, 1.0]:
                    for hold_events in [1, 2, 3, 4, 6]:
                        cfgs.append(
                            StrategyConfig(
                                family=family,
                                funding_z_threshold=q,
                                streak_min=streak_min,
                                oi_z_min=oi_z_min,
                                hold_events=hold_events,
                            )
                        )

    results = []
    for cfg in cfgs:
        out = evaluate_config_no_leakage(
            panels=panels,
            cfg=cfg,
            cost_bps_oneway=args.cost_bps_oneway,
            min_train_trades=args.min_train_trades,
            min_test_trades=args.min_test_trades,
            min_selected_pairs=args.min_selected_pairs,
        )
        if out is not None:
            results.append(out)

    if not results:
        raise SystemExit("No configs passed no-leakage selection thresholds.")

    summary_rows = []
    for r in results:
        c = r["config"]
        summary_rows.append(
            {
                "family": c["family"],
                "funding_z_threshold": c["funding_z_threshold"],
                "streak_min": c["streak_min"],
                "oi_z_min": c["oi_z_min"],
                "hold_events": c["hold_events"],
                "n_selected_pairs": r["n_selected_pairs"],
                "selected_pairs": ",".join(r["selected_pairs"]),
                "train_daily_bps": r["train_daily_bps"],
                "train_trade_bps": r["train_trade_bps"],
                "train_daily_p": r["train_daily_p"],
                "train_trades": r["train_trades"],
                "test_daily_bps": r["test_daily_bps"],
                "test_trade_bps": r["test_trade_bps"],
                "test_daily_p": r["test_daily_p"],
                "test_trades": r["test_trades"],
                "test_pos_pair_share": r["test_pos_pair_share"],
            }
        )

    summary_df = pd.DataFrame(summary_rows).sort_values("train_daily_bps", ascending=False)
    summary_path = out_dir / f"{stamp}_config_results.csv"
    summary_df.to_csv(summary_path, index=False)

    best = results[int(summary_df.index[0])]
    best_cfg = StrategyConfig(**best["config"])

    pair_metrics_path = out_dir / f"{stamp}_best_pair_metrics.csv"
    pd.DataFrame(best["pair_metrics"]).to_csv(pair_metrics_path, index=False)

    best_json_path = out_dir / f"{stamp}_best_config.json"
    with best_json_path.open("w") as f:
        json.dump(best, f, indent=2)

    # Robustness around selected config
    robustness_rows = []
    for cost in [2.0, 3.0, 4.0, 5.0, 6.0]:
        for hold in [4, 6, 8]:
            robustness_rows.append(
                evaluate_fixed_pairs(
                    panels=panels,
                    cfg=best_cfg,
                    selected_pairs=best["selected_pairs"],
                    cost_bps_oneway=cost,
                    hold_events_override=hold,
                )
            )
    robustness_df = pd.DataFrame(robustness_rows)
    robustness_path = out_dir / f"{stamp}_best_robustness_sweep.csv"
    robustness_df.to_csv(robustness_path, index=False)

    print("\nTop 10 configs by TRAIN daily bps:")
    print(
        summary_df[
            [
                "family",
                "funding_z_threshold",
                "streak_min",
                "oi_z_min",
                "hold_events",
                "n_selected_pairs",
                "train_daily_bps",
                "test_daily_bps",
                "test_daily_p",
            ]
        ]
        .head(10)
        .to_string(index=False)
    )
    print("\nSelected config:")
    print(json.dumps(best, indent=2))
    print("\nArtifacts:")
    print(summary_path)
    print(pair_metrics_path)
    print(best_json_path)
    print(robustness_path)


if __name__ == "__main__":
    main()

