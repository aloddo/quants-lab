#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import os
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd


DATA_DIR = Path("app/data/hl_s3_fills_v2")
OUT_DIR = Path("research/v15/out")

LATENCY_MS = 2_000
MAX_EXEC_GAP_MS = 10_000
MAX_EXIT_GAP_MS = 60_000

HORIZONS_S = [60, 300, 900, 3600, 14400]
MIN_NOTIONALS = [0.0, 100.0, 1_000.0, 5_000.0, 10_000.0]

DIR_SIGN = {
    "Open Long": 1,
    "Short > Long": 1,
    "Open Short": -1,
    "Long > Short": -1,
}


def yyyymm(path: str | Path) -> str:
    return Path(path).stem[:6]


def liquid_plain_mask(s: pd.Series) -> pd.Series:
    c = s.astype(str)
    return (~c.str.contains(":", regex=False)) & (~c.str.startswith("@"))


def load_day(path: str | Path, price_only: bool = False) -> pd.DataFrame:
    if price_only:
        cols = ["coin", "price", "time", "size"]
    else:
        cols = ["wallet", "coin", "dir", "crossed", "notional", "price", "time", "size", "oid"]
    df = pd.read_parquet(path, columns=cols)
    df = df[liquid_plain_mask(df["coin"])]
    if df.empty:
        return df
    df["px"] = pd.to_numeric(df["price"], errors="coerce")
    df = df[np.isfinite(df["px"]) & (df["px"] > 0)]
    return df


def price_arrays(frames: list[pd.DataFrame]) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    pxdf = pd.concat([f[["coin", "time", "px"]] for f in frames if f is not None and not f.empty],
                     ignore_index=True)
    out: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    if pxdf.empty:
        return out
    pxdf = pxdf.sort_values(["coin", "time"], kind="mergesort")
    for coin, g in pxdf.groupby("coin", sort=False):
        out[str(coin)] = (
            g["time"].to_numpy(dtype=np.int64, copy=True),
            g["px"].to_numpy(dtype=np.float64, copy=True),
        )
    return out


def mark_coin_events(g: pd.DataFrame, times: np.ndarray, prices: np.ndarray,
                     horizons_s: list[int]) -> pd.DataFrame:
    ev_t = g["time"].to_numpy(dtype=np.int64)
    signs = g["sign"].to_numpy(dtype=np.float64)
    entry_target = ev_t + LATENCY_MS
    entry_ix = np.searchsorted(times, entry_target, side="left")
    valid_entry = entry_ix < len(times)
    entry_px = np.full(len(g), np.nan)
    entry_delay = np.full(len(g), np.nan)
    if valid_entry.any():
        ii = entry_ix[valid_entry]
        entry_px[valid_entry] = prices[ii]
        entry_delay[valid_entry] = times[ii] - entry_target[valid_entry]
    valid_entry &= entry_delay <= MAX_EXEC_GAP_MS

    out = g[["wallet", "coin", "time", "notional"]].copy()
    out["entry_delay_ms"] = entry_delay
    for h in horizons_s:
        target = ev_t + h * 1000
        ix = np.searchsorted(times, target, side="left")
        valid = valid_entry & (ix < len(times))
        exit_px = np.full(len(g), np.nan)
        exit_delay = np.full(len(g), np.nan)
        if valid.any():
            jj = ix[valid]
            exit_px[valid] = prices[jj]
            exit_delay[valid] = times[jj] - target[valid]
        valid &= exit_delay <= MAX_EXIT_GAP_MS
        ret = np.full(len(g), np.nan)
        ok = valid & np.isfinite(entry_px) & np.isfinite(exit_px) & (entry_px > 0)
        ret[ok] = signs[ok] * (exit_px[ok] / entry_px[ok] - 1.0)
        out[f"gross_ret_{h}s"] = ret
    return out


def aggregate_events(marked: pd.DataFrame, month: str, min_notionals: list[float],
                     horizons_s: list[int]) -> list[dict]:
    rows: list[dict] = []
    if marked.empty:
        return rows

    for min_notional in min_notionals:
        m = marked[marked["notional"] >= min_notional]
        if m.empty:
            continue
        for h in horizons_s:
            col = f"gross_ret_{h}s"
            mh = m[["wallet", "coin", "notional", col]].dropna(subset=[col])
            if mh.empty:
                continue
            for entity_type, keys in (("wallet", ["wallet"]), ("wallet_coin", ["wallet", "coin"])):
                agg = mh.groupby(keys, sort=False).agg(
                    n=(col, "size"),
                    sum_gross_ret=(col, "sum"),
                    sum_gross_ret2=(col, lambda x: float(np.square(x.to_numpy()).sum())),
                    sum_source_notional=("notional", "sum"),
                    avg_source_notional=("notional", "mean"),
                ).reset_index()
                agg["month"] = month
                agg["horizon_s"] = h
                agg["min_notional"] = min_notional
                agg["entity_type"] = entity_type
                if entity_type == "wallet":
                    agg["entity"] = agg["wallet"]
                    agg["coin"] = ""
                else:
                    agg["entity"] = agg["wallet"].astype(str) + "|" + agg["coin"].astype(str)
                rows.extend(agg[[
                    "month", "horizon_s", "min_notional", "entity_type", "entity",
                    "wallet", "coin", "n", "sum_gross_ret", "sum_gross_ret2",
                    "sum_source_notional", "avg_source_notional",
                ]].to_dict("records"))
    return rows


def process_file(path: str, next_path: str | None, args) -> tuple[list[dict], dict]:
    day = load_day(path, price_only=False)
    stats = {"file": Path(path).name, "rows": int(len(day))}
    if day.empty:
        return [], stats

    nxt = load_day(next_path, price_only=True) if next_path else pd.DataFrame()
    parr = price_arrays([day, nxt])

    ev = day[(day["crossed"] == True) & (day["dir"].isin(DIR_SIGN))].copy()
    ev["sign"] = ev["dir"].map(DIR_SIGN).astype(np.int8)
    ev = ev[np.isfinite(ev["notional"]) & (ev["notional"] > 0)]
    if args.max_events_per_wallet_coin_day:
        ev = ev.sort_values(["wallet", "coin", "time"], kind="mergesort")
        ev["_rank_wc_day"] = ev.groupby(["wallet", "coin"]).cumcount()
        ev = ev[ev["_rank_wc_day"] < args.max_events_per_wallet_coin_day]
    stats["events"] = int(len(ev))

    parts: list[pd.DataFrame] = []
    for coin, g in ev.groupby("coin", sort=False):
        arr = parr.get(str(coin))
        if arr is None:
            continue
        parts.append(mark_coin_events(g, arr[0], arr[1], args.horizons_s))
    if not parts:
        return [], stats
    marked = pd.concat(parts, ignore_index=True)
    stats["marked"] = int(len(marked))
    rows = aggregate_events(marked, yyyymm(path), args.min_notionals, args.horizons_s)
    stats["agg_rows"] = int(len(rows))
    return rows, stats


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=str, default="")
    ap.add_argument("--end", type=str, default="")
    ap.add_argument("--out", type=Path, default=OUT_DIR / "copy_latency_monthly.parquet")
    ap.add_argument("--horizons-s", type=int, nargs="*", default=HORIZONS_S)
    ap.add_argument("--min-notionals", type=float, nargs="*", default=MIN_NOTIONALS)
    ap.add_argument("--max-events-per-wallet-coin-day", type=int, default=0,
                    help="Optional anti-spam cap applied before aggregation; 0 disables.")
    args = ap.parse_args()

    files = sorted(glob.glob(str(DATA_DIR / "*.parquet")))
    if args.start:
        files = [f for f in files if Path(f).stem >= args.start]
    if args.end:
        files = [f for f in files if Path(f).stem <= args.end]
    all_files = sorted(glob.glob(str(DATA_DIR / "*.parquet")))
    next_by_file = {f: (all_files[i + 1] if i + 1 < len(all_files) else None)
                    for i, f in enumerate(all_files)}

    args.out.parent.mkdir(parents=True, exist_ok=True)
    chunks = []
    for i, f in enumerate(files, 1):
        rows, stats = process_file(f, next_by_file.get(f), args)
        print(f"{i:03d}/{len(files):03d} {stats}", flush=True)
        if rows:
            chunks.append(pd.DataFrame(rows))
    if chunks:
        out = pd.concat(chunks, ignore_index=True)
        keys = ["month", "horizon_s", "min_notional", "entity_type", "entity", "wallet", "coin"]
        out = out.groupby(keys, sort=False, as_index=False).agg(
            n=("n", "sum"),
            sum_gross_ret=("sum_gross_ret", "sum"),
            sum_gross_ret2=("sum_gross_ret2", "sum"),
            sum_source_notional=("sum_source_notional", "sum"),
        )
        out["avg_source_notional"] = out["sum_source_notional"] / out["n"].clip(lower=1)
    else:
        out = pd.DataFrame()
    out.to_parquet(args.out, index=False)
    print(f"wrote {args.out} rows={len(out):,}")


if __name__ == "__main__":
    main()
