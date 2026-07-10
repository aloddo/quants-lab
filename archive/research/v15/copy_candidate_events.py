#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
from pathlib import Path

import numpy as np
import pandas as pd

from copy_latency_markouts import (
    DATA_DIR, DIR_SIGN, LATENCY_MS, MAX_EXEC_GAP_MS, MAX_EXIT_GAP_MS,
    liquid_plain_mask, load_day, price_arrays,
)


COST = 0.001064


def selected_wallets(agg_path: Path, start_month: str, end_month: str, horizon_s: int,
                     min_notional: float, train_window: int, min_train_n: int,
                     topk: int) -> pd.DataFrame:
    cols = ["month", "horizon_s", "min_notional", "entity_type", "entity", "n", "sum_gross_ret"]
    df = pd.read_parquet(agg_path, columns=cols)
    df = df[(df["entity_type"] == "wallet") & (df["horizon_s"] == horizon_s)
            & (df["min_notional"] == min_notional)]
    months = sorted(m for m in df["month"].astype(str).unique() if start_month <= m <= end_month)
    out = []
    for i, m in enumerate(months):
        if i < train_window:
            continue
        tr_months = months[i - train_window:i]
        tr = df[df["month"].isin(tr_months)].groupby("entity", as_index=False).agg(
            n=("n", "sum"), gross=("sum_gross_ret", "sum")
        )
        tr = tr[tr["n"] >= min_train_n].copy()
        tr["score"] = tr["gross"] / tr["n"]
        tr = tr.sort_values(["score", "n"], ascending=[False, False]).head(topk).copy()
        tr["test_month"] = m
        tr["rank"] = np.arange(1, len(tr) + 1)
        out.append(tr.rename(columns={"entity": "wallet"}))
    return pd.concat(out, ignore_index=True)


def mark_selected_day(path: str, next_path: str | None, sel: pd.DataFrame, horizon_s: int,
                      min_notional: float) -> pd.DataFrame:
    month = Path(path).stem[:6]
    sm = sel[sel["test_month"] == month]
    if sm.empty:
        return pd.DataFrame()
    wallets = set(sm["wallet"])
    day = load_day(path, price_only=False)
    if day.empty:
        return pd.DataFrame()
    nxt = load_day(next_path, price_only=True) if next_path else pd.DataFrame()
    parr = price_arrays([day, nxt])

    ev = day[(day["crossed"] == True) & (day["dir"].isin(DIR_SIGN))
             & (day["wallet"].isin(wallets)) & (day["notional"] >= min_notional)].copy()
    if ev.empty:
        return pd.DataFrame()
    ev["sign"] = ev["dir"].map(DIR_SIGN).astype(np.int8)
    ev["leader_side"] = np.where(ev["sign"] > 0, "long", "short")
    ev = ev.merge(sm[["wallet", "rank", "score", "n"]].rename(columns={"n": "train_n"}),
                  on="wallet", how="left")

    # Crossed-fill trailing market volume for capacity. Use plain crossed fills only.
    vol_df = day[(day["crossed"] == True) & np.isfinite(day["notional"]) & (day["notional"] > 0)]
    vol_arr = {}
    for coin, g in vol_df.sort_values(["coin", "time"]).groupby("coin", sort=False):
        t = g["time"].to_numpy(np.int64)
        cs = np.r_[0.0, np.cumsum(g["notional"].to_numpy(np.float64))]
        vol_arr[str(coin)] = (t, cs)

    rows = []
    for coin, g in ev.groupby("coin", sort=False):
        arr = parr.get(str(coin))
        if arr is None:
            continue
        times, prices = arr
        vt, vcs = vol_arr.get(str(coin), (np.array([], dtype=np.int64), np.array([0.0])))
        ev_t = g["time"].to_numpy(np.int64)
        target_entry = ev_t + LATENCY_MS
        entry_ix = np.searchsorted(times, target_entry, side="left")
        valid_entry = entry_ix < len(times)
        entry_px = np.full(len(g), np.nan)
        entry_time = np.full(len(g), -1, dtype=np.int64)
        if valid_entry.any():
            ii = entry_ix[valid_entry]
            entry_px[valid_entry] = prices[ii]
            entry_time[valid_entry] = times[ii]
        valid_entry &= (entry_time - target_entry) <= MAX_EXEC_GAP_MS

        target_exit = ev_t + horizon_s * 1000
        exit_ix = np.searchsorted(times, target_exit, side="left")
        valid = valid_entry & (exit_ix < len(times))
        exit_px = np.full(len(g), np.nan)
        exit_time = np.full(len(g), -1, dtype=np.int64)
        if valid.any():
            jj = exit_ix[valid]
            exit_px[valid] = prices[jj]
            exit_time[valid] = times[jj]
        valid &= (exit_time - target_exit) <= MAX_EXIT_GAP_MS
        if not valid.any():
            continue
        gg = g.iloc[np.flatnonzero(valid)].copy()
        idx = np.flatnonzero(valid)
        sign = gg["sign"].to_numpy(np.float64)
        gross_ret = sign * (exit_px[idx] / entry_px[idx] - 1.0)
        entry_ts = entry_time[idx]

        if len(vt):
            right = np.searchsorted(vt, entry_ts, side="right")
            left = np.searchsorted(vt, entry_ts - 60_000, side="left")
            vol_60s = vcs[right] - vcs[left]
        else:
            vol_60s = np.full(len(gg), np.nan)

        gg["month"] = month
        gg["entry_time"] = entry_ts
        gg["entry_px"] = entry_px[idx]
        gg["exit_time"] = exit_time[idx]
        gg["exit_px"] = exit_px[idx]
        gg["gross_ret"] = gross_ret
        gg["net_ret"] = gross_ret - COST
        gg["mkt_vol_60s"] = vol_60s
        gg["cap_1pct_60s"] = 0.01 * vol_60s
        rows.append(gg[[
            "month", "wallet", "rank", "score", "train_n", "coin", "leader_side",
            "time", "entry_time", "exit_time", "notional", "mkt_vol_60s", "cap_1pct_60s",
            "entry_px", "exit_px", "gross_ret", "net_ret", "dir", "oid",
        ]])
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--agg", type=Path, default=Path("research/v15/out/copy_latency_monthly.parquet"))
    ap.add_argument("--out", type=Path, default=Path("research/v15/out/copy_candidate_wallet_1h_1k_events.parquet"))
    ap.add_argument("--start-month", default="202512")
    ap.add_argument("--end-month", default="202605")
    ap.add_argument("--horizon-s", type=int, default=3600)
    ap.add_argument("--min-notional", type=float, default=1000.0)
    ap.add_argument("--train-window", type=int, default=1)
    ap.add_argument("--min-train-n", type=int, default=20)
    ap.add_argument("--topk", type=int, default=20)
    args = ap.parse_args()

    sel = selected_wallets(args.agg, args.start_month, args.end_month, args.horizon_s,
                           args.min_notional, args.train_window, args.min_train_n, args.topk)
    sel_path = args.out.with_name(args.out.stem + "_selected_wallets.csv")
    sel.to_csv(sel_path, index=False)
    print(f"selected rows={len(sel)} -> {sel_path}")

    files = sorted(glob.glob(str(DATA_DIR / "*.parquet")))
    all_files = files
    next_by_file = {f: (all_files[i + 1] if i + 1 < len(all_files) else None)
                    for i, f in enumerate(all_files)}
    files = [f for f in files if args.start_month <= Path(f).stem[:6] <= args.end_month]
    parts = []
    for i, f in enumerate(files, 1):
        d = mark_selected_day(f, next_by_file.get(f), sel, args.horizon_s, args.min_notional)
        print(f"{i:03d}/{len(files):03d} {Path(f).name} events={len(d)}", flush=True)
        if len(d):
            parts.append(d)
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    out.to_parquet(args.out, index=False)
    print(f"wrote {args.out} rows={len(out):,}")


if __name__ == "__main__":
    main()
