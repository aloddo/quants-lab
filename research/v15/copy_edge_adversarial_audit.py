#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
from pathlib import Path

import numpy as np
import pandas as pd

from copy_latency_markouts import (
    DATA_DIR,
    DIR_SIGN,
    MAX_EXEC_GAP_MS,
    MAX_EXIT_GAP_MS,
    liquid_plain_mask,
    load_day,
    price_arrays,
)


DEFAULT_COST_BPS = 10.64


def month_start_ms(month: str) -> int:
    ts = pd.Timestamp(f"{month[:4]}-{month[4:]}-01", tz="UTC")
    return int(ts.timestamp() * 1000)


def next_month(month: str) -> str:
    ts = pd.Timestamp(f"{month[:4]}-{month[4:]}-01", tz="UTC") + pd.offsets.MonthBegin(1)
    return ts.strftime("%Y%m")


def mark_events_for_grid(
    path: str,
    next_path: str | None,
    latencies_ms: list[int],
    horizons_s: list[int],
    min_base_notional: float,
) -> pd.DataFrame:
    day = load_day(path, price_only=False)
    if day.empty:
        return pd.DataFrame()
    nxt = load_day(next_path, price_only=True) if next_path else pd.DataFrame()
    parr = price_arrays([day, nxt])
    ev = day[
        (day["crossed"] == True)
        & (day["dir"].isin(DIR_SIGN))
        & np.isfinite(day["notional"])
        & (day["notional"] >= min_base_notional)
    ].copy()
    if ev.empty:
        return pd.DataFrame()
    ev["sign"] = ev["dir"].map(DIR_SIGN).astype(np.int8)
    month = Path(path).stem[:6]
    boundary_ms = month_start_ms(next_month(month))
    rows: list[pd.DataFrame] = []
    for coin, g in ev.groupby("coin", sort=False):
        arr = parr.get(str(coin))
        if arr is None:
            continue
        times, prices = arr
        ev_t = g["time"].to_numpy(np.int64)
        signs = g["sign"].to_numpy(np.float64)
        base = g[["wallet", "coin", "time", "notional", "oid"]].copy()
        for latency_ms in latencies_ms:
            target_entry = ev_t + latency_ms
            entry_ix = np.searchsorted(times, target_entry, side="left")
            valid_entry = entry_ix < len(times)
            entry_px = np.full(len(g), np.nan)
            entry_time = np.full(len(g), -1, dtype=np.int64)
            if valid_entry.any():
                ii = entry_ix[valid_entry]
                entry_px[valid_entry] = prices[ii]
                entry_time[valid_entry] = times[ii]
            valid_entry &= (entry_time - target_entry) <= MAX_EXEC_GAP_MS
            for horizon_s in horizons_s:
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
                ok = valid & np.isfinite(entry_px) & np.isfinite(exit_px) & (entry_px > 0)
                if not ok.any():
                    continue
                idx = np.flatnonzero(ok)
                out = base.iloc[idx].copy()
                out["month"] = month
                out["latency_ms"] = latency_ms
                out["horizon_s"] = horizon_s
                out["entry_time"] = entry_time[idx]
                out["exit_time"] = exit_time[idx]
                out["entry_px"] = entry_px[idx]
                out["exit_px"] = exit_px[idx]
                out["gross_ret"] = signs[idx] * (exit_px[idx] / entry_px[idx] - 1.0)
                out["strict_pre_month"] = exit_time[idx] < boundary_ms
                rows.append(out)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def aggregate(marked: pd.DataFrame, min_notionals: list[float]) -> pd.DataFrame:
    rows = []
    for mn in min_notionals:
        d = marked[marked["notional"] >= mn]
        if d.empty:
            continue
        g = d.groupby(["month", "latency_ms", "horizon_s", "wallet"], sort=False).agg(
            n=("gross_ret", "size"),
            sum_gross_ret=("gross_ret", "sum"),
            sum_gross_ret2=("gross_ret", lambda x: float(np.square(x.to_numpy()).sum())),
            sum_source_notional=("notional", "sum"),
        ).reset_index()
        strict = d[d["strict_pre_month"]].groupby(["month", "latency_ms", "horizon_s", "wallet"], sort=False).agg(
            n_strict=("gross_ret", "size"),
            sum_gross_ret_strict=("gross_ret", "sum"),
            sum_gross_ret2_strict=("gross_ret", lambda x: float(np.square(x.to_numpy()).sum())),
        ).reset_index()
        g = g.merge(strict, on=["month", "latency_ms", "horizon_s", "wallet"], how="left")
        g[["n_strict", "sum_gross_ret_strict", "sum_gross_ret2_strict"]] = g[
            ["n_strict", "sum_gross_ret_strict", "sum_gross_ret2_strict"]
        ].fillna(0.0)
        g["min_notional"] = mn
        rows.append(g)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def selected_wallets(
    agg: pd.DataFrame,
    test_months: list[str],
    latency_ms: int,
    horizon_s: int,
    min_notional: float,
    train_window: int,
    min_train_n: int,
    topk: int,
    strict_boundary: bool,
) -> pd.DataFrame:
    d = agg[
        (agg["latency_ms"] == latency_ms)
        & (agg["horizon_s"] == horizon_s)
        & (agg["min_notional"] == min_notional)
    ].copy()
    n_col = "n_strict" if strict_boundary else "n"
    if strict_boundary:
        d["n"] = d["n_strict"]
        d["sum_gross_ret"] = d["sum_gross_ret_strict"]
        d["sum_gross_ret2"] = d["sum_gross_ret2_strict"]
    months = sorted(d["month"].astype(str).unique())
    out = []
    for m in test_months:
        if m not in months:
            continue
        i = months.index(m)
        if i < train_window:
            continue
        tr_months = months[i - train_window:i]
        tr = d[d["month"].isin(tr_months)].groupby("wallet", as_index=False).agg(
            n=("n", "sum"),
            gross=("sum_gross_ret", "sum"),
        )
        tr = tr[tr["n"] >= min_train_n].copy()
        if tr.empty:
            continue
        tr["score"] = tr["gross"] / tr["n"]
        tr = tr.sort_values(["score", "n"], ascending=[False, False]).head(topk)
        tr["test_month"] = m
        tr["rank"] = np.arange(1, len(tr) + 1)
        out.append(tr)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()


def build_all_selections(
    agg: pd.DataFrame,
    test_months: list[str],
    latencies_ms: list[int],
    horizons_s: list[int],
    min_notionals: list[float],
    topks: list[int],
) -> pd.DataFrame:
    rows = []
    for latency_ms in latencies_ms:
        for horizon_s in horizons_s:
            for mn in min_notionals:
                for topk in topks:
                    for strict in [False, True]:
                        sel = selected_wallets(
                            agg=agg,
                            test_months=test_months,
                            latency_ms=latency_ms,
                            horizon_s=horizon_s,
                            min_notional=mn,
                            train_window=1,
                            min_train_n=20,
                            topk=topk,
                            strict_boundary=strict,
                        )
                        if sel.empty:
                            continue
                        sel = sel[["test_month", "wallet", "rank", "score"]].copy()
                        sel["latency_ms"] = latency_ms
                        sel["horizon_s"] = horizon_s
                        sel["min_notional"] = mn
                        sel["topk"] = topk
                        sel["strict_boundary"] = strict
                        rows.append(sel)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def summarize_oos(folds: pd.DataFrame, cost_bps: float) -> pd.DataFrame:
    if folds.empty:
        return pd.DataFrame()
    d = folds.copy()
    d["net_sum"] = d["sum_gross_ret"] - cost_bps / 1e4 * d["n"]
    d["net_mean"] = d["net_sum"] / d["n"]
    rows = []
    keys = ["latency_ms", "horizon_s", "min_notional", "topk", "strict_boundary"]
    for key, g in d.groupby(keys, sort=False):
        total_n = g["n"].sum()
        if total_n <= 0:
            continue
        row = dict(zip(keys, key))
        row.update(
            gross_bps=g["sum_gross_ret"].sum() / total_n * 1e4,
            oos_bps=g["net_sum"].sum() / total_n * 1e4,
            fold_mean_bps=g["net_mean"].mean() * 1e4,
            pos_folds=int((g["net_mean"] > 0).sum()),
            folds=int(len(g)),
            n=int(total_n),
        )
        rows.append(row)
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", type=Path, default=Path("research/v15/out/audit"))
    ap.add_argument("--start", default="20251201")
    ap.add_argument("--end", default="20260527")
    ap.add_argument("--latencies-ms", type=int, nargs="*", default=[1000, 2000, 3000])
    ap.add_argument("--horizons-s", type=int, nargs="*", default=[1800, 3600, 7200])
    ap.add_argument("--min-notionals", dest="min_notionals", type=float, nargs="*", default=[500.0, 1000.0, 2000.0])
    ap.add_argument("--cost-bps", type=float, default=DEFAULT_COST_BPS)
    args = ap.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    all_files = sorted(glob.glob(str(DATA_DIR / "*.parquet")))
    next_by_file = {f: (all_files[i + 1] if i + 1 < len(all_files) else None) for i, f in enumerate(all_files)}
    files = [f for f in all_files if args.start <= Path(f).stem <= args.end]
    agg_parts = []
    print("first pass: wallet-month aggregates", flush=True)
    for i, f in enumerate(files, 1):
        marked = mark_events_for_grid(f, next_by_file.get(f), args.latencies_ms, args.horizons_s, min(args.min_notionals))
        print(f"{i:03d}/{len(files):03d} {Path(f).name} marked={len(marked):,}", flush=True)
        if marked.empty:
            continue
        agg_parts.append(aggregate(marked, args.min_notionals))
    agg = pd.concat(agg_parts, ignore_index=True) if agg_parts else pd.DataFrame()
    if not agg.empty:
        agg = agg.groupby(["month", "latency_ms", "horizon_s", "min_notional", "wallet"], as_index=False).agg(
            n=("n", "sum"),
            sum_gross_ret=("sum_gross_ret", "sum"),
            sum_gross_ret2=("sum_gross_ret2", "sum"),
            sum_source_notional=("sum_source_notional", "sum"),
            n_strict=("n_strict", "sum"),
            sum_gross_ret_strict=("sum_gross_ret_strict", "sum"),
            sum_gross_ret2_strict=("sum_gross_ret2_strict", "sum"),
        )
    agg.to_parquet(args.out_dir / "copy_audit_grid_wallet_month.parquet", index=False)

    test_months = ["202601", "202602", "202603", "202604", "202605"]
    sel = build_all_selections(agg, test_months, args.latencies_ms, args.horizons_s, args.min_notionals, [15, 20, 25])
    sel.to_csv(args.out_dir / "copy_audit_grid_selected_wallets.csv", index=False)
    print(f"selected variant-wallet rows={len(sel):,}", flush=True)

    fold_parts = []
    sel_keys = ["test_month", "wallet", "latency_ms", "horizon_s", "min_notional"]
    print("second pass: selected OOS fold returns", flush=True)
    for i, f in enumerate(files, 1):
        month = Path(f).stem[:6]
        if month not in test_months:
            continue
        marked = mark_events_for_grid(f, next_by_file.get(f), args.latencies_ms, args.horizons_s, min(args.min_notionals))
        print(f"{i:03d}/{len(files):03d} {Path(f).name} selected-pass marked={len(marked):,}", flush=True)
        if marked.empty:
            continue
        expanded = []
        for mn in args.min_notionals:
            d = marked[marked["notional"] >= mn].copy()
            if d.empty:
                continue
            d["min_notional"] = mn
            expanded.append(d)
        if not expanded:
            continue
        day = pd.concat(expanded, ignore_index=True)
        day["test_month"] = day["month"]
        hit = day.merge(sel, on=sel_keys, how="inner")
        if hit.empty:
            continue
        fold_parts.append(
            hit.groupby(["latency_ms", "horizon_s", "min_notional", "topk", "strict_boundary", "month"], as_index=False).agg(
                n=("gross_ret", "size"),
                sum_gross_ret=("gross_ret", "sum"),
            )
        )
    folds = pd.concat(fold_parts, ignore_index=True) if fold_parts else pd.DataFrame()
    if not folds.empty:
        folds = folds.groupby(["latency_ms", "horizon_s", "min_notional", "topk", "strict_boundary", "month"], as_index=False).agg(
            n=("n", "sum"),
            sum_gross_ret=("sum_gross_ret", "sum"),
        )
    folds.to_csv(args.out_dir / "copy_audit_grid_folds.csv", index=False)
    out = summarize_oos(folds, args.cost_bps).sort_values(["latency_ms", "horizon_s", "min_notional", "topk", "strict_boundary"])
    out.to_csv(args.out_dir / "copy_audit_grid_summary.csv", index=False)
    print(out.to_string(index=False))


if __name__ == "__main__":
    main()
