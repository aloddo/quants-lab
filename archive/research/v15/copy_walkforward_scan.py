#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.stats as ss


DEFAULT_COST = 0.001064  # 8.64 bps taker round-trip + 2.00 bps extra slippage


def prep(df: pd.DataFrame, cost: float) -> pd.DataFrame:
    d = df.copy()
    d["net_sum"] = d["sum_gross_ret"] - cost * d["n"]
    d["net_mean"] = d["net_sum"] / d["n"]
    d["gross_mean"] = d["sum_gross_ret"] / d["n"]
    return d


def combine(g: pd.DataFrame) -> pd.Series:
    n = float(g["n"].sum())
    s = float(g["net_sum"].sum())
    ssq = float((g["sum_gross_ret2"] - 2 * 0.0 * g["sum_gross_ret"]).sum())
    return pd.Series({
        "n": n,
        "net_sum": s,
        "net_mean": s / n if n else np.nan,
        "avg_source_notional": np.average(g["avg_source_notional"], weights=g["n"]) if n else np.nan,
        "source_notional": float(g["sum_source_notional"].sum()),
    })


def train_scores(train: pd.DataFrame, min_train_n: int) -> pd.DataFrame:
    g = train.groupby("entity", sort=False).agg(
        n=("n", "sum"),
        net_sum=("net_sum", "sum"),
        gross_sum=("sum_gross_ret", "sum"),
        sum_gross_ret2=("sum_gross_ret2", "sum"),
        source_notional=("sum_source_notional", "sum"),
        avg_source_notional=("avg_source_notional", "mean"),
    ).reset_index()
    g = g[g["n"] >= min_train_n].copy()
    if g.empty:
        return g
    g["score"] = g["gross_sum"] / g["n"]
    # t-stat on gross markout; cost is constant so it does not change dispersion.
    mean = g["gross_sum"] / g["n"]
    var = (g["sum_gross_ret2"] - g["n"] * mean * mean) / np.maximum(g["n"] - 1, 1)
    g["tstat"] = mean / np.sqrt(np.maximum(var, 1e-12) / g["n"])
    return g.sort_values(["score", "n"], ascending=[False, False])


def walk_one(d: pd.DataFrame, months: list[str], train_window: int, min_train_n: int,
             topk: int, min_tstat: float) -> dict | None:
    fold_rows = []
    selected_records = []
    for i, m in enumerate(months):
        if i < train_window:
            continue
        train_months = months[i - train_window:i]
        train = d[d["month"].isin(train_months)]
        test = d[d["month"] == m]
        if train.empty or test.empty:
            continue
        scores = train_scores(train, min_train_n)
        if min_tstat > -999:
            scores = scores[scores["tstat"] >= min_tstat]
        sel = scores.head(topk)
        if sel.empty:
            continue
        st = test[test["entity"].isin(sel["entity"])]
        rest = test[~test["entity"].isin(sel["entity"])]
        if st.empty:
            fold_rows.append({
                "month": m, "n": 0, "net_mean": np.nan, "rest_net_mean": np.nan,
                "n_entities": 0, "selected_train_n": float(sel["n"].sum()),
            })
            continue
        # Event-weighted selected return.
        n = st["n"].sum()
        net_mean = st["net_sum"].sum() / n
        # Clustered entity-month return: one observation per selected entity active in test.
        ent = st.groupby("entity", sort=False).apply(
            lambda x: x["net_sum"].sum() / x["n"].sum(), include_groups=False
        )
        rest_mean = rest["net_sum"].sum() / rest["n"].sum() if rest["n"].sum() else np.nan
        fold_rows.append({
            "month": m,
            "n": int(n),
            "net_mean": float(net_mean),
            "rest_net_mean": float(rest_mean),
            "entity_month_mean": float(ent.mean()),
            "entity_month_median": float(ent.median()),
            "n_entities": int(ent.shape[0]),
            "selected_train_n": float(sel["n"].sum()),
            "source_notional": float(st["sum_source_notional"].sum()),
        })
        for e in sel["entity"].tolist():
            selected_records.append({"test_month": m, "entity": e})
    if not fold_rows:
        return None
    fr = pd.DataFrame(fold_rows)
    active = fr[fr["n"] > 0].copy()
    if active.empty:
        return None
    total_n = active["n"].sum()
    # Month-level t-stat, weighted headline plus unweighted fold robustness.
    headline = np.average(active["net_mean"], weights=active["n"])
    folds = active["net_mean"].to_numpy()
    t = folds.mean() / (folds.std(ddof=1) / np.sqrt(len(folds))) if len(folds) > 1 and folds.std(ddof=1) > 0 else np.nan
    p_gt0 = ss.ttest_1samp(folds, 0.0, alternative="greater").pvalue if len(folds) > 1 else np.nan
    return {
        "oos_bps": headline * 1e4,
        "fold_mean_bps": folds.mean() * 1e4,
        "fold_t": t,
        "fold_p_gt0": p_gt0,
        "pos_folds": int((folds > 0).sum()),
        "folds": int(len(folds)),
        "n": int(total_n),
        "avg_n_per_fold": float(active["n"].mean()),
        "avg_entities_per_fold": float(active["n_entities"].mean()),
        "source_notional": float(active["source_notional"].sum()),
        "fold_detail": fr,
        "selected": pd.DataFrame(selected_records),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--agg", type=Path, default=Path("research/v15/out/copy_latency_monthly.parquet"))
    ap.add_argument("--out", type=Path, default=Path("research/v15/out/copy_walkforward_scan.csv"))
    ap.add_argument("--cost", type=float, default=DEFAULT_COST)
    ap.add_argument("--train-windows", type=int, nargs="*", default=[1, 2, 3])
    ap.add_argument("--min-train-n", type=int, nargs="*", default=[20, 50, 100, 250])
    ap.add_argument("--topk", type=int, nargs="*", default=[5, 10, 20, 40, 80])
    ap.add_argument("--min-tstat", type=float, nargs="*", default=[-999, 1.0, 2.0])
    args = ap.parse_args()

    df = prep(pd.read_parquet(args.agg), args.cost)
    months = sorted(df["month"].astype(str).unique())
    rows = []
    details = {}
    for (entity_type, horizon_s, min_notional), d in df.groupby(
        ["entity_type", "horizon_s", "min_notional"], sort=False
    ):
        d = d.copy()
        for tw in args.train_windows:
            for mn in args.min_train_n:
                for tk in args.topk:
                    for mt in args.min_tstat:
                        res = walk_one(d, months, tw, mn, tk, mt)
                        if res is None:
                            continue
                        key = (entity_type, horizon_s, min_notional, tw, mn, tk, mt)
                        row = {
                            "entity_type": entity_type,
                            "horizon_s": horizon_s,
                            "min_notional": min_notional,
                            "train_window": tw,
                            "min_train_n": mn,
                            "topk": tk,
                            "min_tstat": mt,
                        }
                        for k, v in res.items():
                            if k not in ("fold_detail", "selected"):
                                row[k] = v
                        rows.append(row)
                        details[key] = res
    out = pd.DataFrame(rows).sort_values(
        ["oos_bps", "fold_mean_bps", "n"], ascending=[False, False, False]
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print(out.head(50).to_string(index=False))
    print(f"wrote {args.out} rows={len(out):,}")


if __name__ == "__main__":
    main()
