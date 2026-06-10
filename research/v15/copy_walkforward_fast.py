#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.stats as ss


DEFAULT_COST = 0.001064


def scan_matrix(d: pd.DataFrame, months: list[str], cost: float, train_windows: list[int],
                min_train_ns: list[int], topks: list[int], min_tstats: list[float]) -> list[dict]:
    eidx, entities = pd.factorize(d["entity"], sort=False)
    midx = d["month"].astype(str).map({m: i for i, m in enumerate(months)}).to_numpy()
    e = len(entities)
    m = len(months)
    shape = (e, m)
    n = np.zeros(shape, dtype=np.float64)
    gross = np.zeros(shape, dtype=np.float64)
    gross2 = np.zeros(shape, dtype=np.float64)
    source = np.zeros(shape, dtype=np.float64)
    np.add.at(n, (eidx, midx), d["n"].to_numpy(np.float64))
    np.add.at(gross, (eidx, midx), d["sum_gross_ret"].to_numpy(np.float64))
    np.add.at(gross2, (eidx, midx), d["sum_gross_ret2"].to_numpy(np.float64))
    np.add.at(source, (eidx, midx), d["sum_source_notional"].to_numpy(np.float64))
    net = gross - cost * n

    rows: list[dict] = []
    for tw in train_windows:
        for test_i in range(tw, m):
            tr = slice(test_i - tw, test_i)
            train_n = n[:, tr].sum(axis=1)
            train_gross = gross[:, tr].sum(axis=1)
            train_gross2 = gross2[:, tr].sum(axis=1)
            valid_n_base = train_n
            score = np.full(e, -np.inf)
            okn = train_n > 0
            score[okn] = train_gross[okn] / train_n[okn]
            mean = np.zeros(e)
            mean[okn] = score[okn]
            var = np.full(e, np.inf)
            vv = train_n > 1
            var[vv] = (train_gross2[vv] - train_n[vv] * mean[vv] * mean[vv]) / (train_n[vv] - 1)
            tstat = np.full(e, -np.inf)
            denom = np.sqrt(np.maximum(var, 1e-12) / np.maximum(train_n, 1))
            tstat[okn] = mean[okn] / denom[okn]

            test_n_all = n[:, test_i]
            test_net_all = net[:, test_i]
            rest_total_n_all = test_n_all.sum()
            rest_total_net_all = test_net_all.sum()
            for mn in min_train_ns:
                base = valid_n_base >= mn
                if not base.any():
                    continue
                for mt in min_tstats:
                    cand = base & (tstat >= mt)
                    if not cand.any():
                        continue
                    cand_idx = np.flatnonzero(cand)
                    # highest score first; stable enough for research ranking
                    order = cand_idx[np.argsort(score[cand_idx])[::-1]]
                    for tk in topks:
                        sel = order[:tk]
                        if sel.size == 0:
                            continue
                        tn = test_n_all[sel]
                        active = tn > 0
                        if not active.any():
                            continue
                        sel_active = sel[active]
                        total_n = test_n_all[sel_active].sum()
                        total_net = test_net_all[sel_active].sum()
                        rest_n = rest_total_n_all - total_n
                        rest_net = rest_total_net_all - total_net
                        ent_means = test_net_all[sel_active] / test_n_all[sel_active]
                        rows.append({
                            "train_window": tw,
                            "min_train_n": mn,
                            "topk": tk,
                            "min_tstat": mt,
                            "month": months[test_i],
                            "n": int(total_n),
                            "net_mean": float(total_net / total_n),
                            "rest_net_mean": float(rest_net / rest_n) if rest_n > 0 else np.nan,
                            "entity_month_mean": float(ent_means.mean()),
                            "entity_month_median": float(np.median(ent_means)),
                            "n_entities": int(sel_active.size),
                            "selected_train_n": float(train_n[sel].sum()),
                            "source_notional": float(source[sel_active, test_i].sum()),
                        })
    return rows


def summarize(folds: pd.DataFrame) -> pd.DataFrame:
    out = []
    keys = ["entity_type", "horizon_s", "min_notional", "train_window", "min_train_n", "topk", "min_tstat"]
    for key, g in folds.groupby(keys, sort=False):
        total_n = g["n"].sum()
        if total_n <= 0:
            continue
        net = np.average(g["net_mean"], weights=g["n"])
        f = g["net_mean"].to_numpy()
        rest = np.average(g["rest_net_mean"].dropna(), weights=g.loc[g["rest_net_mean"].notna(), "n"]) \
            if g["rest_net_mean"].notna().any() else np.nan
        t = f.mean() / (f.std(ddof=1) / np.sqrt(len(f))) if len(f) > 1 and f.std(ddof=1) > 0 else np.nan
        p = ss.ttest_1samp(f, 0.0, alternative="greater").pvalue if len(f) > 1 else np.nan
        row = dict(zip(keys, key))
        row.update({
            "oos_bps": net * 1e4,
            "rest_bps": rest * 1e4 if rest == rest else np.nan,
            "edge_vs_rest_bps": (net - rest) * 1e4 if rest == rest else np.nan,
            "fold_mean_bps": f.mean() * 1e4,
            "fold_t": t,
            "fold_p_gt0": p,
            "pos_folds": int((f > 0).sum()),
            "folds": int(len(f)),
            "n": int(total_n),
            "avg_n_per_fold": float(g["n"].mean()),
            "avg_entities_per_fold": float(g["n_entities"].mean()),
            "source_notional": float(g["source_notional"].sum()),
        })
        out.append(row)
    return pd.DataFrame(out)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--agg", type=Path, default=Path("research/v15/out/copy_latency_monthly.parquet"))
    ap.add_argument("--out", type=Path, default=Path("research/v15/out/copy_walkforward_fast.csv"))
    ap.add_argument("--folds-out", type=Path, default=Path("research/v15/out/copy_walkforward_fast_folds.parquet"))
    ap.add_argument("--cost", type=float, default=DEFAULT_COST)
    ap.add_argument("--start-month", type=str, default="")
    ap.add_argument("--end-month", type=str, default="")
    ap.add_argument("--train-windows", type=int, nargs="*", default=[1, 2, 3])
    ap.add_argument("--min-train-n", type=int, nargs="*", default=[20, 50, 100, 250])
    ap.add_argument("--topk", type=int, nargs="*", default=[5, 10, 20, 40, 80])
    ap.add_argument("--min-tstat", type=float, nargs="*", default=[-999, 1.0, 2.0])
    args = ap.parse_args()

    cols = ["month", "horizon_s", "min_notional", "entity_type", "entity", "n",
            "sum_gross_ret", "sum_gross_ret2", "sum_source_notional"]
    df = pd.read_parquet(args.agg, columns=cols)
    if args.start_month:
        df = df[df["month"].astype(str) >= args.start_month]
    if args.end_month:
        df = df[df["month"].astype(str) <= args.end_month]
    months = sorted(df["month"].astype(str).unique())
    all_folds = []
    for cfg, d in df.groupby(["entity_type", "horizon_s", "min_notional"], sort=False):
        entity_type, horizon_s, min_notional = cfg
        rows = scan_matrix(d, months, args.cost, args.train_windows, args.min_train_n,
                           args.topk, args.min_tstat)
        if rows:
            f = pd.DataFrame(rows)
            f["entity_type"] = entity_type
            f["horizon_s"] = horizon_s
            f["min_notional"] = min_notional
            all_folds.append(f)
        print(f"scanned {cfg} rows={len(d):,} folds={len(rows):,}", flush=True)
    folds = pd.concat(all_folds, ignore_index=True) if all_folds else pd.DataFrame()
    folds.to_parquet(args.folds_out, index=False)
    out = summarize(folds).sort_values(
        ["oos_bps", "folds", "n"], ascending=[False, False, False]
    )
    out.to_csv(args.out, index=False)
    print(out.head(80).to_string(index=False))
    print(f"wrote {args.out} rows={len(out):,}; folds {args.folds_out} rows={len(folds):,}")


if __name__ == "__main__":
    main()
