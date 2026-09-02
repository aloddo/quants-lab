#!/usr/bin/env python3
"""Pre-registered A-F walk-forward comparison on existing after-cost M7 outputs.

This is a diagnostic screening comparison, not an M10 deployment verdict. The
source M7 artifact predates open-position cutoff marking, M8/M9/M10 are absent,
and the shortlist contains only 1,000 activity-ranked seats per fold. Those
limitations are emitted in the result and deliberately block any greenlight.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


STRATEGIES = {
    "A_static_robust": "fold-1 robust cohort retained; unavailable later seats conservatively earn cash",
    "B_periodic_robust": "rebalance every 14d on robust pretest M6/behaviour/economics",
    "C_recent_winner": "rebalance every 14d on trailing pretest follower return",
    "D_regime_conditioned": "prior same-regime forward results, with behaviour fallback",
    "E_behaviour_first": "closure/MAE/underwater-add filters, modest return weight",
    "F_shrunk_ensemble": "40% robust + 30% recent + 30% behaviour percentile score",
}


def pct(series: pd.Series, higher: bool = True) -> pd.Series:
    x = pd.to_numeric(series, errors="coerce")
    rank = x.rank(pct=True, method="average")
    return rank if higher else 1.0 - rank


def add_scores(panel: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for _, g0 in panel.groupby("fold_id", sort=True):
        g = g0.copy()
        behaviour_ok = (
            g["pp_n"].ge(25) & g["pp_p90_hold_h"].le(168)
            & g["pp_mae_p90"].le(0.15) & g["pp_uw_add"].le(0.60)
        ).fillna(False)
        g["behaviour_eligible"] = behaviour_ok
        g["score_behaviour"] = (
            0.25 * pct(g["pp_frac_quick"]) + 0.20 * pct(g["pp_p90_hold_h"], False)
            + 0.20 * pct(g["pp_mae_p90"], False) + 0.15 * pct(g["pp_uw_add"], False)
            + 0.20 * pct(g["pp_mean_r"])
        ).where(behaviour_ok)
        g["score_recent"] = pct(g["pre_roe"])
        g["score_robust"] = (
            0.35 * pct(g["m6b_score"]) + 0.25 * pct(g["pre_roe"])
            + 0.20 * pct(g["pre_calmar"]) + 0.20 * g["score_behaviour"].fillna(0)
        )
        g["score_ensemble"] = (
            0.40 * pct(g["score_robust"]) + 0.30 * g["score_recent"]
            + 0.30 * g["score_behaviour"].fillna(0)
        )
        parts.append(g)
    return pd.concat(parts, ignore_index=True)


def regime_labels(folds: pd.DataFrame, marks_path: Path) -> dict[int, str]:
    a = np.load(marks_path, allow_pickle=False)
    ts, pxs = a[0].astype("int64"), a[1].astype("float64")
    labels = {}
    for r in folds.itertuples():
        end = int(pd.Timestamp(r.test_start).value // 1_000_000)
        start = end - 14 * 86_400_000
        lo, hi = np.searchsorted(ts, [start, end], side="right") - 1
        if lo < 0 or hi <= lo:
            labels[int(r.fold_id)] = "unknown"
            continue
        trail = pxs[lo:hi + 1]
        ret = trail[-1] / trail[0] - 1.0
        minute_ret = np.diff(np.log(trail[trail > 0]))
        ann_vol = float(np.std(minute_ret) * np.sqrt(365 * 24 * 60)) if len(minute_ret) else np.nan
        labels[int(r.fold_id)] = f"{'up' if ret >= 0 else 'down'}_{'highvol' if ann_vol >= .80 else 'lowvol'}"
    return labels


def build_panel(run_dir: Path, m04_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    pre = pd.read_parquet(run_dir / "m07_pretest" / "m07_summary.parquet").rename(columns={
        "roe_engine": "pre_engine_roe", "realized_roe": "pre_roe", "max_dd": "pre_dd"})
    test = pd.read_parquet(run_dir / "m07_test" / "m07_summary.parquet").rename(columns={
        "roe_engine": "test_engine_roe", "realized_roe": "test_roe", "max_dd": "test_dd",
        "realized_pnl_total": "test_pnl"})
    pool = pd.read_parquet(run_dir / "m06b_pool.parquet")
    maps = []
    for fold in sorted(pre.fold_id.unique()):
        d = pd.read_parquet(m04_dir / f"m04_entities_f{int(fold)}.parquet",
                            columns=["entity_id", "primary_wallet", "n_members", "entity_confidence"])
        d["fold_id"] = int(fold)
        maps.append(d)
    mapping = pd.concat(maps, ignore_index=True)
    pcols = ["entity_id", "fold_id", "m6b_score", "pp_n", "pp_mean_r", "pp_p90_hold_h",
             "pp_frac_quick", "pp_uw_add", "pp_mae_p90"]
    panel = pre[["entity_id", "fold_id", "pre_roe", "pre_dd"]].merge(
        test[["entity_id", "fold_id", "test_roe", "test_dd", "test_pnl"]],
        on=["entity_id", "fold_id"], how="inner").merge(pool[pcols], on=["entity_id", "fold_id"], how="left").merge(
            mapping, on=["entity_id", "fold_id"], how="left")
    panel["pre_calmar"] = panel["pre_roe"] / panel["pre_dd"].clip(lower=1e-6)
    positions = pd.read_parquet(run_dir / "m07_test" / "m07_positions.parquet",
                                columns=["entity_id", "fold_id", "coin", "entry_ts", "exit_ts", "peak_notional"])
    positions["hold_h"] = (positions["exit_ts"] - positions["entry_ts"]) / 3_600_000.0
    hold = positions.groupby(["entity_id", "fold_id"])["hold_h"].agg(
        median_hold_h="median", p90_hold_h=lambda x: x.quantile(.90)).reset_index()
    coin = positions.groupby(["entity_id", "fold_id", "coin"], as_index=False)["peak_notional"].sum()
    return add_scores(panel), {"hold": hold, "coin": coin}


def selections(panel: pd.DataFrame, regimes: dict[int, str], k: int) -> dict[str, dict[int, list[str]]]:
    out = {name: {} for name in STRATEGIES}
    first = panel[panel.fold_id.eq(panel.fold_id.min())].nlargest(k, "score_robust")["primary_wallet"].tolist()
    history: list[pd.DataFrame] = []
    for fold, g0 in panel.groupby("fold_id", sort=True):
        g = g0.copy()
        out["A_static_robust"][int(fold)] = first
        out["B_periodic_robust"][int(fold)] = g.nlargest(k, "score_robust")["primary_wallet"].tolist()
        out["C_recent_winner"][int(fold)] = g.nlargest(k, "score_recent")["primary_wallet"].tolist()
        e = g[g.behaviour_eligible].nlargest(k, "score_behaviour")
        out["E_behaviour_first"][int(fold)] = e["primary_wallet"].tolist()
        out["F_shrunk_ensemble"][int(fold)] = g.nlargest(k, "score_ensemble")["primary_wallet"].tolist()
        same = [h for h in history if h.attrs.get("regime") == regimes.get(int(fold))]
        if same:
            hist = pd.concat(same).groupby("primary_wallet")["test_roe"].agg(["mean", "count"])
            g = g.join(hist, on="primary_wallet")
            g["score_regime"] = 0.70 * pct(g["mean"].where(g["count"].ge(1))) + 0.30 * g["score_behaviour"].fillna(0)
            pick = g.nlargest(k, "score_regime")
        else:
            pick = e
        out["D_regime_conditioned"][int(fold)] = pick["primary_wallet"].tolist()
        past = g0[["primary_wallet", "test_roe"]].copy()
        past.attrs["regime"] = regimes.get(int(fold))
        history.append(past)
    return out


def evaluate(panel: pd.DataFrame, positions: dict[str, pd.DataFrame], picks: dict[str, dict[int, list[str]]],
             k: int, n_random: int, seed: int) -> list[dict]:
    rng = np.random.default_rng(seed)
    rows = []
    for name, by_fold in picks.items():
        fold_rows = []
        prior: set[str] | None = None
        turnovers = []
        rank_stability = []
        prior_rank = None
        all_holds, coin_notional = [], {}
        for fold, wallets in by_fold.items():
            wanted = set(wallets)
            g = panel[(panel.fold_id == fold) & panel.primary_wallet.isin(wanted)]
            # Fixed denominator: a missing static seat earns cash, not a survivor-biased deletion.
            fold_return = float(g.test_roe.sum() / k)
            fold_rows.append((fold, fold_return, float(g.test_dd.sum() / k), len(g)))
            if prior is not None:
                turnovers.append(1.0 - len(wanted & prior) / max(k, 1))
            prior = wanted
            score_col = {"A_static_robust": "score_robust", "B_periodic_robust": "score_robust",
                         "C_recent_winner": "score_recent", "D_regime_conditioned": "score_behaviour",
                         "E_behaviour_first": "score_behaviour", "F_shrunk_ensemble": "score_ensemble"}[name]
            cur_rank = panel[panel.fold_id.eq(fold)].set_index("primary_wallet")[score_col]
            if prior_rank is not None:
                common = cur_rank.index.intersection(prior_rank.index)
                if len(common) >= 3:
                    rank_stability.append(float(cur_rank.loc[common].corr(prior_rank.loc[common], method="spearman")))
            prior_rank = cur_rank
            eids = g[["entity_id", "fold_id"]]
            hold = positions["hold"].merge(eids, on=["entity_id", "fold_id"], how="inner")
            all_holds.extend(hold[["median_hold_h", "p90_hold_h"]].dropna().itertuples(index=False, name=None))
            coin_rows = positions["coin"].merge(eids, on=["entity_id", "fold_id"], how="inner")
            for coin, value in coin_rows.groupby("coin").peak_notional.sum().items():
                coin_notional[coin] = coin_notional.get(coin, 0.0) + float(value)
        returns = np.array([x[1] for x in fold_rows])
        dds = np.array([x[2] for x in fold_rows])
        random_means = []
        for _ in range(n_random):
            vals = []
            for fold, g in panel.groupby("fold_id"):
                n = min(k, len(g))
                vals.append(float(g.iloc[rng.choice(len(g), n, replace=False)].test_roe.sum() / k))
            random_means.append(np.mean(vals))
        total_coin = sum(coin_notional.values())
        rows.append({
            "strategy": name, "k": k, "mean_forward_follower_return": float(returns.mean()),
            "compounded_forward_return": float(np.prod(1 + returns) - 1),
            "mean_sleeve_drawdown": float(dds.mean()), "worst_period_return": float(returns.min()),
            "mean_turnover": float(np.mean(turnovers)) if turnovers else 0.0,
            "mean_rank_spearman": float(np.nanmean(rank_stability)) if rank_stability else None,
            "entity_weight_concentration": 1.0 / k,
            "coin_notional_concentration": (max(coin_notional.values()) / total_coin if total_coin else None),
            "open_loss_exposure": None,
            "median_hold_h": float(np.median([x[0] for x in all_holds])) if all_holds else None,
            "p90_hold_h": float(np.median([x[1] for x in all_holds])) if all_holds else None,
            "randomized_cohort_percentile": float(100 * np.mean(np.asarray(random_means) <= returns.mean())),
            "mean_seat_coverage": float(np.mean([x[3] / k for x in fold_rows])),
        })
    return rows


def recent_winner_diagnostics(panel: pd.DataFrame, regimes: dict[int, str]) -> dict:
    rows = []
    for fold, g0 in panel.groupby("fold_id", sort=True):
        g = g0[["pre_roe", "test_roe", "behaviour_eligible"]].dropna().copy()
        g["decile"] = pd.qcut(g.pre_roe.rank(method="first"), 10, labels=False)
        by = g.groupby("decile").test_roe.mean()
        rows.append({
            "fold_id": int(fold), "regime": regimes.get(int(fold)), "n": len(g),
            "spearman": float(g.pre_roe.corr(g.test_roe, method="spearman")),
            "bottom_decile_forward": float(by.iloc[0]), "middle_deciles_forward": float(by.iloc[4:6].mean()),
            "top_decile_forward": float(by.iloc[-1]),
            "behaviour_qualified_spearman": (
                float(g[g.behaviour_eligible].pre_roe.corr(g[g.behaviour_eligible].test_roe, method="spearman"))
                if g.behaviour_eligible.sum() >= 3 else None),
        })
    frame = pd.DataFrame(rows)
    regime = frame.groupby("regime").agg(
        n_folds=("fold_id", "size"), mean_spearman=("spearman", "mean"),
        mean_top_forward=("top_decile_forward", "mean"),
        mean_bottom_forward=("bottom_decile_forward", "mean")).reset_index().to_dict("records")
    return {
        "per_fold": rows,
        "mean_spearman": float(frame.spearman.mean()),
        "median_spearman": float(frame.spearman.median()),
        "positive_spearman_folds": int((frame.spearman > 0).sum()),
        "mean_top_decile_forward": float(frame.top_decile_forward.mean()),
        "mean_middle_forward": float(frame.middle_deciles_forward.mean()),
        "mean_bottom_decile_forward": float(frame.bottom_decile_forward.mean()),
        "mean_behaviour_qualified_spearman": float(frame.behaviour_qualified_spearman.mean()),
        "by_regime": regime,
        "refresh_horizons_not_identified": ["daily", "3d", "7d", "30d"],
        "reason": "current canonical replay has only non-overlapping 14-day test folds",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--m04-dir", type=Path, required=True)
    parser.add_argument("--folds", type=Path, required=True)
    parser.add_argument("--btc-marks", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--k", type=int, default=20)
    parser.add_argument("--random", type=int, default=1000)
    args = parser.parse_args()
    panel, positions = build_panel(args.run_dir, args.m04_dir)
    folds = pd.read_parquet(args.folds)
    regimes = regime_labels(folds, args.btc_marks)
    all_results = []
    for k in sorted({max(1, args.k // 2), args.k, args.k * 2}):
        pick = selections(panel, regimes, k)
        all_results.extend(evaluate(panel, positions, pick, k, args.random, seed=20260803 + k))
    result = {
        "status": "diagnostic_only",
        "deployable": False,
        "non_deployable_reasons": [
            "source M7 excludes positions open at fold cutoff",
            "M8 survival artifact absent", "M9 chained portfolio simulation absent",
            "M10 matched-null verdict absent", "candidate panel truncated to activity top-1000 per fold",
            "entity comparison currently broader-view only",
        ],
        "regimes": regimes, "strategy_definitions": STRATEGIES,
        "recent_winner_diagnostics": recent_winner_diagnostics(panel, regimes),
        "results": all_results,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
