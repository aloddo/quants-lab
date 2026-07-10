"""
predict-from-features step 1 -- the last live copy direction.

Pre-registration: brain projects/quant/copy-rebuild/2026-06-28-predict-from-features-prereg
Question: can OTHER pre-eligibility (AS-OF, train-window-only) wallet features predict the forward
after-cost OOS engine ROE / the fat-tail winners, where PAST COPY-EDGE (trail_edge) cannot?

All features AS-OF (train window). Label = m07 roe_engine (realized after-cost OOS). Walk-forward,
frozen holdout fold 8. Decision rule pre-registered. Memory-trivial (7,812 rows).
"""
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from sklearn.linear_model import ElasticNet
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.preprocessing import StandardScaler

RB = "app/data/v15/rebuild_chain"
RNG = np.random.default_rng(42)

LOCKED_FEATURES = [
    "trail_edge", "trail_nj", "trail_notional", "trail_weeks", "rank_in_fold",
    "n_actions_train", "n_journeys_train",
    "sharpe", "net_gross_ratio", "wash_frac", "funding_frac", "price_pnl_var_frac",
    "median_lev", "active_days", "n_fills_asof", "n_anchors",
    "notional_per_journey", "actions_per_journey",
]

def build_panel():
    lab = pd.read_parquet(f"{RB}/broad_m07_test/m07_summary.parquet")[
        ["entity_id", "fold_id", "roe_engine", "max_dd", "round_trip_win_rate", "ruin"]
    ]
    tr = pd.read_parquet(f"{RB}/broad_trailing.parquet")[
        ["entity_id", "fold_id", "wallet", "trail_edge", "trail_nj", "trail_notional",
         "trail_weeks", "rank_in_fold"]
    ]
    act = pd.read_parquet(f"{RB}/m03_wallet_fold_activity.parquet")
    act = act[act.key_kind == "entity"] if "entity" in set(act.key_kind.unique()) else act
    # m03 key is wallet or entity; join on wallet via trailing's wallet, fallback entity
    act_w = act[["key", "fold_id", "n_actions_train", "n_journeys_train"]].rename(columns={"key": "wallet"})

    df = lab.merge(tr, on=["entity_id", "fold_id"], how="inner")
    df = df.merge(act_w, on=["wallet", "fold_id"], how="left")

    # per-fold authenticity (AS-OF)
    auth_cols = ["entity_id", "sharpe", "net_gross_ratio", "wash_frac", "funding_frac",
                 "price_pnl_var_frac", "median_lev", "active_days", "n_fills", "n_anchors"]
    auth_all = []
    for f in range(1, 9):
        a = pd.read_parquet(f"{RB}/m04_authenticity_f{f}.parquet")
        a = a[[c for c in auth_cols if c in a.columns]].copy()
        a = a.drop_duplicates("entity_id")
        a["fold_id"] = f
        auth_all.append(a)
    auth = pd.concat(auth_all, ignore_index=True).rename(columns={"n_fills": "n_fills_asof"})
    df = df.merge(auth, on=["entity_id", "fold_id"], how="left")

    df["notional_per_journey"] = df.trail_notional / df.trail_nj.clip(lower=1)
    df["actions_per_journey"] = df.n_actions_train / df.n_journeys_train.clip(lower=1)
    for c in LOCKED_FEATURES:
        if c not in df.columns:
            df[c] = np.nan
    return df

def decile_eval(pred, realized, base_tail=0.135):
    """top-decile realized ROE (mean/med), tail precision, spearman."""
    n = len(pred)
    order = np.argsort(-pred)  # high pred first
    k = max(1, n // 10)
    top = order[:k]
    sp = spearmanr(pred, realized).correlation
    return {
        "spearman": sp,
        "topdec_mean": float(np.mean(realized[top])),
        "topdec_med": float(np.median(realized[top])),
        "topdec_tailprec": float(np.mean(realized[top] > 0.05)),
        "topdec_posfrac": float(np.mean(realized[top] > 0)),
        "n_top": k,
    }

def fit_predict(train, test, model="hgb"):
    X_tr = train[LOCKED_FEATURES].astype(float).values
    y_tr = train["roe_engine"].clip(-1.0, 1.0).values
    X_te = test[LOCKED_FEATURES].astype(float).values
    if model == "enet":
        med = np.nanmedian(X_tr, axis=0)
        X_tr = np.where(np.isnan(X_tr), med, X_tr)
        X_te = np.where(np.isnan(X_te), med, X_te)
        sc = StandardScaler().fit(X_tr)
        m = ElasticNet(alpha=0.01, l1_ratio=0.5, max_iter=5000).fit(sc.transform(X_tr), y_tr)
        return m.predict(sc.transform(X_te))
    else:
        m = HistGradientBoostingRegressor(
            max_depth=3, learning_rate=0.05, max_iter=300,
            l2_regularization=1.0, min_samples_leaf=40, random_state=42,
        ).fit(X_tr, y_tr)  # HGB handles NaN natively
        return m.predict(X_te)

def main():
    df = build_panel()
    print(f"panel: {df.shape}, folds {sorted(df.fold_id.unique())}")
    print(f"label base rates: tail>+5% {(df.roe_engine>0.05).mean():.3f}  pos {(df.roe_engine>0).mean():.3f}")
    print(f"feature NaN frac:\n{df[LOCKED_FEATURES].isna().mean().round(3).to_string()}")

    # ---- univariate Spearman screen on folds 1-7 (NOT touching holdout 8) ----
    dev = df[df.fold_id <= 7]
    print("\n=== UNIVARIATE Spearman(feature, forward roe_engine) on folds 1-7 ===")
    rows = []
    for c in LOCKED_FEATURES:
        sub = dev[[c, "roe_engine"]].dropna()
        if len(sub) > 50:
            sp = spearmanr(sub[c], sub.roe_engine).correlation
            rows.append((c, sp, len(sub)))
    for c, sp, n in sorted(rows, key=lambda x: -abs(x[1])):
        print(f"  {c:24s} spearman {sp:+.4f}  n={n}")

    # ---- walk-forward: predict fold k from 1..k-1, report 4..8 ----
    print("\n=== WALK-FORWARD decile eval (model vs baselines) ===")
    print(f"{'fold':>4} {'model':>6} {'spearman':>9} {'topdec_mean':>12} {'topdec_med':>11} {'tailprec':>9} {'posfrac':>8}")
    agg = {m: [] for m in ["hgb", "enet", "trail_edge", "random"]}
    for k in range(4, 9):
        train = df[df.fold_id < k].copy()
        test = df[df.fold_id == k].copy()
        realized = test["roe_engine"].values
        preds = {
            "hgb": fit_predict(train, test, "hgb"),
            "enet": fit_predict(train, test, "enet"),
            "trail_edge": test["trail_edge"].fillna(test["trail_edge"].median()).values,
            "random": RNG.standard_normal(len(test)),
        }
        for mname, p in preds.items():
            r = decile_eval(p, realized)
            agg[mname].append(r)
            tag = " <-- HOLDOUT" if k == 8 else ""
            print(f"{k:>4} {mname:>6} {r['spearman']:>+9.4f} {r['topdec_mean']:>+12.4f} "
                  f"{r['topdec_med']:>+11.4f} {r['topdec_tailprec']:>9.3f} {r['topdec_posfrac']:>8.3f}{tag if mname=='hgb' else ''}")
        print()

    # ---- pre-registered decision rule ----
    print("=== PRE-REGISTERED DECISION (folds 5-8 median + frozen holdout fold 8) ===")
    def med(mname, key, folds):
        return float(np.median([agg[mname][k-4][key] for k in folds]))
    f58 = [5, 6, 7, 8]
    hgb_topmean_58 = med("hgb", "topdec_mean", f58)
    hgb_topmean_8 = agg["hgb"][8-4]["topdec_mean"]
    rnd_topmean_58 = med("random", "topdec_mean", f58)
    tre_topmean_58 = med("trail_edge", "topdec_mean", f58)
    hgb_tailprec_8 = agg["hgb"][8-4]["topdec_tailprec"]
    hgb_sp_58 = med("hgb", "spearman", f58)
    print(f"  HGB topdec_mean: holdout8 {hgb_topmean_8:+.4f} | median(5-8) {hgb_topmean_58:+.4f}")
    print(f"  baselines topdec_mean median(5-8): random {rnd_topmean_58:+.4f} | trail_edge {tre_topmean_58:+.4f}")
    print(f"  HGB holdout8 tail precision {hgb_tailprec_8:.3f} (base 0.135) | median(5-8) spearman {hgb_sp_58:+.4f}")
    c1 = hgb_topmean_8 > 0 and hgb_topmean_58 > 0
    c2 = hgb_topmean_58 > rnd_topmean_58 and hgb_topmean_58 > tre_topmean_58
    c3 = hgb_tailprec_8 > 0.135
    c4 = hgb_sp_58 > 0
    print(f"  C1 topdec>0 (holdout & med): {c1}")
    print(f"  C2 beats random & trail_edge: {c2}")
    print(f"  C3 holdout tail precision > base: {c3}")
    print(f"  C4 spearman>0 median(5-8): {c4}")
    verdict = "PASS -> escalate (codex Phase-5)" if (c1 and c2 and c3 and c4) else "FAIL -> stand down copy / repoint"
    print(f"\n  VERDICT: {verdict}")

if __name__ == "__main__":
    main()
