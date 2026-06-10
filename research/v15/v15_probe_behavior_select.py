"""V15 PROBE: behavior-gated selection vs outcome-ranking (Alberto greenlit 2026-06-01).

Tests the reframe (findings/2026-06-01-wallet-ranking-reframe-behavior-bandit): stop ranking wallets on
OUTCOMES (ROE/Calmar -- proven negative forward), GATE on SURVIVAL BEHAVIOR instead. The #1 bet
(codex + me): drawdown-conditioned sizing discipline / anti-ruin behavior.

PHASE 1 (this file): per-(entity,fold) behavioral features from m02_journeys, computed ONLY on each
fold's PRETEST window [pretest_start, test_start) (look-ahead-safe -- known at decision time). Then
race selection rules FORWARD through the 8 folds via the existing M9 chained grader
(v15_forward_select.forward grading pattern), comparing:
  - OUTCOME baseline  : rank by pretest ROE (the M6b-style rule, expected negative)
  - SURVIVABILITY     : the known-GREEN rule (pos every prior fold + low trail DD)
  - BEHAVIOR-GATE     : exclude ruin-mechanics wallets (martingale / hold-losers / liquidation), then
                        broad/equal basket  <-- the reframe
  - BEHAVIOR + SURVIV : intersection

Behavioral features (journey-level, the ruin-mechanics codex ranked #1):
  martingale_corr  = corr(prev journey net_pnl, next journey max_notional). negative => sizes UP after
                     losses => revenge => blowup. disciplined => >= ~0.
  loser_hold_ratio = mean(duration_h | loss) / mean(duration_h | win). >1 => holds losers, cuts winners
                     => bad. disciplined => <= 1.
  winloss_size     = mean(net_pnl | win) / mean(|net_pnl| | loss). >=1 => winners bigger than losers.
  liq_frac         = fraction of journeys closed by liquidation. disciplined => 0.
  pos_frac         = fraction of journeys net-positive (after their own fees/funding).

Entity feature = its PRIMARY wallet's feature (is_entity_primary). Documented approximation.

Run:
  /Users/hermes/miniforge3/envs/quants-lab/bin/python research/v15/v15_probe_behavior_select.py --features
  /Users/hermes/miniforge3/envs/quants-lab/bin/python research/v15/v15_probe_behavior_select.py --grade
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

DATA = Path(__file__).resolve().parent.parent.parent / "app" / "data" / "v15"
FEAT_PATH = DATA / "probe_behavior_features.parquet"
MS_PER_DAY = 86_400_000


# --------------------------------------------------------------------------- #
# PHASE 1: behavioral features per (entity, fold) on the pretest window
# --------------------------------------------------------------------------- #
def _wallet_fold_features(jw: pd.DataFrame) -> dict:
    """jw = one wallet's journeys within one fold's pretest window, sorted by entry_ts."""
    n = len(jw)
    pnl = jw["net_realized_pnl"].to_numpy(dtype="float64")
    notional = jw["max_position_notional"].to_numpy(dtype="float64")
    dur = jw["duration_h"].to_numpy(dtype="float64")
    liq = jw["liq_closed"].to_numpy()
    wins = pnl > 0
    losses = pnl < 0
    # martingale: prev pnl vs next notional
    mart = np.nan
    if n >= 5:
        prev_pnl = pnl[:-1]
        next_notional = notional[1:]
        if np.std(prev_pnl) > 0 and np.std(next_notional) > 0:
            mart = float(np.corrcoef(prev_pnl, next_notional)[0, 1])
    # loser hold ratio
    lhr = np.nan
    if wins.any() and losses.any():
        mw = np.nanmean(dur[wins]); ml = np.nanmean(dur[losses])
        if mw > 0:
            lhr = float(ml / mw)
    # win/loss size ratio
    wls = np.nan
    if wins.any() and losses.any():
        aw = np.nanmean(pnl[wins]); al = np.nanmean(np.abs(pnl[losses]))
        if al > 0:
            wls = float(aw / al)
    return {
        "n_j": int(n),
        "pos_frac": float(np.mean(wins)) if n else np.nan,
        "martingale_corr": mart,
        "loser_hold_ratio": lhr,
        "winloss_size": wls,
        "liq_frac": float(np.mean(liq)) if n else np.nan,
    }


def compute_features() -> pd.DataFrame:
    folds = pd.read_parquet(DATA / "m03_folds.parquet")
    auth = pd.read_parquet(DATA / "m04_authenticity.parquet",
                           columns=["wallet", "entity_id", "is_entity_primary", "copyable"])
    # entity -> primary wallet (fallback: any wallet if no primary flagged)
    prim = auth[auth.is_entity_primary].drop_duplicates("entity_id")[["entity_id", "wallet"]]
    no_prim = auth[~auth.entity_id.isin(prim.entity_id)].drop_duplicates("entity_id")[["entity_id", "wallet"]]
    ent_wallet = pd.concat([prim, no_prim], ignore_index=True)
    wanted_wallets = set(ent_wallet.wallet)

    j = pd.read_parquet(DATA / "m02_journeys.parquet",
                        columns=["wallet", "journey_id", "entry_ts", "duration_h",
                                 "max_position_notional", "net_realized_pnl", "liq_closed"])
    j = j[j.wallet.isin(wanted_wallets)].copy()
    j = j.sort_values(["wallet", "entry_ts"]).reset_index(drop=True)

    fold_win = [(int(r.fold_id),
                 pd.Timestamp(r.pretest_start).value // 1_000_000,
                 pd.Timestamp(r.test_start).value // 1_000_000) for r in folds.itertuples()]

    w2e = ent_wallet.set_index("wallet")["entity_id"].to_dict()
    rows = []
    for w, g in j.groupby("wallet", sort=False):
        eid = w2e.get(w)
        if eid is None:
            continue
        ets = g["entry_ts"].to_numpy()
        for fid, t0, t1 in fold_win:
            sub = g[(ets >= t0) & (ets < t1)]
            if sub.empty:
                continue
            feat = _wallet_fold_features(sub)
            feat["entity_id"] = int(eid)
            feat["fold_id"] = int(fid)
            rows.append(feat)
    out = pd.DataFrame(rows)
    out.to_parquet(FEAT_PATH, index=False)
    return out


def describe_features(df: pd.DataFrame):
    print(f"\n=== behavioral features: {len(df)} (entity,fold) rows, "
          f"{df.entity_id.nunique()} entities, folds {sorted(df.fold_id.unique())}")
    sup = df[df.n_j >= 5]
    print(f"rows with >=5 journeys (martingale-supported): {len(sup)}")
    for c in ["n_j", "pos_frac", "martingale_corr", "loser_hold_ratio", "winloss_size", "liq_frac"]:
        s = df[c].dropna()
        if s.empty:
            print(f"  {c:18s} ALL NaN"); continue
        print(f"  {c:18s} n={len(s):5d}  p10={np.percentile(s,10):8.3f}  "
              f"med={np.percentile(s,50):8.3f}  p90={np.percentile(s,90):8.3f}")
    # how many would gate
    g = behavior_gate(df)
    print(f"\nBEHAVIOR GATE pass: {int(g.sum())}/{len(df)} ({100*g.mean():.1f}%)  "
          f"unique entities passing in >=1 fold: {df[g].entity_id.nunique()}")


# --------------------------------------------------------------------------- #
# GATE definition (codex: exclude known ruin mechanics; veto not optimizer)
# --------------------------------------------------------------------------- #
def behavior_gate(df: pd.DataFrame) -> pd.Series:
    """Pass = NOT exhibiting ruin mechanics. NaN on a sub-signal => not penalized for it (need >=5
    journeys for martingale; need both wins+losses for ratios). Hard vetoes only."""
    mart_ok = df["martingale_corr"].isna() | (df["martingale_corr"] >= -0.20)
    hold_ok = df["loser_hold_ratio"].isna() | (df["loser_hold_ratio"] <= 1.50)
    liq_ok = df["liq_frac"].fillna(0) <= 0.05
    supp = df["n_j"] >= 5
    return (mart_ok & hold_ok & liq_ok & supp).fillna(False)


# --------------------------------------------------------------------------- #
# PHASE 2: forward grade -- race rules through the M9 chained sim
# --------------------------------------------------------------------------- #
def grade(k_select: int = 10, follower_trail: float | None = None):
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    import v15_forward_select as FS  # noqa: E402
    import v15_m07_engine as E  # noqa: E402

    # Wire the 7% circuit breaker (the proven damage-limiter -90%->-27%) into the EngineParams that
    # M9 hardcodes. M9 calls eng.EngineParams(slippage_band=..., start_policy=...) with no follower_trail;
    # wrap the dataclass so follower_trail is always injected when requested.
    orig_ep = E.EngineParams
    if follower_trail is not None:
        def _ep(*a, **kw):
            kw.setdefault("follower_trail", follower_trail)
            return orig_ep(*a, **kw)
        E.EngineParams = _ep

    feats = pd.read_parquet(FEAT_PATH)
    gate = behavior_gate(feats)
    feats = feats.assign(beh_gate=gate.values)
    # entity,fold -> behavioral cols for merge inside score_fns
    fcols = ["entity_id", "fold_id", "beh_gate", "martingale_corr", "loser_hold_ratio",
             "winloss_size", "liq_frac", "pos_frac", "n_j"]
    fb = feats[fcols].rename(columns={"pos_frac": "beh_pos_frac", "n_j": "beh_n_j"})

    # monkeypatch trailing_features to also carry behavioral features (merge on entity_id, fold_id)
    orig_tf = FS.trailing_features

    def tf_with_beh(oos, pre, m5, k):
        f = orig_tf(oos, pre, m5, k)
        return f.merge(fb, on=["entity_id", "fold_id"], how="left")
    FS.trailing_features = tf_with_beh

    rules = {
        "OUTCOME_pre_roe":      (lambda f: f["pre_roe"], False),
        "SURVIV_known_green":   (lambda f: np.where(
            (f["trail_pos_frac"].fillna(0) >= 0.999) & (f["trail_dd"].fillna(1) < 0.25),
            -f["trail_dd"].fillna(1), np.nan), True),
        "BEHGATE_broad":        (lambda f: np.where(f["beh_gate"].fillna(False),
                                                    f["beh_pos_frac"].fillna(0), np.nan), True),
        "BEHGATE_x_surviv":     (lambda f: np.where(
            f["beh_gate"].fillna(False) & (f["trail_pos_frac"].fillna(0) >= 0.999)
            & (f["trail_dd"].fillna(1) < 0.25), -f["trail_dd"].fillna(1), np.nan), True),
    }

    print(f"\n--- K={k_select}  follower_trail={follower_trail} ---")
    print(f"{'RULE':22s} {'chained_roe':>12s} {'maxDD':>8s} {'pos_folds':>10s}  selections")
    results = {}
    for name, (fn, _need_beh) in rules.items():
        try:
            res = FS.forward_backtest(fn, k_select=k_select, require_eligible=True, min_trail=1)
            roe = res.get("chained_roe", float("nan"))
            dd = res.get("max_chained_dd", float("nan"))
            pf = res.get("n_positive_folds", float("nan"))
            sel = res.get("selections", {})
            print(f"{name:22s} {roe:12.4f} {dd:8.3f} {str(pf):>10s}  {sel}")
            results[name] = res
        except Exception as e:
            print(f"{name:22s} ERROR: {e}")
    FS.trailing_features = orig_tf
    E.EngineParams = orig_ep
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--features", action="store_true", help="compute + describe behavioral features")
    ap.add_argument("--grade", action="store_true", help="forward-grade selection rules via M9")
    ap.add_argument("-k", type=int, default=10, help="top-K per fold for grading")
    args = ap.parse_args()
    if args.features or not (args.features or args.grade):
        df = compute_features()
        describe_features(df)
    if args.grade:
        if not FEAT_PATH.exists():
            df = compute_features(); describe_features(df)
        for trail in (None, 0.07):
            for k in (5, 10, 20):
                grade(k_select=k, follower_trail=trail)


if __name__ == "__main__":
    main()
