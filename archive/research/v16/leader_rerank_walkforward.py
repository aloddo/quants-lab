#!/usr/bin/env python
"""
DEPRECATED / THROWAWAY SCRATCH (Alberto correction 9865, 2026-06-22): this bypassed the BINDING V15
harness. A WHO/selection hypothesis MUST be tested by swapping the rank rule inside
research/v15/leadlag_clean_rank_sim.py (causal rank + matched-null + beta isolation + execution_model.py),
NOT in a one-off journey-level flat-fee script. The +15-34bps COMBINED signal here is a HINT to re-test
V15-native, NOT an authoritative result. Kept only as scratch history.

leader_rerank_walkforward.py -- Alberto 9857 part 2, deliverable #2: WALK-FORWARD leader RE-RANK.

Question (codex's strongest rec): is there a SELECTION rule that beats our current live SKILL rank
[z(win)+z(sharpe)+z(-maxdd)] out-of-sample? Test alternative dimensions -- hold-time, consistency,
short-capable -- across MULTIPLE expanding-window folds (not one lucky cutoff), net of cost. Isolates
selection alpha without in-sample survivorship.

Built on skill_selector_forward_validate.py (the single-cutoff validator that shipped the SKILL cohort),
generalized to a rolling walk-forward with extra ranking features.

Folds: expanding train (entry < cutoff), forward = [cutoff, cutoff+FWD_DAYS). 4 cutoffs.
Per-wallet TRAIN features:
  mean_ret, sharpe(=mean/std), win, maxdd            (current SKILL components)
  hold_h     = mean(duration_h)                       (lag-robustness / style)
  pos_wk     = fraction of active weeks net-positive  (CONSISTENCY -- distinct from sharpe)
  short_ret  = mean ret of SHORT journeys             (SHORT-CAPABLE -- rare on a 69%-long book)
Ranking methods (train-ranked top-K, forward edge measured equal-weight):
  PnL, SKILL(live), HOLD, CONSISTENCY, SHORTCAP, COMBINED, random
Verdict: does any method beat SKILL net-of-cost AVERAGED across folds AND in a MAJORITY of folds?

Memory-safe: per fold, vectorized groupby for light features; heavy features (maxdd, pos_wk, short)
computed via apply ONLY on the >=MIN_TRAIN eligible set (bounded). Run:
  ~/miniforge3/envs/quants-lab/bin/python research/v16/leader_rerank_walkforward.py
"""
import numpy as np
import pandas as pd

CUTOFFS = ["2026-03-01", "2026-03-21", "2026-04-10", "2026-05-01"]
FWD_DAYS = 21
MIN_TRAIN = 20
MIN_FWD = 8
RT_COST_BPS = 11.0
KS = [50, 100]


def max_consec_dd(returns):
    eq = np.cumsum(returns); peak = np.maximum.accumulate(eq)
    return float((peak - eq).max()) if len(eq) else 0.0


def z(x):
    return (x - x.mean()) / (x.std() + 1e-9)


def fold_features(tr):
    """Per-wallet train features on this fold. Light aggs vectorized; heavy on eligible subset."""
    g = tr.groupby("wallet")
    light = g.agg(n=("ret", "size"), mean_ret=("ret", "mean"), std_ret=("ret", "std"),
                  win=("ret", lambda r: (r > 0).mean()), sum_pnl=("realized_pnl", "sum"),
                  hold_h=("duration_h", "mean"), liq=("liq_closed", "mean"))
    light = light[light.n >= MIN_TRAIN].copy()
    light["sharpe"] = light["mean_ret"] / (light["std_ret"].fillna(0) + 1e-9)
    elig = set(light.index)
    tre = tr[tr.wallet.isin(elig)]
    # heavy features on eligible only
    def heavy(grp):
        r = grp["ret"].to_numpy()
        wk = (grp["entry_ts"].to_numpy() // (7 * 86400_000))
        # fraction of active weeks net-positive
        dfw = pd.DataFrame({"wk": wk, "r": r}).groupby("wk")["r"].mean()
        pos_wk = float((dfw > 0).mean()) if len(dfw) else 0.0
        sh = grp.loc[grp.side == "short", "ret"]
        short_ret = float(sh.mean()) if len(sh) >= 3 else np.nan
        return pd.Series({"maxdd": max_consec_dd(r), "pos_wk": pos_wk, "short_ret": short_ret})
    hv = tre.groupby("wallet").apply(heavy, include_groups=False)
    feat = light.join(hv)
    # wallets with too few shorts -> neutral short_ret (median) so they aren't unfairly ranked
    feat["short_ret"] = feat["short_ret"].fillna(feat["short_ret"].median())
    return feat


def main():
    cols = ["wallet", "side", "entry_ts", "duration_h", "realized_pnl",
            "net_realized_pnl", "max_position_notional", "liq_closed"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j[j.max_position_notional > 10].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["t"] = pd.to_datetime(j["entry_ts"], unit="ms")
    print(f"universe wallets={j.wallet.nunique()} journeys={len(j)} "
          f"({j.t.min().date()}->{j.t.max().date()}). WALK-FORWARD re-rank, {len(CUTOFFS)} folds.\n")

    methods = ["PnL", "SKILL", "HOLD", "CONSISTENCY", "SHORTCAP", "COMBINED"]
    # per (K, method) -> list of fold forward NET bps; and SKILL beat-count
    agg = {(K, m): [] for K in KS for m in methods}
    rand = {K: [] for K in KS}

    for ci, cut in enumerate(CUTOFFS):
        c = pd.Timestamp(cut); fwd_end = c + pd.Timedelta(days=FWD_DAYS)
        tr = j[j.t < c]; fw = j[(j.t >= c) & (j.t < fwd_end)]
        feat = fold_features(tr)
        fwm = fw.groupby("wallet")["ret"].agg(fwd_mean="mean", fwd_n="count")
        s = feat.join(fwm, how="inner")
        s = s[s.fwd_n >= MIN_FWD].copy()
        if len(s) < max(KS) + 10:
            print(f"fold {ci+1} {cut}: only {len(s)} eligible -- skipping"); continue
        # scores
        s["PnL"] = s["sum_pnl"]
        s["SKILL"] = z(s["win"]) + z(s["sharpe"]) + z(-s["maxdd"])
        s["HOLD"] = z(s["hold_h"])
        s["CONSISTENCY"] = z(s["pos_wk"]) + z(s["sharpe"])
        s["SHORTCAP"] = s["SKILL"] + z(s["short_ret"])
        s["COMBINED"] = z(s["win"]) + z(s["sharpe"]) + z(-s["maxdd"]) + z(s["pos_wk"]) + z(s["short_ret"])
        print(f"fold {ci+1} {cut} (fwd {FWD_DAYS}d): train_wallets={len(feat)} eligible={len(s)}")
        for K in KS:
            for m in methods:
                top = s.nlargest(K, m)
                net = top["fwd_mean"].mean() * 1e4 - RT_COST_BPS
                agg[(K, m)].append(net)
            # random baseline
            rng = np.arange(len(s)); draws = []
            for seed in range(20):
                idx = np.unique((rng * 2654435761 + seed * 40503) % len(s))[:K]
                draws.append(s.iloc[idx]["fwd_mean"].mean() * 1e4 - RT_COST_BPS)
            rand[K].append(np.mean(draws))

    print(f"\n=== WALK-FORWARD AVG forward NET edge (bps), {len(CUTOFFS)} folds ===")
    print(f"{'K':>5}{'method':>13}{'avg_net':>9}{'folds>0':>9}{'beats_SKILL':>13}")
    for K in KS:
        skill_folds = agg[(K, "SKILL")]
        for m in methods:
            vals = np.array(agg[(K, m)])
            beats = int(np.sum(vals > np.array(skill_folds))) if m != "SKILL" else len(vals)
            print(f"{K:>5}{m:>13}{vals.mean():>9.1f}{int((vals>0).sum()):>6}/{len(vals)}"
                  f"{beats:>9}/{len(vals)}")
        rv = np.array(rand[K])
        print(f"{K:>5}{'random':>13}{rv.mean():>9.1f}{int((rv>0).sum()):>6}/{len(rv)}{'--':>13}")
        print()

    print("=== VERDICT ===")
    print("A method only WINS if it beats SKILL on avg_net AND in a MAJORITY of folds (robust, not one fold).")
    print("If nothing robustly beats SKILL -> current live selection rule stays; selection alpha is tapped.")
    print("If a dimension wins robustly -> candidate re-rank; validate deeper + codex before any cohort swap.")


if __name__ == "__main__":
    main()
