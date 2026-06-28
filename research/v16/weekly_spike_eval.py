"""
Weekly M7 spike EVAL -- pre-registered kill-gates (copy-rebuild/2026-06-28-weekly-m7-spike-prereg).

Reads the weekly m07 engine output (per-seat realized after-cost OOS ROE on each weekly TEST window) and asks:
within each week, does ranking pretest-active wallets by TRAILING weekly copy-edge and copying the top-k beat
(a) random and (b) an ACTIVITY baseline, AFTER costs, BY MARGIN, on a frozen holdout AND median across OOS weeks?

NO survivorship: the shortlist is pretest-active only; wallets that vanish in the test week score ~0 and stay in.
Ranking signal (trailing weekly copy-edge) is computed from the causal panel using ONLY weeks before test_start.
"""
import numpy as np, pandas as pd
from pathlib import Path
from scipy.stats import spearmanr

WS = Path("app/data/v15/weekly_spike")
RNG = np.random.default_rng(11)
TOPK_FRAC = 0.10
MARGIN = 0.010  # pre-registered: top-k mean ROE must beat best baseline by >= 1.0% AND be > 0
HOLDOUT_FROM = 16  # last ~5 weekly folds = frozen holdout window

def trailing_edge_per_fold():
    """trailing weekly copy-edge per (wallet, fold) from causal panel, weeks strictly before test_start."""
    folds = pd.read_parquet(WS/"m03_folds.parquet")[["fold_id","train_start","test_start"]]
    panel = pd.read_parquet("app/data/v15/copy_edge_label_panel_causal.parquet")
    # week epoch: week N start = t_min + N*7d ; t_min = min journey entry_ts
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=["entry_ts"])
    t_min = pd.Timestamp(int(j.entry_ts.min()), unit="ms")
    panel["week_start"] = t_min + pd.to_timedelta(panel.week * 7, unit="D")
    out = []
    for r in folds.itertuples():
        ts = pd.Timestamp(r.test_start)
        # trailing window: panel weeks whose week ends (week_start+7d) <= test_start, last 6 weeks
        tr = panel[(panel.week_start + pd.Timedelta(days=7) <= ts) &
                   (panel.week_start >= ts - pd.Timedelta(days=45))]
        g = tr.groupby("wallet").agg(cpnl=("copy_net_pnl","sum"), gross=("copied_gross_notional","sum"))
        g = g[g.gross > 0]
        g["trail_edge"] = g["cpnl"] / g["gross"]
        g["fold_id"] = int(r.fold_id)
        out.append(g.reset_index()[["wallet","fold_id","trail_edge"]])
    return pd.concat(out, ignore_index=True)

def topk_mean(roe, score, k):
    order = np.argsort(-score)
    return float(np.mean(roe[order[:k]]))

def main():
    summ = pd.read_parquet(WS/"m07_test/m07_summary.parquet")[["entity_id","fold_id","roe_engine","n_fills"]]
    ent = pd.read_parquet("app/data/v15/rebuild_chain/m04_entities.parquet")[["entity_id","primary_wallet"]]
    summ = summ.merge(ent, on="entity_id", how="left").rename(columns={"primary_wallet":"wallet"})
    tedge = trailing_edge_per_fold()
    # activity baseline = pretest journey count (from shortlist build proxy: recompute pre_j)
    sl = pd.read_parquet(WS/"weekly_shortlist.parquet")
    df = summ.merge(tedge, on=["wallet","fold_id"], how="left")
    df["trail_edge"] = df.trail_edge.fillna(df.trail_edge.median())
    df["rand"] = RNG.standard_normal(len(df))
    df["activity"] = df.n_fills  # in-test fills is NOT allowed as signal; use pretest proxy below
    # proper activity baseline: pretest journeys -- recompute
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=["wallet","entry_ts","max_position_notional"])
    j["t"] = pd.to_datetime(j.entry_ts, unit="ms")
    folds = pd.read_parquet(WS/"m03_folds.parquet")
    acts = []
    for r in folds.itertuples():
        pre = j[(j.t>=pd.Timestamp(r.train_start))&(j.t<pd.Timestamp(r.test_start))&(j.max_position_notional>50)]
        a = pre.groupby("wallet").size().rename("pre_j").reset_index(); a["fold_id"]=int(r.fold_id); acts.append(a)
    acts = pd.concat(acts, ignore_index=True)
    df = df.merge(acts, on=["wallet","fold_id"], how="left"); df["pre_j"]=df.pre_j.fillna(0)

    print(f"{'fold':>4} {'n':>5} {'k':>4} {'TRAIL':>9} {'ACTIVITY':>9} {'RANDOM':>9} {'all_mean':>9} {'beat_margin':>11}")
    res=[]
    for f in sorted(df.fold_id.unique()):
        g = df[df.fold_id==f]
        roe = g.roe_engine.values; n=len(g); k=max(5,int(TOPK_FRAC*n))
        t_tr = topk_mean(roe, g.trail_edge.values, k)
        t_ac = topk_mean(roe, g.pre_j.values, k)
        t_rn = topk_mean(roe, g['rand'].values, k)
        allm = float(roe.mean())
        margin = t_tr - max(t_ac, t_rn)
        res.append(dict(fold=f, n=n, k=k, trail=t_tr, act=t_ac, rand=t_rn, allm=allm, margin=margin))
        print(f"{f:>4} {n:>5} {k:>4} {t_tr:>+9.4f} {t_ac:>+9.4f} {t_rn:>+9.4f} {allm:>+9.4f} {margin:>+11.4f}")
    R = pd.DataFrame(res)
    oos = R[R.fold>=5]; hold = R[R.fold>=HOLDOUT_FROM]
    print("\n=== PRE-REGISTERED KILL-GATES ===")
    g1 = (hold.trail>0).all() and float(oos.trail.median())>0
    g2 = float(oos.margin.median())>=MARGIN and (hold.margin>0).all()
    sp = spearmanr(df.trail_edge, df.roe_engine).correlation
    print(f"  holdout(folds>={HOLDOUT_FROM}) trail topk means: {hold.trail.round(4).tolist()}")
    print(f"  OOS(folds>=5) median trail topk: {oos.trail.median():+.4f} | median margin vs best baseline: {oos.margin.median():+.4f}")
    print(f"  G1 trail topk>0 (holdout all & OOS median): {g1}")
    print(f"  G2 beats best baseline by >= {MARGIN:.1%} (OOS median) AND holdout all>0: {g2}")
    print(f"  pooled Spearman(trail_edge, roe_engine) = {sp:+.4f}")
    verdict = "PASS -> shorter-cadence copy is the live lead (frontier + codex Phase-5)" if (g1 and g2) else "FAIL -> REPOINT off copy"
    print(f"\n  VERDICT: {verdict}")

if __name__ == "__main__":
    main()
