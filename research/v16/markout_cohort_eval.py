"""
Markout-cohort engine eval (codex decisive test). Compares the TRAIN-KNOWN markout-selected cohort's realized
portfolio-engine ROE (canonical per-coin slip via v11 calib, latency 1860) vs the broad pretest-active pool,
and checks whether train-markout still orders engine ROE within the cohort. codex bar: positive median sleeve
PnL + positive pooled net + tolerable DD + (engine already prices canonical majors slip).
"""
import numpy as np, pandas as pd
from pathlib import Path
import sys; sys.path.insert(0, "research/v16")
import markout_study as M

WS = Path("app/data/v15/weekly_spike")
MAJORS = ["BTC","ETH","SOL","HYPE","XRP","DOGE","SUI","TAO","AVAX","LINK","LTC","BNB","ARB","OP",
          "APT","SEI","TIA","WLD","PUMP","FARTCOIN","ENA","AAVE","NEAR","INJ","ADA","DOT","UNI",
          "PEPE","TON","TRX","FIL","ATOM","ZEC","VVV","kPEPE","kBONK"]

def desc(roe, dd=None, lbl=""):
    print(f"  {lbl:22s} n={len(roe):6d} mean {roe.mean()*100:+6.2f}% med {np.median(roe)*100:+6.2f}% "
          f"pos {np.mean(roe>0):.3f}" + (f" maxDD(med) {np.median(dd)*100:5.2f}%" if dd is not None else ""))

def main():
    coh = pd.read_parquet(WS/"m07_markout_cohort/m07_summary.parquet")
    broad = pd.read_parquet(WS/"m07_test/m07_summary.parquet")
    print(f"=== MARKOUT COHORT vs BROAD POOL (realized engine ROE, 7d test windows, canonical slip) ===")
    desc(coh.roe_engine.values, coh.max_dd.values, "MARKOUT cohort")
    desc(broad.roe_engine.values, broad.max_dd.values, "BROAD pretest-active")
    # holdout (last 5 weekly folds)
    ch = coh[coh.fold_id>=16]; bh = broad[broad.fold_id>=16]
    print("  -- holdout folds>=16 --")
    desc(ch.roe_engine.values, ch.max_dd.values, "MARKOUT cohort (OOS)")
    desc(bh.roe_engine.values, bh.max_dd.values, "BROAD (OOS)")

    # within-cohort: does TRAIN markout order engine ROE?
    folds = pd.read_parquet(WS/"m03_folds.parquet")[["fold_id","train_start","test_start"]]
    ent = pd.read_parquet("app/data/v15/rebuild_chain/m04_entities.parquet")[["entity_id","primary_wallet"]]
    df = M.build_priced(min_hold_h=None, coins_keep=None, min_notional=200)[["wallet","entry_ts","mk_1h"]]
    rows=[]
    for r in folds.itertuples():
        tr0=pd.Timestamp(r.train_start).value//10**6; ts0=pd.Timestamp(r.test_start).value//10**6
        pre=df[(df.entry_ts>=tr0)&(df.entry_ts<ts0)].groupby("wallet").mk_1h.mean().rename("train_mk")
        pre=pre.reset_index(); pre["fold_id"]=int(r.fold_id); rows.append(pre)
    tmk=pd.concat(rows,ignore_index=True)
    m=coh.merge(ent,on="entity_id",how="left").merge(tmk,left_on=["primary_wallet","fold_id"],
        right_on=["wallet","fold_id"],how="left")
    m=m[m.train_mk.notna()]
    m["dec"]=m.groupby("fold_id").train_mk.transform(lambda s: pd.qcut(s.rank(method="first"),5,labels=False) if len(s)>=5 else np.nan)
    print("\n=== within-cohort: engine ROE by TRAIN-markout quintile (4=best train markout) ===")
    q=m.groupby("dec").agg(roe_mean=("roe_engine","mean"),roe_med=("roe_engine","median"),
                           pos=("roe_engine",lambda s:(s>0).mean()),n=("roe_engine","size"))
    print((q*[100,100,1,1]).round(2).to_string())
    top=m[m.dec==4]
    from scipy.stats import spearmanr
    sp=spearmanr(m.train_mk,m.roe_engine).correlation
    print(f"\n  Spearman(train_mk, engine_roe) = {sp:+.4f}")
    print(f"  TOP train-markout quintile engine ROE: mean {top.roe_engine.mean()*100:+.2f}% med {top.roe_engine.median()*100:+.2f}% pos {(top.roe_engine>0).mean():.3f}")
    g1 = top.roe_engine.median()>0
    g2 = top.roe_engine.mean() > broad.roe_engine.mean()
    print(f"\n  GATE top-quintile median engine ROE>0: {g1} | beats broad pool mean: {g2}")
    print(f"  VERDICT: {'COPYABLE EDGE survives the engine -> live lead candidate (codex Phase-5 + frontier)' if (g1 and g2) else 'does NOT survive the portfolio engine -> upper-bound trap, REPOINT'}")

if __name__ == "__main__":
    main()
