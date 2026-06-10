"""Does train ROE persist forward? Merge fwd-persistence + drift/leverage scan and report
by leverage band: train-winners' forward ROE, and rank persistence (Spearman)."""
import sys
sys.path.insert(0, "research/v15")
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

fp = pd.read_parquet("app/data/v15/m01_fwd_persistence.parquet")
sc = pd.read_parquet("app/data/v15/m01_drift_profit_scan.parquet")[
    ["wallet", "max_leverage", "n_fills", "exotic", "mean_equity"]]
d = fp.merge(sc, on="wallet", how="left")
d = d[d["train_roe"].notna() & d["fwd_roe"].notna() & d["max_leverage"].notna()].copy()
print(f"wallets with both train+fwd ROE and leverage: {len(d)}\n")

d["_lev"] = pd.cut(d["max_leverage"], [0, 2, 5, 10, 25, 1e9],
                   labels=["<2x", "2-5x", "5-10x", "10-25x", ">25x"])

print("===== ALL wallets: train->forward persistence by leverage band =====")
print(f"{'band':>7} {'n':>6} {'spearman(tr,fwd)':>17} {'med_train':>10} {'med_fwd':>10} {'%fwd>0':>7}")
for b in d["_lev"].cat.categories:
    s = d[d["_lev"] == b]
    if len(s) < 5:
        continue
    rho = spearmanr(s["train_roe"], s["fwd_roe"]).correlation
    print(f"{b:>7} {len(s):>6} {rho:>17.3f} {s['train_roe'].median():>9.0f}% {s['fwd_roe'].median():>9.0f}% {100*(s['fwd_roe']>0).mean():>6.0f}%")

print("\n===== TRAIN-WINNERS (train_roe in top decile within band): their FORWARD ROE =====")
print(f"{'band':>7} {'n_win':>6} {'med_train':>10} {'med_FWD':>10} {'mean_FWD':>10} {'%fwd>0':>7} {'%fwd>20%':>8}")
for b in d["_lev"].cat.categories:
    s = d[d["_lev"] == b]
    if len(s) < 20:
        continue
    thr = s["train_roe"].quantile(0.9)
    win = s[s["train_roe"] >= thr]
    print(f"{b:>7} {len(win):>6} {win['train_roe'].median():>9.0f}% {win['fwd_roe'].median():>9.0f}% "
          f"{win['fwd_roe'].mean():>9.0f}% {100*(win['fwd_roe']>0).mean():>6.0f}% {100*(win['fwd_roe']>20).mean():>7.0f}%")

print("\n===== the specific cohort: HIGH-LEV (>10x) AND train-profitable -> forward =====")
hi = d[(d["max_leverage"] > 10) & (d["train_roe"] > 0)]
print(f"  n={len(hi)} | median train ROE +{hi['train_roe'].median():.0f}% | median FWD ROE {hi['fwd_roe'].median():+.0f}% "
      f"| mean FWD {hi['fwd_roe'].mean():+.0f}% | %fwd>0 {100*(hi['fwd_roe']>0).mean():.0f}% | %fwd>+50% {100*(hi['fwd_roe']>50).mean():.0f}%")
print("\n===== contrast: DISCIPLINED (<=5x) AND train-profitable -> forward =====")
lo = d[(d["max_leverage"] <= 5) & (d["train_roe"] > 0)]
print(f"  n={len(lo)} | median train ROE +{lo['train_roe'].median():.0f}% | median FWD ROE {lo['fwd_roe'].median():+.0f}% "
      f"| mean FWD {lo['fwd_roe'].mean():+.0f}% | %fwd>0 {100*(lo['fwd_roe']>0).mean():.0f}% | %fwd>+20% {100*(lo['fwd_roe']>20).mean():.0f}%")
print("\noverall spearman(train_roe, fwd_roe):", round(spearmanr(d["train_roe"], d["fwd_roe"]).correlation, 3))
