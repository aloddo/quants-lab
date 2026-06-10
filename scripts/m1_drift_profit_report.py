"""Error-distribution report for the cent-recon GO/NO-GO call. Reads the scan parquet and
prints: drift distribution, the >5% cohort size + profitability, and cross-dimension breakdowns."""
import sys
sys.path.insert(0, "research/v15")
import numpy as np
import pandas as pd

P = sys.argv[1] if len(sys.argv) > 1 else "app/data/v15/m01_drift_profit_scan.parquet"
df = pd.read_parquet(P)
print(f"TOTAL rows: {len(df)}")
print("err breakdown:", df["err"].value_counts().to_dict())
d = df[df["err"] == ""].copy()
print(f"scanned-ok wallets (>=2 anchors): {len(d)}\n")

# profitability flag
d["profitable"] = d["roe_pct"] > 0


def bucket(col, edges, labels):
    return pd.cut(d[col], bins=edges, labels=labels, right=False)


DEDGES = [-1, 0.1, 1, 5, 20, 100, 1e12]
DLAB = ["<0.1%", "0.1-1%", "1-5%", "5-20%", "20-100%", ">100%"]

for metric in ["median_drift_pct", "max_drift_pct"]:
    d["_b"] = bucket(metric, DEDGES, DLAB)
    print(f"===== {metric} distribution =====")
    g = d.groupby("_b", observed=False).agg(
        n=("wallet", "size"),
        pct_profitable=("profitable", lambda s: 100 * s.mean()),
        median_roe=("roe_pct", "median"),
        median_lev=("max_leverage", "median"),
        median_fills=("n_fills", "median"),
        pct_exotic=("exotic", lambda s: 100 * s.mean()),
        median_eq=("mean_equity", "median"),
    )
    g["share"] = (100 * g["n"] / len(d)).round(1)
    print(g.round(1).to_string())
    print()

# THE key question: wallets with drift>5% AT ANY check (max) -- how many, profitable?
print("===== THE CALL: wallets with max_drift > 5% (drift >5% at SOME anchor over 6mo) =====")
over = d[d["max_drift_pct"] > 5]
under = d[d["max_drift_pct"] <= 5]
for label, sub in [("max_drift>5%", over), ("max_drift<=5%", under)]:
    print(f"  {label}: n={len(sub)} ({100*len(sub)/len(d):.1f}%) | profitable={100*sub['profitable'].mean():.1f}% "
          f"| median_roe={sub['roe_pct'].median():.1f}% | median_lev={sub['max_leverage'].median():.2f}x "
          f"| median_fills={sub['n_fills'].median():.0f} | exotic={100*sub['exotic'].mean():.0f}% "
          f"| median_eq=${sub['mean_equity'].median():,.0f}")
print()
# of PROFITABLE wallets, what's the drift?
print("===== of PROFITABLE wallets (roe>0): drift distribution =====")
prof = d[d["profitable"]]
print(f"  profitable n={len(prof)} ({100*len(prof)/len(d):.1f}% of scanned)")
for lo, hi, lab in [(0, 1, "max_drift<1%"), (1, 5, "1-5%"), (5, 100, "5-100%"), (100, 1e12, ">100%")]:
    s = prof[(prof["max_drift_pct"] >= lo) & (prof["max_drift_pct"] < hi)]
    print(f"    {lab}: {len(s)} ({100*len(s)/len(prof):.1f}% of profitable)")
print()
# median-drift version (true recon quality, sentinel-robust)
print("===== of PROFITABLE wallets: MEDIAN drift (sentinel-robust) =====")
for lo, hi, lab in [(0, 1, "median<1%"), (1, 5, "1-5%"), (5, 1e12, ">5%")]:
    s = prof[(prof["median_drift_pct"] >= lo) & (prof["median_drift_pct"] < hi)]
    print(f"    {lab}: {len(s)} ({100*len(s)/len(prof):.1f}% of profitable)")
print()
# leverage cross-tab
print("===== drift vs leverage =====")
d["_lev"] = pd.cut(d["max_leverage"], [0, 2, 5, 10, 1e9], labels=["<2x", "2-5x", "5-10x", ">10x"])
print(d.groupby("_lev", observed=False).agg(n=("wallet", "size"),
      median_drift=("median_drift_pct", "median"), max_drift=("max_drift_pct", "median"),
      pct_profitable=("profitable", lambda s: round(100*s.mean(), 1))).round(2).to_string())
print()
# correlation: does high drift predict unprofitable?
print("corr(median_drift_pct, roe_pct):", round(d[["median_drift_pct", "roe_pct"]].corr().iloc[0, 1], 3))
print("corr(max_leverage, median_drift_pct):", round(d[["max_leverage", "median_drift_pct"]].corr().iloc[0, 1], 3))
