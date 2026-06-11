#!/usr/bin/env python
"""agentG_nb_bucket.py -- H-NB-FILTER card, Steps 3-5 (agent G2, 2026-06-11).

Step 3: bucket the SHIPPED-V17-config baseline replay trades by the causal asof
        NB percentile at entry (exact replica of engine_replay._nb_bucket: last
        completed-hour sample STRICTLY before entry_ts, staleness <= 1h, NaN or
        absent => no_coverage). Buckets vs side:
          ALIGNED  pct >= 0.6 long / pct <= 0.4 short
          OPPOSED  pct <= 0.4 long / pct >= 0.6 short
          NEUTRAL  otherwise; no_coverage = disclosed pass-through.
Step 4: compare the two in-engine filter runs vs the shipped baseline.
Step 5: mechanical verdict, PRE-REGISTERED (locked 2026-06-11 15:0x CEST, BEFORE
        any filter-run numbers were observed; runs were still executing):
          bps arm : pooled trade-weighted mean net bps (sum(net_bps)/n over both
                    folds, from the trade logs) improves >= +5.0 vs baseline.
          dd arm  : pooled max_dd (fold1+fold2 sum) improves >= 10 percent AND
                    pooled book_pnl (fold1+fold2) >= baseline pooled book_pnl.
          PASS if either arm holds for either filter mode; per-fold breakdown
          reported regardless. Else KILL.

Inputs (all produced by env-gated engine_replay.py runs, V16_REPLAY_OUT
redirected so the committed artifact is untouched):
  /tmp/agentG2_shipped_baseline.parquet + _trades.parquet   (baseline)
  /tmp/agentG2_filter_opp.parquet      + _trades.parquet   (block_opposed)
  /tmp/agentG2_filter_oppneu.parquet   + _trades.parquet   (block_opposed_neutral)
  app/data/v18/nb_pct_series.parquet                        (causal NB pct series)
"""
from __future__ import annotations

import numpy as np
import pandas as pd

REPO = "/Users/hermes/quants-lab"
NB_PQ = f"{REPO}/app/data/v18/nb_pct_series.parquet"
ORDER = 50.0
HOUR_MS = 3_600_000

RUNS = {
    "baseline":     ("/tmp/agentG2_shipped_baseline.parquet", "/tmp/agentG2_shipped_baseline_trades.parquet"),
    "block_opp":    ("/tmp/agentG2_filter_opp.parquet",       "/tmp/agentG2_filter_opp_trades.parquet"),
    "block_oppneu": ("/tmp/agentG2_filter_oppneu.parquet",    "/tmp/agentG2_filter_oppneu_trades.parquet"),
}


def load_nb():
    df = pd.read_parquet(NB_PQ)
    out = {}
    for (f, c), g in df.groupby(["fold", "coin"]):
        g = g.sort_values("hour_ts")
        out[(f, c)] = (g["hour_ts"].values.astype(np.int64), g["pct"].values.astype(np.float64))
    return out


def bucket_trades(tr: pd.DataFrame, nb: dict) -> pd.DataFrame:
    """Replicates engine_replay._nb_bucket exactly (searchsorted-left - 1)."""
    buckets, pcts = [], []
    for row in tr.itertuples():
        s = nb.get((row.fold, row.coin))
        if s is None:
            buckets.append("no_coverage"); pcts.append(np.nan); continue
        i = int(np.searchsorted(s[0], row.entry_ts_ms, side="left")) - 1
        if i < 0 or row.entry_ts_ms - int(s[0][i]) > HOUR_MS:
            buckets.append("no_coverage"); pcts.append(np.nan); continue
        p = float(s[1][i])
        if not np.isfinite(p):
            buckets.append("no_coverage"); pcts.append(np.nan); continue
        if row.dir > 0:
            b = "ALIGNED" if p >= 0.6 else ("OPPOSED" if p <= 0.4 else "NEUTRAL")
        else:
            b = "ALIGNED" if p <= 0.4 else ("OPPOSED" if p >= 0.6 else "NEUTRAL")
        buckets.append(b); pcts.append(p)
    tr = tr.copy()
    tr["bucket"] = buckets
    tr["nb_pct"] = pcts
    return tr


def bucket_table(tr: pd.DataFrame, label: str):
    print(f"\n### Bucket table -- {label}")
    print("| fold | bucket | n | mean bps | med bps | total bps | total $ | win% |")
    print("|------|--------|---|----------|---------|-----------|---------|------|")
    order = ["ALIGNED", "NEUTRAL", "OPPOSED", "no_coverage"]
    for fold in ["fold1", "fold2", "POOLED"]:
        sub = tr if fold == "POOLED" else tr[tr.fold == fold]
        for b in order:
            g = sub[sub.bucket == b]
            if not len(g):
                print(f"| {fold} | {b} | 0 | -- | -- | -- | -- | -- |"); continue
            print(f"| {fold} | {b} | {len(g)} | {g.net_bps.mean():+.1f} | {g.net_bps.median():+.1f} "
                  f"| {g.net_bps.sum():+.0f} | {g.net_bps.sum()*ORDER/1e4:+.2f} | {(g.net_bps>0).mean()*100:.1f} |")
        g = sub
        print(f"| {fold} | ALL | {len(g)} | {g.net_bps.mean():+.1f} | {g.net_bps.median():+.1f} "
              f"| {g.net_bps.sum():+.0f} | {g.net_bps.sum()*ORDER/1e4:+.2f} | {(g.net_bps>0).mean()*100:.1f} |")


def main():
    nb = load_nb()
    summ, trades = {}, {}
    for name, (spq, tpq) in RUNS.items():
        summ[name] = pd.read_parquet(spq)
        trades[name] = pd.read_parquet(tpq)

    # ---- Step 3: bucket analysis on the shipped baseline ----
    bt = bucket_trades(trades["baseline"], nb)
    bucket_table(bt, "SHIPPED V17 baseline trades vs causal asof NB pct at entry")
    cov = bt.groupby("fold")["bucket"].apply(lambda s: (s != "no_coverage").mean() * 100)
    print(f"\nNB coverage of baseline trades: " +
          ", ".join(f"{k} {v:.1f}%" for k, v in cov.items()))

    # ---- Step 4: run comparison ----
    print("\n### Run comparison (shipped config; filter runs are engine-side rejects)")
    cols = ["entries", "book_pnl", "trade_mean_bps", "trade_med_bps", "max_dd", "eq_min", "funding_paid"]
    print("| run | fold | " + " | ".join(cols) + " | nb_filter rejects | nb_counts |")
    print("|-----|------|" + "---|" * (len(cols) + 2))
    for name in RUNS:
        df = summ[name]
        for _, r in df.iterrows():
            rej = r["rejects"].get("nb_filter", 0) if isinstance(r["rejects"], dict) else 0
            nbc = r.get("nb_counts", "") if "nb_counts" in df.columns else ""
            vals = " | ".join(f"{r[c]:.3f}" if isinstance(r[c], float) else str(r[c]) for c in cols)
            print(f"| {name} | {r['fold']} | {vals} | {rej} | {nbc} |")

    # ---- Step 5: pre-registered mechanical verdict ----
    def pooled(name):
        df, tr = summ[name], trades[name]
        return dict(
            mean_bps=tr.net_bps.sum() / len(tr),
            n=len(tr),
            book_pnl=df.book_pnl.sum(),
            dd_sum=df.max_dd.sum(),
            dd_by_fold=dict(zip(df.fold, df.max_dd)),
            pnl_by_fold=dict(zip(df.fold, df.book_pnl)),
            mean_by_fold=tr.groupby("fold").net_bps.mean().to_dict(),
        )

    base = pooled("baseline")
    print("\n### Verdict inputs (pooled = both folds; mean bps trade-weighted from trade logs)")
    verdict, detail = "KILL", []
    for name in ["block_opp", "block_oppneu"]:
        f = pooled(name)
        d_bps = f["mean_bps"] - base["mean_bps"]
        dd_impr = 1 - f["dd_sum"] / base["dd_sum"]
        pnl_ok = f["book_pnl"] >= base["book_pnl"]
        bps_arm = d_bps >= 5.0
        dd_arm = (dd_impr >= 0.10) and pnl_ok
        line = (f"{name}: pooled mean {f['mean_bps']:+.1f} vs base {base['mean_bps']:+.1f} "
                f"(d={d_bps:+.1f} bps, arm {'PASS' if bps_arm else 'fail'}) | "
                f"dd_sum {f['dd_sum']:.1f} vs {base['dd_sum']:.1f} ({dd_impr*100:+.1f}% impr, "
                f"pnl {f['book_pnl']:+.1f} vs {base['book_pnl']:+.1f}, arm {'PASS' if dd_arm else 'fail'}) | "
                f"per-fold mean {f['mean_by_fold']} dd {f['dd_by_fold']} pnl {f['pnl_by_fold']}")
        print(line)
        detail.append(line)
        if bps_arm or dd_arm:
            verdict = f"PASS ({name})"
    print(f"\nbaseline per-fold mean {base['mean_by_fold']} dd {base['dd_by_fold']} pnl {base['pnl_by_fold']}")
    print(f"\nMECHANICAL VERDICT: {verdict}")
    bt.to_parquet("/tmp/agentG2_baseline_trades_bucketed.parquet", index=False)
    print("bucketed trades -> /tmp/agentG2_baseline_trades_bucketed.parquet")


if __name__ == "__main__":
    main()
