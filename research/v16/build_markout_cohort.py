"""
Deployable TRAIN-KNOWN markout cohort (codex decisive-test selector, 2026-06-28).

Fixes the ex-post flaw codex caught: select wallets by TRAIN-side hold PROFILE + TRAIN-side delayed-entry
markout (both computed only from journeys ENTERED in the pretest window [train_start, test_start)), then copy
their TEST-week entries. NO look-ahead. Output = m07 shortlist (entity_id, primary_wallet, fold_id, in_shortlist)
for run_shortlist on the weekly folds -> the portfolio-engine validation.
"""
import numpy as np, pandas as pd
from pathlib import Path
import sys; sys.path.insert(0, "research/v16")
import markout_study as M

WS = Path("app/data/v15/weekly_spike")
MAJORS = ["BTC","ETH","SOL","HYPE","XRP","DOGE","SUI","TAO","AVAX","LINK","LTC","BNB","ARB","OP",
          "APT","SEI","TIA","WLD","PUMP","FARTCOIN","ENA","AAVE","NEAR","INJ","ADA","DOT","UNI",
          "PEPE","TON","TRX","FIL","ATOM","ZEC","VVV","kPEPE","kBONK"]
MIN_TRAIN_J = 10        # need a train record
HOLD_MIN_H = 2.0        # deployable slow-holder filter on TRAIN hold profile
TOP_FRAC = 0.20         # select top 20% by train markout among eligible slow-holders

def main():
    folds = pd.read_parquet(WS/"m03_folds.parquet")[["fold_id","train_start","test_start"]]
    ent = pd.read_parquet("app/data/v15/rebuild_chain/m04_entities.parquet")[["entity_id","primary_wallet"]]
    w2e = dict(zip(ent.primary_wallet, ent.entity_id))
    # price ALL majors journeys once (entry markout costed at canonical-ish flat; selection signal only)
    df = M.build_priced(min_hold_h=None, coins_keep=None, min_notional=200)
    df = df[["wallet","entry_ts","duration_h","mk_1h"]].copy()
    rows = []
    for r in folds.itertuples():
        tr0 = pd.Timestamp(r.train_start).value // 10**6
        ts0 = pd.Timestamp(r.test_start).value // 10**6
        pre = df[(df.entry_ts >= tr0) & (df.entry_ts < ts0)]
        g = pre.groupby("wallet").agg(hold=("duration_h","median"), mk=("mk_1h","mean"), n=("mk_1h","size"))
        g = g[(g.n >= MIN_TRAIN_J) & (g.hold >= HOLD_MIN_H)]           # TRAIN-known slow-holder filter
        if len(g) == 0: continue
        k = max(5, int(TOP_FRAC * len(g)))
        sel = g.sort_values("mk", ascending=False).head(k)             # top by TRAIN markout
        for wal in sel.index:
            eid = w2e.get(wal)
            if eid is None: continue
            rows.append((int(eid), wal, int(r.fold_id), True))
    sl = pd.DataFrame(rows, columns=["entity_id","primary_wallet","fold_id","in_shortlist"]).drop_duplicates(["entity_id","fold_id"])
    sl.to_parquet(WS/"markout_cohort_shortlist.parquet", index=False)
    print(f"markout cohort (train-known, slow-hold majors): {len(sl)} seats, {sl.primary_wallet.nunique()} wallets, {sl.fold_id.nunique()} folds")
    print(sl.groupby('fold_id').size().describe().round(1).to_string())

if __name__ == "__main__":
    main()
