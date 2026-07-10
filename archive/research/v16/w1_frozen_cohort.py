"""
FROZEN train-only liquid-individual selector (codex decisive test). NO look-ahead: selection uses ONLY
pretest-window data per fold -- train measured-slip markout + hold>=2h + churn<=20 + liquid-coverage>60% +
n_train>=30. (Tail-safety / test-positivity are OUTCOMES, evaluated later, NOT selection filters -- fixes the
circularity codex flagged.) Outputs an m07 shortlist for the canonical-cost portfolio engine.

FROZEN PARAMS (no more tuning): HOLD_MIN=2h, MAX_CHURN=20, MIN_COV=0.6, MIN_TRAIN_J=30, TOP_K=120/fold,
measured per-coin half-spread (l2 snapshots) with 5bps default for uncovered.
"""
import numpy as np, pandas as pd, json
from pathlib import Path
import sys; sys.path.insert(0, "research/v16")
import markout_study as M

WS = Path("app/data/v15/weekly_spike")
HS = json.load(open("/tmp/measured_halfspread.json")); DEF = 0.0005
HOLD_MIN, MAX_CHURN, MIN_COV, MIN_TRAIN_J, TOP_K = 2.0, 20, 0.6, 30, 120

def main():
    folds = pd.read_parquet(WS/"m03_folds.parquet")[["fold_id","train_start","test_start"]]
    ent = pd.read_parquet("app/data/v15/rebuild_chain/m04_entities.parquet")[["entity_id","primary_wallet"]]
    w2e = dict(zip(ent.primary_wallet, ent.entity_id))
    # price all journeys (gross markout, slip 0), then apply MEASURED per-coin cost
    df = M.build_priced(min_hold_h=HOLD_MIN, max_addon=MAX_CHURN, coins_keep=None, min_notional=200, slip_oneway=0.0)
    slip = df.coin.map(lambda c: HS.get(c, DEF)).astype(float)
    df["costed"] = df["mk_4h"] - 2*slip
    df["covered"] = df.coin.isin(HS)
    df["dur"] = df["duration_h"]
    rows = []
    for r in folds.itertuples(index=False):
        tr0 = pd.Timestamp(r.train_start).value//10**6; ts0 = pd.Timestamp(r.test_start).value//10**6
        pre = df[(df.entry_ts >= tr0) & (df.entry_ts < ts0)]
        g = pre.groupby("wallet").agg(mk=("costed","mean"), n=("costed","size"), covf=("covered","mean"))
        g = g[(g.n >= MIN_TRAIN_J) & (g["covf"] >= MIN_COV)]          # TRAIN-ONLY filters (no look-ahead)
        if len(g) == 0: continue
        sel = g.sort_values("mk", ascending=False).head(TOP_K)    # top by TRAIN measured-slip markout
        for wal in sel.index:
            eid = w2e.get(wal)
            if eid is not None:
                rows.append((int(eid), wal, int(r.fold_id), True))
    sl = pd.DataFrame(rows, columns=["entity_id","primary_wallet","fold_id","in_shortlist"]).drop_duplicates(["entity_id","fold_id"])
    sl.to_parquet(WS/"w1_frozen_shortlist.parquet", index=False)
    print(f"FROZEN train-only liquid-individual cohort: {len(sl)} seats, {sl.primary_wallet.nunique()} wallets, {sl.fold_id.nunique()} folds")
    print(sl.groupby("fold_id").size().describe().round(1).to_string())

if __name__ == "__main__":
    main()
