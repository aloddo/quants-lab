#!/usr/bin/env python
"""Re-evaluate the full ~18k universe (Alberto 9957) -- STEP 1: eligibility + martingale veto + skill on
the WHOLE universe (offline, fast). Dumps the martingale-clean eligible pool for the MTM-DD + copy-edge
steps. Reuses build_skill_cohort functions."""
import json, sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_skill_cohort import skill_scores, martingale_flags, z, ASOF, ACTIVE_DAYS, MIN_J, HOLD_MIN_H, HOLD_MAX_H
def main():
    calib=set(json.load(open("/tmp/agentC_l2_calib_expanded.json")).keys())
    cols=["wallet","coin","entry_ts","realized_pnl","net_realized_pnl","max_position_notional","liq_closed","duration_h"]
    j=pd.read_parquet("app/data/v15/m02_journeys.parquet",columns=cols)
    n_uni=j.wallet.nunique()
    j=j[(j.max_position_notional>10)&(j.coin.isin(calib))].copy()
    j["ret"]=j["net_realized_pnl"]/j["max_position_notional"]; j=j[j.ret.between(-1.0,2.0)].copy()
    j["t"]=pd.to_datetime(j["entry_ts"],unit="ms"); asof=pd.Timestamp(ASOF); j=j[j.t<=asof]
    last=j.t.max(); active=set(j[j.t>=last-pd.Timedelta(days=ACTIVE_DAYS)].wallet.unique())
    s=skill_scores(j)
    elig=s[(s.n>=MIN_J)&(s.index.isin(active))&(s.hold>=HOLD_MIN_H)&(s.hold<=HOLD_MAX_H)].copy()
    mart=martingale_flags(j[j.wallet.isin(elig.index)])
    elig["martingale"]=elig.index.map(mart).fillna(False)
    clean=elig[~elig["martingale"]].copy()
    clean["skill"]=z(clean.win)+z(clean.sharpe)+z(-clean.maxdd)
    print(f"FULL UNIVERSE re-eval funnel:")
    print(f"  raw wallets in m02: {n_uni}")
    print(f"  copyable+active+>={MIN_J}j+hold-band ELIGIBLE: {len(elig)}")
    print(f"  martingale-VETOED: {int(elig['martingale'].sum())} ({elig['martingale'].mean()*100:.0f}% of eligible)")
    print(f"  martingale-CLEAN pool: {len(clean)}")
    clean.sort_values("skill",ascending=False).to_parquet("/tmp/reeval_clean_pool.parquet")
    json.dump(list(clean.index), open("/tmp/reeval_clean_wallets.json","w"))
    print(f"  saved clean pool -> /tmp/reeval_clean_pool.parquet ({len(clean)} wallets) for MTM-DD + copy-edge steps")
if __name__=="__main__": main()
