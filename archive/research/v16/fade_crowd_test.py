"""
FADE-THE-CROWD decisive test (blank-state #1). Does aggregate wallet net-positioning predict a coin's
forward return BEYOND the market move (de-trended), out-of-sample, across many liquid coins?
- net positioning per (coin, hour) = sum(signed notional opened by wallets).
- forward return at +4h/+12h; DE-TRENDED = coin_return - cross-sectional mean across coins that hour (strips beta).
- contrarian if high-net-long -> NEGATIVE de-trended forward return.
- OOS split by date; costed (one position, taker RT once).
"""
import numpy as np, pandas as pd
import sys; sys.path.insert(0, "research/v16"); sys.path.insert(0, "research/v15")
import decoupled_exit_portfolio as D, execution_model as EX
from scipy.stats import spearmanr

LIQUID = ["BTC","ETH","SOL","HYPE","XRP","DOGE","AVAX","LINK","LTC","ADA","DOT","BNB","SUI","TAO",
          "ARB","OP","APT","SEI","NEAR","AAVE","INJ","UNI","TON","TRX","ATOM","FIL","PEPE","WLD"]
MS_HR = 3600*1000

def main():
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet",
                        columns=["coin","side","entry_ts","max_position_notional"])
    j = j[(j.coin.isin(LIQUID)) & (j.max_position_notional > 200)].copy()
    j["signed"] = np.where(j.side=="long",1.0,-1.0)*j.max_position_notional
    j["hr"] = (j.entry_ts//MS_HR)*MS_HR
    flow = j.groupby(["coin","hr"]).signed.sum().rename("netflow").reset_index()
    flow["absflow"] = j.groupby(["coin","hr"]).max_position_notional.sum().values
    flow["netnorm"] = flow.netflow / flow.absflow.clip(lower=1)   # net positioning as fraction of gross (crowd tilt)

    # forward returns per coin via marks
    mk = {c: D.marks(c) for c in LIQUID}
    def fwd(coin, hr, h):
        m = mk.get(coin)
        if m is None: return np.nan
        mins, px = m; e = hr
        i0 = np.searchsorted(mins, e, "right")-1; i1 = np.searchsorted(mins, e+h*MS_HR, "right")-1
        if i0<0 or i1<=i0 or i1>=len(px) or px[i0]<=0: return np.nan
        return (px[i1]-px[i0])/px[i0]
    for h in [4,12]:
        flow[f"r{h}"] = [fwd(c, hr, h) for c, hr in zip(flow.coin, flow.hr)]
    flow = flow.dropna(subset=["r4","r12"])
    # DE-TREND: subtract cross-sectional mean return per hour (strips market beta)
    for h in [4,12]:
        flow[f"res{h}"] = flow[f"r{h}"] - flow.groupby("hr")[f"r{h}"].transform("mean")

    cut = flow.hr.quantile(0.70)
    for label, sub in [("ALL", flow), ("OOS (last 30%)", flow[flow.hr>cut])]:
        print(f"\n=== FADE-THE-CROWD {label} (n={len(sub)}) -- netnorm vs forward return ===")
        for h in [4,12]:
            sp_raw = spearmanr(sub.netnorm, sub[f"r{h}"]).correlation
            sp_res = spearmanr(sub.netnorm, sub[f"res{h}"]).correlation
            # quintile of crowd tilt -> de-trended forward return (contrarian => q4 negative)
            sub2 = sub.copy(); sub2["q"] = pd.qcut(sub2.netnorm.rank(method="first"), 5, labels=False)
            q = sub2.groupby("q")[f"res{h}"].mean()*1e4
            print(f"  +{h:2d}h: spearman RAW {sp_raw:+.3f} DE-TRENDED {sp_res:+.3f} | de-trended fwd by crowd-tilt quintile "
                  f"[q0..q4]: {[round(x,1) for x in q.values]}")
            # fade strategy: short q4 (crowd long) / long q0 (crowd short), de-trended, costed
            spread = (q.iloc[0] - q.iloc[4])  # long-short de-trended bps
            print(f"        FADE long-short de-trended spread {spread:+.1f}bps gross | minus ~{EX.fee_rt(False)*1e4:.1f}bps cost x2 legs")

if __name__ == "__main__":
    main()
