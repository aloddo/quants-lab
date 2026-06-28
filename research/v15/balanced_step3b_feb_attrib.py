#!/usr/bin/env python3
"""STEP 3b -- Feb -0.34 beta LEG-LEVEL attribution (codex r3 next step). Is the single-fold beta tilt from
1-2 legs / a wallet cluster (-> fix with ex-ante caps, rerun all folds) or BROAD (-> needs stronger
neutralization)? Reuses the step3 marked-leg machinery. For the Feb fold (train=Dec,Jan) we take the selected
risk-parity 50/50 legs, build EACH leg's 1h MTM return series over Feb, and report: per-leg realized beta +
W*beta contribution + PnL, long-sleeve vs short-sleeve net beta, and concentration (top 1/3/5 share)."""
import sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import balanced_step3_riskparity as B
import leadlag_clean_rank_sim as S


def leg_series(w, il, W, legs, tm, btc_hourly):
    """1h MTM equity-contribution series for ONE leg over month tm (open bags marked each hour)."""
    tm_start = int(pd.Timestamp(tm+"-01").value//1e6)
    tm_end = int((pd.Timestamp(tm+"-01")+pd.offsets.MonthBegin(1)).value//1e6)
    grid = list(range((tm_start//B.HR)*B.HR, tm_end, B.HR))
    items = [it for it in legs.get((w, il), []) if tm_start <= it[1] < tm_end]
    if not items: return None, None
    eq = np.zeros(len(grid)); per = W/len(items)
    from execution_model import fee_rt, slip_oneway
    FEE_T = fee_rt(maker=False)
    for (coin, ent, ext, mo, net) in items:
        cost = FEE_T + slip_oneway(coin)*2
        for gi, h in enumerate(grid):
            if h < ent: continue
            fr = B.og_frac(coin, il, ent, min(h, ext))
            if fr is None: continue
            fr = max(-B.CAP, min(B.CAP, fr))
            eq[gi] += per*(fr - cost)
    br = np.array([btc_hourly.get(h, 0.0) for h in grid])
    return eq, br


def main():
    cols = ["wallet","coin","side","entry_ts","exit_ts","max_position_notional"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j.dropna(subset=["entry_ts","exit_ts"]); j = j[(j.max_position_notional > 10) & (j.exit_ts > j.entry_ts)]
    j["is_long"] = j.side.str.lower().str.contains("long")
    j["mon"] = pd.to_datetime(j.entry_ts, unit="ms").dt.strftime("%Y-%m")
    j = j[j.mon.isin(B.MONTHS)]
    vc = j.groupby("wallet").size(); j = j[j.wallet.isin(set(vc[vc >= 50].index))]
    print(f"marking pool {j.wallet.nunique()} wallets ...", flush=True)
    legs, exph = B.build_legpool(j)
    btc_by_mon = {m: B.btc_ret_hourly(int(pd.Timestamp(m+'-01').value//1e6),
                  int((pd.Timestamp(m+'-01')+pd.offsets.MonthBegin(1)).value//1e6)) for m in ["2025-12","2026-01","2026-02"]}
    tm = "2026-02"; prior = ["2025-12","2026-01"]
    chL, chS, _ = B.select_fold(legs, exph, prior, btc_by_mon)
    bh = btc_by_mon[tm]
    print(f"Feb fold: {len(chL)} long legs + {len(chS)} short legs", flush=True)
    rows = []
    for (w, W, _) in chL: rows.append((w, True, W))
    for (w, W, _) in chS: rows.append((w, False, W))
    recs = []
    for (w, il, W) in rows:
        eq, br = leg_series(w, il, W, legs, tm, bh)
        if eq is None or len(eq) < 5: continue
        pr = np.diff(eq); b = br[1:]
        beta = float(np.polyfit(b, pr, 1)[0]) if np.std(b) > 1e-9 else 0.0
        recs.append({"wallet": w[:10], "side": "L" if il else "S", "W": W, "pnl": float(eq[-1])*100,
                     "beta": beta, "wbeta": W*beta})
    d = pd.DataFrame(recs)
    Lb = d[d.side == "L"]["wbeta"].sum(); Sb = d[d.side == "S"]["wbeta"].sum()
    print(f"\nSLEEVE net beta (sum W*beta): LONG {Lb:+.3f} | SHORT {Sb:+.3f} | TOTAL {Lb+Sb:+.3f}")
    print(f"(matches the Feb realized -0.34 if construction-consistent)")
    d["abswb"] = d["wbeta"].abs()
    tot = d["abswb"].sum()
    ds = d.sort_values("wbeta")   # most negative (short-beta) first
    print(f"\nTop 6 NEGATIVE-beta contributors (drive the short tilt):")
    print(ds.head(6)[["wallet","side","W","pnl","beta","wbeta"]].to_string(index=False))
    print(f"\nConcentration of |W*beta|: top1 {d.nlargest(1,'abswb')['abswb'].sum()/tot*100:.0f}% | "
          f"top3 {d.nlargest(3,'abswb')['abswb'].sum()/tot*100:.0f}% | top5 {d.nlargest(5,'abswb')['abswb'].sum()/tot*100:.0f}% "
          f"| n_legs {len(d)}")
    net = Lb+Sb
    top3share = d.nlargest(3,'abswb')['abswb'].sum()/tot
    print(f"\nVERDICT: {'CONCENTRATED (top3 carry '+str(round(top3share*100))+'% of beta mass) -> ex-ante leg/beta cap fixes it' if top3share>0.5 else 'BROAD -> needs stronger neutralization constraint'}")
    print("(If concentrated, add an ex-ante per-leg |W*beta| cap and RE-RUN ALL FOLDS -- must not overfit to Feb;")
    print(" validate on frozen holdout, codex r1 look-ahead rule.)")


if __name__ == "__main__":
    main()
