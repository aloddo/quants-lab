#!/usr/bin/env python
"""
build_skill_cohort.py -- generate the DEPLOYABLE skill-ranked cohort + a final forward re-validation on the
COPYABLE universe (only coins we can faithfully copy = agentC-calibrated, liquid).

Gates baked in (from the deploy-caution handoff):
- copyable: skill computed ONLY on the leader's journeys in calibrated/liquid coins we trade.
- active: wallet traded a copyable coin in the last ACTIVE_DAYS of data.
- enough sample: >= MIN_J copyable journeys.
- skill rank = z(win)+z(sharpe)+z(-maxdd), NO return weight.

Outputs: /tmp/skill_cohort_deploy.json (the 100-wallet dict, engine format) + prints a final OOS
forward re-validation (copyable-restricted) confirming SKILL still beats PnL before deploy.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v16/build_skill_cohort.py
"""
import json
import numpy as np
import pandas as pd

ASOF = "2026-05-23"          # selection uses data <= asof (live cohort_asof)
ACTIVE_DAYS = 14
MIN_J = 40
K = 100
RT_COST_BPS = 11.0
HOLD_MIN_H = 2.0             # LAG-ROBUST filter: median hold >= 2h (copy entry-lag immaterial; the <1h
HOLD_MAX_H = 48.0           # scalpers are lag-sensitive +44bps, the 2-24h band is +107bps -- drop scalps)


def max_dd(r):
    eq = np.cumsum(r)
    return float((np.maximum.accumulate(eq) - eq).max()) if len(eq) else 0.0


def skill_scores(df):
    g = df.groupby("wallet")
    s = g.agg(n=("ret", "size"), mean=("ret", "mean"), std=("ret", "std"),
             win=("ret", lambda x: (x > 0).mean()), sum_pnl=("realized_pnl", "sum"),
             liq=("liq_closed", "mean"), hold=("duration_h", "median"))
    s["sharpe"] = s["mean"] / (s["std"] + 1e-9)
    s["maxdd"] = g["ret"].apply(max_dd)
    return s


def z(x):
    return (x - x.mean()) / (x.std() + 1e-9)


def main():
    calib = set(json.load(open("/tmp/agentC_l2_calib_expanded.json")).keys())
    cols = ["wallet", "coin", "entry_ts", "realized_pnl", "net_realized_pnl",
            "max_position_notional", "liq_closed", "duration_h"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j[(j.max_position_notional > 10) & (j.coin.isin(calib))].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["t"] = pd.to_datetime(j["entry_ts"], unit="ms")
    asof = pd.Timestamp(ASOF)
    j = j[j.t <= asof]

    # active filter
    last = j.t.max()
    active = set(j[j.t >= last - pd.Timedelta(days=ACTIVE_DAYS)].wallet.unique())

    # ---- FINAL FORWARD RE-VALIDATION (copyable universe, train/forward split) ----
    cutoff = asof - pd.Timedelta(days=30)
    tr, fw = j[j.t < cutoff], j[j.t >= cutoff]
    ts = skill_scores(tr)
    fwm = fw.groupby("wallet")["ret"].agg(["mean", "count"]).rename(columns={"mean": "fwd", "count": "fn"})
    v = ts.join(fwm, how="inner")
    v = v[(v.n >= MIN_J) & (v.fn >= 10) & (v.index.isin(active)) & (v.hold >= HOLD_MIN_H) & (v.hold <= HOLD_MAX_H)]
    v["skill"] = z(v.win) + z(v.sharpe) + z(-v.maxdd)
    pnl_fwd = v.nlargest(K, "sum_pnl")["fwd"].mean() * 1e4 - RT_COST_BPS
    skl_fwd = v.nlargest(K, "skill")["fwd"].mean() * 1e4 - RT_COST_BPS
    skl_hit = (v.nlargest(K, "skill")["fwd"] > 0).mean() * 100
    print("=== FINAL FORWARD RE-VALIDATION (copyable universe, active wallets) ===")
    print(f"  K={K}: SKILL net {skl_fwd:+.1f}bps (hit {skl_hit:.0f}%) vs PnL net {pnl_fwd:+.1f}bps "
          f"-> {'SKILL WINS' if skl_fwd > pnl_fwd else 'PnL'} (+{skl_fwd - pnl_fwd:.1f})")

    # ---- BUILD the deployable cohort (all data <= asof) ----
    s = skill_scores(j)
    s = s[(s.n >= MIN_J) & (s.index.isin(active)) & (s.hold >= HOLD_MIN_H) & (s.hold <= HOLD_MAX_H)].copy()
    s["skill"] = z(s.win) + z(s.sharpe) + z(-s.maxdd)
    top = s.nlargest(K, "skill").reset_index()
    print(f"\n=== DEPLOYABLE SKILL COHORT (top {K}, copyable+active, data <= {ASOF}) ===")
    print(f"  pool: {len(s)} eligible | selected {len(top)}")
    print(f"  win mean {top.win.mean():.3f} | sharpe mean {top.sharpe.mean():.2f} | "
          f"median journeys {int(top.n.median())} | liq mean {top.liq.mean():.4f} | "
          f"median mean-ret {top['mean'].median()*1e4:+.0f}bps")

    cohort = {}
    for i, r in top.iterrows():
        cohort[r["wallet"]] = {"group": "v16_skill_decile", "rank": int(i + 1),
                                "skill_win": round(float(r["win"]), 3),
                                "skill_sharpe": round(float(r["sharpe"]), 2),
                                "n_rt": int(r["n"])}
    out = {"cohort_asof": f"{ASOF}T23:59:59.000000+00:00", "n": len(cohort),
           "method": "skill_rank z(win)+z(sharpe)+z(-maxdd) on copyable coins, active>=14d, n>=40",
           "wallets": cohort}
    json.dump(out, open("/tmp/skill_cohort_deploy.json", "w"), indent=2)
    cur = set(json.load(open("config/copy_trader_wallets_v17_expansion.json"))["wallets"].keys())
    print(f"  overlap with current live cohort: {len(set(cohort) & cur)}/{K}")
    print(f"  saved /tmp/skill_cohort_deploy.json ({len(cohort)} wallets)")


if __name__ == "__main__":
    main()
