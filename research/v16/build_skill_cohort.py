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


def martingale_flags(df):
    """HARD martingale veto (codex 2026-06-05, was dropped at the 06-14 build -> re-added, Phase 3).
    High win-rate + low REALIZED drawdown is the SIGNATURE of averaging-down/martingale: hold losers,
    size up after losses, realize only winners. Closed-trip metrics are blind to the open bag; this veto
    catches the behavioral tell. Per wallet (journeys ordered by entry_ts): size-up-after-loss, holds-
    losers ratio, win/loss magnitude, ~0% realized losses. Returns bool Series (True = martingale = veto)."""
    out = {}
    for w, g in df.sort_values("entry_ts").groupby("wallet"):
        pnl = g["net_realized_pnl"].to_numpy(); ntl = g["max_position_notional"].to_numpy()
        dur = g["duration_h"].to_numpy(); n = len(pnl)
        if n < 10:
            out[w] = False; continue
        win = pnl > 0; loss = pnl < 0; nloss = int(loss.sum())
        nal = ntl[1:][loss[:-1]]; naw = ntl[1:][win[:-1]]
        su = (np.mean(nal) / np.mean(naw)) if (len(nal) and len(naw) and np.mean(naw) > 0) else np.nan
        ha = (np.mean(dur[loss]) / np.mean(dur[win])) if (nloss > 0 and win.any() and np.mean(dur[win]) > 0) else np.nan
        wm = (np.mean(pnl[win]) / np.mean(np.abs(pnl[loss]))) if (nloss > 0 and win.any() and np.mean(np.abs(pnl[loss])) > 0) else np.inf
        lf = nloss / n
        extreme = ((su == su and su > 3.0) or (ha == ha and ha > 5.0) or (lf < 0.02))
        mild = int((su == su and su > 1.3) + (ha == ha and ha > 2.5) + (wm < 0.6) + (lf < 0.05))
        out[w] = bool(extreme or mild >= 2)
    return pd.Series(out)


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


def mtm_dd_exclude(wallets):
    """MARK-TO-MARKET drawdown gate (Phase 3, Alberto 9947 + codex). Closed-trip maxdd is BLIND to open
    bags; this queries each candidate's CURRENT account mark-to-market equity (HL portfolio month history,
    incl unrealized) and excludes the near-ruin tail. Codex drop-19 criterion: MTM maxDD>=70% OR (60-70%
    AND month return<=-40%). Operational filter (current state = who is bag-holding/blown-up NOW); for
    backtesting the rule use v15_m01 historical reconstruction instead. Returns set to exclude."""
    import urllib.request, time
    def post(b):
        r = urllib.request.Request("https://api.hyperliquid.xyz/info", data=json.dumps(b).encode(),
                                   headers={"Content-Type": "application/json"})
        return json.load(urllib.request.urlopen(r, timeout=20))
    excl = set()
    for w in wallets:
        try:
            d = dict(post({"type": "portfolio", "user": w}))
            avh = d.get("month", {}).get("accountValueHistory", [])
            a = np.array([float(v) for _, v in avh]); a = a[a > 0]
            if len(a) < 5:
                continue
            dd = ((np.maximum.accumulate(a) - a) / np.maximum.accumulate(a)).max() * 100
            ret = (a[-1] / a[0] - 1) * 100
            if (dd >= 70) or (60 <= dd < 70 and ret <= -40):
                excl.add(w)
        except Exception:
            pass
        time.sleep(0.05)
    return excl


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
    # HARD MARTINGALE VETO (Phase 3, codex 06-05 rule re-added). Closed-trip skill metrics select FOR
    # martingales (high win + low realized DD); this disqualifies the behavioral tell BEFORE ranking.
    mart = martingale_flags(j[j.wallet.isin(s.index)])
    s["martingale"] = s.index.map(mart).fillna(False)
    n_mart = int(s["martingale"].sum())
    s = s[~s["martingale"]].copy()
    print(f"\n[Phase-3 martingale veto] eligible {len(s) + n_mart} -> vetoed {n_mart} martingales -> clean {len(s)}")
    s["skill"] = z(s.win) + z(s.sharpe) + z(-s.maxdd)
    # MARK-TO-MARKET DD GATE (Phase 3): rank by skill, then drop the near-ruin tail by CURRENT account
    # mark-to-market drawdown (incl unrealized -- the metric closed-trip maxdd is blind to). Query only the
    # top candidates (API-bounded), exclude, then take top-K survivors.
    cand = s.nlargest(int(K * 1.8), "skill")
    excl = mtm_dd_exclude(list(cand.index))
    print(f"[Phase-3 mark-to-market DD gate] top-{len(cand)} candidates -> excluded {len(excl)} near-ruin (MTM DD>=70% or 60-70%&down)")
    s = s[~s.index.isin(excl)].copy()
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
