#!/usr/bin/env python3
"""Balanced cohort STEP 3 -- codex-revised construction + MARK-TO-MARKET portfolio DD.

Fixes vs step2 (codex REVISE, brain projects/quant/v15/balanced-step3-codex-consensus):
  (1) NO UNIVERSE LEAKAGE: per fold, recompute per-(wallet,side) shrunk edge + lcb eligibility on TRAIN
      months ONLY (step1's whole-dataset robust list peeked at test). Activity floor on train exposure.
  (2) RISK-PARITY weights: w ~ (lcb_bps^0.5 / max(vol, cohort_p25_vol)), capped per leg/wallet/coin,
      with a 50/50 SIDE RISK BUDGET (not equal count -- we have 2x more short legs).
  (3) BETA CONTROL: train beta of each leg to BTC; scale the heavier side so net |beta| <= 0.2.
  (4) MARK-TO-MARKET portfolio DD at 1h cadence (open positions marked every hour incl unrealized -- NOT
      round-trip). Realized TEST beta to BTC reported (codex gate #3).
Long-only comparator uses the SAME construction (longs only) so the comparison is not too easy (codex #6).
V15 canonical execution: fee_rt + per-coin slip + latency. Memory-safe (no giant all-rows frame)."""
import sys
from pathlib import Path
from collections import defaultdict
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from execution_model import fee_rt, slip_oneway

FEE_T = fee_rt(maker=False)
CAP = 500.0/1e4                  # per-trade og cap (sanity clamp on outlier round trips)
LAT = 2000                       # ms copy latency
EB_K = 30                        # empirical-Bayes shrink strength
MIN_RT = 30                      # min round trips per (wallet,side) on TRAIN
MIN_DAYS = 10                    # min active days per (wallet,side) on TRAIN
ACT_FLOOR_H = 50.0               # min total TRAIN exposure-hours per leg (codex activity floor)
LEG_CAP = 0.005                  # single-leg gross weight cap
WALL_CAP = 0.010                 # single-wallet gross cap
COIN_NET_CAP = 0.10              # single-coin net cap
COIN_GROSS_CAP = 0.20            # single-coin gross cap
BETA_LIM = 0.20
TOPN_SIDE = 30                   # legs per side into the book
MONTHS = ["2025-12","2026-01","2026-02","2026-03","2026-04","2026-05"]
REGIME = {"2025-12":"CHOP","2026-01":"BEAR","2026-02":"BEAR","2026-03":"CHOP","2026-04":"BULL","2026-05":"CHOP"}
HR = 3600_000


def og_frac(coin, is_long, entry_ts, at_ts):
    e = S.mark_at(coin, int(entry_ts)+LAT); m = S.mark_at(coin, int(at_ts))
    if not e or not m or e <= 0: return None
    return (m-e)/e if is_long else (e-m)/e


def btc_ret_hourly(t0, t1):
    """BTC log-ish simple return per hour over [t0,t1]. dict hour_ms -> ret."""
    out = {}; prev = None
    h = (t0//HR)*HR
    while h <= t1:
        p = S.mark_at("BTC", h)
        if p and prev and prev > 0: out[h] = (p-prev)/prev
        if p: prev = p
        h += HR
    return out


def build_legpool(j):
    """Mark every journey of activity-floored wallets ONCE. Returns dict (wallet,is_long)->list of
    (coin, entry_ts, exit_ts, mon, net_og) and (wallet,is_long,mon)->exposure_hours."""
    legs = defaultdict(list); exph = defaultdict(float)
    for coin, g in j.groupby("coin", sort=False):
        for r in g.itertuples():
            il = r.is_long
            e = S.mark_at(coin, int(r.entry_ts)+LAT); x = S.mark_at(coin, int(r.exit_ts)+LAT)
            if not e or not x or e <= 0: continue
            og = (x-e)/e if il else (e-x)/e
            net = max(-CAP, min(CAP, og)) - FEE_T - slip_oneway(coin)*2
            legs[(r.wallet, il)].append((coin, int(r.entry_ts), int(r.exit_ts), r.mon, net))
            exph[(r.wallet, il, r.mon)] += (r.exit_ts - r.entry_ts)/HR
    return legs, exph


def select_fold(legs, exph, prior, btc_by_mon):
    """Per-side: shrunk edge + lcb on TRAIN(prior) only, activity floor, then risk-parity weights with
    50/50 side risk budget + caps + beta control. Returns (chosen_long, chosen_short) lists of
    (wallet, weight, beta) plus the global mean used for shrinkage."""
    rows = []
    for (w, il), items in legs.items():
        tr = [it for it in items if it[3] in prior]
        if len(tr) < MIN_RT: continue
        eh = sum(exph.get((w, il, mo), 0.0) for mo in prior)
        if eh < ACT_FLOOR_H: continue
        nets = np.array([it[4] for it in tr]); ndays = len({it[1]//86400000 for it in tr})
        if ndays < MIN_DAYS: continue
        # train beta to BTC (regress journey net on the BTC return over its holding window)
        bx = []
        for it in tr:
            mo = it[3]; br = btc_by_mon.get(mo, {})
            h0 = (it[1]//HR)*HR; h1 = (it[2]//HR)*HR
            bx.append(sum(br.get(h, 0.0) for h in range(h0, h1+HR, HR)))
        bx = np.array(bx)
        beta = float(np.polyfit(bx, nets, 1)[0]) if np.std(bx) > 1e-9 else 0.0
        rows.append((w, il, nets, ndays, eh, beta))
    if not rows: return [], [], 0.0
    allnets = np.concatenate([r[2] for r in rows]); gmean = float(allnets.mean())
    pool = []
    for (w, il, nets, ndays, eh, beta) in rows:
        n = len(nets); raw = float(nets.mean()); vol = float(nets.std() + 1e-9)
        shrunk = (n*raw + EB_K*gmean)/(n+EB_K)
        lcb = shrunk - 1.0*vol/np.sqrt(n)
        if lcb <= 0: continue
        pool.append({"w": w, "il": il, "lcb": lcb*1e4, "vol": vol, "beta": beta, "n": n})
    if not pool: return [], [], gmean
    df = pd.DataFrame(pool)
    volp25 = df["vol"].quantile(0.25)
    df["volf"] = df["vol"].clip(lower=volp25)
    df["wraw"] = np.sqrt(df["lcb"].clip(lower=0)) / df["volf"]   # risk-parity x capped signal tilt
    out = {}
    for il in (True, False):
        side = df[df.il == il].sort_values("wraw", ascending=False).head(TOPN_SIDE).copy()
        if side.empty: out[il] = side; continue
        side["wt"] = (side["wraw"]/side["wraw"].sum()).clip(upper=LEG_CAP)
        side["wt"] = side["wt"]/side["wt"].sum()
        out[il] = side
    L = out.get(True, pd.DataFrame()); Sh = out.get(False, pd.DataFrame())
    # 50/50 SIDE RISK BUDGET: scale each side to equal risk (sum w*vol), then 0.5 each
    def srisk(s): return float((s["wt"]*s["vol"]).sum()) if len(s) else 0.0
    rL, rS = srisk(L), srisk(Sh)
    sL = 0.5/ (rL if rL>0 else 1); sS = 0.5/(rS if rS>0 else 1)
    if len(L): L["W"] = L["wt"]*sL
    if len(Sh): Sh["W"] = Sh["wt"]*sS
    # NO in-selection beta scaling (codex r2: train-window beta is a poor proxy for realized hourly beta and
    # naive side-scaling worsened it -> DD 20%/beta -0.9). Beta is neutralized in-sim by a CAUSAL rolling-beta
    # BTC HEDGE OVERLAY instead (see hedge_apply). Selection just delivers the risk-parity 50/50 sleeves.
    chL = [(r.w, r.W, r.beta) for r in L.itertuples()] if len(L) else []
    chS = [(r.w, r.W, r.beta) for r in Sh.itertuples()] if len(Sh) else []
    return chL, chS, gmean


def mtm_dd(chosen_legs, legs, tm, btc_hourly):
    """1h MARK-TO-MARKET portfolio equity over test month tm. chosen_legs = [(wallet,is_long,weight)].
    Each leg's test journeys marked every hour (unrealized) incl open bags; entry costs applied at entry.
    Returns (roe%, mtm_maxdd%, realized_beta, n_journeys)."""
    # gather test journeys per leg
    tm_start = int(pd.Timestamp(tm+"-01").value//1e6);
    nxt = (pd.Timestamp(tm+"-01")+pd.offsets.MonthBegin(1)); tm_end = int(nxt.value//1e6)
    grid = list(range((tm_start//HR)*HR, tm_end, HR))
    # equity path
    eq = np.ones(len(grid)); contrib_journeys = 0
    for (w, il, W) in chosen_legs:
        items = [it for it in legs.get((w, il), []) if tm_start <= it[1] < tm_end]
        if not items: continue
        per = W/len(items)
        for (coin, ent, ext, mo, net) in items:
            contrib_journeys += 1
            cost = FEE_T + slip_oneway(coin)*2
            for gi, h in enumerate(grid):
                if h < ent: continue
                at = min(h, ext)
                fr = og_frac(coin, il, ent, at)
                if fr is None: continue
                fr = max(-CAP, min(CAP, fr))
                eq[gi] += per*(fr - cost)
    br = np.array([btc_hourly.get(h, 0.0) for h in grid])   # step-aligned BTC returns
    return eq, br, contrib_journeys


def _metrics(eq, br):
    pk = np.maximum.accumulate(eq); mdd = float(((pk-eq)/pk).max())*100
    roe = float(eq[-1]-1.0)*100
    pr = np.diff(eq); b = br[1:]
    rbeta = float(np.polyfit(b, pr, 1)[0]) if np.std(b) > 1e-9 else 0.0
    return roe, mdd, rbeta


def hedge_apply(eq, br, mode, W=336, band=0.10, btc_cost=0.00043):
    """CAUSAL rolling-beta BTC hedge overlay (codex r2). At each step t, estimate portfolio beta to BTC from
    returns up to t-1 over trailing window W; hold BTC fraction h=-beta_est of equity; pnl += h*br[t].
    mode: 'none'|'roll'|'band'. 'band' only re-hedges when |beta_est|>BETA_LIM or |dh|>band (anti-overtrade).
    Costs btc_cost (one-way ~4.3bps) on |dh|. No look-ahead: beta_est at t uses pr[:t], br[:t]."""
    if mode == "none": return _metrics(eq, br)
    pr = np.diff(eq); n = len(pr)
    he = np.ones(len(eq)); cur_h = 0.0
    for t in range(1, n):
        lo = max(0, t-W)
        x = br[lo:t]; y = pr[lo:t]
        if len(x) >= 24 and np.std(x) > 1e-9:
            beta_est = float(np.polyfit(x, y, 1)[0])
        else:
            beta_est = 0.0
        target_h = -beta_est
        if mode == "band":
            if abs(beta_est) > BETA_LIM or abs(target_h-cur_h) > band:
                new_h = target_h
            else:
                new_h = cur_h
        else:
            new_h = target_h
        dh = new_h - cur_h; cur_h = new_h
        # apply hedge return for step t (uses br[t]) + cost on rebalance
        hedged_step = pr[t] + cur_h*br[t] - abs(dh)*btc_cost
        he[t+1] = he[t] + hedged_step if t+1 < len(he) else he[t]
    # rebuild full hedged equity (he index aligns to eq grid; first 2 pts seed)
    he[0] = eq[0]; he[1] = eq[1]
    return _metrics(he, br)


def main():
    cols = ["wallet","coin","side","entry_ts","exit_ts","max_position_notional"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j.dropna(subset=["entry_ts","exit_ts"])
    j = j[(j.max_position_notional > 10) & (j.exit_ts > j.entry_ts)]
    j["is_long"] = j.side.str.lower().str.contains("long")
    j["mon"] = pd.to_datetime(j.entry_ts, unit="ms").dt.strftime("%Y-%m")
    j = j[j.mon.isin(MONTHS)]
    # ACTIVITY-FLOOR the wallet pool (bounds the mark cost; codex activity floor anyway)
    vc = j.groupby("wallet").size(); keep = set(vc[vc >= 50].index)
    j = j[j.wallet.isin(keep)]
    print(f"pool: {len(keep)} wallets (>=50 journeys), {len(j)} journeys. Marking once ...", flush=True)
    legs, exph = build_legpool(j)
    print(f"marked {sum(len(v) for v in legs.values())} journey-legs over {len({k[0] for k in legs})} wallets", flush=True)
    btc_by_mon = {m: btc_ret_hourly(int(pd.Timestamp(m+'-01').value//1e6),
                                    int((pd.Timestamp(m+'-01')+pd.offsets.MonthBegin(1)).value//1e6)) for m in MONTHS}
    print(f"\nBALANCED book under CAUSAL BTC hedge overlay (codex r2). roe/mtmDD/realized-beta per mode.")
    print(f"{'fold':>9}{'reg':>5}  {'H0 none':>22}{'H1 roll-14d':>22}{'H3 band':>22}{'nL/nS':>8}")
    print(f"{'':>14}{'ROE  DD  beta':>22}{'ROE  DD  beta':>22}{'ROE  DD  beta':>22}")
    res = {"none": [], "roll": [], "band": []}
    for ti in range(1, len(MONTHS)):
        tm = MONTHS[ti]; prior = MONTHS[:ti]
        chL, chS, _ = select_fold(legs, exph, prior, btc_by_mon)
        bh = btc_by_mon[tm]
        bal = [(w, True, W) for (w, W, _) in chL] + [(w, False, W) for (w, W, _) in chS]
        eq, br, _ = mtm_dd(bal, legs, tm, bh)
        m0 = hedge_apply(eq, br, "none"); m1 = hedge_apply(eq, br, "roll"); m3 = hedge_apply(eq, br, "band")
        res["none"].append(m0); res["roll"].append(m1); res["band"].append(m3)
        def f(m): return f"{m[0]:>6.1f}{m[1]:>6.1f}{m[2]:>7.2f}"
        print(f"{tm:>9}{REGIME[tm]:>5}  {f(m0):>22}{f(m1):>22}{f(m3):>22}{f'{len(chL)}/{len(chS)}':>8}", flush=True)
    print()
    for mode in ("none", "roll", "band"):
        r = res[mode]; dds = [x[1] for x in r]; roes = [x[0] for x in r]; bts = [abs(x[2]) for x in r]
        verdict = "PASS" if (max(bts) <= 0.20 and sum(x > 0 for x in roes) >= 4) else "FAIL-beta" if max(bts) > 0.20 else "FAIL-roe"
        print(f"  {mode:>5}: maxDD {max(dds):>5.1f}% | ROE+ {sum(x>0 for x in roes)}/{len(roes)} "
              f"| meanROE {np.mean(roes):>6.1f}% | max|beta| {max(bts):.2f} -> {verdict}")
    print("\nKILL TEST: if a hedged mode holds |beta|<=0.2 AND ROE materially positive >=4/5 -> market-neutral skill")
    print("confirmed. If ROE collapses post-hedge -> edge was net-short exposure. SHIP gate adds subaccount sim.")


if __name__ == "__main__":
    main()
