#!/usr/bin/env python3
"""copy_consistent_winners_oos.py -- "is there a consistently profitable trader we can COPY?" (Alberto 9876).

SURVIVORSHIP-SAFE, PORTFOLIO-AWARE, ROLLING-FOLD test (codex design gate, 2026-06-22). Distinguishes
"profitable on their account" from "profitable when WE copy them after real execution".

V15-native: reuses fidelity_replay.roundtrips + execution_model (canonical maker/taker fee + latency) +
leadlag_clean_rank_sim.mark_at. Liquid-only by default (microcap mark artifacts), --all-coin sensitivity.

CODEX-REQUIRED RIGOR:
- SELECTION strictly at each fold's train-end, NO future knowledge. Cohort membership decided on TRAIN only.
- RETAIN every selected wallet through the test even if it stops trading / disappears (contributes 0, stays
  in the capital base). NEVER require test activity for inclusion. Report attrition.
- PORTFOLIO-AWARE grading: equal capital slice per selected wallet (idle slices = drag), copy each test
  round-trip at the slice notional through execution_model; portfolio ROE + maxDD + turnover + %wallets>0,
  beside mean/median bps/trade. random + all-eligible baselines. taker primary, maker shown.
- ROLLING folds (expanding train, ~1mo test). Size = SENSITIVITY (--size-min), not a hard gate.
PASS = positive DEPLOYABLE portfolio taker PnL across folds, beating random. Not just positive mean bps.

Run: ~/miniforge3/envs/quants-lab/bin/python research/v15/copy_consistent_winners_oos.py
"""
from __future__ import annotations
import argparse, sys, json
from collections import defaultdict
from pathlib import Path
import numpy as np, pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
import leadlag_clean_rank_sim as S
from fidelity_replay import roundtrips
from execution_model import fee_rt, set_latency_ms, slip_oneway

FEE_T = fee_rt(maker=False); FEE_M = fee_rt(maker=True)
CAP = 500.0 / 1e4                     # per-trade reconstruction-outlier clip (bps fraction)
MONTH_MS = 30 * 86400_000

# rolling folds: (train_start, test_start, test_end). Expanding train, ~3-week test windows.
FOLDS = [
    ("2025-12-01", "2026-03-01", "2026-03-22"),
    ("2025-12-01", "2026-03-22", "2026-04-12"),
    ("2025-12-01", "2026-04-12", "2026-05-03"),
    ("2025-12-01", "2026-05-03", "2026-05-23"),
]


def _ms(d): return int(pd.Timestamp(d, tz="UTC").timestamp() * 1000)


def copy_net(rts, lo, hi, lat, liquid, fee):
    """list of per-trade copy net returns (fraction) for round-trips with entry in [lo,hi)."""
    out = []
    for c, dir_, ets, xts, evw, xvw, g in rts:
        if not (lo <= ets < hi):
            continue
        if liquid is not None and c not in liquid:
            continue
        ent = S.mark_at(c, ets + lat); ex = S.mark_at(c, xts + lat)
        if ent is None or ex is None or ent <= 0:
            continue
        og = max(-CAP, min(CAP, dir_ * (ex - ent) / ent))
        slip = slip_oneway(c) * 2.0            # canonical per-coin slippage, entry + exit (CLAUDE.md rule)
        out.append((xts, og - fee - slip))     # (exit_ts, net) for equity-curve ordering
    return out


def leader_monthly_pos_frac(rts, lo, hi):
    """fraction of active calendar-months in [lo,hi) where the leader's own gross PnL proxy is > 0."""
    bym = defaultdict(float);
    for c, dir_, ets, xts, evw, xvw, g in rts:
        if lo <= ets < hi:
            bym[ets // MONTH_MS] += max(-CAP, min(CAP, g))
    if not bym:
        return 0.0, 0
    vals = np.array(list(bym.values()))
    return float((vals > 0).mean()), len(vals)


def skill_components(tr):
    """LIVE-cohort-equivalent skill from leader's OWN train round-trips: (win, sharpe, neg_maxdd).
    Mirrors the live selector z(win)+z(sharpe)+z(-maxdd) -- NO copy-edge term, NO return term."""
    g = np.array([max(-CAP, min(CAP, r[6])) for r in tr])
    if len(g) < 2:
        return None
    win = float((g > 0).mean()); sharpe = float(g.mean() / (g.std(ddof=1) + 1e-12))
    eq = np.cumsum(g); peak = np.maximum.accumulate(eq); maxdd = float((peak - eq).max())
    return win, sharpe, maxdd


def portfolio_metrics(cohort_rts_test, lat, liquid, fee, n_selected):
    """Survivorship-safe portfolio: equal slice per SELECTED wallet (dead wallets retained as idle slice).
    Returns ROE (on total allocated capital), maxDD, n_trades, %wallets>0, mean/median bps."""
    if n_selected == 0:
        return dict(roe=float("nan"), maxdd=float("nan"), n=0, wpos=float("nan"),
                    mean_bps=float("nan"), med_bps=float("nan"), attrition=float("nan"))
    slice_cap = 1.0 / n_selected               # total capital normalized to 1.0
    all_trades = []; wallet_ret = []; n_active = 0
    for rts_test in cohort_rts_test:           # one list per selected wallet (may be empty)
        nets = copy_net_precomputed(rts_test, lat, liquid, fee)
        if nets:
            n_active += 1
            wallet_ret.append(sum(n for _, n in nets))
            all_trades.extend(nets)
        else:
            wallet_ret.append(0.0)             # retained, idle -> 0
    if not all_trades:
        return dict(roe=0.0, maxdd=0.0, n=0, wpos=0.0, mean_bps=0.0, med_bps=0.0,
                    attrition=1.0)
    # portfolio ROE: each wallet's slice earns slice_cap * (sum of its per-trade returns)
    roe = sum(slice_cap * wr for wr in wallet_ret) * 100
    # equity curve (time-ordered) for maxDD: each trade contributes slice_cap*net at its exit
    all_trades.sort(key=lambda x: x[0])
    eq = 1.0; peak = 1.0; mdd = 0.0
    for _, net in all_trades:
        eq += slice_cap * net
        peak = max(peak, eq); mdd = max(mdd, (peak - eq) / peak)
    arr = np.array([n for _, n in all_trades])
    wr = np.array(wallet_ret)
    pos = wr[wr > 0]; top5_share = (np.sort(pos)[::-1][:5].sum() / pos.sum() * 100) if pos.sum() > 0 else float("nan")
    # leave-one-out: drop the single top contributor, renormalize slices
    if n_selected > 1:
        j = int(np.argmax(wr)); rem = np.delete(wr, j)
        loo_roe = rem.sum() / (n_selected - 1) * 100
    else:
        loo_roe = float("nan")
    turnover = len(arr) / max(n_active, 1)        # trades per ACTIVE wallet
    return dict(roe=roe, maxdd=mdd * 100, n=len(arr), wpos=(wr > 0).mean() * 100,
                mean_bps=arr.mean() * 1e4, med_bps=float(np.median(arr)) * 1e4,
                attrition=(1 - n_active / n_selected) * 100, turnover=turnover,
                top5_share=top5_share, loo_roe=loo_roe)


def copy_net_precomputed(nets_raw, lat, liquid, fee):
    return nets_raw  # nets already computed per (wallet,fold); kept for clarity


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-rt", type=int, default=30)
    ap.add_argument("--min-months", type=int, default=3)
    ap.add_argument("--pos-frac", type=float, default=0.6, help="leader positive in >= this frac of active months")
    ap.add_argument("--size-min", type=float, default=0.0, help="min account equity (source_equity_post); 0=off")
    ap.add_argument("--skill-k", type=int, default=100, help="live-cohort-equivalent skill top-K (default 100=live)")
    ap.add_argument("--latency-s", type=int, default=2)
    ap.add_argument("--all-coin", action="store_true")
    ap.add_argument("--universe-file", default="app/data/v15/m01_nonerroring_wallets.txt")
    ap.add_argument("--seed", type=int, default=17)
    args = ap.parse_args()
    set_latency_ms(args.latency_s * 1000); lat = args.latency_s * 1000
    liquid = None if args.all_coin else set(json.load(open(S._DATA / "l2_calib_10coin.json")).keys())
    uni = set(l.strip().lower() for l in open(args.universe_file) if l.strip() and not l.startswith("#"))
    g0, _, gN = _ms(FOLDS[0][0]), None, _ms(FOLDS[-1][2])
    print(f"loading m02 fills for {len(uni)} wallets ... (liquid_only={not args.all_coin}, lat={args.latency_s}s)")
    import pyarrow.parquet as pq
    pf = pq.ParquetFile(str(S.M02_ACTIONS))
    wf = defaultdict(list); weq = defaultdict(list)
    for b in pf.iter_batches(batch_size=1_000_000,
                             columns=["wallet", "coin", "ts", "signed_size", "price", "source_equity_post"]):
        d = b.to_pydict()
        for i in range(len(d["wallet"])):
            w = d["wallet"][i]; t = d["ts"][i]
            if w in uni and g0 <= t <= gN:
                wf[w].append((t, d["coin"][i], d["signed_size"][i], d["price"][i]))
                e = d["source_equity_post"][i]
                if e is not None and e > 0:
                    weq[w].append((t, e))
    print(f"{len(wf)} wallets with fills; reconstructing round-trips ...")
    wrts = {w: roundtrips(sorted(fl, key=lambda x: x[0])) for w, fl in wf.items()}

    print(f"\n=== CONSISTENT-COPYABLE-WINNERS OOS | min_rt={args.min_rt} min_months={args.min_months} "
          f"pos_frac>={args.pos_frac} size_min=${args.size_min:.0f} ===")
    print(f"{'fold':>6}{'cohort':>16}{'n_sel':>7}{'attr%':>7}{'taker_roe%':>11}{'maker_roe%':>11}"
          f"{'maxDD%':>8}{'medbps':>8}{'w>0%':>7}")
    rng = np.random.default_rng(args.seed)
    agg = defaultdict(list)
    for fi, (ts0, tss, tse) in enumerate(FOLDS):
        L0, TS, TE = _ms(ts0), _ms(tss), _ms(tse)
        # SELECTION on TRAIN [L0,TS): point-in-time, no future
        eligible = []; consistent = []; train_copy_pos = set(); skill_raw = {}
        for w, rts in wrts.items():
            tr = [r for r in rts if L0 <= r[2] < TS]
            if len(tr) < args.min_rt:
                continue
            pf_, nmo = leader_monthly_pos_frac(rts, L0, TS)
            if nmo < args.min_months:
                continue
            # account equity point-in-time at train-end
            if args.size_min > 0:
                eqs = [e for t, e in weq.get(w, []) if t < TS]
                if not eqs or max(eqs) < args.size_min:
                    continue
            eligible.append(w)
            sc = skill_components(tr)
            if sc is not None:
                skill_raw[w] = sc
            # train copy edge (taker) for the intersection cohort
            tcn = copy_net(rts, L0, TS, lat, liquid, FEE_T)
            if tcn and np.mean([n for _, n in tcn]) > 0:
                train_copy_pos.add(w)
            if pf_ >= args.pos_frac:
                consistent.append(w)
        cw_intersect = [w for w in consistent if w in train_copy_pos]
        rnd = list(rng.choice(eligible, size=min(len(consistent), len(eligible)), replace=False)) if eligible else []
        # LIVE-cohort-equivalent: SKILL-only top-K, then STRATIFY by train-copy-positive (codex #5)
        skill_top = []
        if skill_raw:
            sw = list(skill_raw); arr = np.array([skill_raw[w] for w in sw])
            def _z(c): return (c - c.mean()) / (c.std() + 1e-12)
            score = _z(arr[:, 0]) + _z(arr[:, 1]) + _z(-arr[:, 2])    # z(win)+z(sharpe)+z(-maxdd)
            order = np.argsort(score)[::-1][:args.skill_k]
            skill_top = [sw[i] for i in order]
        skill_cp = [w for w in skill_top if w in train_copy_pos]
        skill_cn = [w for w in skill_top if w not in train_copy_pos]
        cp_frac = (len(skill_cp) / len(skill_top) * 100) if skill_top else float("nan")

        def grade(cohort, fee):
            test_nets = [copy_net(wrts[w], TS, TE, lat, liquid, fee) for w in cohort]
            return portfolio_metrics(test_nets, lat, liquid, fee, len(cohort))

        fold_mt = {}
        for name, cohort in [("ALL-eligible", eligible), ("CONSISTENT", consistent),
                             ("CONSIST∩copy+", cw_intersect), ("RANDOM", rnd),
                             (f"SKILL-top{args.skill_k}", skill_top), ("SKILL∩copy+", skill_cp),
                             ("SKILL∩copy-", skill_cn)]:
            mt = grade(cohort, FEE_T); mm = grade(cohort, FEE_M); fold_mt[name] = mt
            agg[name].append(mt["roe"])
            print(f"{fi+1:>6}{name:>16}{len(cohort):>7}{mt['attrition']:>7.0f}{mt['roe']:>11.2f}"
                  f"{mm['roe']:>11.2f}{mt['maxdd']:>8.1f}{mt['med_bps']:>8.1f}{mt['wpos']:>7.0f}")
        print(f"       [skill-top{args.skill_k}: {cp_frac:.0f}% copy+ | turnover trades/wallet: "
              f"copy+ {fold_mt['SKILL∩copy+'].get('turnover',0):.1f} vs copy- "
              f"{fold_mt['SKILL∩copy-'].get('turnover',0):.1f} | copy+ top5share "
              f"{fold_mt['SKILL∩copy+'].get('top5_share',float('nan')):.0f}% | copy+ LOO-roe "
              f"{fold_mt['SKILL∩copy+'].get('loo_roe',float('nan')):+.2f}%]\n")

    names = ["ALL-eligible", "CONSISTENT", "CONSIST∩copy+", "RANDOM",
             f"SKILL-top{args.skill_k}", "SKILL∩copy+", "SKILL∩copy-"]
    print("=== AGGREGATE (mean taker portfolio ROE across folds, + folds>0) ===")
    for name in names:
        v = np.array(agg[name]); print(f"  {name:>16}: {v.mean():+7.2f}%  folds>0 {int((v>0).sum())}/{len(v)}")
    print("\nREAD (codex #5): if SKILL-only ~= SKILL∩copy+ AND SKILL∩copy- is also fine -> skill already")
    print("captures copyability, live cohort is OK. If SKILL∩copy+ >> SKILL-only/SKILL∩copy- -> the live")
    print("skill selector LEAVES EDGE: add a train-copy-edge filter (rank on OUR copy edge). codex-review next.")


if __name__ == "__main__":
    main()
