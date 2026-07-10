#!/usr/bin/env python3
"""profit_loo_gate1.py -- Alberto's reframed gate (2026-07-07 voice): the test is PROFITABILITY, not
"no bags". A bag only matters if it makes OUR copy book LESS profitable. Measure, per live leader,
copy-WITH vs copy-WITHOUT on OUR realistic net edge (execution_model pricing, 4x gross, latched -25%
stop, hourly MTM incl open bags).

REUSES research/v15/our_drawdown_scorer.py engine wrappers (our_copy_run = OUR net ROE% + MTM-DD% over
the OOS window, marks open bags to market hourly). Adds a NET-EDGE leave-one-out: for each leader,
cohort ROE/DD with that leader removed. A leader is a PROFITABILITY DRAG iff removing them RAISES
cohort ROE (and/or cuts DD). Exclusions justified ONLY where the book gets more profitable.

Read-only. Codex-gated for any live swap. Run:
  ~/miniforge3/envs/quants-lab/bin/python research/v16/profit_loo_gate1.py [--smoke3] [--config PATH]
"""
from __future__ import annotations
import json, sys
import numpy as np

sys.path.insert(0, "research/v15")
import our_drawdown_scorer as ODS
from our_drawdown_scorer import (our_copy_run, live_whitelist, btc_ret_hourly, _ms,
                                 load_actions_for_leaders, _ACTS_CACHE, WIN_START, WIN_END, BUDGET)

CONFIG = "config/copy_trader_wallets_gate1_v4.json"
for a in sys.argv:
    if a.startswith("--config"):
        CONFIG = a.split("=", 1)[1] if "=" in a else sys.argv[sys.argv.index(a) + 1]


def main():
    smoke3 = "--smoke3" in sys.argv
    cfg = json.load(open(CONFIG))
    wallets = list(cfg["wallets"].keys())
    whitelist, base_coins, exp_coins = live_whitelist(cfg)
    if smoke3:
        wallets = wallets[:3]
    lo, hi = _ms(WIN_START), _ms(WIN_END)
    win = (lo, hi)
    print(f"PROFIT-LOO on {CONFIG}: {len(wallets)} leaders | {WIN_START}..{WIN_END} | $150/leader | "
          f"faithful-copy | whitelist {len(whitelist)} coins", flush=True)

    print("building BTC hourly grid ...", flush=True)
    btc_hourly = btc_ret_hourly(lo, hi)

    print("caching per-wallet actions (parquet scanned once each) ...", flush=True)
    for w in wallets:
        raw = load_actions_for_leaders({w}, tm=None, win=win)
        _ACTS_CACHE[w] = raw if raw.empty else raw[raw["coin"].isin(whitelist)].reset_index(drop=True)

    # (1) standalone per-leader OUR net edge + DD
    print("\n=== (1) PER-LEADER OUR net edge (standalone $150 copy) ===", flush=True)
    per = {}
    for i, w in enumerate(wallets, 1):
        roe, dd, rbeta, n_acts, _ = our_copy_run([w], win, btc_hourly, whitelist)
        per[w] = {"roe": roe, "dd": dd, "n": n_acts}
        print(f"  [{i:>2}] {w[:12]}  ROE={roe:8.2f}%  OUR-DD={dd:6.2f}%  n={n_acts}", flush=True)

    # (2) full cohort
    print("\n=== (2) COHORT (all netted) ===", flush=True)
    c_roe, c_dd, _b, c_acts, c_n = our_copy_run(wallets, win, btc_hourly, whitelist)
    print(f"  cohort n={c_n}  ROE={c_roe:.2f}%  OUR-DD={c_dd:.2f}%  n_actions={c_acts}", flush=True)

    # (3) NET-EDGE leave-one-out: drop each leader, re-net remaining, compare cohort ROE + DD
    print("\n=== (3) NET-EDGE LEAVE-ONE-OUT (drop leader -> cohort ROE/DD delta) ===", flush=True)
    loo = []
    for w in wallets:
        rest = [x for x in wallets if x != w]
        r_roe, r_dd, _b, _a, _n = our_copy_run(rest, win, btc_hourly, whitelist)
        d_roe = r_roe - c_roe      # >0 => removing this leader RAISES cohort ROE = profitability drag
        d_dd = r_dd - c_dd         # <0 => removing this leader CUTS cohort DD
        loo.append({"w": w, "roe_wo": r_roe, "dd_wo": r_dd, "d_roe": d_roe, "d_dd": d_dd,
                    "solo_roe": per[w]["roe"], "solo_dd": per[w]["dd"]})
    loo.sort(key=lambda x: -x["d_roe"])  # biggest profitability drag first
    print(f"  (baseline cohort ROE={c_roe:.2f}% DD={c_dd:.2f}%)", flush=True)
    print(f"  {'leader':14s} {'solo_ROE':>9s} {'solo_DD':>8s} {'ROE_wo':>8s} {'dROE':>7s} {'dDD':>7s}  verdict", flush=True)
    for r in loo:
        drag = r["d_roe"] > 0.0
        helps_dd = r["d_dd"] < 0.0
        verdict = "DROP (raises ROE)" if drag else ("keep" if not helps_dd else "keep (edge+, some DD)")
        print(f"  {r['w'][:14]:14s} {r['solo_roe']:9.2f} {r['solo_dd']:8.2f} "
              f"{r['roe_wo']:8.2f} {r['d_roe']:+7.2f} {r['d_dd']:+7.2f}  {verdict}", flush=True)

    out = {"config": CONFIG, "window": [WIN_START, WIN_END],
           "cohort": {"roe": c_roe, "dd": c_dd, "n": c_n},
           "per_leader": {w: per[w] for w in wallets},
           "loo": loo}
    json.dump(out, open("/tmp/profit_loo_gate1.json", "w"), indent=2)
    print("\nwrote /tmp/profit_loo_gate1.json", flush=True)
    print("\nINTERPRETATION: DROP only leaders whose removal RAISES cohort ROE (d_roe>0). A bag-holder "
          "with positive net contribution STAYS (Alberto: profitability, not no-bags-dogma).", flush=True)


if __name__ == "__main__":
    main()
