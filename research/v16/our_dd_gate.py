#!/usr/bin/env python3
"""our_dd_gate.py -- the OUR-DD / marginal-cohort-DD cohort gate (REPLACEMENT candidate for the
leader-account-DD gate mtm_dd_exclude in build_skill_cohort.py).

WHY THIS EXISTS (brain projects/quant/research/2026-06-24-our-drawdown-vs-leader-drawdown, codex-verified)
  The live gate build_skill_cohort.py::mtm_dd_exclude queries each LEADER's OWN HL account
  mark-to-market month drawdown -- at THEIR leverage/capital/risk -- and drops the near-ruin tail.
  Codex confirmed that is the WRONG yardstick: it mis-judges ~22/69 live leaders vs OUR realistic copy
  drawdown (we copy at $150/leader, 4x gross cap, -25% latched stop, 2s latency, execution_model fills,
  on a 50-coin whitelist). The right gate is each candidate's MARGINAL contribution to OUR netted-cohort
  drawdown, computed through the codex-reviewed faithful-copy netted engine at OUR sizing.

WHAT THIS IS
  our_dd_exclude(candidate_wallets, asof, window_days, target_dd, budget) -> (exclude_set, diag_df)
  Runs the candidate set as ONE netted faithful-copy account over [asof-window_days, asof] (selection-time,
  causal: NO data past asof). Computes cohort OUR-MTM-DD, then GREEDY leave-one-out: repeatedly drops the
  candidate whose removal most reduces current cohort OUR-DD, until cohort OUR-DD <= target_dd. Returns
  the greedily-dropped wallets (the set to EXCLUDE) plus a diagnostics frame (per-drop marginal DD
  reduction, cohort DD/ROE trajectory, plus each candidate's standalone DD/ROE for context).

  ALL the heavy lifting (faithful-copy netted engine, 50-coin whitelist, per-wallet cached action slices,
  execution_model pricing, windowed MTM-DD) is REUSED UNCHANGED from research/v15/our_drawdown_scorer.py
  and balanced_step4_netted_sim.py. This module is a thin, side-effect-free wrapper that exposes the
  greedy-LOO gate with a clean (wallets, asof) signature for build_skill_cohort.py to call.

CAUSALITY / NO-LOOK-AHEAD
  The window is [asof - window_days, asof]. All actions and marks consumed are <= asof (the engine's
  mark_at is asof-backward only; load_actions_for_leaders filters ts < end where end = asof_ms). A gate
  decision therefore uses ONLY selection-time data. The forward-holdout harness (our_dd_holdout.py) keeps
  the gate window strictly <= asof and the eval window strictly > asof.

MEMORY (CLAUDE.md rule 8): the 4.6GB m02_actions parquet is NEVER fully loaded. Each candidate wallet's
  whitelist-filtered in-window action slice is pyarrow-scanned ONCE and cached in our_drawdown_scorer's
  module-level _ACTS_CACHE; every LOO/greedy recompute re-nets the cached slices in RAM. Marks are
  page-cached once by leadlag_clean_rank_sim. Smoke a slice under /usr/bin/time -l before any full run.

Run (standalone smoke -- gate only, no holdout):
  ~/miniforge3/envs/quants-lab/bin/python research/v16/our_dd_gate.py --smoke
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# reuse the v15 OUR-DD machinery (engine wrapper, whitelist, cached-acts, windowed-DD)
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "v15"))

import our_drawdown_scorer as ODS  # noqa: E402  (our_copy_run, _cached_acts, live_whitelist, _ms)
from balanced_step3_riskparity import btc_ret_hourly  # noqa: E402

WALLETS_JSON = "config/copy_trader_wallets_v17_expansion.json"


def _live_whitelist():
    """FIX1 live tradeable universe = baseline coin_whitelist UNION expansion.coins, exactly as the live
    V17 book merges them (replicated in our_drawdown_scorer.live_whitelist)."""
    import json
    cfg = json.load(open(WALLETS_JSON))
    return ODS.live_whitelist(cfg)


def our_dd_exclude(candidate_wallets, asof, window_days=60, target_dd=25.0, budget=150.0,
                   whitelist=None, verbose=False):
    """Greedy leave-one-out OUR-DD cohort gate. REPLACEMENT for build_skill_cohort.py::mtm_dd_exclude.

    Args:
      candidate_wallets : iterable of leader wallet addresses (the skill-ranked candidate pool).
      asof              : selection cutoff (str 'YYYY-MM-DD' or pd.Timestamp). Window END. Causal:
                          no action or mark past asof is consumed.
      window_days       : selection window length in days; window = [asof - window_days, asof].
      target_dd         : drop leaders greedily until cohort OUR-MTM-DD% <= target_dd (default 25 =
                          the live -25% account stop).
      budget            : $ per leader (default 150, the live per-leader budget).
      whitelist         : optional pre-computed coin whitelist set; if None, loaded from the live config.
      verbose           : print the drop sequence.

    Returns:
      (exclude_set, diag_df)
        exclude_set : set of wallets greedily dropped to bring cohort OUR-DD <= target_dd.
        diag_df     : DataFrame, one row per candidate, columns:
                        wallet, standalone_dd, standalone_roe, n_acts, dropped (bool),
                        drop_step (int or NA), marginal_dd_reduction (pp), cohort_dd_after,
                        cohort_roe_after. Plus df.attrs holds {'cohort_dd_start','cohort_roe_start',
                        'cohort_dd_final','cohort_roe_final','target_dd','window','asof'}.

    Side-effect free w.r.t. engine defaults: our_copy_run swaps the engine loader inside its own call and
    restores it; execution_model default slip is NOT changed here.
    """
    wallets = list(dict.fromkeys(candidate_wallets))  # de-dupe, preserve order
    if budget != ODS.BUDGET:
        # our_copy_run hardcodes ODS.BUDGET; set it for this gate run (per-leader budget is the lever).
        ODS.BUDGET = float(budget)

    if whitelist is None:
        whitelist, _base, _exp = _live_whitelist()

    asof_ts = pd.Timestamp(asof)
    win_start = asof_ts - pd.Timedelta(days=window_days)
    lo = ODS._ms(win_start)
    hi = ODS._ms(asof_ts)
    win = (lo, hi)

    btc_hourly = btc_ret_hourly(lo, hi)

    # ---- prime the per-wallet whitelist-filtered cache ONCE (parquet scanned once per wallet) -------- #
    for w in wallets:
        if w not in ODS._ACTS_CACHE:
            raw = ODS.load_actions_for_leaders({w}, tm=None, win=win)
            ODS._ACTS_CACHE[w] = (raw if raw.empty
                                  else raw[raw["coin"].isin(whitelist)].reset_index(drop=True))

    # ---- standalone per-leader screen (context for diagnostics; NOT the gate) ----------------------- #
    standalone = {}
    for w in wallets:
        roe, dd, _b, n_acts, _ = ODS.our_copy_run([w], win, btc_hourly, whitelist)
        standalone[w] = {"dd": dd, "roe": roe, "n_acts": n_acts}

    # ---- cohort start ------------------------------------------------------------------------------- #
    c_dd, c_roe = ODS.cohort_dd(wallets, win, btc_hourly, whitelist)
    cohort_dd_start, cohort_roe_start = c_dd, c_roe
    if verbose:
        print(f"[our_dd_exclude] start n={len(wallets)} cohort OUR-DD={c_dd:.2f}% ROE={c_roe:.2f}% "
              f"window {win_start.date()}..{asof_ts.date()} target={target_dd:.1f}%", flush=True)

    # ---- GREEDY leave-one-out drop until cohort OUR-DD <= target_dd ---------------------------------- #
    survivors = list(wallets)
    cur_dd, cur_roe = c_dd, c_roe
    drop_info = {}  # wallet -> (step, marginal_reduction, cohort_dd_after, cohort_roe_after)
    step = 0
    while cur_dd > target_dd and len(survivors) > 1:
        step += 1
        best = None  # (new_dd, dropped_wallet, new_roe)
        for cand in survivors:
            sub = [x for x in survivors if x != cand]
            sub_dd, sub_roe = ODS.cohort_dd(sub, win, btc_hourly, whitelist)
            if best is None or sub_dd < best[0]:
                best = (sub_dd, cand, sub_roe)
        new_dd, dropped_w, new_roe = best
        marginal = cur_dd - new_dd
        survivors = [x for x in survivors if x != dropped_w]
        drop_info[dropped_w] = (step, marginal, new_dd, new_roe)
        if verbose:
            print(f"  drop #{step}: {dropped_w} -> cohort OUR-DD={new_dd:.2f}% ROE={new_roe:.2f}% "
                  f"(marginal {marginal:+.2f}pp) survivors={len(survivors)}", flush=True)
        cur_dd, cur_roe = new_dd, new_roe

    exclude_set = set(drop_info.keys())

    # ---- diagnostics frame -------------------------------------------------------------------------- #
    rows = []
    for w in wallets:
        di = drop_info.get(w)
        rows.append({
            "wallet": w,
            "standalone_dd": standalone[w]["dd"],
            "standalone_roe": standalone[w]["roe"],
            "n_acts": standalone[w]["n_acts"],
            "dropped": w in exclude_set,
            "drop_step": di[0] if di else pd.NA,
            "marginal_dd_reduction": di[1] if di else pd.NA,
            "cohort_dd_after": di[2] if di else pd.NA,
            "cohort_roe_after": di[3] if di else pd.NA,
        })
    diag = pd.DataFrame(rows).sort_values(
        ["dropped", "drop_step", "standalone_dd"], ascending=[False, True, False]
    ).reset_index(drop=True)
    diag.attrs.update({
        "cohort_dd_start": cohort_dd_start, "cohort_roe_start": cohort_roe_start,
        "cohort_dd_final": cur_dd, "cohort_roe_final": cur_roe,
        "target_dd": target_dd, "asof": str(asof_ts.date()),
        "window": f"{win_start.date()}..{asof_ts.date()}",
    })
    return exclude_set, diag


def _smoke():
    import json
    cfg = json.load(open(WALLETS_JSON))
    wallets = list(cfg["wallets"].keys())[:6]
    print(f"=== our_dd_gate SMOKE: {len(wallets)} wallets, asof=2026-05-23, window=60d, target=25% ===",
          flush=True)
    excl, diag = our_dd_exclude(wallets, "2026-05-23", window_days=60, target_dd=25.0, verbose=True)
    print(f"\nexcluded {len(excl)}: {sorted(excl)}", flush=True)
    print(f"cohort start DD={diag.attrs['cohort_dd_start']:.2f}% ROE={diag.attrs['cohort_roe_start']:.2f}% "
          f"-> final DD={diag.attrs['cohort_dd_final']:.2f}% ROE={diag.attrs['cohort_roe_final']:.2f}%",
          flush=True)
    print("\ndiagnostics:", flush=True)
    print(diag.to_string(index=False), flush=True)


if __name__ == "__main__":
    if "--smoke" in sys.argv:
        _smoke()
    else:
        print("import our_dd_exclude(candidate_wallets, asof, window_days=60, target_dd=25.0, budget=150.0)"
              " from this module, or run with --smoke.")
