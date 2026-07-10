#!/usr/bin/env python3
"""our_dd_holdout.py -- FORWARD-HOLDOUT proof: does the OUR-DD gate beat the leader-DD gate out of sample?

THE QUESTION
  build_skill_cohort.py drops near-ruin leaders with mtm_dd_exclude (queries the LEADER's HL account
  drawdown). The codex-verified finding (brain projects/quant/research/2026-06-24-our-drawdown-vs-leader-
  drawdown) says the RIGHT gate is each candidate's MARGINAL contribution to OUR netted-cohort drawdown
  (our_dd_gate.our_dd_exclude). This harness asks, strictly out of sample:
    from the SAME skill-ranked candidate pool, does the OUR-DD-gated cohort have LOWER FORWARD drawdown
    (and acceptable forward ROE) than the leader-DD-gated cohort?

DATA-AVAILABILITY NOTE (why asof is NOT 2026-05-23)
  The requested asof was 2026-05-23 with a forward holdout 2026-05-23..2026-06-23. But the local
  m02_actions.parquet (the leader copy-signal source) ENDS 2026-05-23 23:59 (only 2 stray rows after),
  and the assetctx marks end 2026-06-01. There are ZERO leader actions to replay after 2026-05-23, so a
  forward holdout past that date is IMPOSSIBLE with current local data. To run an HONEST forward holdout
  we pull asof BACK so a real forward window fits inside the data:
      DEFAULT: asof=2026-04-23, gate window [02-23..04-23], holdout (04-23..05-23].
  Gate decisions use ONLY data <= asof; the forward eval uses ONLY data > asof. Strict no-look-ahead.

THE TWO GATES (applied to the IDENTICAL candidate pool, both causal <= asof)
  A. LEADER-DD (causal analog of the live mtm_dd_exclude): the live function queries each leader's CURRENT
     HL account (today's state -- NOT causal at asof). The fair causal proxy is the SAME-WINDOW leader-
     account MTM-DD over [asof-window, asof] (our_drawdown_scorer.leader_account_dd_samewindow), with the
     live KEEP/DROP rule: DROP a leader if its same-window leader-account MTM-DD >= LEADER_DD_DROP (70%).
     This is the most faithful causal stand-in for the deployed gate. (A second variant reports what the
     LIVE function would drop using TODAY's account state, for reference -- but that is non-causal and is
     NOT used for the head-to-head.)
  B. OUR-DD (the candidate gate): our_dd_gate.our_dd_exclude greedy-LOO to cohort OUR-DD <= TARGET_DD,
     over the SAME [asof-window, asof] window, at OUR $150/leader sizing.

FORWARD EVAL (both cohorts, identical engine, window strictly > asof)
  Each surviving cohort is run as ONE netted faithful-copy account over the holdout window through
  balanced_step4_netted_sim (via our_drawdown_scorer.our_copy_run). Report forward OUR-MTM-DD% and
  forward ROE%. Lower forward DD with acceptable ROE = the OUR-DD gate wins.

MEMORY: per-wallet whitelist-filtered action slices are cached ONCE per window (selection + holdout are
  separate windows -> separate caches; the cache is keyed by wallet only, so we CLEAR it between the two
  windows). m02_actions is never fully loaded. Marks page-cached once.

Run:
  ~/miniforge3/envs/quants-lab/bin/python research/v16/our_dd_holdout.py            # full proof
  ~/miniforge3/envs/quants-lab/bin/python research/v16/our_dd_holdout.py --asof=2026-04-23 --window=60
  ~/miniforge3/envs/quants-lab/bin/python research/v16/our_dd_holdout.py --topn=40 --target=25
"""
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))                       # research/v16
sys.path.insert(0, str(ROOT.parent / "v15"))        # research/v15

import our_drawdown_scorer as ODS  # noqa: E402
from our_dd_gate import our_dd_exclude, _live_whitelist  # noqa: E402
from balanced_step3_riskparity import btc_ret_hourly  # noqa: E402
import build_skill_cohort as BSC  # noqa: E402  (skill_scores, z, martingale_flags helpers)

WALLETS_JSON = "config/copy_trader_wallets_v17_expansion.json"

# defaults (overridable on CLI)
ASOF_DEFAULT = "2026-04-23"     # see DATA-AVAILABILITY NOTE; real forward window must fit in local data
WINDOW_DAYS = 60                # gate selection window [asof-WINDOW_DAYS, asof]
HOLDOUT_DAYS = 30               # forward eval window (asof, asof+HOLDOUT_DAYS]
TOPN = 40                       # skill-ranked candidate-pool size to gate (engine-bounded)
TARGET_DD = 25.0                # OUR-DD greedy-LOO target (= live -25% stop)
LEADER_DD_DROP = 70.0           # leader-DD gate: drop if same-window leader-account MTM-DD >= 70%
ACTIVE_DAYS = 14
MIN_J = 40
HOLD_MIN_H, HOLD_MAX_H = 2.0, 48.0


def build_candidate_pool(asof, whitelist, topn):
    """Replicate build_skill_cohort eligibility on data <= asof, restricted to the COPYABLE universe
    (the live 52-coin whitelist -- the coins we can faithfully copy; substitutes for the transient
    agentC calib journey-filter, which is the same copyable intent). Returns the top-`topn` skill-ranked
    wallets (post martingale veto), NO DD gate yet."""
    cols = ["wallet", "coin", "entry_ts", "realized_pnl", "net_realized_pnl",
            "max_position_notional", "liq_closed", "duration_h"]
    j = pd.read_parquet("app/data/v15/m02_journeys.parquet", columns=cols)
    j = j[(j.max_position_notional > 10) & (j.coin.isin(whitelist))].copy()
    j["ret"] = j["net_realized_pnl"] / j["max_position_notional"]
    j = j[j.ret.between(-1.0, 2.0)].copy()
    j["t"] = pd.to_datetime(j["entry_ts"], unit="ms")
    asof_ts = pd.Timestamp(asof)
    j = j[j.t <= asof_ts]                                    # CAUSAL: selection data only

    last = j.t.max()
    active = set(j[j.t >= last - pd.Timedelta(days=ACTIVE_DAYS)].wallet.unique())

    s = BSC.skill_scores(j)
    s = s[(s.n >= MIN_J) & (s.index.isin(active)) &
          (s.hold >= HOLD_MIN_H) & (s.hold <= HOLD_MAX_H)].copy()
    mart = BSC.martingale_flags(j[j.wallet.isin(s.index)])
    s["martingale"] = s.index.map(mart).fillna(False)
    s = s[~s["martingale"]].copy()
    s["skill"] = BSC.z(s.win) + BSC.z(s.sharpe) + BSC.z(-s.maxdd)
    pool = list(s.nlargest(topn, "skill").index)
    return pool, len(s)


def leader_dd_gate(wallets, asof, window_days):
    """CAUSAL leader-DD gate: same-window leader-account MTM-DD over [asof-window, asof]; DROP if >= 70%.
    Returns (exclude_set, info_df)."""
    asof_ts = pd.Timestamp(asof)
    win_start = asof_ts - pd.Timedelta(days=window_days)
    win = (ODS._ms(win_start), ODS._ms(asof_ts))
    ldr = ODS.leader_account_dd_samewindow(wallets, win)
    excl, rows = set(), []
    for w in wallets:
        dd = ldr[w]["dd"]
        drop = (dd is not None and dd >= LEADER_DD_DROP)
        if drop:
            excl.add(w)
        rows.append({"wallet": w, "leader_dd": dd, "period": ldr[w]["period"],
                     "err": ldr[w]["err"], "dropped": drop})
    return excl, pd.DataFrame(rows)


def forward_eval(cohort, asof, holdout_days, whitelist):
    """Run `cohort` as ONE netted faithful-copy $150/leader account over the FORWARD holdout window
    (asof, asof+holdout_days]. Returns (roe%, dd%, n_acts, n). Window strictly > asof (no look-back into
    selection)."""
    if not cohort:
        return float("nan"), float("nan"), 0, 0
    asof_ts = pd.Timestamp(asof)
    fwd_start = asof_ts                                   # exclusive lower bound enforced by ts >= start;
    fwd_end = asof_ts + pd.Timedelta(days=holdout_days)   # selection used ts <= asof, so the 1ms overlap
    lo = ODS._ms(fwd_start) + 1                           # at exactly asof is removed (+1ms) -> strict >.
    hi = ODS._ms(fwd_end)
    win = (lo, hi)
    btc_hourly = btc_ret_hourly(lo, hi)
    roe, dd, _b, n_acts, n = ODS.our_copy_run(list(cohort), win, btc_hourly, whitelist)
    return roe, dd, n_acts, n


def main():
    asof = ASOF_DEFAULT
    window_days, holdout_days, topn, target = WINDOW_DAYS, HOLDOUT_DAYS, TOPN, TARGET_DD
    for a in sys.argv:
        if a.startswith("--asof="):
            asof = a.split("=", 1)[1]
        elif a.startswith("--window="):
            window_days = int(a.split("=", 1)[1])
        elif a.startswith("--holdout="):
            holdout_days = int(a.split("=", 1)[1])
        elif a.startswith("--topn="):
            topn = int(a.split("=", 1)[1])
        elif a.startswith("--target="):
            target = float(a.split("=", 1)[1])

    whitelist, base, exp = _live_whitelist()
    asof_ts = pd.Timestamp(asof)
    sel_start = asof_ts - pd.Timedelta(days=window_days)
    fwd_end = asof_ts + pd.Timedelta(days=holdout_days)

    print("=" * 96, flush=True)
    print("OUR-DD vs LEADER-DD gate -- FORWARD HOLDOUT PROOF", flush=True)
    print(f"  asof={asof}  | gate window [{sel_start.date()}..{asof_ts.date()}] (<= asof, causal)  "
          f"| holdout ({asof_ts.date()}..{fwd_end.date()}] (> asof)", flush=True)
    print(f"  copyable whitelist: {len(whitelist)} coins ({len(base)} calibrated + {len(exp)} expansion)",
          flush=True)
    print(f"  candidate pool top-N={topn} (skill-ranked, post martingale veto) | OUR-DD target={target:.0f}%"
          f" | leader-DD drop>={LEADER_DD_DROP:.0f}%", flush=True)
    print("=" * 96, flush=True)

    # ---- 1. shared skill-ranked candidate pool (causal <= asof) ------------------------------------- #
    pool, n_eligible = build_candidate_pool(asof, whitelist, topn)
    print(f"\n[pool] {n_eligible} eligible (skill+active+veto, copyable, <= asof) -> top-{len(pool)} candidates",
          flush=True)

    # ---- 2a. LEADER-DD gate (causal same-window) ---------------------------------------------------- #
    print(f"\n[gate A: LEADER-DD] querying same-window leader-account MTM-DD over [{sel_start.date()}.."
          f"{asof_ts.date()}] ...", flush=True)
    excl_ldr, ldr_df = leader_dd_gate(pool, asof, window_days)
    cohort_ldr = [w for w in pool if w not in excl_ldr]
    n_ldr_navail = int(ldr_df["err"].notna().sum())
    print(f"  leader-DD dropped {len(excl_ldr)} (>= {LEADER_DD_DROP:.0f}%); "
          f"{n_ldr_navail} had no same-window data (KEPT by default); cohort_A n={len(cohort_ldr)}",
          flush=True)

    # ---- 2b. OUR-DD gate (greedy-LOO, causal) ------------------------------------------------------- #
    print(f"\n[gate B: OUR-DD] greedy-LOO to cohort OUR-DD <= {target:.0f}% over [{sel_start.date()}.."
          f"{asof_ts.date()}] ...", flush=True)
    excl_our, our_diag = our_dd_exclude(pool, asof, window_days=window_days, target_dd=target, verbose=True)
    cohort_our = [w for w in pool if w not in excl_our]
    print(f"  OUR-DD dropped {len(excl_our)} (greedy-LOO); cohort_B n={len(cohort_our)}", flush=True)
    print(f"  selection-window cohort: A(leader-DD) and B(OUR-DD) | OUR-DD@selection "
          f"start={our_diag.attrs['cohort_dd_start']:.2f}% -> final={our_diag.attrs['cohort_dd_final']:.2f}%",
          flush=True)

    # ---- 3. FORWARD HOLDOUT eval (clear the selection-window cache; holdout is a different window) --- #
    ODS._ACTS_CACHE.clear()
    print(f"\n[forward eval] replaying both cohorts over holdout ({asof_ts.date()}..{fwd_end.date()}] "
          f"(strictly > asof) ...", flush=True)
    roe_a, dd_a, na_a, n_a = forward_eval(cohort_ldr, asof, holdout_days, whitelist)
    roe_b, dd_b, na_b, n_b = forward_eval(cohort_our, asof, holdout_days, whitelist)

    # ---- HEAD-TO-HEAD TABLE ------------------------------------------------------------------------- #
    print("\n" + "=" * 96, flush=True)
    print("HEAD-TO-HEAD (forward holdout, out of sample)", flush=True)
    print("=" * 96, flush=True)
    hdr = f"  {'gate':<22}{'n_cohort':>9}{'n_excl':>8}{'fwd_DD%':>9}{'fwd_ROE%':>10}{'fwd_acts':>10}"
    print(hdr, flush=True)
    print("  " + "-" * (len(hdr) - 2), flush=True)
    print(f"  {'A leader-account-DD':<22}{n_a:>9}{len(excl_ldr):>8}{dd_a:>9.2f}{roe_a:>10.2f}{na_a:>10}",
          flush=True)
    print(f"  {'B OUR-marginal-DD':<22}{n_b:>9}{len(excl_our):>8}{dd_b:>9.2f}{roe_b:>10.2f}{na_b:>10}",
          flush=True)
    print("  " + "-" * (len(hdr) - 2), flush=True)

    # which wallets differ
    only_a = set(cohort_ldr) - set(cohort_our)   # kept by A, dropped by B
    only_b = set(cohort_our) - set(cohort_ldr)   # kept by B, dropped by A
    print(f"\n  cohorts differ by {len(only_a) + len(only_b)} wallets:", flush=True)
    print(f"    kept by A(leader-DD) but DROPPED by B(OUR-DD)  [{len(only_a)}]: {sorted(only_a)}", flush=True)
    print(f"    kept by B(OUR-DD) but DROPPED by A(leader-DD)  [{len(only_b)}]: {sorted(only_b)}", flush=True)

    # ---- VERDICT ------------------------------------------------------------------------------------ #
    print("\n" + "=" * 96, flush=True)
    dd_better = (not np.isnan(dd_b)) and (not np.isnan(dd_a)) and dd_b < dd_a
    roe_ok = (not np.isnan(roe_b)) and (roe_b > 0 or roe_b >= roe_a)
    if len(excl_our) == 0 and len(excl_ldr) == 0:
        verdict = ("NEUTRAL: neither gate dropped anyone on this pool/window -- cohorts identical, "
                   "forward DD/ROE identical. No discriminating evidence at this asof.")
    elif dd_better and roe_ok:
        verdict = (f"OUR-DD WINS: forward DD {dd_b:.2f}% < {dd_a:.2f}% (-{dd_a-dd_b:.2f}pp) with "
                   f"forward ROE {roe_b:.2f}% (vs {roe_a:.2f}%). OUR-DD gating lowers OOS drawdown.")
    elif dd_better and not roe_ok:
        verdict = (f"MIXED: OUR-DD lowers forward DD ({dd_b:.2f}% < {dd_a:.2f}%) but at a forward-ROE cost "
                   f"({roe_b:.2f}% < {roe_a:.2f}%). Trade-off call for Alberto.")
    else:
        verdict = (f"NEGATIVE: OUR-DD gating does NOT beat leader-DD out of sample "
                   f"(forward DD {dd_b:.2f}% vs {dd_a:.2f}%, ROE {roe_b:.2f}% vs {roe_a:.2f}%). "
                   f"Leader-DD gate is at least as good here. A negative result is a valid answer.")
    print("VERDICT:", verdict, flush=True)
    print("=" * 96, flush=True)
    print("\nNUMBERS ONLY -- codex review of code + holdout pending; Alberto decides adoption.", flush=True)


if __name__ == "__main__":
    main()
