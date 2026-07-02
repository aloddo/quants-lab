#!/usr/bin/env python3
"""v25 VIRGIN HOLDOUT evaluator -- ONE evaluation, write-once (spec-frozen).

Frozen decision procedure step 2: the fold-stage WINNER (verdict.json) gets ONE holdout
evaluation on/after 2026-07-16. Holdout window: [2026-06-11T00:00Z, eval_end) half-open,
eval_end frozen at evaluation time. Universe/roster: selected at asof = 2026-06-11 from
the holdout actions file's train slice (the Jun-11-known universe, valid).

PASS iff (all frozen):
- one-sided 95% LCB of holdout mean daily $ PnL > hurdle_daily_$ (same formula, holdout
  trips/days; stationary block bootstrap, 7d blocks, 10000 resamples, seed 42, single
  rule so NO familywise adjustment)
- >= 100 realized trips
- no 5% portfolio MTM DD (event-level equity series from initial $500)
Winner fails => STOP (no runner-up holdout). Report includes realized-trip mean,
trips/day, long/short split (activity profile, reported ship input).

WRITE-ONCE ENFORCEMENT: refuses to run if holdout_result.json already exists; the
result file is chmod 444 after writing. Refuses to run before 2026-07-16 UTC.

USAGE
    ... v25_holdout.py --actions PATH_TO_HOLDOUT_M02_ACTIONS --eval-end 2026-07-16 \
        [--rule R1|R2]   # default: winner from verdict.json

The --actions file must be the holdout-extended m02 rebuild (frozen m01/m02 code,
checksummed BEFORE fold evaluation); its sha256 is recorded in the result.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from v25_common import (HOLDOUT_EARLIEST_EVAL, HOLDOUT_LCB_LEVEL, HOLDOUT_MIN_TRIPS,
                        HOLDOUT_START, OUT_DIR, PORT_BLOCK_DAYS, PORT_RESAMPLES,
                        PORT_SEED, SCENARIOS, TRAIN_START, git_commit, install_memory_guard,
                        sha256_file, MarksIndex)
from v25_bootstrap import hurdle_daily_usd, single_rule_lcb
from v25_run_folds import (collect_roster_actions, pass_a, select_rosters)
from v25_portfolio_sim import simulate_portfolio

RESULT_PATH = OUT_DIR / "holdout_result.json"


def holdout_fold(eval_end_ms: int) -> dict:
    asof_ms = int(HOLDOUT_START.value // 10**6)
    return {"fold": "H", "train_start_ms": int(TRAIN_START.value // 10**6),
            "asof_ms": asof_ms, "test_end_ms": eval_end_ms,
            "test_days": (eval_end_ms - asof_ms) / 86_400_000}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--actions", type=Path, required=True,
                    help="holdout-extended m02 actions parquet (frozen m01/m02 rebuild)")
    ap.add_argument("--eval-end", required=True,
                    help="frozen eval_end (UTC date, half-open end of the holdout window)")
    ap.add_argument("--rule", choices=["R1", "R2"], default=None,
                    help="default: the fold-stage winner from verdict.json")
    args = ap.parse_args()

    # ---- write-once + timing enforcement (frozen) ---------------------------------------- #
    if RESULT_PATH.exists():
        raise SystemExit(f"HOLDOUT REFUSED: {RESULT_PATH} already exists. The holdout is "
                         f"evaluated ONCE; there is no re-run.")
    now_utc = pd.Timestamp.utcnow().tz_localize(None)
    if now_utc < HOLDOUT_EARLIEST_EVAL:
        raise SystemExit(f"HOLDOUT REFUSED: evaluation allowed on/after "
                         f"{HOLDOUT_EARLIEST_EVAL.date()} UTC (now: {now_utc}).")
    rule = args.rule
    if rule is None:
        vp = OUT_DIR / "verdict.json"
        if not vp.exists():
            raise SystemExit("HOLDOUT REFUSED: no verdict.json (run folds first) and no "
                             "--rule given")
        with open(vp) as fh:
            v = json.load(fh)
        if not v.get("winner"):
            raise SystemExit("HOLDOUT REFUSED: fold-stage winner is NONE (KILL); the "
                             "frozen procedure has no holdout to run.")
        rule = v["winner"]["rule"]
    eval_end = pd.Timestamp(args.eval_end)
    eval_end_ms = int(eval_end.value // 10**6)
    fold = holdout_fold(eval_end_ms)
    if fold["test_days"] <= 0:
        raise SystemExit("HOLDOUT REFUSED: eval_end must be after 2026-06-11")

    install_memory_guard(soft_gb=12, label="v25_holdout")
    work = OUT_DIR / "holdout_work"
    work.mkdir(parents=True, exist_ok=True)
    marks = MarksIndex()

    print(f"v25 HOLDOUT: rule={rule} window=[{HOLDOUT_START.date()}, {eval_end.date()}) "
          f"({fold['test_days']:.0f}d)")
    pass_a([fold], work, marks, actions_path=args.actions)
    rosters = select_rosters([fold], work, rules=[rule])
    roster = rosters[(rule, "H")]
    ws = set(roster["wallet"]) if len(roster) else set()
    actions = collect_roster_actions(ws, eval_end_ms, actions_path=args.actions)
    merged = (pd.concat([actions[w] for w in ws if w in actions], ignore_index=True)
              if ws else pd.DataFrame(columns=["wallet", "coin", "ts", "action_type",
                                               "signed_size", "price", "position_after",
                                               "journey_id", "is_liquidation"]))
    merged = merged[merged["ts"] < eval_end_ms]
    res = simulate_portfolio(merged, SCENARIOS["BASE"](), marks,
                             fold["asof_ms"], eval_end_ms)

    trips = res["trips"]
    realized = trips[~trips["terminal"]] if len(trips) else trips
    n_real = int(len(realized))
    daily = res["daily"]["daily_pnl"].to_numpy(dtype="float64")
    boot = single_rule_lcb([daily], block_days=PORT_BLOCK_DAYS,
                           n_resamples=PORT_RESAMPLES, seed=PORT_SEED,
                           level=HOLDOUT_LCB_LEVEL)
    hurdle = hurdle_daily_usd(n_real, fold["test_days"])
    dd = float(res.get("max_mtm_dd_frac") or 0.0)
    crit = {
        "lcb_gt_hurdle": bool(boot["lcb"] == boot["lcb"] and boot["lcb"] > hurdle),
        "min_trips_100": n_real >= HOLDOUT_MIN_TRIPS,
        "no_mtm_dd_gt_5pct": dd <= 0.05,
    }
    result = {
        "rule": rule,
        "holdout_start": str(HOLDOUT_START.date()),
        "eval_end": str(eval_end.date()),
        "test_days": fold["test_days"],
        "actions_file": str(args.actions),
        "actions_sha256": sha256_file(args.actions),
        "git_commit": git_commit(),
        "n_roster_entities": int(len(roster)),
        "n_trips_realized": n_real,
        "n_trips_terminal": int(len(trips) - n_real),
        "total_pnl_incl_terminal": res["total_pnl"],
        "final_equity": res.get("final_equity"),
        "max_mtm_dd_frac": dd,
        "hurdle_daily_usd": hurdle,
        "bootstrap": boot,
        "bootstrap_params": {"block_days": PORT_BLOCK_DAYS, "resamples": PORT_RESAMPLES,
                             "seed": PORT_SEED, "level": HOLDOUT_LCB_LEVEL,
                             "familywise_adjustment": None},
        "criteria": crit,
        "PASS": bool(all(crit.values())),
        # activity profile (spec 4b; reported ship input, not an edge criterion)
        "realized_trip_mean_net_bps": (float(realized["net_bps"].mean())
                                       if n_real else float("nan")),
        "realized_trip_mean_net_usd": (float(realized["net_pnl"].mean())
                                       if n_real else float("nan")),
        "trips_per_day": n_real / fold["test_days"],
        "n_trips_long": int((realized["side"] > 0).sum()) if n_real else 0,
        "n_trips_short": int((realized["side"] < 0).sum()) if n_real else 0,
        "counters": res["counters"],
        "evaluated_unix": time.time(),
    }
    trips.to_parquet(work / f"holdout_trips_{rule}.parquet", index=False)
    res["daily"].to_parquet(work / f"holdout_daily_{rule}.parquet", index=False)
    with open(RESULT_PATH, "w") as fh:
        json.dump(result, fh, indent=2, default=float)
    os.chmod(RESULT_PATH, 0o444)          # write-once: read-only after the single write
    print(f"\nHOLDOUT {'PASS' if result['PASS'] else 'FAIL (STOP -- no runner-up)'}: "
          f"lcb=${boot['lcb']:.3f}/day vs hurdle=${hurdle:.3f}/day, trips={n_real}, "
          f"dd={dd:.2%}")
    print(f"result written READ-ONLY: {RESULT_PATH}")


if __name__ == "__main__":
    main()
