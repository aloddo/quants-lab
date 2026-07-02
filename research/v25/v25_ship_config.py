#!/usr/bin/env python3
"""v25 deterministic ship-config generator (spec-frozen; gate-b blocker #7).

Frozen decision procedure step 4: Ship = deterministic script (rule, asof) -> config
JSON with provenance (commit, hashes, universe manifest); biweekly re-selection reruns
this script at a new asof.

Determinism: given the same (rule, asof, actions file, marks cache, harness commit),
the output config body is byte-identical (no wallclock timestamps inside the config
body; provenance carries content hashes, not times). The roster is recomputed from
scratch via the frozen selection pipeline (pass A gates -> clustering -> rule scoring)
unless --from-run-dir points at an existing harness output containing the roster
parquet for this (rule, asof) (fast path; provenance records which path was used).

USAGE
    ... v25_ship_config.py --rule R2 --asof 2026-06-11 --out config/copy_trader_wallets_v25.DRAFT.json
        [--actions PATH] [--from-run-dir DIR]
"""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))

from v25_common import (ACTIONS_PARQUET, EXIT_TRIGGER_FRAC, INITIAL_EQUITY,
                        MAX_COIN_SIDE_X, MAX_GROSS_X, MAX_MARGIN_UTIL, ORDER_USD,
                        PREREG_DOC, RESERVE_LEV, TRAIN_START, git_commit,
                        install_memory_guard, sha256_file, MarksIndex)
from v25_freeze import FREEZE_PATH


def build_roster(rule: str, asof: pd.Timestamp, actions_path: Path,
                 from_run_dir: Path | None) -> tuple[pd.DataFrame, str]:
    """Roster for (rule, asof). Fast path: reuse a harness roster parquet whose fold
    asof matches. Otherwise recompute via the frozen selection pipeline."""
    if from_run_dir is not None:
        man_p = from_run_dir / "manifest.json"
        if man_p.exists():
            with open(man_p) as fh:
                man = json.load(fh)
            asof_ms = int(asof.value // 10**6)
            for f in man.get("folds", []):
                if int(f["asof_ms"]) == asof_ms:
                    rp = from_run_dir / f"roster_{rule}_fold{f['fold']}.parquet"
                    if rp.exists():
                        return pd.read_parquet(rp), f"reused:{rp}"
        raise SystemExit(f"--from-run-dir {from_run_dir}: no roster for {rule} @ "
                         f"{asof.date()} (asof must equal a fold test_start)")
    # recompute: frozen selection pipeline at this asof
    from v25_run_folds import pass_a, select_rosters
    fold = {"fold": "S", "train_start_ms": int(TRAIN_START.value // 10**6),
            "asof_ms": int(asof.value // 10**6),
            "test_end_ms": int(asof.value // 10**6), "test_days": 0}
    with tempfile.TemporaryDirectory(prefix="v25_ship_") as td:
        work = Path(td)
        marks = MarksIndex()
        pass_a([fold], work, marks, actions_path=actions_path)
        rosters = select_rosters([fold], work, rules=[rule])
        return rosters[(rule, "S")], "recomputed"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rule", choices=["R1", "R2"], required=True)
    ap.add_argument("--asof", required=True, help="selection asof (UTC date)")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--actions", type=Path, default=ACTIONS_PARQUET)
    ap.add_argument("--from-run-dir", type=Path, default=None,
                    help="reuse roster parquet from an existing harness output dir")
    args = ap.parse_args()
    install_memory_guard(soft_gb=12, label="v25_ship_config")
    asof = pd.Timestamp(args.asof)

    roster, roster_source = build_roster(args.rule, asof, args.actions, args.from_run_dir)
    if not len(roster):
        raise SystemExit(f"EMPTY ROSTER for {args.rule} @ {asof.date()}: refusing to "
                         f"emit a ship config")
    freeze = None
    if FREEZE_PATH.exists():
        with open(FREEZE_PATH) as fh:
            freeze = json.load(fh)

    cfg = {
        "global": {
            "strategy": f"v25_{args.rule}_selection",
            "sizing_mode": "fixed",
            "order_size_usd": ORDER_USD,
            "initial_equity_usd": INITIAL_EQUITY,
            "max_margin_util": MAX_MARGIN_UTIL,
            "margin_reserve_max_lev": int(RESERVE_LEV),
            "netx_cap_x": MAX_GROSS_X,
            "coin_side_cap_x": MAX_COIN_SIDE_X,
            "exit_min_trim_pct": EXIT_TRIGGER_FRAC,
            "full_exit_trim_pct": EXIT_TRIGGER_FRAC,
            "global_stop_pct": 0.05,
            "cohort_asof": f"{asof.date()}T00:00:00+00:00",
            "selection_rule": args.rule,
            "biweekly_reselection": True,
            "v17_authorization": "DRAFT -- v25 pre-registered selection; requires "
                                 "holdout PASS + codex gate (c) + Alberto GO before live",
        },
        "wallets": [
            {"address": r["wallet"], "entity": r["entity"], "rank": int(r["rank"]),
             "score": float(r["score"])}
            for _, r in roster.iterrows()
        ],
        "provenance": {
            "harness": "research/v25",
            "git_commit": git_commit(),
            "prereg_doc": str(PREREG_DOC),
            "prereg_doc_sha256": (sha256_file(PREREG_DOC) if PREREG_DOC.exists()
                                  else "MISSING"),
            "freeze_git_commit": freeze.get("git_commit") if freeze else None,
            "freeze_harness_sha256": (freeze.get("harness_combined_sha256")
                                      if freeze else None),
            "actions_file": str(args.actions),
            "actions_sha256": sha256_file(args.actions),
            "rule": args.rule,
            "asof": str(asof.date()),
            "roster_source": roster_source,
            "n_entities": int(roster["entity"].nunique()),
        },
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w") as fh:
        json.dump(cfg, fh, indent=2, sort_keys=True)
    print(f"ship config written: {args.out} ({len(roster)} wallets, "
          f"roster {roster_source})")


if __name__ == "__main__":
    main()
