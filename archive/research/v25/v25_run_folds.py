#!/usr/bin/env python3
"""v25 orchestrator: 5 folds x 2 rules x 2 frozen cost scenarios (spec-frozen).

USAGE
    # smoke (ALLOWED): <=200 wallets x 1 fold, asserts flat memory
    /Users/hermes/miniforge3/envs/quants-lab/bin/python research/v25/v25_run_folds.py --smoke

    # full run: FORBIDDEN until codex gate (b) passes; requires the explicit flag AND a
    # valid FREEZE.json (validated against current code + doc + inputs at startup)
    ... v25_run_folds.py --confirm-codex-gate-b

Outputs under app/data/research/v25/:
    gates_fold{K}.parquet, entries_fold{K}.parquet, journeys_fold{K}.parquet,
    equity_fold{K}.parquet, r2trips_fold{K}.parquet, entities_fold{K}.parquet,
    edges_fold{K}.parquet, roster_{rule}_fold{K}.parquet,
    trips_{rule}_{scenario}_fold{K}[_seed{S}].parquet,
    daily_{rule}_{scenario}_fold{K}[_seed{S}].parquet,
    exclusions.json, counters.json, verdict.json, manifest.json

Fold windows (frozen, gate-b blocker #2): train_k = [2025-12-01T00:00Z, test_start_k)
half-open; test_k = [test_start_k, test_end_k) half-open; asof_k = test_start_k.

Pass criteria (frozen): joint max-statistic bootstrap is THE method; the Bonferroni
97.5% fallback is used IFF the joint bootstrap raises ANY exception or returns a
non-finite LCB, and the trigger event is written to manifest.json BEFORE the fallback
runs. Winner = higher adjusted LCB among passers (deterministic tie: rule name asc).

HOLDOUT ISOLATION: refuses to run if any file with 'holdout' in its name exists in the
input directory (the virgin holdout must never be readable by the fold harness).
"""
from __future__ import annotations

import argparse
import json
import resource
import sys
import time
import traceback
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

sys.path.insert(0, str(Path(__file__).resolve().parent))

from v25_common import (ACTIONS_PARQUET, DROPOUT_SEEDS, MS_DAY, OUT_DIR,
                        PORT_BLOCK_DAYS, PORT_BLOCK_DAYS_ROBUST, SCENARIOS,
                        ShardedParquetWriter, build_journeys, folds,
                        git_commit, install_memory_guard, iter_wallet_frames,
                        sha256_file, MarksIndex)
from v25_gates import (cluster_entities, exclusion_summary, extract_entries,
                       wallet_gate_row)
from v25_r1_causal import score_r1
from v25_r2_lcb import score_r2
from v25_bootstrap import bonferroni_lcb, hurdle_daily_usd, joint_lcb
from v25_portfolio_sim import simulate_portfolio, simulate_wallet_trips

RULES = ["R1", "R2"]
BANNER = (
    "=" * 88 + "\n"
    "  v25: FULL FOLD RUNS ARE FORBIDDEN until codex gate (b) passes\n"
    "  (harness code diff vs /tmp/v25_prereg_v3.md, joint design+code sign-off).\n"
    "  Only --smoke (<=200 wallets x 1 fold) is allowed before the gate.\n"
    "  To run full folds AFTER the gate: pass --confirm-codex-gate-b\n"
    + "=" * 88)


def holdout_isolation_check(input_path: Path):
    hits = [p.name for p in input_path.parent.iterdir() if "holdout" in p.name.lower()]
    if hits:
        raise SystemExit(f"HOLDOUT ISOLATION: refusing to run -- holdout file(s) present "
                         f"in input path {input_path.parent}: {hits}")


def merge_manifest(out_dir: Path, updates: dict):
    """Merge-write manifest.json (used to persist the Bonferroni trigger event BEFORE
    the fallback runs, and by the final manifest write)."""
    p = out_dir / "manifest.json"
    m = {}
    if p.exists():
        with open(p) as fh:
            m = json.load(fh)
    m.update(updates)
    with open(p, "w") as fh:
        json.dump(m, fh, indent=2, default=float)
    return m


def pass_a(fold_list, out_dir: Path, marks: MarksIndex, max_wallets=None,
           actions_path: Path = ACTIONS_PARQUET) -> dict:
    """Single streaming pass over the wallet-sorted actions parquet. Per wallet x fold:
    gate row, entry events, train journeys (via opening/closing journey ids), daily
    equity samples, and (if eligible) the R2 per-wallet cold-start copy sim on train.
    Train slice is HALF-OPEN ts < asof (frozen). All outputs stream to sharded parquet."""
    sc_base = SCENARIOS["BASE"]()
    writers = {}
    for f in fold_list:
        k = f["fold"]
        for name in ("gates", "entries", "journeys", "equity", "r2trips"):
            writers[(name, k)] = ShardedParquetWriter(out_dir / f"{name}_fold{k}.parquet",
                                                      flush_rows=500_000)
    n_wallets = 0
    t0 = time.time()
    for wallet, wdf in iter_wallet_frames(actions_path, max_wallets=max_wallets):
        n_wallets += 1
        wdf = wdf.sort_values("ts")
        ts = wdf["ts"].to_numpy()
        for f in fold_list:
            k, asof = f["fold"], f["asof_ms"]
            tr = wdf.iloc[: int(np.searchsorted(ts, asof, side="left"))]  # ts < asof
            if tr.empty:
                continue
            j = build_journeys(tr)
            row = wallet_gate_row(wallet, tr, asof, marks, journeys=j)
            row["fold"] = k
            writers[("gates", k)].add(row)
            for (coin, side, ets, w) in extract_entries(wallet, tr):
                writers[("entries", k)].add({"coin": coin, "side": side, "ts": ets,
                                             "wallet": w})
            if len(j):
                closed = j[j["exit_ts"].notna() & (j["exit_ts"] <= asof)]
                for r in closed.to_dict("records"):
                    r["wallet"] = wallet
                    writers[("journeys", k)].add(r)
            eq = tr[(tr["ts"] >= asof - 35 * MS_DAY) & (tr["source_equity_post"] > 0)]
            if len(eq):
                day = (eq["ts"] // MS_DAY) * MS_DAY
                last = eq.groupby(day)["source_equity_post"].last()
                for dms, v in last.items():
                    writers[("equity", k)].add({"wallet": wallet, "day_ms": int(dms),
                                                "equity": float(v)})
            if row["eligible"]:
                res = simulate_wallet_trips(tr, sc_base, marks, f["train_start_ms"], asof)
                if len(res["trips"]):
                    for r in res["trips"].to_dict("records"):
                        r["wallet"] = wallet
                        writers[("r2trips", k)].add(r)
        if n_wallets % 500 == 0:
            print(f"  pass A: {n_wallets} wallets, {time.time()-t0:.0f}s, "
                  f"rss={resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/2**30:.2f}GB")
    counts = {}
    for key, w in writers.items():
        counts[f"{key[0]}_fold{key[1]}"] = w.close()
    counts["n_wallets_streamed"] = n_wallets
    print(f"  pass A done: {n_wallets} wallets in {time.time()-t0:.0f}s")
    return counts


def _read_filtered(path: Path, wallets: set[str] | None = None,
                   columns: list[str] | None = None) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    d = ds.dataset(path)
    filt = ds.field("wallet").isin(list(wallets)) if wallets is not None else None
    t = d.to_table(filter=filt, columns=columns)
    return t.to_pandas()


def select_rosters(fold_list, out_dir: Path, rules=None) -> dict:
    """Gates summary -> clustering -> R1 + R2 rosters per fold. BASE scenario costs are
    used for SELECTION (documented resolution; WORST is an evaluation scenario only)."""
    rules = rules or RULES
    sc_base = SCENARIOS["BASE"]()
    excl_all = {}
    rosters = {}
    for f in fold_list:
        k, asof = f["fold"], f["asof_ms"]
        gates = pd.read_parquet(out_dir / f"gates_fold{k}.parquet") \
            if (out_dir / f"gates_fold{k}.parquet").exists() else pd.DataFrame()
        if gates.empty:
            excl_all[f"fold{k}"] = {"n_wallets": 0, "n_eligible": 0}
            for rule in rules:
                rosters[(rule, k)] = pd.DataFrame()
            continue
        excl_all[f"fold{k}"] = exclusion_summary(gates)
        eligible = set(gates[gates["eligible"]]["wallet"])
        entries = _read_filtered(out_dir / f"entries_fold{k}.parquet", eligible)
        entity_map, edges = cluster_entities(entries, eligible)
        ent_df = pd.DataFrame({"wallet": list(entity_map.keys()),
                               "entity": [entity_map[w] for w in entity_map]})
        ent_df.to_parquet(out_dir / f"entities_fold{k}.parquet", index=False)
        (edges if len(edges) else pd.DataFrame(columns=["wallet_a", "wallet_b", "matched",
                                                        "min_entries", "overlap_frac", "edge"])
         ).to_parquet(out_dir / f"edges_fold{k}.parquet", index=False)
        excl_all[f"fold{k}"]["n_entities"] = int(ent_df["entity"].nunique()) if len(ent_df) else 0

        journeys = _read_filtered(out_dir / f"journeys_fold{k}.parquet", eligible)
        equity = pd.read_parquet(out_dir / f"equity_fold{k}.parquet") \
            if (out_dir / f"equity_fold{k}.parquet").exists() else \
            pd.DataFrame(columns=["wallet", "day_ms", "equity"])
        if "R1" in rules:
            r1_roster, _s, r1_diag = score_r1(journeys, gates, entity_map, equity,
                                              sc_base, asof) \
                if len(journeys) else (pd.DataFrame(), None, {})
            excl_all[f"fold{k}"]["r1_diag"] = r1_diag
            rosters[("R1", k)] = r1_roster
        if "R2" in rules:
            r2trips = _read_filtered(out_dir / f"r2trips_fold{k}.parquet", eligible)
            r2_roster, _s, r2_diag = score_r2(r2trips, entity_map,
                                              f["train_start_ms"], asof) \
                if len(r2trips) else (pd.DataFrame(), None, {})
            excl_all[f"fold{k}"]["r2_diag"] = r2_diag
            rosters[("R2", k)] = r2_roster
        for rule in rules:
            roster = rosters[(rule, k)]
            (roster if len(roster) else pd.DataFrame(
                columns=["wallet", "entity", "rank", "score"])
             ).to_parquet(out_dir / f"roster_{rule}_fold{k}.parquet", index=False)
            print(f"  fold {k} {rule}: {len(roster)} entities selected")
    with open(out_dir / "exclusions.json", "w") as fh:
        json.dump(excl_all, fh, indent=2, default=str)
    return rosters


def collect_roster_actions(wallets: set[str], end_ms: int,
                           actions_path: Path = ACTIONS_PARQUET) -> dict:
    """Second bounded pass: pull the action history (ts < end_ms, half-open) for the
    roster wallet union only (small set). Early-stops past the lexicographic max."""
    if not wallets:
        return {}
    hi = max(wallets)
    out = {}
    for wallet, wdf in iter_wallet_frames(actions_path):
        if wallet in wallets:
            out[wallet] = wdf[wdf["ts"] < end_ms]
        if wallet > hi:
            break
    return out


def run_portfolios(fold_list, rosters: dict, actions_by_wallet: dict, out_dir: Path,
                   marks: MarksIndex) -> dict:
    results = {}
    empty_cols = ["wallet", "coin", "ts", "action_type", "signed_size", "price",
                  "position_after", "journey_id", "opening_journey_id",
                  "closing_journey_id", "is_liquidation"]
    for f in fold_list:
        k = f["fold"]
        for rule in RULES:
            roster = rosters.get((rule, k), pd.DataFrame())
            ws = list(roster["wallet"]) if len(roster) else []
            merged = pd.concat([actions_by_wallet[w] for w in ws if w in actions_by_wallet],
                               ignore_index=True) if ws else pd.DataFrame(columns=empty_cols)
            merged = merged[merged["ts"] < f["test_end_ms"]]        # half-open
            emap = dict(zip(roster["wallet"], roster["entity"])) if len(roster) else {}
            for sc_name, sc_fn in SCENARIOS.items():
                sc = sc_fn()
                res = simulate_portfolio(merged, sc, marks, f["asof_ms"], f["test_end_ms"])
                _attach_entity(res, emap)
                _write_port(res, out_dir, rule, sc_name, k, None)
                results[(rule, sc_name, k, None)] = res
            # participation stress: BASE scenario, 3 frozen event-hash seeds
            for seed in DROPOUT_SEEDS:
                sc = SCENARIOS["BASE"]()
                res = simulate_portfolio(merged, sc, marks, f["asof_ms"], f["test_end_ms"],
                                         dropout_seed=seed)
                _attach_entity(res, emap)
                _write_port(res, out_dir, rule, "BASE", k, seed)
                results[(rule, "BASE", k, seed)] = res
    return results


def _attach_entity(res: dict, emap: dict):
    """Every trip (realized AND terminal-MTM) attributes to exactly one (entity, coin);
    the entity of the wallet whose signal OPENED the lot (frozen attribution)."""
    if len(res["trips"]):
        res["trips"]["entity"] = res["trips"]["wallet"].map(emap).fillna(
            res["trips"]["wallet"])


def _write_port(res: dict, out_dir: Path, rule: str, sc: str, k, seed):
    sfx = f"_seed{seed}" if seed is not None else ""
    res["trips"].to_parquet(out_dir / f"trips_{rule}_{sc}_fold{k}{sfx}.parquet", index=False)
    res["daily"].to_parquet(out_dir / f"daily_{rule}_{sc}_fold{k}{sfx}.parquet", index=False)


def _fold_report(res: dict, worst: dict, stress: list, test_days: int) -> dict:
    """Per rule-fold report row (gate-b blocker #7): realized-trip mean, trips/day,
    long/short split, plus the frozen fold-level pass inputs."""
    trips = res["trips"]
    realized = trips[~trips["terminal"]] if len(trips) else trips
    n_real = int(len(realized))
    return {
        "total_pnl_incl_terminal": res["total_pnl"],
        "final_equity": res.get("final_equity"),
        "max_mtm_dd_frac": res.get("max_mtm_dd_frac"),
        "n_trips_realized": n_real,
        "n_trips_terminal": int(len(trips) - n_real),
        "realized_trip_mean_net_bps": (float(realized["net_bps"].mean())
                                       if n_real else float("nan")),
        "realized_trip_mean_net_usd": (float(realized["net_pnl"].mean())
                                       if n_real else float("nan")),
        "trips_per_day": (n_real / test_days) if test_days > 0 else float("nan"),
        "n_trips_long": int((realized["side"] > 0).sum()) if n_real else 0,
        "n_trips_short": int((realized["side"] < 0).sum()) if n_real else 0,
        "worst_total_pnl": worst["total_pnl"],
        "stress_total_pnl_by_seed": dict(zip(map(str, DROPOUT_SEEDS), stress)),
        "counters": res["counters"],
    }


def _boot_with_fallback(rule_segments: dict, out_dir: Path) -> tuple[dict, dict, str]:
    """THE method = joint max-statistic (7d primary + 14d robustness). FROZEN
    deterministic fallback trigger: ANY exception from the joint bootstrap OR a
    non-finite LCB for any rule at either block size. The trigger event is written to
    manifest.json BEFORE the Bonferroni path runs."""
    trigger = None
    boot7 = boot14 = None
    try:
        boot7 = joint_lcb(rule_segments, block_days=PORT_BLOCK_DAYS)
        boot14 = joint_lcb(rule_segments, block_days=PORT_BLOCK_DAYS_ROBUST)
        bad = [(r, bs) for bs, b in (("7d", boot7), ("14d", boot14))
               for r in b["rules"]
               if not np.isfinite(b["rules"][r]["lcb_maxstat"])]
        if bad:
            trigger = {"type": "nonfinite_lcb", "detail": [f"{r}@{bs}" for r, bs in bad]}
    except Exception as e:
        trigger = {"type": "exception", "detail": f"{type(e).__name__}: {e}",
                   "traceback": traceback.format_exc()[-2000:]}
    if trigger is None:
        return boot7, boot14, "joint_maxstat"
    # write the trigger event BEFORE the fallback runs (frozen ordering)
    trigger["fallback"] = "bonferroni_97.5_one_sided"
    trigger["trigger_unix"] = time.time()
    merge_manifest(out_dir, {"bonferroni_trigger": trigger})
    boot7 = bonferroni_lcb(rule_segments, block_days=PORT_BLOCK_DAYS)
    boot14 = bonferroni_lcb(rule_segments, block_days=PORT_BLOCK_DAYS_ROBUST)
    return boot7, boot14, "bonferroni_fallback"


def evaluate(fold_list, results: dict, out_dir: Path) -> dict:
    """Frozen pass criteria (fold stage) + automated winner selection. Full evaluation
    logic runs even in smoke so the machinery is exercised; smoke verdicts are NOT
    decision evidence."""
    verdict = {"per_rule": {}}
    total_days = sum(f["test_days"] for f in fold_list)
    rule_segments = {}
    for rule in RULES:
        r = {"folds": {}}
        segs = []
        total_trips = 0
        all_trips = []
        for f in fold_list:
            k = f["fold"]
            res = results[(rule, "BASE", k, None)]
            segs.append(res["daily"]["daily_pnl"].to_numpy(dtype="float64"))
            trips = res["trips"]
            realized = trips[~trips["terminal"]] if len(trips) else trips
            total_trips += len(realized)
            if len(trips):
                t = trips.copy()
                t["fold"] = k
                all_trips.append(t)
            worst = results[(rule, "WORST", k, None)]
            stress = [results[(rule, "BASE", k, s)]["total_pnl"] for s in DROPOUT_SEEDS]
            r["folds"][k] = _fold_report(res, worst, stress, f["test_days"])
        rule_segments[rule] = segs
        r["total_realized_trips"] = total_trips
        r["total_test_days"] = total_days
        r["trips_per_day_pooled"] = (total_trips / total_days) if total_days else 0.0
        r["hurdle_daily_usd"] = hurdle_daily_usd(total_trips, total_days)
        at = pd.concat(all_trips, ignore_index=True) if all_trips else pd.DataFrame()
        r["criteria"] = _criteria(r, at, fold_list)
        verdict["per_rule"][rule] = r
    # familywise bootstrap: joint max-stat is THE method; deterministic Bonferroni
    # fallback with trigger persisted to manifest.json BEFORE the fallback runs
    boot7, boot14, method = _boot_with_fallback(rule_segments, out_dir)
    verdict["bootstrap_method"] = method
    verdict["bootstrap_7d"] = boot7
    verdict["bootstrap_14d"] = boot14
    for rule in RULES:
        r = verdict["per_rule"][rule]
        h = r["hurdle_daily_usd"]
        l7 = boot7["rules"][rule]["lcb_maxstat"]
        l14 = boot14["rules"][rule]["lcb_maxstat"]
        r["adjusted_lcb_daily_usd"] = l7
        r["criteria"]["lcb7_gt_hurdle"] = bool(l7 == l7 and l7 > h)
        # frozen consequence: 14d-block conclusion must AGREE with 7d, else FAIL
        r["criteria"]["block_robustness_agree"] = bool(
            (l7 == l7) and (l14 == l14) and ((l7 > h) == (l14 > h)))
        r["PASS"] = bool(all(v for kk, v in r["criteria"].items()))
    # automated winner selection (frozen): higher adjusted LCB among passers;
    # deterministic tie-break = rule name ascending. No passers => KILL.
    passers = [rule for rule in RULES if verdict["per_rule"][rule]["PASS"]]
    if passers:
        winner = sorted(passers,
                        key=lambda x: (-verdict["per_rule"][x]["adjusted_lcb_daily_usd"], x))[0]
        verdict["winner"] = {
            "rule": winner,
            "adjusted_lcb_daily_usd": verdict["per_rule"][winner]["adjusted_lcb_daily_usd"],
            "next_step": "ONE holdout evaluation on/after 2026-07-16 (v25_holdout.py); "
                         "winner fails => STOP (no runner-up holdout)"}
    else:
        verdict["winner"] = None
        verdict["recommendation"] = "KILL"
    with open(out_dir / "verdict.json", "w") as fh:
        json.dump(verdict, fh, indent=2, default=float)
    return verdict


def _criteria(r: dict, all_trips: pd.DataFrame, fold_list) -> dict:
    """Frozen fold-stage criteria. Concentration attribution (frozen): every realized
    trip AND every terminal-MTM row attributes its full net $ to exactly one
    (entity, coin); terminal-MTM dollars are INCLUDED in all $ attribution sums.
    Registered tests ONLY: entity trip-count <= 30% (realized trips), entity |net $|
    <= 40%, coin |net $| <= 40%. The coin trip-count gate is NOT registered and does
    not exist. Any concentration test with an exactly-zero denominator passes trivially."""
    c = {}
    fk = r["folds"]
    n_folds = len(fold_list)
    c["folds_positive_4of5"] = sum(
        1 for k in fk if fk[k]["total_pnl_incl_terminal"] > 0) >= min(4, n_folds)
    c["worst_pooled_positive"] = sum(fk[k]["worst_total_pnl"] for k in fk) > 0
    c["min_trips_150"] = r["total_realized_trips"] >= 150
    c["min_test_days_60"] = r["total_test_days"] >= 60
    c["no_fold_mtm_dd_gt_5pct"] = all(
        (fk[k]["max_mtm_dd_frac"] or 0) <= 0.05 for k in fk)
    pooled_by_seed = [sum(fk[k]["stress_total_pnl_by_seed"][str(s)] for k in fk)
                      for s in DROPOUT_SEEDS]
    c["stress_median_positive"] = (float(np.median(pooled_by_seed)) > 0) if pooled_by_seed else False

    realized = all_trips[~all_trips["terminal"]] if len(all_trips) else all_trips
    has_ent = len(all_trips) > 0 and "entity" in all_trips.columns
    # entity trip-count concentration (realized trips; zero denominator => pass)
    if has_ent and len(realized):
        share_trips = realized.groupby("entity").size() / len(realized)
        c["entity_trip_conc_le_30pct"] = bool((share_trips <= 0.30).all())
    else:
        c["entity_trip_conc_le_30pct"] = True       # zero denominator: trivially pass
    # $ concentration INCLUDING terminal-MTM rows (frozen)
    if has_ent:
        abse = all_trips.groupby("entity")["net_pnl"].sum().abs()
        dene = float(abse.sum())
        c["entity_pnl_conc_le_40pct"] = (True if dene == 0.0
                                         else bool((abse / dene <= 0.40).all()))
        absc = all_trips.groupby("coin")["net_pnl"].sum().abs()
        denc = float(absc.sum())
        c["coin_pnl_conc_le_40pct"] = (True if denc == 0.0
                                       else bool((absc / denc <= 0.40).all()))
    else:
        c["entity_pnl_conc_le_40pct"] = True
        c["coin_pnl_conc_le_40pct"] = True
    c["min_entities_15"] = (int(realized["entity"].nunique()) >= 15
                            if has_ent and len(realized) else False)
    return c


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--smoke", action="store_true",
                    help="<=200 wallets x 1 fold against the real m02 parquet (ALLOWED)")
    ap.add_argument("--max-wallets", type=int, default=200)
    ap.add_argument("--confirm-codex-gate-b", action="store_true")
    args = ap.parse_args()

    if not args.smoke and not args.confirm_codex_gate_b:
        print(BANNER)
        raise SystemExit(2)
    if not args.smoke:
        print(BANNER)
        print("  --confirm-codex-gate-b supplied: proceeding with FULL fold runs.\n")
        # FREEZE validation (gate-b blocker #8): refuse on any code/doc/input mismatch
        from v25_freeze import validate_freeze
        fz = validate_freeze(check_inputs=True)
        print(f"  FREEZE.json validated (doc {fz['prereg_doc_sha256'][:16]}..., "
              f"commit {fz['git_commit'][:12]})\n")

    install_memory_guard(soft_gb=12, label="v25")
    holdout_isolation_check(ACTIONS_PARQUET)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    fold_list = folds()[:1] if args.smoke else folds()
    max_wallets = min(args.max_wallets, 200) if args.smoke else None
    marks = MarksIndex()

    print(f"v25 run: {'SMOKE' if args.smoke else 'FULL'} | folds={len(fold_list)} "
          f"| max_wallets={max_wallets}")
    counts = pass_a(fold_list, OUT_DIR, marks, max_wallets=max_wallets)
    rosters = select_rosters(fold_list, OUT_DIR)
    roster_union = set()
    for r in rosters.values():
        if len(r):
            roster_union |= set(r["wallet"])
    end_max = max(f["test_end_ms"] for f in fold_list)
    actions = collect_roster_actions(roster_union, end_max)
    results = run_portfolios(fold_list, rosters, actions, OUT_DIR, marks)
    verdict = evaluate(fold_list, results, OUT_DIR)

    counters = {f"{rule}_{sc}_fold{k}" + (f"_seed{s}" if s is not None else ""):
                res["counters"] for (rule, sc, k, s), res in results.items()}
    with open(OUT_DIR / "counters.json", "w") as fh:
        json.dump(counters, fh, indent=2)

    merge_manifest(OUT_DIR, {
        "mode": "smoke" if args.smoke else "full",
        "git_commit": git_commit(),
        "input": str(ACTIONS_PARQUET),
        "input_sha256": sha256_file(ACTIONS_PARQUET),
        "marks_cache_dir": str(MarksIndex().cache_dir),
        "seeds": {"r2_bootstrap": 42, "portfolio_bootstrap": 42,
                  "dropout": DROPOUT_SEEDS},
        "folds": fold_list,
        "pass_a_counts": counts,
        "n_roster_wallets": len(roster_union),
        "bootstrap_method": verdict["bootstrap_method"],
        "generated_unix": time.time(),
    })

    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 2**30
    print(f"\nDONE. peak RSS {peak:.2f}GB | outputs in {OUT_DIR}")
    for rule in RULES:
        r = verdict["per_rule"][rule]
        print(f"  {rule}: trips={r['total_realized_trips']} "
              f"hurdle=${r['hurdle_daily_usd']:.3f}/day "
              f"lcb7=${r['adjusted_lcb_daily_usd']:.3f} "
              f"({verdict['bootstrap_method']}) "
              f"{'PASS' if r['PASS'] else 'FAIL'}"
              + ("  [SMOKE: not decision evidence]" if args.smoke else ""))
    w = verdict.get("winner")
    print(f"  winner: {w['rule'] if w else 'NONE (KILL)'}")
    if args.smoke and peak > 10:
        raise SystemExit(f"SMOKE MEMORY ASSERT FAILED: peak RSS {peak:.1f}GB > 10GB")


if __name__ == "__main__":
    main()
