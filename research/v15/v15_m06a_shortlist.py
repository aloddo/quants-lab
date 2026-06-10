#!/usr/bin/env python3
"""V15 M6a — Cheap Broad Source-Score Shortlist.

DESIGN: brain projects/quant/v15/modules/m06a (r6, codex DESIGN-SHIP r5 + persistence
Option-B coverage-adjusted per Alberto + codex consult /tmp/codex_m6a_persist_out.txt).

M6a is the CHEAP, NO-ENGINE candidate generator. It ranks the M5-eligible (entity, fold)
survivors by the codex#7 source_score, FOLD-PRETEST-PURE on [train_start_k, test_start_k) for the
RETURN terms, with a COVERAGE-ADJUSTED causal-activity persistence term, and emits a high-recall
shortlist via pure per-fold top-N (budget = sum_k min(N, n_rankable_k) <= 8N). It is the bouncer,
not the judge: M6b (post-M7-engine) is the real, copyability-adjusted ranking. Raw source ROE never
locks the pool (strategy thesis 4b).

source_score_pretest_k = roe_pretest_flow_adj
                       * persistence_term_k            # = min(1, active_blocks / min(6, possible_blocks))
                       * log1p(n_journeys_pretest)
                       * clamp(1 - max_dd_pretest, 0.20, 1.0)

KEY DISCIPLINE:
- RETURN terms (ROE, DD, journeys) are M5 pretest-only (no look-ahead; W1/W2).
- persistence is ACTIVITY/EXISTENCE only and may look back over the whole causal history
  [max(START, test_start_k - 168d), test_start_k) (W1/W2 carve-out C3, Alberto-approved). No future
  leak: every block ends before test_start_k.
- N is PRE-REGISTERED in a run manifest BEFORE any rank/score is inspected (multiple-testing
  discipline). N=1000 was chosen by Alberto as an engine-budget round number, not from scores.

PERSISTENCE = ACTION-BASED (codex-SHIPped r6 design). A block is ACTIVE iff it contains >=1 causal
m02_actions ts. This deliberately REWARDS active, turn-it-over traders and down-ranks one-burst
wallets AND multi-week sitters (Alberto 2026-05-31: "I want to realize returns fast, not be stressed
about volatility" — we do NOT reward holding a position open for weeks). The recency gate (active in
the most recent 14d block) likewise drops long-sitters, which is the intent. (An earlier
"holder-aware" variant that counted open-position-spanning blocks was REVERTED per that correction.)

Output:
  <outdir>/m06a_shortlist.parquet      per (entity, fold) FULL audit (incl drops)
  <outdir>/m06a_pool_summary.parquet   per entity
  <outdir>/m06a_waterfall.json         per-fold counts + budget + diagnostic marginal band
"""
from __future__ import annotations

import argparse
import json
import logging
import math
from dataclasses import dataclass, asdict
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [v15_m06a] %(message)s")
logger = logging.getLogger("v15_m06a")

# Alberto 2026-06-09: when M5 runs copyability-only (M5_COPYABILITY_ONLY=1), eligible rows may have roe<=0
# (M5 no longer floors performance). M6a then RANKS performance instead of asserting it. Default OFF =
# strict behavior byte-identical.
import os as _os
COPYABILITY_ONLY = _os.environ.get("M5_COPYABILITY_ONLY", "0") == "1"

# === Constants (LOCKED by design r6) ===
BLOCK_DAYS = 14
HORIZON_DAYS = 168            # ~6mo intended persistence lookback (archive is 174d -> natural cap)
PERS_SAT_BLOCKS = 6           # codex#7 "6-block saturation": min(6, possible_blocks) denominator
DD_LO = 0.20                  # clamp floor; aligns with M5 max_dd<=0.80 (no-op rail)
MS_DAY = 86_400_000
BLOCK_MS = BLOCK_DAYS * MS_DAY
HORIZON_MS = HORIZON_DAYS * MS_DAY
HORIZON_BLOCKS = HORIZON_DAYS // BLOCK_DAYS   # 12 (denominator for persistence_coverage)

DEFAULT_MANIFEST = {
    "manifest_version": "v1",
    "mode": "shortlist",                 # "shortlist" | "rank_only"
    "shortlist_n_per_fold": 1000,        # Alberto, pre-registered (engine-budget round number)
    "recency_gate": True,                # drop entities with no action/open-position in the most recent 14d block
    "contamination_status": "clean_oos", # clean_oos | exploratory_calibration | alberto_override
    "persistence_horizon_days": HORIZON_DAYS,
}


# ---------------------------------------------------------------------------
# Persistence: coverage-adjusted causal activity over fold-anchored 14d blocks
# ---------------------------------------------------------------------------
@dataclass
class PersistenceResult:
    persistence_term: float          # min(1, active_blocks / min(6, possible_blocks))
    active_blocks: int
    possible_blocks: int
    recent_block_active: bool         # block 0 = [test_start-14d, test_start) had action OR open position
    lookback_days: float
    persistence_coverage: float       # possible_blocks / HORIZON_BLOCKS (degree of left-censoring)
    left_censored: bool               # lookback_days < HORIZON_DAYS


def _block_index(ts_ms: int, test_start_ms: int) -> int:
    """Fold-anchored block index walking back from test_start_k.
    block b = [test_start - 14d*(b+1), test_start - 14d*b). A ts strictly < test_start lands in
    b = floor((test_start - 1 - ts) / BLOCK_MS) so the instant test_start-1 is b=0 and test_start
    itself (== a boundary) is excluded (half-open)."""
    return (test_start_ms - 1 - ts_ms) // BLOCK_MS


def compute_persistence(ts_ms: np.ndarray, test_start_ms: int, start_ms: int) -> PersistenceResult:
    """ts_ms: the PRIMARY wallet's m02_actions timestamps (ms). Counts distinct 14d blocks in
    [max(START, test_start-168d), test_start) that contain >=1 ACTION (action-based; Alberto:
    reward active turn-it-over traders, not multi-week sitters). Fully causal: every block ends
    <= test_start (no future data)."""
    horizon_start = max(start_ms, test_start_ms - HORIZON_MS)
    if test_start_ms <= horizon_start:
        return PersistenceResult(0.0, 0, 0, False, 0.0, 0.0, True)
    # codex code-r1 #2: count fold-anchored blocks INTERSECTING [horizon_start, test_start), incl a
    # partial oldest block. possible = block index of horizon_start + 1 (NOT floor(span/14d), which
    # drops the partial oldest block and can zero out short histories).
    possible = _block_index(horizon_start, test_start_ms) + 1
    if possible <= 0:
        return PersistenceResult(0.0, 0, 0, False, 0.0, 0.0, True)

    active: set[int] = set()
    for i in range(len(ts_ms)):
        ts = int(ts_ms[i])
        if horizon_start <= ts < test_start_ms:
            b = _block_index(ts, test_start_ms)
            if 0 <= b < possible:
                active.add(b)

    n_active = len(active)
    denom = min(PERS_SAT_BLOCKS, possible)
    term = min(1.0, n_active / denom) if denom > 0 else 0.0
    return PersistenceResult(
        persistence_term=term,
        active_blocks=n_active,
        possible_blocks=possible,
        recent_block_active=(0 in active),
        lookback_days=(test_start_ms - horizon_start) / MS_DAY,
        persistence_coverage=possible / HORIZON_BLOCKS,
        left_censored=(test_start_ms - horizon_start) < HORIZON_MS,
    )


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _null_wallet(v) -> bool:
    """NA-safe null check for a wallet scalar (handles None, pd.NA, np.nan, '')."""
    if v is None:
        return True
    if bool(pd.isna(v)):   # pd.isna returns a plain bool for scalars (pd.NA, nan -> True)
        return True
    return v == ""


def _load_entities_by_fold(m04_dir: Path, folds: pd.DataFrame) -> dict[int, pd.DataFrame]:
    out = {}
    for fid in sorted(folds["fold_id"].astype(int).unique()):
        p = m04_dir / f"m04_entities_f{fid}.parquet"
        if not p.exists():
            raise FileNotFoundError(f"missing fold-pure M4 entities file for fold {fid}: {p}")
        out[int(fid)] = pd.read_parquet(p)
    return out


def _entity_maps(entities: pd.DataFrame) -> tuple[dict[int, object], dict[int, object], dict[int, object]]:
    cop = dict(zip(entities["entity_id"].astype(int), entities["copyable"]))
    prim = dict(zip(entities["entity_id"].astype(int), entities["primary_wallet"]))
    alloc = dict(zip(entities["entity_id"].astype(int), entities["entity_alloc_weight"]))
    return cop, prim, alloc


# ---------------------------------------------------------------------------
# Core run
# ---------------------------------------------------------------------------
def run(elig: pd.DataFrame, pool: pd.DataFrame, folds: pd.DataFrame,
        entities: pd.DataFrame | None, actions: pd.DataFrame, manifest: dict,
        entities_by_fold: dict[int, pd.DataFrame] | None = None) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    mode = manifest["mode"]
    if mode not in ("shortlist", "rank_only"):
        raise ValueError(f"manifest.mode must be 'shortlist' or 'rank_only', got {mode!r}")
    recency_gate = bool(manifest["recency_gate"])
    mver = manifest["manifest_version"]
    contam = manifest.get("contamination_status", "clean_oos")
    # codex code-r1 #7: disabling the recency gate is a production-design change -> only in a
    # non-clean (explicitly diagnostic) manifest.
    if not recency_gate and contam == "clean_oos":
        raise ValueError("recency_gate=False requires contamination_status != 'clean_oos' "
                         "(disabling the stale-drop is a non-production diagnostic, not clean OOS).")
    N = None
    if mode == "shortlist":
        # codex code-r1 #6: N must be a real positive int (reject bool/float/negative).
        Nraw = manifest["shortlist_n_per_fold"]
        if isinstance(Nraw, bool) or not isinstance(Nraw, int) or Nraw <= 0:
            raise ValueError(f"shortlist_n_per_fold must be a positive int, got {Nraw!r}")
        N = int(Nraw)

    # --- I9 rankable contract: every M5-eligible row must be a copyable primary with a canonical
    #     primary_wallet matching m04 (codex code-r1 #5). ---
    if entities_by_fold is None:
        if entities is None:
            raise ValueError("run() requires either entities or entities_by_fold")
        cop, prim, alloc = _entity_maps(entities)
        maps_by_fold = None
        logger.warning("LOUD WARNING: using a single M4 entities file for all folds; "
                       "M6a M4 copyability/canonical-wallet checks are not fold-pure. Pass --m04-dir.")
    else:
        maps_by_fold = {int(fid): _entity_maps(df) for fid, df in entities_by_fold.items()}
        cop = prim = alloc = None
        logger.info("fold-pure M4 entities enabled for M6a: folds=%s", sorted(maps_by_fold))

    def _maps_for_fold(fid: int):
        if maps_by_fold is None:
            return cop, prim, alloc
        return maps_by_fold.get(int(fid), ({}, {}, {}))

    bad_cop, bad_wallet = [], []
    for _, r in elig[elig["eligible"] == True].iterrows():  # noqa: E712
        eid = int(r["entity_id"])
        fcop, fprim, _ = _maps_for_fold(int(r["fold_id"]))
        cv = fcop.get(eid)
        copyable_true = bool(cv) if pd.notna(cv) else False   # NA-safe (pd.NA copyable -> not copyable)
        canon = fprim.get(eid)
        if not copyable_true or _null_wallet(canon):
            bad_cop.append(eid)
            continue
        ew = r.get("primary_wallet")
        if _null_wallet(ew) or ew != canon:   # short-circuit: != only on two non-null strings
            bad_wallet.append((eid, ew, canon))
    if bad_cop:
        raise AssertionError(
            f"RANKABLE CONTRACT VIOLATION (I9): {len(bad_cop)} eligible entity_ids are not copyable "
            f"primaries (e.g. {bad_cop[:5]}). M5 must only mark copyable primaries eligible.")
    if bad_wallet:
        raise AssertionError(
            f"PRIMARY-WALLET MISMATCH (I9): {len(bad_wallet)} eligible rows have a primary_wallet "
            f"that is null or != m04 canonical (e.g. {bad_wallet[:3]}).")

    # --- archive start (= fold 1 train_start) for the persistence horizon clamp ---
    start_ms = int(pd.Timestamp(folds["train_start"].min()).tz_localize("UTC").timestamp() * 1000) \
        if folds["train_start"].min().tzinfo is None \
        else int(pd.Timestamp(folds["train_start"].min()).timestamp() * 1000)

    # --- per-PRIMARY action ts arrays (sorted), once (action-based persistence) ---
    act = actions[["wallet", "ts"]].sort_values(["wallet", "ts"])
    act_by: dict[str, np.ndarray] = {w: g["ts"].to_numpy(dtype="int64")
                                     for w, g in act.groupby("wallet", sort=False)}

    # --- pool-summary reporting fields (carried, never select) ---
    pool_by = {int(r["entity_id"]): r for _, r in pool.iterrows()} if len(pool) else {}

    fold_test_start = {int(r["fold_id"]): r["test_start"] for _, r in folds.iterrows()}
    fold_chain = {int(r["fold_id"]): int(r["oos_chain_order"]) for _, r in folds.iterrows()}

    def _ts_ms(t) -> int:
        ts = pd.Timestamp(t)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return int(ts.timestamp() * 1000)

    def _num(x):
        return pd.to_numeric(x, errors="coerce")

    rows = []
    for fid in sorted(elig["fold_id"].unique()):
        sub = elig[(elig["fold_id"] == fid)].copy()
        test_start_ms = _ts_ms(fold_test_start[int(fid)])
        for _, r in sub.iterrows():
            eid = int(r["entity_id"])
            elig_row = bool(r["eligible"])
            fcop, fprim, falloc = _maps_for_fold(int(fid))
            w = fprim.get(eid)  # canonical primary wallet (validated above for eligible rows)
            ts_arr = act_by.get(w, np.empty(0, "int64"))
            pr = compute_persistence(ts_arr, test_start_ms, start_ms)

            # codex code-r1 #3/#4: coerce metrics (ineligible/drop rows may be NA) and check
            # FINITENESS before any int()/log (np.inf must not crash before the contract assert).
            roe_n = _num(r["roe_pretest_flow_adj"])
            roe = float(roe_n) if (pd.notna(roe_n) and np.isfinite(roe_n)) else np.nan
            njr_n = _num(r["n_journeys_pretest"])
            njr_finite = bool(pd.notna(njr_n) and np.isfinite(njr_n))
            njr = int(njr_n) if njr_finite else 0
            dd_n = _num(r["max_dd_pretest"])
            dd = float(dd_n) if (pd.notna(dd_n) and np.isfinite(dd_n)) else np.nan
            dd_term = _clamp(1.0 - dd, DD_LO, 1.0) if np.isfinite(dd) else np.nan
            # eligible rows MUST have finite, positive required inputs (M5 floors net_pnl>0, roe>0,
            # n_journeys>=3, max_dd<=0.80). A violation is an upstream contract breach -> hard-fail.
            # COPYABILITY_ONLY (Alberto 2026-06-09): M5 no longer floors performance, so eligible rows may
            # have roe<=0. M6a then RANKS performance (it does not require it): the score is monotone in roe,
            # so losers rank below winners; all <=1000/fold are shortlisted and the engine sims them; M6b does
            # the real post-engine ranking. We still require FINITE inputs (NaN would be a true data breach).
            if elig_row:
                if not np.isfinite(roe):
                    raise AssertionError(f"ELIGIBLE row entity {eid} fold {fid} has non-finite roe={roe_n}.")
                if not (np.isfinite(dd) and njr_finite and njr_n >= 0):
                    raise AssertionError(f"ELIGIBLE row entity {eid} fold {fid} has invalid dd/n_journeys (dd={dd_n}, nj={njr_n}).")
                if not COPYABILITY_ONLY and roe <= 0:
                    raise AssertionError(f"ELIGIBLE row entity {eid} fold {fid} has roe={roe_n} (strict M5 guarantees roe>0).")
                # monotone-in-roe score; sign-preserving so roe<=0 ranks below positives without crashing log1p.
                score = roe * pr.persistence_term * math.log1p(njr) * dd_term
            else:
                score = float("nan")

            rankable = elig_row and (not recency_gate or pr.recent_block_active) and bool(np.isfinite(score))
            pinfo = pool_by.get(eid, {})
            rows.append({
                "entity_id": eid, "primary_wallet": w, "fold_id": int(fid),
                "oos_chain_order": fold_chain[int(fid)],
                "eligible": elig_row, "copyable": bool(fcop.get(eid, False)),
                "has_primary": (w is not None) and not (isinstance(w, float) and pd.isna(w)) and w != "",
                "recent_block_active": pr.recent_block_active,
                "rankable": rankable,
                "source_score_pretest": score,
                "roe_pretest_flow_adj": roe,
                "persistence_term": pr.persistence_term,
                "active_blocks": pr.active_blocks, "possible_blocks": pr.possible_blocks,
                "persistence_lookback_days": pr.lookback_days,
                "left_censored_persistence": pr.left_censored,
                "persistence_coverage": pr.persistence_coverage,
                "n_journeys_pretest": njr, "max_dd_pretest": dd, "dd_clamp_term": dd_term,
                "m4_tier": r.get("m4_tier"),
                "entity_alloc_weight": float(falloc.get(eid, np.nan)),
                # reporting-only (carried; never selects):
                "source_6m_roe_full": float(pinfo.get("source_6m_roe_full", np.nan)) if len(pinfo) else np.nan,
                "active_test_folds": pinfo.get("active_test_folds", np.nan) if len(pinfo) else np.nan,
                "g5_pool_candidate_pass": bool(pinfo.get("g5_pool_candidate_pass", False)) if len(pinfo) else False,
                "as_of_ms": test_start_ms,
                "run_mode": mode, "manifest_version": mver,
            })

    sl = pd.DataFrame(rows)

    # --- the CUT (deterministic, per fold) ---
    # rank_in_fold is emitted in BOTH modes; selection/budget columns ONLY in shortlist mode
    # (codex code-r1 #1: rank_only must not carry B_k / in_shortlist / shortlist_reason).
    sl["rank_in_fold"] = pd.Series([pd.NA] * len(sl), dtype="Int64")
    if mode == "shortlist":
        sl["B_k"] = pd.Series([pd.NA] * len(sl), dtype="Int64")
        sl["in_shortlist"] = pd.Series([pd.NA] * len(sl), dtype="boolean")
        sl["shortlist_reason"] = pd.Series([pd.NA] * len(sl), dtype="object")
        # M5 emits ineligible (entity,fold) rows too; NOT M6a's ranking universe. Label explicitly
        # (not a silent drop), exclude from rankability/budget/conservation.
        inelig_mask = sl["eligible"] != True  # noqa: E712
        sl.loc[inelig_mask, "in_shortlist"] = False
        sl.loc[inelig_mask, "shortlist_reason"] = "ineligible"

    waterfall = {"run_mode": mode, "manifest_version": mver,
                 "recency_gate": recency_gate, "folds": {}}
    if mode == "shortlist":
        waterfall["shortlist_n_per_fold"] = N
    total_sims = 0
    for fid in sorted(sl["fold_id"].unique()):
        m = (sl["fold_id"] == fid) & (sl["rankable"] == True)  # noqa: E712
        idx = sl[m].sort_values(["source_score_pretest", "entity_id"],
                                ascending=[False, True]).index
        sl.loc[idx, "rank_in_fold"] = np.arange(1, len(idx) + 1)
        n_rankable = len(idx)
        n_eligible = int(((sl["fold_id"] == fid) & (sl["eligible"] == True)).sum())  # noqa: E712
        n_stale = int(((sl["fold_id"] == fid) & (sl["eligible"] == True) & (sl["rankable"] == False)).sum())  # noqa: E712
        wf = {"n_eligible": n_eligible, "n_rankable": n_rankable, "n_stale_dropped": n_stale}
        sq = {q: float(np.nanpercentile(sl.loc[idx, "source_score_pretest"], q)) for q in (50, 75, 90, 99)} if n_rankable else {}
        if mode == "shortlist":
            B_k = min(N, n_rankable)
            sl.loc[idx, "B_k"] = B_k
            in_sl_idx, drop_idx = idx[:B_k], idx[B_k:]
            sl.loc[in_sl_idx, "in_shortlist"] = True
            sl.loc[in_sl_idx, "shortlist_reason"] = "top_n"
            sl.loc[drop_idx, "in_shortlist"] = False
            sl.loc[drop_idx, "shortlist_reason"] = "dropped_rank"
            stale_idx = sl[(sl["fold_id"] == fid) & (sl["eligible"] == True) & (sl["rankable"] == False)].index  # noqa: E712
            sl.loc[stale_idx, "in_shortlist"] = False
            sl.loc[stale_idx, "shortlist_reason"] = "dropped_stale"
            total_sims += B_k
            band = sl.loc[idx[B_k:B_k + 200], ["entity_id", "source_score_pretest", "rank_in_fold"]]
            wf.update({"B_k": int(B_k), "n_shortlisted": int(B_k),
                       "p90_diag": (sq.get(90) if n_rankable else None),
                       "marginal_band": band.to_dict("records"), "score_quantiles": sq})
        else:
            wf["score_quantiles"] = sq
        waterfall["folds"][int(fid)] = wf

    if mode == "shortlist":
        waterfall["total_engine_sims"] = int(total_sims)
        waterfall["engine_budget"] = 8 * N
        waterfall["budget_ok"] = bool(total_sims <= 8 * N)
        assert total_sims <= 8 * N, f"BUDGET BREACH: {total_sims} > {8*N}"

    # --- pool summary ---
    g = sl.groupby("entity_id")
    pool_rows = []
    for eid, grp in g:
        n_elig_folds = int((grp["eligible"] == True).sum())  # noqa: E712
        if mode == "shortlist":
            n_sl = int((grp["in_shortlist"] == True).sum())  # noqa: E712
            ever = bool(n_sl > 0)
        else:
            n_sl = None; ever = None
        first = grp.iloc[0]
        pool_rows.append({
            "entity_id": int(eid), "primary_wallet": first["primary_wallet"],
            "n_folds_eligible": n_elig_folds,
            "n_folds_in_shortlist": n_sl, "ever_in_shortlist": ever,
            "source_6m_roe_full": first["source_6m_roe_full"],
            "active_test_folds": first["active_test_folds"],
            "g5_pool_candidate_pass": bool(first["g5_pool_candidate_pass"]),
            "entity_tier": first["m4_tier"], "entity_alloc_weight": first["entity_alloc_weight"],
            "max_persistence_coverage": float(grp["persistence_coverage"].max()),
            "any_left_censored": bool(grp["left_censored_persistence"].any()),
        })
    pool_df = pd.DataFrame(pool_rows)
    return sl, pool_df, waterfall


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--eligibility", required=True, help="m05_eligibility.parquet")
    ap.add_argument("--pool-summary", required=True, help="m05_pool_summary.parquet")
    ap.add_argument("--folds", required=True, help="m03_folds.parquet")
    ap.add_argument("--entities", default=None, help="single m04_entities.parquet (compatibility mode)")
    ap.add_argument("--m04-dir", default=None,
                    help="directory with m04_entities_f{fold_id}.parquet for fold-pure M4")
    ap.add_argument("--actions", required=True, help="m02_actions.parquet")
    ap.add_argument("--manifest", default=None, help="JSON run manifest (N pre-registered). Omit -> default v1 N=1000.")
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    manifest = dict(DEFAULT_MANIFEST)
    if args.manifest:
        manifest.update(json.loads(Path(args.manifest).read_text()))
    logger.info(f"manifest: {manifest}")

    elig = pd.read_parquet(args.eligibility)
    pool = pd.read_parquet(args.pool_summary)
    folds = pd.read_parquet(args.folds)
    if args.m04_dir:
        entities_by_fold = _load_entities_by_fold(Path(args.m04_dir), folds)
        entities = None
    else:
        if not args.entities:
            raise ValueError("provide --m04-dir for fold-pure M4 or --entities for compatibility mode")
        entities = pd.read_parquet(args.entities)
        entities_by_fold = None
    actions = pd.read_parquet(args.actions, columns=["wallet", "ts"])
    n_entities = sum(len(v) for v in entities_by_fold.values()) if entities_by_fold is not None else len(entities)
    logger.info(f"elig={len(elig):,} pool={len(pool):,} folds={len(folds)} entities_rows={n_entities:,} actions={len(actions):,}")

    sl, pool_df, waterfall = run(elig, pool, folds, entities, actions, manifest,
                                 entities_by_fold=entities_by_fold)

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    sl.to_parquet(outdir / "m06a_shortlist.parquet", index=False)
    pool_df.to_parquet(outdir / "m06a_pool_summary.parquet", index=False)
    (outdir / "m06a_waterfall.json").write_text(json.dumps(waterfall, indent=2, default=str))
    if manifest["mode"] == "shortlist":
        logger.info(f"shortlist: {int((sl['in_shortlist']==True).sum()):,} (entity,fold) seats; "  # noqa: E712
                    f"engine_sims={waterfall['total_engine_sims']}/{waterfall['engine_budget']} "
                    f"budget_ok={waterfall['budget_ok']}; distinct entities shortlisted="
                    f"{pool_df['ever_in_shortlist'].sum() if 'ever_in_shortlist' in pool_df else 'NA'}")
    else:
        logger.info("RANK-ONLY: ranks + N-independent diagnostics emitted (no in_shortlist).")


if __name__ == "__main__":
    main()
