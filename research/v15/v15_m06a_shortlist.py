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
import hashlib
import json
import logging
import math
import shutil
import sys
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
# M5 upstream-contract rails asserted for eligible rows (codex m06a P2). Mirror m05 MIN_JOURNEYS_PRETEST /
# MAXDD_CAP; these are non-perf blocking gates M5 enforces in BOTH strict and COPYABILITY_ONLY mode.
MIN_JOURNEYS_M5 = 3
MAXDD_M5 = 0.80
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
    "score_basis": "equity_roe",          # equity_roe | activity_only
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
    score_basis = manifest.get("score_basis", "equity_roe")
    if score_basis not in ("equity_roe", "activity_only"):
        raise ValueError(f"unknown score_basis {score_basis!r}")
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

    # AUDIT 2026-07-10 (codex m06a P1): `eligible` MUST be a strict bool. A NaN/non-bool slips PAST the I9
    # precheck (`== True` is False for NaN) yet `bool(np.nan)` is True in the scoring loop -> the row can become
    # rankable + shortlisted with NO copyability check. Fail CLOSED here (validate the column once, up front).
    if "eligible" not in elig.columns:
        raise AssertionError("M5 eligibility frame lacks the 'eligible' column")
    _bad_elig = elig["eligible"].isna() | ~elig["eligible"].isin([True, False])
    if bool(_bad_elig.any()):
        raise AssertionError(
            f"M5 eligibility has {int(_bad_elig.sum())} missing/non-bool 'eligible' values "
            f"(e.g. entity_ids {elig.loc[_bad_elig, 'entity_id'].head(5).tolist()}); fail closed.")

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

    # --- per-PRIMARY replayable action ts arrays, once ---
    if "stream_replay_valid" in actions.columns:
        actions = actions[actions["stream_replay_valid"].fillna(False).astype(bool)].copy()
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
            elig_row = (r["eligible"] == True)  # noqa: E712 -- validated strict-bool above; no bool(NaN) trap
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
                # AUDIT 2026-07-10 (codex m06a P2): finiteness is not enough -- assert the M5 RANGE contract so an
                # upstream floor breach fails CLOSED instead of being silently clamped. n_journeys>=3 is a non-perf
                # blocking gate M5 enforces in EVERY lane (strict, COPYABILITY_ONLY, AND the equity-independent
                # activity_only lane), so it is asserted here for ALL eligible rows. The max_dd<=0.80 rail is
                # asserted ONLY in the equity_roe branch below: activity_only is the equity-INDEPENDENT lane where
                # M5 ran equity_required=False, so max_dd_pretest is legitimately NaN/absent and DD is deferred to
                # M7/M6b (asserting it here would false-positive on that lane by design).
                if not (njr_finite and njr_n >= MIN_JOURNEYS_M5):
                    raise AssertionError(
                        f"ELIGIBLE row entity {eid} fold {fid} has n_journeys={njr_n} (< M5 floor {MIN_JOURNEYS_M5})."
                    )
                if score_basis == "activity_only":
                    # Equity-independent high-recall bouncer. Performance is
                    # deliberately deferred to fixed-position M7 and M6b.
                    dd_term = 1.0
                    score = pr.persistence_term * math.log1p(njr)
                else:
                    if not np.isfinite(roe):
                        raise AssertionError(f"ELIGIBLE row entity {eid} fold {fid} has non-finite roe={roe_n}.")
                    if not np.isfinite(dd):
                        raise AssertionError(f"ELIGIBLE row entity {eid} fold {fid} has invalid dd={dd_n}.")
                    # DD rail: equity_roe lane only (see the note above the n_journeys assert). A breach here would
                    # otherwise hide behind the 0.20 dd_term clamp -> fail closed instead.
                    if dd > MAXDD_M5:
                        raise AssertionError(
                            f"ELIGIBLE row entity {eid} fold {fid} has max_dd={dd_n} (> M5 cap {MAXDD_M5}); "
                            f"upstream floor breach, fail closed (not silently clamped).")
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
                "score_basis": score_basis,
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
                 "score_basis": score_basis,
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
        # Engine budget = n_folds * N (per-fold top-N is pre-registered at N; total is bounded by the
        # number of folds, each emitting <= N). The literal 8 here hardcoded the original 8-fold calendar;
        # the recency base chains 12 folds, so bound by the ACTUAL fold count (per-fold N unchanged =
        # multiple-testing discipline intact). Byte-identical for an 8-fold run.
        n_folds_budget = len(waterfall["folds"])
        engine_budget = n_folds_budget * N
        waterfall["total_engine_sims"] = int(total_sims)
        waterfall["engine_budget"] = int(engine_budget)
        waterfall["n_folds"] = int(n_folds_budget)
        waterfall["budget_ok"] = bool(total_sims <= engine_budget)
        assert total_sims <= engine_budget, f"BUDGET BREACH: {total_sims} > {engine_budget} ({n_folds_budget} folds x N={N})"

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


# CONTENT-HASH CACHE (Fable + codex gated 2026-07-17). SOUNDNESS CONTRACT: m06a runs as a SEQUENTIAL stage of
# recal_pipeline (one m06a at a time; its inputs m01->m05 are produced by earlier stages and NOT rewritten
# while m06a runs). Under that contract the cache cannot serve stale (the key content-hashes every input +
# resolved manifest + env flag + code + runtime). The residual codex flagged (a concurrent m06a rewriting
# shared inputs/outdir mid-run, or ABA) is OUT OF CONTRACT -- do NOT run two m06a into the same --outdir
# concurrently. The private generation dir + atomic marker-last cache rename still make the CACHE robust to a
# crash; only the shared --outdir publication assumes the single-writer contract.
_ARTIFACTS = ("m06a_shortlist.parquet", "m06a_pool_summary.parquet", "m06a_waterfall.json")
_CACHE_DIR = Path(__file__).resolve().parents[1] / "app" / "data" / "v15" / "m06a_cache"
# CODE version hashed ONCE AT IMPORT (codex P1): reflects the ACTUALLY-LOADED source, so editing the file
# mid-run can't store old-logic output under a new source hash.
_CODE_SHA = hashlib.sha256(Path(__file__).resolve().read_bytes()).hexdigest()
# RUNTIME fingerprint (codex P2): a pandas/numpy/pyarrow/python upgrade can change parquet decoding / numeric /
# ordering / serialization while inputs+code are unchanged -> must invalidate the cache.
try:
    import pyarrow as _pa
    _pa_ver = _pa.__version__
except Exception:  # noqa: BLE001
    _pa_ver = "na"
_RUNTIME_SHA = hashlib.sha256(
    f"py{sys.version_info[:3]}|pd{pd.__version__}|np{np.__version__}|pa{_pa_ver}".encode()).hexdigest()


def _file_sha(p: Path, h) -> None:
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)


def _m06a_input_files(args) -> list:
    """The concrete input files whose CONTENT determines m06a's output. For fold-pure mode, hash the concrete
    m04_entities_f*.parquet list actually present (a superset is safe -> at worst a harmless miss), not the dir."""
    files = [Path(args.eligibility), Path(args.pool_summary), Path(args.folds), Path(args.actions)]
    if args.m04_dir:
        files += sorted(Path(args.m04_dir).glob("m04_entities_f*.parquet"))
    else:
        files.append(Path(args.entities))
    return files


def _m06a_cache_key(args, manifest: dict) -> str:
    """CONTENT-hash cache key (Fable plan-gate 2026-07-17): byte-hash of every input file + the RESOLVED
    manifest + the M5_COPYABILITY_ONLY env flag (an import-time impurity that changes scoring) + the
    entities-mode + a CODE-VERSION hash of THIS source file (covers the LOCKED constants + DEFAULT_MANIFEST, so
    editing m06a can never serve a pre-edit output). Byte-hash, NOT size+mtime: recal_pipeline rewrites upstream
    every run so mtimes always change even when content doesn't -> mtime gives ZERO hits; a byte false-miss is
    safe (recompute), a false-hit is structurally impossible."""
    h = hashlib.sha256()
    h.update(_CODE_SHA.encode()); h.update(b"|code|")          # import-time (codex P1)
    h.update(_RUNTIME_SHA.encode()); h.update(b"|runtime|")    # pandas/numpy/pyarrow/python (codex P2)
    h.update(json.dumps(manifest, sort_keys=True, default=str).encode()); h.update(b"|manifest|")
    h.update(b"copyonly=1|" if COPYABILITY_ONLY else b"copyonly=0|")
    h.update((b"m04dir|" if args.m04_dir else b"entities|"))
    for p in _m06a_input_files(args):
        h.update(p.name.encode()); h.update(b":")
        _file_sha(p, h); h.update(b"|")
    return h.hexdigest()


def _cache_hit_dir(key: str) -> Path | None:
    d = _CACHE_DIR / key
    if (d / ".complete").exists() and all((d / a).exists() for a in _ARTIFACTS):
        return d
    return None


def _cache_store(key: str, srcdir: Path) -> None:
    """Populate the cache atomically FROM THIS RUN'S PRIVATE generation dir (codex P1: never re-read the shared
    outdir, which a concurrent run could overwrite). Write a tmp dir, copy the 3 artifacts, write the .complete
    marker LAST, then atomic-rename into place. A crash mid-copy leaves only the tmp dir -> next run MISSES (no
    marker), never a partial set (Fable P(d)). Race-safe (codex P2): if another run wins the rename (dst now
    exists + complete), discard our tmp and treat as success."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    dst = _CACHE_DIR / key
    if _cache_hit_dir(key) is not None:
        return
    tmp = _CACHE_DIR / f".tmp_{key}_{_os.getpid()}"
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    for a in _ARTIFACTS:
        shutil.copy2(srcdir / a, tmp / a)
    (tmp / ".complete").write_text(key)
    try:
        _os.replace(tmp, dst)   # atomic; fails if another run already created dst (non-empty)
    except OSError:
        shutil.rmtree(tmp, ignore_errors=True)
        if _cache_hit_dir(key) is None:   # not a lost-race (dst isn't a complete winner) -> real error, re-raise
            raise


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
    ap.add_argument("--no-cache", action="store_true", help="Force recompute; ignore + do not write the content-hash cache.")
    args = ap.parse_args()

    manifest = dict(DEFAULT_MANIFEST)
    if args.manifest:
        manifest.update(json.loads(Path(args.manifest).read_text()))
    logger.info(f"manifest: {manifest}")

    if not args.m04_dir and not args.entities:
        raise ValueError("provide --m04-dir for fold-pure M4 or --entities for compatibility mode")

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)
    # CONTENT-HASH CACHE (Fable-gated 2026-07-17): m06a is a pure function of its inputs+manifest+env+code, so a
    # rerun with unchanged content reuses the cached output instead of recomputing the whole shared stage.
    cache_key = None
    if not args.no_cache:
        cache_key = _m06a_cache_key(args, manifest)
        hit = _cache_hit_dir(cache_key)
        if hit is not None:
            for a in _ARTIFACTS:
                shutil.copy2(hit / a, outdir / a)
            logger.info(f"m06a CACHE HIT {cache_key[:12]} -> reused {', '.join(_ARTIFACTS)} (skipped recompute)")
            return
        logger.info(f"m06a CACHE MISS {cache_key[:12]} -> computing")

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
    # MEMORY-EFFICIENT actions load (Alberto 2026-07-23 binding requirement: M6a/M6b/M7 run many times,
    # must stream, never load the big stuff in RAM). Persistence only needs, per wallet, the set of
    # DISTINCT 14d-BLOCK memberships, and blocks are anchored to each fold's test_start which is
    # MIDNIGHT-aligned (verified: all fold test_start/pretest_start/train_start are 00:00:00 UTC, and
    # BLOCK_MS = 14*MS_DAY is an integer number of days). Therefore every action timestamp within a
    # day-aligned day maps to the SAME fold-anchored block, so reducing to DISTINCT (wallet, day) and
    # using the day-start ms as the representative is BYTE-IDENTICAL to compute_persistence on raw ts,
    # while collapsing ~150M rows -> ~2.6M (57x) with a memory-BOUNDED DuckDB pass (1GB cap, streams +
    # spills). act_by feeds compute_persistence ONLY (line ~294), so no other consumer sees the reduction.
    import duckdb as _ddb
    _con = _ddb.connect()
    _con.execute("PRAGMA memory_limit='1GB'; PRAGMA threads=4")
    actions = _con.execute(
        f"SELECT wallet, CAST(day AS BIGINT) * {MS_DAY} AS ts FROM ("
        f"  SELECT DISTINCT wallet, ts // {MS_DAY} AS day FROM read_parquet(?) WHERE stream_replay_valid"
        f")",
        [str(args.actions)],
    ).df()
    _con.close()
    n_entities = sum(len(v) for v in entities_by_fold.values()) if entities_by_fold is not None else len(entities)
    logger.info(f"elig={len(elig):,} pool={len(pool):,} folds={len(folds)} entities_rows={n_entities:,} "
                f"action_wallet_days={len(actions):,} (mem-efficient DuckDB day-reduction)")

    sl, pool_df, waterfall = run(elig, pool, folds, entities, actions, manifest,
                                 entities_by_fold=entities_by_fold)

    # Write this run's outputs to a PRIVATE generation dir first, then publish to outdir AND cache from it
    # (codex P1/P2): the cache reflects THIS run's immutable output, never a shared dir a concurrent run could
    # overwrite between our write and our cache-read.
    import tempfile
    gen = Path(tempfile.mkdtemp(prefix=".m06a_gen_", dir=str(outdir)))
    try:
        sl.to_parquet(gen / "m06a_shortlist.parquet", index=False)
        pool_df.to_parquet(gen / "m06a_pool_summary.parquet", index=False)
        (gen / "m06a_waterfall.json").write_text(json.dumps(waterfall, indent=2, default=str))
        for a in _ARTIFACTS:
            shutil.copy2(gen / a, outdir / a)
        if cache_key is not None:
            # TOCTOU guard (codex P1): only cache if the inputs are STILL what the key hashed -- a concurrent
            # upstream rewrite between hashing and reading would otherwise store B's output under A's key.
            if _m06a_cache_key(args, manifest) == cache_key:
                _cache_store(cache_key, gen)
                logger.info(f"m06a cached {cache_key[:12]}")
            else:
                logger.warning("m06a: inputs changed during compute -> NOT caching this run")
    finally:
        shutil.rmtree(gen, ignore_errors=True)
    if manifest["mode"] == "shortlist":
        logger.info(f"shortlist: {int((sl['in_shortlist']==True).sum()):,} (entity,fold) seats; "  # noqa: E712
                    f"engine_sims={waterfall['total_engine_sims']}/{waterfall['engine_budget']} "
                    f"budget_ok={waterfall['budget_ok']}; distinct entities shortlisted="
                    f"{pool_df['ever_in_shortlist'].sum() if 'ever_in_shortlist' in pool_df else 'NA'}")
    else:
        logger.info("RANK-ONLY: ranks + N-independent diagnostics emitted (no in_shortlist).")


if __name__ == "__main__":
    main()
