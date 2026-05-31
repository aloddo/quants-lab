#!/usr/bin/env python3
"""Tests for V15 M6a (v15_m06a_shortlist). Invariants I1-I11 of modules/m06a (r6).

Persistence is ACTION-BASED (Alberto 2026-05-31: reward fast turnover, not multi-week holders).
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v15"))
import v15_m06a_shortlist as m6a  # noqa: E402

MS_DAY = 86_400_000


def ms(s: str) -> int:
    d = datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    return int(d.timestamp() * 1000)


# fold 1 test_start 2026-01-26 (START 2025-12-01 -> 56d -> 4 possible blocks)
TS1 = ms("2026-01-26")
START = ms("2025-12-01")


# ---------------------------------------------------------------- persistence
def test_persistence_action_coverage_and_recency():
    # actions in block0 (01-20), block1 (01-05), block2 (12-20), block3 (12-05) -> 4 active / 4 possible
    ts = np.array([ms("2026-01-20"), ms("2026-01-05"), ms("2025-12-20"), ms("2025-12-05")], dtype="int64")
    pr = m6a.compute_persistence(ts, TS1, START)
    assert pr.possible_blocks == 4
    assert pr.active_blocks == 4
    assert pr.persistence_term == 1.0          # min(1, 4/min(6,4)) = 1.0 (early fold can reach 1.0)
    assert pr.recent_block_active is True
    assert pr.persistence_coverage == 4 / 12   # left-censored vs 168d horizon
    assert pr.left_censored is True


def test_persistence_partial_and_stale():
    # only old blocks 2 & 3, nothing recent
    ts = np.array([ms("2025-12-05"), ms("2025-12-20")], dtype="int64")
    pr = m6a.compute_persistence(ts, TS1, START)
    assert pr.active_blocks == 2
    assert pr.persistence_term == 2 / 4
    assert pr.recent_block_active is False     # no action in block 0 -> stale


def test_persistence_no_future_leak_and_bounds():
    # action AFTER test_start (future) and an action exactly AT test_start must NOT count
    ts = np.array([ms("2026-02-01"), TS1, ms("2026-01-20")], dtype="int64")
    pr = m6a.compute_persistence(ts, TS1, START)
    assert pr.active_blocks == 1               # only 2026-01-20 (block0) counts
    assert pr.recent_block_active is True
    # action exactly at horizon_start (START) IS included (half-open [start, end))
    ts2 = np.array([START], dtype="int64")
    pr2 = m6a.compute_persistence(ts2, TS1, START)
    assert pr2.active_blocks == 1              # START lands in the oldest block


def test_persistence_block_boundary():
    # block boundary 2026-01-12 = block0.start; an action at exactly 01-12 is block0 (half-open)
    ts = np.array([ms("2026-01-12")], dtype="int64")
    pr = m6a.compute_persistence(ts, TS1, START)
    assert pr.recent_block_active is True      # [01-12, 01-26) is block0
    # one ms before 01-12 falls in block1 (not recent)
    ts2 = np.array([ms("2026-01-12") - 1], dtype="int64")
    pr2 = m6a.compute_persistence(ts2, TS1, START)
    assert pr2.recent_block_active is False


# ---------------------------------------------------------------- fixtures for run()
def _folds():
    return pd.DataFrame([
        {"fold_id": 1, "train_start": pd.Timestamp("2025-12-01"), "test_start": pd.Timestamp("2026-01-26"), "oos_chain_order": 1},
        {"fold_id": 2, "train_start": pd.Timestamp("2025-12-15"), "test_start": pd.Timestamp("2026-02-09"), "oos_chain_order": 2},
    ])


def _entities(eids, copyable=True):
    return pd.DataFrame([
        {"entity_id": e, "primary_wallet": f"0xw{e}", "copyable": copyable,
         "entity_alloc_weight": 1.0, "entity_tier": "CLEAN"} for e in eids
    ])


def _pool(eids):
    return pd.DataFrame([
        {"entity_id": e, "source_6m_roe_full": 0.5, "active_test_folds": 3,
         "g5_pool_candidate_pass": True} for e in eids
    ])


def _elig_row(e, fid, roe=0.5, nj=10, dd=0.2, eligible=True):
    return {"entity_id": e, "primary_wallet": f"0xw{e}", "fold_id": fid, "eligible": eligible,
            "roe_pretest_flow_adj": roe, "n_journeys_pretest": nj, "max_dd_pretest": dd, "m4_tier": "CLEAN"}


def _actions(eids):
    # each entity active in all 4 recent blocks of fold 1 + recent of fold 2
    rows = []
    for e in eids:
        for d in ("2026-01-20", "2026-01-05", "2025-12-20", "2026-02-05"):
            rows.append({"wallet": f"0xw{e}", "ts": ms(d)})
    return pd.DataFrame(rows)


def _manifest(mode="shortlist", n=1000, recency=True):
    return {"manifest_version": "test", "mode": mode, "shortlist_n_per_fold": n,
            "recency_gate": recency, "contamination_status": "clean_oos", "persistence_horizon_days": 168}


# ---------------------------------------------------------------- I3 full-window not in selection
def test_full_window_fields_do_not_change_shortlist():
    eids = [1, 2, 3]
    args = (_elig := pd.DataFrame([_elig_row(e, 1, roe=0.1 * e) for e in eids]),
            _pool(eids), _folds(), _entities(eids), _actions(eids))
    sl_a, _, _ = m6a.run(*args, _manifest(n=2))
    pool_b = _pool(eids).copy(); pool_b["source_6m_roe_full"] = [9.9, -5.0, 0.0]  # garbage
    sl_b, _, _ = m6a.run(_elig, pool_b, _folds(), _entities(eids), _actions(eids), _manifest(n=2))
    a = sl_a.sort_values(["fold_id", "entity_id"])["in_shortlist"].tolist()
    b = sl_b.sort_values(["fold_id", "entity_id"])["in_shortlist"].tolist()
    assert a == b  # carried reporting fields cannot move the cut


# ---------------------------------------------------------------- I4 entity/exec unit
def test_only_copyable_eligible_primaries_ranked():
    eids = [1, 2]
    elig = pd.DataFrame([_elig_row(1, 1), _elig_row(2, 1, eligible=False)])
    sl, _, _ = m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest())
    # the ineligible entity is present in the audit but never rankable/shortlisted
    assert sl.loc[sl.entity_id == 2, "rankable"].iloc[0] == False  # noqa: E712
    assert sl.loc[sl.entity_id == 2, "in_shortlist"].iloc[0] == False  # noqa: E712
    assert sl.loc[sl.entity_id == 2, "shortlist_reason"].iloc[0] == "ineligible"


# ---------------------------------------------------------------- I5 monotonicity + sign
def test_monotonic_in_roe():
    eids = [1, 2]
    elig = pd.DataFrame([_elig_row(1, 1, roe=0.2), _elig_row(2, 1, roe=0.8)])
    sl, _, _ = m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest())
    s1 = sl.loc[sl.entity_id == 1, "source_score_pretest"].iloc[0]
    s2 = sl.loc[sl.entity_id == 2, "source_score_pretest"].iloc[0]
    assert s2 > s1  # higher ROE, all else equal -> higher score


def test_eligible_nonpositive_roe_hardfails():
    # codex code-r1 #4: M5 guarantees eligible->roe>0; a violation is an upstream contract breach.
    eids = [1]
    elig = pd.DataFrame([_elig_row(1, 1, roe=-0.3)])
    with pytest.raises(AssertionError):
        m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest())


def test_eligible_nan_metric_hardfails():
    eids = [1]
    elig = pd.DataFrame([_elig_row(1, 1)]); elig.loc[0, "max_dd_pretest"] = np.nan
    with pytest.raises(AssertionError):
        m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest())


# ---------------------------------------------------------------- recency gate drops stale
def test_recency_gate_drops_stale_holder():
    eids = [1, 2]
    elig = pd.DataFrame([_elig_row(1, 1), _elig_row(2, 1)])
    acts = pd.DataFrame([
        {"wallet": "0xw1", "ts": ms("2026-01-20")},                 # recent -> rankable
        {"wallet": "0xw2", "ts": ms("2025-12-05")},                 # only old -> stale
    ])
    sl, _, _ = m6a.run(elig, _pool(eids), _folds(), _entities(eids), acts, _manifest())
    f1 = sl[sl.fold_id == 1]
    assert f1.loc[f1.entity_id == 1, "rankable"].iloc[0] == True   # noqa: E712
    assert f1.loc[f1.entity_id == 2, "rankable"].iloc[0] == False  # noqa: E712
    assert f1.loc[f1.entity_id == 2, "shortlist_reason"].iloc[0] == "dropped_stale"


# ---------------------------------------------------------------- I7 budget + conservation
def test_budget_and_conservation():
    eids = list(range(1, 11))
    elig = pd.DataFrame([_elig_row(e, 1, roe=0.05 * e) for e in eids])
    sl, _, wf = m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest(n=3))
    f1 = sl[sl.fold_id == 1]
    assert wf["folds"][1]["B_k"] == 3
    assert int((f1["in_shortlist"] == True).sum()) == 3            # noqa: E712
    n_short = int((f1["in_shortlist"] == True).sum())              # noqa: E712
    n_drop = int((f1["in_shortlist"] == False).sum())             # noqa: E712
    assert n_short + n_drop == len(f1)                            # conservation
    assert wf["total_engine_sims"] <= 8 * 3
    assert wf["budget_ok"] is True


def test_budget_caps_at_eligible_when_small():
    eids = [1, 2]
    elig = pd.DataFrame([_elig_row(e, 1) for e in eids])
    sl, _, wf = m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest(n=1000))
    assert wf["folds"][1]["B_k"] == 2  # min(1000, 2 rankable)


# ---------------------------------------------------------------- I8 determinism
def test_determinism():
    eids = list(range(1, 8))
    elig = pd.DataFrame([_elig_row(e, 1, roe=0.3) for e in eids])  # tie scores -> entity_id tiebreak
    sl1, _, _ = m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest(n=3))
    sl2, _, _ = m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest(n=3))
    a = sl1.sort_values(["fold_id", "entity_id"]).reset_index(drop=True)
    b = sl2.sort_values(["fold_id", "entity_id"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(a, b)
    # tie-break: among equal scores, lowest entity_id ranks first
    f1 = sl1[(sl1.fold_id == 1) & (sl1["in_shortlist"] == True)]   # noqa: E712
    assert set(f1.entity_id) == {1, 2, 3}


# ---------------------------------------------------------------- I2 within-fold independence
def test_within_fold_independence():
    eids = [1, 2, 3]
    elig = pd.DataFrame([_elig_row(e, 1, roe=0.1 * e) for e in eids] +
                        [_elig_row(e, 2, roe=0.1 * e) for e in eids])
    sl_base, _, _ = m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest(n=2))
    # add a monster entity only to fold 2
    eids2 = eids + [99]
    elig2 = pd.concat([elig, pd.DataFrame([_elig_row(99, 2, roe=5.0)])], ignore_index=True)
    sl_new, _, _ = m6a.run(elig2, _pool(eids2), _folds(), _entities(eids2), _actions(eids2), _manifest(n=2))
    f1_base = sl_base[sl_base.fold_id == 1].sort_values("entity_id")["in_shortlist"].tolist()
    f1_new = sl_new[(sl_new.fold_id == 1) & (sl_new.entity_id != 99)].sort_values("entity_id")["in_shortlist"].tolist()
    assert f1_base == f1_new  # fold 1 unchanged by a fold-2 addition


# ---------------------------------------------------------------- I9 rankable contract hard-fail
def test_rankable_contract_hardfail_noncopyable():
    eids = [1]
    elig = pd.DataFrame([_elig_row(1, 1)])
    ents = _entities(eids, copyable=False)  # eligible but NOT copyable -> contract breach
    with pytest.raises(AssertionError):
        m6a.run(elig, _pool(eids), _folds(), ents, _actions(eids), _manifest())


# ---------------------------------------------------------------- I11 rank-only purity
def test_rank_only_purity():
    eids = [1, 2, 3]
    elig = pd.DataFrame([_elig_row(e, 1, roe=0.1 * e) for e in eids])
    # rank_only manifest OMITS shortlist_n_per_fold entirely (codex code-r1 #1)
    man = {"manifest_version": "test", "mode": "rank_only", "recency_gate": True,
           "contamination_status": "exploratory_calibration"}
    sl, _, wf = m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), man)
    # selection/budget columns must NOT exist in rank_only output
    assert "in_shortlist" not in sl.columns
    assert "B_k" not in sl.columns
    assert "shortlist_reason" not in sl.columns
    assert sl["rank_in_fold"].notna().any()      # ranks ARE emitted
    # no budget / candidate-N fields leak into the waterfall
    assert "total_engine_sims" not in wf and "budget_ok" not in wf and "shortlist_n_per_fold" not in wf
    for fwf in wf["folds"].values():
        assert "B_k" not in fwf and "marginal_band" not in fwf and "n_shortlisted" not in fwf


def test_persistence_partial_blocks():
    # codex code-r1 #2: partial oldest block must still count; short history must not zero out
    near = ms("2026-01-20")  # inside block0 of fold1
    # 7d lookback (start = test_start - 7d): one partial block, recent action counts
    pr7 = m6a.compute_persistence(np.array([near], dtype="int64"), TS1, TS1 - 7 * MS_DAY)
    assert pr7.possible_blocks == 1 and pr7.active_blocks == 1 and pr7.recent_block_active
    # 15d lookback: intersects 2 fold-anchored blocks
    pr15 = m6a.compute_persistence(np.array([near], dtype="int64"), TS1, TS1 - 15 * MS_DAY)
    assert pr15.possible_blocks == 2


def test_ineligible_na_metrics_retained_not_ranked():
    eids = [1, 2]
    e1 = _elig_row(1, 1)
    e2 = _elig_row(2, 1, eligible=False)
    e2["roe_pretest_flow_adj"] = np.nan; e2["max_dd_pretest"] = np.nan  # NA on a drop row
    elig = pd.DataFrame([e1, e2])
    sl, _, _ = m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest())
    row2 = sl[sl.entity_id == 2].iloc[0]
    assert row2["rankable"] == False                       # noqa: E712
    assert row2["shortlist_reason"] == "ineligible"
    assert bool(pd.isna(row2["source_score_pretest"]))


def test_primary_wallet_mismatch_hardfails():
    eids = [1]
    elig = pd.DataFrame([_elig_row(1, 1)]); elig.loc[0, "primary_wallet"] = "0xWRONG"
    with pytest.raises(AssertionError):
        m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest())


def test_na_primary_wallet_hardfails():
    # codex code-r2 #1: pd.NA wallet must hard-fail cleanly (no ambiguous-bool crash)
    eids = [1]
    elig = pd.DataFrame([_elig_row(1, 1)])
    elig["primary_wallet"] = elig["primary_wallet"].astype(object); elig.loc[0, "primary_wallet"] = pd.NA
    ents = _entities(eids); ents["primary_wallet"] = ents["primary_wallet"].astype(object); ents.loc[0, "primary_wallet"] = pd.NA
    with pytest.raises(AssertionError):
        m6a.run(elig, _pool(eids), _folds(), ents, _actions(eids), _manifest())


def test_eligible_inf_njourneys_hardfails():
    # codex code-r2 #2: non-finite n_journeys on an eligible row must hard-fail, not crash on int()
    eids = [1]
    elig = pd.DataFrame([_elig_row(1, 1)]); elig["n_journeys_pretest"] = elig["n_journeys_pretest"].astype(float); elig.loc[0, "n_journeys_pretest"] = np.inf
    with pytest.raises(AssertionError):
        m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest())


@pytest.mark.parametrize("badN", [-1, 0, 3.5, True])
def test_invalid_N_rejected(badN):
    eids = [1]
    elig = pd.DataFrame([_elig_row(1, 1)])
    with pytest.raises(ValueError):
        m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), _manifest(n=badN))


def test_recency_disable_requires_noncleanoos():
    eids = [1]
    elig = pd.DataFrame([_elig_row(1, 1)])
    man = _manifest(recency=False)  # contamination_status default clean_oos -> must reject
    with pytest.raises(ValueError):
        m6a.run(elig, _pool(eids), _folds(), _entities(eids), _actions(eids), man)


def test_na_copyable_hardfails():
    eids = [1]
    elig = pd.DataFrame([_elig_row(1, 1)])
    ents = _entities(eids); ents["copyable"] = ents["copyable"].astype(object); ents.loc[0, "copyable"] = pd.NA
    with pytest.raises(AssertionError):
        m6a.run(elig, _pool(eids), _folds(), ents, _actions(eids), _manifest())
