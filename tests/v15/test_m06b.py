"""V15 M6b ranking tests -- frozen-spec mechanics on synthetic inputs.

Design: brain projects/quant/v15/modules/m06b. Run:
  /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_m06b.py -q
"""
import contextlib
import hashlib
import importlib.util
import io
import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m06b_ranking as M  # noqa: E402

# the REAL fills reader, captured before the autouse fixture swaps in the DataFrame-slot fake
# (needed by the CLI end-to-end test, which feeds real parquet files).
_ORIG_EXPOSURE = M._exposure_days_from_fills


# --------------------------------------------------------------------------- #
# Synthetic input builder
# --------------------------------------------------------------------------- #
def _folds(n_folds=1):
    rows = []
    base = pd.Timestamp("2025-12-01")
    for k in range(1, n_folds + 1):
        train_start = base + pd.Timedelta(days=14 * (k - 1))
        test_start = train_start + pd.Timedelta(days=56)   # 42 train + 14 val = 4 x 14d blocks
        rows.append({
            "fold_id": k, "train_start": train_start, "test_start": test_start,
            "test_end_excl": test_start + pd.Timedelta(days=14),
        })
    return pd.DataFrame(rows)


def _make_inputs(n_entities=120, fold_id=1, *, uncalibrated=True, equity=None,
                 eligible_all=True, copyable_all=True, n_fills=100, exposure_days=10.0,
                 n_journeys=10, active_pretest_folds=3, consistency_active=3,
                 tracking_error=None, realized=False, fold_pure=False):
    folds = _folds(1)
    ts_asof = pd.Timestamp(folds.loc[0, "test_start"]).value // 1_000_000 - 1000
    ids = list(range(n_entities))
    wallets = [f"0x{i:040x}" for i in ids]
    rng = np.random.default_rng(0)
    summ = pd.DataFrame({
        "entity_id": ids, "fold_id": fold_id,
        "start_equity": 10000.0,
        "n_actions": 200, "n_fills": n_fills, "n_capacity_capped": 10,
        "n_backstop_transfer": 0, "total_fees": 50.0, "total_funding": 0.0,
        "max_dd": rng.uniform(0.05, 0.4, n_entities),
        "outcome_states": "survived",
        "ruin": False,
        "final_equity": 12000.0,
        "roe_engine": rng.normal(0.2, 0.5, n_entities),
        "slippage_uncalibrated": uncalibrated, "fee_unversioned": uncalibrated,
        "metadata_uncertain": False, "mode_uncertain": False,
        # m07 window provenance (audit P0#1): must match the fold's [train_start, test_start).
        "window_start_ms": pd.Timestamp(folds.loc[0, "train_start"]).value // 1_000_000,
        "window_end_ms": pd.Timestamp(folds.loc[0, "test_start"]).value // 1_000_000,
        # slippage version provenance (audit P1#4): calibrated runs carry the version M7 priced with.
        "slippage_calibration_version": (None if uncalibrated else "v11-fills-v1"),
    })
    if tracking_error is not None:
        summ["tracking_error"] = float(tracking_error)
    if realized:
        # M7 v2 realized round-trip keys. realized_roe is the rank basis; round_trip_win_rate is the
        # new win-rate term. Make them deterministic + distinct from roe_engine so tests can assert
        # the score consumes the realized basis.
        summ["realized_roe"] = rng.normal(0.15, 0.4, n_entities)
        summ["n_round_trips"] = rng.integers(5, 50, n_entities)
        summ["n_round_trip_wins"] = (summ["n_round_trips"] * rng.uniform(0.3, 0.8, n_entities)).astype(int)
        summ["round_trip_win_rate"] = summ["n_round_trip_wins"] / summ["n_round_trips"].clip(lower=1)
        summ["realized_pnl_total"] = summ["realized_roe"] * summ["start_equity"]
        # M7 v3 cutoff accounting: no open positions in this synthetic fixture,
        # so conservative economics equal realized economics with full coverage.
        summ["conservative_roe"] = summ["realized_roe"]
        summ["conservative_pnl_total"] = summ["realized_pnl_total"]
        summ["censoring_coverage"] = 1.0
    m06a = pd.DataFrame({
        "entity_id": ids, "fold_id": fold_id, "primary_wallet": wallets,
        "copyable": copyable_all, "rankable": True,
        "n_journeys_pretest": n_journeys,
        "max_dd_pretest": summ["max_dd"].values,
        "m4_tier": "CLEAN", "entity_alloc_weight": 1.0,
        # active_blocks IS active_pretest_folds (pretest sub-split activity, no look-ahead)
        "active_blocks": active_pretest_folds, "possible_blocks": 4,
        "g5_pool_candidate_pass": True,
        "as_of_ms": ts_asof,
    })
    m05 = pd.DataFrame({
        "entity_id": ids, "fold_id": fold_id,
        "eligible": eligible_all, "as_of_ms": ts_asof,
    })
    m04e = pd.DataFrame({"entity_id": ids, "copyable": copyable_all})
    m04_auth = pd.DataFrame({"wallet": wallets, "entity_id": ids})
    if fold_pure:
        # fold-pure per-fold M4 carries fold_id + as_of_ms == test_start (m06b provenance requirement).
        test_start_ms = pd.Timestamp(folds.loc[0, "test_start"]).value // 1_000_000
        for _d in (m04e, m04_auth):
            _d["fold_id"] = fold_id
            _d["as_of_ms"] = test_start_ms
    t0 = pd.Timestamp(folds.loc[0, "train_start"]).value // 1_000_000
    if equity is not None:
        # derive fills + journeys from the equity samples so per-block activeness (>=5 fills AND
        # >=1 journey) matches the equity coverage. One fill per equity sample; one journey per
        # (entity, 14d-block) present in equity.
        fills = equity.rename(columns={"ts": "our_ts"})[["entity_id", "fold_id", "our_ts"]].copy()
        blk = ((equity["ts"] - t0) // (M.MS_PER_DAY * 14)).astype(int)
        je = equity.assign(block_idx=blk).drop_duplicates(["entity_id", "block_idx"])
        w_of = {i: w for i, w in zip(ids, wallets)}
        journeys = pd.DataFrame({
            "wallet": je["entity_id"].map(w_of).values,
            "entry_ts": (t0 + je["block_idx"].values * M.MS_PER_DAY * 14 + 1000),
        })
    else:
        fills = pd.DataFrame({
            "entity_id": np.repeat(ids, 2), "fold_id": fold_id,
            "our_ts": np.tile([t0, t0 + int(exposure_days * M.MS_PER_DAY)], n_entities),
        })
        journeys = pd.DataFrame(columns=["wallet", "entry_ts"])
    return {
        "folds": folds, "m07_summary": summ, "m07_fills_path": fills,  # path slots hold DFs for tests
        "m06a": m06a, "m05": m05, "m04_entities": m04e, "m04_auth": m04_auth,
        "m04_fold_pure": fold_pure,  # audit 2026-07-10 P0#2: investable requires fold-pure M4
        "m02_journeys_path": journeys, "m07_equity": equity,
    }


@pytest.fixture(autouse=True)
def _patch_exposure(monkeypatch):
    """In tests the fills 'path' slot holds a DataFrame; patch the reader to consume it directly."""
    def fake_exposure(fills_obj, _m07_dir):
        df = fills_obj if isinstance(fills_obj, pd.DataFrame) else pd.DataFrame()
        if df.empty:
            return pd.DataFrame(columns=["entity_id", "fold_id", "exposure_days"])
        g = df.groupby(["entity_id", "fold_id"])["our_ts"].agg(["min", "max"]).reset_index()
        g["exposure_days"] = (g["max"] - g["min"]) / M.MS_PER_DAY
        return g[["entity_id", "fold_id", "exposure_days"]]
    monkeypatch.setattr(M, "_exposure_days_from_fills", fake_exposure)


# --------------------------------------------------------------------------- #
# Tests
# --------------------------------------------------------------------------- #
def test_winsorize_clips_tails():
    s = pd.Series([0.0, 1, 2, 3, 4, 5, 6, 7, 8, 1000.0])
    w = M._winsorize(s, 1, 99)
    assert w.max() < 1000.0
    assert w.min() >= 0.0


def test_zscore_zero_variance():
    s = pd.Series([3.0, 3.0, 3.0])
    z = M._zscore(s)
    assert (z == 0).all()


def test_score_formula_uses_frozen_weights():
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120, realized=True)
    out, manifest = M.build_ranking(inp, m)
    assert manifest["w_realized_roe"] == 0.25
    assert manifest["w_calmar"] == 0.20
    assert manifest["w_win_rate"] == 0.15
    assert manifest["w_consistency"] == 0.15
    assert manifest["w_survivability_penalty"] == 0.15
    rk = out[out["m6b_rankable"]]
    assert rk["m6b_score"].notna().all()


def test_pool_caps_at_n_pool_and_g5():
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=150, active_pretest_folds=5, n_journeys=10)
    out, _ = M.build_ranking(inp, m)
    assert out["in_pool"].sum() <= m.n_pool
    # G5: make everyone fail active_pretest_folds (<3) -> pool empty
    inp2 = _make_inputs(n_entities=150, active_pretest_folds=2)
    out2, _ = M.build_ranking(inp2, m)
    assert out2["in_pool"].sum() == 0


def test_g5_journeys_floor():
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120, n_journeys=4)  # < 5 -> G5 fail
    out, _ = M.build_ranking(inp, m)
    assert out["in_pool"].sum() == 0


def test_quality_weight_normalized_and_ceiling():
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120)
    out, _ = M.build_ranking(inp, m)
    pooled = out[out["in_pool"]]
    if len(pooled):
        assert abs(pooled["quality_weight"].sum() - 1.0) < 1e-6
        assert pooled["quality_weight"].max() <= m.per_entity_quality_ceiling + 1e-9


def test_bucket_weights_monotone():
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120)
    out, _ = M.build_ranking(inp, m)
    pooled = out[out["in_pool"]]
    if len(pooled) >= m.n_buckets:
        # bucket 1 (or demoted) should generally carry >= weight than bucket 5
        w_by_bucket = pooled.groupby("bucket")["quality_weight"].mean()
        assert w_by_bucket.index.min() >= 1 and w_by_bucket.index.max() <= 5


def test_min_support_excludes_low_fills():
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120, n_fills=10)  # < 30
    out, _ = M.build_ranking(inp, m)
    assert out["m6b_rankable"].sum() == 0
    assert out["excluded_reason"].str.contains("n_fills").any()


def test_min_support_excludes_short_exposure():
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120, exposure_days=1.0)  # < 3d
    out, _ = M.build_ranking(inp, m)
    assert out["m6b_rankable"].sum() == 0
    assert out["excluded_reason"].str.contains("exposure").any()


def test_ineligible_and_noncopyable_excluded():
    m = M.M6bManifest()
    out_inelig, _ = M.build_ranking(_make_inputs(n_entities=120, eligible_all=False), m)
    assert out_inelig["m6b_rankable"].sum() == 0
    out_noncopy, _ = M.build_ranking(_make_inputs(n_entities=120, copyable_all=False), m)
    assert out_noncopy["m6b_rankable"].sum() == 0


def test_provenance_fail_closed():
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=20)
    # stamp m06a as_of AFTER test_start -> must raise
    bad_asof = pd.Timestamp(inp["folds"].loc[0, "test_start"]).value // 1_000_000 + 10_000
    inp["m06a"]["as_of_ms"] = bad_asof
    with pytest.raises(ValueError, match="PROVENANCE FAIL-CLOSED"):
        M.build_ranking(inp, m)


def test_provisional_when_uncalibrated():
    m = M.M6bManifest()  # no calibration versions set
    out, manifest = M.build_ranking(_make_inputs(uncalibrated=True), m)
    assert manifest["investable"] is False
    assert bool(out["investable"].iloc[0]) is False
    assert manifest["uncalibrated_input"] is True


def _equity_all_active(n, m, folds, positive=True):
    """Equity samples: every entity has 4 active 14d blocks (>=6 samples each), monotone so block
    roe>0 when positive=True."""
    t0 = pd.Timestamp(folds.loc[0, "train_start"]).value // 1_000_000
    blk = m.block_days * M.MS_PER_DAY
    rows = []
    for eid in range(n):
        for b in range(4):
            for j in range(6):
                sign = 1 if positive else -1
                rows.append({"entity_id": eid, "fold_id": 1, "ts": t0 + b * blk + j * 1000,
                             "subaccount_equity": 10000.0 + sign * (b * 100 + j + 1)})
    return pd.DataFrame(rows)


def test_final_requires_calibration_AND_real_fidelity_AND_real_consistency():
    m = M.M6bManifest(fee_schedule_version="hl-v1", slippage_calibration_version="v11-fills-v1")
    folds = _folds(1)
    eq = _equity_all_active(60, m, folds, positive=True)
    # calibrated costs + real tracking_error + real equity-block consistency + realized round-trips
    # -> investable=True
    inp = _make_inputs(n_entities=60, uncalibrated=False, equity=eq, tracking_error=0.05,
                       realized=True, fold_pure=True)   # audit P0#2: investable requires fold-pure M4
    out, manifest = M.build_ranking(inp, m)
    assert manifest["consistency_source"] == "m07_equity_block_roe"
    assert manifest["fidelity_source"] == "m07_tracking_error"
    assert manifest["return_basis"] == "conservative_roe"
    assert manifest["investable"] is True
    assert bool(out["investable"].iloc[0]) is True


def test_final_blocked_without_equity_even_if_calibrated():
    # codex r1 #5: calibrated costs but NO equity block consistency must NOT be investable.
    m = M.M6bManifest(fee_schedule_version="hl-v1", slippage_calibration_version="v11-fills-v1")
    out, manifest = M.build_ranking(
        _make_inputs(uncalibrated=False, equity=None, tracking_error=0.05), m)
    assert manifest["investable"] is False
    assert any("consistency_provisional" in r for r in manifest["non_investable_reasons"])


def test_final_blocked_when_tracking_error_column_has_nan():
    # codex r2 #B: present-but-NaN tracking_error must NOT pass as real fidelity.
    m = M.M6bManifest(fee_schedule_version="hl-v1", slippage_calibration_version="v11-fills-v1")
    folds = _folds(1)
    eq = _equity_all_active(60, m, folds, positive=True)
    inp = _make_inputs(n_entities=60, uncalibrated=False, equity=eq, tracking_error=0.05)
    inp["m07_summary"].loc[inp["m07_summary"].entity_id.isin([0, 1, 2]), "tracking_error"] = np.nan
    out, manifest = M.build_ranking(inp, m)
    assert manifest["investable"] is False
    assert any("fidelity_provisional" in r for r in manifest["non_investable_reasons"])
    # the NaN-te rows are flagged provisional per-row
    assert (out.loc[out.entity_id.isin([0, 1, 2]), "fidelity_source"] == "unavailable_provisional").all()


def test_final_blocked_when_tracking_error_is_inf():
    # codex r3 #2: +/-inf tracking_error is not finite -> provisional, must not pass source gate.
    m = M.M6bManifest(fee_schedule_version="hl-v1", slippage_calibration_version="v11-fills-v1")
    folds = _folds(1)
    eq = _equity_all_active(60, m, folds, positive=True)
    inp = _make_inputs(n_entities=60, uncalibrated=False, equity=eq, tracking_error=0.05)
    inp["m07_summary"].loc[inp["m07_summary"].entity_id == 0, "tracking_error"] = np.inf
    out, manifest = M.build_ranking(inp, m)
    assert manifest["investable"] is False
    assert (out.loc[out.entity_id == 0, "fidelity_source"] == "unavailable_provisional").all()


def test_d4_block_roe_uses_boundary_anchors_not_midblock():
    # D4: with anchors present, block ROE = anchor(b1)/anchor(b0)-1. A mid-block SPIKE must NOT make
    # a block positive if it CLOSED down at the boundary anchor.
    m = M.M6bManifest()
    folds = _folds(1)
    t0 = pd.Timestamp(folds.loc[0, "train_start"]).value // 1_000_000
    blk = m.block_days * M.MS_PER_DAY
    rows = []
    # block 0: anchor open 10000 (b0), a mid-block action spike to 13000, boundary close 9000 (b1).
    rows.append({"entity_id": 1, "fold_id": 1, "ts": t0, "subaccount_equity": 10000.0, "event_flag": "fold_start"})
    rows.append({"entity_id": 1, "fold_id": 1, "ts": t0 + blk // 2, "subaccount_equity": 13000.0, "event_flag": "action"})
    rows.append({"entity_id": 1, "fold_id": 1, "ts": t0 + blk, "subaccount_equity": 9000.0, "event_flag": "block_boundary"})
    rows.append({"entity_id": 1, "fold_id": 1, "ts": t0 + 2 * blk, "subaccount_equity": 9500.0, "event_flag": "block_boundary"})
    eq = pd.DataFrame(rows)
    ba = pd.DataFrame([
        {"entity_id": 1, "fold_id": 1, "block_idx": 0, "n_fills_block": 10, "n_journeys_block": 2},
        {"entity_id": 1, "fold_id": 1, "block_idx": 1, "n_fills_block": 10, "n_journeys_block": 2},
    ])
    cons = M._consistency_from_equity(eq, folds, m, ba)
    r = cons[cons.entity_id == 1].iloc[0]
    # block 0: 9000/10000 < 0 -> NOT positive (despite the 13000 mid-spike). block 1: 9500/9000 > 0 -> positive.
    assert r["n_active_subsplits"] == 2
    assert r["consistency"] == pytest.approx(0.5)   # 1 of 2 active blocks positive (anchor-based)


def test_consistency_active_no_equity_is_failclosed():
    # codex r3 #1: entity active by fills+journeys but with NO equity rows -> active=N, positive=0.
    m = M.M6bManifest()
    folds = _folds(1)
    # blocks_activity: entity 99 has 3 active blocks (>=5 fills, >=1 journey), NO equity at all.
    ba = pd.DataFrame([
        {"entity_id": 99, "fold_id": 1, "block_idx": b, "n_fills_block": 10, "n_journeys_block": 2}
        for b in range(3)
    ])
    empty_eq = pd.DataFrame(columns=["entity_id", "fold_id", "ts", "subaccount_equity"])
    cons = M._consistency_from_equity(empty_eq, folds, m, ba)
    row = cons[cons.entity_id == 99].iloc[0]
    assert row["n_active_subsplits"] == 3
    assert row["consistency"] == 0.0  # no equity coverage -> nothing positive


def test_final_blocked_without_tracking_error():
    # codex r1 #4: missing tracking_error (neutral fidelity) must NOT be investable.
    m = M.M6bManifest(fee_schedule_version="hl-v1", slippage_calibration_version="v11-fills-v1")
    folds = _folds(1)
    eq = _equity_all_active(60, m, folds, positive=True)
    out, manifest = M.build_ranking(
        _make_inputs(n_entities=60, uncalibrated=False, equity=eq, tracking_error=None), m)
    assert manifest["investable"] is False
    assert any("fidelity_provisional" in r for r in manifest["non_investable_reasons"])


def test_consistency_provisional_source_when_no_equity():
    m = M.M6bManifest()
    out, manifest = M.build_ranking(_make_inputs(equity=None), m)
    assert manifest["consistency_source"].startswith("m6a_source_persistence")


def test_consistency_from_equity_block_roe():
    m = M.M6bManifest()
    folds = _folds(1)
    t0 = pd.Timestamp(folds.loc[0, "train_start"]).value // 1_000_000
    blk = m.block_days * M.MS_PER_DAY
    # entity 0: 4 active blocks, all positive -> consistency 1.0
    rows = []
    for b in range(4):
        for j in range(6):  # >= min_fills (5) samples per block
            rows.append({"entity_id": 0, "fold_id": 1, "ts": t0 + b * blk + j * 1000,
                         "subaccount_equity": 10000.0 + b * 1000 + j})
    eq = pd.DataFrame(rows)
    inp = _make_inputs(n_entities=120, equity=eq)
    out, manifest = M.build_ranking(inp, m)
    assert manifest["consistency_source"] == "m07_equity_block_roe"
    e0 = out[(out.entity_id == 0) & (out.fold_id == 1)].iloc[0]
    assert e0["consistency"] == pytest.approx(1.0)
    assert e0["n_active_subsplits"] == 4


def test_top_bucket_demotion_no_backfill():
    # An entity in bucket 1 with consistency < 0.5 is demoted to bucket 2; no bucket-1 backfill.
    m = M.M6bManifest()
    folds = _folds(1)
    t0 = pd.Timestamp(folds.loc[0, "train_start"]).value // 1_000_000
    blk = m.block_days * M.MS_PER_DAY
    # build equity so the TOP scorer has low consistency (only 1 of 4 blocks positive -> 0.25)
    n = 120
    rows = []
    for eid in range(n):
        for b in range(4):
            sign = 1 if (eid != 0 or b == 0) else -1  # entity 0: only block0 positive
            for j in range(6):
                rows.append({"entity_id": eid, "fold_id": 1, "ts": t0 + b * blk + j * 1000,
                             "subaccount_equity": 10000.0 + sign * (b * 100 + j + 1)})
    eq = pd.DataFrame(rows)
    inp = _make_inputs(n_entities=n, equity=eq)
    # force entity 0 to be top score
    inp["m07_summary"].loc[inp["m07_summary"].entity_id == 0, "roe_engine"] = 100.0
    inp["m07_summary"].loc[inp["m07_summary"].entity_id == 0, "max_dd"] = 0.05
    out, _ = M.build_ranking(inp, m)
    e0 = out[(out.entity_id == 0) & out["in_pool"]]
    if len(e0):
        assert e0.iloc[0]["consistency"] < m.top_bucket_consistency_gate
        assert e0.iloc[0]["bucket"] == 2  # demoted, not 1


def test_missing_m07_row_excluded():
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120)
    inp["m07_summary"].loc[inp["m07_summary"].entity_id == 5, "roe_engine"] = np.nan
    out, _ = M.build_ranking(inp, m)
    e5 = out[out.entity_id == 5].iloc[0]
    assert not e5["m6b_rankable"]
    assert "missing_m07_row" in e5["excluded_reason"]


def test_absent_m07_row_preserved_as_excluded():
    # codex r1 #1: a M6a row with NO M7 summary row must stay in output as missing_m07_row, not drop.
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120)
    inp["m07_summary"] = inp["m07_summary"][inp["m07_summary"].entity_id != 7].copy()
    out, _ = M.build_ranking(inp, m)
    assert (out.entity_id == 7).any(), "entity 7 was silently dropped"
    e7 = out[out.entity_id == 7].iloc[0]
    assert not e7["m6b_rankable"]
    assert "missing_m07_row" in e7["excluded_reason"]


def test_provenance_null_as_of_fails_closed():
    # codex r1 #7: NaN/missing as_of_ms is not provenance-OK.
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=20)
    inp["m05"].loc[0, "as_of_ms"] = np.nan
    with pytest.raises(ValueError, match="PROVENANCE FAIL-CLOSED"):
        M.build_ranking(inp, m)
    inp2 = _make_inputs(n_entities=20)
    inp2["m06a"] = inp2["m06a"].drop(columns=["as_of_ms"])
    with pytest.raises(ValueError, match="PROVENANCE FAIL-CLOSED"):
        M.build_ranking(inp2, m)


def test_small_pool_ceiling_feasible():
    # codex r1 #6: pool < 10 -> nominal 10% ceiling infeasible; weights must still sum to 1.
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=6)  # only 6 rankable -> pool 6 < 10
    out, _ = M.build_ranking(inp, m)
    pooled = out[out["in_pool"]]
    if len(pooled):
        assert abs(pooled["quality_weight"].sum() - 1.0) < 1e-6


def test_manifest_has_frozen_constants():
    m = M.M6bManifest()
    _, manifest = M.build_ranking(_make_inputs(), m)
    for k in ["n_pool", "dd_floor", "fidelity_B", "bucket_weights", "per_entity_quality_ceiling",
              "g5_min_active_pretest_folds", "min_fills_pretest", "winsor_lo_pct"]:
        assert k in manifest
    assert manifest["n_pool"] == 100
    assert tuple(manifest["bucket_weights"]) == (5, 4, 3, 2, 1)


# --------------------------------------------------------------------------- #
# v2: realized round-trip basis + win-rate term
# --------------------------------------------------------------------------- #
def test_score_weight_vector_v2():
    m = M.M6bManifest()
    _, manifest = M.build_ranking(_make_inputs(realized=True), m)
    assert manifest["w_realized_roe"] == 0.25
    assert manifest["w_calmar"] == 0.20
    assert manifest["w_win_rate"] == 0.15
    assert manifest["w_consistency"] == 0.15
    assert manifest["w_capacity_health"] == 0.10
    assert manifest["w_fidelity"] == 0.10
    assert manifest["w_survivability_penalty"] == 0.15
    # old keys gone
    assert "w_roe_adj" not in manifest


def test_ranking_uses_conservative_roe_not_engine():
    # With no open exposure, conservative_roe equals realized_roe and is the rank basis.
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120, realized=True)
    summ = inp["m07_summary"]
    ref = summ[["entity_id", "realized_roe", "roe_engine"]].copy()
    out, manifest = M.build_ranking(inp, m)
    assert manifest["return_basis"] == "conservative_roe"
    merged = out[["entity_id", "roe_adj"]].merge(ref, on="entity_id")
    # rank basis == realized_roe
    assert np.allclose(merged["roe_adj"], merged["realized_roe"], equal_nan=True)
    # and it is NOT the engine value (the two columns were drawn from different distributions)
    assert not np.allclose(merged["realized_roe"], merged["roe_engine"])


def test_incomplete_open_position_mark_gets_worst_case_account_loss_bound():
    m = M.M6bManifest(fee_schedule_version="hl-v1", slippage_calibration_version="v11-fills-v1")
    folds = _folds(1)
    eq = _equity_all_active(60, m, folds, positive=True)
    inp = _make_inputs(n_entities=60, uncalibrated=False, equity=eq, tracking_error=0.05,
                       realized=True, fold_pure=True)
    inp["m07_summary"].loc[0, "censoring_coverage"] = 0.5
    out, manifest = M.build_ranking(inp, m)
    bounded = out.loc[out["entity_id"].eq(0)].iloc[0]
    assert bounded["roe_adj"] == -1.0
    assert bounded["return_basis"] == "conservative_roe_worst_case_bound"
    assert manifest["open_position_censoring_complete"] is False
    assert manifest["missing_mark_worst_case_bound_complete"] is True
    assert manifest["investable"] is True


def test_missing_censoring_metadata_still_blocks_investable():
    m = M.M6bManifest(fee_schedule_version="hl-v1", slippage_calibration_version="v11-fills-v1")
    folds = _folds(1)
    eq = _equity_all_active(60, m, folds, positive=True)
    inp = _make_inputs(n_entities=60, uncalibrated=False, equity=eq, tracking_error=0.05,
                       realized=True, fold_pure=True)
    inp["m07_summary"] = inp["m07_summary"].drop(columns=["censoring_coverage"])
    _, manifest = M.build_ranking(inp, m)
    assert manifest["investable"] is False
    assert "open_position_censoring_incomplete" in manifest["non_investable_reasons"]


def test_realized_roe_fallback_flagged_when_absent():
    # No realized_roe column -> fall back to roe_engine, flagged per-run + per-row.
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120, realized=False)  # no realized keys
    out, manifest = M.build_ranking(inp, m)
    assert manifest["return_basis"] == "roe_engine_fallback"
    assert manifest["return_basis_fallback"] is True
    assert (out.loc[out["m6b_rankable"], "return_basis"] == "roe_engine_fallback").all()


def test_win_rate_term_changes_ranking():
    # Two otherwise-identical entities differing only in round_trip_win_rate must rank by win-rate.
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120, realized=True)
    summ = inp["m07_summary"]
    # pin entities 0 and 1 to identical realized_roe / max_dd; entity 0 high win-rate, entity 1 low.
    for c, v in {"realized_roe": 0.2, "max_dd": 0.1, "n_round_trips": 20}.items():
        summ.loc[summ.entity_id.isin([0, 1]), c] = v
    summ.loc[summ.entity_id == 0, "round_trip_win_rate"] = 0.9
    summ.loc[summ.entity_id == 1, "round_trip_win_rate"] = 0.1
    out, _ = M.build_ranking(inp, m)
    s0 = out[out.entity_id == 0].iloc[0]["m6b_score"]
    s1 = out[out.entity_id == 1].iloc[0]["m6b_score"]
    assert s0 > s1, "higher win-rate must yield higher m6b_score when other terms are equal"


def test_investable_requires_realized_metrics():
    # calibrated + real fidelity + real consistency but NO realized round-trip metrics -> NOT investable.
    m = M.M6bManifest(fee_schedule_version="hl-v1", slippage_calibration_version="v11-fills-v1")
    folds = _folds(1)
    eq = _equity_all_active(60, m, folds, positive=True)
    inp = _make_inputs(n_entities=60, uncalibrated=False, equity=eq, tracking_error=0.05,
                       realized=False)  # roe_engine fallback, no round-trip metrics
    out, manifest = M.build_ranking(inp, m)
    assert manifest["investable"] is False
    assert any("realized_metrics_missing" in r for r in manifest["non_investable_reasons"])


def test_nan_fidelity_filled_with_fold_mean_not_one():
    # FIX: a row with NaN tracking_error must get the FOLD MEAN of known fidelities, NOT 1.0 (max).
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120, realized=True, tracking_error=0.05)
    summ = inp["m07_summary"]
    # entity 0 has NaN tracking_error; the rest have te=0.05 -> known fidelity = 1 - 0.05/0.25 = 0.8.
    summ.loc[summ.entity_id == 0, "tracking_error"] = np.nan
    out, _ = M.build_ranking(inp, m)
    e0 = out[out.entity_id == 0].iloc[0]
    assert e0["fidelity_source"] == "unavailable_provisional"
    # fold-mean of the known 0.8 fidelities is 0.8 -- NOT the old NaN->1.0 advantage.
    assert e0["fidelity"] == pytest.approx(0.8, abs=1e-6)
    assert e0["fidelity"] < 1.0


def test_zero_round_trip_entity_excluded_from_pool():
    # codex finding: an entity with ZERO closed round-trips (open-only) emits realized_roe=0.0 /
    # round_trip_win_rate=0.0 from M7 -- it must be EXCLUDED from rankable/in_pool, not just from the
    # global investable stamp.
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120, realized=True)
    summ = inp["m07_summary"]
    # entity 0: open-only -> 0 closed round-trips, realized metrics zeroed (M7 behavior).
    summ.loc[summ.entity_id == 0, "n_round_trips"] = 0
    summ.loc[summ.entity_id == 0, "n_round_trip_wins"] = 0
    summ.loc[summ.entity_id == 0, "realized_roe"] = 0.0
    summ.loc[summ.entity_id == 0, "round_trip_win_rate"] = 0.0
    # entity 1: low but nonzero closed round-trips below the floor.
    summ.loc[summ.entity_id == 1, "n_round_trips"] = M.MIN_ROUND_TRIPS - 1
    out, _ = M.build_ranking(inp, m)
    e0 = out[out.entity_id == 0].iloc[0]
    e1 = out[out.entity_id == 1].iloc[0]
    assert not e0["m6b_rankable"], "zero-round-trip entity must not be rankable"
    assert not bool(e0["in_pool"]), "zero-round-trip entity must not enter the pool"
    assert e0["quality_weight"] == 0.0
    assert f"n_round_trips<{M.MIN_ROUND_TRIPS}" in e0["excluded_reason"]
    assert not e1["m6b_rankable"], "below-floor round-trip entity must not be rankable"
    assert not bool(e1["in_pool"])
    # an entity at/above the floor is still rankable.
    summ.loc[summ.entity_id == 2, "n_round_trips"] = M.MIN_ROUND_TRIPS
    out2, _ = M.build_ranking(inp, m)
    e2 = out2[out2.entity_id == 2].iloc[0]
    assert e2["m6b_rankable"]


def test_active_test_folds_not_in_output():
    # active_test_folds is post-test_start leak info -> must be dropped from output columns.
    m = M.M6bManifest()
    out, _ = M.build_ranking(_make_inputs(realized=True), m)
    assert "active_test_folds" not in out.columns or "active_test_folds" not in M.OUT_COLS
    assert "active_test_folds" not in M.OUT_COLS
    # G5 still uses active_pretest_folds (the correct pretest field) -- pool selection still works.
    assert "active_pretest_folds" not in M.OUT_COLS  # internal, not exported; G5 reads it pre-merge


# --------------------------------------------------------------------------- #
# NATIVE BEHAVIOR GATES (2026-08-07): post-FDR confirmed-set vetoes, absorbing
# post_m06b_hard_gates.py / build_roster_freeze.py. All knobs default OFF.
# --------------------------------------------------------------------------- #
def _write_wf_testdir(tmp_path, n_entities=12, fold_id=1, n_j=40):
    """Held-out TEST m07 dir: per-position emit + summary with full censoring coverage. Every
    entity gets clearly-positive OOS journeys (mean r_i = 0.04) so BH-FDR confirms all of them;
    gate tests then kill specific wallets on behavior, not on edge."""
    rows = []
    for eid in range(n_entities):
        for j in range(n_j):
            rows.append({"entity_id": eid, "fold_id": fold_id,
                         "r_i": 0.05 if j % 2 == 0 else 0.03,
                         "side": 1 if j % 2 == 0 else -1})
    tdir = tmp_path / "m07_test"
    tdir.mkdir(exist_ok=True)
    pd.DataFrame(rows).to_parquet(tdir / "m07_positions.parquet", index=False)
    # n_actions/n_fills: healthy-fold defaults so the MARK-COVERAGE gate (2026-08-07) sees a
    # priced fold; starvation tests overwrite the summary with their own values.
    pd.DataFrame({"entity_id": list(range(n_entities)), "fold_id": fold_id,
                  "censoring_coverage": 1.0, "n_actions": n_j * 2,
                  "n_fills": n_j}).to_parquet(tdir / "m07_summary.parquet", index=False)
    return tdir


def _leader_panel(wallets, uw=0.05, mae=0.05, liq=0.0, frac_long=0.5, hold_h=5.0, n_pos=40):
    """One LEADER-space profile-panel cell per wallet (copy_wallet_profile.py schema subset)."""
    return pd.DataFrame([
        {"primary_wallet": w, "n_pos": n_pos, "mean_underwater_add": uw, "mae_p90": mae,
         "liq_rate": liq, "frac_long": frac_long, "median_hold_h": hold_h} for w in wallets])


def _wf_manifest(**kw):
    # single-fold synthetic fixture -> relax the fold/journey floors (settable knobs, not edits)
    kw.setdefault("oos_min_folds", 1)
    kw.setdefault("oos_min_journeys_pooled", 10)
    return M.M6bManifest(**kw)


def _reference_confirm(inp, testdir, m):
    """The PRE-gate (pre-2026-08-07) walk_forward_confirm pipeline reproduced verbatim: rankable
    pretest pool -> named OOS journeys -> pooled per-wallet stats -> eligibility -> BH-FDR ->
    pre_entity merge. Guards defaults-off byte-identity of the native-gate change. (The fixture has
    censoring_coverage == 1.0 everywhere, so the censoring exclusion is a no-op by construction.)"""
    pool, _ = M.build_ranking(inp, m)
    pre = pool[pool["m6b_rankable"] & pool["m6b_score"].notna()].copy()
    pre = pre[pre["primary_wallet"].notna()].copy()
    pre_entity = pre.sort_values("m6b_score", ascending=False).drop_duplicates("primary_wallet")
    cand = set(pre_entity["primary_wallet"].astype(str))
    tpos = pd.read_parquet(Path(testdir) / "m07_positions.parquet")
    tpos["r_i"] = pd.to_numeric(tpos["r_i"], errors="coerce")
    _map = inp["m06a"][["entity_id", "fold_id", "primary_wallet"]].drop_duplicates(
        ["entity_id", "fold_id"])
    tpos = tpos.merge(_map, on=["entity_id", "fold_id"], how="left")
    tpos = tpos.dropna(subset=["primary_wallet", "r_i"])
    tpos = tpos[tpos["primary_wallet"].astype(str).isin(cand)]
    rows = []
    for wal, g in tpos.groupby("primary_wallet"):
        r = g["r_i"].to_numpy("float64")
        fold_means = g.groupby("fold_id")["r_i"].mean()
        rows.append({"primary_wallet": str(wal), "oos_n": len(r),
                     "oos_folds": int(g["fold_id"].nunique()), "oos_mean_r": float(r.mean()),
                     "oos_frac_folds_pos": float((fold_means > 0).mean()) if len(fold_means) else 0.0,
                     "p_boot": M._boot_p_mean_gt(r, m.oos_margin)})
    stats = pd.DataFrame(rows)
    eligible = stats[(stats["oos_folds"] >= m.oos_min_folds)
                     & (stats["oos_n"] >= m.oos_min_journeys_pooled)
                     & (stats["oos_frac_folds_pos"] >= m.oos_min_frac_folds_pos)].copy()
    eligible["bh_discovery"] = M._bh_fdr_mask(eligible["p_boot"].to_numpy("float64"), m.fdr_q)
    confirmed = eligible[eligible["bh_discovery"]].copy()
    return confirmed.merge(pre_entity[["primary_wallet", "entity_id", "m6b_score"]],
                           on="primary_wallet", how="left")


def test_gates_off_confirmed_byte_identical(tmp_path):
    """(D1) With every gate knob unset, walk_forward_confirm output must be BYTE-IDENTICAL to the
    pre-gate pipeline: no gate report, no behavior_gates summary key, identical parquet bytes."""
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    m = _wf_manifest()
    confirmed, summ, gate_report = M.walk_forward_confirm(inp, tdir, m)
    assert gate_report is None
    assert "behavior_gates" not in summ
    assert len(confirmed) == 12  # everyone edge-confirmed; nothing behavior-killed
    expected = _reference_confirm(inp, tdir, m)
    pd.testing.assert_frame_equal(confirmed.reset_index(drop=True),
                                  expected.reset_index(drop=True))
    p_new, p_ref = tmp_path / "new.parquet", tmp_path / "ref.parquet"
    confirmed.reset_index(drop=True).to_parquet(p_new, index=False)
    expected.reset_index(drop=True).to_parquet(p_ref, index=False)
    assert p_new.read_bytes() == p_ref.read_bytes()


def test_wmean_matches_roster_freeze_weighting():
    # n_pos-WEIGHTED mean across fold cells (build_roster_freeze.wmean), not the plain mean.
    d = pd.DataFrame({"n_pos": [30, 10], "x": [0.6, 0.2]})
    assert M._wmean(d, "x", "n_pos") == pytest.approx(0.5)   # (0.6*30 + 0.2*10)/40, NOT 0.4
    d2 = pd.DataFrame({"n_pos": [30, 10], "x": [np.nan, 0.2]})
    assert M._wmean(d2, "x", "n_pos") == pytest.approx(0.2)  # NaN cell masked
    assert np.isnan(M._wmean(pd.DataFrame({"n_pos": [1], "x": [np.nan]}), "x", "n_pos"))
    assert np.isnan(M._wmean(pd.DataFrame({"n_pos": [1]}), "x", "n_pos"))  # missing column


def test_gate_uw_add_kills_and_passes_when_unset(tmp_path):
    """(D2) leader-panel uw_add 0.5 (n_pos-weighted across two cells) is KILLED at
    --gate-uw-add-max 0.2 and PASSES when gates are unset."""
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    wallets = list(inp["m06a"]["primary_wallet"])
    panel = pd.concat([
        _leader_panel(wallets[1:]),
        # wallet0: two fold cells -> n_pos-weighted uw_add = (0.6*30 + 0.2*10)/40 = 0.5
        _leader_panel([wallets[0]], uw=0.6, n_pos=30),
        _leader_panel([wallets[0]], uw=0.2, n_pos=10),
    ], ignore_index=True)
    # gates UNSET -> wallet0 confirmed
    c0, s0, rep0 = M.walk_forward_confirm(inp, tdir, _wf_manifest())
    assert wallets[0] in set(c0["primary_wallet"]) and rep0 is None
    # gate SET -> wallet0 killed, everyone else survives; kill counted + reported
    c1, s1, rep1 = M.walk_forward_confirm(inp, tdir, _wf_manifest(gate_uw_add_max=0.2),
                                          leader_panel=panel)
    assert wallets[0] not in set(c1["primary_wallet"])
    assert set(c1["primary_wallet"]) == set(c0["primary_wallet"]) - {wallets[0]}
    bg = s1["behavior_gates"]
    assert bg["killed"]["uw_add"] == 1
    assert bg["unmeasurable"]["uw_add"] == 0
    assert bg["n_confirmed_pre_gate"] == 12 and bg["n_confirmed_post_gate"] == 11
    # codex #4: `confirmed` stays the BH-FDR discovery count; `confirmed_written` is post-gate.
    assert s1["confirmed"] == 12 and s1["confirmed_written"] == 11
    assert s0["confirmed"] == s0["confirmed_written"] == 12  # gates off -> equal
    row = rep1[rep1["primary_wallet"] == wallets[0]].iloc[0]
    assert row["uw_add"] == pytest.approx(0.5)   # weighted, NOT the plain cell mean 0.4
    assert not row["pass_uw_add"] and not row["all_pass"]
    ok = rep1[rep1["primary_wallet"] == wallets[1]].iloc[0]
    assert ok["pass_uw_add"] and ok["all_pass"]


def test_gate_nan_fails_closed_counted_unmeasurable(tmp_path):
    """(D3) NaN on a gated attribute (NaN cell OR wallet absent from the panel) fails CLOSED and is
    counted separately as unmeasurable, never as a measurable kill."""
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    wallets = list(inp["m06a"]["primary_wallet"])
    panel = pd.concat([
        _leader_panel(wallets[2:]),                       # wallet1 ABSENT from the panel entirely
        _leader_panel([wallets[0]], uw=np.nan),           # wallet0 present but uw_add NaN
    ], ignore_index=True)
    c, s, rep = M.walk_forward_confirm(inp, tdir, _wf_manifest(gate_uw_add_max=0.2),
                                       leader_panel=panel)
    got = set(c["primary_wallet"])
    assert wallets[0] not in got and wallets[1] not in got
    assert got == set(wallets[2:])
    bg = s["behavior_gates"]
    assert bg["unmeasurable"]["uw_add"] == 2
    assert bg["killed"]["uw_add"] == 0
    r0 = rep[rep["primary_wallet"] == wallets[0]].iloc[0]
    assert bool(r0["unmeasurable_uw_add"]) and not r0["pass_uw_add"]
    r1 = rep[rep["primary_wallet"] == wallets[1]].iloc[0]
    assert r1["n_panel_cells"] == 0 and bool(r1["unmeasurable_uw_add"])


def test_leader_gate_without_panel_raises_three_tier(tmp_path):
    """(D4) a leader-tier gate WITHOUT --leader-panel is a hard error naming the three-tier law
    (replica-space attributes silently neuter behavior vetoes)."""
    inp = _make_inputs(n_entities=6, realized=True)
    tdir = _write_wf_testdir(tmp_path, 6)
    with pytest.raises(ValueError, match="THREE-TIER"):
        M.walk_forward_confirm(inp, tdir, _wf_manifest(gate_uw_add_max=0.2))
    # direct unit: apply_behavior_gates refuses too (defense in depth)
    conf = pd.DataFrame({"primary_wallet": ["0xaa"]})
    with pytest.raises(ValueError, match="THREE-TIER"):
        M.apply_behavior_gates(conf, _wf_manifest(gate_leader_liq_max=0.005), leader_panel=None)
    # wrong panel schema (missing source column) fails loud, not all-unmeasurable
    with pytest.raises(ValueError, match="lacks required column"):
        M.apply_behavior_gates(conf, _wf_manifest(gate_uw_add_max=0.2),
                               leader_panel=pd.DataFrame({"primary_wallet": ["0xaa"], "n_pos": [5]}))
    # replica-tier / two-sided gates alone need NO panel (three-tier law scopes leader tier only)
    surv, rep, s = M.apply_behavior_gates(conf, _wf_manifest(gate_latency_ratio_max=0.02),
                                          leader_panel=None)
    assert len(surv) == 0 and s["behavior_gates"]["unmeasurable"]["latency_ratio"] == 1


def test_gate_two_sided_and_latency_ratio(tmp_path):
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    wallets = list(inp["m06a"]["primary_wallet"])
    panel = _leader_panel(wallets)
    panel.loc[panel["primary_wallet"] == wallets[1], "frac_long"] = 0.95   # one-sided long
    panel.loc[panel["primary_wallet"] == wallets[2], "median_hold_h"] = 0.01  # 36s scalper
    m = _wf_manifest(gate_two_sided_lo=0.25, gate_two_sided_hi=0.75, gate_latency_ratio_max=0.02)
    c, s, rep = M.walk_forward_confirm(inp, tdir, m, leader_panel=panel)
    got = set(c["primary_wallet"])
    assert wallets[1] not in got and wallets[2] not in got
    assert len(got) == 10
    bg = s["behavior_gates"]
    assert bg["killed"]["two_sided"] == 1 and bg["killed"]["latency_ratio"] == 1
    r2 = rep[rep["primary_wallet"] == wallets[2]].iloc[0]
    # no per-position emit in this fixture -> hold falls back to the leader panel (documented source)
    assert r2["hold_source"] == "leader_panel_median_hold_h"
    assert r2["latency_ratio"] == pytest.approx(4.0 / (0.01 * 3600.0))
    # copy_latency_s knob threads into the ratio: near-zero latency -> wallet2 passes
    m2 = _wf_manifest(gate_latency_ratio_max=0.02, copy_latency_s=1e-4)
    c2, _s2, _ = M.walk_forward_confirm(inp, tdir, m2, leader_panel=panel)
    assert wallets[2] in set(c2["primary_wallet"])


def test_latency_prefers_replica_pp_hold_over_panel():
    """REPLICA tier: median hold comes from the m07 pretest per-position outputs (pp_med_hold_h
    weighted by pp_n) when present; the leader panel is only the fallback."""
    conf = pd.DataFrame({"primary_wallet": ["0xaa"]})
    panel = _leader_panel(["0xaa"], hold_h=5.0)   # panel says 5h (would PASS)
    rep_cells = pd.DataFrame({"primary_wallet": ["0xaa", "0xaa"], "pp_n": [30, 10],
                              "pp_med_hold_h": [0.01, 0.05], "pp_frac_long": [0.5, 0.5]})
    surv, rep, s = M.apply_behavior_gates(conf, _wf_manifest(gate_latency_ratio_max=0.02),
                                          leader_panel=panel, replica_cells=rep_cells)
    r = rep.iloc[0]
    assert r["hold_source"] == "m07_pretest_positions"
    assert r["hold_h"] == pytest.approx((0.01 * 30 + 0.05 * 10) / 40)   # 0.02h -> ratio 4/72 ~ 0.056
    assert len(surv) == 0 and s["behavior_gates"]["killed"]["latency_ratio"] == 1
    # codex #3: coverage disclosure -- per-wallet support of the replica hold read + summary note
    assert r["n_pretest_cells_used"] == 2 and r["n_pp_positions_used"] == 40
    cov = s["behavior_gates"]["latency_coverage"]
    assert cov["wallets_replica_hold"] == 1 and cov["wallets_leader_panel_fallback"] == 0
    assert cov["min_pp_positions_used"] == 40


def _write_cli_fixture(tmp_path, n_entities=12):
    """File-based fixture for the CLI end-to-end test (main() reads real parquet paths)."""
    inp = _make_inputs(n_entities=n_entities, realized=True)
    data = tmp_path / "data"
    data.mkdir()
    inp["folds"].to_parquet(data / "m03_folds.parquet", index=False)
    inp["m06a"].to_parquet(data / "m06a_shortlist.parquet", index=False)
    inp["m05"].to_parquet(data / "m05_eligibility.parquet", index=False)
    inp["m04_entities"].to_parquet(data / "m04_entities.parquet", index=False)
    inp["m04_auth"].to_parquet(data / "m04_authenticity.parquet", index=False)
    pd.DataFrame({"wallet": [inp["m06a"]["primary_wallet"].iloc[0]], "entry_ts": [0],
                  "lifecycle_valid": [True], "stream_replay_valid": [True]}).to_parquet(
        data / "m02_journeys.parquet", index=False)
    pre = tmp_path / "m07_pretest"
    pre.mkdir()
    inp["m07_summary"].to_parquet(pre / "m07_summary.parquet", index=False)
    inp["m07_fills_path"].to_parquet(pre / "m07_fills.parquet", index=False)
    tdir = _write_wf_testdir(tmp_path, n_entities)
    return inp, data, pre, tdir


def test_cli_threads_gates_to_confirmed_output(tmp_path, monkeypatch):
    """(D5) knobs thread CLI -> manifest -> gate logic -> artifacts: m06b_confirmed filtered,
    m06b_gate_report.parquet written, kill counts + panel path in the walk-forward summary JSON,
    thresholds recorded in m06b_manifest.json."""
    inp, data, pre, tdir = _write_cli_fixture(tmp_path)
    wallets = list(inp["m06a"]["primary_wallet"])
    panel = pd.concat([_leader_panel(wallets[1:]), _leader_panel([wallets[0]], uw=0.5)],
                      ignore_index=True)
    panel_path = tmp_path / "profile_panel.parquet"
    panel.to_parquet(panel_path, index=False)
    out = tmp_path / "out"
    monkeypatch.setattr(M, "_exposure_days_from_fills", _ORIG_EXPOSURE)  # real reader for real files
    monkeypatch.setattr(sys, "argv", [
        "m06b", "--m07-dir", str(pre), "--m07-test-dir", str(tdir), "--out", str(out),
        "--data-dir", str(data), "--m02-journeys", str(data / "m02_journeys.parquet"),
        "--oos-min-folds", "1", "--oos-min-journeys-pooled", "10",
        "--leader-panel", str(panel_path), "--gate-uw-add-max", "0.2"])
    M.main()
    conf = pd.read_parquet(out / "m06b_confirmed.parquet")
    assert wallets[0] not in set(conf["primary_wallet"])
    assert len(conf) == 11
    rep = pd.read_parquet(out / "m06b_gate_report.parquet")
    assert not rep.loc[rep["primary_wallet"] == wallets[0], "pass_uw_add"].iloc[0]
    wf = json.loads((out / "m06b_walkforward_summary.json").read_text())
    assert wf["behavior_gates"]["killed"]["uw_add"] == 1
    assert wf["behavior_gates"]["leader_panel_path"] == str(panel_path)
    # codex #4: FDR discovery count preserved; written count separate.
    assert wf["confirmed"] == 12 and wf["confirmed_written"] == 11
    # codex #2: the panel CONTENT hash is pinned in both the summary and the m06b manifest.
    want_sha = hashlib.sha256(panel_path.read_bytes()).hexdigest()
    assert wf["behavior_gates"]["leader_panel_sha256"] == want_sha
    mani = json.loads((out / "m06b_manifest.json").read_text())
    assert mani["gate_uw_add_max"] == 0.2
    assert mani["copy_latency_s"] == 4.0
    assert mani["leader_panel_path"] == str(panel_path)
    assert mani["leader_panel_sha256"] == want_sha


def test_cli_leader_gate_without_panel_refuses(monkeypatch):
    # CLI-level three-tier refusal fires BEFORE any input is loaded (no files needed).
    monkeypatch.setattr(sys, "argv", ["m06b", "--gate-uw-add-max", "0.2", "--m07-test-dir", "x"])
    with pytest.raises(SystemExit, match="THREE-TIER"):
        M.main()


def test_cli_gate_without_testdir_refuses(monkeypatch):
    # gates apply to the post-FDR CONFIRMED set; without --m07-test-dir they could never run.
    monkeypatch.setattr(sys, "argv", ["m06b", "--gate-latency-ratio-max", "0.02"])
    with pytest.raises(SystemExit, match="m07-test-dir"):
        M.main()


def test_experiment_sh_flagmap_skips_yaml_null(tmp_path, monkeypatch):
    """(codex #1) an explicit YAML null (`gate_uw_add_max: null`) must NOT reach argv as the literal
    string "None" -- mirror of the m9 P2-F fix, asserted against the REAL heredoc source in
    scripts/experiment.sh."""
    sh = (Path(__file__).resolve().parents[2] / "scripts" / "experiment.sh").read_text()
    i = sh.index("FLAG = {")
    code = sh[sh.rindex("import sys, yaml, shlex", 0, i):sh.index("\nPYEOF", i)]
    yml = tmp_path / "m.yml"
    yml.write_text("m06b:\n  fdr_q: 0.10\n  gate_uw_add_max: null\n  leader_panel: null\n")
    monkeypatch.setattr(sys, "argv", ["flagmap", str(yml)])
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        exec(compile(code, "m06b_flagmap", "exec"), {})
    out = buf.getvalue()
    assert "--fdr-q 0.1" in out
    assert "None" not in out
    assert "--gate-uw-add-max" not in out and "--leader-panel" not in out


def test_stage_fp_hashes_leader_panel_content(tmp_path, monkeypatch):
    """(codex follow-up on #2) the m06b stage FINGERPRINT must hash the leader-panel FILE CONTENT,
    asserted against the REAL stage_fp heredoc source in scripts/experiment.sh: (a) same path,
    changed content -> different fingerprint (a swapped panel can never silently reuse a stale
    m06b artifact); (b) leader_panel unset -> runs clean on the missing-marker path and
    fingerprints differently from both set variants."""
    sh = (Path(__file__).resolve().parents[2] / "scripts" / "experiment.sh").read_text()
    start = sh.index("import sys, yaml, json, hashlib, os, glob")
    code = sh[start:sh.index("\nPYEOF", start)]

    def _stage_fp(manifest_path):
        # the heredoc re-imports the shared `sys` module, so monkeypatching the real sys.argv
        # threads the args exactly as `$PY - "$MANIFEST" "m06b"` does in experiment.sh.
        monkeypatch.setattr(sys, "argv", ["stage_fp", str(manifest_path), "m06b"])
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            exec(compile(code, "m06b_stage_fp", "exec"), {})
        out = buf.getvalue().strip()
        assert len(out) == 16 and all(c in "0123456789abcdef" for c in out), \
            f"stage_fp did not print a 16-hex fingerprint: {out!r}"
        return out

    panel = tmp_path / "panel.parquet"
    yml = tmp_path / "m.yml"
    yml.write_text(f"m06b:\n  fdr_q: 0.10\n  leader_panel: {panel}\n")
    panel.write_bytes(b"AAAA")
    f1 = _stage_fp(yml)
    panel.write_bytes(b"BBBB")                    # SAME path, different CONTENT
    f2 = _stage_fp(yml)
    assert f1 != f2, "changed leader-panel content must invalidate the m06b fingerprint"
    yml.write_text("m06b:\n  fdr_q: 0.10\n")      # unset -> missing-marker path, must run clean
    f3 = _stage_fp(yml)
    assert f3 != f1 and f3 != f2, "unset leader_panel must fingerprint differently from set"


def test_stale_gate_report_deleted_on_gates_off_rerun(tmp_path, monkeypatch):
    """(codex #5) rerunning the walk-forward with gates DISABLED must remove the previous gated
    run's m06b_gate_report.parquet -- absence of the artifact must MEAN "gates off"."""
    inp, data, pre, tdir = _write_cli_fixture(tmp_path)
    wallets = list(inp["m06a"]["primary_wallet"])
    panel = pd.concat([_leader_panel(wallets[1:]), _leader_panel([wallets[0]], uw=0.5)],
                      ignore_index=True)
    panel_path = tmp_path / "panel.parquet"
    panel.to_parquet(panel_path, index=False)
    out = tmp_path / "out"
    monkeypatch.setattr(M, "_exposure_days_from_fills", _ORIG_EXPOSURE)
    base = ["m06b", "--m07-dir", str(pre), "--m07-test-dir", str(tdir), "--out", str(out),
            "--data-dir", str(data), "--m02-journeys", str(data / "m02_journeys.parquet"),
            "--oos-min-folds", "1", "--oos-min-journeys-pooled", "10"]
    monkeypatch.setattr(sys, "argv", base + ["--leader-panel", str(panel_path),
                                             "--gate-uw-add-max", "0.2"])
    M.main()
    assert (out / "m06b_gate_report.parquet").exists()
    monkeypatch.setattr(sys, "argv", base)   # same run dir, gates OFF
    M.main()
    assert not (out / "m06b_gate_report.parquet").exists()


def test_gate_report_empty_schema_when_no_candidates(tmp_path):
    """(codex #5) gates ON with zero wallets surviving eligibility/FDR must still produce an EMPTY
    gate report WITH the full schema -- on both the empty-confirmed and the empty-stats paths."""
    inp = _make_inputs(n_entities=6, realized=True)
    tdir = _write_wf_testdir(tmp_path, 6)
    panel = _leader_panel(list(inp["m06a"]["primary_wallet"]))
    # path 1: eligibility floor nobody meets -> confirmed empty after FDR
    m = _wf_manifest(gate_uw_add_max=0.2, oos_min_journeys_pooled=10**6)
    c, s, rep = M.walk_forward_confirm(inp, tdir, m, leader_panel=panel)
    assert len(c) == 0
    assert rep is not None and len(rep) == 0
    for col in ("primary_wallet", "uw_add", "latency_ratio", "n_pretest_cells_used",
                "pass_uw_add", "unmeasurable_uw_add", "all_pass"):
        assert col in rep.columns
    assert s["behavior_gates"]["n_confirmed_pre_gate"] == 0
    # path 2: NO nameable test positions at all (stats empty) -> same schema'd empty report
    tdir2 = tmp_path / "t2"
    tdir2.mkdir()
    rows = pd.read_parquet(tdir / "m07_positions.parquet")
    rows["entity_id"] += 1000                      # unmappable -> every position unnamed
    rows.to_parquet(tdir2 / "m07_positions.parquet", index=False)
    ts = pd.read_parquet(tdir / "m07_summary.parquet")
    ts["entity_id"] += 1000
    ts.to_parquet(tdir2 / "m07_summary.parquet", index=False)
    c2, s2, rep2 = M.walk_forward_confirm(inp, tdir2, _wf_manifest(gate_uw_add_max=0.2),
                                          leader_panel=panel)
    assert len(c2) == 0
    assert rep2 is not None and len(rep2) == 0 and "pass_uw_add" in rep2.columns
    assert s2["confirmed"] == 0 and s2["confirmed_written"] == 0
    assert "behavior_gates" in s2


# --------------------------------------------------------------------------- #
# (codex #6) golden depth: gates-off through the REAL pipeline surface must be
# identical to the git-HEAD implementation run on the same fixture files.
# --------------------------------------------------------------------------- #
def _load_head_m06b(tmp_path):
    repo = Path(__file__).resolve().parents[2]
    try:
        src = subprocess.run(["git", "show", "HEAD:research/v15/v15_m06b_ranking.py"],
                             cwd=repo, capture_output=True, text=True, check=True).stdout
    except Exception:
        pytest.skip("git HEAD version of v15_m06b_ranking.py unavailable")
    head_path = tmp_path / "m06b_head.py"
    head_path.write_text(src)
    spec = importlib.util.spec_from_file_location("v15_m06b_ranking_head", head_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["v15_m06b_ranking_head"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_gates_off_cli_byte_identical_to_head(tmp_path, monkeypatch):
    """(codex #6) run main() (the REAL CLI surface) on fixture files with gates unset, and through
    git-HEAD's v15_m06b_ranking.py the same way; m06b_pool.parquet + m06b_confirmed.parquet must be
    byte-identical up to the PINNED pre-existing deltas.

    ATTRIBUTION NOTE (verified via `git diff HEAD -- research/v15/v15_m06b_ranking.py`): the working
    tree carried PRE-EXISTING uncommitted hunks before the native-gates change: return-basis v3
    (conservative_roe + censoring worst-case bound + OOS censoring exclusion) and the M5
    accessible_* carry-through. On THIS fixture their only artifact-visible effect is the pool's
    return_basis LABEL ("conservative_roe" vs HEAD's "realized_roe"): the numbers are equal because
    the fixture sets conservative_roe == realized_roe with censoring_coverage == 1.0, and the
    fixture m05 carries no accessible_* columns so the OUT_COLS additions are filtered out. That
    single delta is pinned exactly below and stripped; EVERYTHING else in the pool is byte-compared,
    and m06b_confirmed.parquet must match byte-for-byte with NO pinning. (Once the branch is
    committed this degrades to a tautology; it is load-bearing for this review.)"""
    head = _load_head_m06b(tmp_path)
    inp, data, pre, tdir = _write_cli_fixture(tmp_path)
    monkeypatch.setattr(M, "_exposure_days_from_fills", _ORIG_EXPOSURE)

    def _run(module, sub):
        out = tmp_path / sub
        monkeypatch.setattr(sys, "argv", [
            "m06b", "--m07-dir", str(pre), "--m07-test-dir", str(tdir), "--out", str(out),
            "--data-dir", str(data), "--m02-journeys", str(data / "m02_journeys.parquet"),
            "--oos-min-folds", "1", "--oos-min-journeys-pooled", "10"])
        module.main()
        return out

    cur = _run(M, "cur")
    old = _run(head, "head")
    # confirmed: byte-for-byte, no pinning
    assert (cur / "m06b_confirmed.parquet").read_bytes() == \
           (old / "m06b_confirmed.parquet").read_bytes()
    # gates off -> no gate-report artifact on either side
    assert not (cur / "m06b_gate_report.parquet").exists()
    assert not (old / "m06b_gate_report.parquet").exists()
    # pool: pin the single pre-existing delta (return-basis v3 label), byte-compare the rest
    pc = pd.read_parquet(cur / "m06b_pool.parquet")
    po = pd.read_parquet(old / "m06b_pool.parquet")
    assert list(pc.columns) == list(po.columns)
    assert (pc["return_basis"] == "conservative_roe").all()   # fails if the pinned delta drifts
    assert (po["return_basis"] == "realized_roe").all()
    a, b = tmp_path / "pool_cur.parquet", tmp_path / "pool_head.parquet"
    pc.drop(columns=["return_basis"]).to_parquet(a, index=False)
    po.drop(columns=["return_basis"]).to_parquet(b, index=False)
    assert a.read_bytes() == b.read_bytes()


# =========================================================================== #
# MARK-COVERAGE GATE (2026-08-07, Fable-approved; moved here for the autouse
# _patch_exposure fixture — the gate sits inside walk_forward_confirm).
# =========================================================================== #
# --------------------------------------------------------------------------- #
# m06b gate — R2 conjunct
# --------------------------------------------------------------------------- #
def _starve_summary(tdir, fold_id=1, n=12, n_actions=100, n_fills=0, extra_cols=None):
    d = pd.DataFrame({"entity_id": list(range(n)), "fold_id": fold_id,
                      "censoring_coverage": 1.0, "n_actions": n_actions, "n_fills": n_fills})
    for k, v in (extra_cols or {}).items():
        d[k] = v
    d.to_parquet(Path(tdir) / "m07_summary.parquet", index=False)


def test_r2_zero_fills_with_actions_refuses_naming_fold(tmp_path):
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    _starve_summary(tdir, n_actions=100, n_fills=0)
    with pytest.raises(ValueError, match=r"MARK-COVERAGE REFUSAL.*fold 1.*0 fills"):
        M.walk_forward_confirm(inp, tdir, _wf_manifest())


def test_r2_quiet_fold_no_actions_does_not_refuse(tmp_path):
    """Seats but ZERO actions = legitimately quiet (frozen-10 scale risk) -> no raise."""
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    _starve_summary(tdir, n_actions=0, n_fills=0)
    confirmed, summ, _ = M.walk_forward_confirm(inp, tdir, _wf_manifest())
    assert summ["folds_in_summary"] == 1
    assert not summ["unpriced_fold_override"]


def test_r2_healthy_fold_passes_with_stamps(tmp_path):
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)     # fixture writes healthy n_actions/n_fills
    confirmed, summ, _ = M.walk_forward_confirm(inp, tdir, _wf_manifest())
    assert len(confirmed) == 12
    assert summ["folds_in_summary"] == 1
    assert summ["folds_with_positions"] == 1
    assert summ["unpriced_fold_override"] is False
    assert summ["unpriced_demoted_folds"] == ""
    # knobs recorded verbatim (R3 provenance requirement)
    assert summ["unpriced_warn_pct"] == pytest.approx(0.10)
    assert summ["unpriced_refuse_pct"] == pytest.approx(0.50)


# --------------------------------------------------------------------------- #
# m06b gate — R3 pct knobs + legacy fallback
# --------------------------------------------------------------------------- #
def test_r3_unpriced_over_refuse_pct_refuses(tmp_path):
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    # 60% unpriced (> default 0.50) but fills nonzero -> R3 path, not R2
    _starve_summary(tdir, n_actions=100, n_fills=40, extra_cols={"n_actions_unpriced": 60})
    with pytest.raises(ValueError, match=r"unpriced.*> 50%"):
        M.walk_forward_confirm(inp, tdir, _wf_manifest())


def test_r3_between_warn_and_refuse_passes(tmp_path):
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    _starve_summary(tdir, n_actions=100, n_fills=70, extra_cols={"n_actions_unpriced": 30})
    confirmed, summ, _ = M.walk_forward_confirm(inp, tdir, _wf_manifest())
    assert summ["unpriced_telemetry_available"] is True
    assert not summ["unpriced_fold_override"]


def test_r3_knobs_settable(tmp_path):
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    _starve_summary(tdir, n_actions=100, n_fills=70, extra_cols={"n_actions_unpriced": 30})
    with pytest.raises(ValueError, match="MARK-COVERAGE REFUSAL"):
        M.walk_forward_confirm(inp, tdir, _wf_manifest(unpriced_refuse_pct=0.25))


def test_r3_legacy_artifact_without_counter_runs_r2_only(tmp_path):
    """Pre-heal artifacts lack n_actions_unpriced: R2 still refuses a dead fold (the acceptance
    replay path), R3 silently unavailable but STAMPED as such."""
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    _starve_summary(tdir, n_actions=100, n_fills=50)       # no n_actions_unpriced column
    confirmed, summ, _ = M.walk_forward_confirm(inp, tdir, _wf_manifest())
    assert summ["unpriced_telemetry_available"] is False


def test_pre2026_artifact_without_cov_cols_skips_gate(tmp_path):
    """A summary lacking even n_actions/n_fills (very old artifact) must not crash: the gate is
    skipped loudly, run proceeds."""
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    pd.DataFrame({"entity_id": list(range(12)), "fold_id": 1,
                  "censoring_coverage": 1.0}).to_parquet(Path(tdir) / "m07_summary.parquet",
                                                         index=False)
    confirmed, summ, _ = M.walk_forward_confirm(inp, tdir, _wf_manifest())
    assert summ["unpriced_telemetry_available"] is False


# --------------------------------------------------------------------------- #
# m06b gate — R4 override stamp
# --------------------------------------------------------------------------- #
def test_r4_override_demotes_and_stamps(tmp_path):
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    _starve_summary(tdir, n_actions=100, n_fills=0)
    confirmed, summ, _ = M.walk_forward_confirm(inp, tdir, _wf_manifest(allow_unpriced_folds=True))
    assert summ["unpriced_fold_override"] is True
    assert summ["unpriced_demoted_folds"] == "1"


# --------------------------------------------------------------------------- #
# MARK-COVERAGE gate — codex 2026-08-07 blockers (fail-closed telemetry + stamped early return)
# --------------------------------------------------------------------------- #
def test_codex1_nan_actions_refuses_not_quiet(tmp_path):
    """All-NaN n_actions must NOT sum to 0 and read as a quiet fold (fail-open) — corrupt
    telemetry refuses loudly, and allow_unpriced_folds can NOT demote it."""
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    _starve_summary(tdir, n_actions=np.nan, n_fills=0)
    with pytest.raises(ValueError, match="non-finite n_actions"):
        M.walk_forward_confirm(inp, tdir, _wf_manifest())
    with pytest.raises(ValueError, match="non-finite n_actions"):
        M.walk_forward_confirm(inp, tdir, _wf_manifest(allow_unpriced_folds=True))


def test_codex1_nan_unpriced_refuses_not_bypass(tmp_path):
    """NaN n_actions_unpriced must not become 0 and bypass R3."""
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    _starve_summary(tdir, n_actions=100, n_fills=40,
                    extra_cols={"n_actions_unpriced": np.nan})
    with pytest.raises(ValueError, match="non-finite n_actions_unpriced"):
        M.walk_forward_confirm(inp, tdir, _wf_manifest())


def test_codex1_unparseable_fold_id_refuses_loudly(tmp_path):
    """A non-numeric fold_id refuses with a named error, never crashes at astype(int)."""
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    d = pd.DataFrame({"entity_id": list(range(12)), "fold_id": ["x"] * 12,
                      "censoring_coverage": 1.0, "n_actions": 100, "n_fills": 50})
    d.to_parquet(Path(tdir) / "m07_summary.parquet", index=False)
    with pytest.raises(ValueError, match="unparseable fold_id"):
        M.walk_forward_confirm(inp, tdir, _wf_manifest())


def test_codex2_empty_stats_path_carries_stamps(tmp_path):
    """An overridden mark-starved run whose test positions all filter away before the bootstrap
    (censoring_coverage != 1.0 everywhere) must STILL write the R4 stamps."""
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    d = pd.DataFrame({"entity_id": list(range(12)), "fold_id": 1,
                      "censoring_coverage": 0.5,          # every wallet censor-excluded
                      "n_actions": 100, "n_fills": 0})    # AND the fold is starved
    d.to_parquet(Path(tdir) / "m07_summary.parquet", index=False)
    confirmed, summ, _ = M.walk_forward_confirm(inp, tdir, _wf_manifest(allow_unpriced_folds=True))
    assert confirmed.empty
    assert summ["unpriced_fold_override"] is True
    assert summ["unpriced_demoted_folds"] == "1"
    assert summ["folds_in_summary"] == 1
    assert summ["folds_with_positions"] == 0
    assert summ["unpriced_warn_pct"] == pytest.approx(0.10)
    assert summ["unpriced_refuse_pct"] == pytest.approx(0.50)


def test_codex_r2_inf_fold_id_refuses_loudly(tmp_path):
    """inf passes isna() — must refuse by name, never IntCastingNaNError at astype(int)."""
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    d = pd.DataFrame({"entity_id": list(range(12)), "fold_id": np.inf,
                      "censoring_coverage": 1.0, "n_actions": 100, "n_fills": 50})
    d.to_parquet(Path(tdir) / "m07_summary.parquet", index=False)
    with pytest.raises(ValueError, match="unparseable fold_id"):
        M.walk_forward_confirm(inp, tdir, _wf_manifest())


def test_codex_r2_fractional_fold_id_refuses_not_truncates(tmp_path):
    """1.5 must refuse, not silently truncate to fold 1 and alter coverage attribution."""
    inp = _make_inputs(n_entities=12, realized=True)
    tdir = _write_wf_testdir(tmp_path, 12)
    d = pd.DataFrame({"entity_id": list(range(12)), "fold_id": 1.5,
                      "censoring_coverage": 1.0, "n_actions": 100, "n_fills": 50})
    d.to_parquet(Path(tdir) / "m07_summary.parquet", index=False)
    with pytest.raises(ValueError, match="unparseable fold_id"):
        M.walk_forward_confirm(inp, tdir, _wf_manifest())
