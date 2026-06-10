"""V15 M6b ranking tests -- frozen-spec mechanics on synthetic inputs.

Design: brain projects/quant/v15/modules/m06b. Run:
  /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_m06b.py -q
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m06b_ranking as M  # noqa: E402


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
                 tracking_error=None, realized=False):
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
                       realized=True)
    out, manifest = M.build_ranking(inp, m)
    assert manifest["consistency_source"] == "m07_equity_block_roe"
    assert manifest["fidelity_source"] == "m07_tracking_error"
    assert manifest["return_basis"] == "realized_roe"
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


def test_ranking_uses_realized_roe_not_engine():
    # roe_adj must equal realized_roe (the rank basis), NOT full-window roe_engine.
    m = M.M6bManifest()
    inp = _make_inputs(n_entities=120, realized=True)
    summ = inp["m07_summary"]
    ref = summ[["entity_id", "realized_roe", "roe_engine"]].copy()
    out, manifest = M.build_ranking(inp, m)
    assert manifest["return_basis"] == "realized_roe"
    merged = out[["entity_id", "roe_adj"]].merge(ref, on="entity_id")
    # rank basis == realized_roe
    assert np.allclose(merged["roe_adj"], merged["realized_roe"], equal_nan=True)
    # and it is NOT the engine value (the two columns were drawn from different distributions)
    assert not np.allclose(merged["realized_roe"], merged["roe_engine"])


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
