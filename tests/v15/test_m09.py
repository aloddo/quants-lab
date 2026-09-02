"""V15 M9 allocation-core tests: sizing chain, anti-corr, min-notional feasibility, cap-aware water-fill.

Run: /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_m09.py -q
"""
import importlib.util
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import v15_m09_sim as M9  # noqa: E402
import v15_m07_engine as E  # noqa: E402
import test_m07 as m07t  # noqa: E402  (real-engine fixtures: FakeMarketData, _flat_ohlc, _action, T0)

M = M9.M9Manifest()


def test_matched_null_provider_requires_explicit_seed():
    with pytest.raises(ValueError, match="explicit seed"):
        M9.run_m09_chained(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            eng=None, md=None, acts_loader=None, m=M, b0=500.0,
            pool_provider="matched_null",
        )


def test_unknown_pool_provider_fails_closed():
    with pytest.raises(NotImplementedError, match="unsupported"):
        M9.run_m09_chained(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            eng=None, md=None, acts_loader=None, m=M, b0=500.0,
            pool_provider="anything_else", seed=1,
        )


def test_sizing_chain_is_product():
    assert M9.sizing_chain_weight(0.5, 0.25, 0.1) == pytest.approx(0.0125)
    assert M9.sizing_chain_weight(1.0, 1.0, 1.0) == 1.0


def test_anti_corr_keeps_higher_ranked():
    # entities 1 (priority 10) and 2 (priority 5) correlate >0.7 -> keep 1, drop 2. 3 uncorrelated kept.
    cand = pd.DataFrame([
        {"entity_id": 1, "select_priority": 10.0},
        {"entity_id": 2, "select_priority": 5.0},
        {"entity_id": 3, "select_priority": 7.0},
    ])
    corr = {(1, 2): 0.9, (1, 3): 0.1, (2, 3): 0.1}
    out = M9.anti_corr_select(cand, corr, M)
    sel = set(out[out.anti_corr_selected].entity_id)
    assert sel == {1, 3}                    # 2 dropped (correlated with higher-ranked 1)
    assert out[out.entity_id == 2].iloc[0]["dropped_reason"].startswith("corr>")


def test_anti_corr_keeps_negative_correlation():
    # codex r1#1: a strongly NEGATIVE correlation (-0.9) is diversifying -> KEEP both; +0.9 drops.
    cand = pd.DataFrame([{"entity_id": 1, "select_priority": 10.0}, {"entity_id": 2, "select_priority": 5.0}])
    out_neg = M9.anti_corr_select(cand, {(1, 2): -0.9}, M)
    assert set(out_neg[out_neg.anti_corr_selected].entity_id) == {1, 2}
    out_pos = M9.anti_corr_select(cand, {(1, 2): 0.9}, M)
    assert set(out_pos[out_pos.anti_corr_selected].entity_id) == {1}


def test_anti_corr_respects_target_count_ceiling():
    m = M9.M9Manifest()
    object.__setattr__(m, "target_count", 2) if False else None
    m2 = M9.M9Manifest(target_count=2)
    cand = pd.DataFrame([{"entity_id": i, "select_priority": 10 - i} for i in range(5)])
    out = M9.anti_corr_select(cand, {}, m2)
    assert int(out.anti_corr_selected.sum()) == 2   # only top-2 by priority


def test_min_notional_feasibility_at_capital():
    # action exposure 5% of slice; at $500 slice a 5% action = $25 >= $10 -> feasible.
    ok, need, _ = M9.min_notional_feasible(0.05, 0.9, 500.0, M)
    assert ok and need == pytest.approx(200.0)         # 10/0.05
    # at $100 slice a 5% action = $5 < $10 -> NOT feasible
    bad, need2, reason = M9.min_notional_feasible(0.05, 0.9, 100.0, M)
    assert not bad and "min_notional" in reason
    # low accessible_frac -> infeasible even if slice ok
    bad2, _, r2 = M9.min_notional_feasible(0.05, 0.2, 500.0, M)
    assert not bad2 and "accessible_frac" in r2
    # codex r1#3: UNKNOWN accessible_frac (None/NaN) -> infeasible (can't prove min-notional clearance)
    bad3, _, r3 = M9.min_notional_feasible(0.05, None, 500.0, M)
    assert not bad3 and r3 == "accessible_frac_unknown"
    bad4, _, _ = M9.min_notional_feasible(0.05, float("nan"), 500.0, M)
    assert not bad4
    # codex r1#2: frac<=0 -> infeasible with inf viable slice
    bad5, need5, r5 = M9.min_notional_feasible(0.0, 0.9, 500.0, M)
    assert not bad5 and need5 == float("inf") and "<=0" in r5


def test_waterfill_respects_per_entity_cap_no_overflow():
    # two entities, equal desire, cash 100, each capped at 30 residual -> 30+30 funded, 40 stays cash.
    res = M9.cap_aware_waterfill(
        desired={1: 0.5, 2: 0.5}, carried_exposure={1: 0.0, 2: 0.0},
        caps={1: 30.0, 2: 30.0}, m=M, cash_available=100.0, portfolio_equity=100.0)
    assert res["per_entity"][1]["cash_funded"] == pytest.approx(30.0)
    assert res["per_entity"][2]["cash_funded"] == pytest.approx(30.0)
    assert res["cash_funded_total"] == pytest.approx(60.0)
    assert res["cash_shortfall"] == pytest.approx(40.0)   # un-fundable due to caps -> stays cash


def test_waterfill_redistributes_freed_cash_to_headroom():
    # entity 1 wants more but capped at 10; entity 2 has headroom -> freed cash flows to 2.
    res = M9.cap_aware_waterfill(
        desired={1: 0.5, 2: 0.5}, carried_exposure={},
        caps={1: 10.0, 2: 1000.0}, m=M, cash_available=100.0, portfolio_equity=100.0)
    assert res["per_entity"][1]["cash_funded"] == pytest.approx(10.0)   # capped
    assert res["per_entity"][2]["cash_funded"] == pytest.approx(90.0)   # got the rest
    assert res["cash_shortfall"] == pytest.approx(0.0)


def test_waterfill_carried_exposure_reduces_residual():
    # entity 1 cap 50 but already carries 45 -> only 5 residual; winner untouched gets little new.
    res = M9.cap_aware_waterfill(
        desired={1: 1.0}, carried_exposure={1: 45.0},
        caps={1: 50.0}, m=M, cash_available=100.0, portfolio_equity=100.0)
    assert res["per_entity"][1]["cash_funded"] == pytest.approx(5.0)
    assert res["cash_shortfall"] == pytest.approx(95.0)


def test_effective_caps_min_of_g7_and_m8():
    ent = pd.DataFrame([
        {"entity_id": 1, "max_survivable_slice": 50.0},     # M8 clamp tighter than G7
        {"entity_id": 2, "max_survivable_slice": np.inf},   # G7 binds
    ])
    caps = M9.effective_caps(ent, M, portfolio_equity=1000.0)
    assert caps[1] == pytest.approx(50.0)                    # min(0.4*1000=400, 50)
    assert caps[2] == pytest.approx(400.0)                   # min(400, inf)
    # codex r1#4: M8 max_survivable_slice=0 (capped out) -> cap 0 (NOT G7); negative clamps to 0.
    ent2 = pd.DataFrame([{"entity_id": 3, "max_survivable_slice": 0.0},
                         {"entity_id": 4, "max_survivable_slice": -5.0}])
    caps2 = M9.effective_caps(ent2, M, portfolio_equity=1000.0)
    assert caps2[3] == 0.0 and caps2[4] == 0.0


def test_portfolio_path_merges_subaccounts_stepwise():
    # entity 1: 100@t0, 120@t2; entity 2: 50@t1. Portfolio = forward-filled sum on the merged clock.
    sub = {
        1: pd.DataFrame({"ts": [0, 2], "equity": [100.0, 120.0]}),
        2: pd.DataFrame({"ts": [1], "equity": [50.0]}),
    }
    pp = M9.portfolio_path(sub).set_index("ts")["portfolio_equity"]
    assert pp[0] == pytest.approx(100.0)         # only entity 1 active
    assert pp[1] == pytest.approx(150.0)         # 100 (held) + 50
    assert pp[2] == pytest.approx(170.0)         # 120 + 50 (held)


def test_portfolio_path_order_independent():
    sub = {1: pd.DataFrame({"ts": [0, 5], "equity": [100.0, 90.0]}),
           2: pd.DataFrame({"ts": [3], "equity": [40.0]})}
    a = M9.portfolio_path(sub)
    b = M9.portfolio_path({2: sub[2], 1: sub[1]})   # reversed iteration
    pd.testing.assert_frame_equal(a.sort_values("ts").reset_index(drop=True),
                                  b.sort_values("ts").reset_index(drop=True))


def test_portfolio_path_same_ts_duplicate_takes_min_deterministic():
    # codex r1#1: same-ts duplicate samples within a subaccount collapse to MIN (conservative, never
    # hides a dip) and are order-independent. entity 1 has 100 AND 40 at t1 -> uses 40.
    sub_a = {1: pd.DataFrame({"ts": [0, 1, 1, 2], "equity": [100.0, 100.0, 40.0, 110.0]})}
    sub_b = {1: pd.DataFrame({"ts": [0, 1, 1, 2], "equity": [100.0, 40.0, 100.0, 110.0]})}  # rows reordered
    pa = M9.portfolio_path(sub_a).set_index("ts")["portfolio_equity"]
    pb = M9.portfolio_path(sub_b).set_index("ts")["portfolio_equity"]
    assert pa[1] == pytest.approx(40.0) and pb[1] == pytest.approx(40.0)   # min, regardless of order
    # the same-ts dip is now VISIBLE to G4 (would be hidden if the 100 sample won)
    g4 = M9.g4_intrafold_kill(M9.portfolio_path(sub_a), fold_initial_equity=100.0, m=M)
    assert g4["killed"] is True and g4["kill_ts"] == 1


def test_global_dd_derisk_trigger():
    # peak 100 then drops to 60 (40% DD > 35%) -> de-risk fires at that ts.
    pdf = pd.DataFrame({"ts": [0, 1, 2], "portfolio_equity": [100.0, 80.0, 60.0]})
    ts = M9.detect_global_dd_derisk(pdf, M)
    assert ts == 2                                # 80 is 20% DD (no), 60 is 40% (yes)
    # never breaches -> None
    assert M9.detect_global_dd_derisk(pd.DataFrame({"ts": [0, 1], "portfolio_equity": [100.0, 80.0]}), M) is None


def test_g4_kill_no_hindsight_recovery():
    # dips to 45 (< 50% of 100) at t2 then "recovers" to 120 -> STILL fails G4 (kill froze at t2).
    pdf = pd.DataFrame({"ts": [0, 1, 2, 3], "portfolio_equity": [100.0, 70.0, 45.0, 120.0]})
    r = M9.g4_intrafold_kill(pdf, fold_initial_equity=100.0, m=M)
    assert r["killed"] is True and r["kill_ts"] == 2
    assert r["fold_end_equity"] == pytest.approx(45.0)   # frozen at kill, NOT the 120 recovery
    assert r["g4_pass"] is False


def test_g4_pass_when_never_breaches():
    pdf = pd.DataFrame({"ts": [0, 1, 2], "portfolio_equity": [100.0, 80.0, 90.0]})
    r = M9.g4_intrafold_kill(pdf, fold_initial_equity=100.0, m=M)
    assert r["killed"] is False and r["g4_pass"] is True and r["fold_end_equity"] == pytest.approx(90.0)


# --------------------------------------------------------------------------- #
# Gross-cap / levered-margin budget + chained-sim wiring (anti-corr, cash conservation)
# --------------------------------------------------------------------------- #
def test_apply_gross_budget_caps_aggregate_notional():
    # two entities, 2x leverage each, funded 100 each -> 400 notional. Budget = 300 -> trim NEW to 300.
    funded = {1: 100.0, 2: 100.0}
    lev = {1: 2.0, 2: 2.0}
    out, returned, gross = M9.apply_gross_budget(funded, carried_exposure={}, lev=lev,
                                                 gross_budget_notional=300.0)
    assert gross == pytest.approx(300.0)                       # aggregate notional == budget exactly
    assert sum(out.values()) == pytest.approx(150.0)           # 300 notional / 2x lev = 150 margin
    assert returned == pytest.approx(50.0)                     # 200 funded - 150 deployed
    # per-leader leverage is NOT capped: each still funded pro-rata (75 each), lev untouched.
    assert out[1] == pytest.approx(75.0) and out[2] == pytest.approx(75.0)


def test_apply_gross_budget_carried_consumes_budget_no_force_trim():
    # carried 100 @ 3x = 300 notional already at the budget -> NO new margin funded, carried untouched.
    out, returned, gross = M9.apply_gross_budget(
        funded={2: 100.0}, carried_exposure={1: 100.0}, lev={1: 3.0, 2: 1.0},
        gross_budget_notional=300.0)
    assert out[2] == pytest.approx(0.0)                        # no room for new
    assert returned == pytest.approx(100.0)                    # all new margin returned to cash
    assert gross == pytest.approx(300.0)                       # carried notional preserved (not trimmed)


def test_expected_leverage_fixed_position_counts_concurrent_open_coins():
    actions = pd.DataFrame([
        {"ts": 1, "event_order": 0, "coin": "BTC", "position_after": 1.0},
        {"ts": 2, "event_order": 1, "coin": "ETH", "position_after": -2.0},
        {"ts": 3, "event_order": 2, "coin": "BTC", "position_after": 0.0},
    ])
    assert M9.expected_leverage(actions, "fixed_position", 0.25) == pytest.approx(0.50)


def test_apply_gross_budget_under_budget_passthrough():
    out, returned, gross = M9.apply_gross_budget(
        funded={1: 50.0}, carried_exposure={}, lev={1: 2.0}, gross_budget_notional=1000.0)
    assert out[1] == pytest.approx(50.0) and returned == pytest.approx(0.0) and gross == pytest.approx(100.0)


def test_expected_leverage_uses_abs_target_exposure():
    adf = pd.DataFrame({"target_exposure_pct": [2.0, -3.0, 4.0, -1.0]})
    lev = M9.expected_leverage(adf)
    assert lev >= 1.0 and lev == pytest.approx(pd.Series([2, 3, 4, 1]).quantile(0.75))
    assert M9.expected_leverage(None) == 1.0                   # no actions at all -> unlevered budgeting


def test_expected_leverage_leader_equity_refuses_unusable_input():
    """CONTRACT CHANGE 2026-07-30 (Fable plan gate, fail-loud pass).

    This file previously asserted `expected_leverage(pd.DataFrame({"x": [1]})) == 1.0` -- "missing col
    -> 1.0", i.e. degrade silently to unlevered budgeting. That degradation is now a RAISE, because in
    the no-M1 world it is not a graceful fallback, it is a wrong answer:

      m02_journey_trace derives target_exposure_pct from source_equity_post. With M1 out of scope
      (Alberto 2026-07-17, reconfirmed 2026-07-30 "No M1 no equity") that anchor never exists, so the
      column is permanently NO_ANCHOR/null in EVERY actions store. The old fallback therefore returned
      1.0x leverage for EVERY leader on EVERY real run -- a degenerate sim reported as a normal one.

    `adf is None` still returns 1.0: "no actions supplied" is a genuinely different question from
    "actions supplied but the sizing input is unusable".
    """
    with pytest.raises(ValueError, match="DEPRECATED"):
        M9.expected_leverage(pd.DataFrame({"x": [1]}))                             # column absent
    with pytest.raises(ValueError, match="entirely null"):
        M9.expected_leverage(pd.DataFrame({"target_exposure_pct": [None, None]}))   # present, all null
    # the supported mode is unaffected
    acts = pd.DataFrame({"coin": ["BTC"], "position_after": [1.0]})
    assert M9.expected_leverage(acts, "fixed_position", 0.25) == pytest.approx(0.25)


# ---- chained-sim integration with a tiny fake engine (exercises wiring end-to-end) ----
class _FakePos:
    def __init__(self, szi, entry_px):
        self.szi = szi; self.entry_px = entry_px; self.mode = "cross"


class _FakeState:
    def __init__(self, cross_collateral=None, cooldown_until_ms=0):
        self.cross_collateral = dict(cross_collateral or {})
        self.cooldown_until_ms = cooldown_until_ms
        self.positions = {}


class _FakeEng:
    """Minimal engine stub: each subaccount ends flat at start_eq x (1 + ret). target_exposure_pct in
    the actions drives expected_leverage; the engine itself just returns a deterministic equity path."""
    AccountState = _FakeState
    Position = _FakePos

    def __init__(self, ret_by_eid=None):
        self.ret_by_eid = ret_by_eid or {}

    class EngineParams:
        def __init__(self, slippage_band="base", start_policy="causal_carry_in"):
            self.slippage_band = slippage_band; self.start_policy = start_policy

    def step_subaccount(self, adf, md, start_equity, params, end_ts_ms, start_ts_ms,
                        start_state=None, entity_id=None, fold_id=None):
        ret = self.ret_by_eid.get(entity_id, 0.0)
        end_eq = start_equity * (1.0 + ret)
        return {
            "equity": [{"ts": start_ts_ms, "subaccount_equity": start_equity},
                       {"ts": end_ts_ms, "subaccount_equity": end_eq}],
            "ending_account_state": {"cross_collateral": {"USDC": end_eq}, "cooldown_until_ms": 0,
                                     "positions": {}},
            "summary": {"final_equity": end_eq},
        }


class _StateCheckingEng(_FakeEng):
    """Flat fake that verifies carried top-ups reach AccountState, not only a scalar."""

    def step_subaccount(self, adf, md, start_equity, params, end_ts_ms, start_ts_ms,
                        start_state=None, entity_id=None, fold_id=None):
        if start_state is not None and not start_state.positions:
            assert sum(start_state.cross_collateral.values()) == pytest.approx(start_equity)
        return super().step_subaccount(
            adf, md, start_equity, params, end_ts_ms, start_ts_ms,
            start_state=start_state, entity_id=entity_id, fold_id=fold_id,
        )


def _chained_inputs(extra_wallet=None):
    folds = pd.DataFrame([{"fold_id": 0, "oos_chain_order": 0,
                           "train_start": "2024-12-01", "test_start": "2025-01-01",
                           "pretest_start": "2024-12-01", "pretest_end_excl": "2025-01-01",
                           "test_end_excl": "2025-01-02"}])
    m06b = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "in_pool": True, "quality_weight": 0.5},
        {"entity_id": 2, "fold_id": 0, "in_pool": True, "quality_weight": 0.5},
    ])
    m08 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "survival_multiplier": 1.0, "tier": "ok", "max_survivable_slice": np.inf},
        {"entity_id": 2, "fold_id": 0, "survival_multiplier": 1.0, "tier": "ok", "max_survivable_slice": np.inf},
    ])
    m04 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"},
        {"entity_id": 2, "fold_id": 0, "primary_wallet": "0xbbb" if extra_wallet is None else extra_wallet,
         "entity_tier": "CLEAN"},
    ])
    return m06b, m08, m04, folds


def _acts(target_exp=1.0):
    def loader(wallet, t0, t1):
        if wallet is None:
            return None
        return pd.DataFrame({"target_exposure_pct": [target_exp, target_exp]})
    return loader


def test_chained_aggregate_gross_notional_within_budget(tmp_path):
    # High leverage so the AGGREGATE gross cap BINDS (not just the per-entity caps). b0=500,
    # gross_cap=3 -> notional budget 1500. With per_entity_cap=1.0 the water-fill would deploy ~500
    # margin; at 8x that is 4000 notional >> 1500 -> the gross trim MUST bind and clamp to 1500.
    m06b, m08, m04, folds = _chained_inputs()
    eng = _FakeEng()
    m = M9.M9Manifest(per_entity_cap=1.0)                   # relax per-entity so gross is the binding cap
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=_acts(8.0),
                             m=m, b0=500.0, out_dir=str(tmp_path))
    fc = out["fold_caps_applied"][0]
    assert fc["implied_gross_notional"] <= m.gross_cap * 500.0 + 1e-6
    assert fc["implied_gross_notional"] == pytest.approx(1500.0)   # gross cap binds exactly
    assert fc["gross_trimmed_cash"] > 0                            # trim actually fired


def test_chained_anti_corr_pruning_applied(tmp_path):
    # entities 1 & 2 correlate 0.9 -> only the higher select_priority is funded (n_selected == 1).
    m06b, m08, m04, folds = _chained_inputs()
    m06b.loc[m06b.entity_id == 1, "quality_weight"] = 0.8   # higher priority -> kept
    m06b.loc[m06b.entity_id == 2, "quality_weight"] = 0.2   # pruned
    eng = _FakeEng()
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=_acts(1.0),
                             m=M9.M9Manifest(), b0=500.0, corr={(1, 2): 0.9}, out_dir=str(tmp_path))
    assert out["per_fold"][0]["n_selected"] == 1            # entity 2 anti-corr pruned


def test_chained_unrunnable_entity_cash_returned(tmp_path):
    # entity 2 has no wallet (None) -> its funded slice must be RETURNED to cash (conservation). With a
    # flat (0 return) engine and no leverage, final equity must equal b0 (nothing lost to the leak).
    m06b, m08, m04, folds = _chained_inputs(extra_wallet=None)
    m04.loc[m04.entity_id == 2, "primary_wallet"] = None
    eng = _FakeEng(ret_by_eid={1: 0.0})
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=_acts(1.0),
                             m=M9.M9Manifest(), b0=500.0, out_dir=str(tmp_path))
    assert out["final_equity"] == pytest.approx(500.0)      # no cash leaked by the unrunnable entity


def test_fixed_position_does_not_require_source_size_accessibility(tmp_path):
    m06b, m08, m04, folds = _chained_inputs()
    # The no-M1 fixed-position lane has no source-size accessibility estimate. Follower orders are
    # nevertheless known to be the fixed sleeve exposure and must use that actual execution contract.
    m06b["accessible_frac_notional"] = np.nan
    eng = _FakeEng()
    out = M9.run_m09_chained(
        m06b, m08, m04, folds, eng, md=None, acts_loader=_acts(1.0),
        m=M9.M9Manifest(sizing_mode="fixed_position", fixed_target_exposure=1.0),
        b0=500.0, out_dir=str(tmp_path),
    )
    assert out["fold_caps_applied"][0]["n_min_notional_dropped"] == 0
    assert out["fold_caps_applied"][0]["n_accessible_unchecked"] == 0


def test_manifest_frozen_knobs():
    m = M9.M9Manifest()
    assert m.b0 == 500.0 and m.gross_cap == 3.0 and m.global_dd_derisk == 0.35
    assert m.rho_max == 0.70 and m.per_entity_cap == 0.40 and m.suspicious_cohort_cap == 0.10
    assert m.idle_cash_return == 0.0


# --------------------------------------------------------------------------- #
# Codex-flagged fixes: (a) no OOS look-ahead in leverage, (b) carried top-up cash conservation,
# (c) gross budget = b0 x gross_cap fixed across folds, (d) G4 kill flattens carried.
# --------------------------------------------------------------------------- #
def test_expected_leverage_uses_pretest_not_test_actions(tmp_path):
    # (a) NO LOOK-AHEAD: the allocation leverage must come from the PRETEST window [train_start,test_start)
    # only. Loader returns lev=1.0 over pretest but a huge lev over the test fold; the gross budget must
    # be sized off the pretest (1.0), so a single 500-margin position implies ~500 notional (1x), well
    # under the 1500 budget -> NO gross trim. If the test-fold leverage (50x) had leaked in, the budget
    # would bind hard and trim fire.
    m06b, m08, m04, folds = _chained_inputs()
    # keep just one entity so sizing is unambiguous
    m06b = m06b[m06b.entity_id == 1].copy()
    m08 = m08[m08.entity_id == 1].copy()
    m04 = m04[m04.entity_id == 1].copy()
    test_start_ms = pd.Timestamp("2025-01-01").value // 1_000_000

    def loader(wallet, t0, t1):
        if wallet is None:
            return None
        # t0 < test_start => pretest window (causal). t0 >= test_start => test fold.
        exp = 1.0 if t0 < test_start_ms else 50.0
        return pd.DataFrame({"target_exposure_pct": [exp, exp]})

    eng = _FakeEng()
    m = M9.M9Manifest(per_entity_cap=1.0)
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=loader,
                             m=m, b0=500.0, out_dir=str(tmp_path))
    fc = out["fold_caps_applied"][0]
    # pretest leverage 1.0 -> implied gross ~= margin deployed (<=500) << 1500 budget -> NO trim.
    assert fc["gross_trimmed_cash"] == pytest.approx(0.0)
    assert fc["implied_gross_notional"] <= 500.0 + 1e-6
    # sanity: had the 50x test-fold leverage leaked in, implied gross would be ~25000 and trim would fire.


def test_chained_carried_topup_cash_conserved(tmp_path):
    # (b) CASH CONSERVATION with a carried top-up across two folds. A winner carried into fold 1 may get a
    # water-fill top-up; that margin must enter the carried subaccount (not vanish). With a flat (0-return)
    # engine and no leverage, total equity must be conserved exactly == b0 every step, across BOTH folds.
    folds = pd.DataFrame([
        {"fold_id": 0, "oos_chain_order": 0, "train_start": "2024-12-01", "pretest_start": "2024-12-01",
         "pretest_end_excl": "2025-01-01", "test_start": "2025-01-01", "test_end_excl": "2025-01-02"},
        {"fold_id": 1, "oos_chain_order": 1, "train_start": "2024-12-02", "pretest_start": "2024-12-02",
         "pretest_end_excl": "2025-01-02", "test_start": "2025-01-02", "test_end_excl": "2025-01-03"},
    ])
    # entity 1 in both folds (carried); entity 2 only in fold 1 (so fold-1 water-fill re-runs + tops up 1).
    m06b = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "in_pool": True, "quality_weight": 0.5},
        {"entity_id": 1, "fold_id": 1, "in_pool": True, "quality_weight": 0.5},
        {"entity_id": 2, "fold_id": 1, "in_pool": True, "quality_weight": 0.5},
    ])
    m08 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "survival_multiplier": 1.0, "tier": "ok", "max_survivable_slice": np.inf},
        {"entity_id": 1, "fold_id": 1, "survival_multiplier": 1.0, "tier": "ok", "max_survivable_slice": np.inf},
        {"entity_id": 2, "fold_id": 1, "survival_multiplier": 1.0, "tier": "ok", "max_survivable_slice": np.inf},
    ])
    m04 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"},
        {"entity_id": 1, "fold_id": 1, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"},
        {"entity_id": 2, "fold_id": 1, "primary_wallet": "0xbbb", "entity_tier": "CLEAN"},
    ])
    eng = _StateCheckingEng(ret_by_eid={1: 0.0, 2: 0.0})  # checks state-level conservation too
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=_acts(1.0),
                             m=M9.M9Manifest(per_entity_cap=1.0), b0=500.0, out_dir=str(tmp_path))
    assert out["final_equity"] == pytest.approx(500.0)    # no cash leaked by the carried top-up


def test_chained_state_keys_on_wallet_not_fold_local_entity_id(tmp_path):
    class RecordingEng(_FakeEng):
        def __init__(self):
            super().__init__()
            self.start_states = {}

        def step_subaccount(self, adf, md, start_equity, params, end_ts_ms, start_ts_ms,
                            start_state=None, entity_id=None, fold_id=None):
            self.start_states[int(fold_id)] = start_state
            return super().step_subaccount(
                adf, md, start_equity, params, end_ts_ms, start_ts_ms,
                start_state=start_state, entity_id=entity_id, fold_id=fold_id,
            )

    folds = pd.DataFrame([
        {"fold_id": 0, "oos_chain_order": 0, "train_start": "2024-12-01",
         "pretest_start": "2024-12-01", "pretest_end_excl": "2025-01-01",
         "test_start": "2025-01-01", "test_end_excl": "2025-01-02"},
        {"fold_id": 1, "oos_chain_order": 1, "train_start": "2024-12-02",
         "pretest_start": "2024-12-02", "pretest_end_excl": "2025-01-02",
         "test_start": "2025-01-02", "test_end_excl": "2025-01-03"},
    ])
    m06b = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "in_pool": True, "quality_weight": 1.0},
        {"entity_id": 1, "fold_id": 1, "in_pool": True, "quality_weight": 1.0},
    ])
    m08 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "survival_multiplier": 1.0,
         "tier": "ok", "max_survivable_slice": np.inf},
        {"entity_id": 1, "fold_id": 1, "survival_multiplier": 1.0,
         "tier": "ok", "max_survivable_slice": np.inf},
    ])
    # The same positional seat names different wallets in adjacent folds. Wallet B must cold-start.
    m04 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"},
        {"entity_id": 1, "fold_id": 1, "primary_wallet": "0xbbb", "entity_tier": "CLEAN"},
    ])
    eng = RecordingEng()
    M9.run_m09_chained(
        m06b, m08, m04, folds, eng, md=None, acts_loader=_acts(1.0),
        m=M9.M9Manifest(per_entity_cap=1.0), b0=500.0, out_dir=str(tmp_path),
    )
    assert eng.start_states[0] is None
    assert eng.start_states[1] is None


def test_chained_uses_fold_pure_primary_wallet(tmp_path):
    folds = pd.DataFrame([
        {"fold_id": 0, "oos_chain_order": 0, "train_start": "2024-12-01",
         "pretest_start": "2024-12-01", "pretest_end_excl": "2025-01-01",
         "test_start": "2025-01-01", "test_end_excl": "2025-01-02"},
        {"fold_id": 1, "oos_chain_order": 1, "train_start": "2024-12-02",
         "pretest_start": "2024-12-02", "pretest_end_excl": "2025-01-02",
         "test_start": "2025-01-02", "test_end_excl": "2025-01-03"},
    ])
    m06b = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "in_pool": True, "quality_weight": 1.0},
        {"entity_id": 1, "fold_id": 1, "in_pool": True, "quality_weight": 1.0},
    ])
    m08 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "survival_multiplier": 1.0,
         "tier": "ok", "max_survivable_slice": np.inf},
        {"entity_id": 1, "fold_id": 1, "survival_multiplier": 1.0,
         "tier": "ok", "max_survivable_slice": np.inf},
    ])
    m04 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "primary_wallet": "0xfold0", "entity_tier": "CLEAN"},
        {"entity_id": 1, "fold_id": 1, "primary_wallet": "0xfold1", "entity_tier": "CLEAN"},
    ])
    seen = []

    def loader(wallet, t0, t1):
        seen.append(wallet)
        return pd.DataFrame({"target_exposure_pct": [1.0]})

    M9.run_m09_chained(
        m06b, m08, m04, folds, _FakeEng(), md=None, acts_loader=loader,
        m=M9.M9Manifest(per_entity_cap=1.0), b0=500.0, out_dir=str(tmp_path),
    )
    assert "0xfold0" in seen and "0xfold1" in seen


def test_dropped_entity_pays_boundary_exit_cost(tmp_path):
    class Pos:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    class ExitCostEngine(_FakeEng):
        Position = Pos

        def step_subaccount(self, adf, md, start_equity, params, end_ts_ms, start_ts_ms,
                            start_state=None, entity_id=None, fold_id=None):
            if start_state is not None and len(adf):
                end_eq = start_equity - 10.0
                return {
                    "equity": [{"ts": end_ts_ms, "subaccount_equity": end_eq}],
                    "ending_account_state": {"cross_collateral": {"main": end_eq},
                                             "cooldown_until_ms": 0, "positions": {}},
                    "summary": {"final_equity": end_eq},
                }
            return {
                "equity": [{"ts": start_ts_ms, "subaccount_equity": start_equity},
                           {"ts": end_ts_ms, "subaccount_equity": start_equity}],
                "ending_account_state": {
                    "cross_collateral": {"main": start_equity}, "cooldown_until_ms": 0,
                    "positions": {"BTC": {"coin": "BTC", "szi": 1.0, "entry_px": 100.0,
                                              "mode": "cross", "leverage": 10.0,
                                              "cum_funding": 0.0, "isolated_margin": 0.0}},
                },
                "summary": {"final_equity": start_equity},
            }

    folds = pd.DataFrame([
        {"fold_id": 0, "oos_chain_order": 0, "train_start": "2024-12-01",
         "pretest_start": "2024-12-01", "pretest_end_excl": "2025-01-01",
         "test_start": "2025-01-01", "test_end_excl": "2025-01-02"},
        {"fold_id": 1, "oos_chain_order": 1, "train_start": "2024-12-02",
         "pretest_start": "2024-12-02", "pretest_end_excl": "2025-01-02",
         "test_start": "2025-01-02", "test_end_excl": "2025-01-03"},
    ])
    m06b = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "in_pool": True, "quality_weight": 1.0},
    ])
    m08 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "survival_multiplier": 1.0,
         "tier": "ok", "max_survivable_slice": np.inf},
    ])
    m04 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"},
    ])
    out = M9.run_m09_chained(
        m06b, m08, m04, folds, ExitCostEngine(), md=None,
        acts_loader=_acts(1.0), m=M9.M9Manifest(per_entity_cap=1.0),
        b0=500.0, out_dir=str(tmp_path),
    )
    assert out["final_equity"] == pytest.approx(490.0)


def test_gross_budget_fixed_at_b0_across_folds(tmp_path):
    # (c) gross budget must key off the FIXED bankroll b0, NOT live portfolio equity. A fold-0 winner that
    # doubles equity must NOT enlarge the fold-1 gross budget. Both folds report gross_budget == b0*gross_cap.
    folds = pd.DataFrame([
        {"fold_id": 0, "oos_chain_order": 0, "train_start": "2024-12-01", "pretest_start": "2024-12-01",
         "pretest_end_excl": "2025-01-01", "test_start": "2025-01-01", "test_end_excl": "2025-01-02"},
        {"fold_id": 1, "oos_chain_order": 1, "train_start": "2024-12-02", "pretest_start": "2024-12-02",
         "pretest_end_excl": "2025-01-02", "test_start": "2025-01-02", "test_end_excl": "2025-01-03"},
    ])
    m06b = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "in_pool": True, "quality_weight": 0.5},
        {"entity_id": 1, "fold_id": 1, "in_pool": True, "quality_weight": 0.5},
    ])
    m08 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "survival_multiplier": 1.0, "tier": "ok", "max_survivable_slice": np.inf},
        {"entity_id": 1, "fold_id": 1, "survival_multiplier": 1.0, "tier": "ok", "max_survivable_slice": np.inf},
    ])
    m04 = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"},
                        {"entity_id": 1, "fold_id": 1, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"}])
    eng = _FakeEng(ret_by_eid={1: 1.0})                   # +100% fold-0: equity doubles going into fold 1
    m = M9.M9Manifest(per_entity_cap=1.0)
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=_acts(1.0),
                             m=m, b0=500.0, out_dir=str(tmp_path))
    budgets = [fc["gross_budget"] for fc in out["fold_caps_applied"]]
    assert budgets[0] == pytest.approx(1500.0)            # b0(500) * gross_cap(3)
    assert budgets[1] == pytest.approx(1500.0)            # STILL b0-based despite equity now ~1000+


def test_g4_kill_flattens_carried_to_cash(tmp_path):
    # (d) On a G4 intra-fold breach (<=50% of fold-initial), carried positions are FLATTENED to cash at the
    # breach -- NO post-kill recovery carried to the next fold. Engine returns a path that dips to 40% then
    # recovers; the kill must freeze fold-end at the breach equity and carry NOTHING forward.
    folds = pd.DataFrame([
        {"fold_id": 0, "oos_chain_order": 0, "train_start": "2024-12-01", "pretest_start": "2024-12-01",
         "pretest_end_excl": "2025-01-01", "test_start": "2025-01-01", "test_end_excl": "2025-01-02"},
        {"fold_id": 1, "oos_chain_order": 1, "train_start": "2024-12-02", "pretest_start": "2024-12-02",
         "pretest_end_excl": "2025-01-02", "test_start": "2025-01-02", "test_end_excl": "2025-01-03"},
    ])
    m06b = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "in_pool": True, "quality_weight": 0.5},
        {"entity_id": 1, "fold_id": 1, "in_pool": True, "quality_weight": 0.5},
    ])
    m08 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "survival_multiplier": 1.0, "tier": "ok", "max_survivable_slice": np.inf},
        {"entity_id": 1, "fold_id": 1, "survival_multiplier": 1.0, "tier": "ok", "max_survivable_slice": np.inf},
    ])
    m04 = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"},
                        {"entity_id": 1, "fold_id": 1, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"}])

    class _DipEng(_FakeEng):
        def step_subaccount(self, adf, md, start_equity, params, end_ts_ms, start_ts_ms,
                            start_state=None, entity_id=None, fold_id=None):
            if fold_id == 0:
                # dip to 30% of start (breach) at mid then "recover" to 110% at end
                mid = (start_ts_ms + end_ts_ms) // 2
                return {
                    "equity": [{"ts": start_ts_ms, "subaccount_equity": start_equity},
                               {"ts": mid, "subaccount_equity": start_equity * 0.30},
                               {"ts": end_ts_ms, "subaccount_equity": start_equity * 1.10}],
                    "ending_account_state": {"cross_collateral": {"USDC": start_equity * 1.10},
                                             "cooldown_until_ms": 0, "positions": {}},
                    "summary": {"final_equity": start_equity * 1.10},
                }
            return super().step_subaccount(adf, md, start_equity, params, end_ts_ms, start_ts_ms,
                                           start_state, entity_id, fold_id)

    eng = _DipEng(ret_by_eid={1: 0.0})
    m = M9.M9Manifest(per_entity_cap=1.0)
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=_acts(1.0),
                             m=m, b0=500.0, out_dir=str(tmp_path))
    f0 = out["per_fold"][0]
    assert f0["g4_killed"] is True
    assert f0["intervention"] == "g4_kill"
    # fold-end frozen at the ~30% breach, NOT the 110% recovery
    assert f0["fold_end_equity"] == pytest.approx(0.30 * f0["fold_initial"], rel=1e-6)
    assert out["n_g4_kills"] == 1
    # carried flattened to cash: fold 1 starts from the breach equity (~150), not the 550 recovery.
    assert out["per_fold"][1]["fold_initial"] == pytest.approx(0.30 * f0["fold_initial"], rel=1e-6)


def test_global_dd_before_g4_does_not_report_g4_kill(tmp_path):
    # CAUSAL G4 DIAGNOSTIC (codex regression): global-DD (35%) breaches BEFORE a later (would-be) G4
    # level (50%) in the same fold. The fold flattens at the EARLIER global-DD ts, so the deeper G4 dip
    # never causally happens. intervention must be global_dd_derisk AND g4_killed must be False -- the
    # post-flatten G4 must NOT be reported on the diagnostic (it is evaluated only on the causal path).
    folds = pd.DataFrame([
        {"fold_id": 0, "oos_chain_order": 0, "train_start": "2024-12-01", "pretest_start": "2024-12-01",
         "pretest_end_excl": "2025-01-01", "test_start": "2025-01-01", "test_end_excl": "2025-01-02"},
    ])
    m06b = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "in_pool": True, "quality_weight": 0.5}])
    m08 = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "survival_multiplier": 1.0, "tier": "ok",
                         "max_survivable_slice": np.inf}])
    m04 = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"},
                        {"entity_id": 1, "fold_id": 1, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"}])

    class _GddThenG4Eng(_FakeEng):
        def step_subaccount(self, adf, md, start_equity, params, end_ts_ms, start_ts_ms,
                            start_state=None, entity_id=None, fold_id=None):
            # peak at start; drop to 64% (36% DD > 35% -> global-DD fires) BEFORE the deeper drop to 40%
            # (< 50% of fold-initial -> would be G4). global-DD ts is EARLIER -> it is the intervention.
            mid = (start_ts_ms + end_ts_ms) // 2
            return {
                "equity": [{"ts": start_ts_ms, "subaccount_equity": start_equity},
                           {"ts": mid, "subaccount_equity": start_equity * 0.64},        # global-DD breach
                           {"ts": end_ts_ms, "subaccount_equity": start_equity * 0.40}],  # later G4 level
                "ending_account_state": {"cross_collateral": {"USDC": start_equity * 0.40},
                                         "cooldown_until_ms": 0, "positions": {}},
                "summary": {"final_equity": start_equity * 0.40},
            }

    eng = _GddThenG4Eng(ret_by_eid={1: 0.0})
    m = M9.M9Manifest(per_entity_cap=1.0)
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=_acts(1.0),
                             m=m, b0=500.0, out_dir=str(tmp_path))
    f0 = out["per_fold"][0]
    assert f0["intervention"] == "global_dd_derisk"          # global-DD fired first
    assert f0["g4_killed"] is False                          # post-flatten G4 NOT reported (causal)
    assert out["n_g4_kills"] == 0
    assert out["n_global_dd_derisks"] == 1
    # diagnostics reflect only the causal path: G4 floor never causally breached
    assert out["g4_no_kill"] is True
    # fold frozen at the global-DD breach (~64%), NOT the deeper 40% G4 level
    assert f0["fold_end_equity"] == pytest.approx(0.64 * f0["fold_initial"], rel=1e-6)


# --------------------------------------------------------------------------- #
# PARITY knobs (settled design 2026-08-06): m07 live-parity knobs threaded through M9.
# --------------------------------------------------------------------------- #
class _ParamRecordingEng(_FakeEng):
    """Fake engine that records the exact EngineParams instance passed to every step call."""

    def __init__(self, ret_by_eid=None):
        super().__init__(ret_by_eid)
        self.seen_params = []
        self.seen_start_eq = []

    def step_subaccount(self, adf, md, start_equity, params, end_ts_ms, start_ts_ms,
                        start_state=None, entity_id=None, fold_id=None):
        self.seen_params.append(params)
        self.seen_start_eq.append(float(start_equity))
        return super().step_subaccount(adf, md, start_equity, params, end_ts_ms, start_ts_ms,
                                       start_state=start_state, entity_id=entity_id, fold_id=fold_id)


_ENGINE_PARAM_DEFAULTS = {
    # must equal v15_m07_engine.EngineParams defaults -- the byte-identical no-knob contract.
    "fixed_notional_usd": 100.0, "reversal_mode": "flip", "exit_latency_ms": None,
    "exit_entry_grace_ms": 90_000, "leader_dust_floor_usd": 0.0, "sl_bps": None,
    "global_stop_pct": None,
}


def test_parity_knobs_default_path_regression(tmp_path):
    # (1) DEFAULT-PATH REGRESSION: a run with the manifest defaults must be IDENTICAL to a run with
    # every new knob spelled out at its default value, and the params reaching step_subaccount must
    # carry exactly the m07 EngineParams defaults (so the threading is inert when unset).
    m06b, m08, m04, folds = _chained_inputs()
    out_a = M9.run_m09_chained(m06b, m08, m04, folds, _FakeEng(), md=None, acts_loader=_acts(1.0),
                               m=M9.M9Manifest(), b0=500.0, out_dir=str(tmp_path / "a"))
    explicit = M9.M9Manifest(fixed_notional_usd=100.0, reversal_mode="flip", exit_latency_ms=None,
                             exit_entry_grace_ms=90_000, leader_dust_floor_usd=0.0, sl_bps=None,
                             global_stop_pct=None)
    eng = _ParamRecordingEng()
    out_b = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=_acts(1.0),
                               m=explicit, b0=500.0, out_dir=str(tmp_path / "b"))
    out_a.pop("equity_path"); out_b.pop("equity_path")           # only the tmp dir differs
    assert out_a == out_b
    assert len(eng.seen_params) > 0
    for p in eng.seen_params:
        for attr, want in _ENGINE_PARAM_DEFAULTS.items():
            assert getattr(p, attr) == want, f"{attr} not at engine default"


def test_parity_knobs_threaded_to_engine_params(tmp_path):
    # (2) THREADING: every parity knob set in the manifest reaches step_subaccount verbatim, and the
    # fixed_notional allocation semantics hold: p90 concurrent legs = 2 (occupancy-weighted: both
    # coins held to the pretest window end dominate the dwell), so sleeve margin = 250 * 2 / 5.0
    # (conservative fallback leverage, md=None) * 1.05 headroom = $105, and the gross budget counts
    # the fixed implied gross 2 sleeves x (250 x 2 legs) = $1000 by sleeve count (no margin scaling).
    m06b, m08, m04, folds = _chained_inputs()
    test_start_ms = pd.Timestamp("2025-01-01").value // 1_000_000

    def loader(wallet, t0, t1):
        if wallet is None:
            return None
        if t0 < test_start_ms:                                   # PRETEST: 2 coins held to window end
            return pd.DataFrame([{"ts": 1, "coin": "BTC", "position_after": 1.0},
                                 {"ts": 2, "coin": "ETH", "position_after": -1.0}])
        return pd.DataFrame({"coin": ["BTC"], "position_after": [1.0]})

    manifest = M9.M9Manifest(sizing_mode="fixed_notional", fixed_notional_usd=250.0,
                             reversal_mode="flatten_only", exit_latency_ms=20_000,
                             exit_entry_grace_ms=45_000, leader_dust_floor_usd=10.0,
                             sl_bps=-2500.0, global_stop_pct=0.15)
    eng = _ParamRecordingEng()
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=loader,
                             m=manifest, b0=500.0, out_dir=str(tmp_path))
    assert len(eng.seen_params) == 2                             # both sleeves ran
    for p in eng.seen_params:
        assert p.sizing_mode == "fixed_notional"
        assert p.fixed_notional_usd == 250.0
        assert p.reversal_mode == "flatten_only"
        assert p.exit_latency_ms == 20_000
        assert p.exit_entry_grace_ms == 45_000
        assert p.leader_dust_floor_usd == 10.0
        assert p.sl_bps == -2500.0
        assert p.global_stop_pct == 0.15
    # sleeve margin need = 250 * p90(2) / assumed_lev(5) * headroom(1.05) = 105 each
    want = 250.0 * 2.0 / 5.0 * M9.FIXED_NOTIONAL_MARGIN_HEADROOM
    assert sorted(eng.seen_start_eq) == [pytest.approx(want), pytest.approx(want)]
    fc = out["fold_caps_applied"][0]
    assert fc["implied_gross_notional"] == pytest.approx(1000.0)  # 2 x (250 x 2 legs), by sleeve count
    assert fc["n_gross_dropped"] == 0                             # both fit the 1500 budget
    assert fc["n_min_notional_dropped"] == 0                      # $250 order >= $10: feasible by construction


def _m09_cli(monkeypatch, extra):
    argv = ["v15_m09_sim.py", "--m06b-pool", "nope.parquet", "--m08-survival", "nope.parquet",
            "--m04-dir", "nope", "--folds", "nope.parquet", "--actions", "nope",
            "--slip-calib", "nope.json", "--out", "nope", "--b0", "500", "--target-count", "4",
            *extra]
    monkeypatch.setattr(sys, "argv", argv)
    M9.main()


def test_fixed_notional_gross_feasibility_precheck(monkeypatch):
    # (3) CLI parse-time refusal: target_count * fixed_notional_usd > gross_cap * b0 is gross-infeasible.
    # 4 x 500 = 2000 > 3 x 500 = 1500 -> refuse with a clear message, before any file is touched.
    with pytest.raises(ValueError, match="gross-infeasible"):
        _m09_cli(monkeypatch, ["--sizing-mode", "fixed_notional", "--fixed-notional-usd", "500"])
    # feasible config (4 x 100 = 400 <= 1500) passes the precheck and only dies later on the
    # (deliberately) missing input files -- proves the check does not over-refuse.
    with pytest.raises((FileNotFoundError, OSError)):
        _m09_cli(monkeypatch, ["--sizing-mode", "fixed_notional", "--fixed-notional-usd", "100"])


def test_sl_bps_without_flatten_only_fails_fast(monkeypatch):
    # (4) FAIL-FAST: sl_bps without reversal_mode=flatten_only must die at parse time with the mirrored
    # m07 validation message, not mid-fold inside the engine.
    with pytest.raises(ValueError, match="flatten_only"):
        _m09_cli(monkeypatch, ["--sl-bps", "-2500"])
    # the valid combination clears validation and only fails later on the missing input files.
    with pytest.raises((FileNotFoundError, OSError)):
        _m09_cli(monkeypatch, ["--sl-bps", "-2500", "--reversal-mode", "flatten_only"])


def test_p90_concurrent_legs_occupancy_weighted():
    # (5, codex P1-D) OCCUPANCY-weighted quantile: 3 coins, overlapping 1-tick spans, window end
    # right after the last action -> uniform dwell. Levels by time: {0:1, 1:2, 2:2, 3:1} of 6.
    # Levels <= 2 cover only 5/6 (83%) of the time -> the smallest level covering >= 90% is 3.
    rows = [
        {"ts": 1, "coin": "BTC", "position_after": 1.0},
        {"ts": 2, "coin": "ETH", "position_after": -2.0},
        {"ts": 3, "coin": "SOL", "position_after": 0.5},
        {"ts": 4, "coin": "BTC", "position_after": 0.0},
        {"ts": 5, "coin": "ETH", "position_after": 0.0},
        {"ts": 6, "coin": "SOL", "position_after": 0.0},
    ]
    adf = pd.DataFrame(rows)
    assert M9.p90_concurrent_legs(adf, window_end_ms=7) == pytest.approx(3.0)
    # order-independent: the helper sorts on (ts, event_order) itself
    shuffled = adf.sample(frac=1.0, random_state=7).reset_index(drop=True)
    assert M9.p90_concurrent_legs(shuffled, window_end_ms=7) == pytest.approx(3.0)
    # fallbacks: missing stream / empty / missing columns / all-flat all floor to 1.0
    assert M9.p90_concurrent_legs(None) == 1.0
    assert M9.p90_concurrent_legs(pd.DataFrame()) == 1.0
    assert M9.p90_concurrent_legs(pd.DataFrame({"target_exposure_pct": [1.0]})) == 1.0
    assert M9.p90_concurrent_legs(
        pd.DataFrame([{"ts": 1, "coin": "BTC", "position_after": 0.0}]), window_end_ms=100) == 1.0


def test_p90_same_ts_burst_collapses_to_final_state():
    # codex P1-D: all rows of one ts apply, then the level is sampled ONCE. A 2-coin burst at t=0
    # held to t=100 is level 2 for 100% of the time -- level 1 must never be sampled.
    burst = pd.DataFrame([
        {"ts": 0, "event_order": 0, "coin": "BTC", "position_after": 1.0},
        {"ts": 0, "event_order": 1, "coin": "ETH", "position_after": -1.0},
    ])
    assert M9.p90_concurrent_legs(burst, window_end_ms=100) == pytest.approx(2.0)
    # degenerate no-dwell stream (no window end): fall back to the terminal holding, not 1-per-event
    burst3 = pd.DataFrame([
        {"ts": 5, "event_order": i, "coin": c, "position_after": 1.0}
        for i, c in enumerate(["BTC", "ETH", "SOL"])
    ])
    assert M9.p90_concurrent_legs(burst3, window_end_ms=None) == pytest.approx(3.0)


def test_p90_terminal_holding_weighted_to_window_end():
    # codex P1-D: the terminal open interval is weighted to the pretest window end. One coin held
    # [0,90), two coins only [90,100) -> level 1 covers 90% -> p90 = 1. Two coins held the whole
    # window -> 2.
    adf = pd.DataFrame([{"ts": 0, "coin": "BTC", "position_after": 1.0},
                        {"ts": 90, "coin": "ETH", "position_after": 1.0}])
    assert M9.p90_concurrent_legs(adf, window_end_ms=100) == pytest.approx(1.0)
    both = pd.DataFrame([{"ts": 0, "coin": "BTC", "position_after": 1.0},
                         {"ts": 1, "coin": "ETH", "position_after": 1.0}])
    assert M9.p90_concurrent_legs(both, window_end_ms=1000) == pytest.approx(2.0)


def test_p90_nan_position_after_keeps_previous_state():
    # codex P1-D: a NaN position_after is UNKNOWN -> the coin keeps its previous open state. Discard
    # semantics (the old bug) would close ETH at t=10 and answer 1.0; keep-state answers 2.0.
    adf = pd.DataFrame([
        {"ts": 0, "event_order": 0, "coin": "BTC", "position_after": 1.0},
        {"ts": 0, "event_order": 1, "coin": "ETH", "position_after": 1.0},
        {"ts": 10, "event_order": 0, "coin": "ETH", "position_after": np.nan},
    ])
    assert M9.p90_concurrent_legs(adf, window_end_ms=100) == pytest.approx(2.0)


def test_p90_long_dwell_time_weighting_beats_event_weighting():
    # codex P1-D: a burst of quick flips (levels 1,2,3,2 within 4 ticks) followed by a 996-tick
    # dwell at level 1. Event-weighted p90 of [1,2,3,2,1] would be ~2.6; occupancy-weighted, level 1
    # covers 997/1000 of the time -> 1.0.
    adf = pd.DataFrame([
        {"ts": 0, "coin": "A", "position_after": 1.0},
        {"ts": 1, "coin": "B", "position_after": 1.0},
        {"ts": 2, "coin": "C", "position_after": 1.0},
        {"ts": 3, "coin": "C", "position_after": 0.0},
        {"ts": 4, "coin": "B", "position_after": 0.0},
    ])
    assert M9.p90_concurrent_legs(adf, window_end_ms=1000) == pytest.approx(1.0)


# --------------------------------------------------------------------------- #
# codex P1-B: fixed_notional gross budget binds by SLEEVE COUNT (fund fully or drop), never by
# scaling margin (m07 orders fixed $ per leg regardless of sleeve margin).
# --------------------------------------------------------------------------- #
def _two_fold_frame():
    return pd.DataFrame([
        {"fold_id": 0, "oos_chain_order": 0, "train_start": "2024-12-01", "pretest_start": "2024-12-01",
         "pretest_end_excl": "2025-01-01", "test_start": "2025-01-01", "test_end_excl": "2025-01-02"},
        {"fold_id": 1, "oos_chain_order": 1, "train_start": "2024-12-02", "pretest_start": "2024-12-02",
         "pretest_end_excl": "2025-01-02", "test_start": "2025-01-02", "test_end_excl": "2025-01-03"},
    ])


def test_fixed_notional_gross_budget_enforced_by_sleeve_count(tmp_path):
    # codex P1-B r2 (TWO-PASS): CARRIED sleeve sits at selection rank 3, NEW sleeves at ranks 1-2.
    # Budget (3 x 500 = 1500) fits carried (600) + rank-1 (600) but NOT rank-2. Pass 1 must reserve
    # the carried gross FIRST regardless of rank; pass 2 admits rank-1 fully and drops rank-2
    # entirely -> implied gross never exceeds the budget (the single-walk bug admitted rank-1+rank-2
    # before counting the untrimmable rank-3 carried sleeve, ending at 1800 > 1500).
    folds = _two_fold_frame()
    m06b = pd.DataFrame([
        {"entity_id": 3, "fold_id": 0, "in_pool": True, "quality_weight": 0.2},
        {"entity_id": 1, "fold_id": 1, "in_pool": True, "quality_weight": 0.8},   # rank 1 (new)
        {"entity_id": 2, "fold_id": 1, "in_pool": True, "quality_weight": 0.5},   # rank 2 (new)
        {"entity_id": 3, "fold_id": 1, "in_pool": True, "quality_weight": 0.2},   # rank 3 (CARRIED)
    ])
    m08 = pd.DataFrame([
        {"entity_id": e, "fold_id": f, "survival_multiplier": 1.0, "tier": "ok",
         "max_survivable_slice": np.inf}
        for e, f in [(3, 0), (1, 1), (2, 1), (3, 1)]
    ])
    m04 = pd.DataFrame([
        {"entity_id": 3, "fold_id": 0, "primary_wallet": "0xccc", "entity_tier": "CLEAN"},
        {"entity_id": 1, "fold_id": 1, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"},
        {"entity_id": 2, "fold_id": 1, "primary_wallet": "0xbbb", "entity_tier": "CLEAN"},
        {"entity_id": 3, "fold_id": 1, "primary_wallet": "0xccc", "entity_tier": "CLEAN"},
    ])
    test0_ms = pd.Timestamp("2025-01-01").value // 1_000_000

    def loader(wallet, t0, t1):
        if wallet is None:
            return None
        if t0 < test0_ms:                                    # any PRETEST: 2 coins held -> p90 = 2
            return pd.DataFrame([{"ts": 1, "coin": "BTC", "position_after": 1.0},
                                 {"ts": 2, "coin": "ETH", "position_after": 1.0}])
        return pd.DataFrame({"coin": ["BTC"], "position_after": [1.0]})

    manifest = M9.M9Manifest(sizing_mode="fixed_notional", fixed_notional_usd=300.0,
                             per_entity_cap=1.0)                   # implied 300 x 2 = 600 / sleeve
    eng = _ParamRecordingEng()
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=loader,
                             m=manifest, b0=500.0, out_dir=str(tmp_path))
    fc1 = out["fold_caps_applied"][1]
    assert fc1["carried_gross_over_budget"] is False
    assert fc1["n_gross_dropped"] == 1                             # rank-2 dropped ENTIRELY
    assert fc1["implied_gross_notional"] == pytest.approx(1200.0)  # carried 600 + rank-1 600
    assert fc1["implied_gross_notional"] <= fc1["gross_budget"] + 1e-9
    margin = 300.0 * 2 / 5.0 * M9.FIXED_NOTIONAL_MARGIN_HEADROOM   # 126: funded FULLY, no pro-rata
    # engine ran: fold-0 seed of the carried sleeve, then fold-1 carried + rank-1 (rank-2 never ran)
    assert len(eng.seen_start_eq) == 3
    assert all(eq == pytest.approx(margin) for eq in eng.seen_start_eq)
    assert fc1["gross_trimmed_cash"] == pytest.approx(margin)      # rank-2 margin returned whole
    assert out["final_equity"] == pytest.approx(500.0)             # 0-return engine: no cash leaked


def test_fixed_notional_carried_over_budget_flagged_not_trimmed(tmp_path):
    # codex P1-B r2: carried sleeves ALONE exceeding the budget are flagged LOUDLY on the fold
    # record and NOT force-trimmed (flattening a live sleeve is the demotion machinery's decision,
    # not allocation's). Wallet holds 1 concurrent leg in fold-0's pretest (implied 300 <= budget
    # 500) but 4 legs in fold-1's pretest (implied 1200 > 500) -> fold 1 flags, keeps running.
    folds = _two_fold_frame()
    m06b = pd.DataFrame([{"entity_id": 1, "fold_id": f, "in_pool": True, "quality_weight": 1.0}
                         for f in (0, 1)])
    m08 = pd.DataFrame([{"entity_id": 1, "fold_id": f, "survival_multiplier": 1.0, "tier": "ok",
                         "max_survivable_slice": np.inf} for f in (0, 1)])
    m04 = pd.DataFrame([{"entity_id": 1, "fold_id": f, "primary_wallet": "0xaaa",
                         "entity_tier": "CLEAN"} for f in (0, 1)])
    test0_ms = pd.Timestamp("2025-01-01").value // 1_000_000

    def loader(wallet, t0, t1):
        if wallet is None:
            return None
        if t0 < test0_ms and t1 <= test0_ms:                 # fold-0 PRETEST: 1 coin held
            return pd.DataFrame([{"ts": 1, "coin": "BTC", "position_after": 1.0}])
        if t0 < test0_ms:                                    # fold-1 PRETEST: 4 coins held
            return pd.DataFrame([{"ts": i + 1, "coin": c, "position_after": 1.0}
                                 for i, c in enumerate(["BTC", "ETH", "SOL", "XRP"])])
        return pd.DataFrame({"coin": ["BTC"], "position_after": [1.0]})

    manifest = M9.M9Manifest(sizing_mode="fixed_notional", fixed_notional_usd=300.0,
                             gross_cap=1.0, per_entity_cap=1.0)    # budget = 1.0 x 500 = 500
    eng = _ParamRecordingEng()
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=loader,
                             m=manifest, b0=500.0, out_dir=str(tmp_path))
    fc0, fc1 = out["fold_caps_applied"]
    assert fc0["carried_gross_over_budget"] is False
    assert fc1["carried_gross_over_budget"] is True                # flagged loudly...
    assert fc1["n_gross_dropped"] == 0                             # (no new sleeves existed to drop)
    assert fc1["implied_gross_notional"] == pytest.approx(1200.0)  # ...and NOT trimmed
    assert len(eng.seen_start_eq) == 2                             # carried sleeve kept running
    assert eng.seen_start_eq[0] == pytest.approx(300.0 * 1 / 5.0 * M9.FIXED_NOTIONAL_MARGIN_HEADROOM)
    assert eng.seen_start_eq[1] == pytest.approx(300.0 * 4 / 5.0 * M9.FIXED_NOTIONAL_MARGIN_HEADROOM)
    assert out["final_equity"] == pytest.approx(500.0)             # 0-return engine: conservation


# --------------------------------------------------------------------------- #
# codex P1-A: HEAD-comparison contamination test. The fixed_position path must produce results
# identical to the git-HEAD implementation run on the same scenario.
# --------------------------------------------------------------------------- #
def _load_head_m09(tmp_path):
    repo = Path(__file__).resolve().parents[2]
    try:
        src = subprocess.run(["git", "show", "HEAD:research/v15/v15_m09_sim.py"],
                             cwd=repo, capture_output=True, text=True, check=True).stdout
    except Exception:
        pytest.skip("git HEAD version of v15_m09_sim.py unavailable")
    head_path = tmp_path / "m09_head.py"
    head_path.write_text(src)
    spec = importlib.util.spec_from_file_location("v15_m09_sim_head", head_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["v15_m09_sim_head"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_fixed_position_results_identical_to_head(tmp_path):
    """codex P1-A contamination test: run the SAME fixed_position scenario through the current module
    and through git-HEAD's v15_m09_sim.py; results must be deep-equal.

    ATTRIBUTION NOTE (verified via `git diff HEAD -- research/v15/v15_m09_sim.py`): the working tree
    already carried ~195 lines of PRE-EXISTING uncommitted work before the parity change (wallet-keyed
    carried state replacing HEAD's entity-keyed carried, the fixed_position mf/af sizing-contract lane,
    the CLI main(), extra provenance output keys). Those pre-existing deltas are pinned EXACTLY below
    (asserted key-for-key) and stripped; everything else -- every number the allocation, water-fill,
    gross budget, feasibility, G4/global-DD and ledger produce -- must match HEAD bit-for-bit, so any
    fixed_position contamination from the parity diff fails this test. (Once the branch is committed,
    HEAD == working tree and the test degrades to a tautology; it is load-bearing for this review.)"""
    head = _load_head_m09(tmp_path)
    folds = pd.DataFrame([
        {"fold_id": 0, "oos_chain_order": 0, "train_start": "2024-12-01", "pretest_start": "2024-12-01",
         "pretest_end_excl": "2025-01-01", "test_start": "2025-01-01", "test_end_excl": "2025-01-02"},
        {"fold_id": 1, "oos_chain_order": 1, "train_start": "2024-12-02", "pretest_start": "2024-12-02",
         "pretest_end_excl": "2025-01-02", "test_start": "2025-01-02", "test_end_excl": "2025-01-03"},
    ])
    m06b = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "in_pool": True, "quality_weight": 0.5, "accessible_frac_notional": 0.9},
        {"entity_id": 1, "fold_id": 1, "in_pool": True, "quality_weight": 0.5, "accessible_frac_notional": 0.9},
        {"entity_id": 2, "fold_id": 1, "in_pool": True, "quality_weight": 0.5, "accessible_frac_notional": 0.9},
    ])
    m08 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "survival_multiplier": 1.0, "tier": "ok", "max_survivable_slice": np.inf},
        {"entity_id": 1, "fold_id": 1, "survival_multiplier": 1.0, "tier": "ok", "max_survivable_slice": np.inf},
        {"entity_id": 2, "fold_id": 1, "survival_multiplier": 1.0, "tier": "ok", "max_survivable_slice": np.inf},
    ])
    m04 = pd.DataFrame([
        {"entity_id": 1, "fold_id": 0, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"},
        {"entity_id": 1, "fold_id": 1, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"},
        {"entity_id": 2, "fold_id": 1, "primary_wallet": "0xbbb", "entity_tier": "CLEAN"},
    ])

    def _run(module, sub):
        eng = _FakeEng(ret_by_eid={1: 0.1, 2: -0.05})     # nontrivial pnl in both folds
        manifest = module.M9Manifest(sizing_mode="fixed_position", fixed_target_exposure=1.0)
        return module.run_m09_chained(
            m06b.copy(), m08.copy(), m04.copy(), folds.copy(), eng, md=None,
            acts_loader=_acts(1.0), m=manifest, b0=500.0, out_dir=str(tmp_path / sub))

    cur = _run(M9, "cur")
    old = _run(head, "head")
    # pin the PRE-EXISTING (uncommitted, non-parity) output-contract deltas exactly -- nothing else
    # may differ in shape:
    assert set(cur) - set(old) == {"top_entity_wallet", "entity_pnl"}
    assert set(old) - set(cur) == set()
    for f in cur["per_fold"]:
        f.pop("selected_entity_ids")          # KeyError (=test failure) if the delta ever drifts
        f.pop("selected_wallets")
    cur.pop("top_entity_wallet"); cur.pop("entity_pnl")
    cur.pop("equity_path"); old.pop("equity_path")        # tmp dirs differ by construction
    assert cur == old


# --------------------------------------------------------------------------- #
# codex P1-C: the 1.05 margin headroom is load-bearing -- the sized sleeve's own leg must FILL in
# the REAL m07 engine at the assumed leverage.
# --------------------------------------------------------------------------- #
def test_fixed_notional_margin_headroom_entry_fills_real_engine():
    md = m07t.FakeMarketData(m07t._flat_ohlc("BTC", m07t.T0 - E.MS_MIN, 60, 100.0), maxlev=5.0)
    t_entry = m07t.T0 + 2 * E.MS_MIN
    end = m07t.T0 + 10 * E.MS_MIN
    pre = pd.DataFrame([{"ts": m07t.T0, "coin": "BTC", "position_after": 1.0}])
    legs = M9.p90_concurrent_legs(pre, window_end_ms=end)
    assert legs == pytest.approx(1.0)                       # one coin held -> one leg
    assumed = M9._assumed_sleeve_leverage(md, pre)
    assert assumed == pytest.approx(5.0)                    # real engine metadata, not the fallback
    fixed_usd = 100.0
    margin = fixed_usd * legs / assumed * M9.FIXED_NOTIONAL_MARGIN_HEADROOM   # exactly m09's sizing
    acts = pd.DataFrame([m07t._action("BTC", t_entry, np.nan, "ENTRY",
                                      position_after=1.0, signed_size=1.0)])
    params = E.EngineParams(copy_latency_ms=0, sizing_mode="fixed_notional",
                            fixed_notional_usd=fixed_usd)
    out = E.step_subaccount(acts, md, margin, params, end_ts_ms=end)
    assert out["summary"]["n_fills"] == 1                   # the entry actually FILLS
    pos = out["ending_account_state"]["positions"]["BTC"]
    assert abs(pos["szi"]) * 100.0 == pytest.approx(fixed_usd, rel=0.02)   # full $100 leg booked
    # WITHOUT headroom (margin exactly fixed/leverage) the IM+fee admission cannot book the full leg
    # -- that shortfall is precisely why FIXED_NOTIONAL_MARGIN_HEADROOM exists.
    out0 = E.step_subaccount(acts, md, fixed_usd / assumed, params, end_ts_ms=end)
    p0 = out0["ending_account_state"]["positions"].get("BTC")
    booked0 = abs(p0["szi"]) * 100.0 if p0 else 0.0
    assert booked0 < fixed_usd * 0.999


# --------------------------------------------------------------------------- #
# codex P1-E: boundary flatten must PRICE the close (fee+slip), not cash out at pure mark.
# Defect attribution: PRE-EXISTING at HEAD (same end_ts_ms == ts call shape) -- reproduced below.
# --------------------------------------------------------------------------- #
def test_boundary_flatten_close_pays_fee_and_slip():
    md = m07t.FakeMarketData(m07t._flat_ohlc("BTC", m07t.T0 - E.MS_MIN, 60, 100.0), maxlev=10.0)
    t_close = m07t.T0 + 5 * E.MS_MIN

    def _mk_state():
        st = E.AccountState(cross_collateral={"main": 50.0})
        st.positions["BTC"] = E.Position(coin="BTC", szi=1.0, entry_px=100.0, mode="cross",
                                         leverage=10.0)
        return st

    rows = pd.DataFrame([{
        "coin": "BTC", "ts": int(t_close), "event_order": 0, "action_type": "EXIT",
        "signed_size": -1.0, "position_after": 0.0, "target_exposure_pct": 0.0,
        "is_liquidation": False, "carry_in_status": "SEEDED",
        "lifecycle_valid": True, "stream_replay_valid": True}])
    params = E.EngineParams(slippage_band="base", start_policy="future_delta_only")
    params.copy_latency_ms = 0
    params.sizing_mode = "fixed_position"
    # DEFECT REPRODUCTION (HEAD call shape, end == ts): m07's half-open window [start, end) drops
    # the synthetic EXIT -> NO fill; the sleeve would become cash at pure mark, zero fees/slip.
    broken = E.step_subaccount(rows, md, 50.0, params, end_ts_ms=int(t_close),
                               start_ts_ms=int(t_close), start_state=_mk_state())
    assert broken["summary"]["n_fills"] == 0
    assert broken["summary"]["final_equity"] == pytest.approx(50.0)
    # THE FIX (m09 now passes end = ts + 1): the close books through the full execution path.
    fixed = E.step_subaccount(rows, md, 50.0, params, end_ts_ms=int(t_close) + 1,
                              start_ts_ms=int(t_close), start_state=_mk_state())
    assert fixed["summary"]["n_fills"] == 1
    assert fixed["ending_account_state"]["positions"] == {}
    assert fixed["summary"]["final_equity"] < 50.0          # paid fee + slippage
    assert fixed["summary"]["final_equity"] > 49.0          # ...and nothing more than that


def test_boundary_flatten_engine_window_wiring(tmp_path):
    # codex P1-E wiring: m09's _flatten_at_boundary must call the engine with the half-open-valid
    # window [ts, ts+1) so the synthetic EXIT survives m07's `ts < end_ts_ms` filter.
    windows = []

    class Pos:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    class Recorder(_FakeEng):
        Position = Pos

        def step_subaccount(self, adf, md, start_equity, params, end_ts_ms, start_ts_ms,
                            start_state=None, entity_id=None, fold_id=None):
            if start_state is not None and getattr(start_state, "positions", None):
                windows.append((int(start_ts_ms), int(end_ts_ms)))     # the boundary-flatten call
                return {"equity": [{"ts": end_ts_ms, "subaccount_equity": start_equity}],
                        "ending_account_state": {"cross_collateral": {"main": start_equity},
                                                 "cooldown_until_ms": 0, "positions": {}},
                        "summary": {"final_equity": start_equity}}
            return {"equity": [{"ts": start_ts_ms, "subaccount_equity": start_equity},
                               {"ts": end_ts_ms, "subaccount_equity": start_equity}],
                    "ending_account_state": {
                        "cross_collateral": {"main": start_equity}, "cooldown_until_ms": 0,
                        "positions": {"BTC": {"coin": "BTC", "szi": 1.0, "entry_px": 100.0,
                                              "mode": "cross", "leverage": 10.0,
                                              "cum_funding": 0.0, "isolated_margin": 0.0}}},
                    "summary": {"final_equity": start_equity}}

    folds = pd.DataFrame([
        {"fold_id": 0, "oos_chain_order": 0, "train_start": "2024-12-01", "pretest_start": "2024-12-01",
         "pretest_end_excl": "2025-01-01", "test_start": "2025-01-01", "test_end_excl": "2025-01-02"},
        {"fold_id": 1, "oos_chain_order": 1, "train_start": "2024-12-02", "pretest_start": "2024-12-02",
         "pretest_end_excl": "2025-01-02", "test_start": "2025-01-02", "test_end_excl": "2025-01-03"},
    ])
    m06b = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "in_pool": True, "quality_weight": 1.0}])
    m08 = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "survival_multiplier": 1.0, "tier": "ok",
                         "max_survivable_slice": np.inf}])
    m04 = pd.DataFrame([{"entity_id": 1, "fold_id": 0, "primary_wallet": "0xaaa", "entity_tier": "CLEAN"}])
    M9.run_m09_chained(m06b, m08, m04, folds, Recorder(), md=None, acts_loader=_acts(1.0),
                       m=M9.M9Manifest(per_entity_cap=1.0), b0=500.0, out_dir=str(tmp_path))
    t0_f1 = int(pd.Timestamp("2025-01-02").value // 1_000_000)
    assert windows == [(t0_f1, t0_f1 + 1)]


# ── pinned pool provider (2026-08-07: hold-the-cohort after rotation was buried by m10) ──────────

def test_pinned_provider_holds_only_listed_wallets(tmp_path):
    """pool_provider='pinned' must restrict each fold's candidates to the pinned wallets BEFORE
    anti-corr/caps; the unlisted entity gets no sleeve. Recorder pattern as the threading test."""
    m06b, m08, m04, folds = _chained_inputs()
    eng = _ParamRecordingEng()
    out = M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=_acts(),
                             m=M9.M9Manifest(sizing_mode="leader_equity"), b0=500.0,
                             pool_provider="pinned", pinned_wallets={"0xAAA"},
                             out_dir=str(tmp_path))
    assert len(eng.seen_params) == 1, "only the pinned wallet's sleeve may run"
    assert set(out["entity_pnl"].keys()) == {"0xaaa"} or list(out["entity_pnl"]) == ["0xaaa"] or \
        all(k == "0xaaa" for k in out["entity_pnl"]), f"unexpected sleeves: {out['entity_pnl']}"


def test_pinned_provider_requires_wallets():
    import pytest
    m06b, m08, m04, folds = _chained_inputs()
    with pytest.raises(ValueError, match="pinned_wallets"):
        M9.run_m09_chained(m06b, m08, m04, folds, _ParamRecordingEng(), md=None,
                           acts_loader=_acts(), m=M9.M9Manifest(), b0=500.0,
                           pool_provider="pinned", pinned_wallets=None)


def test_pinned_provider_case_insensitive():
    m06b, m08, m04, folds = _chained_inputs()
    eng = _ParamRecordingEng()
    M9.run_m09_chained(m06b, m08, m04, folds, eng, md=None, acts_loader=_acts(),
                       m=M9.M9Manifest(sizing_mode="leader_equity"), b0=500.0,
                       pool_provider="pinned", pinned_wallets={"0XAAA", "0xBBB"},
                       out_dir=None)
    assert len(eng.seen_params) == 2, "case-insensitive match must admit both"
