"""V15 M9 allocation-core tests: sizing chain, anti-corr, min-notional feasibility, cap-aware water-fill.

Run: /Users/hermes/miniforge3/envs/quants-lab/bin/python -m pytest tests/v15/test_m09.py -q
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "research" / "v15"))
import v15_m09_sim as M9  # noqa: E402

M = M9.M9Manifest()


def test_unimplemented_matched_null_provider_fails_closed():
    with pytest.raises(NotImplementedError, match="matched_null"):
        M9.run_m09_chained(
            pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
            eng=None, md=None, acts_loader=None, m=M, b0=500.0,
            pool_provider="matched_null",
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
