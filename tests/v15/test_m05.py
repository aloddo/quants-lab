"""V15 M5 eligibility tests. Asserts the codex-SHIP design contract (modules/m05)."""
import sys
from datetime import date, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "research" / "v15"))
import v15_m03_fold_geometry as m03  # noqa: E402
import v15_m05_eligibility as m5  # noqa: E402

FOLDS = m03.folds_to_frame(m03.build_folds(), market_data_fn=None)
F1 = m03.build_folds()[0]  # train Dec01, val Jan12, test Jan26


def ms(d):
    return m5._ms(d)


# === flow_adjusted_twr ===
def test_twr_positive_and_dd():
    # equity 1000 -> 1100 -> 990 (no flows): roe ~ -1%? build a clean +10% then -10%
    df = pd.DataFrame({"date": pd.date_range("2026-01-01", periods=3, freq="D").date,
                       "equity_usd": [1000.0, 1100.0, 1100.0], "ext_flow_cum": [0.0, 0.0, 0.0]})
    r = m5.flow_adjusted_twr(df)
    assert r["roe"] > 0 and not r["structural_ruin"]


def test_twr_flow_stripped():
    # equity jumps 1000->2000 purely from a 1000 deposit -> ROE ~0, not +100%
    df = pd.DataFrame({"date": pd.date_range("2026-01-01", periods=2, freq="D").date,
                       "equity_usd": [1000.0, 2000.0], "ext_flow_cum": [0.0, 1000.0]})
    r = m5.flow_adjusted_twr(df)
    assert abs(r["roe"]) < 1e-6


def test_twr_maxdd_counts_first_drop():
    # codex code-r1 #3: 1000 -> 500 -> 500 must report ~50% DD (baseline prepended), not 0.
    df = pd.DataFrame({"date": pd.date_range("2026-01-01", periods=3, freq="D").date,
                       "equity_usd": [1000.0, 500.0, 500.0], "ext_flow_cum": [0.0, 0.0, 0.0]})
    r = m5.flow_adjusted_twr(df)
    assert abs(r["max_dd"] - 0.5) < 1e-6


def test_twr_structural_ruin():
    df = pd.DataFrame({"date": pd.date_range("2026-01-01", periods=3, freq="D").date,
                       "equity_usd": [1000.0, 500.0, 0.2], "ext_flow_cum": [0.0, 0.0, 0.0]})
    assert m5.flow_adjusted_twr(df)["structural_ruin"] is True


# === journey_metrics + floors ===
def _jr(wallet, entry_d, exit_d, pnl, coin="BTC", notional=1000.0):
    return {"wallet": wallet, "coin": coin,
            "entry_ts": ms(entry_d), "exit_ts": ms(exit_d),
            "duration_h": (ms(exit_d) - ms(entry_d)) / 3.6e6,
            "net_realized_pnl": pnl, "max_position_notional": notional}


def _good_eqm():
    return {"roe": 0.2, "max_dd": 0.3, "n_days": 50, "structural_ruin": False,
            "median_equity": 10000.0}


def _good_jm():
    return {"net_pnl": 500.0, "n_journeys": 5, "median_hold_s": 3600.0,
            "share_below_latency": 0.0, "accessible_frac_notional": 1.0, "accessible_frac_count": 1.0}


def test_floor_pass():
    ok, f = m5.apply_floors(_good_eqm(), _good_jm())
    assert ok and not f


def test_floor_fail_negative_pnl():
    jm = _good_jm(); jm["net_pnl"] = -10
    assert not m5.apply_floors(_good_eqm(), jm)[0]


def test_floor_fail_dd():
    e = _good_eqm(); e["max_dd"] = 0.9
    assert not m5.apply_floors(e, _good_jm())[0]


def test_floor_fail_thin_journeys():
    jm = _good_jm(); jm["n_journeys"] = 2
    assert not m5.apply_floors(_good_eqm(), jm)[0]


def test_floor_fail_hft_holdtime():
    jm = _good_jm(); jm["median_hold_s"] = 30.0  # < 60s HOLD_FLOOR
    assert not m5.apply_floors(_good_eqm(), jm)[0]


def test_floor_fail_share_below_latency():
    jm = _good_jm(); jm["share_below_latency"] = 0.4  # > 0.25
    assert not m5.apply_floors(_good_eqm(), jm)[0]


def test_floor_fail_week_holder():
    # median hold > 48h SWING_MAX_HOLD_S -> reject multi-day/week holder
    jm = _good_jm(); jm["median_hold_s"] = 7 * 24 * 3600.0  # 7-day hold
    ok, f = m5.apply_floors(_good_eqm(), jm)
    assert not ok
    assert any("hold_too_long" in x for x in f)


def test_floor_fail_tiny_equity():
    # pretest equity below MIN_EQUITY_USD ($2000) -> drop tiny degen account
    e = _good_eqm(); e["median_equity"] = 276.0  # $276 degen account
    ok, f = m5.apply_floors(e, _good_jm())
    assert not ok
    assert any("equity_too_small" in x for x in f)


def test_floor_fast_wallet_still_passes():
    # valid fast directional wallet: hours-scale hold, ample equity -> passes
    jm = _good_jm(); jm["median_hold_s"] = 4 * 3600.0  # 4h hold (minutes-to-hours thesis)
    e = _good_eqm(); e["median_equity"] = 8000.0
    ok, f = m5.apply_floors(e, jm)
    assert ok and not f


def test_floor_fail_boundary_straddling_week_holder():
    # codex finding a: a position OPENED in pretest still OPEN at test_start with a censored hold
    # exceeding the swing cap is a week-holder, even though it's invisible to median_hold_s.
    jm = _good_jm(); jm["censored_max_hold_s"] = 5 * 24 * 3600.0  # 5-day open straddle
    ok, f = m5.apply_floors(_good_eqm(), jm)
    assert not ok
    assert any("hold_too_long_censored" in x for x in f)


def test_journey_metrics_censored_hold_for_open_position():
    # an open position (exit_ts NaN) opened in pretest -> censored_max_hold_s = hi - entry.
    lo = ms(date(2025, 12, 1)); hi = ms(date(2025, 12, 30))
    jr = pd.DataFrame([{
        "wallet": "0xw", "coin": "BTC",
        "entry_ts": ms(date(2025, 12, 2)), "exit_ts": np.nan,
        "duration_h": np.nan, "net_realized_pnl": 0.0, "max_position_notional": 1000.0,
    }])
    jm = m5.journey_metrics(jr, lo, hi, None)
    expected = (hi - ms(date(2025, 12, 2))) / 1000.0
    assert abs(jm["censored_max_hold_s"] - expected) < 1.0
    # straddling position (exit AFTER hi) also counts as censored.
    jr2 = pd.DataFrame([{
        "wallet": "0xw", "coin": "BTC",
        "entry_ts": ms(date(2025, 12, 2)), "exit_ts": ms(date(2026, 1, 15)),
        "duration_h": 1000.0, "net_realized_pnl": 50.0, "max_position_notional": 1000.0,
    }])
    jm2 = m5.journey_metrics(jr2, lo, hi, None)
    assert abs(jm2["censored_max_hold_s"] - expected) < 1.0
    assert jm2["n_journeys"] == 0  # not closed-in-pretest -> excluded from count/PnL


def test_data_gap_account_not_falsely_failed():
    # codex finding b: a data gap (missing equity_usd day) must NOT drag median_equity to 0 via the
    # 0.0-fill. Raw non-null median stays well above the $2k floor.
    df = pd.DataFrame({
        "date": pd.date_range("2026-01-01", periods=5, freq="D").date,
        "equity_usd": [10000.0, np.nan, 10000.0, np.nan, 10000.0],
        "ext_flow_cum": [0.0, 0.0, 0.0, 0.0, 0.0],
    })
    r = m5.flow_adjusted_twr(df)
    # raw non-null median = 10000 (NOT the 0-filled median ~10000 dragged down by zeros)
    assert r["median_equity"] == 10000.0
    ok, f = m5.apply_floors(r, _good_jm())
    assert not any("equity_too_small" in x for x in f)


def test_median_equity_emitted_in_elig_rows():
    # codex finding c: median_equity_pretest is a column in elig_df for auditability.
    journeys = pd.DataFrame([
        _jr("0xg", date(2025, 12, 5), date(2025, 12, 6), 100),
        _jr("0xg", date(2025, 12, 10), date(2025, 12, 11), 100),
        _jr("0xg", date(2026, 1, 2), date(2026, 1, 3), 100),
        _jr("0xg", date(2026, 1, 8), date(2026, 1, 9), 100),
    ])
    eq = pd.DataFrame({"wallet": "0xg",
                       "date": pd.date_range("2025-12-01", "2026-01-25", freq="D").date,
                       "equity_usd": np.linspace(10000, 13000, 56), "ext_flow_cum": 0.0})
    elig, pool, wf = m5.run(FOLDS, journeys, eq, _m04(["0xg"]))
    assert "median_equity_pretest" in elig.columns
    f1 = elig[(elig["primary_wallet"] == "0xg") & (elig["fold_id"] == 1)].iloc[0]
    assert f1["median_equity_pretest"] > 2000.0


def test_floor_accessibility_unknown_does_not_fail():
    jm = _good_jm(); jm["accessible_frac_notional"] = float("nan")  # unknown
    assert m5.apply_floors(_good_eqm(), jm)[0]  # passes (loose)


def test_floor_accessibility_known_low_fails():
    jm = _good_jm(); jm["accessible_frac_notional"] = 0.5
    assert not m5.apply_floors(_good_eqm(), jm)[0]


def test_floor_structural_ruin_fails():
    e = _good_eqm(); e["structural_ruin"] = True
    assert not m5.apply_floors(e, _good_jm())[0]


# === end-to-end run: fold-purity + entity-unit ===
def _m04(copyable_wallets, killed_wallet=None):
    rows = []
    for i, w in enumerate(copyable_wallets):
        rows.append({"wallet": w, "entity_id": i, "is_entity_primary": True, "n_entity_wallets": 1,
                     "tier": "CLEAN", "reason_codes": "", "copyable": True})
    if killed_wallet:
        rows.append({"wallet": killed_wallet, "entity_id": 99, "is_entity_primary": True,
                     "n_entity_wallets": 1, "tier": "KILL", "reason_codes": "wash", "copyable": False})
    return pd.DataFrame(rows)


def test_run_fold_pure_and_entity_unit():
    # wallet 0xg: 4 good journeys in F1 pretest (Dec-Jan), profitable, long holds -> eligible F1
    # journeys in the TEST window (after Jan26) must NOT count toward F1 eligibility (fold-pure)
    g_journeys = [
        _jr("0xg", date(2025, 12, 5), date(2025, 12, 6), 100),
        _jr("0xg", date(2025, 12, 10), date(2025, 12, 11), 100),
        _jr("0xg", date(2026, 1, 2), date(2026, 1, 3), 100),
        _jr("0xg", date(2026, 1, 8), date(2026, 1, 9), 100),
        _jr("0xg", date(2026, 2, 1), date(2026, 2, 2), 9999),   # in F1 TEST window -> excluded
    ]
    journeys = pd.DataFrame(g_journeys)
    eq = pd.DataFrame({"wallet": "0xg",
                       "date": pd.date_range("2025-12-01", "2026-01-25", freq="D").date,
                       "equity_usd": np.linspace(10000, 13000, 56), "ext_flow_cum": 0.0})
    m04 = _m04(["0xg"], killed_wallet="0xk")
    elig, pool, wf = m5.run(FOLDS, journeys, eq, m04)
    # KILL wallet 0xk never evaluated (not copyable)
    assert "0xk" not in set(elig["primary_wallet"])
    f1 = elig[(elig["primary_wallet"] == "0xg") & (elig["fold_id"] == 1)].iloc[0]
    assert f1["n_journeys_pretest"] == 4          # the Feb test-window journey excluded (fold-pure)
    assert f1["eligible"]
    assert f1["net_pnl_pretest"] == 400           # not 10399 (test journey not counted)


def test_run_g5_pool_candidate_reported_not_gated():
    # one journey only -> per-fold thin, but pool flag computes independently
    journeys = pd.DataFrame([_jr("0xt", date(2025, 12, 5), date(2025, 12, 6), 50)])
    eq = pd.DataFrame({"wallet": "0xt", "date": pd.date_range("2025-12-01", "2026-01-25", freq="D").date,
                       "equity_usd": np.linspace(1000, 1100, 56), "ext_flow_cum": 0.0})
    elig, pool, wf = m5.run(FOLDS, journeys, eq, _m04(["0xt"]))
    # thin -> not eligible per fold
    assert not elig[(elig["primary_wallet"] == "0xt") & (elig["fold_id"] == 1)].iloc[0]["eligible"]
    # pool candidate flag exists and is a separate diagnostic (here False: <5 journeys)
    assert "g5_pool_candidate_pass" in pool.columns
    assert not pool[pool["primary_wallet"] == "0xt"].iloc[0]["g5_pool_candidate_pass"]
