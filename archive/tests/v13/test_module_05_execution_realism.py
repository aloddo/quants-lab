"""V13 Module 05 — Execution Realism tests.

Maps to per-Module 12 spec fixtures F5-1 through F5-5.
Tests the execute_or_skip path used by Modules 04 + 08 + live shadow.
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))


@pytest.fixture
def ledger():
    from v13_portfolio_ledger import CopyLedger
    return CopyLedger(cash_usd=100_000.0)


@pytest.fixture
def coin_info_btc():
    from v13_execution_realism import CoinInfo
    return CoinInfo(coin="BTC", tier="majors", tick_size=0.5, qty_step=0.0001, min_order_usd=10.0)


@pytest.fixture
def coin_info_liquid():
    from v13_execution_realism import CoinInfo
    return CoinInfo(coin="ADA", tier="liquid", tick_size=0.001, qty_step=1.0, min_order_usd=10.0)


@pytest.fixture
def coin_info_thin():
    from v13_execution_realism import CoinInfo
    return CoinInfo(coin="WIF", tier="thin", tick_size=0.001, qty_step=0.1, min_order_usd=10.0)


def test_F5_classify_coin_tier_pinned_majors():
    from v13_execution_realism import classify_coin_tier
    assert classify_coin_tier("BTC", 100.0) == "majors"  # pinned regardless of volume
    assert classify_coin_tier("ETH", 0) == "majors"
    assert classify_coin_tier("SOL", -1) == "majors"
    assert classify_coin_tier("HYPE", 1e9) == "majors"


def test_F5_classify_coin_tier_volume_buckets():
    from v13_execution_realism import classify_coin_tier
    assert classify_coin_tier("XYZ", 10_000_000) == "majors"
    assert classify_coin_tier("XYZ", 1_000_000) == "liquid"
    assert classify_coin_tier("XYZ", 200_000) == "mid"
    assert classify_coin_tier("XYZ", 50_000) == "thin"
    assert classify_coin_tier("XYZ", 5_000) == "illiquid"
    assert classify_coin_tier("XYZ", 0) == "illiquid"
    assert classify_coin_tier("XYZ", -1) == "illiquid"


def test_F5_1_majors_tier_base_slip(ledger, coin_info_btc):
    """Majors get 2 bps slip; should execute with fill rate 95%."""
    from v13_execution_realism import execute_or_skip, SLIP_TIERS
    mark = 50_000.0
    # Buy $1000 worth at $50K → 0.02 BTC raw → rounded down to qty_step
    res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=mark,
                          ledger=ledger, coin_info=coin_info_btc)
    assert res.executed
    assert res.leg is not None
    # executable_px should be mark × (1 + 2/10000)
    expected_px = mark * (1 + SLIP_TIERS["tier_slip_bps"]["majors"] / 10000)
    assert res.leg.executable_px == pytest.approx(expected_px)
    # qty: 1000/expected_px → ~0.02 → rounded to qty_step 0.0001 → 0.0199...
    # Then × tier_rate 0.95 = ~0.019 → rounded to qty_step
    assert res.leg.qty > 0
    assert res.leg.qty <= 0.02
    # Side check
    assert res.leg.side == 1


def test_F5_2_liquid_tier_higher_slip(ledger, coin_info_liquid):
    from v13_execution_realism import execute_or_skip, SLIP_TIERS
    mark = 1.0
    res = execute_or_skip("ADA", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=mark,
                          ledger=ledger, coin_info=coin_info_liquid)
    assert res.executed
    expected_px = mark * (1 + SLIP_TIERS["tier_slip_bps"]["liquid"] / 10000)
    assert res.leg.executable_px == pytest.approx(expected_px)


def test_F5_3_ioc_cap_REJECTS_thin_alt(ledger, coin_info_thin):
    """Thin tier excluded from pool → rejected as ineligible_tier."""
    from v13_execution_realism import execute_or_skip
    mark = 1.0
    res = execute_or_skip("WIF", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=mark,
                          ledger=ledger, coin_info=coin_info_thin)
    assert not res.executed
    assert res.reject_reason == "ineligible_tier"
    # Ledger unchanged
    assert "WIF" not in ledger.position_qty


def test_F5_3b_ioc_cap_rejection_proposed_slip_too_high(ledger):
    """Construct a coin with slip > MAX_SLIPPAGE_BPS → execute_or_skip rejects ioc_cap."""
    from v13_execution_realism import execute_or_skip, CoinInfo, SLIP_TIERS, MAX_SLIPPAGE_BPS
    # Temporarily test: thin coins have 30 bps slip > 15 bps cap → ioc_cap should fire
    # But thin is also excluded by tier. So construct a custom mid-tier coin scenario.
    # Hack: monkey-patch slip for ADA to 30 bps to simulate ioc_cap rejection.
    import v13_execution_realism as m
    original = m.SLIP_TIERS["tier_slip_bps"]["liquid"]
    m.SLIP_TIERS["tier_slip_bps"]["liquid"] = 30  # > MAX_SLIPPAGE_BPS=15
    try:
        ci = CoinInfo(coin="TEST", tier="liquid", tick_size=0.001, qty_step=1.0, min_order_usd=10.0)
        res = execute_or_skip("TEST", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=1.0,
                              ledger=ledger, coin_info=ci)
        assert not res.executed
        assert res.reject_reason == "ioc_cap"
    finally:
        m.SLIP_TIERS["tier_slip_bps"]["liquid"] = original


def test_F5_4_slip_baked_into_executable_px(ledger, coin_info_btc):
    """slip_attribution is DIAGNOSTIC; cashflow already reflects executable_px (no double-count)."""
    from v13_execution_realism import execute_or_skip
    mark = 50_000.0
    res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=mark,
                          ledger=ledger, coin_info=coin_info_btc)
    assert res.executed
    leg = res.leg
    # cashflow_usd = -side × qty × executable_px (buy outflow)
    assert leg.cashflow_usd == pytest.approx(-leg.side * leg.qty * leg.executable_px)
    # slip_attribution = (executable_px - mark) × side × qty
    assert leg.slip_attribution_usd == pytest.approx((leg.executable_px - mark) * leg.side * leg.qty)
    # Verify cashflow does NOT double-subtract slip
    naive_no_slip_cashflow = -leg.side * leg.qty * mark
    # cashflow should differ from naive by EXACTLY -slip_attribution (for buy, executable > mark, so cashflow more negative)
    assert (leg.cashflow_usd - naive_no_slip_cashflow) == pytest.approx(-leg.slip_attribution_usd)


def test_F5_5_side_aware_slip_short(ledger, coin_info_btc):
    """Short side: executable_px BELOW mark; opposite sign of long."""
    from v13_execution_realism import execute_or_skip, SLIP_TIERS
    mark = 50_000.0
    # Set up a short: delta_usd < 0
    res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=-1000.0, mark_at_exec=mark,
                          ledger=ledger, coin_info=coin_info_btc)
    assert res.executed
    assert res.leg.side == -1
    expected_px = mark * (1 - SLIP_TIERS["tier_slip_bps"]["majors"] / 10000)
    assert res.leg.executable_px == pytest.approx(expected_px)
    # Short = cash INFLOW
    assert res.leg.cashflow_usd > 0


def test_F5_min_order_rejection(ledger, coin_info_btc):
    """Leg notional below min_order_usd → reject."""
    from v13_execution_realism import execute_or_skip
    # Very small delta_usd → leg below min
    res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=5.0, mark_at_exec=50_000.0,
                          ledger=ledger, coin_info=coin_info_btc)
    assert not res.executed
    assert res.reject_reason == "min_order"


def test_F5_invalid_mark_rejection(ledger, coin_info_btc):
    """codex m05 r1+r3: NaN/Inf/None/<=0 + bool MUST reject as invalid_mark."""
    from v13_execution_realism import execute_or_skip
    for bad_mark in [None, float('nan'), float('inf'), 0, -1, True, False]:
        res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=bad_mark,
                              ledger=ledger, coin_info=coin_info_btc)
        assert not res.executed, f"executed at bad mark {bad_mark!r}"
        assert res.reject_reason == "invalid_mark", f"got {res.reject_reason} for {bad_mark!r}"


def test_F5_zero_delta_rejection(ledger, coin_info_btc):
    from v13_execution_realism import execute_or_skip
    res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=0.0, mark_at_exec=50_000.0,
                          ledger=ledger, coin_info=coin_info_btc)
    assert not res.executed
    assert res.reject_reason == "zero_delta"


def test_F5_ledger_unchanged_on_rejection(ledger, coin_info_btc):
    """ANY rejection MUST leave ledger.cash + position untouched."""
    from v13_execution_realism import execute_or_skip
    initial_cash = ledger.cash_usd
    res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=5.0, mark_at_exec=50_000.0,
                          ledger=ledger, coin_info=coin_info_btc)
    assert not res.executed
    assert ledger.cash_usd == initial_cash
    assert "BTC" not in ledger.position_qty


def test_F5_invalid_delta_rejection(ledger, coin_info_btc):
    """codex m05 r1 fix: NaN/Inf delta_usd rejected with invalid_delta."""
    from v13_execution_realism import execute_or_skip
    import math
    for bad in [float('nan'), float('inf'), -float('inf'), True, None, "abc"]:
        res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=bad, mark_at_exec=50_000.0,
                              ledger=ledger, coin_info=coin_info_btc)
        assert not res.executed
        assert res.reject_reason == "invalid_delta", f"got {res.reject_reason} for {bad!r}"


def test_F5_invalid_coin_info_qty_step_rejection(ledger):
    """codex m05 r1 fix: bad qty_step rejected."""
    from v13_execution_realism import execute_or_skip, CoinInfo
    bad_ci = CoinInfo(coin="BTC", tier="majors", tick_size=0.5, qty_step=0, min_order_usd=10.0)
    res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=50_000.0,
                          ledger=ledger, coin_info=bad_ci)
    assert not res.executed
    assert "qty_step" in res.reject_reason

    bad_ci_nan = CoinInfo(coin="BTC", tier="majors", tick_size=0.5, qty_step=float('nan'), min_order_usd=10.0)
    res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=50_000.0,
                          ledger=ledger, coin_info=bad_ci_nan)
    assert not res.executed
    assert "qty_step" in res.reject_reason


def test_F5_invalid_coin_info_min_order_rejection(ledger):
    """codex m05 r1 fix: bad min_order_usd rejected."""
    from v13_execution_realism import execute_or_skip, CoinInfo
    bad_ci = CoinInfo(coin="BTC", tier="majors", tick_size=0.5, qty_step=0.0001, min_order_usd=float('nan'))
    res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=50_000.0,
                          ledger=ledger, coin_info=bad_ci)
    assert not res.executed
    assert "min_order" in res.reject_reason


def test_F5_huge_int_rejected_not_overflow_error(ledger, coin_info_btc):
    """codex m05 r4 fix: huge Python int (>float max as int) MUST reject cleanly, NOT leak OverflowError."""
    from v13_execution_realism import execute_or_skip, CoinInfo
    huge = 10 ** 10000

    # delta_usd huge int
    res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=huge, mark_at_exec=50_000.0,
                          ledger=ledger, coin_info=coin_info_btc)
    assert not res.executed
    assert res.reject_reason == "invalid_delta"

    # mark_at_exec huge int
    res2 = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=huge,
                           ledger=ledger, coin_info=coin_info_btc)
    assert not res2.executed
    assert res2.reject_reason == "invalid_mark"

    # qty_step huge int
    bad_ci = CoinInfo(coin="BTC", tier="majors", tick_size=0.5, qty_step=huge, min_order_usd=10.0)
    res3 = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=50_000.0,
                           ledger=ledger, coin_info=bad_ci)
    assert not res3.executed
    assert "qty_step" in res3.reject_reason


def test_F5_bool_rejected_in_coin_info(ledger):
    """codex m05 r2 fix: bool subclasses int — must be explicitly rejected from qty_step/min_order."""
    from v13_execution_realism import execute_or_skip, CoinInfo
    # bool qty_step
    bad_ci = CoinInfo(coin="BTC", tier="majors", tick_size=0.5, qty_step=True, min_order_usd=10.0)
    res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=50_000.0,
                          ledger=ledger, coin_info=bad_ci)
    assert not res.executed
    assert "qty_step" in res.reject_reason
    # bool min_order (False=0 would silently allow sub-minimum orders)
    bad_ci2 = CoinInfo(coin="BTC", tier="majors", tick_size=0.5, qty_step=0.0001, min_order_usd=False)
    res2 = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=50_000.0,
                           ledger=ledger, coin_info=bad_ci2)
    assert not res2.executed
    assert "min_order" in res2.reject_reason


def test_F5_classify_coin_tier_case_insensitive():
    """codex m05 r1 fix: pinned majors case-normalized."""
    from v13_execution_realism import classify_coin_tier
    assert classify_coin_tier("btc", 0) == "majors"
    assert classify_coin_tier("eth", 0) == "majors"
    assert classify_coin_tier("Sol", 0) == "majors"
    assert classify_coin_tier("hype", 0) == "majors"


def test_F5_qty_rounded_to_qty_step(ledger, coin_info_btc):
    """qty MUST be multiple of qty_step."""
    from v13_execution_realism import execute_or_skip
    mark = 50_000.0
    res = execute_or_skip("BTC", copy_exec_ts=1000, delta_usd=1000.0, mark_at_exec=mark,
                          ledger=ledger, coin_info=coin_info_btc)
    assert res.executed
    # qty should be exactly a multiple of qty_step=0.0001
    qty = res.leg.qty
    n_steps = round(qty / coin_info_btc.qty_step)
    assert qty == pytest.approx(n_steps * coin_info_btc.qty_step, abs=1e-12)
