"""V13 Module 06 — Cold-Start State Machine tests.

Maps to per-Module 12 spec fixtures F6-1 through F6-5.
"""
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))


def test_F6_1_pending_flat_initial_state_for_new_pool_entry():
    from v13_cold_start import ColdStartState, PENDING_FLAT, ALLOWED
    cs = ColdStartState()
    cs.initialize_pool_entry({"BTC": 0.5, "ETH": -1.0, "SOL": 0.0})
    assert cs.state["BTC"] == PENDING_FLAT
    assert cs.state["ETH"] == PENDING_FLAT  # short still triggers
    assert "SOL" not in cs.state  # zero → implicit ALLOWED, not tracked
    assert cs.is_allowed("BTC") is False
    assert cs.is_allowed("ETH") is False
    assert cs.is_allowed("SOL") is True  # implicit
    assert cs.is_allowed("ADA") is True  # never seen → implicit ALLOWED


def test_F6_2_pending_to_allowed_on_source_flat():
    from v13_cold_start import ColdStartState
    cs = ColdStartState()
    cs.initialize_pool_entry({"BTC": 0.5})
    assert cs.is_allowed("BTC") is False
    # Source still holds BTC at next poll → no transition
    cs.update_from_poll({"BTC": 0.5})
    assert cs.is_allowed("BTC") is False
    # Source flattens BTC → transition
    n_trans = cs.update_from_poll({"BTC": 0.0})
    assert n_trans == 1
    assert cs.is_allowed("BTC") is True


def test_F6_3_allowed_stays_allowed_on_source_nonzero():
    """ALLOWED is terminal — once transitioned, source re-opening doesn't push back to PENDING."""
    from v13_cold_start import ColdStartState
    cs = ColdStartState()
    cs.initialize_pool_entry({"BTC": 0.5})
    cs.update_from_poll({"BTC": 0.0})  # transition to ALLOWED
    assert cs.is_allowed("BTC") is True
    # Source re-opens
    cs.update_from_poll({"BTC": 1.0})
    assert cs.is_allowed("BTC") is True  # stays ALLOWED


def test_F6_4_new_pool_entry_while_source_mid_trade_stays_pending():
    """Source has open ETH at pool entry; never flattens → stays PENDING forever."""
    from v13_cold_start import ColdStartState
    cs = ColdStartState()
    cs.initialize_pool_entry({"ETH": 2.0})
    # 10 polls, source never flat
    for _ in range(10):
        cs.update_from_poll({"ETH": 2.0})
    assert cs.is_allowed("ETH") is False
    # Now source flattens → ALLOWED
    cs.update_from_poll({"ETH": 0.0})
    assert cs.is_allowed("ETH") is True


def test_F6_intra_poll_flat_detection_backtest():
    """Backtest can use fills to detect intra-poll flat."""
    from v13_cold_start import ColdStartState
    cs = ColdStartState()
    cs.initialize_pool_entry({"BTC": 0.5})
    # Fills in this poll: source sold 0.5 (flat) then bought 0.3 (re-open)
    fills = [
        {"time": 1, "signed_sz": -0.5},  # flat at time 1
        {"time": 2, "signed_sz": 0.3},   # re-open at time 2
    ]
    transitioned = cs.detect_intra_poll_flat_from_fills("BTC", starting_position=0.5, fills_in_poll=fills)
    assert transitioned
    assert cs.is_allowed("BTC") is True


def test_F6_intra_poll_no_flat_does_not_transition():
    from v13_cold_start import ColdStartState
    cs = ColdStartState()
    cs.initialize_pool_entry({"BTC": 0.5})
    # Fills add to position; never flat
    fills = [
        {"time": 1, "signed_sz": 0.2},
        {"time": 2, "signed_sz": 0.1},
    ]
    transitioned = cs.detect_intra_poll_flat_from_fills("BTC", starting_position=0.5, fills_in_poll=fills)
    assert not transitioned
    assert cs.is_allowed("BTC") is False


def test_F6_intra_poll_only_works_on_pending():
    """If coin already ALLOWED, intra-poll detection is a no-op (doesn't reset state)."""
    from v13_cold_start import ColdStartState
    cs = ColdStartState()
    # Never initialized → ALLOWED by default
    transitioned = cs.detect_intra_poll_flat_from_fills("BTC", starting_position=0.0, fills_in_poll=[])
    assert not transitioned  # no PENDING state to transition
    assert cs.is_allowed("BTC") is True


def test_F6_pool_entry_negative_position_triggers_pending():
    """SHORT position at pool entry must also trigger PENDING_FLAT (not just longs)."""
    from v13_cold_start import ColdStartState
    cs = ColdStartState()
    cs.initialize_pool_entry({"BTC": -2.0})
    assert cs.is_allowed("BTC") is False
    # Source flattens short
    cs.update_from_poll({"BTC": 0.0})
    assert cs.is_allowed("BTC") is True


def test_F6_intra_poll_sorts_unsorted_fills():
    """codex m06 r2 fix: fills_in_poll sorted by time before walking. Unsorted input
    must NOT miss a true intra-poll flat."""
    from v13_cold_start import ColdStartState
    cs = ColdStartState()
    cs.initialize_pool_entry({"BTC": 0.5})
    # Unsorted fills: chronologically (-1.0 at t=1, +0.6 at t=2) crosses flat at t=1
    fills_unsorted = [
        {"time": 2, "signed_sz": 0.6},
        {"time": 1, "signed_sz": -1.0},
    ]
    transitioned = cs.detect_intra_poll_flat_from_fills("BTC", starting_position=0.5, fills_in_poll=fills_unsorted)
    assert transitioned, "unsorted fills must still detect intra-poll flat after sort"
    assert cs.is_allowed("BTC")


def test_F6_intra_poll_drops_fills_missing_time():
    from v13_cold_start import ColdStartState
    cs = ColdStartState()
    cs.initialize_pool_entry({"BTC": 0.5})
    fills = [
        {"signed_sz": -1.0},  # missing time - dropped
        {"time": 1, "signed_sz": -0.4},  # would NOT reach flat from 0.5 alone (running=0.1)
    ]
    transitioned = cs.detect_intra_poll_flat_from_fills("BTC", starting_position=0.5, fills_in_poll=fills)
    # First fill dropped; second alone doesn't cross flat → no transition
    assert not transitioned


def test_F6_terminal_allowed_preserved_on_reinit():
    """codex m06 r1 fix: ALLOWED is terminal; later initialize_pool_entry must NOT regress to PENDING."""
    from v13_cold_start import ColdStartState, ALLOWED
    cs = ColdStartState()
    cs.initialize_pool_entry({"BTC": 0.5})
    cs.update_from_poll({"BTC": 0.0})  # transition to ALLOWED
    assert cs.is_allowed("BTC")
    # Re-initialize (e.g., wallet re-added to pool with current open position)
    cs.initialize_pool_entry({"BTC": 1.0})
    # MUST stay ALLOWED — terminal
    assert cs.is_allowed("BTC"), "ALLOWED terminal violated by re-init"
    assert cs.state["BTC"] == ALLOWED


def test_F6_intra_poll_sign_crossing_detects_flat():
    """codex m06 r1 fix: single fill flipping sign (e.g., +0.5 → -0.5 via -1.0 fill)
    must transition to ALLOWED (necessarily crossed flat)."""
    from v13_cold_start import ColdStartState
    cs = ColdStartState()
    cs.initialize_pool_entry({"BTC": 0.5})
    # Single fill that flips sign
    fills = [{"time": 1, "signed_sz": -1.0}]
    transitioned = cs.detect_intra_poll_flat_from_fills("BTC", starting_position=0.5, fills_in_poll=fills)
    assert transitioned, "sign crossing should detect flat"
    assert cs.is_allowed("BTC")


def test_F6_eps_boundary_consistent():
    """codex m06 r1 fix: pool entry uses > EPS, poll update uses <= EPS. Symmetric."""
    from v13_cold_start import ColdStartState, EPS
    cs = ColdStartState()
    cs.initialize_pool_entry({"BTC": EPS})  # at-EPS is NOT > EPS → not tracked → implicit ALLOWED
    assert "BTC" not in cs.state

    cs2 = ColdStartState()
    cs2.initialize_pool_entry({"BTC": EPS * 2})  # > EPS → PENDING
    assert not cs2.is_allowed("BTC")
    cs2.update_from_poll({"BTC": EPS})  # ≤ EPS → flat → ALLOWED
    assert cs2.is_allowed("BTC")


def test_F6_dust_position_at_pool_entry_treated_as_zero():
    """Position smaller than EPS → not tracked (already effectively flat)."""
    from v13_cold_start import ColdStartState
    cs = ColdStartState()
    cs.initialize_pool_entry({"BTC": 1e-12})  # dust
    assert "BTC" not in cs.state
    assert cs.is_allowed("BTC") is True
