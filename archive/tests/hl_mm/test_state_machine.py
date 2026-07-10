"""Tests for state_machine.py — Per-pair state transitions and output flags."""
import time
import pytest

from app.services.hl_mm.state_machine import StateMachine, PairState, PairContext, PairStateInfo


@pytest.fixture
def sm():
    return StateMachine(pause_cooldown_s=10.0)


def _ctx(**kwargs) -> PairContext:
    """Helper to build context with sensible defaults for healthy quoting."""
    defaults = dict(
        hl_book_fresh=True,
        bybit_anchor_healthy=True,
        hl_book_age_ms=100.0,
        spread_threshold_met=True,
        bid_side_ev_positive=True,
        ask_side_ev_positive=True,
        inventory_usd=0.0,
        q_soft=60.0,
        q_hard=80.0,
        q_emergency=100.0,
        inventory_age_s=0.0,
        adverse_move_bps=0.0,
        circuit_breaker_active=False,
        oms_mismatch=False,
        regime_shock=False,
        strong_imbalance=False,
        imbalance_side=0,
        hedge_in_progress=False,
        bybit_hedge_available=True,
        native_spread_bps=10.0,
    )
    defaults.update(kwargs)
    return PairContext(**defaults)


class TestIdleToQuoting:
    def test_idle_to_quoting_both_on_healthy_data(self, sm):
        sm.register_pair("BIO")
        info = sm.transition("BIO", _ctx())
        assert info.state == PairState.QUOTING_BOTH
        assert info.quote_bid is True
        assert info.quote_ask is True

    def test_idle_stays_if_spread_not_met(self, sm):
        sm.register_pair("BIO")
        info = sm.transition("BIO", _ctx(spread_threshold_met=False))
        assert info.state == PairState.IDLE

    def test_idle_to_one_side_if_only_bid_ev(self, sm):
        sm.register_pair("BIO")
        info = sm.transition("BIO", _ctx(ask_side_ev_positive=False))
        assert info.state == PairState.QUOTING_ONE_SIDE


class TestPauseTransitions:
    def test_circuit_breaker_causes_pause(self, sm):
        sm.register_pair("BIO")
        sm.transition("BIO", _ctx())  # to QUOTING_BOTH
        info = sm.transition("BIO", _ctx(circuit_breaker_active=True))
        assert info.state == PairState.PAUSE
        assert info.quote_bid is False
        assert info.quote_ask is False

    def test_stale_data_causes_pause(self, sm):
        sm.register_pair("BIO")
        sm.transition("BIO", _ctx())
        info = sm.transition("BIO", _ctx(hl_book_fresh=False, hl_book_age_ms=2000))
        assert info.state == PairState.PAUSE

    def test_pause_does_not_preempt_emergency_flatten(self, sm):
        sm.register_pair("BIO")
        sm.force_state("BIO", PairState.EMERGENCY_FLATTEN, "test")
        info = sm.transition("BIO", _ctx(circuit_breaker_active=True, inventory_usd=50.0))
        assert info.state == PairState.EMERGENCY_FLATTEN


class TestInventoryExit:
    def test_quoting_to_inventory_exit_on_soft_limit(self, sm):
        sm.register_pair("BIO")
        sm.transition("BIO", _ctx())  # QUOTING_BOTH
        info = sm.transition("BIO", _ctx(inventory_usd=65.0, inventory_age_s=5.0))
        assert info.state == PairState.INVENTORY_EXIT

    def test_no_exit_with_zero_inventory(self, sm):
        """Bug #2 fix: age alone shouldn't trigger exit with zero inventory."""
        sm.register_pair("BIO")
        sm.transition("BIO", _ctx())  # QUOTING_BOTH
        info = sm.transition("BIO", _ctx(inventory_usd=0.0, inventory_age_s=50.0))
        assert info.state != PairState.INVENTORY_EXIT

    def test_inventory_exit_to_idle_when_flat(self, sm):
        sm.register_pair("BIO")
        sm.force_state("BIO", PairState.INVENTORY_EXIT, "test")
        info = sm.transition("BIO", _ctx(inventory_usd=2.0))
        assert info.state == PairState.IDLE

    def test_inventory_exit_quotes_reduce_side_only(self, sm):
        sm.register_pair("BIO")
        sm.force_state("BIO", PairState.INVENTORY_EXIT, "test")
        info = sm.transition("BIO", _ctx(inventory_usd=50.0))
        assert info.quote_bid is False
        assert info.quote_ask is True  # sell to reduce long


class TestEmergencyFlatten:
    def test_triggers_on_old_age(self, sm):
        sm.register_pair("BIO")
        sm.force_state("BIO", PairState.INVENTORY_EXIT, "test")
        info = sm.transition("BIO", _ctx(inventory_usd=50.0, inventory_age_s=125.0))
        assert info.state == PairState.EMERGENCY_FLATTEN

    def test_does_not_trigger_at_moderate_age(self, sm):
        """45s old position should NOT taker close — give maker a chance."""
        sm.register_pair("BIO")
        sm.force_state("BIO", PairState.INVENTORY_EXIT, "test")
        info = sm.transition("BIO", _ctx(inventory_usd=50.0, inventory_age_s=50.0))
        assert info.state != PairState.EMERGENCY_FLATTEN

    def test_triggers_on_adverse_move(self, sm):
        sm.register_pair("BIO")
        sm.force_state("BIO", PairState.INVENTORY_EXIT, "test")
        info = sm.transition("BIO", _ctx(inventory_usd=50.0, adverse_move_bps=25.0))
        assert info.state == PairState.EMERGENCY_FLATTEN

    def test_does_not_trigger_at_moderate_adverse(self, sm):
        """8bps adverse should NOT taker close — normal shitcoin noise."""
        sm.register_pair("BIO")
        sm.force_state("BIO", PairState.INVENTORY_EXIT, "test")
        info = sm.transition("BIO", _ctx(inventory_usd=50.0, adverse_move_bps=10.0))
        assert info.state != PairState.EMERGENCY_FLATTEN

    def test_reason_string_matches_threshold(self, sm):
        sm.register_pair("BIO")
        sm.force_state("BIO", PairState.INVENTORY_EXIT, "test")
        info = sm.transition("BIO", _ctx(inventory_usd=50.0, inventory_age_s=125.0))
        assert "120s" in info.reason


class TestForceState:
    def test_force_pause(self, sm):
        sm.register_pair("BIO")
        sm.force_pause("BIO", 30.0, "test reason")
        info = sm.get_state("BIO")
        assert info.state == PairState.PAUSE
        assert info.pause_until > time.time()

    def test_force_state_arbitrary(self, sm):
        sm.register_pair("BIO")
        sm.force_state("BIO", PairState.EMERGENCY_FLATTEN, "manual override")
        info = sm.get_state("BIO")
        assert info.state == PairState.EMERGENCY_FLATTEN
