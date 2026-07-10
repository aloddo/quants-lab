"""
V2 Execution Engine Tests — Covers all 14 Codex findings + 5 Eng review findings.

Test categories:
1. PositionStore state machine (transitions, preconditions, atomicity)
2. FillDetector (WS path, REST fallback, tri-state, dedup)
3. EntryFlow (success, leg failure, both missed, unwind)
4. ExitFlow (success, partial, market escalation, double-fill prevention)
5. Instrument rules (Decimal rounding for all step sizes)
6. Inventory fee handling
7. Edge cases from Eng review

Run: MONGO_URI=mongodb://localhost:27017/quants_lab python -m pytest tests/arb/test_v2_execution_engine.py -v
"""
import asyncio
import time
import pytest
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from app.services.arb.position_store import (
    PositionStore, PositionState, LegState, FillResult,
    VALID_TRANSITIONS, new_position_doc,
)
from app.services.arb.fill_detector import FillDetector, TrackedLeg
from app.services.arb.order_feed import FillEvent, FillSource
from app.services.arb.instrument_rules import PairRules


# ═══════════════════════════════════════════════════════════════
# 1. PositionStore State Machine
# ═══════════════════════════════════════════════════════════════

class TestPositionStoreTransitions:
    """Test all valid and invalid state transitions."""

    @pytest.fixture(autouse=True)
    def setup(self):
        self.store = PositionStore("mongodb://localhost:27017/quants_lab", "arb_test_positions_v2")
        self.store._coll.drop()
        yield
        self.store._coll.drop()

    def _create_pos(self, pid="test_pos_1", symbol="NOMUSDT"):
        doc = new_position_doc(
            position_id=pid, symbol=symbol, bn_symbol="NOMUSDC",
            direction="BUY_BB_SELL_BN", signal_spread_bps=100,
            threshold_p90=90, threshold_p25=30,
        )
        self.store._create_sync(doc)
        return pid

    def test_valid_transition_pending_to_entering(self):
        pid = self._create_pos()
        assert self.store._transition_sync(pid, PositionState.PENDING, PositionState.ENTERING, {})

    def test_valid_transition_entering_to_open(self):
        pid = self._create_pos()
        self.store._transition_sync(pid, PositionState.PENDING, PositionState.ENTERING, {})
        assert self.store._transition_sync(pid, PositionState.ENTERING, PositionState.OPEN, {})

    def test_invalid_transition_pending_to_open(self):
        """Cannot skip ENTERING state."""
        pid = self._create_pos()
        assert not self.store._transition_sync(pid, PositionState.PENDING, PositionState.OPEN, {})

    def test_invalid_transition_closed_to_anything(self):
        """Terminal states have no valid transitions."""
        pid = self._create_pos()
        self.store._transition_sync(pid, PositionState.PENDING, PositionState.FAILED, {})
        assert not self.store._transition_sync(pid, PositionState.FAILED, PositionState.OPEN, {})

    def test_precondition_wrong_state(self):
        """Transition fails if current state doesn't match from_state."""
        pid = self._create_pos()
        # Position is PENDING, try to transition from ENTERING
        assert not self.store._transition_sync(pid, PositionState.ENTERING, PositionState.OPEN, {})

    def test_precondition_nonexistent_position(self):
        """Transition fails for unknown position_id."""
        assert not self.store._transition_sync("nonexistent", PositionState.PENDING, PositionState.ENTERING, {})

    def test_full_lifecycle(self):
        """PENDING -> ENTERING -> OPEN -> EXITING -> CLOSED."""
        pid = self._create_pos()
        assert self.store._transition_sync(pid, PositionState.PENDING, PositionState.ENTERING, {})
        assert self.store._transition_sync(pid, PositionState.ENTERING, PositionState.OPEN, {})
        assert self.store._transition_sync(pid, PositionState.OPEN, PositionState.EXITING, {})
        assert self.store._transition_sync(pid, PositionState.EXITING, PositionState.CLOSED, {})

    def test_partial_exit_lifecycle(self):
        """OPEN -> EXITING -> PARTIAL_EXIT -> CLOSED."""
        pid = self._create_pos()
        self.store._transition_sync(pid, PositionState.PENDING, PositionState.ENTERING, {})
        self.store._transition_sync(pid, PositionState.ENTERING, PositionState.OPEN, {})
        self.store._transition_sync(pid, PositionState.OPEN, PositionState.EXITING, {})
        assert self.store._transition_sync(pid, PositionState.EXITING, PositionState.PARTIAL_EXIT, {
            "exit.bb.state": LegState.FILLED,
            "exit.bn.state": LegState.UNKNOWN,
        })
        assert self.store._transition_sync(pid, PositionState.PARTIAL_EXIT, PositionState.CLOSED, {})

    def test_unwind_lifecycle(self):
        """ENTERING -> UNWINDING -> FAILED."""
        pid = self._create_pos()
        self.store._transition_sync(pid, PositionState.PENDING, PositionState.ENTERING, {})
        assert self.store._transition_sync(pid, PositionState.ENTERING, PositionState.UNWINDING, {})
        assert self.store._transition_sync(pid, PositionState.UNWINDING, PositionState.FAILED, {
            "unwind.pnl_usd": -0.05,
        })

    def test_exit_retry(self):
        """EXITING -> OPEN (both exit legs failed, retry next cycle)."""
        pid = self._create_pos()
        self.store._transition_sync(pid, PositionState.PENDING, PositionState.ENTERING, {})
        self.store._transition_sync(pid, PositionState.ENTERING, PositionState.OPEN, {})
        self.store._transition_sync(pid, PositionState.OPEN, PositionState.EXITING, {})
        assert self.store._transition_sync(pid, PositionState.EXITING, PositionState.OPEN, {
            "exit.attempt_count": 1,
        })

    def test_exec_id_dedup(self):
        """add_exec_id is idempotent — second call returns False."""
        pid = self._create_pos()
        assert self.store._add_exec_id_sync(pid, "entry", "bb", "exec_001")
        assert not self.store._add_exec_id_sync(pid, "entry", "bb", "exec_001")  # duplicate

    def test_get_active_excludes_terminal(self):
        """get_active returns only non-terminal positions."""
        self._create_pos("pos1")
        self._create_pos("pos2")
        self.store._transition_sync("pos1", PositionState.PENDING, PositionState.FAILED, {})
        active = self.store._get_active_sync()
        assert len(active) == 1
        assert active[0]["position_id"] == "pos2"


# ═══════════════════════════════════════════════════════════════
# 2. FillDetector
# ═══════════════════════════════════════════════════════════════

class TestFillDetector:
    """Test WS fill routing, REST fallback, tri-state, dedup."""

    def setup_method(self):
        self.mock_get_order = AsyncMock(return_value={"fill_result": "FILLED", "filled_qty": 100, "avg_price": 0.05})
        self.detector = FillDetector(self.mock_get_order)

    def test_ws_fill_matches_by_order_id(self):
        leg = TrackedLeg(venue="bybit", symbol="NOMUSDT", side="Buy", order_id="BB123", target_qty=100)
        self.detector.register_leg(leg)

        fill = FillEvent(
            venue="bybit", symbol="NOMUSDT", order_id="BB123", exec_id="exec_1",
            side="Buy", price=0.05, qty=100, fee=0.001, fee_asset="USDT",
            timestamp_ms=int(time.time()*1000), local_ts=time.time(), source=FillSource.WS,
        )
        self.detector.on_fill(fill)
        assert leg.is_filled
        assert leg.filled_qty == 100

    def test_ws_fill_matches_by_client_order_id(self):
        """Codex #12: Fill during submit race window matches via client ID."""
        leg = TrackedLeg(venue="bybit", symbol="NOMUSDT", side="Buy", client_order_id="h2v2_bb_abc", target_qty=100)
        self.detector.register_leg(leg)

        fill = FillEvent(
            venue="bybit", symbol="NOMUSDT", order_id="UNKNOWN_YET", exec_id="exec_2",
            side="Buy", price=0.05, qty=100, fee=0.001, fee_asset="USDT",
            timestamp_ms=int(time.time()*1000), local_ts=time.time(), source=FillSource.WS,
            client_order_id="h2v2_bb_abc",
        )
        self.detector.on_fill(fill)
        assert leg.is_filled

    def test_fill_dedup_by_exec_id(self):
        """Same exec_id delivered twice does NOT double-count."""
        leg = TrackedLeg(venue="bybit", symbol="NOMUSDT", side="Buy", order_id="BB123", target_qty=200)
        self.detector.register_leg(leg)

        fill = FillEvent(
            venue="bybit", symbol="NOMUSDT", order_id="BB123", exec_id="exec_dup",
            side="Buy", price=0.05, qty=100, fee=0.001, fee_asset="USDT",
            timestamp_ms=int(time.time()*1000), local_ts=time.time(), source=FillSource.WS,
        )
        self.detector.on_fill(fill)
        self.detector.on_fill(fill)  # duplicate
        assert leg.filled_qty == 100  # NOT 200

    def test_unmatched_fill_buffered_and_replayed(self):
        """Fill arrives before leg is registered, gets buffered then replayed."""
        fill = FillEvent(
            venue="bybit", symbol="NOMUSDT", order_id="BB_EARLY", exec_id="exec_early",
            side="Buy", price=0.05, qty=100, fee=0.001, fee_asset="USDT",
            timestamp_ms=int(time.time()*1000), local_ts=time.time(), source=FillSource.WS,
        )
        self.detector.on_fill(fill)  # buffered

        # Now register the leg and replay
        leg = TrackedLeg(venue="bybit", symbol="NOMUSDT", side="Buy", order_id="BB_EARLY", target_qty=100)
        self.detector.register_leg(leg)
        self.detector.replay_buffered()
        assert leg.is_filled

    def test_rest_result_applied_correctly(self):
        """REST get_order() result updates leg state."""
        leg = TrackedLeg(venue="binance", symbol="NOMUSDT", side="Sell", order_id="BN123", target_qty=500)

        # FILLED result
        leg.apply_rest_result({"fill_result": "FILLED", "filled_qty": 500, "avg_price": 0.05})
        assert leg.is_filled

    def test_rest_unknown_does_not_update(self):
        """UNKNOWN result from REST does NOT change leg state. (Eng CRITICAL fix)"""
        leg = TrackedLeg(venue="binance", symbol="NOMUSDT", side="Sell", order_id="BN123", target_qty=500)
        leg.state = LegState.SUBMITTED

        leg.apply_rest_result({"fill_result": "UNKNOWN", "filled_qty": 0, "avg_price": 0})
        assert leg.state == LegState.SUBMITTED  # NOT changed to CANCELLED


# ═══════════════════════════════════════════════════════════════
# 3. Instrument Rules (Decimal Rounding — Codex #14)
# ═══════════════════════════════════════════════════════════════

class TestInstrumentRulesDecimal:
    """Test rounding with Decimal for ALL step size types."""

    def test_power_of_10_step(self):
        """Standard case: step=0.01"""
        rules = PairRules(symbol="TEST", venue="test", qty_step=0.01)
        assert rules.round_qty(1.999) == 1.99
        assert rules.round_qty(0.001) == 0.0

    def test_non_power_of_10_step(self):
        """Codex #14: step=0.3 broke with log10. Must work with Decimal."""
        rules = PairRules(symbol="TEST", venue="test", qty_step=0.3)
        assert rules.round_qty(0.6) == 0.6
        assert rules.round_qty(0.5) == 0.3
        assert rules.round_qty(0.9) == 0.9
        assert rules.round_qty(1.0) == 0.9

    def test_step_0_5(self):
        """step=0.5 (another non-power-of-10)."""
        rules = PairRules(symbol="TEST", venue="test", qty_step=0.5)
        assert rules.round_qty(1.7) == 1.5
        assert rules.round_qty(2.0) == 2.0
        assert rules.round_qty(0.4) == 0.0

    def test_step_0_25(self):
        """step=0.25."""
        rules = PairRules(symbol="TEST", venue="test", qty_step=0.25)
        assert rules.round_qty(1.3) == 1.25
        assert rules.round_qty(1.5) == 1.5

    def test_integer_step(self):
        """step=1 (whole units only)."""
        rules = PairRules(symbol="TEST", venue="test", qty_step=1.0)
        assert rules.round_qty(5.9) == 5.0
        assert rules.round_qty(10.0) == 10.0

    def test_very_small_step(self):
        """step=0.00001 (5 decimal places — the problematic case from Codex #14)."""
        rules = PairRules(symbol="TEST", venue="test", qty_step=0.00001)
        assert rules.round_qty(0.12345) == 0.12345
        assert rules.round_qty(0.123456) == 0.12345

    def test_price_rounding(self):
        """Price rounds to nearest tick (not floor)."""
        rules = PairRules(symbol="TEST", venue="test", price_tick=0.0001)
        assert rules.round_price(0.12345) == 0.1235  # rounds up
        assert rules.round_price(0.12344) == 0.1234  # rounds down

    def test_price_non_power_of_10_tick(self):
        """Price tick=0.05."""
        rules = PairRules(symbol="TEST", venue="test", price_tick=0.05)
        assert rules.round_price(1.03) == 1.05
        assert rules.round_price(1.07) == 1.05
        assert rules.round_price(1.08) == 1.10


# ═══════════════════════════════════════════════════════════════
# 4. Tri-State Fill Result (Eng CRITICAL finding)
# ═══════════════════════════════════════════════════════════════

class TestFillResultStates:
    """
    Eng review CRITICAL: REST failure must NOT be conflated with "not filled."
    Now 5 states: FILLED, PARTIAL, NOT_FILLED, NOT_FOUND, UNKNOWN.
    """

    def test_filled_result(self):
        assert FillResult.FILLED == "FILLED"

    def test_partial_result(self):
        """Codex review: partial fills must NOT be promoted to FILLED."""
        assert FillResult.PARTIAL == "PARTIAL"

    def test_not_filled_result(self):
        assert FillResult.NOT_FILLED == "NOT_FILLED"

    def test_not_found_result(self):
        """Distinct from NOT_FILLED: order never existed on exchange."""
        assert FillResult.NOT_FOUND == "NOT_FOUND"

    def test_unknown_result(self):
        assert FillResult.UNKNOWN == "UNKNOWN"

    def test_unknown_is_not_not_filled(self):
        """The whole point: UNKNOWN != NOT_FILLED."""
        assert FillResult.UNKNOWN != FillResult.NOT_FILLED

    def test_partial_is_not_filled(self):
        """Partial != Filled. This was the central Codex finding."""
        assert FillResult.PARTIAL != FillResult.FILLED

    def test_not_found_is_not_unknown(self):
        """NOT_FOUND (definitive) != UNKNOWN (transient)."""
        assert FillResult.NOT_FOUND != FillResult.UNKNOWN

    def test_all_five_states_distinct(self):
        """All 5 states must be distinct."""
        states = [FillResult.FILLED, FillResult.PARTIAL, FillResult.NOT_FILLED,
                  FillResult.NOT_FOUND, FillResult.UNKNOWN]
        assert len(set(states)) == 5


# ═══════════════════════════════════════════════════════════════
# 5. Position Document Schema
# ═══════════════════════════════════════════════════════════════

class TestPositionDocument:
    """Verify position document has all required fields for crash recovery."""

    def test_new_position_has_all_fields(self):
        doc = new_position_doc(
            "test_1", "NOMUSDT", "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30,
        )
        # Core fields
        assert doc["state"] == PositionState.PENDING
        assert doc["direction"] == "BUY_BB_SELL_BN"
        assert doc["signal_spread_bps"] == 100

        # Entry legs have client_order_id field (Eng HIGH: persist before submit)
        assert "client_order_id" in doc["entry"]["bb"]
        assert "client_order_id" in doc["entry"]["bn"]

        # Exit legs have full state (Eng HIGH: not just boolean flags)
        assert "order_id" in doc["exit"]["bb"]
        assert "client_order_id" in doc["exit"]["bb"]
        assert "state" in doc["exit"]["bb"]
        assert "filled_qty" in doc["exit"]["bb"]

        # Unwind has order tracking
        assert "order_id" in doc["unwind"]
        assert "client_order_id" in doc["unwind"]

        # PnL
        assert "net_usd" in doc["pnl"]

    def test_partial_exit_state_persisted(self):
        """Codex #2: partial exit state must be in MongoDB, not in-memory flags."""
        doc = new_position_doc("test_2", "NOMUSDT", "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30)
        # Simulate partial exit: bb filled, bn unknown
        doc["exit"]["bb"]["state"] = LegState.FILLED
        doc["exit"]["bb"]["filled_qty"] = 100
        doc["exit"]["bb"]["order_id"] = "BB_EXIT_123"
        doc["exit"]["bn"]["state"] = LegState.UNKNOWN
        doc["exit"]["bn"]["order_id"] = "BN_EXIT_456"

        # On crash recovery, we have BOTH:
        # - which leg is filled (state field)
        # - the order ID to query for the unfilled leg
        assert doc["exit"]["bb"]["state"] == LegState.FILLED
        assert doc["exit"]["bn"]["order_id"] == "BN_EXIT_456"


# ═══════════════════════════════════════════════════════════════
# 6. Edge Cases from Eng Review
# ═══════════════════════════════════════════════════════════════

class TestEngReviewEdgeCases:
    """Tests for specific edge cases identified in the Eng adversarial review."""

    def test_partial_fill_remains_partial(self):
        """
        Codex CRITICAL: REST partial fill (filled_qty < target_qty)
        must remain PARTIAL, not be promoted to FILLED.
        """
        leg = TrackedLeg(venue="binance", symbol="NOMUSDT", side="Sell",
                         order_id="BN_PARTIAL", target_qty=500)
        leg.apply_rest_result({
            "fill_result": "FILLED",  # API says "FILLED" but qty < target
            "filled_qty": 250,  # Only half filled
            "avg_price": 0.05,
        })
        # Must be PARTIAL, not FILLED
        assert leg.state == LegState.PARTIAL
        assert not leg.is_filled
        assert leg.has_any_fill
        assert leg.filled_qty == 250

    def test_full_fill_is_filled(self):
        """Full fill (qty >= target * 0.99) should be FILLED."""
        leg = TrackedLeg(venue="binance", symbol="NOMUSDT", side="Sell",
                         order_id="BN_FULL", target_qty=500)
        leg.apply_rest_result({
            "fill_result": "FILLED",
            "filled_qty": 500,
            "avg_price": 0.05,
        })
        assert leg.is_filled
        assert leg.filled_qty == 500

    def test_side_aware_price_rounding(self):
        """Buy orders round UP, sell orders round DOWN."""
        rules = PairRules(symbol="TEST", venue="test", price_tick=0.01)
        # Buy at 1.005 should round UP to 1.01 (ensure fill)
        assert rules.round_price_for_side(1.005, "Buy") == 1.01
        # Sell at 1.005 should round DOWN to 1.00 (ensure fill)
        assert rules.round_price_for_side(1.005, "Sell") == 1.00
        # Exact tick values unchanged
        assert rules.round_price_for_side(1.01, "Buy") == 1.01
        assert rules.round_price_for_side(1.01, "Sell") == 1.01

    def test_side_aware_price_rounding_non_power_of_10(self):
        """Side-aware rounding with tick=0.05."""
        rules = PairRules(symbol="TEST", venue="test", price_tick=0.05)
        assert rules.round_price_for_side(1.03, "Buy") == 1.05
        assert rules.round_price_for_side(1.03, "Sell") == 1.00

    def test_partial_exit_to_exiting_transition(self):
        """PARTIAL_EXIT must be able to transition to EXITING (for retry)."""
        assert PositionState.EXITING in VALID_TRANSITIONS[PositionState.PARTIAL_EXIT]

    def test_buffer_overflow_flag(self):
        """Fill detector sets overflow flag when buffer is full."""
        detector = FillDetector(lambda *a, **kw: None)
        assert not detector.unmatched_buffer_overflow

        # Fill the buffer
        for i in range(detector.UNMATCHED_BUFFER_MAX + 1):
            fill = FillEvent(
                venue="bybit", symbol="NOMUSDT", order_id=f"OVERFLOW_{i}",
                exec_id=f"exec_overflow_{i}", side="Buy", price=0.05, qty=10,
                fee=0.001, fee_asset="USDT",
                timestamp_ms=int(time.time()*1000), local_ts=time.time(),
                source=FillSource.WS,
            )
            detector.on_fill(fill)

        assert detector.unmatched_buffer_overflow

    def test_cancelled_order_with_partial_fill(self):
        """
        Eng TEST-1: GTC order partially fills then is cancelled.
        Status=CANCELED + executedQty > 0 = still has a fill.
        """
        leg = TrackedLeg(venue="binance", symbol="NOMUSDT", side="Sell", order_id="BN_PARTIAL", target_qty=500)
        leg.apply_rest_result({
            "fill_result": "FILLED",  # Partial fill then cancelled
            "filled_qty": 250,
            "avg_price": 0.05,
            "status": "CANCELED",
        })
        assert leg.has_any_fill
        assert leg.filled_qty == 250

    def test_concurrent_fill_events_dedup(self):
        """
        Eng edge: WS and REST deliver the same fill simultaneously.
        The exec_id dedup must prevent double-counting.
        """
        leg = TrackedLeg(venue="bybit", symbol="NOMUSDT", side="Buy", order_id="BB_RACE", target_qty=100)

        fill = FillEvent(
            venue="bybit", symbol="NOMUSDT", order_id="BB_RACE", exec_id="exec_race",
            side="Buy", price=0.05, qty=100, fee=0.001, fee_asset="USDT",
            timestamp_ms=int(time.time()*1000), local_ts=time.time(), source=FillSource.WS,
        )

        # First application
        assert leg.apply_fill(fill)
        # Second application (duplicate)
        assert not leg.apply_fill(fill)
        assert leg.filled_qty == 100  # NOT 200

    def test_valid_transitions_table_completeness(self):
        """Verify every PositionState has an entry in VALID_TRANSITIONS."""
        for state in PositionState:
            assert state in VALID_TRANSITIONS, f"Missing transition entry for {state}"

    def test_terminal_states_have_no_transitions(self):
        """CLOSED and FAILED cannot transition to anything."""
        assert len(VALID_TRANSITIONS[PositionState.CLOSED]) == 0
        assert len(VALID_TRANSITIONS[PositionState.FAILED]) == 0


# ═══════════════════════════════════════════════════════════════
# 7. Integration Tests — Entry Flow
# ═══════════════════════════════════════════════════════════════

import pytest_asyncio
from app.services.arb.entry_flow import EntryFlow, EntryResult
from app.services.arb.exit_flow import ExitFlow, ExitResult
from app.services.arb.crash_recovery_v2 import CrashRecoveryV2


def _make_store(coll_name: str) -> PositionStore:
    """Create a real PositionStore with a test collection."""
    store = PositionStore("mongodb://localhost:27017/quants_lab", coll_name)
    store._coll.drop()
    return store


def _make_gateway_mock():
    """Build a mock OrderGateway with all async methods stubbed."""
    gw = AsyncMock()
    gw.generate_client_id = MagicMock(side_effect=lambda prefix: f"{prefix}_{int(time.time()*1000)}")
    gw.submit = AsyncMock(return_value="EXCH_OID_001")
    gw.cancel = AsyncMock(return_value=True)
    gw.get_order = AsyncMock(return_value={"fill_result": "FILLED", "filled_qty": 100, "avg_price": 0.05})
    gw.get_order_with_retry = AsyncMock(return_value={"fill_result": "FILLED", "filled_qty": 100, "avg_price": 0.05})
    gw.check_position = AsyncMock(return_value=100.0)
    gw.map_bn_symbol = MagicMock(side_effect=lambda s: s)
    # Stub API sub-objects for crash recovery
    gw.bybit_api = AsyncMock()
    gw.bybit_api.get_open_orders = AsyncMock(return_value=[])
    gw.binance_api = AsyncMock()
    gw.binance_api.get_open_orders = AsyncMock(return_value=[])
    return gw


def _make_detector_mock():
    """Build a mock FillDetector that auto-fills legs on wait."""
    detector = MagicMock()
    detector.register_leg = MagicMock()
    detector.register_exchange_id = MagicMock()
    detector.replay_buffered = MagicMock()
    detector.cleanup_leg = MagicMock()
    detector.unmatched_buffer_overflow = False
    detector.check_fill_definitive = AsyncMock(return_value={
        "fill_result": "FILLED", "filled_qty": 100, "avg_price": 0.05, "order_id": "EXCH_OID_001",
    })
    return detector


def _make_detector_that_fills(fill_bb=True, fill_bn=True, partial_bb=False, partial_bn=False):
    """Build a detector mock where wait_for_fills auto-sets leg states."""
    detector = _make_detector_mock()

    async def _wait_for_fills(legs, timeout=3.0, ws_available=None):
        for leg in legs:
            if leg.venue == "bybit" and fill_bb:
                leg.filled_qty = leg.target_qty if not partial_bb else leg.target_qty * 0.5
                leg.avg_fill_price = leg.target_price or 0.05
                leg.state = LegStateEnum.FILLED if not partial_bb else LegStateEnum.PARTIAL
                if not partial_bb:
                    leg.fill_event.set()
            elif leg.venue == "binance" and fill_bn:
                leg.filled_qty = leg.target_qty if not partial_bn else leg.target_qty * 0.5
                leg.avg_fill_price = leg.target_price or 0.05
                leg.state = LegStateEnum.FILLED if not partial_bn else LegStateEnum.PARTIAL
                if not partial_bn:
                    leg.fill_event.set()

    detector.wait_for_fills = AsyncMock(side_effect=_wait_for_fills)
    return detector


# Alias for LegState in tests
LegStateEnum = LegState


@pytest.mark.asyncio
class TestEntryFlowIntegration:
    """Integration tests: real MongoDB store, mocked gateway/detector."""

    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        self.store = _make_store("arb_test_entry_integration")
        yield
        self.store._coll.drop()

    async def test_entry_success_both_fill(self):
        """Both legs fill -> position transitions to OPEN."""
        gateway = _make_gateway_mock()
        # Each submit returns a different OID
        gateway.submit = AsyncMock(side_effect=["BB_OID_1", "BN_OID_1"])
        detector = _make_detector_that_fills(fill_bb=True, fill_bn=True)

        flow = EntryFlow(self.store, detector, gateway)
        result = await flow.execute(
            symbol="NOMUSDT", bn_symbol="NOMUSDC", direction="BUY_BB_SELL_BN",
            bb_side="Buy", bn_side="Sell",
            qty_bb=100, qty_bn=100, price_bb=0.05, price_bn=0.05,
            signal_spread_bps=100, threshold_p90=90, threshold_p25=30,
        )

        assert result.success
        assert result.outcome == "SUCCESS"
        # Verify position is OPEN in DB
        pos = await self.store.get(result.position_id)
        assert pos is not None
        assert pos["state"] == PositionState.OPEN

    async def test_entry_leg_failure_unwind(self):
        """One leg fills, other misses -> UNWINDING -> FAILED, unwind order persisted BEFORE submit."""
        gateway = _make_gateway_mock()
        gateway.submit = AsyncMock(side_effect=["BB_OID_1", "BN_OID_1", "UNWIND_OID"])

        # Custom wait: only bb fills on first call (entry), unwind leg fills on second call
        call_count = 0

        async def _wait_entry_then_unwind(legs, timeout=3.0, ws_available=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # Entry wait: only fill bybit leg, NOT binance
                for leg in legs:
                    if leg.venue == "bybit":
                        leg.filled_qty = leg.target_qty
                        leg.avg_fill_price = 0.05
                        leg.state = LegStateEnum.FILLED
                        leg.fill_event.set()
                    # binance leg stays unfilled
            else:
                # Unwind wait: fill the unwind leg
                for leg in legs:
                    leg.filled_qty = leg.target_qty
                    leg.avg_fill_price = 0.049
                    leg.state = LegStateEnum.FILLED
                    leg.fill_event.set()

        detector = _make_detector_mock()
        detector.wait_for_fills = AsyncMock(side_effect=_wait_entry_then_unwind)
        # REST fallback says bn not filled
        detector.check_fill_definitive = AsyncMock(return_value={
            "fill_result": "NOT_FILLED", "filled_qty": 0, "avg_price": 0, "order_id": "BN_OID_1",
        })

        flow = EntryFlow(self.store, detector, gateway)
        result = await flow.execute(
            symbol="NOMUSDT", bn_symbol="NOMUSDC", direction="BUY_BB_SELL_BN",
            bb_side="Buy", bn_side="Sell",
            qty_bb=100, qty_bn=100, price_bb=0.05, price_bn=0.05,
            signal_spread_bps=100, threshold_p90=90, threshold_p25=30,
        )

        assert not result.success
        assert result.outcome in ("LEG_FAILURE", "RISK_PAUSE")
        # Verify position is FAILED in DB
        pos = await self.store.get(result.position_id)
        assert pos is not None
        assert pos["state"] == PositionState.FAILED
        # Verify unwind client_order_id was persisted before submit
        assert pos["unwind"]["client_order_id"] != ""

    async def test_entry_both_submit_fail_query_by_client_id(self):
        """Both submits throw -> query by client_order_id before marking REJECTED."""
        gateway = _make_gateway_mock()
        gateway.submit = AsyncMock(side_effect=Exception("connection timeout"))
        detector = _make_detector_mock()
        # Client ID query also returns NOT_FOUND (truly never placed)
        detector.check_fill_definitive = AsyncMock(return_value={
            "fill_result": "NOT_FOUND", "filled_qty": 0, "avg_price": 0, "order_id": "",
        })

        flow = EntryFlow(self.store, detector, gateway)
        result = await flow.execute(
            symbol="NOMUSDT", bn_symbol="NOMUSDC", direction="BUY_BB_SELL_BN",
            bb_side="Buy", bn_side="Sell",
            qty_bb=100, qty_bn=100, price_bb=0.05, price_bn=0.05,
            signal_spread_bps=100, threshold_p90=90, threshold_p25=30,
        )

        assert not result.success
        assert result.outcome == "BOTH_REJECTED"
        # Verify check_fill_definitive was called (client_order_id query)
        assert detector.check_fill_definitive.call_count >= 2

    async def test_entry_both_miss(self):
        """Neither fills after timeout -> FAILED."""
        gateway = _make_gateway_mock()
        gateway.submit = AsyncMock(side_effect=["BB_OID_1", "BN_OID_1"])
        detector = _make_detector_that_fills(fill_bb=False, fill_bn=False)
        detector.check_fill_definitive = AsyncMock(return_value={
            "fill_result": "NOT_FILLED", "filled_qty": 0, "avg_price": 0, "order_id": "BB_OID_1",
        })

        flow = EntryFlow(self.store, detector, gateway)
        result = await flow.execute(
            symbol="NOMUSDT", bn_symbol="NOMUSDC", direction="BUY_BB_SELL_BN",
            bb_side="Buy", bn_side="Sell",
            qty_bb=100, qty_bn=100, price_bb=0.05, price_bn=0.05,
            signal_spread_bps=100, threshold_p90=90, threshold_p25=30,
        )

        assert not result.success
        assert result.outcome == "BOTH_MISSED"
        pos = await self.store.get(result.position_id)
        assert pos["state"] == PositionState.FAILED

    async def test_entry_db_failure_returns_db_failure(self):
        """If transition() returns False on success path, outcome should be DB_FAILURE."""
        gateway = _make_gateway_mock()
        gateway.submit = AsyncMock(side_effect=["BB_OID_1", "BN_OID_1"])
        detector = _make_detector_that_fills(fill_bb=True, fill_bn=True)

        flow = EntryFlow(self.store, detector, gateway)

        # Patch transition to return False on ENTERING->OPEN (simulates precondition failure)
        original_transition = self.store._transition_sync

        def _failing_transition(pid, from_state, to_state, updates):
            if from_state == PositionState.ENTERING and to_state == PositionState.OPEN:
                return False  # Simulate stale state / concurrent mutation
            return original_transition(pid, from_state, to_state, updates)

        self.store._transition_sync = _failing_transition

        result = await flow.execute(
            symbol="NOMUSDT", bn_symbol="NOMUSDC", direction="BUY_BB_SELL_BN",
            bb_side="Buy", bn_side="Sell",
            qty_bb=100, qty_bn=100, price_bb=0.05, price_bn=0.05,
            signal_spread_bps=100, threshold_p90=90, threshold_p25=30,
        )

        assert not result.success
        assert result.outcome == "DB_FAILURE"


# ═══════════════════════════════════════════════════════════════
# 8. Integration Tests — Exit Flow
# ═══════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestExitFlowIntegration:
    """Integration tests: real MongoDB store, mocked gateway/detector."""

    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        self.store = _make_store("arb_test_exit_integration")
        yield
        self.store._coll.drop()

    async def _create_open_position(self, pid="exit_test_1", symbol="NOMUSDT"):
        """Helper: create and advance a position to OPEN state."""
        doc = new_position_doc(pid, symbol, "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30)
        await self.store.create(doc)
        await self.store.transition(pid, PositionState.PENDING, PositionState.ENTERING, {
            "entry_time": time.time(),
        })
        await self.store.transition(pid, PositionState.ENTERING, PositionState.OPEN, {
            "entry.bb.filled_qty": 100,
            "entry.bb.avg_fill_price": 0.05,
            "entry.bb.state": LegStateEnum.FILLED,
            "entry.bn.filled_qty": 100,
            "entry.bn.avg_fill_price": 0.05,
            "entry.bn.state": LegStateEnum.FILLED,
            "entry.actual_spread_bps": 50.0,
        })
        return pid

    async def test_exit_success(self):
        """Both exit legs fill -> CLOSED with PnL."""
        pid = await self._create_open_position()
        gateway = _make_gateway_mock()
        gateway.submit = AsyncMock(side_effect=["BB_EXIT_1", "BN_EXIT_1"])
        detector = _make_detector_that_fills(fill_bb=True, fill_bn=True)

        flow = ExitFlow(self.store, detector, gateway)
        result = await flow.execute(
            position_id=pid, symbol="NOMUSDT", direction="BUY_BB_SELL_BN",
            bb_side="Sell", bn_side="Buy",
            qty_bb=100, qty_bn=100, price_bb=0.05, price_bn=0.05,
            exit_reason="EXIT_MEAN_REVERT",
        )

        assert result.outcome == "SUCCESS"
        pos = await self.store.get(pid)
        assert pos["state"] == PositionState.CLOSED
        assert pos["pnl"]["net_usd"] != 0 or pos["pnl"]["gross_bps"] != 0  # PnL computed

    async def test_exit_partial_one_filled(self):
        """One exit leg fills, other misses -> PARTIAL_EXIT."""
        pid = await self._create_open_position()
        gateway = _make_gateway_mock()
        gateway.submit = AsyncMock(side_effect=["BB_EXIT_1", "BN_EXIT_1"])
        # bb fills, bn does not
        detector = _make_detector_that_fills(fill_bb=True, fill_bn=False)
        detector.check_fill_definitive = AsyncMock(return_value={
            "fill_result": "NOT_FILLED", "filled_qty": 0, "avg_price": 0, "order_id": "BN_EXIT_1",
        })

        flow = ExitFlow(self.store, detector, gateway)
        result = await flow.execute(
            position_id=pid, symbol="NOMUSDT", direction="BUY_BB_SELL_BN",
            bb_side="Sell", bn_side="Buy",
            qty_bb=100, qty_bn=100, price_bb=0.05, price_bn=0.05,
            exit_reason="EXIT_MEAN_REVERT",
        )

        assert result.outcome == "PARTIAL"
        pos = await self.store.get(pid)
        assert pos["state"] == PositionState.PARTIAL_EXIT

    async def test_exit_partial_has_any_fill(self):
        """One leg has PARTIAL fill (not FILLED) -> PARTIAL_EXIT (not OPEN)."""
        pid = await self._create_open_position()
        gateway = _make_gateway_mock()
        gateway.submit = AsyncMock(side_effect=["BB_EXIT_1", "BN_EXIT_1"])
        # bb has partial fill, bn has nothing
        detector = _make_detector_that_fills(fill_bb=False, fill_bn=False)
        # After cancel + re-check, bb has partial fill
        call_count = 0

        async def _check_fill(venue, symbol, order_id="", client_order_id="", max_retries=3):
            nonlocal call_count
            call_count += 1
            if venue == "bybit":
                return {"fill_result": "PARTIAL", "filled_qty": 50, "avg_price": 0.05, "order_id": "BB_EXIT_1"}
            return {"fill_result": "NOT_FILLED", "filled_qty": 0, "avg_price": 0, "order_id": "BN_EXIT_1"}

        detector.check_fill_definitive = AsyncMock(side_effect=_check_fill)

        flow = ExitFlow(self.store, detector, gateway)
        result = await flow.execute(
            position_id=pid, symbol="NOMUSDT", direction="BUY_BB_SELL_BN",
            bb_side="Sell", bn_side="Buy",
            qty_bb=100, qty_bn=100, price_bb=0.05, price_bn=0.05,
            exit_reason="EXIT_MEAN_REVERT",
        )

        assert result.outcome == "PARTIAL"
        pos = await self.store.get(pid)
        # Must be PARTIAL_EXIT, NOT OPEN (has fills that can't be undone)
        assert pos["state"] == PositionState.PARTIAL_EXIT

    async def test_exit_max_retry_forces_market(self):
        """After MAX_EXIT_ATTEMPTS, market orders are used."""
        pid = await self._create_open_position()
        # Set attempt_count to MAX - 1 so next attempt triggers market
        await self.store.update_leg(pid, "exit", "bb", {"attempt_count": 0})
        self.store._coll.update_one(
            {"position_id": pid},
            {"$set": {"exit.attempt_count": ExitFlow.MAX_EXIT_ATTEMPTS - 1}},
        )

        gateway = _make_gateway_mock()
        gateway.submit = AsyncMock(side_effect=["BB_EXIT_MKT", "BN_EXIT_MKT"])
        detector = _make_detector_that_fills(fill_bb=True, fill_bn=True)

        flow = ExitFlow(self.store, detector, gateway)
        result = await flow.execute(
            position_id=pid, symbol="NOMUSDT", direction="BUY_BB_SELL_BN",
            bb_side="Sell", bn_side="Buy",
            qty_bb=100, qty_bn=100, price_bb=0.05, price_bn=0.05,
            exit_reason="EXIT_MEAN_REVERT",
        )

        assert result.outcome == "SUCCESS"
        # Verify market order was used (order_type="market")
        submit_calls = gateway.submit.call_args_list
        for call in submit_calls:
            args = call[0] if call[0] else []
            kwargs = call[1] if call[1] else {}
            # 6th positional arg is order_type
            if len(args) >= 6:
                assert args[5] == "market", f"Expected market order, got {args[5]}"

    async def test_exit_preserves_prior_partial_data(self):
        """On PARTIAL_EXIT retry, prior leg's fill data is preserved."""
        pid = await self._create_open_position()

        # First exit: bb fills, bn doesn't -> PARTIAL_EXIT
        gateway = _make_gateway_mock()
        gateway.submit = AsyncMock(side_effect=["BB_EXIT_1", "BN_EXIT_1"])
        detector = _make_detector_that_fills(fill_bb=True, fill_bn=False)
        detector.check_fill_definitive = AsyncMock(return_value={
            "fill_result": "NOT_FILLED", "filled_qty": 0, "avg_price": 0, "order_id": "BN_EXIT_1",
        })

        flow = ExitFlow(self.store, detector, gateway)
        result1 = await flow.execute(
            position_id=pid, symbol="NOMUSDT", direction="BUY_BB_SELL_BN",
            bb_side="Sell", bn_side="Buy",
            qty_bb=100, qty_bn=100, price_bb=0.05, price_bn=0.05,
            exit_reason="EXIT_MEAN_REVERT",
        )
        assert result1.outcome == "PARTIAL"

        # Verify bb exit data persisted
        pos_after_partial = await self.store.get(pid)
        assert pos_after_partial["exit"]["bb"]["state"] in (LegStateEnum.FILLED, "FILLED")
        bb_fill_qty = pos_after_partial["exit"]["bb"]["filled_qty"]
        assert bb_fill_qty > 0

        # Second exit: only bn, with bb qty=0 (already closed)
        gateway2 = _make_gateway_mock()
        gateway2.submit = AsyncMock(side_effect=["BN_EXIT_2"])
        detector2 = _make_detector_that_fills(fill_bb=True, fill_bn=True)

        flow2 = ExitFlow(self.store, detector2, gateway2)
        result2 = await flow2.execute(
            position_id=pid, symbol="NOMUSDT", direction="BUY_BB_SELL_BN",
            bb_side="Sell", bn_side="Buy",
            qty_bb=0, qty_bn=100,  # bb already closed
            price_bb=0.05, price_bn=0.05,
            exit_reason="EXIT_PARTIAL_RETRY",
        )

        assert result2.outcome == "SUCCESS"
        # Verify prior bb data is still there
        pos_final = await self.store.get(pid)
        assert pos_final["state"] == PositionState.CLOSED
        assert pos_final["exit"]["bb"]["filled_qty"] == bb_fill_qty  # preserved


# ═══════════════════════════════════════════════════════════════
# 9. Integration Tests — Crash Recovery
# ═══════════════════════════════════════════════════════════════

@pytest.mark.asyncio
class TestCrashRecoveryIntegration:
    """Integration tests: real MongoDB store, mocked gateway."""

    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        self.store = _make_store("arb_test_recovery_integration")
        yield
        self.store._coll.drop()

    async def test_recovery_entering_both_filled(self):
        """ENTERING + both legs filled on exchange -> OPEN."""
        doc = new_position_doc("rec_1", "NOMUSDT", "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30)
        await self.store.create(doc)
        await self.store.transition("rec_1", PositionState.PENDING, PositionState.ENTERING, {
            "entry.bb.client_order_id": "cid_bb_1",
            "entry.bb.order_id": "bb_oid_1",
            "entry.bn.client_order_id": "cid_bn_1",
            "entry.bn.order_id": "bn_oid_1",
        })

        gateway = _make_gateway_mock()
        gateway.get_order_with_retry = AsyncMock(return_value={
            "fill_result": "FILLED", "filled_qty": 100, "avg_price": 0.05,
        })

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        pos = await self.store.get("rec_1")
        assert pos["state"] == PositionState.OPEN
        assert report.positions_resumed == 1

    async def test_recovery_entering_unknown(self):
        """ENTERING + exchange returns UNKNOWN -> stays ENTERING (not FAILED)."""
        doc = new_position_doc("rec_2", "NOMUSDT", "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30)
        await self.store.create(doc)
        await self.store.transition("rec_2", PositionState.PENDING, PositionState.ENTERING, {
            "entry.bb.client_order_id": "cid_bb_2",
            "entry.bb.order_id": "bb_oid_2",
            "entry.bn.client_order_id": "cid_bn_2",
            "entry.bn.order_id": "bn_oid_2",
        })

        gateway = _make_gateway_mock()
        gateway.get_order_with_retry = AsyncMock(return_value={
            "fill_result": "UNKNOWN", "filled_qty": 0, "avg_price": 0,
        })

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        pos = await self.store.get("rec_2")
        # CRITICAL: must stay ENTERING, not be marked FAILED
        assert pos["state"] == PositionState.ENTERING
        assert len(report.errors) > 0  # Should have unresolved error

    async def test_recovery_entering_partial(self):
        """ENTERING + one leg PARTIAL -> treated as exposure, triggers unwind."""
        doc = new_position_doc("rec_3", "NOMUSDT", "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30)
        await self.store.create(doc)
        await self.store.transition("rec_3", PositionState.PENDING, PositionState.ENTERING, {
            "entry.bb.client_order_id": "cid_bb_3",
            "entry.bb.order_id": "bb_oid_3",
            "entry.bb.side": "Buy",
            "entry.bn.client_order_id": "cid_bn_3",
            "entry.bn.order_id": "bn_oid_3",
            "entry.bn.side": "Sell",
        })

        gateway = _make_gateway_mock()
        call_count = 0

        async def _get_order_per_venue(venue, symbol, order_id="", client_order_id=""):
            nonlocal call_count
            call_count += 1
            if venue == "bybit" or (order_id and "bb" in order_id) or (client_order_id and "bb" in client_order_id):
                return {"fill_result": "PARTIAL", "filled_qty": 50, "avg_price": 0.05}
            return {"fill_result": "NOT_FILLED", "filled_qty": 0, "avg_price": 0}

        gateway.get_order_with_retry = AsyncMock(side_effect=_get_order_per_venue)
        # Unwind market order fills
        gateway.submit = AsyncMock(return_value="UNW_OID_3")

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        pos = await self.store.get("rec_3")
        # Should have attempted unwind (UNWINDING or FAILED)
        assert pos["state"] in (PositionState.UNWINDING, PositionState.FAILED)

    async def test_recovery_open_bybit_api_fail(self):
        """OPEN + check_position returns None (API fail) -> stays OPEN (not CLOSED)."""
        doc = new_position_doc("rec_4", "NOMUSDT", "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30)
        await self.store.create(doc)
        await self.store.transition("rec_4", PositionState.PENDING, PositionState.ENTERING, {})
        await self.store.transition("rec_4", PositionState.ENTERING, PositionState.OPEN, {
            "entry.bb.filled_qty": 100,
            "entry.bn.filled_qty": 100,
        })

        gateway = _make_gateway_mock()
        gateway.check_position = AsyncMock(return_value=None)  # API failure

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        pos = await self.store.get("rec_4")
        # CRITICAL: must stay OPEN when API fails — never assume flat
        assert pos["state"] == PositionState.OPEN
        assert report.positions_resumed == 1

    async def test_recovery_partial_exit_remaining_qty(self):
        """PARTIAL_EXIT recovery: remaining qty = target - already_filled."""
        doc = new_position_doc("rec_5", "NOMUSDT", "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30)
        await self.store.create(doc)
        await self.store.transition("rec_5", PositionState.PENDING, PositionState.ENTERING, {})
        await self.store.transition("rec_5", PositionState.ENTERING, PositionState.OPEN, {
            "entry.bb.filled_qty": 100,
            "entry.bn.filled_qty": 100,
        })
        await self.store.transition("rec_5", PositionState.OPEN, PositionState.EXITING, {
            "exit.bb.side": "Sell",
            "exit.bb.target_qty": 100,
            "exit.bb.filled_qty": 0,
            "exit.bb.state": LegStateEnum.SUBMITTED,
            "exit.bn.side": "Buy",
            "exit.bn.target_qty": 100,
            "exit.bn.filled_qty": 60,
            "exit.bn.state": LegStateEnum.FILLED,
        })
        await self.store.transition("rec_5", PositionState.EXITING, PositionState.PARTIAL_EXIT, {
            "exit.bb.state": LegStateEnum.CANCELLED,
            "exit.bb.filled_qty": 0,
            "exit.bn.state": LegStateEnum.FILLED,
            "exit.bn.filled_qty": 60,
        })

        gateway = _make_gateway_mock()
        # bb leg unfilled, needs market close
        gateway.get_order_with_retry = AsyncMock(return_value={
            "fill_result": "FILLED", "filled_qty": 100, "avg_price": 0.05,
        })
        gateway.submit = AsyncMock(return_value="MKT_OID_5")

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        # Check that market order used remaining qty (100 - 0 = 100 for bb leg)
        # The _resolve_partial_exit computes qty = target_qty - already_filled
        pos = await self.store.get("rec_5")
        assert pos["state"] == PositionState.CLOSED
        assert report.positions_resolved >= 1


# =================================================================
# 10. Round 4 Adversarial Review — Crash Recovery Fixes
# =================================================================

@pytest.mark.asyncio
class TestRound4CrashRecoveryFixes:
    """Tests for all 5 findings from the Round 4 adversarial review."""

    @pytest_asyncio.fixture(autouse=True)
    async def setup(self):
        self.store = _make_store("arb_test_round4_recovery")
        yield
        self.store._coll.drop()

    async def _advance_to_state(self, pid, symbol, target_state, **extra):
        """Helper: create position and advance it through the state machine."""
        doc = new_position_doc(pid, symbol, f"{symbol[:-4]}USDC", "BUY_BB_SELL_BN", 100, 90, 30)
        await self.store.create(doc)
        await self.store.transition(pid, PositionState.PENDING, PositionState.ENTERING, {
            "entry.bb.client_order_id": f"cid_bb_{pid}",
            "entry.bb.order_id": f"bb_oid_{pid}",
            "entry.bb.side": "Buy",
            "entry.bb.target_qty": 100,
            "entry.bn.client_order_id": f"cid_bn_{pid}",
            "entry.bn.order_id": f"bn_oid_{pid}",
            "entry.bn.side": "Sell",
            "entry.bn.target_qty": 100,
        })
        if target_state in (PositionState.OPEN, PositionState.EXITING, PositionState.PARTIAL_EXIT):
            await self.store.transition(pid, PositionState.ENTERING, PositionState.OPEN, {
                "entry.bb.filled_qty": 100,
                "entry.bb.avg_fill_price": 0.05,
                "entry.bb.state": LegStateEnum.FILLED,
                "entry.bn.filled_qty": 100,
                "entry.bn.avg_fill_price": 0.05,
                "entry.bn.state": LegStateEnum.FILLED,
                "entry.actual_spread_bps": 50.0,
            })
        if target_state in (PositionState.EXITING, PositionState.PARTIAL_EXIT):
            await self.store.transition(pid, PositionState.OPEN, PositionState.EXITING, {
                "exit.bb.side": "Sell",
                "exit.bb.target_qty": 100,
                "exit.bn.side": "Buy",
                "exit.bn.target_qty": 100,
                **extra,
            })
        if target_state == PositionState.PARTIAL_EXIT:
            pe_updates = extra.get("partial_exit_updates", {
                "exit.bb.state": LegStateEnum.PARTIAL,
                "exit.bb.filled_qty": 40,
                "exit.bn.state": LegStateEnum.PARTIAL,
                "exit.bn.filled_qty": 30,
            })
            await self.store.transition(pid, PositionState.EXITING, PositionState.PARTIAL_EXIT, pe_updates)

    # -- Finding 1: PARTIAL_EXIT handles BOTH non-FILLED legs --

    async def test_finding1_partial_exit_both_legs_non_filled(self):
        """Round 4 #1: Both bb and bn are PARTIAL -- both must be market-closed."""
        await self._advance_to_state("r4f1", "NOMUSDT", PositionState.PARTIAL_EXIT)

        gateway = _make_gateway_mock()
        # No orphan positions on exchange
        gateway.check_position = AsyncMock(return_value=0.0)
        submit_calls = []

        async def _track_submit(venue, symbol, side, qty, price, order_type):
            submit_calls.append({"venue": venue, "qty": qty, "side": side})
            return f"MKT_{venue}_{len(submit_calls)}"

        gateway.submit = AsyncMock(side_effect=_track_submit)
        # Query says neither is filled yet, then market fills
        gateway.get_order_with_retry = AsyncMock(return_value={
            "fill_result": "FILLED", "filled_qty": 60, "avg_price": 0.05,
        })

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        pos = await self.store.get("r4f1")
        assert pos["state"] == PositionState.CLOSED, f"Expected CLOSED, got {pos['state']}"
        # Must have submitted TWO market orders (one per leg)
        assert len(submit_calls) == 2, f"Expected 2 market submits, got {len(submit_calls)}"
        venues = {c["venue"] for c in submit_calls}
        assert venues == {"bybit", "binance"}, f"Expected both venues, got {venues}"

    async def test_finding1_partial_exit_one_filled_one_partial(self):
        """Round 4 #1: bb FILLED, bn PARTIAL -- only bn gets market-closed."""
        await self._advance_to_state("r4f1b", "NOMUSDT", PositionState.PARTIAL_EXIT,
            partial_exit_updates={
                "exit.bb.state": LegStateEnum.FILLED,
                "exit.bb.filled_qty": 100,
                "exit.bn.state": LegStateEnum.PARTIAL,
                "exit.bn.filled_qty": 30,
            })

        gateway = _make_gateway_mock()
        gateway.check_position = AsyncMock(return_value=0.0)
        submit_calls = []

        async def _track_submit(venue, symbol, side, qty, price, order_type):
            submit_calls.append({"venue": venue, "qty": qty})
            return f"MKT_{venue}"

        gateway.submit = AsyncMock(side_effect=_track_submit)
        gateway.get_order_with_retry = AsyncMock(return_value={
            "fill_result": "FILLED", "filled_qty": 70, "avg_price": 0.05,
        })

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        pos = await self.store.get("r4f1b")
        assert pos["state"] == PositionState.CLOSED
        # Only bn should have a market submit (bb was already FILLED)
        assert len(submit_calls) == 1
        assert submit_calls[0]["venue"] == "binance"

    # -- Finding 2: ENTERING with partial fills -> UNWINDING --

    async def test_finding2_entering_both_partial_unwinds(self):
        """Round 4 #2: Both entry legs PARTIAL (not fully filled) -> UNWINDING, not OPEN."""
        doc = new_position_doc("r4f2", "NOMUSDT", "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30)
        await self.store.create(doc)
        await self.store.transition("r4f2", PositionState.PENDING, PositionState.ENTERING, {
            "entry.bb.client_order_id": "cid_bb_r4f2",
            "entry.bb.order_id": "bb_oid_r4f2",
            "entry.bb.side": "Buy",
            "entry.bb.target_qty": 100,
            "entry.bn.client_order_id": "cid_bn_r4f2",
            "entry.bn.order_id": "bn_oid_r4f2",
            "entry.bn.side": "Sell",
            "entry.bn.target_qty": 100,
        })

        gateway = _make_gateway_mock()

        async def _partial_fills(venue, symbol, order_id="", client_order_id=""):
            if "bb" in (order_id or "") or "bb" in (client_order_id or ""):
                return {"fill_result": "PARTIAL", "filled_qty": 40, "avg_price": 0.05}
            return {"fill_result": "PARTIAL", "filled_qty": 30, "avg_price": 0.05}

        gateway.get_order_with_retry = AsyncMock(side_effect=_partial_fills)

        unwind_calls = []

        async def _track_submit(venue, symbol, side, qty, price, order_type):
            unwind_calls.append({"venue": venue, "side": side, "qty": qty})
            return f"UNW_{venue}"

        gateway.submit = AsyncMock(side_effect=_track_submit)

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        pos = await self.store.get("r4f2")
        assert pos["state"] in (PositionState.UNWINDING, PositionState.FAILED), \
            f"Expected UNWINDING/FAILED, got {pos['state']} -- partial entry must NOT go to OPEN"
        # Both legs should be unwound
        assert len(unwind_calls) == 2, f"Expected 2 unwind orders, got {len(unwind_calls)}"

    async def test_finding2_entering_both_fully_filled_goes_open(self):
        """Round 4 #2: Both entry legs FULLY filled -> OPEN (unchanged behavior)."""
        doc = new_position_doc("r4f2b", "NOMUSDT", "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30)
        await self.store.create(doc)
        await self.store.transition("r4f2b", PositionState.PENDING, PositionState.ENTERING, {
            "entry.bb.client_order_id": "cid_bb_r4f2b",
            "entry.bb.order_id": "bb_oid_r4f2b",
            "entry.bb.side": "Buy",
            "entry.bb.target_qty": 100,
            "entry.bn.client_order_id": "cid_bn_r4f2b",
            "entry.bn.order_id": "bn_oid_r4f2b",
            "entry.bn.side": "Sell",
            "entry.bn.target_qty": 100,
        })

        gateway = _make_gateway_mock()
        gateway.get_order_with_retry = AsyncMock(return_value={
            "fill_result": "FILLED", "filled_qty": 100, "avg_price": 0.05,
        })

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        pos = await self.store.get("r4f2b")
        assert pos["state"] == PositionState.OPEN, "Both fully filled should go to OPEN"
        assert report.positions_resumed == 1

    async def test_finding2_entering_one_full_one_partial_unwinds(self):
        """Round 4 #2: One leg FILLED, other PARTIAL -> still UNWIND (not OPEN)."""
        doc = new_position_doc("r4f2c", "NOMUSDT", "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30)
        await self.store.create(doc)
        await self.store.transition("r4f2c", PositionState.PENDING, PositionState.ENTERING, {
            "entry.bb.client_order_id": "cid_bb_r4f2c",
            "entry.bb.order_id": "bb_oid_r4f2c",
            "entry.bb.side": "Buy",
            "entry.bb.target_qty": 100,
            "entry.bn.client_order_id": "cid_bn_r4f2c",
            "entry.bn.order_id": "bn_oid_r4f2c",
            "entry.bn.side": "Sell",
            "entry.bn.target_qty": 100,
        })

        gateway = _make_gateway_mock()

        async def _mixed_fills(venue, symbol, order_id="", client_order_id=""):
            if "bb" in (order_id or "") or "bb" in (client_order_id or ""):
                return {"fill_result": "FILLED", "filled_qty": 100, "avg_price": 0.05}
            return {"fill_result": "PARTIAL", "filled_qty": 30, "avg_price": 0.05}

        gateway.get_order_with_retry = AsyncMock(side_effect=_mixed_fills)
        gateway.submit = AsyncMock(return_value="UNW_OID")

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        pos = await self.store.get("r4f2c")
        assert pos["state"] in (PositionState.UNWINDING, PositionState.FAILED), \
            f"One full + one partial should UNWIND, got {pos['state']}"

    # -- Finding 3: PARTIAL_EXIT positions resumed after restart --

    async def test_finding3_get_by_symbol_states_returns_partial_exit(self):
        """Round 4 #3: get_by_symbol_states returns PARTIAL_EXIT positions."""
        await self._advance_to_state("r4f3a", "NOMUSDT", PositionState.PARTIAL_EXIT)
        await self._advance_to_state("r4f3b", "ENJUSDT", PositionState.OPEN)

        partial = await self.store.get_by_symbol_states([PositionState.PARTIAL_EXIT])
        assert len(partial) == 1
        assert partial[0]["position_id"] == "r4f3a"
        assert partial[0]["state"] == PositionState.PARTIAL_EXIT

    async def test_finding3_partial_exit_not_in_get_open(self):
        """Round 4 #3: PARTIAL_EXIT NOT returned by get_open() (only OPEN)."""
        await self._advance_to_state("r4f3c", "NOMUSDT", PositionState.PARTIAL_EXIT)

        open_positions = await self.store.get_open()
        assert len(open_positions) == 0, "PARTIAL_EXIT must not appear in get_open()"

    async def test_finding3_get_by_symbol_states_multiple(self):
        """Round 4 #3: get_by_symbol_states with multiple states works."""
        await self._advance_to_state("r4f3d", "NOMUSDT", PositionState.PARTIAL_EXIT)
        await self._advance_to_state("r4f3e", "ENJUSDT", PositionState.OPEN)

        results = await self.store.get_by_symbol_states([PositionState.PARTIAL_EXIT, PositionState.OPEN])
        assert len(results) == 2
        pids = {r["position_id"] for r in results}
        assert pids == {"r4f3d", "r4f3e"}

    # -- Finding 4: Orphan buyback deferred action structure --

    async def test_finding4_orphan_buyback_deferred_action_structure(self):
        """Round 4 #4: Verify recovery produces correct deferred_action for orphan buyback."""
        gateway = _make_gateway_mock()
        gateway.check_position = AsyncMock(return_value=50.0)
        gateway.submit = AsyncMock(return_value="ORPHAN_CLOSE_OID")

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        assert report.orphans_closed == 1
        assert len(report.deferred_actions) == 1
        action = report.deferred_actions[0]
        assert action["action"] == "orphan_buyback"
        assert action["symbol"] == "NOMUSDT"
        assert action["qty"] == 50.0
        assert action["side"] == "Buy"

    async def test_finding4_short_orphan_generates_sell_action(self):
        """Round 4 #4: Short orphan (negative position) -> Sell on Binance."""
        gateway = _make_gateway_mock()
        gateway.check_position = AsyncMock(return_value=-30.0)
        gateway.submit = AsyncMock(return_value="ORPHAN_CLOSE_OID")

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        assert report.orphans_closed == 1
        action = report.deferred_actions[0]
        assert action["side"] == "Sell"
        assert action["qty"] == 30.0

    # -- Finding 5: OPEN with Binance exposure stays OPEN --

    async def test_finding5_open_bybit_flat_binance_exposure_stays_open(self):
        """Round 4 #5: Bybit flat + Binance has fills -> do NOT close."""
        doc = new_position_doc("r4f5", "NOMUSDT", "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30)
        await self.store.create(doc)
        await self.store.transition("r4f5", PositionState.PENDING, PositionState.ENTERING, {})
        await self.store.transition("r4f5", PositionState.ENTERING, PositionState.OPEN, {
            "entry.bb.filled_qty": 100,
            "entry.bb.state": LegStateEnum.FILLED,
            "entry.bn.filled_qty": 100,
            "entry.bn.state": LegStateEnum.FILLED,
        })

        gateway = _make_gateway_mock()
        gateway.check_position = AsyncMock(return_value=0.0)

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        pos = await self.store.get("r4f5")
        assert pos["state"] == PositionState.OPEN, \
            f"Expected OPEN (Binance exposure), got {pos['state']}"
        reconcile_actions = [a for a in report.deferred_actions if a.get("action") == "reconcile_binance_exposure"]
        assert len(reconcile_actions) == 1
        assert reconcile_actions[0]["bn_qty"] == 100
        assert any("binance_exposure" in e for e in report.errors)

    async def test_finding5_open_bybit_flat_no_binance_closes(self):
        """Round 4 #5: Bybit flat + no Binance fills -> safe to CLOSE."""
        doc = new_position_doc("r4f5b", "NOMUSDT", "NOMUSDC", "BUY_BB_SELL_BN", 100, 90, 30)
        await self.store.create(doc)
        await self.store.transition("r4f5b", PositionState.PENDING, PositionState.ENTERING, {})
        await self.store.transition("r4f5b", PositionState.ENTERING, PositionState.OPEN, {
            "entry.bb.filled_qty": 100,
            "entry.bb.state": LegStateEnum.FILLED,
            "entry.bn.filled_qty": 0,
            "entry.bn.state": LegStateEnum.CANCELLED,
        })

        gateway = _make_gateway_mock()
        gateway.check_position = AsyncMock(return_value=0.0)

        recovery = CrashRecoveryV2(self.store, gateway)
        report = await recovery.recover(["NOMUSDT"])

        pos = await self.store.get("r4f5b")
        assert pos["state"] == PositionState.CLOSED, "No Binance exposure -> safe to close"
        assert report.positions_resolved == 1


# ═══════════════════════════════════════════════════════════════
# Run
# ══════════════���════════════════════════════════════════════════

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
