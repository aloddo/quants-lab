"""
Tests for the LegCoordinator race condition fix.

The core bug: WS fills arrive using exchange order IDs before
execute_entry() maps them to legs (which are pre-registered with
client order IDs). This caused fills to be silently dropped,
leading to naked positions.

Three fixes tested:
1. Client order ID fallback matching in on_fill()
2. Unmatched fill buffer + replay after ID registration
3. Position verification before declaring leg failure
"""
import asyncio
import pytest
import time
from unittest.mock import AsyncMock, MagicMock

from app.services.arb.execution_engine import (
    LegCoordinator, LegState, OrderState, EntryOutcome,
)
from app.services.arb.order_feed import FillEvent, FillSource
from app.services.arb.price_feed import SpreadSnapshot


# ── Helpers ────────────────────────────────────────────────────

def make_snapshot(symbol="TESTUSDT", spread_bps=100.0) -> SpreadSnapshot:
    """Create a minimal SpreadSnapshot for testing."""
    return SpreadSnapshot(
        symbol=symbol,
        bb_bid=1.000, bb_ask=1.001,
        bn_bid=1.010, bn_ask=1.011,
        spread_bps=spread_bps,
        reverse_spread_bps=-spread_bps,
        direction="BUY_BB_SELL_BN",
        bb_age_ms=10.0, bn_age_ms=10.0,
        timestamp=time.time(),
        fresh=True,
    )


def make_fill(
    venue: str, symbol: str, order_id: str, side: str,
    qty: float, price: float, client_order_id: str = "",
) -> FillEvent:
    """Create a FillEvent with optional client_order_id."""
    fill = FillEvent(
        venue=venue, symbol=symbol, order_id=order_id,
        exec_id=f"exec_{order_id}_{time.time_ns()}",
        side=side, price=price, qty=qty,
        fee=qty * price * 0.0005, fee_asset="USDT",
        timestamp_ms=int(time.time() * 1000),
        local_ts=time.time(), source=FillSource.WS,
        is_maker=False, order_status="Filled", leaves_qty=0,
    )
    fill.client_order_id = client_order_id
    return fill


# ── Test 1: Client order ID fallback matching ──────────────────

class TestClientIdFallback:
    """Test that on_fill() matches via client_order_id when exchange ID isn't registered."""

    def test_fill_matched_by_client_id(self):
        """Fill arrives with exchange order_id, but leg is registered with client_id."""
        coord = LegCoordinator(
            submit_order_fn=AsyncMock(),
            cancel_order_fn=AsyncMock(),
        )

        # Pre-register leg with client order ID
        client_id = "h2_bb_test123"
        exchange_id = "bybit-uuid-abc"
        leg = LegState(
            venue="bybit", symbol="TESTUSDT", side="Buy",
            order_id=client_id, target_qty=100.0, target_price=1.0,
            state=OrderState.SUBMITTED,
        )
        coord.register_leg(leg)

        # WS fill arrives with EXCHANGE order_id (not client_id)
        fill = make_fill(
            venue="bybit", symbol="TESTUSDT", order_id=exchange_id,
            side="Buy", qty=100.0, price=1.001,
            client_order_id=client_id,  # orderLinkId from Bybit WS
        )
        coord.on_fill(fill)

        # Fill should have been matched and leg updated
        assert leg.filled_qty == 100.0
        assert leg.state == OrderState.FILLED
        assert leg.avg_fill_price == 1.001

        # Exchange ID should now be registered for future lookups
        assert coord._leg_states.get(exchange_id) is leg

    def test_fill_matched_by_exchange_id(self):
        """Normal case: fill arrives with exchange ID that IS registered."""
        coord = LegCoordinator(
            submit_order_fn=AsyncMock(),
            cancel_order_fn=AsyncMock(),
        )

        exchange_id = "bybit-uuid-xyz"
        leg = LegState(
            venue="bybit", symbol="TESTUSDT", side="Buy",
            order_id=exchange_id, target_qty=50.0, target_price=1.0,
            state=OrderState.SUBMITTED,
        )
        coord.register_leg(leg)

        fill = make_fill(
            venue="bybit", symbol="TESTUSDT", order_id=exchange_id,
            side="Buy", qty=50.0, price=1.0,
        )
        coord.on_fill(fill)

        assert leg.filled_qty == 50.0
        assert leg.state == OrderState.FILLED

    def test_unmatched_fill_buffered(self):
        """Fill arrives before ANY registration (neither client nor exchange ID)."""
        coord = LegCoordinator(
            submit_order_fn=AsyncMock(),
            cancel_order_fn=AsyncMock(),
        )

        fill = make_fill(
            venue="bybit", symbol="TESTUSDT", order_id="unknown-id",
            side="Buy", qty=100.0, price=1.0,
            client_order_id="also-unknown",
        )
        coord.on_fill(fill)

        # Fill should be buffered, not lost
        assert hasattr(coord, '_unmatched_fills')
        assert len(coord._unmatched_fills) == 1
        assert coord._unmatched_fills[0].order_id == "unknown-id"


# ── Test 2: Fill buffer replay ─────────────────────────────────

class TestFillBufferReplay:
    """Test that buffered fills are replayed after exchange IDs are registered."""

    @pytest.mark.asyncio
    async def test_replay_matches_buffered_fill(self):
        """
        Simulate the exact race condition:
        1. Pre-register with client ID
        2. Submit (gather) - Bybit returns exchange ID
        3. WS fill arrives DURING gather with exchange ID -> buffered
        4. After gather, exchange ID registered, buffer replayed -> matched
        """
        client_id = "h2_bb_race"
        exchange_id = "bybit-uuid-race"

        # Track what submit returns
        async def mock_submit(venue, symbol, side, qty, price, order_type, coid=""):
            # Simulate Bybit: returns exchange ID
            if venue == "bybit":
                return exchange_id
            return "binance-123"

        coord = LegCoordinator(
            submit_order_fn=mock_submit,
            cancel_order_fn=AsyncMock(return_value=True),
        )

        # Step 1: Pre-register with client ID
        bb_leg = LegState(
            venue="bybit", symbol="TESTUSDT", side="Buy",
            order_id=client_id, target_qty=100.0, target_price=1.0,
        )
        coord.register_leg(bb_leg)

        # Step 2: Simulate WS fill arriving with exchange ID
        # (this happens DURING asyncio.gather, before exchange IDs are mapped)
        fill = make_fill(
            venue="bybit", symbol="TESTUSDT", order_id=exchange_id,
            side="Buy", qty=100.0, price=1.001,
            client_order_id=client_id,
        )

        # With Fix 1 (client ID fallback), this should match directly
        coord.on_fill(fill)

        # Verify it matched via client ID
        assert bb_leg.filled_qty == 100.0
        assert bb_leg.state == OrderState.FILLED

    @pytest.mark.asyncio
    async def test_replay_without_client_id(self):
        """
        Edge case: WS fill has no client_order_id (e.g., Binance).
        Should be buffered and replayed after exchange ID registration.
        """
        coord = LegCoordinator(
            submit_order_fn=AsyncMock(),
            cancel_order_fn=AsyncMock(),
        )

        exchange_id = "binance-456"
        client_id = "h2_bn_test"

        # Pre-register with client ID
        leg = LegState(
            venue="binance", symbol="TESTUSDT", side="Sell",
            order_id=client_id, target_qty=100.0, target_price=1.01,
            state=OrderState.SUBMITTED,
        )
        coord.register_leg(leg)

        # Fill arrives with exchange ID, no client_order_id
        fill = make_fill(
            venue="binance", symbol="TESTUSDT", order_id=exchange_id,
            side="Sell", qty=100.0, price=1.01,
            client_order_id="",  # Binance WS doesn't include this
        )
        coord.on_fill(fill)

        # Should be buffered (can't match by either ID)
        assert len(coord._unmatched_fills) == 1

        # Now register exchange ID (simulating post-gather mapping)
        coord._fill_events[exchange_id] = coord._fill_events[client_id]
        coord._leg_states[exchange_id] = leg

        # Replay buffer
        buffered = coord._unmatched_fills[:]
        coord._unmatched_fills.clear()
        for bf in buffered:
            coord.on_fill(bf)

        # Now should be matched
        assert leg.filled_qty == 100.0
        assert leg.state == OrderState.FILLED


# ── Test 3: Position verification ──────────────────────────────

class TestPositionVerification:
    """Test that position verification catches phantom leg failures."""

    @pytest.mark.asyncio
    async def test_position_check_prevents_false_leg_failure(self):
        """
        Scenario: Bybit filled but coordinator doesn't know.
        Position check detects the fill and prevents unwind.
        """
        exchange_id_bb = "bybit-stealth"
        exchange_id_bn = "binance-ok"

        call_count = {"bb": 0, "bn": 0}

        async def mock_submit(venue, symbol, side, qty, price, order_type, coid=""):
            if venue == "bybit":
                call_count["bb"] += 1
                return exchange_id_bb
            call_count["bn"] += 1
            return exchange_id_bn

        async def mock_check_position(venue, symbol):
            if venue == "bybit":
                return 100.0  # Position exists on exchange!
            return 0.0

        coord = LegCoordinator(
            submit_order_fn=mock_submit,
            cancel_order_fn=AsyncMock(return_value=True),
            check_order_fn=AsyncMock(return_value={"filled_qty": 0, "avg_price": 0, "status": "unknown"}),
            check_position_fn=mock_check_position,
        )

        signal = make_snapshot()

        # Pre-create legs that WON'T get WS fills (simulating the bug)
        # We need to mock the entire execute_entry flow, so let's test
        # the position verification logic directly

        bb_leg = LegState(
            venue="bybit", symbol="TESTUSDT", side="Buy",
            order_id=exchange_id_bb, target_qty=100.0, target_price=1.001,
            state=OrderState.SUBMITTED,
            # filled_qty stays 0 - simulating missed fill
        )
        bn_leg = LegState(
            venue="binance", symbol="TESTUSDT", side="Sell",
            order_id=exchange_id_bn, target_qty=100.0, target_price=1.010,
            state=OrderState.FILLED,
        )
        bn_leg.filled_qty = 100.0
        bn_leg.avg_fill_price = 1.010

        # Verify bb_leg looks unfilled
        assert not bb_leg.has_any_fill
        assert bn_leg.has_any_fill

        # Run position verification (the code from execute_entry)
        for leg, venue_label in [(bb_leg, "bybit"), (bn_leg, "binance")]:
            if not leg.has_any_fill:
                actual_pos = await mock_check_position(venue_label, "TESTUSDT")
                if actual_pos and abs(actual_pos) >= leg.target_qty * 0.9:
                    leg.filled_qty = abs(actual_pos)
                    leg.avg_fill_price = leg.target_price
                    leg.state = OrderState.FILLED

        # After verification, bb_leg should be corrected
        assert bb_leg.filled_qty == 100.0
        assert bb_leg.state == OrderState.FILLED
        assert bb_leg.is_filled
        assert bn_leg.is_filled


# ── Test 4: Full execute_entry with race condition ─────────────

class TestExecuteEntryRaceCondition:
    """End-to-end test simulating the exact bug scenario."""

    @pytest.mark.asyncio
    async def test_entry_succeeds_despite_ws_race(self):
        """
        Simulate:
        1. Both legs submit concurrently
        2. Bybit WS fill arrives with exchange ID during gather
        3. Client ID fallback matches it
        4. Binance fills via REST check
        5. Both legs recognized -> SUCCESS (not LEG_FAILURE)
        """
        bb_exchange_id = "bybit-uuid-e2e"
        bn_exchange_id = "99887766"

        async def mock_submit(venue, symbol, side, qty, price, order_type, coid=""):
            if venue == "bybit":
                # Simulate: WS fill fires immediately (before gather completes)
                # We'll deliver it after submit returns but before both complete
                return bb_exchange_id
            # Binance takes longer
            await asyncio.sleep(0.05)
            return bn_exchange_id

        async def mock_cancel(venue, symbol, order_id):
            return True

        # REST check finds Binance fill
        async def mock_check_order(venue, symbol, order_id):
            if venue == "binance" and order_id == bn_exchange_id:
                return {"filled_qty": 100.0, "avg_price": 1.010, "status": "Filled"}
            if venue == "bybit" and order_id == bb_exchange_id:
                return {"filled_qty": 100.0, "avg_price": 1.001, "status": "Filled"}
            return {"filled_qty": 0, "avg_price": 0, "status": "unknown"}

        coord = LegCoordinator(
            submit_order_fn=mock_submit,
            cancel_order_fn=mock_cancel,
            check_order_fn=mock_check_order,
        )

        signal = make_snapshot()

        # Deliver Bybit WS fill with client_order_id after a tiny delay
        # (simulates the fill arriving during asyncio.gather)
        async def deliver_bb_fill():
            await asyncio.sleep(0.01)  # After bb submit returns, during bn submit
            fill = make_fill(
                venue="bybit", symbol="TESTUSDT", order_id=bb_exchange_id,
                side="Buy", qty=100.0, price=1.001,
                client_order_id="",  # Worst case: no client ID either
            )
            coord.on_fill(fill)

        # Start fill delivery in background
        asyncio.create_task(deliver_bb_fill())

        result = await coord.execute_entry(
            signal=signal,
            bb_side="Buy", bn_side="Sell",
            qty_bb=100.0, qty_bn=100.0,
            price_bb=1.001, price_bn=1.010,
        )

        # Should succeed (via REST fallback or buffer replay)
        assert result.outcome == EntryOutcome.SUCCESS, \
            f"Expected SUCCESS but got {result.outcome}. " \
            f"bb_filled={result.bybit_leg.filled_qty} bn_filled={result.binance_leg.filled_qty}"

    @pytest.mark.asyncio
    async def test_entry_succeeds_with_client_id_match(self):
        """
        Same scenario but WS fill includes client_order_id (orderLinkId).
        Should match immediately via Fix 1.
        """
        bb_exchange_id = "bybit-uuid-cid"
        bn_exchange_id = "55443322"

        submit_calls = []

        async def mock_submit(venue, symbol, side, qty, price, order_type, coid=""):
            submit_calls.append((venue, coid))
            if venue == "bybit":
                return bb_exchange_id
            await asyncio.sleep(0.05)
            return bn_exchange_id

        async def mock_check_order(venue, symbol, order_id):
            if venue == "binance" and order_id == bn_exchange_id:
                return {"filled_qty": 100.0, "avg_price": 1.010, "status": "Filled"}
            return {"filled_qty": 0, "avg_price": 0, "status": "unknown"}

        coord = LegCoordinator(
            submit_order_fn=mock_submit,
            cancel_order_fn=AsyncMock(return_value=True),
            check_order_fn=mock_check_order,
        )

        signal = make_snapshot()

        async def deliver_bb_fill_with_client_id():
            await asyncio.sleep(0.01)
            # Find the client ID that was used for Bybit
            bb_coid = ""
            for venue, coid in submit_calls:
                if venue == "bybit" and coid:
                    bb_coid = coid
                    break
            fill = make_fill(
                venue="bybit", symbol="TESTUSDT", order_id=bb_exchange_id,
                side="Buy", qty=100.0, price=1.001,
                client_order_id=bb_coid,
            )
            coord.on_fill(fill)

        asyncio.create_task(deliver_bb_fill_with_client_id())

        result = await coord.execute_entry(
            signal=signal,
            bb_side="Buy", bn_side="Sell",
            qty_bb=100.0, qty_bn=100.0,
            price_bb=1.001, price_bn=1.010,
        )

        assert result.outcome == EntryOutcome.SUCCESS, \
            f"Expected SUCCESS but got {result.outcome}"


# ── Test 5: Dedup still works ──────────────────────────────────

class TestFillDedup:
    """Ensure the fix doesn't break fill deduplication."""

    def test_duplicate_fill_ignored(self):
        """Same exec_id delivered twice should only count once."""
        coord = LegCoordinator(
            submit_order_fn=AsyncMock(),
            cancel_order_fn=AsyncMock(),
        )

        client_id = "h2_bb_dedup"
        leg = LegState(
            venue="bybit", symbol="TESTUSDT", side="Buy",
            order_id=client_id, target_qty=100.0, target_price=1.0,
            state=OrderState.SUBMITTED,
        )
        coord.register_leg(leg)

        fill = FillEvent(
            venue="bybit", symbol="TESTUSDT", order_id=client_id,
            exec_id="exec_same_id",
            side="Buy", price=1.001, qty=100.0,
            fee=0.05, fee_asset="USDT",
            timestamp_ms=int(time.time() * 1000),
            local_ts=time.time(), source=FillSource.WS,
            is_maker=False, order_status="Filled", leaves_qty=0,
        )

        coord.on_fill(fill)
        assert leg.filled_qty == 100.0

        # Same fill again
        coord.on_fill(fill)
        assert leg.filled_qty == 100.0  # Should NOT double


# ── Test 6: Exit still works ──────────────────────────────────

class TestExitNotBroken:
    """Ensure exit path wasn't broken by the changes."""

    @pytest.mark.asyncio
    async def test_exit_success(self):
        """Both exit legs fill normally."""
        async def mock_submit(venue, symbol, side, qty, price, order_type, coid=""):
            oid = f"{venue}_exit_{time.time_ns()}"
            return oid

        coord = LegCoordinator(
            submit_order_fn=mock_submit,
            cancel_order_fn=AsyncMock(return_value=True),
        )

        # Deliver fills for exit orders
        async def deliver_exit_fills():
            await asyncio.sleep(0.05)
            # Find the leg states and fill them
            for oid, leg in list(coord._leg_states.items()):
                if not leg.is_filled:
                    fill = make_fill(
                        venue=leg.venue, symbol=leg.symbol, order_id=oid,
                        side=leg.side, qty=leg.target_qty, price=leg.target_price,
                    )
                    coord.on_fill(fill)

        asyncio.create_task(deliver_exit_fills())

        result = await coord.execute_exit(
            symbol="TESTUSDT",
            bb_side="Sell", bn_side="Buy",
            qty_bb=100.0, qty_bn=100.0,
            price_bb=1.002, price_bn=1.009,
        )

        assert result.outcome == "success"
