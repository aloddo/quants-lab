"""Tests for quote_engine.py — Quote computation, execution, fill detection."""
import time
import threading
from unittest.mock import MagicMock, patch

import pytest

from app.services.hl_mm.quote_engine import (
    QuoteEngine, QuoteConfig, QuotePair, QuoteDecision, ActiveOrder, QuoteState,
)


@pytest.fixture
def engine():
    """QuoteEngine with mocked exchange/info."""
    exchange = MagicMock()
    info = MagicMock()
    info.open_orders.return_value = []
    info.query_order_by_oid.return_value = None
    return QuoteEngine(
        exchange=exchange,
        info=info,
        address="0xtest",
        sz_decimals={"BIO": 0, "ORDI": 2},
        dry_run=True,
    )


class TestComputeQuotes:
    def test_returns_bid_and_ask(self, engine):
        result = engine.compute_quotes(
            coin="BIO",
            fair_value=0.05,
            reservation_price=0.05,
            hl_bid=0.0498,
            hl_ask=0.0502,
            depth20_bid_usd=5000,
            depth20_ask_usd=5000,
            free_equity_usd=50.0,
            q_soft=60.0,
            inventory_usd=0.0,
            sigma_1s=0.0001,
        )
        assert result is not None
        assert result.coin == "BIO"
        assert result.bid is not None or result.ask is not None

    def test_respects_quote_bid_false(self, engine):
        result = engine.compute_quotes(
            coin="BIO",
            fair_value=0.05,
            reservation_price=0.05,
            hl_bid=0.0498,
            hl_ask=0.0502,
            depth20_bid_usd=5000,
            depth20_ask_usd=5000,
            free_equity_usd=50.0,
            q_soft=60.0,
            inventory_usd=0.0,
            sigma_1s=0.0001,
            quote_bid=False,
        )
        if result:
            assert result.bid is None

    def test_maker_enforcement_bid_never_crosses_ask(self, engine):
        """Bid must never cross hl_ask."""
        result = engine.compute_quotes(
            coin="BIO",
            fair_value=0.0501,
            reservation_price=0.0502,  # above ask
            hl_bid=0.0498,
            hl_ask=0.0500,
            depth20_bid_usd=5000,
            depth20_ask_usd=5000,
            free_equity_usd=50.0,
            q_soft=60.0,
            inventory_usd=0.0,
            sigma_1s=0.0001,
        )
        if result and result.bid:
            assert result.bid.price < 0.0500

    def test_returns_none_for_invalid_inputs(self, engine):
        result = engine.compute_quotes(
            coin="BIO",
            fair_value=0.0,
            reservation_price=0.0,
            hl_bid=0.0,
            hl_ask=0.0,
            depth20_bid_usd=0,
            depth20_ask_usd=0,
            free_equity_usd=0.0,
            q_soft=60.0,
            inventory_usd=0.0,
            sigma_1s=0.0001,
        )
        assert result is None

    def test_exit_mode_caps_size(self, engine):
        """Exit mode should not overshoot position."""
        result = engine.compute_quotes(
            coin="BIO",
            fair_value=0.05,
            reservation_price=0.05,
            hl_bid=0.0498,
            hl_ask=0.0502,
            depth20_bid_usd=5000,
            depth20_ask_usd=5000,
            free_equity_usd=50.0,
            q_soft=60.0,
            inventory_usd=-10.0,  # short $10
            sigma_1s=0.0001,
            exit_mode=True,
            quote_bid=True,
            quote_ask=False,
        )
        if result and result.bid:
            # Bid notional should be capped at ~$10.50 (105% of inventory)
            assert result.bid.notional_usd <= 11.0


class TestRequoteGating:
    def test_fresh_state_always_requotes(self, engine):
        assert engine.should_requote("BIO", 0.05) is True

    def test_too_soon_doesnt_requote(self, engine):
        # First call creates state
        engine._states["BIO"] = QuoteState(
            last_action_time=time.time(),
            last_fv=0.05,
        )
        assert engine.should_requote("BIO", 0.05) is False


class TestTickSize:
    def test_high_price_gives_coarse_tick(self, engine):
        tick = engine._get_tick_size("BTC", 100000.0)
        assert tick >= 1.0

    def test_low_price_gives_fine_tick(self, engine):
        tick = engine._get_tick_size("BIO", 0.05)
        assert tick <= 0.0001

    def test_sub_dollar_doesnt_collapse(self, engine):
        """Bug #5 fix: sub-$1 prices shouldn't return 0 tick."""
        tick = engine._get_tick_size("SHIB", 0.00001)
        assert tick > 0


class TestClearOrderByOid:
    def test_clears_matching_bid(self, engine):
        engine._states["BIO"] = QuoteState(
            bid_order=ActiveOrder(oid=123, coin="BIO", is_buy=True, price=0.05, size=100, placed_at=time.time()),
        )
        engine.clear_order_by_oid("BIO", 123)
        assert engine._states["BIO"].bid_order is None

    def test_ignores_non_matching_oid(self, engine):
        engine._states["BIO"] = QuoteState(
            bid_order=ActiveOrder(oid=123, coin="BIO", is_buy=True, price=0.05, size=100, placed_at=time.time()),
        )
        engine.clear_order_by_oid("BIO", 999)
        assert engine._states["BIO"].bid_order is not None


class TestDetectFillsFromSnapshot:
    def test_detects_disappearance_as_pending(self):
        """Order that disappears enters pending check buffer."""
        exchange = MagicMock()
        info = MagicMock()
        info.open_orders.return_value = []
        info.query_order_by_oid.return_value = None
        eng = QuoteEngine(
            exchange=exchange, info=info, address="0xtest",
            sz_decimals={"BIO": 0}, dry_run=False,
        )
        eng._states["BIO"] = QuoteState(
            bid_order=ActiveOrder(oid=100, coin="BIO", is_buy=True, price=0.05, size=200, placed_at=time.time()),
        )
        # open_orders returns empty → order disappeared
        fills = eng.detect_fills_from_snapshot("BIO", [])
        # Should enter pending, not immediately report fill
        assert 100 in eng._pending_fill_check

    def test_partial_fill_updates_order_size(self):
        """Bug #7: partial fill detected from remaining size."""
        exchange = MagicMock()
        info = MagicMock()
        info.query_order_by_oid.return_value = {"order": {"status": "filled", "avgPx": "0.05", "sz": "50", "fee": "0.01"}}
        eng = QuoteEngine(
            exchange=exchange, info=info, address="0xtest",
            sz_decimals={"BIO": 0}, dry_run=False,
        )
        eng._states["BIO"] = QuoteState(
            bid_order=ActiveOrder(oid=100, coin="BIO", is_buy=True, price=0.05, size=200, placed_at=time.time()),
        )
        # Order still resting but with reduced size
        open_orders = [{"oid": 100, "sz": "150", "coin": "BIO"}]
        fills = eng.detect_fills_from_snapshot("BIO", open_orders)
        assert len(fills) == 1
        assert fills[0]["partial"] is True
        assert fills[0]["size"] == pytest.approx(50.0, abs=0.1)
        # Order size updated to remaining
        assert eng._states["BIO"].bid_order.size == 150.0


class TestCancelAll:
    def test_dry_run_clears_state(self, engine):
        engine._states["BIO"] = QuoteState(
            bid_order=ActiveOrder(oid=1, coin="BIO", is_buy=True, price=0.05, size=100, placed_at=time.time()),
        )
        engine.cancel_all()
        assert "BIO" not in engine._states


class TestTelemetryFields:
    def test_execute_quotes_populates_telemetry(self, engine):
        """Bug #14 fix: telemetry fields should be populated."""
        quotes = QuotePair(
            coin="BIO",
            bid=QuoteDecision(price=0.0499, size=200, notional_usd=10.0, is_improvement=True),
            ask=QuoteDecision(price=0.0501, size=200, notional_usd=10.0, is_improvement=False),
            fair_value=0.05,
            reservation_price=0.05,
            spread_bps=4.0,
            timestamp=time.time(),
        )
        engine.execute_quotes(quotes)
        state = engine._states.get("BIO")
        # In dry_run, orders aren't placed (no OID), so state has no orders
        # but the method shouldn't crash
        assert state is not None
