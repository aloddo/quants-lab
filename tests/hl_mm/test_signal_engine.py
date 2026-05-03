"""Tests for V2 signal_engine changes: trade ring buffer, real L2 OFI, notional imbalance."""
import time
from collections import deque
from unittest.mock import MagicMock

import pytest

from app.services.hl_mm.signal_engine import SignalEngine


@pytest.fixture
def engine():
    return SignalEngine()


class TestUpdateTrades:
    """V2: Trade ring buffer stores (ts, direction, notional, price)."""

    def test_stores_full_trade_data(self, engine):
        engine.update_trades("BIO", [
            {"side": "B", "px": "0.055", "sz": "100"},
            {"side": "S", "px": "0.054", "sz": "200"},
        ])
        trades = list(engine._trade_sides["BIO"])
        assert len(trades) == 2
        # First trade: buy, notional = 0.055 * 100 = 5.5
        ts, direction, notional, price = trades[0]
        assert direction == 1
        assert abs(notional - 5.5) < 0.01
        assert abs(price - 0.055) < 0.001

    def test_stores_direction_size_price(self, engine):
        engine.update_trades("ORDI", [
            {"side": "S", "px": "5.0", "sz": "10"},
        ])
        trades = list(engine._trade_sides["ORDI"])
        ts, direction, notional, price = trades[0]
        assert direction == -1  # sell
        assert abs(notional - 50.0) < 0.01
        assert abs(price - 5.0) < 0.01

    def test_ring_buffer_bounded(self, engine):
        """Buffer should not grow unbounded."""
        engine._ensure_coin("BIO")
        for i in range(3000):
            engine.update_trades("BIO", [{"side": "B", "px": "1.0", "sz": "1"}])
        assert len(engine._trade_sides["BIO"]) == 2000  # maxlen

    def test_handles_missing_fields(self, engine):
        """Trades with missing px/sz should not crash."""
        engine.update_trades("BIO", [{"side": "B"}])
        trades = list(engine._trade_sides["BIO"])
        assert len(trades) == 1
        _, _, notional, _ = trades[0]
        assert notional == 0.0


class TestTradeImbalance:
    """V2: Notional-weighted imbalance (not count-based)."""

    def test_notional_weighted_buy_heavy(self, engine):
        """One large buy + many small sells should be buy-heavy."""
        engine._ensure_coin("BIO")
        now = time.time()
        # 1 big buy: $100
        engine._trade_sides["BIO"].append((now, 1, 100.0, 1.0))
        # 9 small sells: $1 each = $9 total
        for _ in range(9):
            engine._trade_sides["BIO"].append((now, -1, 1.0, 1.0))

        # Count-based: 1 buy / 10 total = 10% buy (would NOT trigger)
        # Notional: $100 / $109 = 91.7% buy (SHOULD trigger at 70%)
        result = engine._check_trade_imbalance("BIO")
        assert result is True

    def test_balanced_flow_no_trigger(self, engine):
        engine._ensure_coin("BIO")
        now = time.time()
        for _ in range(10):
            engine._trade_sides["BIO"].append((now, 1, 10.0, 1.0))
            engine._trade_sides["BIO"].append((now, -1, 10.0, 1.0))
        result = engine._check_trade_imbalance("BIO")
        assert result is False

    def test_too_few_trades_no_trigger(self, engine):
        engine._ensure_coin("BIO")
        now = time.time()
        engine._trade_sides["BIO"].append((now, 1, 100.0, 1.0))
        result = engine._check_trade_imbalance("BIO")
        assert result is False  # < 5 trades

    def test_old_trades_excluded(self, engine):
        """Trades older than 3s should not count."""
        engine._ensure_coin("BIO")
        old = time.time() - 5.0
        now = time.time()
        # Old trades: all buys
        for _ in range(20):
            engine._trade_sides["BIO"].append((old, 1, 10.0, 1.0))
        # Recent trades: balanced
        for _ in range(5):
            engine._trade_sides["BIO"].append((now, 1, 10.0, 1.0))
            engine._trade_sides["BIO"].append((now, -1, 10.0, 1.0))
        result = engine._check_trade_imbalance("BIO")
        assert result is False


class TestRealL2OFI:
    """V2: L2 OFI from book size deltas (not mid-price changes)."""

    def test_bid_size_increase_positive_ofi(self, engine):
        """Bid depth increasing while ask stable = positive OFI."""
        engine._ensure_coin("BIO")
        now = time.time()
        # Sequence: bid growing, ask stable
        for i in range(10):
            engine._depth5_history["BIO"].append(
                (now + i * 0.5, 1000.0 + i * 100, 1000.0)  # bid grows
            )
        ofi = engine._compute_ofi("BIO")
        assert ofi > 0  # positive = buying pressure

    def test_ask_size_increase_negative_ofi(self, engine):
        """Ask depth increasing while bid stable = negative OFI."""
        engine._ensure_coin("BIO")
        now = time.time()
        for i in range(10):
            engine._depth5_history["BIO"].append(
                (now + i * 0.5, 1000.0, 1000.0 + i * 100)  # ask grows
            )
        ofi = engine._compute_ofi("BIO")
        assert ofi < 0  # negative = selling pressure

    def test_no_history_returns_zero(self, engine):
        engine._ensure_coin("BIO")
        ofi = engine._compute_ofi("BIO")
        assert ofi == 0.0

    def test_stable_book_near_zero(self, engine):
        """Stable book should produce near-zero OFI."""
        engine._ensure_coin("BIO")
        now = time.time()
        for i in range(10):
            engine._depth5_history["BIO"].append(
                (now + i * 0.5, 1000.0, 1000.0)
            )
        ofi = engine._compute_ofi("BIO")
        assert abs(ofi) < 0.1
