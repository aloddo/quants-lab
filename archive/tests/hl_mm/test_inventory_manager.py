"""Tests for inventory_manager.py — Position tracking, AS reservation, PnL."""
import time
import threading
from unittest.mock import MagicMock, patch

import pytest

from app.services.hl_mm.inventory_manager import (
    InventoryManager, PositionState, PairLimits, ExitMode, PAIR_LIMITS,
)


@pytest.fixture
def mgr():
    """InventoryManager with mocked HL Info."""
    info = MagicMock()
    info.user_state.return_value = None
    info.spot_user_state.return_value = {"balances": [{"coin": "USDC", "total": "50.0"}]}
    return InventoryManager(info=info, address="0xtest")


class TestRecordFill:
    def test_opens_long_position(self, mgr):
        mgr.record_fill("BIO", "bid", price=0.05, size=200.0, fee=0.01)
        pos = mgr.get_position("BIO")
        assert pos.size == 200.0
        assert pos.entry_price == 0.05
        assert pos.opened_at > 0

    def test_opens_short_position(self, mgr):
        mgr.record_fill("BIO", "ask", price=0.05, size=200.0, fee=0.01)
        pos = mgr.get_position("BIO")
        assert pos.size == -200.0

    def test_reduces_long(self, mgr):
        mgr.record_fill("BIO", "bid", price=0.05, size=200.0)
        mgr.record_fill("BIO", "ask", price=0.06, size=100.0)
        pos = mgr.get_position("BIO")
        assert pos.size == 100.0
        # Entry stays at 0.05 (not averaged with sell)
        assert pos.entry_price == 0.05

    def test_realized_pnl_on_close(self, mgr):
        mgr.record_fill("BIO", "bid", price=0.05, size=200.0)
        mgr.record_fill("BIO", "ask", price=0.06, size=200.0)
        # PnL = (0.06 - 0.05) * 200 = $2.00
        assert abs(mgr._realized_pnl - 2.0) < 0.001

    def test_flips_position(self, mgr):
        mgr.record_fill("BIO", "bid", price=0.05, size=100.0)
        mgr.record_fill("BIO", "ask", price=0.06, size=200.0)
        pos = mgr.get_position("BIO")
        assert pos.size == -100.0
        # Entry price for new short = fill price
        assert pos.entry_price == 0.06

    def test_thread_safety(self, mgr):
        """Bug #1 fix: concurrent fills shouldn't corrupt state."""
        errors = []

        def fill_loop(side, n):
            try:
                for _ in range(n):
                    mgr.record_fill("BIO", side, price=0.05, size=1.0)
            except Exception as e:
                errors.append(e)

        t1 = threading.Thread(target=fill_loop, args=("bid", 100))
        t2 = threading.Thread(target=fill_loop, args=("ask", 100))
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert not errors
        pos = mgr.get_position("BIO")
        # 100 buys - 100 sells = 0 (approximately)
        assert abs(pos.size) < 1.0

    def test_notional_updated_after_fill(self, mgr):
        """Bug #7 fix: notional_usd should update on fill."""
        mgr.record_fill("BIO", "bid", price=0.05, size=200.0)
        pos = mgr.get_position("BIO")
        assert pos.notional_usd == pytest.approx(10.0, abs=0.1)


class TestReservationPrice:
    def test_no_inventory_returns_fv(self, mgr):
        res = mgr.compute_reservation_price("BIO", fair_value=0.05, sigma_1s=0.0001)
        assert res == 0.05

    def test_long_inventory_skews_down(self, mgr):
        mgr.record_fill("BIO", "bid", price=0.05, size=200.0)
        res = mgr.compute_reservation_price("BIO", fair_value=0.05, sigma_1s=0.0001)
        # Long position → reservation < FV (discourage more buying)
        assert res < 0.05

    def test_short_inventory_skews_up(self, mgr):
        mgr.record_fill("BIO", "ask", price=0.05, size=200.0)
        res = mgr.compute_reservation_price("BIO", fair_value=0.05, sigma_1s=0.0001)
        # Short position → reservation > FV
        assert res > 0.05


class TestExitMode:
    def test_flat_returns_none(self, mgr):
        assert mgr.get_exit_mode("BIO") == ExitMode.NONE

    def test_young_small_adverse_passive(self, mgr):
        mgr.record_fill("BIO", "bid", price=0.05, size=200.0)
        # Fresh position, low adverse
        assert mgr.get_exit_mode("BIO") == ExitMode.PASSIVE


class TestDailyPnlReset:
    def test_reset_pnl_baseline(self, mgr):
        mgr._equity = 50.0
        mgr._equity_ever_confirmed = True
        mgr._session_start_equity = 48.0
        mgr._daily_pnl = 2.0
        mgr.reset_pnl_baseline()
        assert mgr._daily_pnl == 0.0
        assert mgr._session_start_equity == 50.0


class TestGetPosition:
    def test_returns_copy(self, mgr):
        """Codex #9: get_position returns a copy, not a reference."""
        mgr.record_fill("BIO", "bid", price=0.05, size=200.0)
        pos1 = mgr.get_position("BIO")
        pos1.size = 999.0  # mutate the copy
        pos2 = mgr.get_position("BIO")
        assert pos2.size == 200.0  # original unchanged
