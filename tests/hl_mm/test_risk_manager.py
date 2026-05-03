"""Tests for risk_manager.py — Portfolio risk checks and stops."""
import time
from datetime import datetime, timezone

import pytest

from app.services.hl_mm.risk_manager import RiskManager, RiskConfig, RiskAction, RiskState


@pytest.fixture
def rm():
    return RiskManager(RiskConfig(
        daily_stop_usd=3.0,
        hard_stop_usd=5.0,
        max_gross_notional=20.0,
        max_net_exposure=14.0,
        max_gross_resting=40.0,
    ))


class TestDailyStop:
    def test_triggers_on_loss(self, rm):
        state = rm.evaluate(
            daily_pnl=-3.5,
            gross_notional=10.0,
            net_exposure=5.0,
            live_pair_count=2,
            hl_book_ages_ms={"BIO": 100},
            has_inventory={"BIO": False},
        )
        assert RiskAction.DAILY_STOP in state.actions
        assert state.is_daily_stopped

    def test_sticky_across_calls(self, rm):
        rm.evaluate(daily_pnl=-3.5, gross_notional=0, net_exposure=0,
                    live_pair_count=0, hl_book_ages_ms={}, has_inventory={})
        # Even with positive PnL next call, still stopped
        state = rm.evaluate(daily_pnl=1.0, gross_notional=0, net_exposure=0,
                           live_pair_count=0, hl_book_ages_ms={}, has_inventory={})
        assert RiskAction.DAILY_STOP in state.actions


class TestHardStop:
    def test_triggers_on_large_loss(self, rm):
        state = rm.evaluate(
            daily_pnl=-5.5,
            gross_notional=0,
            net_exposure=0,
            live_pair_count=0,
            hl_book_ages_ms={},
            has_inventory={},
        )
        assert RiskAction.HARD_STOP in state.actions


class TestGapRisk:
    def test_stale_book_triggers_cancel(self, rm):
        state = rm.evaluate(
            daily_pnl=0,
            gross_notional=0,
            net_exposure=0,
            live_pair_count=1,
            hl_book_ages_ms={"BIO": 2000.0},  # > 1500ms
            has_inventory={"BIO": False},
        )
        assert RiskAction.CANCEL_ALL_QUOTES in state.actions

    def test_stale_with_inventory_triggers_hedge(self, rm):
        state = rm.evaluate(
            daily_pnl=0,
            gross_notional=0,
            net_exposure=0,
            live_pair_count=1,
            hl_book_ages_ms={"BIO": 4000.0},  # > 3000ms
            has_inventory={"BIO": True},
        )
        assert RiskAction.HEDGE_IMMEDIATELY in state.actions


class TestNotionalLimits:
    def test_within_limit_allowed(self, rm):
        assert rm.check_notional_limit(10.0, 5.0) is True

    def test_exceeds_limit_blocked(self, rm):
        assert rm.check_notional_limit(18.0, 5.0) is False


class TestNetExposure:
    def test_exit_side_always_allowed(self, rm):
        """Bug #2 fix: exit-side orders bypass net limit."""
        # We're long (net > 0), selling (is_buy=False) reduces exposure → always ok
        assert rm.check_net_exposure(
            current_net=13.0, new_order_exposure=5.0,
            is_buy=False, has_inventory=True,
        ) is True

    def test_entry_side_blocked_at_limit(self, rm):
        # Already at limit, buying more
        assert rm.check_net_exposure(
            current_net=13.0, new_order_exposure=5.0,
            is_buy=True, has_inventory=False,
        ) is False


class TestRestingNotional:
    def test_within_limit(self, rm):
        assert rm.check_resting_notional(30.0) is True

    def test_exceeds_limit(self, rm):
        assert rm.check_resting_notional(45.0) is False


class TestFundingAvoidance:
    def test_returns_bool(self, rm):
        result = rm.is_funding_avoidance_window()
        assert isinstance(result, bool)
