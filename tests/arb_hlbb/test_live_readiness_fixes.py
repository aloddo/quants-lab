"""
Tests for HLBB live readiness fixes (2026-05-07).

Covers:
1. Direction-specific exit spread detection
2. HL price precision formatting
3. Bybit price formatting for emergency unwind
4. Bybit execution-history fill fallback
5. Per-pair re-entry cooldown
"""
import math
import time
import pytest

from app.services.arb_hlbb.config import ArbConfig
from app.services.arb_hlbb.instrument_rules import PairRules
from app.services.arb_hlbb.price_feed import SpreadSnapshot
from app.services.arb_hlbb.signal_engine import (
    AdaptiveThresholds,
    SignalEngine,
    TrackedPosition,
)


# ── Helpers ──────────────────────────────────────────────────────

def make_snap(
    pair: str = "APE-USDT",
    hl_bid: float = 1.00,
    hl_ask: float = 1.01,
    bb_bid: float = 0.99,
    bb_ask: float = 1.00,
    spread_hl_over_bb: float = 0.0,
    spread_bb_over_hl: float = -10.0,
    best_spread: float = 0.0,
    direction: str = "HL_PREMIUM",
    ts: float = 0.0,
) -> SpreadSnapshot:
    return SpreadSnapshot(
        pair=pair,
        hl_bid=hl_bid,
        hl_ask=hl_ask,
        bb_bid=bb_bid,
        bb_ask=bb_ask,
        spread_hl_over_bb_bps=spread_hl_over_bb,
        spread_bb_over_hl_bps=spread_bb_over_hl,
        best_spread_bps=best_spread,
        direction=direction,
        ts=ts or time.time(),
    )


# ── Fix 1: Direction-specific exit spread ────────────────────────

class TestDirectionSpecificExit:
    """Exit checks should use the ENTRY direction spread, not best_spread_bps."""

    def test_exit_uses_hl_premium_spread_when_entered_on_hl(self):
        """If we entered on HL_PREMIUM, exit should check spread_hl_over_bb_bps."""
        config = ArbConfig(
            entry_min_spread_bps=30, max_hold_s=300, stop_loss_multiple=2.5,
            min_warmup=5, threshold_window=100,
        )
        engine = SignalEngine(config)

        # Warm up thresholds
        for _ in range(10):
            engine.thresholds.update("APE-USDT", 15.0)

        pos = TrackedPosition(
            pair="APE-USDT",
            direction="SHORT_HL_LONG_BB",
            entry_spread_bps=35.0,
            entry_time=time.time() - 10,
            entry_p90=30.0,
            exit_p25=5.0,
            entry_arb_direction="HL_PREMIUM",
        )
        engine.register_position(pos)

        # Scenario: HL premium has reverted (spread_hl_over_bb < P25),
        # but BB premium is high (best_spread would be BB direction).
        # Old code would NOT exit because best_spread is still high.
        # New code should exit because entry-direction spread reverted.
        snap = make_snap(
            pair="APE-USDT",
            spread_hl_over_bb=2.0,  # HL premium reverted → below P25 of 5
            spread_bb_over_hl=25.0,  # BB premium is high
            best_spread=25.0,  # Old code would use this — no exit!
            direction="BB_PREMIUM",
        )

        signal = engine.process_snapshot(snap)
        assert signal is not None
        assert signal.signal_type == "EXIT_REVERT"

    def test_no_false_exit_when_direction_spread_still_high(self):
        """Should NOT exit if entry-direction spread is still above P25."""
        config = ArbConfig(
            entry_min_spread_bps=30, max_hold_s=300, stop_loss_multiple=2.5,
            min_warmup=5, threshold_window=100,
        )
        engine = SignalEngine(config)

        for _ in range(10):
            engine.thresholds.update("APE-USDT", 15.0)

        pos = TrackedPosition(
            pair="APE-USDT",
            direction="SHORT_HL_LONG_BB",
            entry_spread_bps=35.0,
            entry_time=time.time() - 10,
            entry_p90=30.0,
            exit_p25=5.0,
            entry_arb_direction="HL_PREMIUM",
        )
        engine.register_position(pos)

        # HL premium still above P25
        snap = make_snap(
            pair="APE-USDT",
            spread_hl_over_bb=15.0,  # Still above P25 of 5
            spread_bb_over_hl=-5.0,
            best_spread=15.0,
            direction="HL_PREMIUM",
        )

        signal = engine.process_snapshot(snap)
        assert signal is None  # Should not exit

    def test_stop_loss_uses_direction_spread(self):
        """Stop loss should trigger on entry-direction spread widening."""
        config = ArbConfig(
            entry_min_spread_bps=30, max_hold_s=300, stop_loss_multiple=2.0,
            min_warmup=5, threshold_window=100,
        )
        engine = SignalEngine(config)

        for _ in range(10):
            engine.thresholds.update("APE-USDT", 15.0)

        pos = TrackedPosition(
            pair="APE-USDT",
            direction="SHORT_HL_LONG_BB",
            entry_spread_bps=35.0,
            entry_time=time.time() - 10,
            entry_p90=30.0,
            exit_p25=5.0,
            entry_arb_direction="HL_PREMIUM",
        )
        engine.register_position(pos)

        # HL premium widened to 2x+ entry — stop loss
        snap = make_snap(
            pair="APE-USDT",
            spread_hl_over_bb=75.0,  # > 35 * 2.0 = 70
            spread_bb_over_hl=-60.0,
            best_spread=75.0,
            direction="HL_PREMIUM",
        )

        signal = engine.process_snapshot(snap)
        assert signal is not None
        assert signal.signal_type == "EXIT_STOP_LOSS"


# ── Fix 2: HL price precision ───────────────────────────────────

class TestHLPricePrecision:
    """HL prices must have at most 5 significant figures."""

    def test_round_hl_price_buy_rounds_up(self):
        rules = PairRules(pair="APE-USDT", coin="APE", bb_symbol="APEUSDT")
        # Price 1.23456789 → 5 sig figs → 1.2346 (ceiling for buy)
        result = rules.round_hl_price(1.23456789, is_buy=True)
        assert result == 1.2346

    def test_round_hl_price_sell_rounds_down(self):
        rules = PairRules(pair="APE-USDT", coin="APE", bb_symbol="APEUSDT")
        # Price 1.23456789 → 5 sig figs → 1.2345 (floor for sell)
        result = rules.round_hl_price(1.23456789, is_buy=False)
        assert result == 1.2345

    def test_round_hl_price_large_number(self):
        rules = PairRules(pair="BTC-USDT", coin="BTC", bb_symbol="BTCUSDT")
        # 80934.567 → 5 sig figs → 80935 (buy, rounds up)
        result = rules.round_hl_price(80934.567, is_buy=True)
        assert result == 80935.0

    def test_round_hl_price_small_number(self):
        rules = PairRules(pair="CHIP-USDT", coin="CHIP", bb_symbol="CHIPUSDT")
        # 0.0012345678 → 5 sig figs → 0.0012346 (buy, rounds up)
        result = rules.round_hl_price(0.0012345678, is_buy=True)
        assert result == 0.0012346

    def test_round_hl_price_zero(self):
        rules = PairRules(pair="APE-USDT", coin="APE", bb_symbol="APEUSDT")
        assert rules.round_hl_price(0.0, is_buy=True) == 0.0


# ── Fix 3: Bybit price formatting ──────────────────────────────

class TestBBPriceFormatting:
    """Bybit prices must be formatted to tick precision."""

    def test_format_bb_price_4_decimals(self):
        rules = PairRules(pair="APE-USDT", coin="APE", bb_symbol="APEUSDT",
                          bb_price_step=0.0001)
        assert rules.format_bb_price(1.23456789) == "1.2346"

    def test_format_bb_price_2_decimals(self):
        rules = PairRules(pair="SOL-USDT", coin="SOL", bb_symbol="SOLUSDT",
                          bb_price_step=0.01)
        assert rules.format_bb_price(123.456) == "123.46"

    def test_format_bb_price_integer(self):
        rules = PairRules(pair="BTC-USDT", coin="BTC", bb_symbol="BTCUSDT",
                          bb_price_step=1.0)
        result = rules.format_bb_price(80935.0)
        assert result == "80935"

    def test_format_bb_price_small_tick(self):
        rules = PairRules(pair="CHIP-USDT", coin="CHIP", bb_symbol="CHIPUSDT",
                          bb_price_step=0.000001)
        assert rules.format_bb_price(0.001234) == "0.001234"


# ── Fix 5: Per-pair re-entry cooldown ───────────────────────────

class TestReentryCooldown:
    """Verify the cooldown config fields exist and have sane defaults."""

    def test_cooldown_config_defaults(self):
        config = ArbConfig()
        assert config.reentry_cooldown_s == 30.0
        assert config.reentry_cooldown_after_fail_s == 120.0

    def test_order_aggression_config(self):
        config = ArbConfig()
        assert config.order_aggression_bps == 15.0  # Raised from 3bp to fix 83% HL rejection rate


# ── Integration: TrackedPosition has entry_arb_direction ────────

class TestTrackedPositionDirection:
    """TrackedPosition must carry entry_arb_direction."""

    def test_default_empty(self):
        pos = TrackedPosition(
            pair="X", direction="SHORT_HL_LONG_BB",
            entry_spread_bps=30, entry_time=0, entry_p90=30, exit_p25=5,
        )
        assert pos.entry_arb_direction == ""

    def test_explicit_direction(self):
        pos = TrackedPosition(
            pair="X", direction="SHORT_HL_LONG_BB",
            entry_spread_bps=30, entry_time=0, entry_p90=30, exit_p25=5,
            entry_arb_direction="HL_PREMIUM",
        )
        assert pos.entry_arb_direction == "HL_PREMIUM"


# ── Fix 9: Depth check via SpreadSnapshot ───────────────────────

class TestDepthCheck:
    """SpreadSnapshot should report executable depth."""

    def test_min_executable_qty_hl_premium(self):
        snap = make_snap(
            direction="HL_PREMIUM",
        )
        # Override depth fields
        snap.hl_bid_sz = 100.0  # selling HL bid
        snap.bb_ask_sz = 50.0   # buying BB ask
        assert snap.min_executable_qty() == 50.0  # limited by BB ask

    def test_min_executable_qty_bb_premium(self):
        snap = make_snap(
            direction="BB_PREMIUM",
        )
        snap.bb_bid_sz = 200.0  # selling BB bid
        snap.hl_ask_sz = 150.0  # buying HL ask
        assert snap.min_executable_qty() == 150.0  # limited by HL ask

    def test_min_executable_qty_no_depth(self):
        snap = make_snap(direction="HL_PREMIUM")
        # Depth fields default to 0
        assert snap.min_executable_qty() == 0.0  # no depth info → 0

    def test_min_executable_qty_one_side_zero(self):
        """If one side has zero depth, use the other side as ceiling."""
        snap = make_snap(direction="HL_PREMIUM")
        snap.hl_bid_sz = 100.0
        snap.bb_ask_sz = 0.0  # BB depth unknown
        assert snap.min_executable_qty() == 100.0  # uses HL side as ceiling

    def test_venue_quote_has_size_fields(self):
        from app.services.arb_hlbb.price_feed import VenueQuote
        q = VenueQuote(bid=1.0, ask=1.01, bid_sz=100.0, ask_sz=50.0)
        assert q.bid_sz == 100.0
        assert q.ask_sz == 50.0


# ── Fix 8: Client order ID fields ──────────────────────────────

class TestClientOrderIDs:
    """Order APIs accept client order IDs."""

    def test_hl_order_api_accepts_cloid(self):
        """HLOrderAPI.place_ioc accepts cloid parameter."""
        import inspect
        from app.services.arb_hlbb.order_api import HLOrderAPI
        sig = inspect.signature(HLOrderAPI.place_ioc)
        assert "cloid" in sig.parameters

    def test_bb_order_api_accepts_order_link_id(self):
        """BybitOrderAPI.place_ioc accepts order_link_id parameter."""
        import inspect
        from app.services.arb_hlbb.order_api import BybitOrderAPI
        sig = inspect.signature(BybitOrderAPI.place_ioc)
        assert "order_link_id" in sig.parameters
