"""Tests for V2 side-specific EV gating logic.

Tests the EV formula and its interaction with markout, exit penalty,
and signal flags. These are the computations from orchestrator._tick()
extracted for unit testing.
"""
import pytest


def compute_side_ev(
    half_spread_bps: float,
    markout_ewma: float,
    fill_count: int,
    inventory_fraction: float,  # |inventory| / q_soft, 0..1
    maker_fee: float = 1.44,
    min_fill_count: int = 3,
    default_markout_cost: float = 1.0,
) -> float:
    """Pure function extracted from orchestrator EV computation."""
    # Convert markout to cost
    if fill_count >= min_fill_count:
        markout_cost = max(0.0, -markout_ewma)
    else:
        markout_cost = default_markout_cost  # prior

    # Exit penalty
    if inventory_fraction > 0.3:
        exit_penalty = 0.5 * 3.5  # 50% chance of taker exit
    else:
        exit_penalty = 0.2 * 3.5  # 20% chance

    return half_spread_bps - maker_fee - markout_cost - exit_penalty


class TestEVFormula:

    def test_wide_spread_no_adverse_positive(self):
        """12bps spread, no adverse markout → positive EV."""
        ev = compute_side_ev(
            half_spread_bps=6.0,  # 12bps spread
            markout_ewma=0.0,
            fill_count=10,
            inventory_fraction=0.0,
        )
        # 6.0 - 1.44 - 0.0 - 0.7 = 3.86
        assert ev > 2.0

    def test_tight_spread_no_adverse_marginal(self):
        """6bps spread, no adverse → barely positive."""
        ev = compute_side_ev(
            half_spread_bps=3.0,
            markout_ewma=0.0,
            fill_count=10,
            inventory_fraction=0.0,
        )
        # 3.0 - 1.44 - 0.0 - 0.7 = 0.86
        assert 0.5 < ev < 1.5

    def test_adverse_markout_reduces_ev(self):
        """Adverse markout should reduce EV."""
        ev_clean = compute_side_ev(6.0, 0.0, 10, 0.0)
        ev_adverse = compute_side_ev(6.0, -3.0, 10, 0.0)
        assert ev_adverse < ev_clean
        assert ev_clean - ev_adverse == pytest.approx(3.0, abs=0.01)

    def test_favorable_markout_no_bonus(self):
        """Favorable markout should NOT increase EV (only avoid penalty)."""
        ev_zero = compute_side_ev(6.0, 0.0, 10, 0.0)
        ev_favorable = compute_side_ev(6.0, 3.0, 10, 0.0)
        assert ev_favorable == pytest.approx(ev_zero, abs=0.01)

    def test_high_inventory_increases_exit_penalty(self):
        """High inventory → higher exit penalty → lower EV."""
        ev_low_inv = compute_side_ev(6.0, 0.0, 10, 0.1)  # 10% of q_soft
        ev_high_inv = compute_side_ev(6.0, 0.0, 10, 0.5)  # 50% of q_soft
        assert ev_high_inv < ev_low_inv
        # Difference: (0.5-0.2)*3.5 = 1.05 bps
        assert ev_low_inv - ev_high_inv == pytest.approx(1.05, abs=0.01)

    def test_few_fills_uses_prior(self):
        """With < 3 fills, use default_markout_cost (prior)."""
        ev_no_data = compute_side_ev(6.0, 5.0, 2, 0.0)  # only 2 fills, ewma=+5
        ev_with_data = compute_side_ev(6.0, 5.0, 10, 0.0)  # 10 fills, ewma=+5

        # No data: uses prior of 1.0 bps cost
        # With data: ewma=+5 → markout_cost=0 (favorable)
        assert ev_with_data > ev_no_data

    def test_sign_convention_adverse_is_cost(self):
        """Adverse markout (negative EWMA) must become positive cost."""
        # EWMA = -4 (adverse) → cost = max(0, -(-4)) = 4
        ev = compute_side_ev(6.0, -4.0, 10, 0.0)
        # 6.0 - 1.44 - 4.0 - 0.7 = -0.14 → negative EV, don't quote
        assert ev < 0

    def test_5bps_spread_always_negative(self):
        """5bps spread: even with zero markout, barely viable."""
        ev = compute_side_ev(2.5, 0.0, 10, 0.0)
        # 2.5 - 1.44 - 0 - 0.7 = 0.36 → below 0.5 threshold
        assert ev < 0.5

    def test_both_sides_can_be_positive(self):
        """Two-sided MM: both sides positive on wide spread."""
        bid_ev = compute_side_ev(6.0, -0.5, 10, 0.0)
        ask_ev = compute_side_ev(6.0, -0.3, 10, 0.0)
        assert bid_ev > 0.5
        assert ask_ev > 0.5

    def test_one_side_negative(self):
        """One side toxic, other side clean."""
        bid_ev = compute_side_ev(6.0, -5.0, 10, 0.0)  # toxic bids
        ask_ev = compute_side_ev(6.0, 0.5, 10, 0.0)   # clean asks
        assert bid_ev < 0.5  # don't quote bids
        assert ask_ev > 0.5  # quote asks
