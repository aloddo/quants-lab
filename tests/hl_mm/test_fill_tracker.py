"""Tests for V2 fill_tracker changes: per-side EWMA markout, sign convention."""
import time
import pytest

from app.services.hl_mm.fill_tracker import FillTracker, Fill


@pytest.fixture
def tracker():
    return FillTracker()


class TestSideMarkoutEWMA:
    """V2: Per-side EWMA markout for EV gating."""

    def test_separate_bid_ask_tracking(self, tracker):
        """Bid and ask should have independent EWMA values."""
        tracker.record_fill("BIO", "bid", 0.055, 100, 5.5, 0.001, 1)
        tracker.record_fill("BIO", "ask", 0.056, 100, 5.6, 0.001, 2)

        # Simulate markout: bid fill was adverse (-3bps), ask fill was favorable (+2bps)
        fills = list(tracker._fills)
        fills[0].markout_5s = -3.0
        fills[1].markout_5s = 2.0
        tracker._update_toxicity(fills[0])
        tracker._update_toxicity(fills[1])

        bid_ewma, bid_count = tracker.get_side_markout_ewma("BIO", "bid")
        ask_ewma, ask_count = tracker.get_side_markout_ewma("BIO", "ask")

        assert bid_count == 1
        assert ask_count == 1
        assert bid_ewma == -3.0  # first fill = raw value
        assert ask_ewma == 2.0

    def test_ewma_decays_with_new_fills(self, tracker):
        """EWMA should weight recent fills more."""
        # First fill: markout = -5bps
        tracker.record_fill("BIO", "bid", 0.055, 100, 5.5, 0.001, 1)
        fill1 = list(tracker._fills)[0]
        fill1.markout_5s = -5.0
        tracker._update_toxicity(fill1)

        # Second fill: markout = +1bps
        tracker.record_fill("BIO", "bid", 0.055, 100, 5.5, 0.001, 2)
        fill2 = list(tracker._fills)[1]
        fill2.markout_5s = 1.0
        tracker._update_toxicity(fill2)

        ewma, count = tracker.get_side_markout_ewma("BIO", "bid")
        assert count == 2
        # EWMA: 0.15 * 1.0 + 0.85 * (-5.0) = 0.15 - 4.25 = -4.10
        assert abs(ewma - (-4.1)) < 0.01

    def test_no_fills_returns_zero(self, tracker):
        ewma, count = tracker.get_side_markout_ewma("BIO", "bid")
        assert ewma == 0.0
        assert count == 0

    def test_different_coins_independent(self, tracker):
        tracker.record_fill("BIO", "bid", 0.055, 100, 5.5, 0.001, 1)
        tracker.record_fill("ORDI", "bid", 5.0, 5, 25.0, 0.003, 2)

        fill1 = list(tracker._fills)[0]
        fill1.markout_5s = -2.0
        tracker._update_toxicity(fill1)

        fill2 = list(tracker._fills)[1]
        fill2.markout_5s = 3.0
        tracker._update_toxicity(fill2)

        bio_ewma, _ = tracker.get_side_markout_ewma("BIO", "bid")
        ordi_ewma, _ = tracker.get_side_markout_ewma("ORDI", "bid")
        assert bio_ewma == -2.0
        assert ordi_ewma == 3.0


class TestSignConvention:
    """V2: Verify sign convention for EV formula.

    Convention: favorable markout = POSITIVE, adverse = NEGATIVE.
    EV formula converts: markout_cost = max(0, -ewma)
    So adverse ewma of -3 becomes cost of +3.
    """

    def test_adverse_markout_is_negative(self, tracker):
        """If price moves against us after buying, markout should be negative."""
        # Buy at 100, price drops to 99.5 → adverse → negative markout
        tracker.record_fill("BIO", "bid", 100.0, 1.0, 100.0, 0.01, 1)
        fill = list(tracker._fills)[0]
        # update_markouts would compute this, but we set directly for unit test
        # bid fill: direction = +1, price dropped = (99.5 - 100)/100 * 10000 * 1 = -50bps
        fill.markout_5s = -5.0  # adverse
        tracker._update_toxicity(fill)

        ewma, _ = tracker.get_side_markout_ewma("BIO", "bid")
        assert ewma < 0  # adverse = negative

        # EV formula: markout_cost = max(0, -(-5)) = 5 bps
        markout_cost = max(0.0, -ewma)
        assert markout_cost == 5.0

    def test_favorable_markout_is_positive(self, tracker):
        """If price moves in our favor, markout should be positive."""
        tracker.record_fill("BIO", "bid", 100.0, 1.0, 100.0, 0.01, 1)
        fill = list(tracker._fills)[0]
        fill.markout_5s = 3.0  # favorable
        tracker._update_toxicity(fill)

        ewma, _ = tracker.get_side_markout_ewma("BIO", "bid")
        assert ewma > 0  # favorable = positive

        # EV formula: markout_cost = max(0, -(+3)) = 0 bps (no penalty)
        markout_cost = max(0.0, -ewma)
        assert markout_cost == 0.0
