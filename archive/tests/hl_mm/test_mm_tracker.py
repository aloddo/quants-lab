"""Tests for mm_tracker.py — Competitor MM tracking and crowding detection."""
import time

import pytest

from app.services.hl_mm.mm_tracker import MMTracker, MMWallet, CoinCrowding


@pytest.fixture
def tracker():
    return MMTracker(target_coins=["BIO", "ORDI"], poll_interval_s=1.0)


class TestCrowdingComputation:
    def test_no_wallets_zero_crowding(self, tracker):
        tracker._compute_crowding()
        crowding = tracker.get_crowding("BIO")
        assert crowding.mm_count == 0
        assert crowding.crowding_score == 0.0

    def test_single_mm_long_gives_some_crowding(self, tracker):
        tracker._wallets["0x1"] = MMWallet(
            address="0x1", positions={"BIO": 100.0}, prev_positions={},
        )
        tracker._compute_crowding()
        crowding = tracker.get_crowding("BIO")
        assert crowding.mm_count == 1
        assert crowding.net_mm_direction == 1.0

    def test_opposing_mms_reduce_crowding(self, tracker):
        tracker._wallets["0x1"] = MMWallet(
            address="0x1", positions={"BIO": 100.0}, prev_positions={},
        )
        tracker._wallets["0x2"] = MMWallet(
            address="0x2", positions={"BIO": -80.0}, prev_positions={},
        )
        tracker._compute_crowding()
        crowding = tracker.get_crowding("BIO")
        assert crowding.mm_count == 2
        # Opposing directions: alignment = 0/2 = 0
        assert crowding.crowding_score < 0.5

    def test_all_same_direction_high_crowding(self, tracker):
        for i in range(5):
            tracker._wallets[f"0x{i}"] = MMWallet(
                address=f"0x{i}", positions={"BIO": 50.0}, prev_positions={},
            )
        tracker._compute_crowding()
        crowding = tracker.get_crowding("BIO")
        assert crowding.mm_count == 5
        assert crowding.crowding_score > 0.8


class TestIsCrowded:
    def test_not_crowded_empty(self, tracker):
        assert tracker.is_crowded("BIO") is False

    def test_crowded_threshold(self, tracker):
        for i in range(5):
            tracker._wallets[f"0x{i}"] = MMWallet(
                address=f"0x{i}", positions={"BIO": 50.0}, prev_positions={},
            )
        tracker._compute_crowding()
        assert tracker.is_crowded("BIO", threshold=0.6) is True


class TestMMReducingSide:
    def test_no_signal_without_positions(self, tracker):
        assert tracker.mm_reducing_side("BIO") is None

    def test_detects_mm_selling(self, tracker):
        tracker._wallets["0x1"] = MMWallet(
            address="0x1",
            positions={"BIO": 50.0},
            prev_positions={"BIO": 200.0},  # was 200, now 50 → reducing longs
        )
        tracker._compute_crowding()
        # position_change_velocity = 50 - 200 = -150
        # threshold = -0.1 * 50 = -5 → velocity(-150) < -5 → bid signal
        result = tracker.mm_reducing_side("BIO")
        assert result == "bid"


class TestUpdateTargetCoins:
    def test_updates_set(self, tracker):
        tracker.update_target_coins({"SOL", "BTC"})
        assert tracker.target_coins == {"SOL", "BTC"}


class TestMongoExport:
    def test_exports_crowding_docs(self, tracker):
        tracker._wallets["0x1"] = MMWallet(
            address="0x1", positions={"BIO": 100.0}, prev_positions={},
        )
        tracker._compute_crowding()
        docs = tracker.to_mongo_docs()
        assert len(docs) >= 1
        assert any(d["coin"] == "BIO" for d in docs)
