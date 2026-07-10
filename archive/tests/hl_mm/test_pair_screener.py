"""Tests for pair_screener.py — Pair ranking, lifecycle, EV scoring."""
import time
from unittest.mock import MagicMock, patch

import pytest

from app.services.hl_mm.pair_screener import (
    PairScreener, PairRanking, ScreenerConfig, BLOCKED_PAIRS, BYBIT_PERPS,
)


@pytest.fixture
def screener():
    """PairScreener with mocked HL Info and MongoDB."""
    info = MagicMock()
    with patch("app.services.hl_mm.pair_screener.MongoClient") as mock_client:
        mock_db = MagicMock()
        mock_client.return_value.__getitem__ = MagicMock(return_value=mock_db)
        s = PairScreener(info=info, config=ScreenerConfig(max_live_pairs=3))
    return s


class TestScorePairFromStats:
    def test_wide_spread_coin_gets_positive_score(self, screener):
        stats = {
            "median_spread_bps": 12.0,
            "depth_bid_usd": 1000.0,
            "depth_ask_usd": 1000.0,
            "avg_mid_px": 0.05,
            "count": 100,
        }
        ranking = screener._score_pair_from_stats("BIO", 500_000, stats)
        assert ranking is not None
        assert ranking.score > 0
        assert ranking.edge_room_bps > 0

    def test_tight_spread_gets_penalty(self, screener):
        stats = {
            "median_spread_bps": 4.0,  # below 8bps penalty threshold
            "depth_bid_usd": 1000.0,
            "depth_ask_usd": 1000.0,
            "avg_mid_px": 0.05,
            "count": 100,
        }
        ranking = screener._score_pair_from_stats("BIO", 500_000, stats)
        assert ranking is not None
        # Tight spread gets heavy penalty
        assert ranking.score < 50.0  # much lower than wide spread

    def test_no_anchor_gets_penalty(self, screener):
        stats = {
            "median_spread_bps": 12.0,
            "depth_bid_usd": 1000.0,
            "depth_ask_usd": 1000.0,
            "avg_mid_px": 0.05,
            "count": 100,
        }
        # Use a coin NOT in BYBIT_PERPS
        ranking = screener._score_pair_from_stats("RANDOMCOIN", 500_000, stats)
        assert ranking is not None
        assert ranking.anchor_type == "none"


class TestManagePairLifecycle:
    def test_promotes_top_n_positive_score(self, screener):
        """Bug P0 #1 fix: lifecycle gates on score > 0, not edge_room > 0."""
        rankings = [
            PairRanking(coin="BIO", timestamp=time.time(), score=100.0, spread_bps=10,
                       edge_room_bps=3.0, daily_volume_usd=1e6, depth_bid_usd=500,
                       depth_ask_usd=500, anchor_type="direct", tox_estimate=1.0,
                       status="IDLE", native_half_spread=5.0, trade_count_hint=100,
                       sz_decimals=0, leverage_available=5),
            PairRanking(coin="ORDI", timestamp=time.time(), score=50.0, spread_bps=8,
                       edge_room_bps=2.0, daily_volume_usd=500_000, depth_bid_usd=500,
                       depth_ask_usd=500, anchor_type="direct", tox_estimate=1.0,
                       status="IDLE", native_half_spread=4.0, trade_count_hint=50,
                       sz_decimals=2, leverage_available=5),
        ]
        screener._manage_pair_lifecycle(rankings)
        assert "BIO" in screener.active_pairs
        assert "ORDI" in screener.active_pairs

    def test_does_not_promote_negative_edge(self, screener):
        """Pairs with negative edge_room_bps are not promoted."""
        rankings = [
            PairRanking(coin="BAD", timestamp=time.time(), score=-5.0, spread_bps=3,
                       edge_room_bps=-0.5, daily_volume_usd=100_000, depth_bid_usd=100,
                       depth_ask_usd=100, anchor_type="none", tox_estimate=1.0,
                       status="IDLE", native_half_spread=1.5, trade_count_hint=10,
                       sz_decimals=4, leverage_available=5),
        ]
        screener._manage_pair_lifecycle(rankings)
        assert "BAD" not in screener.active_pairs

    def test_demotes_pair_on_drop(self, screener):
        screener._active_pairs = {"BIO", "ORDI"}
        # Only BIO in top N now
        rankings = [
            PairRanking(coin="BIO", timestamp=time.time(), score=100.0, spread_bps=10,
                       edge_room_bps=3.0, daily_volume_usd=1e6, depth_bid_usd=500,
                       depth_ask_usd=500, anchor_type="direct", tox_estimate=1.0,
                       status="IDLE", native_half_spread=5.0, trade_count_hint=100,
                       sz_decimals=0, leverage_available=5),
        ]
        screener._manage_pair_lifecycle(rankings)
        assert "ORDI" not in screener.active_pairs
        assert "ORDI" in screener.get_pending_idle_close()


class TestBlockedPairs:
    def test_mega_blocked(self):
        assert "MEGA" in BLOCKED_PAIRS

    def test_purr_blocked(self):
        assert "PURR" in BLOCKED_PAIRS


class TestForceActive:
    def test_bypasses_screener(self, screener):
        screener.force_active("DASH")
        assert "DASH" in screener.active_pairs

    def test_force_block_removes(self, screener):
        screener._active_pairs = {"BIO"}
        screener.force_block("BIO")
        assert "BIO" not in screener.active_pairs


class TestMongoInitSafety:
    def test_survives_mongo_down(self):
        """Bug P1 #8 fix: MongoDB being down shouldn't crash init."""
        info = MagicMock()
        with patch("app.services.hl_mm.pair_screener.MongoClient") as mock_client:
            mock_db = MagicMock()
            mock_col = MagicMock()
            mock_col.create_index.side_effect = Exception("MongoDB connection refused")
            mock_db.__getitem__ = MagicMock(return_value=mock_col)
            mock_client.return_value.__getitem__ = MagicMock(return_value=mock_db)
            # Should not raise
            s = PairScreener(info=info)
            assert s is not None
