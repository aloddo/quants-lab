"""Tests for V2 wallet toxicity scorer."""
import time
import pytest

from app.services.hl_mm.wallet_scorer import WalletScorer


@pytest.fixture
def scorer():
    return WalletScorer(min_trades=5, min_notional=100.0, toxic_threshold_bps=-1.5)


class TestRecordTrade:

    def test_records_trade(self, scorer):
        scorer.record_trade("BIO", "B", 0.055, 100, "0xbuyer", "0xseller")
        stats = scorer.get_wallet_stats("0xbuyer")  # buyer is aggressor on "B"
        assert stats is not None
        assert stats.trade_count == 1
        assert stats.total_notional == pytest.approx(5.5, abs=0.01)

    def test_tracks_aggressor(self, scorer):
        """Aggressor is buyer on B side, seller on S side."""
        scorer.record_trade("BIO", "B", 1.0, 10, "0xbuyer", "0xseller")
        scorer.record_trade("BIO", "S", 1.0, 10, "0xbuyer2", "0xseller2")

        buyer_stats = scorer.get_wallet_stats("0xbuyer")
        seller_stats = scorer.get_wallet_stats("0xseller2")
        assert buyer_stats.trade_count == 1  # aggressor on B
        assert seller_stats.trade_count == 1  # aggressor on S

    def test_ignores_empty_addresses(self, scorer):
        scorer.record_trade("BIO", "B", 1.0, 10, "", "0xseller")
        assert scorer.get_wallet_stats("") is None


class TestAttributeMarkout:

    def test_attributes_to_counterparty(self, scorer):
        """Our bid fill → aggressor is seller (they sold to us)."""
        now = time.time()
        # Trade happens: someone sells to us
        scorer.record_trade("BIO", "S", 0.055, 100, "0xbuyer", "0xtoxic_seller")

        # Our bid fill markout comes back adverse (-3bps)
        scorer.attribute_markout("BIO", "bid", 0.055, now, -3.0)

        # The seller should have the markout attributed
        stats = scorer.get_wallet_stats("0xtoxic_seller")
        assert stats is not None
        assert stats.ewma_markout == pytest.approx(-3.0, abs=0.1)

    def test_ask_fill_attributes_to_buyer(self, scorer):
        """Our ask fill → aggressor is buyer (they bought from us)."""
        now = time.time()
        scorer.record_trade("BIO", "B", 0.055, 100, "0xtoxic_buyer", "0xseller")
        scorer.attribute_markout("BIO", "ask", 0.055, now, -4.0)

        stats = scorer.get_wallet_stats("0xtoxic_buyer")
        assert stats is not None
        assert stats.ewma_markout == pytest.approx(-4.0, abs=0.1)


class TestToxicityDetection:

    def test_becomes_toxic_after_threshold(self, scorer):
        """Wallet with consistent adverse markout should be flagged toxic."""
        now = time.time()
        # 10 trades with -3bps markout each
        for i in range(10):
            scorer.record_trade("BIO", "B", 1.0, 100, "0xtoxic", f"0xmaker{i}")
            scorer.attribute_markout("BIO", "ask", 1.0, now + i * 0.1, -3.0)

        stats = scorer.get_wallet_stats("0xtoxic")
        assert stats.trade_count >= 5
        assert stats.is_toxic is True

    def test_not_toxic_with_few_trades(self, scorer):
        """Need min_trades before flagging."""
        now = time.time()
        for i in range(3):  # < min_trades=5
            scorer.record_trade("BIO", "B", 1.0, 100, "0xwallet", f"0x{i}")
            scorer.attribute_markout("BIO", "ask", 1.0, now + i * 0.1, -5.0)

        stats = scorer.get_wallet_stats("0xwallet")
        assert stats.is_toxic is False

    def test_not_toxic_with_positive_markout(self, scorer):
        """Wallet with favorable markout should not be toxic."""
        now = time.time()
        for i in range(10):
            scorer.record_trade("BIO", "B", 1.0, 100, "0xclean", f"0x{i}")
            scorer.attribute_markout("BIO", "ask", 1.0, now + i * 0.1, 2.0)

        stats = scorer.get_wallet_stats("0xclean")
        assert stats.is_toxic is False


class TestLiveGating:

    def test_toxic_activity_detected(self, scorer):
        """When a toxic wallet trades, is_toxic_active should return True."""
        now = time.time()
        # Build up toxic wallet
        for i in range(10):
            scorer.record_trade("BIO", "B", 1.0, 100, "0xtoxic", f"0x{i}")
            scorer.attribute_markout("BIO", "ask", 1.0, now + i * 0.1, -3.0)

        # Toxic wallet trades again
        scorer.record_trade("BIO", "B", 1.0, 50, "0xtoxic", "0xvictim")

        is_active, count = scorer.is_toxic_active("BIO", lookback_s=10.0)
        assert is_active is True
        assert count >= 1

    def test_no_toxic_activity_on_clean_coin(self, scorer):
        is_active, count = scorer.is_toxic_active("ORDI")
        assert is_active is False
        assert count == 0

    def test_toxic_activity_expires(self, scorer):
        """Activity older than lookback should not count."""
        now = time.time()
        for i in range(10):
            scorer.record_trade("BIO", "B", 1.0, 100, "0xtoxic", f"0x{i}")
            scorer.attribute_markout("BIO", "ask", 1.0, now + i * 0.1, -3.0)

        # Record activity in the past (simulate by directly manipulating)
        scorer._toxic_activity["BIO"] = [(now - 20.0, "0xtoxic")]  # 20s ago

        is_active, count = scorer.is_toxic_active("BIO", lookback_s=10.0)
        assert is_active is False


class TestMongoExport:

    def test_exports_wallets_with_data(self, scorer):
        now = time.time()
        for i in range(6):
            scorer.record_trade("BIO", "B", 1.0, 100, "0xwallet", f"0x{i}")
            scorer.attribute_markout("BIO", "ask", 1.0, now + i * 0.1, -2.0)

        docs = scorer.to_mongo_docs()
        assert len(docs) >= 1
        wallet_doc = next(d for d in docs if d["address"] == "0xwallet")
        assert wallet_doc["trade_count"] == 6
        assert wallet_doc["ewma_markout"] < 0
