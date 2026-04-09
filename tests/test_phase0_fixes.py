"""
Tests for Phase 0 emergency fixes.

Validates:
- pair_selector returns all pairs when top_n <= 0
- TRADEABLE_PAIRS safeguard blocks non-tradeable pairs
- watchdog API health checks handle unreachable services gracefully
"""
import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch


# ── 0A: pair_selector top_n=0 returns all ────────────────

class TestPairSelectorTopN:
    """Verify that top_n <= 0 returns all eligible pairs."""

    def test_top_n_zero_returns_all(self):
        from app.engines.pair_selector import get_eligible_pairs

        mock_mongo = AsyncMock()
        mock_mongo.get_documents = AsyncMock(side_effect=self._mock_documents)

        result, rejections = asyncio.get_event_loop().run_until_complete(
            get_eligible_pairs(mock_mongo, "E1", top_n=0)
        )
        assert len(result) == 3  # 3 pairs with features

    def test_top_n_positive_limits(self):
        from app.engines.pair_selector import get_eligible_pairs

        mock_mongo = AsyncMock()
        mock_mongo.get_documents = AsyncMock(side_effect=self._mock_documents)

        result, rejections = asyncio.get_event_loop().run_until_complete(
            get_eligible_pairs(mock_mongo, "E1", top_n=2)
        )
        assert len(result) == 2

    async def _mock_documents(self, collection, query, **kwargs):
        """Return mock docs based on collection and query."""
        if collection == "pair_historical":
            return [
                {"pair": "BTC-USDT", "verdict": "ALLOW", "profit_factor": 1.5, "win_rate": 55},
                {"pair": "ETH-USDT", "verdict": "ALLOW", "profit_factor": 1.3, "win_rate": 52},
                {"pair": "SOL-USDT", "verdict": "ALLOW", "profit_factor": 1.1, "win_rate": 50},
            ]
        elif collection == "features":
            pair = query.get("trading_pair", "")
            fname = query.get("feature_name", "")
            if fname == "atr":
                return [{"value": {"atr_percentile_90d": 0.3, "atr_14_1h": 100}}]
            elif fname == "volume":
                return [{"value": {"vol_zscore_20": 1.0, "vol_avg_20": 1000}}]
            elif fname == "range":
                return [{"value": {"range_high_20": 100, "range_low_20": 90, "range_width": 10, "range_expanding": 0.1}}]
            elif fname == "momentum":
                return [{"value": {"ema_alignment": 1.0}}]
            elif fname == "candle_structure":
                return [{"value": {"price_position_in_range": 0.5}}]
            return []
        return []


# ── 0B: TRADEABLE_PAIRS safeguard ──────────────────────

class TestTradeablePairs:
    """Verify the TRADEABLE_PAIRS safety gate."""

    def test_known_pairs_are_tradeable(self):
        from app.tasks.screening.signal_scan_task import TRADEABLE_PAIRS
        assert "BTC-USDT" in TRADEABLE_PAIRS
        assert "ETH-USDT" in TRADEABLE_PAIRS
        assert "SOL-USDT" in TRADEABLE_PAIRS

    def test_blocked_pairs_not_tradeable(self):
        from app.tasks.screening.signal_scan_task import TRADEABLE_PAIRS
        assert "PIPPIN-USDT" not in TRADEABLE_PAIRS
        assert "PUMPFUN-USDT" not in TRADEABLE_PAIRS
        assert "TRUMP-USDT" not in TRADEABLE_PAIRS
        assert "FARTCOIN-USDT" not in TRADEABLE_PAIRS

    def test_count_matches_documented(self):
        from app.tasks.screening.signal_scan_task import TRADEABLE_PAIRS
        assert len(TRADEABLE_PAIRS) == 27


# ── 0C: Watchdog API health checks ────────────────────

class TestWatchdogApiHealth:
    """Verify watchdog detects unreachable APIs."""

    def test_unreachable_api_detected(self):
        from app.tasks.watchdog_task import WatchdogTask

        mock_config = MagicMock()
        mock_config.name = "watchdog"
        mock_config.config = {}
        task = WatchdogTask(mock_config)

        with patch.dict("os.environ", {
            "HUMMINGBOT_API_USERNAME": "admin",
            "HUMMINGBOT_API_PASSWORD": "admin",
        }):
            issues = asyncio.get_event_loop().run_until_complete(
                task._check_api_health()
            )
            assert isinstance(issues, list)
            # QL API is down — should be flagged
            ql_issues = [i for i in issues if "QL API" in i]
            assert len(ql_issues) >= 1, f"Expected QL API issue, got: {issues}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
