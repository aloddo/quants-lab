"""
Tests for Phase 1: Strategy Factory core infrastructure.

Validates:
- StrategyMetadata and STRATEGY_REGISTRY
- CandidateBase inheritance
- Generic signal dispatch via registry
- Backward compatibility with old ENGINE_REGISTRY
- Scaffold CLI generates valid files
"""
import asyncio
import os
import shutil
import tempfile
import pytest
from unittest.mock import AsyncMock, MagicMock


class TestStrategyRegistry:
    """Verify the typed strategy registry."""

    def test_registry_has_e1_e2(self):
        from app.engines.strategy_registry import STRATEGY_REGISTRY
        assert "E1" in STRATEGY_REGISTRY
        assert "E2" in STRATEGY_REGISTRY

    def test_metadata_fields(self):
        from app.engines.strategy_registry import get_strategy
        e1 = get_strategy("E1")
        assert e1.display_name == "Compression Breakout"
        assert e1.intervals == ["1h", "5m"]
        assert e1.backtesting_resolution == "5m"
        assert e1.direction == "BOTH"
        assert e1.max_concurrent == 2
        assert "stop_loss" in e1.exit_params
        assert e1.enabled is True

    def test_unknown_strategy_raises(self):
        from app.engines.strategy_registry import get_strategy
        with pytest.raises(KeyError):
            get_strategy("NONEXISTENT")

    def test_evaluate_fn_resolves(self):
        from app.engines.strategy_registry import get_evaluate_fn
        from app.engines.e1_compression_breakout import evaluate_e1
        fn = get_evaluate_fn("E1")
        assert fn is evaluate_e1

    def test_validate_registry_passes(self):
        from app.engines.strategy_registry import validate_registry
        errors = validate_registry()
        assert len(errors) == 0, f"Validation errors: {errors}"


class TestCandidateBase:
    """Verify CandidateBase inheritance and serialization."""

    def test_e1_inherits(self):
        from app.engines.models import CandidateBase
        from app.engines.e1_compression_breakout import E1Candidate
        assert issubclass(E1Candidate, CandidateBase)

    def test_e2_inherits(self):
        from app.engines.models import CandidateBase
        from app.engines.e2_range_fade import E2Candidate
        assert issubclass(E2Candidate, CandidateBase)

    def test_to_dict(self):
        from app.engines.e1_compression_breakout import E1Candidate
        c = E1Candidate(pair="BTC-USDT", direction="LONG")
        d = c.to_dict()
        assert d["pair"] == "BTC-USDT"
        assert d["direction"] == "LONG"
        assert "candidate_id" in d
        assert "disposition" in d

    def test_base_fields_present(self):
        from app.engines.e2_range_fade import E2Candidate
        c = E2Candidate(pair="ETH-USDT")
        # Base fields
        assert hasattr(c, "trigger_fired")
        assert hasattr(c, "disposition")
        assert hasattr(c, "composite_score")
        # E2-specific fields
        assert hasattr(c, "tp_price")
        assert hasattr(c, "sl_price")
        assert hasattr(c, "range_midpoint")


class TestGenericDispatch:
    """Verify generic evaluation dispatch works."""

    def test_e1_dispatch(self):
        from app.engines.strategy_registry import get_evaluate_fn
        from app.engines.models import DecisionSnapshot, FeatureRow

        fn = get_evaluate_fn("E1")
        fr = FeatureRow(pair="BTC-USDT", timestamp_utc=0, close=50000.0)
        snap = DecisionSnapshot(pair="BTC-USDT", features=fr, staleness_ok=True)
        cand = fn(snap)
        assert cand.pair == "BTC-USDT"
        assert cand.disposition in ("SKIPPED_NO_TRIGGER", "FILTERED_STALENESS", "CANDIDATE_READY")

    def test_e2_dispatch_reads_from_featurerow(self):
        """E2 should read candle_high/low/open from FeatureRow, not separate params."""
        from app.engines.strategy_registry import get_evaluate_fn
        from app.engines.models import DecisionSnapshot, FeatureRow

        fn = get_evaluate_fn("E2")
        fr = FeatureRow(
            pair="BTC-USDT", timestamp_utc=0, close=50000.0,
            atr_percentile_90d=0.15,  # compressed
            atr_14_1h=500.0,
            range_high_20=51000.0, range_low_20=49000.0,
            candle_high=50500.0, candle_low=48900.0,  # touches low
            candle_open=49500.0,
            range_expanding=0.05,
        )
        snap = DecisionSnapshot(pair="BTC-USDT", features=fr, staleness_ok=True)
        cand = fn(snap)
        assert cand.pair == "BTC-USDT"
        # Should process (may or may not trigger depending on other filters)
        assert cand.disposition != "FILTERED_STALENESS"


class TestBackwardCompat:
    """Verify old ENGINE_REGISTRY still works."""

    def test_engine_registry_dict(self):
        from app.engines.registry import ENGINE_REGISTRY
        assert "E1" in ENGINE_REGISTRY
        assert ENGINE_REGISTRY["E1"]["intervals"] == ["1h", "5m"]

    def test_get_engine(self):
        from app.engines.registry import get_engine
        e1 = get_engine("E1")
        assert e1.display_name == "Compression Breakout"

    def test_build_backtest_config(self):
        from app.engines.registry import build_backtest_config
        config = build_backtest_config("E1", "bybit_perpetual", "BTC-USDT")
        assert config.trading_pair == "BTC-USDT"


class TestScaffold:
    """Verify scaffold-strategy generates valid files."""

    def test_scaffold_creates_files(self):
        from cli import scaffold_strategy

        # Use the real app directories
        try:
            scaffold_strategy("E99", "Test Strategy")
            engine_path = "app/engines/e99_test_strategy.py"
            controller_path = "app/controllers/directional_trading/e99_test_strategy.py"

            assert os.path.exists(engine_path)
            assert os.path.exists(controller_path)

            # Verify the engine file is importable
            import importlib
            mod = importlib.import_module("app.engines.e99_test_strategy")
            assert hasattr(mod, "evaluate_e99")
            assert hasattr(mod, "E99Candidate")

            # Verify candidate inherits from CandidateBase
            from app.engines.models import CandidateBase
            assert issubclass(mod.E99Candidate, CandidateBase)
        finally:
            # Clean up
            for p in [engine_path, controller_path]:
                if os.path.exists(p):
                    os.remove(p)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
