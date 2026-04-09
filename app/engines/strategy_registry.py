"""
Strategy Registry — typed, validated single source of truth for all engines.

Replaces the untyped ENGINE_REGISTRY dict from registry.py with a formal
StrategyMetadata dataclass. Adds startup validation to catch configuration
errors before the pipeline runs.

Usage:
    from app.engines.strategy_registry import STRATEGY_REGISTRY, get_strategy, validate_registry

Adding a new engine:
    1. Write evaluate_eN() in app/engines/eN_*.py (pure function)
    2. Write HB V2 controller in app/controllers/directional_trading/eN_*.py
    3. Add StrategyMetadata entry to STRATEGY_REGISTRY below
    4. Run validate_registry() to check everything resolves
    5. Add engine name to signal_scan engines list in hermes_pipeline.yml
"""
import importlib
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Dict, List, Optional, Type

from hummingbot.data_feed.candles_feed.data_types import CandlesConfig

logger = logging.getLogger(__name__)


@dataclass
class StrategyMetadata:
    """Typed metadata for a trading strategy / engine."""

    # Identity
    name: str                      # e.g. "E1"
    display_name: str              # e.g. "Compression Breakout"

    # Evaluation function: (DecisionSnapshot) -> CandidateBase
    evaluate_fn: Callable

    # Backtesting controller
    controller_module: str         # e.g. "app.controllers.directional_trading.e1_..."
    config_class_name: str         # e.g. "E1CompressionBreakoutConfig"

    # Timeframes
    intervals: List[str]           # e.g. ["1h", "5m"]
    backtesting_resolution: str    # e.g. "5m"

    # Exit parameters (used by testnet resolver + backtesting)
    exit_params: Dict[str, Any] = field(default_factory=dict)
    trailing_stop: Optional[Dict[str, Any]] = None

    # Direction and pair constraints
    direction: str = "BOTH"        # "BOTH", "LONG_ONLY", "SHORT_ONLY"
    blocked_pairs: List[str] = field(default_factory=list)

    # Required features (validated against ALL_FEATURES)
    required_features: List[str] = field(
        default_factory=lambda: ["atr", "range", "volume"]
    )

    # Portfolio limits
    max_concurrent: int = 2

    # Runtime state
    enabled: bool = True
    shadow_of: Optional[str] = None  # if set, this is a shadow variant of another engine


# ── Registry ──────────────────────────────────────────────

def _lazy_e1():
    from app.engines.e1_compression_breakout import evaluate_e1
    return evaluate_e1

def _lazy_e2():
    from app.engines.e2_range_fade import evaluate_e2
    return evaluate_e2


STRATEGY_REGISTRY: Dict[str, StrategyMetadata] = {
    "E1": StrategyMetadata(
        name="E1",
        display_name="Compression Breakout",
        evaluate_fn=_lazy_e1,
        controller_module="app.controllers.directional_trading.e1_compression_breakout",
        config_class_name="E1CompressionBreakoutConfig",
        intervals=["1h", "5m"],
        backtesting_resolution="5m",
        # Fallback exit params — E1 now computes dynamic ATR-based TP/SL
        # in evaluate_e1() (tp_price/sl_price on E1Candidate) and in the
        # controller's get_executor_config(). These values are used only
        # when dynamic levels are unavailable.
        exit_params={
            "stop_loss": Decimal("0.015"),
            "take_profit": Decimal("0.03"),
            "time_limit": 86400,
            "entry_quality_filter": False,
        },
        trailing_stop={
            "activation_price": Decimal("0.015"),
            "trailing_delta": Decimal("0.005"),
        },
        direction="BOTH",
        blocked_pairs=["BTC-USDT"],
        required_features=["atr", "range", "volume", "momentum", "derivatives"],
        max_concurrent=2,
    ),
    "E2": StrategyMetadata(
        name="E2",
        display_name="Range Fade",
        evaluate_fn=_lazy_e2,
        controller_module="app.controllers.directional_trading.e2_range_fade",
        config_class_name="E2RangeFadeConfig",
        intervals=["1h"],
        backtesting_resolution="1h",
        exit_params={
            "stop_loss": Decimal("0.02"),
            "take_profit": Decimal("0.015"),
            "time_limit": 43200,
        },
        trailing_stop=None,
        direction="LONG_ONLY",
        blocked_pairs=[],
        required_features=["atr", "range", "volume", "derivatives"],
        max_concurrent=1,
    ),
}


# ── Accessors ─────────────────────────────────────────────

def get_strategy(name: str) -> StrategyMetadata:
    """Get strategy metadata. Raises KeyError if not registered."""
    if name not in STRATEGY_REGISTRY:
        raise KeyError(
            f"Unknown strategy: {name}. "
            f"Registered: {list(STRATEGY_REGISTRY.keys())}"
        )
    return STRATEGY_REGISTRY[name]


def get_evaluate_fn(name: str) -> Callable:
    """Get the resolved evaluation function for a strategy.

    Handles lazy wrappers: if evaluate_fn is a zero-arg callable that
    returns the actual function, resolve it and cache the result.
    """
    meta = get_strategy(name)
    fn = meta.evaluate_fn
    # Detect lazy wrapper: function name starts with _lazy_
    if callable(fn) and getattr(fn, '__name__', '').startswith('_lazy_'):
        fn = fn()
        meta.evaluate_fn = fn  # cache for next call
    return fn


def get_config_class(name: str):
    """Dynamically import and return the config class for an engine."""
    meta = get_strategy(name)
    module = importlib.import_module(meta.controller_module)
    return getattr(module, meta.config_class_name)


# ── Backtesting support (migrated from registry.py) ──────

def _build_candles_config(
    connector: str, pair: str, intervals: List[str],
) -> List[CandlesConfig]:
    MAX_RECORDS = {"1h": 50, "5m": 50}
    configs = []
    for interval in intervals:
        mr = MAX_RECORDS.get(interval, 50)
        configs.append(CandlesConfig(
            connector=connector, trading_pair=pair,
            interval=interval, max_records=mr,
        ))
    return configs


def build_backtest_config(
    engine_name: str,
    connector: str,
    pair: str,
    total_amount_quote: Decimal = Decimal("100"),
    **overrides,
):
    """Build a complete controller config for backtesting."""
    from hummingbot.strategy_v2.executors.position_executor.data_types import TrailingStop
    from hummingbot.core.data_type.common import OrderType

    meta = get_strategy(engine_name)
    ConfigClass = get_config_class(engine_name)

    candles = _build_candles_config(connector, pair, meta.intervals)

    config_kwargs = {
        "id": f"{engine_name.lower()}_bulk_{pair}",
        "connector_name": connector,
        "trading_pair": pair,
        "total_amount_quote": total_amount_quote,
        "max_executors_per_side": 1,
        "cooldown_time": 60,
        "leverage": 1,
        "candles_config": candles,
    }

    config_kwargs.update(meta.exit_params)

    if meta.trailing_stop:
        config_kwargs["trailing_stop"] = TrailingStop(**meta.trailing_stop)
        config_kwargs["take_profit_order_type"] = OrderType.MARKET

    config_kwargs.update(overrides)

    return ConfigClass(**config_kwargs)


# ── Validation ────────────────────────────────────────────

def validate_registry() -> List[str]:
    """
    Validate all registry entries. Returns list of errors (empty = OK).

    Checks:
    1. controller_module is importable
    2. config_class_name exists in that module
    3. evaluate_fn resolves to a callable
    4. required_features have corresponding FeatureBase classes
    """
    errors = []

    for name, meta in STRATEGY_REGISTRY.items():
        # 1. Controller module
        try:
            mod = importlib.import_module(meta.controller_module)
        except ImportError as e:
            errors.append(f"{name}: cannot import controller_module '{meta.controller_module}': {e}")
            continue

        # 2. Config class
        if not hasattr(mod, meta.config_class_name):
            errors.append(
                f"{name}: config class '{meta.config_class_name}' "
                f"not found in {meta.controller_module}"
            )

        # 3. Evaluate function
        try:
            fn = get_evaluate_fn(name)
            if not callable(fn):
                errors.append(f"{name}: evaluate_fn is not callable: {fn}")
        except Exception as e:
            errors.append(f"{name}: evaluate_fn resolution failed: {e}")

        # 4. Required features
        try:
            from app.features import ALL_FEATURES
            available = {f.__name__.replace("Feature", "").lower() for f in ALL_FEATURES}
            for feat in meta.required_features:
                if feat not in available and feat != "derivatives":
                    # derivatives comes from MongoDB, not FeatureBase
                    errors.append(f"{name}: required feature '{feat}' not in ALL_FEATURES")
        except ImportError:
            pass  # features module not available (e.g. in tests)

    if errors:
        for e in errors:
            logger.error(f"Registry validation: {e}")
    else:
        logger.info(
            f"Registry validation: {len(STRATEGY_REGISTRY)} strategies OK "
            f"({', '.join(STRATEGY_REGISTRY.keys())})"
        )

    return errors
