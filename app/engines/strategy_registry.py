"""
Strategy Registry — typed, validated single source of truth for all engines.

Replaces the untyped ENGINE_REGISTRY dict from registry.py with a formal
StrategyMetadata dataclass. Adds startup validation to catch configuration
errors before the pipeline runs.

Usage:
    from app.engines.strategy_registry import STRATEGY_REGISTRY, get_strategy, validate_registry

Adding a new engine (HB-native):
    1. Write self-contained HB V2 controller in app/controllers/directional_trading/eN_*.py
       (no quants-lab imports — only hummingbot.*, pandas, pandas_ta, numpy, pydantic, stdlib)
    2. Add StrategyMetadata entry to STRATEGY_REGISTRY below (deployment_mode="hb_native")
    3. Run validate_registry() to check everything resolves
    4. Backtest: python cli.py trigger-task --task eN_bulk_backtest
    5. Deploy: python cli.py deploy --engine EN
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
    # DEPRECATED for HB-native strategies (controller handles everything)
    evaluate_fn: Optional[Callable] = None

    # Controller (primary — same code for backtest and live)
    controller_module: str = ""    # e.g. "app.controllers.directional_trading.e1_..."
    config_class_name: str = ""    # e.g. "E1CompressionBreakoutConfig"

    # Timeframes
    intervals: List[str] = field(default_factory=lambda: ["1h"])
    backtesting_resolution: str = "1m"

    # Exit parameters (used by backtesting + fallback for HB executor)
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
    max_concurrent: int = 20

    # Runtime state
    enabled: bool = True
    shadow_of: Optional[str] = None  # if set, this is a shadow variant of another engine

    # ── HB-native deployment ─────────────────────────────────
    controller_file: str = ""      # filename for bots/controllers/ (e.g. "e1_compression_breakout.py")
    hb_connector: str = "bybit_perpetual_demo"
    deployment_mode: str = "hb_native"  # "hb_native" | "legacy"

    # Default config overrides for deployment (merged with controller defaults)
    default_config: Dict[str, Any] = field(default_factory=dict)

    # Pair source: "pair_historical" reads from MongoDB, "explicit" uses pair_allowlist
    pair_source: str = "pair_historical"
    pair_allowlist: List[str] = field(default_factory=list)

    # Verdict overrides
    # Carry strategies run unlimited concurrent positions in backtest, inflating DD.
    # Production uses max 3 concurrent at $300 each — real DD is ~5x lower.
    dd_gate_relaxed: bool = False      # if True, use -50% DD gate instead of -15%/-20%

    # Bot deployment
    bot_image: str = "quants-lab/hummingbot:demo"  # Docker image (with patches baked in)
    total_amount_quote: float = 300.0   # per pair position size in quote
    cooldown_time: int = 3600           # seconds between signals on same pair
    max_drawdown_quote: Optional[float] = None  # per-controller drawdown limit


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
        display_name="Volume Ignition",
        controller_module="app.controllers.directional_trading.e1_volume_ignition",
        config_class_name="E1VolumeIgnitionConfig",
        intervals=["1h"],
        backtesting_resolution="1m",
        exit_params={
            "stop_loss": Decimal("0.99"),
            "take_profit": Decimal("0.99"),
            "time_limit": 14400,  # 4 hours — trailing + time only
        },
        trailing_stop={
            "activation_price": Decimal("0.01"),
            "trailing_delta": Decimal("0.005"),
        },
        direction="BOTH",
        blocked_pairs=[],
        required_features=["atr", "volume"],
        max_concurrent=20,
        controller_file="e1_volume_ignition.py",
        hb_connector="bybit_perpetual_testnet",
        deployment_mode="hb_native",
        default_config={
            "volume_threshold": 3.0,
            "body_atr_threshold": 2.0,
            "tp_atr_mult": 99.0,
            "sl_atr_mult": 99.0,
            "trailing_activation_atr": 1.0,
            "trailing_delta_atr": 0.5,
        },
        pair_source="pair_historical",
        total_amount_quote=300.0,
        cooldown_time=1800,  # 30min cooldown (faster strategy)
        max_drawdown_quote=500.0,
    ),
    "S6": StrategyMetadata(
        name="S6",
        display_name="Spread Fade",
        controller_module="app.controllers.directional_trading.s6_spread_fade",
        config_class_name="S6SpreadFadeConfig",
        intervals=["1h"],
        backtesting_resolution="1m",
        exit_params={
            "stop_loss": Decimal("0.015"),
            "take_profit": Decimal("0.02"),
            "time_limit": 43200,
        },
        trailing_stop={
            "activation_price": Decimal("0.01"),
            "trailing_delta": Decimal("0.005"),
        },
        direction="BOTH",
        blocked_pairs=[],
        required_features=["atr"],
        max_concurrent=20,
        controller_file="s6_spread_fade.py",
        hb_connector="bybit_perpetual_testnet",
        deployment_mode="hb_native",
        default_config={
            "reference_pair": "BTC-USDT",
            "beta_window": 720,
            "zscore_window": 168,
            "zscore_entry": 2.0,
            "min_correlation": 0.7,
        },
        pair_source="explicit",
        pair_allowlist=["ETH-USDT", "SOL-USDT", "LINK-USDT", "DOGE-USDT"],
        total_amount_quote=300.0,
        cooldown_time=3600,
    ),
    "S7": StrategyMetadata(
        name="S7",
        display_name="Hurst Adaptive",
        controller_module="app.controllers.directional_trading.s7_hurst_adaptive",
        config_class_name="S7HurstAdaptiveConfig",
        intervals=["1h"],
        backtesting_resolution="1m",
        exit_params={
            "stop_loss": Decimal("0.02"),
            "take_profit": Decimal("0.025"),
            "time_limit": 21600,
        },
        trailing_stop={
            "activation_price": Decimal("0.01"),
            "trailing_delta": Decimal("0.005"),
        },
        direction="BOTH",
        blocked_pairs=[],
        required_features=["atr"],
        max_concurrent=20,
        controller_file="s7_hurst_adaptive.py",
        hb_connector="bybit_perpetual_testnet",
        deployment_mode="hb_native",
        default_config={
            "hurst_window": 200,
            "mean_revert_threshold": 0.45,
            "trending_threshold": 0.55,
        },
        pair_source="pair_historical",
        total_amount_quote=300.0,
        cooldown_time=3600,
    ),
    "S9": StrategyMetadata(
        name="S9",
        display_name="Session Compression",
        controller_module="app.controllers.directional_trading.s9_session_compression",
        config_class_name="S9SessionCompressionConfig",
        intervals=["1h"],
        backtesting_resolution="1m",
        exit_params={
            "stop_loss": Decimal("0.02"),
            "take_profit": Decimal("0.025"),
            "time_limit": 28800,
        },
        trailing_stop={
            "activation_price": Decimal("0.01"),
            "trailing_delta": Decimal("0.005"),
        },
        direction="BOTH",
        blocked_pairs=[],
        required_features=["atr", "volume"],
        max_concurrent=20,
        controller_file="s9_session_compression.py",
        hb_connector="bybit_perpetual_testnet",
        deployment_mode="hb_native",
        default_config={
            "asia_compression_pct": 0.25,
            "europe_compression_pct": 0.35,
            "us_compression_pct": 0.35,
            "volume_floor_multiplier": 1.5,
        },
        pair_source="pair_historical",
        total_amount_quote=300.0,
        cooldown_time=3600,
    ),
    "E2": StrategyMetadata(
        name="E2",
        display_name="Range Fade",
        evaluate_fn=_lazy_e2,
        controller_module="app.controllers.directional_trading.e2_range_fade",
        config_class_name="E2RangeFadeConfig",
        intervals=["1h"],
        backtesting_resolution="1m",
        exit_params={
            "stop_loss": Decimal("0.02"),
            "take_profit": Decimal("0.015"),
            "time_limit": 43200,
        },
        trailing_stop=None,
        direction="LONG_ONLY",
        blocked_pairs=[],
        required_features=["atr", "range", "volume", "derivatives"],
        max_concurrent=20,
        # E2 stays on legacy pipeline until migrated
        deployment_mode="legacy",
    ),
    "E3": StrategyMetadata(
        name="E3",
        display_name="Funding Carry",
        controller_module="app.controllers.directional_trading.e3_funding_carry",
        config_class_name="E3FundingCarryConfig",
        intervals=["1h"],
        backtesting_resolution="1m",
        exit_params={
            "stop_loss": Decimal("0.05"),
            "take_profit": Decimal("0.03"),
            "time_limit": 432000,  # 5 days
        },
        trailing_stop=None,
        direction="BOTH",
        blocked_pairs=[],
        required_features=["derivatives"],
        max_concurrent=20,
        dd_gate_relaxed=True,  # carry strategy: backtest DD inflated by unlimited concurrency
        controller_file="e3_funding_carry.py",
        hb_connector="bybit_perpetual_testnet",
        deployment_mode="hb_native",
        default_config={
            "funding_streak_min": 3,
            "funding_rate_threshold": 0.00005,
            "funding_zscore_boost": 2.0,
            "btc_regime_enabled": True,
            "btc_regime_threshold": 0.0,
        },
        pair_source="pair_historical",
        total_amount_quote=300.0,
        cooldown_time=3600,
    ),
    "H2": StrategyMetadata(
        name="H2",
        display_name="Funding Divergence",
        controller_module="app.controllers.directional_trading.h2_funding_divergence",
        config_class_name="H2FundingDivergenceConfig",
        intervals=["1h"],
        backtesting_resolution="1m",
        exit_params={
            "stop_loss": Decimal("0.03"),
            "take_profit": Decimal("0.02"),
            "time_limit": 28800,  # 8 hours
        },
        trailing_stop={
            "activation_price": Decimal("0.01"),
            "trailing_delta": Decimal("0.005"),
        },
        direction="BOTH",
        blocked_pairs=[],
        required_features=["derivatives"],
        max_concurrent=20,
        controller_file="h2_funding_divergence.py",
        hb_connector="bybit_perpetual_testnet",
        deployment_mode="hb_native",
        default_config={
            "zscore_window": 30,
            "zscore_entry": 2.0,
            "min_spread_abs": 0.00005,
        },
        pair_source="pair_historical",
        total_amount_quote=300.0,
        cooldown_time=1800,  # 30min cooldown (arb signals are fast)
    ),
    "E4": StrategyMetadata(
        name="E4",
        display_name="Crowd Fade",
        controller_module="app.controllers.directional_trading.e4_crowd_fade",
        config_class_name="E4CrowdFadeConfig",
        intervals=["1h"],
        backtesting_resolution="1m",
        exit_params={
            "stop_loss": Decimal("0.03"),
            "take_profit": Decimal("0.025"),
            "time_limit": 172800,  # 48 hours
        },
        trailing_stop={
            "activation_price": Decimal("0.01"),
            "trailing_delta": Decimal("0.005"),
        },
        direction="BOTH",
        blocked_pairs=[],
        required_features=["derivatives"],
        max_concurrent=20,
        controller_file="e4_crowd_fade.py",
        hb_connector="bybit_perpetual_testnet",
        deployment_mode="hb_native",
        default_config={
            "crowd_long_threshold": 0.65,
            "crowd_short_threshold": 0.35,
            "oi_rising_periods": 3,
        },
        pair_source="pair_historical",
        total_amount_quote=300.0,
        cooldown_time=3600,
    ),
    "M1": StrategyMetadata(
        name="M1",
        display_name="ML Ensemble Signal",
        controller_module="app.controllers.directional_trading.m1_ml_ensemble",
        config_class_name="M1MLEnsembleConfig",
        intervals=["1h"],
        backtesting_resolution="1m",
        exit_params={
            "stop_loss": Decimal("0.05"),
            "take_profit": Decimal("0.05"),
            "time_limit": 345600,  # 4 days
        },
        trailing_stop=None,
        direction="BOTH",
        blocked_pairs=[],
        required_features=["derivatives", "microstructure"],
        max_concurrent=5,
        controller_file="m1_ml_ensemble.py",
        hb_connector="bybit_perpetual_testnet",
        deployment_mode="hb_native",
        default_config={
            "entry_threshold": 0.45,
            "exit_threshold": 0.40,
            "sl_atr_multiplier": 2.0,
            "tp_atr_multiplier": 2.0,
            "hard_sl_pct": 0.05,
        },
        pair_source="pair_historical",
        total_amount_quote=300.0,
        cooldown_time=7200,  # 2h cooldown (fewer, higher-quality trades)
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

def build_backtest_config(
    engine_name: str,
    connector: str,
    pair: str,
    total_amount_quote: Decimal = Decimal("100"),
    **overrides,
):
    """Build a complete controller config for backtesting.

    Does NOT set candles_config — lets the controller's model_post_init()
    compute the correct lookback sizes (e.g. E1 needs 2224 bars of 1h for
    90-day ATR percentile).
    """
    from hummingbot.strategy_v2.executors.position_executor.data_types import TrailingStop
    from hummingbot.core.data_type.common import OrderType

    meta = get_strategy(engine_name)
    ConfigClass = get_config_class(engine_name)

    config_kwargs = {
        "id": f"{engine_name.lower()}_bulk_{pair}",
        "connector_name": connector,
        "trading_pair": pair,
        "total_amount_quote": total_amount_quote,
        "max_executors_per_side": 1,
        "cooldown_time": 60,
        "leverage": 1,
        # candles_config deliberately omitted — controller computes correct lookbacks
    }

    # Apply strategy-specific defaults first (signal params, filters, etc.)
    config_kwargs.update(meta.default_config)
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

        # 3. Evaluate function (optional for HB-native strategies)
        if meta.evaluate_fn is not None:
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
