"""
Engine Registry — single source of truth for engine metadata.

Used by:
- BulkBacktestTask (resolution, candles, exit params, config class)
- Bulk backtest notebooks (make_config pattern)
- Skills reference (no hard-coded engine details in .claude/skills/)

Adding a new engine:
1. Write evaluation function in app/engines/e3_*.py
2. Write HB V2 controller in app/controllers/directional_trading/e3_*.py
3. Add entry to ENGINE_REGISTRY below
4. Add to signal_scan engines list in config/hermes_pipeline.yml
5. Add bulk backtest task entry in hermes_pipeline.yml
"""
from decimal import Decimal
from typing import Any, Dict, List

from hummingbot.data_feed.candles_feed.data_types import CandlesConfig


def _build_candles_config(
    connector: str, pair: str, intervals: List[str],
) -> List[CandlesConfig]:
    """
    Build CandlesConfig list from engine's declared intervals.

    max_records controls the buffer the backtesting engine requests BEFORE
    the backtest start. If start_time - (max_records * interval_seconds) is
    before the cached parquet data starts, the engine falls through to a live
    API fetch -- which fails intermittently (Bybit rate limits / no auth).

    Keep max_records small so the buffer stays within cached data. The engine
    already has all cached data loaded via load_cached_data=True; the
    controller reads the full feed directly for lookback (ATR percentile etc).
    """
    MAX_RECORDS = {"1h": 50, "5m": 50}

    configs = []
    for interval in intervals:
        mr = MAX_RECORDS.get(interval, 50)
        configs.append(CandlesConfig(
            connector=connector, trading_pair=pair,
            interval=interval, max_records=mr,
        ))
    return configs


ENGINE_REGISTRY: Dict[str, Dict[str, Any]] = {
    "E1": {
        "name": "Compression Breakout",
        "architecture": "two_layer",  # 1h setup + 5m trigger
        "intervals": ["1h", "5m"],
        "backtesting_resolution": "5m",
        "controller_module": "app.controllers.directional_trading.e1_compression_breakout",
        "config_class_name": "E1CompressionBreakoutConfig",
        "exit_params": {
            "stop_loss": Decimal("0.015"),
            "take_profit": Decimal("0.03"),
            "time_limit": 86400,
            "entry_quality_filter": False,  # disabled after stress testing (Apr 2026)
        },
        "trailing_stop": {
            "activation_price": Decimal("0.015"),
            "trailing_delta": Decimal("0.005"),
        },
        "direction": "BOTH",
        "blocked_pairs": ["BTC-USDT"],
    },
    "E2": {
        "name": "Range Fade",
        "architecture": "single_layer",  # 1h only
        "intervals": ["1h"],
        "backtesting_resolution": "1h",
        "controller_module": "app.controllers.directional_trading.e2_range_fade",
        "config_class_name": "E2RangeFadeConfig",
        "exit_params": {
            "stop_loss": Decimal("0.02"),
            "take_profit": Decimal("0.015"),
            "time_limit": 43200,
        },
        "trailing_stop": None,
        "direction": "LONG_ONLY",
        "blocked_pairs": [],
    },
}


def get_engine(engine_name: str) -> Dict[str, Any]:
    """Get engine metadata. Raises KeyError if not registered."""
    if engine_name not in ENGINE_REGISTRY:
        raise KeyError(f"Unknown engine: {engine_name}. Registered: {list(ENGINE_REGISTRY.keys())}")
    return ENGINE_REGISTRY[engine_name]


def get_config_class(engine_name: str):
    """Dynamically import and return the config class for an engine."""
    import importlib
    engine = get_engine(engine_name)
    module = importlib.import_module(engine["controller_module"])
    return getattr(module, engine["config_class_name"])


def build_backtest_config(
    engine_name: str,
    connector: str,
    pair: str,
    total_amount_quote: Decimal = Decimal("100"),
    **overrides,
):
    """
    Build a complete controller config for backtesting.
    Handles candles_config, exit params, and trailing stop automatically.
    """
    from hummingbot.strategy_v2.executors.position_executor.data_types import TrailingStop
    from hummingbot.core.data_type.common import OrderType

    engine = get_engine(engine_name)
    ConfigClass = get_config_class(engine_name)

    # Build candles config from engine's declared intervals
    candles = _build_candles_config(connector, pair, engine["intervals"])

    # Base config
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

    # Exit params
    config_kwargs.update(engine["exit_params"])

    # Trailing stop
    if engine.get("trailing_stop"):
        config_kwargs["trailing_stop"] = TrailingStop(**engine["trailing_stop"])
        config_kwargs["take_profit_order_type"] = OrderType.MARKET

    # User overrides
    config_kwargs.update(overrides)

    return ConfigClass(**config_kwargs)
