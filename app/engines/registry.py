"""
Engine Registry — DEPRECATED, use strategy_registry.py instead.

This module re-exports from strategy_registry for backward compatibility.
All new code should import from app.engines.strategy_registry directly.
"""
from app.engines.strategy_registry import (
    STRATEGY_REGISTRY as _REG,
    get_strategy as get_engine,
    get_config_class,
    build_backtest_config,
)

# Backward compat: ENGINE_REGISTRY as a dict-like view
# Existing code that does ENGINE_REGISTRY["E1"]["intervals"] etc.
# will continue to work via get_engine().
ENGINE_REGISTRY = {
    name: {
        "name": meta.display_name,
        "intervals": meta.intervals,
        "backtesting_resolution": meta.backtesting_resolution,
        "controller_module": meta.controller_module,
        "config_class_name": meta.config_class_name,
        "exit_params": meta.exit_params,
        "trailing_stop": meta.trailing_stop,
        "direction": meta.direction,
        "blocked_pairs": meta.blocked_pairs,
    }
    for name, meta in _REG.items()
}
