"""
Portfolio risk manager — pure sizing and admission functions.

No I/O. Used by:
- ThinExecutor (signal admission + position sizing)
- RiskStateTask (heat computation)
- cli.py deploy (pre-deploy check)

Design decisions (from CEO + Eng dual-voice review, Apr 17 2026):
- Heat uses conservative estimate: qty * (sl_distance + 2*fee_rate + slippage)
- Conviction scales risk budget per trade; portfolio heat cap is the hard envelope
- Staleness > 5min triggers circuit breaker (safe default, not stale default)
"""

import math
import logging
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Default risk parameters
DEFAULT_RISK_CONFIG = {
    "risk_per_trade_pct": 0.01,         # 1% equity at risk per trade
    "max_portfolio_heat_pct": 0.06,      # 6% max total risk across ALL positions
    "max_strategy_heat_pct": 0.03,       # 3% max risk from any single strategy
    "max_pair_heat_pct": 0.02,           # 2% max risk on any pair across strategies
    "max_leverage": 5,                   # Hard cap
    "min_equity_pct": 0.80,              # Circuit breaker if equity < 80% of initial
    "max_concurrent_positions": 15,
    "enabled": True,
}


def compute_position_size(
    equity: float,
    sl_distance_pct: float,
    conviction_mult: float,
    risk_per_trade_pct: float,
    price: float,
    max_leverage: int = 5,
    fee_rate: float = 0.000375,
    slippage_bps: float = 5.0,
) -> dict:
    """Risk-based position sizing with conservative heat accounting.

    Args:
        equity: Current account equity in USD.
        sl_distance_pct: Stop loss distance as fraction (e.g., 0.015 = 1.5%).
        conviction_mult: Signal conviction multiplier (1.0 = base, 2.0 = max).
        risk_per_trade_pct: Max equity at risk per trade (e.g., 0.01 = 1%).
        price: Current asset price.
        max_leverage: Hard cap on leverage.
        fee_rate: One-way fee rate (default: maker+taker blend).
        slippage_bps: Expected slippage in basis points.

    Returns:
        Dict with notional, amount, leverage, risk_amount, heat,
        margin_needed, effective_sl.
    """
    if equity <= 0 or sl_distance_pct <= 0 or price <= 0:
        return {
            "notional": 0, "amount": 0, "leverage": 1,
            "risk_amount": 0, "heat": 0,
            "margin_needed": 0, "effective_sl": 0,
        }

    # Conviction scales risk budget within portfolio heat cap
    conviction_mult = max(0.5, min(2.0, conviction_mult))
    risk_budget = equity * risk_per_trade_pct * conviction_mult

    # Conservative: include round-trip fees + slippage in loss estimate
    slippage_pct = slippage_bps / 10000.0
    effective_sl = sl_distance_pct + 2 * fee_rate + slippage_pct

    notional = risk_budget / effective_sl
    amount = notional / price

    # Leverage: financing only, derived from notional vs available margin
    leverage = min(max_leverage, max(1, math.ceil(notional / (equity * 0.9))))
    margin_needed = notional / leverage

    # Heat: conservative worst-case loss (used for portfolio tracking)
    heat = amount * price * effective_sl

    return {
        "notional": round(notional, 2),
        "amount": amount,
        "leverage": leverage,
        "risk_amount": round(risk_budget, 2),
        "heat": round(heat, 2),
        "margin_needed": round(margin_needed, 2),
        "effective_sl": round(effective_sl, 6),
    }


def check_admission(
    risk_state: dict,
    engine: str,
    pair: str,
    proposed_heat: float,
) -> tuple:
    """Check if a new position is allowed under portfolio limits.

    Args:
        risk_state: Current portfolio_risk_state document from MongoDB.
        engine: Strategy engine name (e.g., "X5").
        pair: Trading pair (e.g., "BTC-USDT").
        proposed_heat: Conservative heat for the proposed position.

    Returns:
        Tuple of (allowed: bool, reason: str).
    """
    if not risk_state:
        return False, "no risk state available"

    cfg = risk_state.get("risk_config", DEFAULT_RISK_CONFIG)

    if not cfg.get("enabled", True):
        return True, "risk management disabled"

    # Circuit breaker
    cb = risk_state.get("circuit_breaker", {})
    if cb.get("triggered", False):
        return False, f"circuit breaker: {cb.get('reason', 'unknown')}"

    equity = risk_state.get("equity", 0)
    if equity <= 0:
        return False, "equity is zero or negative"

    heat = risk_state.get("portfolio_heat", {})

    # Include active reservations in current heat
    reservations = risk_state.get("reservations", {})
    reserved_heat = sum(r.get("heat", 0) for r in reservations.values())
    total_heat_pct = heat.get("total_risk_pct", 0) + reserved_heat / equity

    # Portfolio heat cap
    if total_heat_pct + (proposed_heat / equity) > cfg.get("max_portfolio_heat_pct", 0.06):
        return False, (
            f"portfolio heat {total_heat_pct:.1%} + proposed {proposed_heat/equity:.1%} "
            f"would exceed {cfg['max_portfolio_heat_pct']:.0%}"
        )

    # Strategy heat cap
    by_strategy = heat.get("by_strategy", {})
    strat_info = by_strategy.get(engine, {})
    strat_heat_pct = strat_info.get("risk_pct", 0)
    if strat_heat_pct + (proposed_heat / equity) > cfg.get("max_strategy_heat_pct", 0.03):
        return False, (
            f"strategy {engine} heat {strat_heat_pct:.1%} + proposed "
            f"would exceed {cfg['max_strategy_heat_pct']:.0%}"
        )

    # Pair heat cap (cross-strategy)
    by_pair = heat.get("by_pair", {})
    pair_info = by_pair.get(pair, {})
    pair_heat_pct = pair_info.get("risk_pct", 0)
    if pair_heat_pct + (proposed_heat / equity) > cfg.get("max_pair_heat_pct", 0.02):
        return False, (
            f"pair {pair} heat {pair_heat_pct:.1%} + proposed "
            f"would exceed {cfg['max_pair_heat_pct']:.0%}"
        )

    # Position count
    position_count = heat.get("position_count", 0)
    if position_count >= cfg.get("max_concurrent_positions", 15):
        return False, f"position count {position_count} at limit"

    return True, "ok"


def compute_risk_state(
    equity: float,
    available_margin: float,
    initial_capital: float,
    positions: list,
    risk_config: dict,
    reservations: dict = None,
) -> dict:
    """Compute full portfolio risk state document for MongoDB.

    Args:
        equity: Current account equity.
        available_margin: Available margin on exchange.
        initial_capital: Initial capital (for circuit breaker).
        positions: List of position dicts from BybitExchangeClient.
        risk_config: Risk configuration parameters.
        reservations: Current active reservations (preserved from previous state).

    Returns:
        Complete portfolio_risk_state document ready for MongoDB upsert.
    """
    now = datetime.now(timezone.utc)

    # Circuit breaker check
    equity_pct = equity / initial_capital if initial_capital > 0 else 1.0
    circuit_breaker = {"triggered": False, "reason": None}
    if equity_pct < risk_config.get("min_equity_pct", 0.80):
        circuit_breaker = {
            "triggered": True,
            "reason": f"equity {equity_pct:.1%} below {risk_config['min_equity_pct']:.0%} threshold",
            "triggered_at": now,
        }

    # Compute per-position heat (using exchange SL if available, else estimate)
    by_strategy = {}
    by_pair = {}
    total_heat = 0.0

    for pos in positions:
        pair = pos.get("pair", "")
        engine = pos.get("engine", "untracked")
        entry = pos.get("entry_price", 0)
        mark = pos.get("mark_price", 0)
        qty = pos.get("qty", 0)
        side = pos.get("side", "")

        # Estimate heat: use 2% SL as default if no SL data available
        sl_distance = 0.02
        heat = qty * mark * (sl_distance + 2 * 0.000375 + 0.0005)
        total_heat += heat

        heat_pct = heat / equity if equity > 0 else 0

        # Strategy aggregation
        if engine not in by_strategy:
            by_strategy[engine] = {"risk_pct": 0, "positions": 0, "pairs": []}
        by_strategy[engine]["risk_pct"] += heat_pct
        by_strategy[engine]["positions"] += 1
        if pair not in by_strategy[engine]["pairs"]:
            by_strategy[engine]["pairs"].append(pair)

        # Pair aggregation
        if pair not in by_pair:
            by_pair[pair] = {"risk_pct": 0, "strategies": []}
        by_pair[pair]["risk_pct"] += heat_pct
        if engine not in by_pair[pair]["strategies"]:
            by_pair[pair]["strategies"].append(engine)

    total_risk_pct = total_heat / equity if equity > 0 else 0

    return {
        "_id": "current",
        "updated_at": now,
        "equity": equity,
        "available_margin": available_margin,
        "initial_capital": initial_capital,
        "equity_pct_of_initial": equity_pct,
        "risk_config": risk_config,
        "portfolio_heat": {
            "total_risk_pct": round(total_risk_pct, 6),
            "total_heat": round(total_heat, 2),
            "position_count": len(positions),
            "by_strategy": by_strategy,
            "by_pair": by_pair,
        },
        "positions": positions,
        "circuit_breaker": circuit_breaker,
        "reservations": reservations or {},
    }
