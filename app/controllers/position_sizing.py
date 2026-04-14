"""
Strategy-aware position sizing models.

Each strategy type has fundamentally different risk/reward characteristics
and requires different position sizing logic:

- CARRY: Large positions, minimal leverage (1-3x). Edge IS funding income,
  not price movement. Size to maximize funding collection while keeping
  directional risk manageable. Wide emergency SL only.

- DIRECTIONAL: Moderate positions, moderate leverage (5-10x). Size scales
  with signal conviction. Higher conviction → larger position within risk
  budget.

- MEAN_REVERSION: Smaller positions, higher leverage (10-20x), many
  concurrent. Each trade is low-conviction individually but edge comes
  from volume. Small AUM share per position.

- VOLATILITY: Position size inversely proportional to current volatility.
  High vol → small positions. Low vol → larger positions. Used as overlay
  on other types.

IMPORTANT: This module is designed to be importable from HB controllers.
Only stdlib + pydantic + decimal imports. No app.* imports.
"""

from decimal import Decimal
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class StrategyType(str, Enum):
    CARRY = "carry"
    DIRECTIONAL = "directional"
    MEAN_REVERSION = "mean_reversion"
    VOLATILITY = "volatility"


class PositionSizingConfig(BaseModel):
    """
    Strategy-aware position sizing configuration.

    Embed this in your controller config to get dynamic sizing.
    """
    strategy_type: StrategyType = StrategyType.DIRECTIONAL

    # Capital allocation
    portfolio_capital: Decimal = Decimal("100000")  # Total portfolio AUM
    max_portfolio_exposure_pct: Decimal = Decimal("0.20")  # Max 20% of AUM in positions
    max_single_position_pct: Decimal = Decimal("0.05")  # Max 5% of AUM per position

    # Strategy-type defaults (overridden per strategy type)
    base_leverage: int = 10
    min_leverage: int = 1
    max_leverage: int = 20

    # Conviction scaling (for DIRECTIONAL)
    min_conviction_pct: Decimal = Decimal("0.3")  # Min 30% of max position at low conviction
    max_conviction_pct: Decimal = Decimal("1.0")  # 100% of max position at max conviction

    # Volatility adjustment
    vol_target_pct: Decimal = Decimal("0.02")  # 2% daily vol target
    vol_lookback_periods: int = 20  # Periods for vol calculation

    # Carry-specific
    carry_min_funding_rate: Decimal = Decimal("0.0001")  # Min funding rate to size up
    carry_position_floor_pct: Decimal = Decimal("0.03")  # Min 3% AUM per carry position

    class Config:
        use_enum_values = True


def compute_position_size(
    config: PositionSizingConfig,
    current_price: Decimal,
    signal_strength: float = 1.0,
    current_volatility: Optional[float] = None,
    current_portfolio_exposure: Decimal = Decimal("0"),
    num_active_positions: int = 0,
    funding_rate: Optional[float] = None,
) -> dict:
    """
    Compute position size based on strategy type and market conditions.

    Returns dict with:
        amount_quote: Decimal — position size in quote currency
        leverage: int — recommended leverage
        reason: str — human-readable sizing rationale
    """
    strategy = config.strategy_type
    capital = config.portfolio_capital
    max_exposure = capital * config.max_portfolio_exposure_pct
    max_single = capital * config.max_single_position_pct

    # Check portfolio-level exposure limit
    remaining_exposure = max_exposure - current_portfolio_exposure
    if remaining_exposure <= 0:
        return {
            "amount_quote": Decimal("0"),
            "leverage": config.base_leverage,
            "reason": f"Portfolio exposure limit reached: {current_portfolio_exposure}/{max_exposure}",
            "blocked": True,
        }

    if strategy == StrategyType.CARRY:
        return _size_carry(config, capital, max_single, remaining_exposure, funding_rate)
    elif strategy == StrategyType.DIRECTIONAL:
        return _size_directional(config, capital, max_single, remaining_exposure,
                                  signal_strength, current_volatility)
    elif strategy == StrategyType.MEAN_REVERSION:
        return _size_mean_reversion(config, capital, max_single, remaining_exposure,
                                     num_active_positions, current_volatility)
    elif strategy == StrategyType.VOLATILITY:
        return _size_volatility(config, capital, max_single, remaining_exposure,
                                 current_volatility)
    else:
        # Fallback: static sizing
        amount = min(max_single, remaining_exposure)
        return {
            "amount_quote": amount,
            "leverage": config.base_leverage,
            "reason": f"Static fallback: {amount}",
            "blocked": False,
        }


def _size_carry(
    config: PositionSizingConfig,
    capital: Decimal,
    max_single: Decimal,
    remaining_exposure: Decimal,
    funding_rate: Optional[float],
) -> dict:
    """
    Carry strategy: large positions, low leverage.
    Edge is funding income — size to maximize collection.
    Higher funding rate → larger position (within limits).
    """
    leverage = min(config.base_leverage, 3)  # Carry caps at 3x

    # Base size: large — carry wants big positions
    base_size = capital * config.carry_position_floor_pct  # 3% AUM minimum

    if funding_rate is not None:
        abs_rate = abs(funding_rate)
        # Scale up with funding rate: higher rate = more confident = bigger position
        # At min_funding_rate → floor. At 3x min → max_single.
        min_rate = float(config.carry_min_funding_rate)
        if abs_rate >= min_rate:
            scale = min(abs_rate / (min_rate * 3), 1.0)
            base_size = base_size + (max_single - base_size) * Decimal(str(scale))
        else:
            # Below min threshold — skip or use floor
            return {
                "amount_quote": Decimal("0"),
                "leverage": leverage,
                "reason": f"Funding rate {funding_rate:.6f} below threshold {min_rate:.6f}",
                "blocked": True,
            }

    amount = min(base_size, max_single, remaining_exposure)
    return {
        "amount_quote": amount,
        "leverage": leverage,
        "reason": f"Carry: {amount:.0f} USDT at {leverage}x (funding={funding_rate:.6f})" if funding_rate else f"Carry: {amount:.0f} USDT at {leverage}x",
        "blocked": False,
    }


def _size_directional(
    config: PositionSizingConfig,
    capital: Decimal,
    max_single: Decimal,
    remaining_exposure: Decimal,
    signal_strength: float,
    current_volatility: Optional[float],
) -> dict:
    """
    Directional strategy: moderate positions, moderate leverage.
    Size scales with signal conviction.
    Volatility-adjusted if vol data available.
    """
    leverage = config.base_leverage  # 5-10x typical

    # Conviction scaling: signal_strength 0-1 maps to min_conviction_pct..max_conviction_pct
    conviction = max(0.0, min(1.0, signal_strength))
    min_pct = float(config.min_conviction_pct)
    max_pct = float(config.max_conviction_pct)
    conviction_scale = Decimal(str(min_pct + conviction * (max_pct - min_pct)))

    base_size = max_single * conviction_scale

    # Volatility adjustment: reduce size in high vol
    if current_volatility is not None and current_volatility > 0:
        vol_target = float(config.vol_target_pct)
        vol_ratio = vol_target / current_volatility
        vol_scale = Decimal(str(max(0.2, min(2.0, vol_ratio))))  # Clamp 0.2x-2x
        base_size = base_size * vol_scale

    amount = min(base_size, max_single, remaining_exposure)
    reason = f"Directional: {amount:.0f} USDT at {leverage}x (conviction={conviction:.2f}"
    if current_volatility:
        reason += f", vol={current_volatility:.4f}"
    reason += ")"

    return {
        "amount_quote": amount,
        "leverage": leverage,
        "reason": reason,
        "blocked": False,
    }


def _size_mean_reversion(
    config: PositionSizingConfig,
    capital: Decimal,
    max_single: Decimal,
    remaining_exposure: Decimal,
    num_active_positions: int,
    current_volatility: Optional[float],
) -> dict:
    """
    Mean reversion: small positions, higher leverage, many concurrent.
    Edge from volume, not individual trade conviction.
    """
    leverage = min(config.max_leverage, 20)  # Can go up to 20x

    # Small per-position: spread across many concurrent trades
    # Max 10 concurrent positions, each 1/10 of max_single
    max_concurrent = 10
    per_position = max_single / Decimal(str(max_concurrent))

    # Reduce further if already have many active
    if num_active_positions >= max_concurrent:
        return {
            "amount_quote": Decimal("0"),
            "leverage": leverage,
            "reason": f"Max concurrent ({max_concurrent}) reached",
            "blocked": True,
        }

    amount = min(per_position, remaining_exposure)

    return {
        "amount_quote": amount,
        "leverage": leverage,
        "reason": f"MeanRev: {amount:.0f} USDT at {leverage}x ({num_active_positions}/{max_concurrent} active)",
        "blocked": False,
    }


def _size_volatility(
    config: PositionSizingConfig,
    capital: Decimal,
    max_single: Decimal,
    remaining_exposure: Decimal,
    current_volatility: Optional[float],
) -> dict:
    """
    Volatility strategy: size inversely proportional to vol.
    When vol is low, take bigger positions (expecting expansion).
    When vol is high, take smaller positions (risk management).
    """
    leverage = config.base_leverage

    if current_volatility is not None and current_volatility > 0:
        vol_target = float(config.vol_target_pct)
        vol_scale = Decimal(str(max(0.1, min(3.0, vol_target / current_volatility))))
        base_size = max_single * vol_scale
    else:
        base_size = max_single * Decimal("0.5")  # Conservative without vol data

    amount = min(base_size, max_single, remaining_exposure)

    return {
        "amount_quote": amount,
        "leverage": leverage,
        "reason": f"Vol-sized: {amount:.0f} USDT at {leverage}x (vol={current_volatility:.4f})" if current_volatility else f"Vol-sized: {amount:.0f} USDT (no vol data)",
        "blocked": False,
    }
