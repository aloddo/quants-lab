"""
Avellaneda-Stoikov utilities.

The AS reservation price computation now lives in InventoryManager.
This module provides helper functions for AS-related calculations
that other modules may need (e.g., optimal spread estimation).

References:
  - Avellaneda-Stoikov (2008): inventory pricing skeleton
  - GLFT (Gueant-Lehalle-Fernandez-Tapia 2013): empirical spread/intensity
"""
import math
from dataclasses import dataclass


@dataclass
class ASParams:
    """Avellaneda-Stoikov model parameters."""
    gamma: float          # risk aversion
    sigma_1s: float       # per-second volatility
    tau_s: float           # time horizon (seconds)
    kappa: float           # order arrival intensity


def optimal_spread_bps(params: ASParams) -> float:
    """Compute AS optimal spread in basis points.

    delta = gamma * sigma^2 * tau + (2/gamma) * ln(1 + gamma/kappa)

    Both terms are dimensionless (fractional). Returned in bps.
    """
    vol_term = params.gamma * (params.sigma_1s ** 2) * params.tau_s
    if params.kappa > 1e-10:
        arrival_term = (2.0 / params.gamma) * math.log(1 + params.gamma / params.kappa)
    else:
        arrival_term = 0.02  # 200bps default if no data

    return (vol_term + arrival_term) * 10000


def reservation_shift_bps(
    q_norm: float, gamma: float, sigma_1s: float, tau_s: float
) -> float:
    """Compute AS inventory-adjusted reservation shift in bps.

    shift = q_norm * gamma * sigma^2 * tau

    Positive shift = reservation below mid (when long).
    """
    return q_norm * gamma * (sigma_1s ** 2) * tau_s * 10000


def calibrate_gamma(target_shift_bps: float, sigma_1s: float, tau_s: float) -> float:
    """Calibrate gamma so that at q_norm=1, shift equals target_shift_bps.

    gamma = (target_shift_bps / 10000) / (sigma_1s^2 * tau_s)
    """
    sigma_sq_tau = sigma_1s ** 2 * tau_s
    if sigma_sq_tau <= 0:
        return 0.0
    return (target_shift_bps / 10000.0) / sigma_sq_tau


def estimate_kappa(
    trade_rate_per_sec: float, default: float = 500.0
) -> float:
    """Estimate kappa from observed trade rate.

    Higher trade rate = denser book = higher kappa = tighter spreads justified.
    Scale: 1 trade/sec -> kappa=1000, 0.01/sec -> kappa=100.
    """
    if trade_rate_per_sec <= 0:
        return default * 0.5

    return max(50.0, min(5000.0, trade_rate_per_sec * 1000.0))
