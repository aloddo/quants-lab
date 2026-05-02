"""
Avellaneda-Stoikov Optimal Quoter for Hyperliquid — V2 (Codex fixes applied).

Fixes from adversarial review:
- F11: Uses per-second volatility (NOT annualized) in AS formula
- F12: Funding bias sign corrected (positive funding → bias SHORT)
- Units are consistent: sigma = per-second stdev, tau = seconds
"""
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ASConfig:
    """Avellaneda-Stoikov configuration, sized for $50 capital."""
    # Risk aversion (gamma) — higher = wider spreads, less risk
    gamma: float = 0.3

    # Horizon in seconds (1 hour = HL funding cycle)
    horizon_seconds: float = 3600.0

    # Spread bounds (basis points)
    min_spread_bps: float = 30.0   # floor: must exceed 2*maker_fee + adverse_selection
    max_spread_bps: float = 200.0  # ceiling

    # Volatility estimation
    vol_window: int = 60  # observations for vol estimate
    vol_ema_alpha: float = 0.05

    # Order arrival intensity (kappa) — AS book density parameter
    # NOT trades/second. Calibrated so arrival_term produces ~30-50bps spread:
    # 2/gamma * ln(1 + gamma/kappa) ≈ target_spread_return
    # For gamma=0.3, kappa=500 → ~40bps arrival term
    kappa_default: float = 500.0
    kappa_window: int = 300  # seconds to observe fill rate for adaptive kappa

    # Inventory — SIZED FOR $50 CAPITAL
    max_inventory_usd: float = 10.0   # hard limit per pair (20% of $50)
    order_size_usd: float = 11.0      # per order (HL minimum notional ~$10)

    # Funding (updated externally each hour)
    funding_rate_hourly: float = 0.0  # positive = longs pay shorts

    # Maker fee (bps)
    maker_fee_bps: float = 1.44  # MetaMask referral tier

    # Fill management
    fill_cooldown_seconds: float = 3.0


@dataclass
class QuoteResult:
    """Optimal bid/ask output."""
    bid_price: float
    ask_price: float
    bid_size: float  # in base asset units
    ask_size: float
    reservation_price: float
    spread_bps: float
    inventory_skew_bps: float
    sigma_per_second: float
    should_quote_bid: bool  # False if at max long inventory
    should_quote_ask: bool  # False if at max short inventory


class AvellanedaQuoter:
    """Compute optimal bid/ask using AS model with correct units."""

    def __init__(self, config: ASConfig):
        self.config = config
        self._mid_history: deque = deque(maxlen=config.vol_window)
        self._trade_timestamps: deque = deque(maxlen=config.kappa_window)
        self._last_fill_time: float = 0.0
        self._sigma_per_sec: float = 0.0

    def update_mid(self, mid_price: float, timestamp: Optional[float] = None):
        """Feed new mid-price. Call every tick."""
        self._mid_history.append((timestamp or time.time(), mid_price))

    def record_trade(self, timestamp: Optional[float] = None):
        """Record a market trade for kappa estimation."""
        self._trade_timestamps.append(timestamp or time.time())

    def record_fill(self):
        """Record that one of our orders filled."""
        self._last_fill_time = time.time()

    def in_cooldown(self) -> bool:
        """Check if we're in post-fill cooldown."""
        return (time.time() - self._last_fill_time) < self.config.fill_cooldown_seconds

    def compute_quotes(
        self,
        mid_price: float,
        inventory_usd: float,
        funding_rate_hourly: float = 0.0,
        best_bid: float = 0.0,
        best_ask: float = 0.0,
    ) -> Optional[QuoteResult]:
        """Compute optimal quotes using AS model.

        All internal math uses PER-SECOND volatility (not annualized).
        tau is in seconds. sigma is stdev of returns per second.
        """
        cfg = self.config

        if self.in_cooldown():
            return None

        # Estimate per-second volatility
        sigma_s = self._estimate_vol_per_second()
        if sigma_s <= 0:
            sigma_s = 1e-6  # tiny default (will produce max_spread)
        self._sigma_per_sec = sigma_s

        # Estimate order arrival rate
        kappa = self._estimate_kappa()

        # Time to horizon (seconds)
        tau = cfg.horizon_seconds

        # Inventory in base asset units
        q = inventory_usd / mid_price if mid_price > 0 else 0.0

        # --- RESERVATION PRICE (F1 FIX: work in RETURN space, convert at end) ---
        # AS formula in return space: r_pct = -q * gamma * sigma_s^2 * tau
        # This gives the fractional shift from mid (dimensionless)
        inventory_shift_pct = q * cfg.gamma * (sigma_s ** 2) * tau

        # Funding bias: positive funding → bias SHORT → shift reservation DOWN
        # funding_rate_hourly is in fraction (e.g., 0.0001 = 0.01%/hr)
        # Convert to per-second and apply over tau
        funding_shift_pct = (funding_rate_hourly / 3600.0) * tau * 0.5  # half-Kelly

        # Reservation price (in actual price units)
        reservation_price = mid_price * (1.0 - inventory_shift_pct - funding_shift_pct)

        # --- OPTIMAL SPREAD (F1 FIX: compute in return space, then convert) ---
        # delta_pct = gamma * sigma_s^2 * tau + (2/gamma) * ln(1 + gamma/kappa)
        # Both terms are dimensionless (fractional spread)
        vol_term = cfg.gamma * (sigma_s ** 2) * tau
        arrival_term = (2.0 / cfg.gamma) * math.log(1 + cfg.gamma / kappa) if kappa > 1e-10 else cfg.max_spread_bps / 10000.0

        optimal_spread_pct = vol_term + arrival_term

        # Convert to bps
        spread_bps = optimal_spread_pct * 10000.0

        # Enforce minimum (must cover 2x maker fee + buffer for adverse selection)
        min_required = 2 * cfg.maker_fee_bps + 5.0  # 2 * 1.44 + 5 = 7.88bps minimum
        spread_bps = max(cfg.min_spread_bps, max(min_required, spread_bps))
        spread_bps = min(spread_bps, cfg.max_spread_bps)

        # Convert to price
        half_spread = mid_price * (spread_bps / 2.0 / 10000.0)

        # Final quotes
        bid_price = reservation_price - half_spread
        ask_price = reservation_price + half_spread

        # --- POST-ONLY ENFORCEMENT (F13) ---
        # Ensure bid < best_ask and ask > best_bid to avoid crossing
        if best_ask > 0 and bid_price >= best_ask:
            bid_price = best_ask * 0.9999  # pull back
        if best_bid > 0 and ask_price <= best_bid:
            ask_price = best_bid * 1.0001  # pull back

        # --- INVENTORY-AWARE SIZING (F3) ---
        base_size_usd = cfg.order_size_usd
        inventory_ratio = abs(inventory_usd) / cfg.max_inventory_usd if cfg.max_inventory_usd > 0 else 0

        should_bid = True
        should_ask = True

        if inventory_usd >= cfg.max_inventory_usd:
            should_bid = False  # at max long, don't buy more
            bid_size = 0.0
            ask_size = base_size_usd * 1.5  # aggressive sell
        elif inventory_usd <= -cfg.max_inventory_usd:
            should_ask = False  # at max short, don't sell more
            ask_size = 0.0
            bid_size = base_size_usd * 1.5  # aggressive buy
        else:
            # Scale down the side that adds to inventory
            if inventory_usd > 0:
                bid_size = base_size_usd * max(0.2, 1.0 - inventory_ratio)
                ask_size = base_size_usd * min(1.5, 1.0 + inventory_ratio * 0.3)
            elif inventory_usd < 0:
                ask_size = base_size_usd * max(0.2, 1.0 - inventory_ratio)
                bid_size = base_size_usd * min(1.5, 1.0 + inventory_ratio * 0.3)
            else:
                bid_size = base_size_usd
                ask_size = base_size_usd

        skew_bps = (mid_price - reservation_price) / mid_price * 10000 if mid_price > 0 else 0

        return QuoteResult(
            bid_price=bid_price,
            ask_price=ask_price,
            bid_size=bid_size / mid_price if mid_price > 0 else 0,
            ask_size=ask_size / mid_price if mid_price > 0 else 0,
            reservation_price=reservation_price,
            spread_bps=spread_bps,
            inventory_skew_bps=skew_bps,
            sigma_per_second=sigma_s,
            should_quote_bid=should_bid,
            should_quote_ask=should_ask,
        )

    def _estimate_vol_per_second(self) -> float:
        """Estimate per-second standard deviation of returns.

        F11 FIX: Returns sigma in per-second units. NOT annualized.
        AS formula: sigma^2 * tau where both are in the same time unit (seconds).
        """
        if len(self._mid_history) < 5:
            # Default: assume BTC-like vol (~50% annual = ~1.5e-5 per second)
            return 1.5e-5

        prices = [p for _, p in self._mid_history]
        times = [t for t, _ in self._mid_history]

        returns = np.diff(np.log(prices))
        dts = np.diff(times)

        if len(returns) == 0 or len(dts) == 0:
            return 1.5e-5

        # Filter out zero/negative dt (duplicates)
        valid = dts > 0
        if valid.sum() < 3:
            return 1.5e-5

        returns = returns[valid]
        dts = dts[valid]

        # Per-second variance: var(return) / dt for each observation
        # Then average
        per_sec_vars = (returns ** 2) / dts
        sigma_per_sec = math.sqrt(np.mean(per_sec_vars))

        # EMA smooth
        if self._sigma_per_sec > 0:
            alpha = self.config.vol_ema_alpha
            sigma_per_sec = alpha * sigma_per_sec + (1 - alpha) * self._sigma_per_sec

        return sigma_per_sec

    def _estimate_kappa(self) -> float:
        """Estimate kappa (AS book density parameter).

        In practice: kappa is calibrated from observed fill rates at different
        spread levels. Higher kappa = denser book = tighter spreads needed for fills.

        Adaptive: if we're getting fills, kappa is high (book is active).
        If no fills for a while, kappa drops (book is thin, widen spread).
        """
        if len(self._trade_timestamps) < 5:
            return self.config.kappa_default

        now = time.time()
        window_start = now - self.config.kappa_window
        recent = [t for t in self._trade_timestamps if t >= window_start]

        if len(recent) < 3:
            # No recent activity → thin book → lower kappa → wider spread
            return self.config.kappa_default * 0.5

        # More trades = more active book = higher kappa = tighter quotes justified
        trades_per_sec = len(recent) / (now - recent[0]) if (now - recent[0]) > 0 else 0
        # Scale: 1 trade/sec → kappa=1000 (tight), 0.01 trade/sec → kappa=100 (wide)
        adaptive_kappa = max(100.0, min(2000.0, trades_per_sec * 1000.0))

        return adaptive_kappa
