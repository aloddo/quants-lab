"""
Fair Value Engine — Three-tier anchor system (Spec Section 2).

Tier 1: Direct Bybit pair available (weight 0.70)
  anchored_mid = bybit_mid * exp(EMA_300s(log(HL_mid / bybit_mid)))
  fv = 0.70 * anchored_mid + 0.30 * microprice + clip(ofi_term, +/-0.75bps)

Tier 2: Sparse/new Bybit pair (weight 0.55)
  fv = 0.55 * anchored_mid + 0.45 * microprice

Tier 3: No direct Bybit pair (weight 0.35)
  synthetic_anchor via BTC/ETH/SOL beta regression
  fv = 0.35 * synthetic_anchor + 0.65 * microprice

Staleness rules:
  - Bybit stale > 500ms: halve anchor weight
  - Bybit stale > 2s:    anchor weight = 0

Data source: arb_hl_bybit_perp_snapshots collection (5s intervals, 30 pairs)
plus live Bybit WS feed for sub-second freshness.
"""
import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


class AnchorTier(Enum):
    """Which Bybit anchor tier a pair belongs to."""
    DIRECT = 1       # Bybit perp exists and is liquid
    SPARSE = 2       # Bybit perp exists but illiquid or new
    SYNTHETIC = 3    # No Bybit pair, use beta regression
    NONE = 4         # No usable anchor


@dataclass
class FairValueEstimate:
    """Multi-source fair value for a single pair."""
    coin: str
    timestamp: float
    tier: AnchorTier

    # HL-native pricing
    hl_mid: float
    hl_microprice: float
    hl_bid: float
    hl_ask: float

    # Bybit anchor
    bybit_mid: float
    bybit_age_ms: float       # milliseconds since last Bybit update
    anchor_weight: float       # effective weight after staleness discount

    # Derived
    fair_value: float
    anchored_mid: float        # the EMA-adjusted anchor before blending
    hl_premium_bps: float      # HL mid vs Bybit mid
    is_dislocated: bool        # |premium| > threshold


@dataclass
class BybitTickerState:
    """Cached Bybit ticker for one pair."""
    bid: float = 0.0
    ask: float = 0.0
    mid: float = 0.0
    last_update: float = 0.0


class FairValueEngine:
    """Compute fair value from three-tier anchor system.

    This engine does NOT own the Bybit WS connection. The orchestrator feeds
    Bybit ticks via update_bybit_ticker(). For startup, it can also pull
    from MongoDB arb_hl_bybit_perp_snapshots as bootstrap.
    """

    # Tier weights from spec
    TIER_WEIGHTS = {
        AnchorTier.DIRECT: 0.70,
        AnchorTier.SPARSE: 0.55,
        AnchorTier.SYNTHETIC: 0.35,
        AnchorTier.NONE: 0.0,
    }

    def __init__(
        self,
        bybit_pairs: dict[str, str],   # HL coin -> Bybit symbol (e.g. "ORDI" -> "ORDIUSDT")
        ema_halflife_s: float = 300.0,  # 300s EMA for basis tracking
        dislocation_threshold_bps: float = 15.0,
        ofi_clip_bps: float = 0.75,
    ):
        self.bybit_pairs = bybit_pairs
        self.dislocation_threshold_bps = dislocation_threshold_bps
        self.ofi_clip_bps = ofi_clip_bps

        # EMA decay factor: alpha = 1 - exp(-dt / halflife)
        self._ema_halflife_s = ema_halflife_s

        # Per-coin state
        self._bybit_tickers: dict[str, BybitTickerState] = {}
        self._basis_ema: dict[str, float] = {}          # EMA of log(HL_mid / bybit_mid)
        self._basis_last_update: dict[str, float] = {}
        self._tier_cache: dict[str, AnchorTier] = {}

        # Synthetic anchor: beta coefficients from rolling regression
        self._betas: dict[str, dict[str, float]] = {}   # coin -> {BTC: b1, ETH: b2, SOL: b3}
        self._beta_r2: dict[str, float] = {}
        self._beta_last_fit: dict[str, float] = {}
        self._return_history: dict[str, deque] = {}      # coin -> deque of (ts, log_return)

        # OFI accumulator (order flow imbalance)
        self._ofi_accum: dict[str, float] = {}

    def set_tier(self, coin: str, tier: AnchorTier) -> None:
        """Set the anchor tier for a coin. Called by pair screener."""
        self._tier_cache[coin] = tier

    def get_tier(self, coin: str) -> AnchorTier:
        """Get the anchor tier for a coin."""
        return self._tier_cache.get(coin, AnchorTier.NONE)

    def update_bybit_ticker(self, coin: str, bid: float, ask: float) -> None:
        """Feed a Bybit ticker update. Called from Bybit WS handler."""
        if bid <= 0 or ask <= 0:
            return
        self._bybit_tickers[coin] = BybitTickerState(
            bid=bid, ask=ask, mid=(bid + ask) / 2.0, last_update=time.time(),
        )

    def update_ofi(self, coin: str, ofi_bps: float) -> None:
        """Feed order flow imbalance term (bps). Called from signal processing."""
        clipped = max(-self.ofi_clip_bps, min(self.ofi_clip_bps, ofi_bps))
        self._ofi_accum[coin] = clipped

    def compute(
        self,
        coin: str,
        hl_bid: float,
        hl_ask: float,
        bid_qty_top1: float,
        ask_qty_top1: float,
    ) -> Optional[FairValueEstimate]:
        """Compute fair value for a single coin given current HL L2 top of book.

        Args:
            coin: HL coin name (e.g. "ORDI")
            hl_bid: best bid price on HL
            hl_ask: best ask price on HL
            bid_qty_top1: size at best bid (in base units)
            ask_qty_top1: size at best ask (in base units)

        Returns:
            FairValueEstimate or None if insufficient data.
        """
        if hl_bid <= 0 or hl_ask <= 0:
            return None

        now = time.time()
        hl_mid = (hl_bid + hl_ask) / 2.0

        # Microprice: Stoikov (2017)
        total_qty = bid_qty_top1 + ask_qty_top1
        if total_qty > 0:
            imbalance = bid_qty_top1 / total_qty  # I
            microprice = hl_ask * imbalance + hl_bid * (1.0 - imbalance)
        else:
            microprice = hl_mid

        # Determine tier
        tier = self._tier_cache.get(coin, AnchorTier.NONE)

        # Get Bybit state (only relevant for DIRECT/SPARSE tiers)
        bb = self._bybit_tickers.get(coin)
        bybit_mid = bb.mid if bb else 0.0
        bybit_age_ms = (now - bb.last_update) * 1000 if bb and bb.last_update > 0 else 999999.0

        # Bug #1 fix: Compute anchor weight based on tier.
        # For SYNTHETIC tier, skip Bybit staleness logic entirely —
        # synthetic anchor uses BTC/ETH/SOL Bybit mids, not a direct ticker.
        base_weight = self.TIER_WEIGHTS.get(tier, 0.0)
        if tier == AnchorTier.SYNTHETIC:
            # Synthetic tier: anchor weight is independent of direct Bybit ticker
            anchor_weight = base_weight
        elif tier in (AnchorTier.DIRECT, AnchorTier.SPARSE):
            # Direct/Sparse: apply staleness discount on the direct Bybit ticker
            if bybit_age_ms > 2000:
                anchor_weight = 0.0
            elif bybit_age_ms > 500:
                anchor_weight = base_weight * 0.5
            else:
                anchor_weight = base_weight
        else:
            anchor_weight = 0.0

        # Compute anchored_mid with EMA basis tracking
        anchored_mid = 0.0
        if tier in (AnchorTier.DIRECT, AnchorTier.SPARSE) and bybit_mid > 0 and anchor_weight > 0:
            anchored_mid = self._compute_anchored_mid(coin, hl_mid, bybit_mid, now)
        elif tier == AnchorTier.SYNTHETIC:
            anchored_mid = self._compute_synthetic_anchor(coin, hl_mid, now)
            if anchored_mid <= 0:
                anchor_weight = 0.0

        # Blend fair value
        if anchor_weight > 0 and anchored_mid > 0:
            fv = anchor_weight * anchored_mid + (1.0 - anchor_weight) * microprice
            # Add OFI term for Tier 1 only
            if tier == AnchorTier.DIRECT:
                ofi_term = self._ofi_accum.get(coin, 0.0)
                fv *= (1.0 + ofi_term / 10000.0)
        else:
            fv = microprice

        # HL premium
        hl_premium_bps = 0.0
        if bybit_mid > 0:
            hl_premium_bps = (hl_mid - bybit_mid) / bybit_mid * 10000
        is_dislocated = abs(hl_premium_bps) > self.dislocation_threshold_bps

        return FairValueEstimate(
            coin=coin,
            timestamp=now,
            tier=tier,
            hl_mid=hl_mid,
            hl_microprice=microprice,
            hl_bid=hl_bid,
            hl_ask=hl_ask,
            bybit_mid=bybit_mid,
            bybit_age_ms=bybit_age_ms,
            anchor_weight=anchor_weight,
            fair_value=fv,
            anchored_mid=anchored_mid,
            hl_premium_bps=hl_premium_bps,
            is_dislocated=is_dislocated,
        )

    def _compute_anchored_mid(
        self, coin: str, hl_mid: float, bybit_mid: float, now: float
    ) -> float:
        """Compute anchored_mid = bybit_mid * exp(EMA_300s(log(HL_mid / bybit_mid))).

        The EMA tracks the persistent basis between HL and Bybit. This
        compensates for systematic HL premium/discount while still anchoring
        to Bybit's more liquid price.
        """
        log_basis = math.log(hl_mid / bybit_mid)

        prev_ema = self._basis_ema.get(coin)
        prev_time = self._basis_last_update.get(coin)

        if prev_ema is None or prev_time is None:
            # Initialize
            self._basis_ema[coin] = log_basis
            self._basis_last_update[coin] = now
            return bybit_mid * math.exp(log_basis)

        dt = now - prev_time
        if dt <= 0:
            return bybit_mid * math.exp(prev_ema)

        # EMA update: alpha = 1 - exp(-dt / halflife)
        alpha = 1.0 - math.exp(-dt / self._ema_halflife_s)
        new_ema = alpha * log_basis + (1.0 - alpha) * prev_ema

        self._basis_ema[coin] = new_ema
        self._basis_last_update[coin] = now

        return bybit_mid * math.exp(new_ema)

    def _compute_synthetic_anchor(
        self, coin: str, hl_mid: float, now: float
    ) -> float:
        """Tier 3: synthetic anchor via BTC/ETH/SOL beta regression.

        synthetic = last_hl_mid * exp(beta_btc*dBTC + beta_eth*dETH + beta_sol*dSOL)
        Only used if R^2 >= 0.25.
        """
        betas = self._betas.get(coin)
        r2 = self._beta_r2.get(coin, 0.0)

        if not betas or r2 < 0.25:
            return 0.0

        # Bug #1 fix: Get recent returns for BTC, ETH, SOL from their
        # Bybit mids directly (log return since last fit), NOT from _basis_ema
        # which tracks HL-vs-Bybit basis — wrong signal for synthetic anchor.
        fit_time = self._beta_last_fit.get(coin, 0.0)
        total_return = 0.0
        for ref_coin, beta in betas.items():
            ref_bb = self._bybit_tickers.get(ref_coin)
            if ref_bb and ref_bb.mid > 0:
                # Compute log return of the reference coin's Bybit mid since last beta fit
                # Use return history if available, otherwise use a small window via EMA
                ref_return = 0.0
                # Check if we have a stored mid at fit time
                fit_mid_key = f"_fit_mid_{ref_coin}"
                fit_mid = getattr(self, '_fit_mids', {}).get(ref_coin, 0.0)
                if fit_mid > 0:
                    ref_return = math.log(ref_bb.mid / fit_mid)
                total_return += beta * ref_return

        return hl_mid * math.exp(total_return)

    def fit_synthetic_betas(
        self,
        coin: str,
        coin_returns: list[float],
        btc_returns: list[float],
        eth_returns: list[float],
        sol_returns: list[float],
    ) -> None:
        """Fit rolling OLS for synthetic anchor. Called periodically by screener.

        Uses 6h of 1s returns.
        """
        n = len(coin_returns)
        if n < 100:
            return

        y = np.array(coin_returns[:n])
        X = np.column_stack([
            np.array(btc_returns[:n]),
            np.array(eth_returns[:n]),
            np.array(sol_returns[:n]),
            np.ones(n),
        ])

        try:
            # OLS: beta = (X'X)^-1 X'y
            XtX = X.T @ X
            Xty = X.T @ y
            beta = np.linalg.solve(XtX, Xty)

            y_hat = X @ beta
            ss_res = np.sum((y - y_hat) ** 2)
            ss_tot = np.sum((y - y.mean()) ** 2)
            r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0

            self._betas[coin] = {"BTC": beta[0], "ETH": beta[1], "SOL": beta[2]}
            self._beta_r2[coin] = r2
            self._beta_last_fit[coin] = time.time()

            # Bug #1 fix: Store reference coin Bybit mids at fit time
            # so _compute_synthetic_anchor can compute returns since fit.
            if not hasattr(self, '_fit_mids'):
                self._fit_mids: dict[str, float] = {}
            for ref_coin in ("BTC", "ETH", "SOL"):
                ref_bb = self._bybit_tickers.get(ref_coin)
                if ref_bb and ref_bb.mid > 0:
                    self._fit_mids[ref_coin] = ref_bb.mid

            logger.info(
                f"{coin} synthetic betas: BTC={beta[0]:.3f} ETH={beta[1]:.3f} "
                f"SOL={beta[2]:.3f} R2={r2:.3f}"
            )
        except np.linalg.LinAlgError:
            logger.warning(f"{coin} synthetic beta fit failed (singular matrix)")
