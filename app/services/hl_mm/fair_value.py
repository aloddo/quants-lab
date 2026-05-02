"""
Fair Value Engine — External reference pricing from Bybit.

Codex Round 3 recommendation: "Without external fair value, you are pricing
off the venue you are being picked off on."

Uses Bybit perpetual mid as the primary fair value estimate for HL-listed pairs.
When HL mid diverges from Bybit mid, that's either:
  - An arb opportunity (HL is stale) → widen quotes / pause
  - A lead signal (HL leads Bybit) → lean into the HL direction

Also provides microprice (size-weighted mid) for more precise valuation.
"""
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

import requests

logger = logging.getLogger(__name__)

# Bybit public API (no auth needed for market data)
BYBIT_API = "https://api.bybit.com"


@dataclass
class FairValueEstimate:
    """Multi-source fair value for a pair."""
    coin: str
    timestamp: float

    # HL-native pricing
    hl_mid: float
    hl_microprice: float  # size-weighted mid from L2

    # External reference (Bybit)
    bybit_mid: float
    bybit_fresh: bool  # False if Bybit data is stale (>2s)

    # Derived
    fair_value: float  # blended estimate
    hl_premium_bps: float  # how much HL is above/below Bybit (positive = HL premium)
    is_dislocated: bool  # True if |premium| > threshold → pause quoting or arb


class FairValueEngine:
    """Compute fair value from HL L2 + external Bybit reference."""

    def __init__(
        self,
        coins: list[str],
        dislocation_threshold_bps: float = 15.0,  # pause if HL vs Bybit diverges > this
        bybit_weight: float = 0.6,  # weight for Bybit in blended fair value (0-1)
        cache_ttl_ms: float = 1500.0,  # Bybit cache validity
    ):
        self.coins = coins
        self.dislocation_threshold_bps = dislocation_threshold_bps
        self.bybit_weight = bybit_weight
        self.cache_ttl_ms = cache_ttl_ms

        # Bybit symbol mapping (HL uses "APE", Bybit uses "APEUSDT")
        self._bybit_symbols = {coin: f"{coin}USDT" for coin in coins}
        self._bybit_mids: dict[str, float] = {}
        self._bybit_last_fetch: float = 0.0
        self._estimates: dict[str, FairValueEstimate] = {}

    def update(self, hl_signals: dict) -> dict[str, FairValueEstimate]:
        """Compute fair value for all coins. Call every tick.

        Args:
            hl_signals: dict of ImbalanceSignal from signal_engine (has HL L2 data)
        """
        # Fetch Bybit mids (batched, one API call for all symbols)
        self._refresh_bybit()

        for coin in self.coins:
            sig = hl_signals.get(coin)
            if not sig:
                continue

            estimate = self._compute_fair_value(coin, sig)
            if estimate:
                self._estimates[coin] = estimate

        return self._estimates

    def get_estimate(self, coin: str) -> Optional[FairValueEstimate]:
        return self._estimates.get(coin)

    def _compute_fair_value(self, coin: str, sig) -> Optional[FairValueEstimate]:
        """Blend HL and Bybit data into a fair value estimate."""
        hl_mid = sig.mid_price
        if hl_mid <= 0:
            return None

        # MICROPRICE (Stoikov 2017): the core signal IS the fair value estimate
        # microprice = ask * I + bid * (1-I) where I = bid_qty / (bid_qty + ask_qty)
        # When bids are heavy (I > 0.5), true value is closer to the ask
        # This is the CONTRARIAN insight: heavy bids → value is UP → quote higher
        # The imbalance signal is NOT used for one-sided skewing — it's IN the microprice
        hl_microprice = sig.best_ask * sig.imbalance + sig.best_bid * (1.0 - sig.imbalance)

        # Bybit reference
        bybit_mid = self._bybit_mids.get(coin, 0.0)
        bybit_fresh = bybit_mid > 0 and (time.time() - self._bybit_last_fetch) * 1000 < self.cache_ttl_ms

        # Blended fair value
        if bybit_fresh and bybit_mid > 0:
            # Weight toward Bybit (more liquid, less manipulable)
            fair_value = bybit_mid * self.bybit_weight + hl_microprice * (1 - self.bybit_weight)
        else:
            # Bybit unavailable — use HL microprice only
            fair_value = hl_microprice

        # Premium: how much HL is above Bybit
        hl_premium_bps = 0.0
        is_dislocated = False
        if bybit_mid > 0:
            hl_premium_bps = (hl_mid - bybit_mid) / bybit_mid * 10000
            is_dislocated = abs(hl_premium_bps) > self.dislocation_threshold_bps

        return FairValueEstimate(
            coin=coin,
            timestamp=time.time(),
            hl_mid=hl_mid,
            hl_microprice=hl_microprice,
            bybit_mid=bybit_mid,
            bybit_fresh=bybit_fresh,
            fair_value=fair_value,
            hl_premium_bps=hl_premium_bps,
            is_dislocated=is_dislocated,
        )

    def _refresh_bybit(self):
        """Fetch Bybit tickers (batched). Rate limit: 1 call per 1.5s."""
        now = time.time()
        if now - self._bybit_last_fetch < 1.5:
            return

        try:
            # Bybit V5 tickers endpoint (all symbols in one call)
            resp = requests.get(
                f"{BYBIT_API}/v5/market/tickers",
                params={"category": "linear"},
                timeout=3,
            )
            if resp.status_code != 200:
                return

            data = resp.json()
            tickers = data.get("result", {}).get("list", [])

            for ticker in tickers:
                symbol = ticker.get("symbol", "")
                # Match to our coins
                for coin, bybit_sym in self._bybit_symbols.items():
                    if symbol == bybit_sym:
                        bid1 = float(ticker.get("bid1Price", 0) or 0)
                        ask1 = float(ticker.get("ask1Price", 0) or 0)
                        if bid1 > 0 and ask1 > 0:
                            self._bybit_mids[coin] = (bid1 + ask1) / 2

            self._bybit_last_fetch = now

        except Exception as e:
            logger.debug(f"Bybit fetch failed: {e}")
