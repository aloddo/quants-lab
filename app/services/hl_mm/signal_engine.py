"""
Signal Engine V2 — L2 imbalance with anti-spoof filter.

Fixes from adversarial review:
- F9: Anti-spoof: require trade volume confirmation alongside imbalance
- F10: Staleness tracking — flag when data is too old for safe quoting
- Rate limit handling with staggered calls
"""
import logging
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

import numpy as np
from hyperliquid.info import Info

logger = logging.getLogger(__name__)


@dataclass
class ImbalanceSignal:
    """Current signal state for a single pair."""
    coin: str
    timestamp: float
    imbalance: float      # 0=all asks, 0.5=balanced, 1=all bids
    z_score: float
    mid_price: float
    best_bid: float
    best_ask: float
    spread_bps: float
    direction: int        # +1=bid heavy, -1=ask heavy, 0=neutral
    is_stale: bool        # True if data is too old for safe quoting
    trade_confirmed: bool # True if recent trades confirm the imbalance direction
    data_age_ms: float    # milliseconds since last successful fetch


class SignalEngine:
    """L2 imbalance signal with anti-spoof and staleness tracking."""

    def __init__(
        self,
        info: Info,
        coins: list[str],
        z_window: int = 300,
        z_threshold: float = 2.0,
        top_n_levels: int = 10,
        stale_threshold_ms: float = 2000.0,  # data older than 2s = stale
        spoof_confirmation_trades: int = 3,   # need N trades in predicted direction
    ):
        self.info = info
        self.coins = coins
        self.z_window = z_window
        self.z_threshold = z_threshold
        self.top_n_levels = top_n_levels
        self.stale_threshold_ms = stale_threshold_ms
        self.spoof_confirmation_trades = spoof_confirmation_trades

        self._imb_history: dict[str, deque] = {
            coin: deque(maxlen=z_window) for coin in coins
        }
        self._last_fetch_time: dict[str, float] = {coin: 0.0 for coin in coins}
        self._recent_trade_dirs: dict[str, deque] = {
            coin: deque(maxlen=20) for coin in coins  # last 20 trade directions
        }
        self._signals: dict[str, ImbalanceSignal] = {}
        self._last_mids: dict[str, float] = {}

    def update(self) -> dict[str, ImbalanceSignal]:
        """Fetch L2 for all coins. Staggered to avoid rate limits."""
        for coin in self.coins:
            try:
                signal = self._compute_signal(coin)
                if signal:
                    self._signals[coin] = signal
            except Exception as e:
                if "429" in str(e):
                    logger.debug(f"Rate limited on {coin}, skipping this tick")
                else:
                    logger.warning(f"Signal update failed for {coin}: {e}")
            time.sleep(0.2)  # 200ms between calls

        return self._signals

    def get_signal(self, coin: str) -> Optional[ImbalanceSignal]:
        return self._signals.get(coin)

    def _compute_signal(self, coin: str) -> Optional[ImbalanceSignal]:
        """Compute imbalance with anti-spoof confirmation."""
        # F2 FIX: measure staleness as time SINCE LAST SUCCESSFUL fetch
        # (not overwritten until end of this function)
        prev_fetch_time = self._last_fetch_time[coin]
        l2 = self.info.l2_snapshot(coin)

        if not l2 or "levels" not in l2:
            return None

        bids, asks = l2["levels"]
        if not bids or not asks:
            return None

        # Compute USD-weighted imbalance over top-N levels
        n = min(self.top_n_levels, len(bids), len(asks))
        bid_sz = sum(float(bids[i]["sz"]) * float(bids[i]["px"]) for i in range(n))
        ask_sz = sum(float(asks[i]["sz"]) * float(asks[i]["px"]) for i in range(n))
        total = bid_sz + ask_sz
        if total == 0:
            return None

        imbalance = bid_sz / total

        # Track mid for trade direction inference
        best_bid = float(bids[0]["px"])
        best_ask = float(asks[0]["px"])
        mid = (best_bid + best_ask) / 2.0

        # Infer trade direction from mid-price changes (F9: anti-spoof)
        prev_mid = self._last_mids.get(coin)
        if prev_mid and prev_mid != mid:
            # Mid moved up = likely buy trade, down = sell trade
            trade_dir = 1 if mid > prev_mid else -1
            self._recent_trade_dirs[coin].append(trade_dir)
        self._last_mids[coin] = mid

        # Update history
        self._imb_history[coin].append(imbalance)
        history = self._imb_history[coin]

        # Z-score (need warmup)
        if len(history) < self.z_window // 3:
            z_score = 0.0
        else:
            arr = np.array(history)
            mean = arr.mean()
            std = arr.std()
            z_score = (imbalance - mean) / std if std > 1e-10 else 0.0

        # Direction
        if z_score > self.z_threshold:
            raw_direction = 1
        elif z_score < -self.z_threshold:
            raw_direction = -1
        else:
            raw_direction = 0

        # Anti-spoof confirmation (F9): check if recent trades support the signal
        trade_confirmed = self._check_trade_confirmation(coin, raw_direction)

        # Only emit directional signal if trade-confirmed
        direction = raw_direction if trade_confirmed else 0

        # F2 FIX: Update fetch time AFTER processing (so next tick sees correct age)
        self._last_fetch_time[coin] = time.time()

        # Staleness: how long between THIS fetch and the PREVIOUS one
        # If > threshold, we missed ticks (API errors, rate limits)
        data_age_ms = (time.time() - prev_fetch_time) * 1000 if prev_fetch_time > 0 else 0
        is_stale = data_age_ms > self.stale_threshold_ms

        spread_bps = (best_ask - best_bid) / best_bid * 10000 if best_bid > 0 else 999

        return ImbalanceSignal(
            coin=coin,
            timestamp=time.time(),
            imbalance=imbalance,
            z_score=z_score,
            mid_price=mid,
            best_bid=best_bid,
            best_ask=best_ask,
            spread_bps=spread_bps,
            direction=direction,
            is_stale=is_stale,
            trade_confirmed=trade_confirmed,
            data_age_ms=data_age_ms,
        )

    def _check_trade_confirmation(self, coin: str, predicted_dir: int) -> bool:
        """F9: Require recent trades to confirm imbalance direction.

        If imbalance says bid-heavy (price should go up), we need to see
        recent actual buys (mid moving up). This filters spoofed depth.
        """
        if predicted_dir == 0:
            return True  # neutral always confirmed

        recent = list(self._recent_trade_dirs[coin])
        if len(recent) < self.spoof_confirmation_trades:
            return False  # not enough data to confirm

        # Count trades in predicted direction (last N trades)
        last_n = recent[-self.spoof_confirmation_trades:]
        confirming = sum(1 for d in last_n if d == predicted_dir)

        # Need majority (>50%) of recent trades to confirm
        return confirming > len(last_n) // 2
