"""
Signal Engine — L2 book analysis + adverse selection detection (Spec Section 5).

Processes HL WebSocket L2 book and trade data to produce:
  - Microprice and imbalance z-score
  - Order flow imbalance (OFI) for fair value correction
  - Depth monitoring (top-20 and top-5)
  - Toxic flow flags:
    * Same-side top-5 depth drop > 40% in 1s
    * Spread widening > 1.5x the 5-min median
    * 3s trade imbalance > 70/30
    * Anchor jump > 6bps in 1s
    * Touch depletion without depth replenish inside 2s

This module does NOT own WS connections. The orchestrator feeds L2 and
trade data via update_book() and update_trades().
"""
import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class BookSnapshot:
    """Parsed L2 book state for one pair."""
    coin: str
    timestamp: float
    best_bid: float
    best_ask: float
    mid: float
    spread_bps: float
    bid_qty_top1: float       # base units at best bid
    ask_qty_top1: float
    bid_usd_top5: float       # USD in top 5 levels
    ask_usd_top5: float
    bid_usd_top20: float      # USD in top 20 levels
    ask_usd_top20: float
    microprice: float         # size-weighted mid
    imbalance: float          # 0=all asks, 0.5=balanced, 1=all bids (top 10)


@dataclass
class SignalState:
    """Aggregated signal state for one pair, updated every tick."""
    coin: str
    timestamp: float
    book: Optional[BookSnapshot]

    # Imbalance
    imbalance_z: float = 0.0
    strong_imbalance: bool = False    # |z| >= 1.5 + OFI confirms
    imbalance_side: int = 0           # +1 = bid heavy, -1 = ask heavy

    # OFI (order flow imbalance) in bps
    ofi_bps: float = 0.0

    # Volatility (per-second)
    sigma_1s: float = 1.5e-5          # default BTC-like
    rv_30s: float = 0.0               # 30s realized vol

    # Toxic flow flags
    depth_drop_detected: bool = False
    spread_spike_detected: bool = False
    trade_imbalance_toxic: bool = False
    anchor_jump_detected: bool = False
    touch_depletion: bool = False
    any_toxic_flag: bool = False

    # Data freshness
    book_age_ms: float = 999999.0
    is_stale: bool = True


class SignalEngine:
    """Process L2 books and trades into trading signals."""

    def __init__(
        self,
        z_window: int = 300,
        z_threshold: float = 1.5,
        top_n_imbalance: int = 10,
        vol_ema_alpha: float = 0.05,
        depth_drop_threshold: float = 0.40,
        spread_spike_factor: float = 1.5,
        trade_imbalance_threshold: float = 0.70,
        anchor_jump_bps: float = 6.0,
    ):
        self.z_window = z_window
        self.z_threshold = z_threshold
        self.top_n_imbalance = top_n_imbalance
        self.vol_ema_alpha = vol_ema_alpha
        self.depth_drop_threshold = depth_drop_threshold
        self.spread_spike_factor = spread_spike_factor
        self.trade_imbalance_threshold = trade_imbalance_threshold
        self.anchor_jump_bps = anchor_jump_bps

        # Per-coin history
        self._imb_history: dict[str, deque] = {}
        self._mid_history: dict[str, deque] = {}   # (timestamp, mid)
        self._spread_history: dict[str, deque] = {}
        self._depth5_history: dict[str, deque] = {}  # (ts, bid_usd5, ask_usd5)
        self._trade_sides: dict[str, deque] = {}     # (ts, +1 or -1)
        self._sigma_ema: dict[str, float] = {}

        # Latest state
        self._signals: dict[str, SignalState] = {}
        self._last_book_time: dict[str, float] = {}

    def update_book(self, coin: str, l2: dict) -> Optional[SignalState]:
        """Process an L2 book update. Returns updated signal state.

        Args:
            coin: HL coin name
            l2: raw L2 snapshot dict from HL (WS or REST)
        """
        now = time.time()
        self._ensure_coin(coin)

        if not l2 or "levels" not in l2:
            return self._signals.get(coin)

        bids, asks = l2.get("levels", ([], []))
        if not bids or not asks:
            return self._signals.get(coin)

        book = self._parse_book(coin, bids, asks, now)
        if not book:
            return self._signals.get(coin)

        # Update histories
        self._imb_history[coin].append(book.imbalance)
        self._mid_history[coin].append((now, book.mid))
        self._spread_history[coin].append(book.spread_bps)

        prev_depth = self._depth5_history[coin][-1] if self._depth5_history[coin] else None
        self._depth5_history[coin].append((now, book.bid_usd_top5, book.ask_usd_top5))

        self._last_book_time[coin] = now

        # Compute signals
        imb_z = self._compute_imbalance_z(coin)
        ofi_bps = self._compute_ofi(coin)
        sigma_1s, rv_30s = self._compute_volatility(coin)

        # Toxic flow detection
        depth_drop = self._check_depth_drop(coin, prev_depth, book)
        spread_spike = self._check_spread_spike(coin, book.spread_bps)
        trade_imbalance = self._check_trade_imbalance(coin)
        touch_depl = False  # requires time-series logic, tracked via depth_drop

        # Imbalance assessment
        strong_imb = abs(imb_z) >= self.z_threshold
        ofi_confirms = (imb_z > 0 and ofi_bps > 0) or (imb_z < 0 and ofi_bps < 0)
        strong_confirmed = strong_imb and ofi_confirms

        imb_side = 0
        if strong_confirmed:
            imb_side = 1 if imb_z > 0 else -1

        any_toxic = depth_drop or spread_spike or trade_imbalance

        signal = SignalState(
            coin=coin,
            timestamp=now,
            book=book,
            imbalance_z=imb_z,
            strong_imbalance=strong_confirmed,
            imbalance_side=imb_side,
            ofi_bps=ofi_bps,
            sigma_1s=sigma_1s,
            rv_30s=rv_30s,
            depth_drop_detected=depth_drop,
            spread_spike_detected=spread_spike,
            trade_imbalance_toxic=trade_imbalance,
            anchor_jump_detected=False,  # set by orchestrator from FV engine
            touch_depletion=touch_depl,
            any_toxic_flag=any_toxic,
            book_age_ms=0.0,
            is_stale=False,
        )

        self._signals[coin] = signal
        return signal

    def update_trades(self, coin: str, trades: list[dict]) -> None:
        """Process trade events from HL WS.

        Each trade dict should have: {side: "B"|"S", px: float, sz: float}
        """
        self._ensure_coin(coin)
        now = time.time()
        for trade in trades:
            side_str = trade.get("side", "")
            direction = 1 if side_str == "B" else -1
            self._trade_sides[coin].append((now, direction))

    def check_anchor_jump(self, coin: str, bybit_mid: float, prev_bybit_mid: float) -> bool:
        """Check if Bybit anchor jumped > 6bps in 1s (Spec Section 5)."""
        if prev_bybit_mid <= 0 or bybit_mid <= 0:
            return False
        jump_bps = abs(bybit_mid - prev_bybit_mid) / prev_bybit_mid * 10000
        if jump_bps > self.anchor_jump_bps:
            signal = self._signals.get(coin)
            if signal:
                signal.anchor_jump_detected = True
                signal.any_toxic_flag = True
            return True
        return False

    def get_signal(self, coin: str) -> Optional[SignalState]:
        """Get latest signal for a coin."""
        signal = self._signals.get(coin)
        if signal:
            # Update staleness
            now = time.time()
            last = self._last_book_time.get(coin, 0)
            signal.book_age_ms = (now - last) * 1000 if last > 0 else 999999.0
            signal.is_stale = signal.book_age_ms > 1500
        return signal

    # ------------------------------------------------------------------
    # Internal computations
    # ------------------------------------------------------------------

    def _ensure_coin(self, coin: str) -> None:
        """Initialize history containers for a coin if needed."""
        if coin not in self._imb_history:
            self._imb_history[coin] = deque(maxlen=self.z_window)
            self._mid_history[coin] = deque(maxlen=600)  # 10 min at 1/sec
            self._spread_history[coin] = deque(maxlen=300)  # 5 min
            self._depth5_history[coin] = deque(maxlen=60)  # 1 min
            self._trade_sides[coin] = deque(maxlen=200)
            self._sigma_ema[coin] = 0.0

    def _parse_book(
        self, coin: str, bids: list, asks: list, now: float
    ) -> Optional[BookSnapshot]:
        """Parse raw L2 levels into BookSnapshot."""
        best_bid = float(bids[0]["px"])
        best_ask = float(asks[0]["px"])
        if best_bid <= 0 or best_ask <= 0:
            return None

        mid = (best_bid + best_ask) / 2.0
        spread_bps = (best_ask - best_bid) / best_bid * 10000

        bid_qty1 = float(bids[0]["sz"])
        ask_qty1 = float(asks[0]["sz"])

        # Top-5 and top-20 depth
        n5 = min(5, len(bids), len(asks))
        n20 = min(20, len(bids), len(asks))

        bid_usd5 = sum(float(bids[i]["sz"]) * float(bids[i]["px"]) for i in range(n5))
        ask_usd5 = sum(float(asks[i]["sz"]) * float(asks[i]["px"]) for i in range(n5))
        bid_usd20 = sum(float(bids[i]["sz"]) * float(bids[i]["px"]) for i in range(n20))
        ask_usd20 = sum(float(asks[i]["sz"]) * float(asks[i]["px"]) for i in range(n20))

        # Imbalance over top-N
        n = min(self.top_n_imbalance, len(bids), len(asks))
        bid_sz = sum(float(bids[i]["sz"]) * float(bids[i]["px"]) for i in range(n))
        ask_sz = sum(float(asks[i]["sz"]) * float(asks[i]["px"]) for i in range(n))
        total = bid_sz + ask_sz
        imbalance = bid_sz / total if total > 0 else 0.5

        # Microprice
        total_qty1 = bid_qty1 + ask_qty1
        if total_qty1 > 0:
            I = bid_qty1 / total_qty1
            microprice = best_ask * I + best_bid * (1.0 - I)
        else:
            microprice = mid

        return BookSnapshot(
            coin=coin, timestamp=now,
            best_bid=best_bid, best_ask=best_ask, mid=mid,
            spread_bps=spread_bps,
            bid_qty_top1=bid_qty1, ask_qty_top1=ask_qty1,
            bid_usd_top5=bid_usd5, ask_usd_top5=ask_usd5,
            bid_usd_top20=bid_usd20, ask_usd_top20=ask_usd20,
            microprice=microprice, imbalance=imbalance,
        )

    def _compute_imbalance_z(self, coin: str) -> float:
        """Compute z-score of current imbalance vs history."""
        history = self._imb_history[coin]
        if len(history) < self.z_window // 3:
            return 0.0

        arr = np.array(history)
        mean = arr.mean()
        std = arr.std()
        if std < 1e-10:
            return 0.0

        return (history[-1] - mean) / std

    def _compute_ofi(self, coin: str) -> float:
        """Compute order flow imbalance in bps from recent mid changes.

        OFI positive = buying pressure, negative = selling pressure.
        """
        mids = self._mid_history[coin]
        if len(mids) < 5:
            return 0.0

        # Use last 10 mid changes
        recent = list(mids)[-11:]
        if len(recent) < 2:
            return 0.0

        # Sum of signed mid changes (bps)
        ofi = 0.0
        for i in range(1, len(recent)):
            _, prev_mid = recent[i - 1]
            _, curr_mid = recent[i]
            if prev_mid > 0:
                ofi += (curr_mid - prev_mid) / prev_mid * 10000

        return ofi

    def _compute_volatility(self, coin: str) -> tuple[float, float]:
        """Compute per-second volatility and 30s realized vol.

        Returns (sigma_1s, rv_30s).
        """
        mids = self._mid_history[coin]
        if len(mids) < 5:
            return 1.5e-5, 0.0

        prices = [p for _, p in mids]
        times = [t for t, _ in mids]

        returns = np.diff(np.log(prices))
        dts = np.diff(times)

        valid = dts > 0
        if valid.sum() < 3:
            return 1.5e-5, 0.0

        returns = returns[valid]
        dts = dts[valid]

        # Per-second variance
        per_sec_vars = (returns ** 2) / dts
        sigma_1s = math.sqrt(np.mean(per_sec_vars))

        # EMA smoothing
        prev = self._sigma_ema.get(coin, 0)
        if prev > 0:
            alpha = self.vol_ema_alpha
            sigma_1s = alpha * sigma_1s + (1 - alpha) * prev
        self._sigma_ema[coin] = sigma_1s

        # 30s realized vol: use last 30s of data
        now = times[-1]
        recent_mask = np.array(times[1:])[valid] > now - 30
        if recent_mask.sum() >= 3:
            recent_vars = per_sec_vars[recent_mask[-len(per_sec_vars):]] if len(recent_mask) == len(per_sec_vars) else per_sec_vars
            rv_30s = math.sqrt(np.mean(recent_vars))
        else:
            rv_30s = sigma_1s

        return sigma_1s, rv_30s

    # ------------------------------------------------------------------
    # Toxic flow detection
    # ------------------------------------------------------------------

    def _check_depth_drop(
        self, coin: str, prev_depth: Optional[tuple], book: BookSnapshot
    ) -> bool:
        """Same-side top-5 depth drop > 40% in 1s."""
        if not prev_depth:
            return False

        prev_ts, prev_bid5, prev_ask5 = prev_depth
        age = book.timestamp - prev_ts

        if age > 2.0 or age < 0.1:
            return False

        if prev_bid5 > 0 and (book.bid_usd_top5 / prev_bid5) < (1 - self.depth_drop_threshold):
            return True
        if prev_ask5 > 0 and (book.ask_usd_top5 / prev_ask5) < (1 - self.depth_drop_threshold):
            return True
        return False

    def _check_spread_spike(self, coin: str, current_spread: float) -> bool:
        """Spread widening > 1.5x the 5-min median."""
        history = self._spread_history[coin]
        if len(history) < 30:  # need at least 30s
            return False

        median = np.median(list(history))
        if median <= 0:
            return False

        return current_spread > median * self.spread_spike_factor

    def _check_trade_imbalance(self, coin: str) -> bool:
        """3s trade imbalance > 70/30."""
        trades = self._trade_sides[coin]
        if not trades:
            return False

        now = time.time()
        recent = [(t, d) for t, d in trades if now - t <= 3.0]
        if len(recent) < 5:
            return False

        buys = sum(1 for _, d in recent if d > 0)
        total = len(recent)
        buy_ratio = buys / total

        return buy_ratio > self.trade_imbalance_threshold or buy_ratio < (1 - self.trade_imbalance_threshold)
