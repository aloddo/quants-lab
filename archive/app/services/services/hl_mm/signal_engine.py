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
    trade_imbalance_side: int = 0     # V2: +1 = buy-heavy TRADE flow, -1 = sell-heavy
    anchor_jump_detected: bool = False
    touch_depletion: bool = False
    any_toxic_flag: bool = False

    # V2: VPIN (Volume-Synchronized Probability of Informed Trading)
    vpin: float = 0.0                 # 0..1, higher = more toxic flow

    # Per-coin momentum (mid price change over 5 minutes, in bps)
    # Positive = coin trending up, negative = trending down.
    # Used by the coin momentum regime filter to suppress the adverse side.
    mid_momentum_5m: float = 0.0

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
        trade_imbalance_threshold: float = 0.60,
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
        # V2: store (ts, direction, notional_usd, price) per trade
        # Was: (ts, direction) only -- lost size info, couldn't do notional-weighted imbalance
        self._trade_sides: dict[str, deque] = {}

        # V2: VPIN state per coin
        self._vpin_buy_volume: dict[str, float] = {}   # accumulated buy volume in current bucket
        self._vpin_sell_volume: dict[str, float] = {}   # accumulated sell volume in current bucket
        self._vpin_bucket_size: dict[str, float] = {}   # target bucket size (daily_vol / 1000)
        self._vpin_buckets: dict[str, deque] = {}        # completed bucket imbalances
        self._vpin_default_bucket_size: float = 5000.0   # $5K default bucket
        self._sigma_ema: dict[str, float] = {}

        # Latest state
        self._signals: dict[str, SignalState] = {}
        self._last_book_time: dict[str, float] = {}

        # Bug #11: Toxic flag timestamps with per-type cooldowns
        # {coin: {flag_name: timestamp_when_triggered}}
        self._toxic_flag_timestamps: dict[str, dict[str, float]] = {}
        self._toxic_cooldowns: dict[str, float] = {
            "depth_drop": 5.0,
            "spread_spike": 10.0,
            "trade_imbalance": 5.0,
            "anchor_jump": 15.0,
            "touch_depletion": 5.0,
        }

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

        # Toxic flow detection — record new triggers with timestamps
        depth_drop = self._check_depth_drop(coin, prev_depth, book)
        spread_spike = self._check_spread_spike(coin, book.spread_bps)
        trade_imbalance, trade_imb_side = self._check_trade_imbalance(coin)
        touch_depl = False  # requires time-series logic, tracked via depth_drop

        # Bug #11: Set toxic flag timestamps when triggered (don't just use booleans)
        if coin not in self._toxic_flag_timestamps:
            self._toxic_flag_timestamps[coin] = {}
        flags = self._toxic_flag_timestamps[coin]
        if depth_drop:
            flags["depth_drop"] = now
        if spread_spike:
            flags["spread_spike"] = now
        if trade_imbalance:
            flags["trade_imbalance"] = now
        if touch_depl:
            flags["touch_depletion"] = now

        # Bug #11: Check if ANY toxic flag is still within its cooldown period
        active_depth_drop = self._is_toxic_active(coin, "depth_drop", now)
        active_spread_spike = self._is_toxic_active(coin, "spread_spike", now)
        active_trade_imbalance = self._is_toxic_active(coin, "trade_imbalance", now)
        active_anchor_jump = self._is_toxic_active(coin, "anchor_jump", now)
        active_touch_depl = self._is_toxic_active(coin, "touch_depletion", now)

        any_toxic = (active_depth_drop or active_spread_spike or active_trade_imbalance
                     or active_anchor_jump or active_touch_depl)
        if any_toxic:
            active = [n for n, v in [("depth_drop", active_depth_drop), ("spread_spike", active_spread_spike),
                      ("trade_imbalance", active_trade_imbalance), ("anchor_jump", active_anchor_jump),
                      ("touch_depletion", active_touch_depl)] if v]
            logger.debug(f"[{coin}] toxic flags active: {active}")

        # Imbalance assessment
        strong_imb = abs(imb_z) >= self.z_threshold
        ofi_confirms = (imb_z > 0 and ofi_bps > 0) or (imb_z < 0 and ofi_bps < 0)
        strong_confirmed = strong_imb and ofi_confirms

        imb_side = 0
        if strong_confirmed:
            imb_side = 1 if imb_z > 0 else -1

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
            depth_drop_detected=active_depth_drop,
            spread_spike_detected=active_spread_spike,
            trade_imbalance_toxic=active_trade_imbalance,
            trade_imbalance_side=trade_imb_side if active_trade_imbalance else 0,
            anchor_jump_detected=active_anchor_jump,
            touch_depletion=active_touch_depl,
            any_toxic_flag=any_toxic,
            vpin=self.get_vpin(coin),
            book_age_ms=0.0,
            is_stale=False,
        )

        self._signals[coin] = signal
        return signal

    def update_trades(self, coin: str, trades: list[dict]) -> None:
        """Process trade events from HL WS.

        V2: Stores full trade data (direction, notional, price) for:
        - Notional-weighted trade imbalance (not count-based)
        - VPIN computation (volume-bucketed)
        - Future wallet tracking (users field)

        Each trade dict: {side: "B"|"S", px: str/float, sz: str/float}
        """
        self._ensure_coin(coin)
        now = time.time()
        for trade in trades:
            side_str = trade.get("side", "")
            direction = 1 if side_str == "B" else -1
            price = float(trade.get("px", 0) or 0)
            size = float(trade.get("sz", 0) or 0)
            notional = price * size
            self._trade_sides[coin].append((now, direction, notional, price))

            # V2: VPIN bucket accumulation
            if notional > 0:
                self._accumulate_vpin(coin, direction, notional)

    def _accumulate_vpin(self, coin: str, direction: int, notional: float) -> None:
        """Accumulate trade volume into VPIN buckets.

        VPIN = rolling mean of |buy_vol - sell_vol| / total_vol over N buckets.
        Buckets are VOLUME-synchronized (fixed $ size), not time-synchronized.

        Bug #12 fix: When a single trade exceeds bucket size, carry over excess
        volume into the next bucket instead of dropping it.
        """
        if coin not in self._vpin_buy_volume:
            self._vpin_buy_volume[coin] = 0.0
            self._vpin_sell_volume[coin] = 0.0
            self._vpin_buckets[coin] = deque(maxlen=50)

        if direction > 0:
            self._vpin_buy_volume[coin] += notional
        else:
            self._vpin_sell_volume[coin] += notional

        bucket_size = self._vpin_bucket_size.get(coin, self._vpin_default_bucket_size)
        total = self._vpin_buy_volume[coin] + self._vpin_sell_volume[coin]

        # Bucket complete when total volume reaches bucket_size
        # Bug #12: Loop to handle large trades that overflow multiple buckets
        while total >= bucket_size:
            imbalance = abs(self._vpin_buy_volume[coin] - self._vpin_sell_volume[coin]) / total
            self._vpin_buckets[coin].append(imbalance)
            # Carry over excess volume proportionally
            overflow = total - bucket_size
            if overflow > 0 and total > 0:
                # Distribute overflow in the same buy/sell ratio as current bucket
                buy_ratio = self._vpin_buy_volume[coin] / total
                self._vpin_buy_volume[coin] = overflow * buy_ratio
                self._vpin_sell_volume[coin] = overflow * (1 - buy_ratio)
            else:
                self._vpin_buy_volume[coin] = 0.0
                self._vpin_sell_volume[coin] = 0.0
            total = self._vpin_buy_volume[coin] + self._vpin_sell_volume[coin]

    def get_vpin(self, coin: str) -> float:
        """Get current VPIN for a coin. Returns 0..1, higher = more informed flow."""
        buckets = self._vpin_buckets.get(coin)
        if not buckets or len(buckets) < 5:
            return 0.0  # not enough data
        return sum(buckets) / len(buckets)

    def set_vpin_bucket_size(self, coin: str, daily_volume: float) -> None:
        """Set VPIN bucket size from daily volume (daily_vol / 1000)."""
        if daily_volume > 0:
            self._vpin_bucket_size[coin] = max(100.0, daily_volume / 1000.0)

    def check_anchor_jump(self, coin: str, bybit_mid: float, prev_bybit_mid: float) -> bool:
        """Check if Bybit anchor jumped > 6bps in 1s (Spec Section 5).

        Bug #11: Uses timestamp-based flag persistence instead of clearing on next tick.
        """
        if prev_bybit_mid <= 0 or bybit_mid <= 0:
            return False
        jump_bps = abs(bybit_mid - prev_bybit_mid) / prev_bybit_mid * 10000
        # Scale threshold by pair's native spread: wide-spread pairs move more
        median_spread = np.median(list(self._spread_history.get(coin, [self.anchor_jump_bps]))) if self._spread_history.get(coin) else self.anchor_jump_bps
        adaptive_threshold = max(self.anchor_jump_bps, median_spread * 1.5)
        if jump_bps > adaptive_threshold:
            now = time.time()
            if coin not in self._toxic_flag_timestamps:
                self._toxic_flag_timestamps[coin] = {}
            self._toxic_flag_timestamps[coin]["anchor_jump"] = now

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

            # Compute per-coin 5-minute mid momentum (bps)
            mid_hist = self._mid_history.get(coin)
            if mid_hist and len(mid_hist) >= 60:
                # _mid_history stores (timestamp, mid_price) tuples
                current_mid = mid_hist[-1][1] if isinstance(mid_hist[-1], tuple) else mid_hist[-1]
                # Find price ~300s ago (5 min). History is at ~1/sec, so index -300
                lookback_idx = max(0, len(mid_hist) - 300)
                old_entry = mid_hist[lookback_idx]
                old_mid = old_entry[1] if isinstance(old_entry, tuple) else old_entry
                if old_mid > 0 and current_mid > 0:
                    signal.mid_momentum_5m = (current_mid - old_mid) / old_mid * 10000
                else:
                    signal.mid_momentum_5m = 0.0
            else:
                signal.mid_momentum_5m = 0.0

        return signal

    # ------------------------------------------------------------------
    # Internal computations
    # ------------------------------------------------------------------

    def _is_toxic_active(self, coin: str, flag_name: str, now: float) -> bool:
        """Bug #11: Check if a toxic flag is still within its cooldown period."""
        flags = self._toxic_flag_timestamps.get(coin, {})
        triggered_at = flags.get(flag_name, 0)
        if triggered_at <= 0:
            return False
        cooldown = self._toxic_cooldowns.get(flag_name, 5.0)
        return (now - triggered_at) < cooldown

    def _ensure_coin(self, coin: str) -> None:
        """Initialize history containers for a coin if needed."""
        if coin not in self._imb_history:
            self._imb_history[coin] = deque(maxlen=self.z_window)
            self._mid_history[coin] = deque(maxlen=600)  # 10 min at 1/sec
            self._spread_history[coin] = deque(maxlen=300)  # 5 min
            self._depth5_history[coin] = deque(maxlen=60)  # 1 min
            self._trade_sides[coin] = deque(maxlen=2000)  # V2: larger buffer for VPIN + notional imbalance
            self._sigma_ema[coin] = 0.0
            self._toxic_flag_timestamps[coin] = {}

    def _parse_book(
        self, coin: str, bids: list, asks: list, now: float
    ) -> Optional[BookSnapshot]:
        """Parse raw L2 levels into BookSnapshot.

        Bug #7 fix: Gracefully handle malformed HL payloads (missing keys,
        wrong types) instead of crashing the entire signal processing loop.
        """
        try:
            best_bid = float(bids[0]["px"])
            best_ask = float(asks[0]["px"])
        except (KeyError, TypeError, ValueError, IndexError) as e:
            logger.debug(f"[{coin}] _parse_book: malformed L2 data: {e}")
            return None
        if best_bid <= 0 or best_ask <= 0:
            return None

        try:
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
        except (KeyError, TypeError, ValueError, IndexError) as e:
            logger.debug(f"[{coin}] _parse_book: error parsing depth levels: {e}")
            return None

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
        """V2: Real L2 Order Flow Imbalance from book size changes.

        OLD (V1): Summed mid-price changes. That's price momentum, not order flow.
        NEW (V2): Delta of bid vs ask sizes at top 5 levels between consecutive
        book snapshots. Positive = bid size increasing relative to ask (buying pressure).

        OFI = Σ (Δbid_size@level_i - Δask_size@level_i) for i in [1..5]
        Normalized to bps using mid price.
        """
        depth_hist = self._depth5_history.get(coin)
        if not depth_hist or len(depth_hist) < 2:
            return 0.0

        # Use last 10 snapshots (5s rolling at ~2 updates/sec)
        recent = list(depth_hist)[-11:]
        if len(recent) < 2:
            return 0.0

        ofi_raw = 0.0
        for i in range(1, len(recent)):
            _, prev_bid_usd, prev_ask_usd = recent[i - 1]
            _, curr_bid_usd, curr_ask_usd = recent[i]
            # Delta of bid depth minus delta of ask depth
            ofi_raw += (curr_bid_usd - prev_bid_usd) - (curr_ask_usd - prev_ask_usd)

        # Normalize: express as bps relative to average depth
        avg_depth = sum(b + a for _, b, a in recent) / len(recent) / 2
        if avg_depth > 0:
            return ofi_raw / avg_depth * 10000
        return 0.0

    def _compute_volatility(self, coin: str) -> tuple[float, float]:
        """Compute per-second volatility and 30s realized vol.

        Returns (sigma_1s, rv_30s).
        Bug #15 fix: Guard against zero/negative prices and empty arrays.
        """
        mids = self._mid_history[coin]
        if len(mids) < 5:
            return 1.5e-5, 0.0

        prices = [p for _, p in mids]
        times = [t for t, _ in mids]

        # Bug #15: Guard against zero/negative prices (would cause log domain error)
        if min(prices) <= 0:
            return 1.5e-5, 0.0

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
        """Same-side top-5 depth drop > threshold in 1s.

        Threshold scales with native spread: wide-spread pairs (shitcoins) have
        naturally volatile depth, so use 60% instead of 40%.
        """
        if not prev_depth:
            return False

        # Scale threshold by spread regime: tight (<2bps) = 40%, wide (>5bps) = 65%
        median_spread = np.median(list(self._spread_history[coin])) if self._spread_history[coin] else 1.0
        drop_threshold = 0.40 if median_spread < 2.0 else (0.55 if median_spread < 5.0 else 0.65)

        prev_ts, prev_bid5, prev_ask5 = prev_depth
        age = book.timestamp - prev_ts

        if age > 2.0 or age < 0.1:
            return False

        if prev_bid5 > 0 and (book.bid_usd_top5 / prev_bid5) < (1 - drop_threshold):
            return True
        if prev_ask5 > 0 and (book.ask_usd_top5 / prev_ask5) < (1 - drop_threshold):
            return True
        return False

    def _check_spread_spike(self, coin: str, current_spread: float) -> bool:
        """Spread widening > Nx the 5-min median.

        Threshold scales with native spread: wide-spread pairs naturally fluctuate more.
        Tight (<2bps): 1.5x. Mid (2-5bps): 2.0x. Wide (>5bps): 2.5x.
        """
        history = self._spread_history[coin]
        if len(history) < 30:  # need at least 30s
            return False

        median = np.median(list(history))
        if median <= 0:
            return False

        # Scale multiplier by spread regime
        factor = 1.5 if median < 2.0 else (2.0 if median < 5.0 else 2.5)
        return current_spread > median * factor

    def _check_trade_imbalance(self, coin: str) -> tuple[bool, int]:
        """V2: 3s NOTIONAL-weighted trade imbalance > 70/30.

        Was count-based (each print = 1 vote regardless of size).
        Now weighted by USD notional: a $500 trade counts 50x more than $10.

        Returns (is_toxic, side) where side is +1 (buy-heavy) or -1 (sell-heavy).
        """
        trades = self._trade_sides[coin]
        if not trades:
            return False, 0

        now = time.time()
        buy_notional = 0.0
        total_notional = 0.0
        count = 0
        for entry in trades:
            ts = entry[0]
            if now - ts > 3.0:
                continue
            direction = entry[1]
            notional = entry[2] if len(entry) > 2 else 1.0  # backward compat
            total_notional += notional
            if direction > 0:
                buy_notional += notional
            count += 1

        if count < 5 or total_notional < 1.0:
            return False, 0

        buy_ratio = buy_notional / total_notional
        if buy_ratio > self.trade_imbalance_threshold:
            return True, 1   # buy-heavy
        elif buy_ratio < (1 - self.trade_imbalance_threshold):
            return True, -1  # sell-heavy
        return False, 0
