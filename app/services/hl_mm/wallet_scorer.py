"""
Wallet Toxicity Scorer — HL-unique edge for market making.

On Hyperliquid, every trade includes wallet addresses (buyer + seller).
No CEX gives you this. We use it to score wallets by their historical
markout: wallets whose trades consistently predict adverse price moves
are TOXIC. When a toxic wallet trades our coin, we widen or pull quotes.

Data flow:
  1. Trade WS callback extracts `users` field → (buyer, seller)
  2. Each trade stored with wallet addresses in ring buffer
  3. When markout_5s is computed for a fill, we attribute the markout
     to the counterparty wallet
  4. Per-wallet EWMA markout builds a toxicity score over time
  5. Live gating: if recent trades include a toxic wallet → widen/pull

Confidence gates:
  - Need >= 20 trades per wallet before scoring
  - Need >= $500 matched notional
  - Lower confidence bound must be < -1.5bps to flag as toxic
  - Scores decay with a 24h half-life (wallets can change behavior)

This is a CORE signal, not research. It runs live from the first trade.
"""
import logging
import math
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class WalletStats:
    """Per-wallet markout statistics."""
    address: str
    trade_count: int = 0
    total_notional: float = 0.0
    markout_sum: float = 0.0          # sum of markout_5s in bps
    markout_sum_sq: float = 0.0       # sum of squared markouts (for variance)
    ewma_markout: float = 0.0         # EWMA of markout_5s
    last_trade_time: float = 0.0
    is_toxic: bool = False            # cached toxicity verdict


@dataclass
class WalletTrade:
    """A trade with wallet attribution."""
    coin: str
    timestamp: float
    side: str                         # "B" or "S" (aggressor side)
    price: float
    size: float
    notional: float
    buyer: str                        # wallet address
    seller: str                       # wallet address
    markout_5s: Optional[float] = None  # filled in later


class WalletScorer:
    """Score wallets by adverse selection and gate quoting decisions.

    Thread-safe: WS callbacks and tick loop both access this.
    """

    def __init__(
        self,
        min_trades: int = 20,
        min_notional: float = 500.0,
        toxic_threshold_bps: float = -1.5,  # lower bound on markout to flag toxic
        ewma_alpha: float = 0.08,           # ~12-trade half-life
        decay_halflife_s: float = 86400.0,  # 24h score decay
        max_history: int = 10000,
    ):
        self.min_trades = min_trades
        self.min_notional = min_notional
        self.toxic_threshold_bps = toxic_threshold_bps
        self.ewma_alpha = ewma_alpha
        self.decay_halflife_s = decay_halflife_s

        self._lock = threading.Lock()
        self._wallets: dict[str, WalletStats] = {}
        self._recent_trades: deque[WalletTrade] = deque(maxlen=max_history)

        # Per-coin running trade size stats for institutional clip detection
        # Tracks median trade size so we can flag clips that are >Nx normal
        self._coin_trade_sizes: dict[str, deque] = {}  # coin -> last 500 trade notionals
        self._coin_trade_size_window: int = 500

        # Per-coin recent toxic wallet activity (for live gating)
        # coin -> list of (timestamp, wallet_addr) for toxic wallets seen in last 10s
        self._toxic_activity: dict[str, list[tuple[float, str]]] = {}

        # V3: Active metaorder detection state
        # coin -> MetaorderSignal (or None)
        self._active_metaorders: dict[str, Optional["MetaorderSignal"]] = {}

    def record_trade(
        self,
        coin: str,
        side: str,
        price: float,
        size: float,
        buyer: str,
        seller: str,
        exchange_time: float = 0.0,
    ) -> None:
        """Record a trade with wallet addresses.

        Called from WS trade callback for every trade on our coins.

        Bug #5 fix: Use exchange_time (from HL trade 'time' field, ms epoch)
        instead of local time.time(). This prevents WS subscription replay trades
        from being recorded with current timestamps and fabricating metaorders.
        Trades older than 120s are silently dropped (stale replay).
        """
        if not buyer or not seller:
            return

        # Use exchange time if provided, else fall back to local
        now = time.time()
        ts = exchange_time if exchange_time > 0 else now

        # Bug #5: Drop stale replay trades (>120s old)
        if now - ts > 120.0:
            return

        notional = price * size

        # Track per-coin trade sizes for institutional clip detection
        with self._lock:
            if coin not in self._coin_trade_sizes:
                self._coin_trade_sizes[coin] = deque(maxlen=self._coin_trade_size_window)
            self._coin_trade_sizes[coin].append(notional)

        trade = WalletTrade(
            coin=coin, timestamp=ts, side=side,
            price=price, size=size, notional=notional,
            buyer=buyer, seller=seller,
        )

        with self._lock:
            self._recent_trades.append(trade)

            # Track the aggressor wallet (the one who crossed the spread)
            # Bug #11 note: trade_count tracks total observed trades for confidence
            # gating (min_trades threshold). attribute_markout does NOT increment
            # trade_count — it only updates markout statistics. No double-counting.
            aggressor = buyer if side == "B" else seller
            self._ensure_wallet(aggressor)
            ws = self._wallets[aggressor]
            ws.trade_count += 1
            ws.total_notional += notional
            ws.last_trade_time = now

            # Check if aggressor is known toxic → record activity for live gating
            if ws.is_toxic:
                if coin not in self._toxic_activity:
                    self._toxic_activity[coin] = []
                self._toxic_activity[coin].append((now, aggressor))

    def attribute_markout(
        self,
        coin: str,
        fill_side: str,
        fill_price: float,
        fill_time: float,
        markout_5s: float,
    ) -> None:
        """Attribute a markout to counterparty wallets.

        When our fill gets a markout, find trades around that time+price
        that match, and attribute the markout to the aggressor wallet.

        The aggressor on our fill is the counterparty who hit our quote.
        If we were quoting bid and got filled: the aggressor is the SELLER.
        If we were quoting ask and got filled: the aggressor is the BUYER.
        """
        with self._lock:
            # Find trades matching this fill (within 2s and 0.5% price)
            for trade in reversed(list(self._recent_trades)):
                if abs(trade.timestamp - fill_time) > 2.0:
                    break
                if trade.coin != coin:
                    continue
                if abs(trade.price - fill_price) / fill_price > 0.005:
                    continue

                # The counterparty who hit OUR quote is the aggressor
                # Our bid got filled → someone sold to us → aggressor = seller
                # Our ask got filled → someone bought from us → aggressor = buyer
                if fill_side == "bid":
                    aggressor = trade.seller
                else:
                    aggressor = trade.buyer

                if not aggressor:
                    continue

                self._ensure_wallet(aggressor)
                ws = self._wallets[aggressor]

                # Update markout stats
                # Convention: THEIR markout (from our perspective as counterparty)
                # If they bought from us (ask fill) and price went up → they profited
                # → adverse for us → markout is negative (adverse)
                ws.markout_sum += markout_5s
                ws.markout_sum_sq += markout_5s ** 2

                # EWMA update
                if ws.trade_count <= 1:
                    ws.ewma_markout = markout_5s
                else:
                    ws.ewma_markout = (
                        self.ewma_alpha * markout_5s
                        + (1 - self.ewma_alpha) * ws.ewma_markout
                    )

                # Update toxicity verdict
                ws.is_toxic = self._is_wallet_toxic(ws)

                # Only attribute to first matching trade
                break

    def is_toxic_active(self, coin: str, lookback_s: float = 10.0) -> tuple[bool, int]:
        """Check if any toxic wallet has been active on this coin recently.

        Returns (is_active, toxic_trade_count_in_window).
        Used for live quoting gate.
        """
        with self._lock:
            activity = self._toxic_activity.get(coin, [])
            if not activity:
                return False, 0

            now = time.time()
            cutoff = now - lookback_s
            recent = [(t, w) for t, w in activity if t > cutoff]

            # Prune old entries
            self._toxic_activity[coin] = recent

            return len(recent) > 0, len(recent)

    def get_wallet_stats(self, address: str) -> Optional[WalletStats]:
        """Get stats for a specific wallet (for analysis/debugging)."""
        with self._lock:
            return self._wallets.get(address)

    def get_toxic_wallets(self, min_confidence: float = 0.7) -> list[WalletStats]:
        """Get all wallets flagged as toxic (for reporting)."""
        with self._lock:
            return [
                ws for ws in self._wallets.values()
                if ws.is_toxic and ws.trade_count >= self.min_trades
            ]

    def get_stats_summary(self) -> dict:
        """Summary stats for logging/monitoring."""
        with self._lock:
            total = len(self._wallets)
            toxic = sum(1 for ws in self._wallets.values() if ws.is_toxic)
            tracked = sum(1 for ws in self._wallets.values() if ws.trade_count >= self.min_trades)
            return {
                "total_wallets": total,
                "tracked_wallets": tracked,
                "toxic_wallets": toxic,
                "total_trades": len(self._recent_trades),
            }

    def to_mongo_docs(self) -> list[dict]:
        """Export wallet scores to MongoDB for analysis."""
        with self._lock:
            return [
                {
                    "address": ws.address,
                    "trade_count": ws.trade_count,
                    "total_notional": ws.total_notional,
                    "ewma_markout": ws.ewma_markout,
                    "is_toxic": ws.is_toxic,
                    "last_trade_time": ws.last_trade_time,
                    "mean_markout": ws.markout_sum / ws.trade_count if ws.trade_count > 0 else 0,
                }
                for ws in self._wallets.values()
                if ws.trade_count >= 5  # only export wallets with some data
            ]

    def _ensure_wallet(self, address: str) -> None:
        """Create wallet stats entry if it doesn't exist.

        Bug #6 fix: Cap wallet dict at 100K entries. If exceeded, evict
        the oldest non-toxic wallets by last_trade_time.
        """
        if address not in self._wallets:
            if len(self._wallets) >= 100_000:
                # Evict oldest non-toxic wallets
                non_toxic = [
                    (ws.last_trade_time, addr)
                    for addr, ws in self._wallets.items()
                    if not ws.is_toxic
                ]
                if non_toxic:
                    non_toxic.sort()
                    # Evict oldest 10%
                    evict_count = max(1, len(non_toxic) // 10)
                    for _, addr in non_toxic[:evict_count]:
                        del self._wallets[addr]
            self._wallets[address] = WalletStats(address=address)

    def _apply_decay(self, ws: WalletStats) -> float:
        """Bug #10 fix: Apply time-based decay to EWMA markout score.

        Returns the decayed EWMA. Decay pulls the score toward 0 (neutral)
        with a half-life of decay_halflife_s (default 24h).
        """
        if ws.last_trade_time <= 0 or self.decay_halflife_s <= 0:
            return ws.ewma_markout
        elapsed = time.time() - ws.last_trade_time
        if elapsed <= 0:
            return ws.ewma_markout
        decay_factor = math.pow(0.5, elapsed / self.decay_halflife_s)
        return ws.ewma_markout * decay_factor

    def _is_wallet_toxic(self, ws: WalletStats) -> bool:
        """Determine if a wallet is toxic based on accumulated evidence.

        Requires:
        1. Minimum trade count (default 20)
        2. Minimum notional volume (default $500)
        3. EWMA markout below threshold (default -1.5bps) after decay
        4. Mean markout with confidence: lower bound of 95% CI < threshold
        """
        if ws.trade_count < self.min_trades:
            return False
        if ws.total_notional < self.min_notional:
            return False

        # Bug #10 fix: Apply time-based decay before checking threshold
        decayed_ewma = self._apply_decay(ws)

        # EWMA check (fast, responsive to recent behavior)
        if decayed_ewma >= self.toxic_threshold_bps:
            return False

        # Statistical check: mean markout with confidence interval
        mean = ws.markout_sum / ws.trade_count
        if ws.trade_count >= 2:
            variance = (ws.markout_sum_sq / ws.trade_count) - mean ** 2
            if variance > 0:
                stderr = math.sqrt(variance / ws.trade_count)
                lower_bound = mean - 1.96 * stderr  # 95% CI lower bound
                if lower_bound >= self.toxic_threshold_bps:
                    return False  # not enough confidence

        return mean < self.toxic_threshold_bps

    # ------------------------------------------------------------------
    # V3: Metaorder Detection
    # ------------------------------------------------------------------

    def detect_metaorders(self, coin: str) -> None:
        """Scan recent trades for active metaorder patterns on a single coin.

        A metaorder is: same wallet, same direction, >= 3 clips in <= 90s,
        with regular cadence (CV < 0.6) and meaningful notional (>= $200).

        Called every tick. Updates self._active_metaorders[coin].

        For batch scanning across many coins, use detect_metaorders_batch()
        which iterates the deque once instead of N times.
        """
        with self._lock:
            now = time.time()
            cutoff = now - 90.0

            # Group recent trades by (aggressor, direction) for this coin only
            wallet_clips: dict[tuple[str, str], list[WalletTrade]] = {}
            for i in range(len(self._recent_trades) - 1, -1, -1):
                trade = self._recent_trades[i]
                if trade.timestamp < cutoff:
                    break
                if trade.coin != coin:
                    continue
                aggressor = trade.buyer if trade.side == "B" else trade.seller
                direction = "buy" if trade.side == "B" else "sell"
                key = (aggressor, direction)
                if key not in wallet_clips:
                    wallet_clips[key] = []
                wallet_clips[key].append(trade)

            best_signal = self._evaluate_clips(coin, wallet_clips)

            # Bug #7 fix: Log metaorder detection so dry runs show signal activity
            prev_signal = self._active_metaorders.get(coin)
            if best_signal and not prev_signal:
                logger.info(
                    f"[{coin}] METAORDER DETECTED: {best_signal.direction} "
                    f"wallet={best_signal.wallet[:10]}... "
                    f"clips={best_signal.clip_count} "
                    f"notional=${best_signal.total_notional:.0f} "
                    f"interval={best_signal.avg_interval_s:.1f}s "
                    f"cv={best_signal.cv:.2f} "
                    f"conf={best_signal.confidence:.2f}"
                )
            elif prev_signal and not best_signal:
                logger.info(f"[{coin}] METAORDER EXPIRED: was {prev_signal.direction} from {prev_signal.wallet[:10]}...")
            self._active_metaorders[coin] = best_signal

    def detect_metaorders_batch(self, coins: set[str]) -> dict[str, Optional["MetaorderSignal"]]:
        """Scan recent trades for metaorder patterns across ALL coins in one pass.

        Instead of calling detect_metaorders(coin) N times (each iterating the
        full deque), we iterate the deque ONCE and group by coin. This is O(T)
        instead of O(N*T) where T = trades in window, N = monitored coins.

        Returns dict of coin -> MetaorderSignal (or None).
        """
        with self._lock:
            now = time.time()
            cutoff = now - 90.0

            # Single pass: group trades by (coin, aggressor, direction)
            wallet_clips: dict[str, dict[tuple[str, str], list[WalletTrade]]] = {}
            for i in range(len(self._recent_trades) - 1, -1, -1):
                trade = self._recent_trades[i]
                if trade.timestamp < cutoff:
                    break
                if trade.coin not in coins:
                    continue
                aggressor = trade.buyer if trade.side == "B" else trade.seller
                direction = "buy" if trade.side == "B" else "sell"
                if trade.coin not in wallet_clips:
                    wallet_clips[trade.coin] = {}
                key = (aggressor, direction)
                if key not in wallet_clips[trade.coin]:
                    wallet_clips[trade.coin][key] = []
                wallet_clips[trade.coin][key].append(trade)

            # Evaluate each coin's clips
            results: dict[str, Optional[MetaorderSignal]] = {}
            for coin in coins:
                coin_clips = wallet_clips.get(coin, {})
                best_signal = self._evaluate_clips(coin, coin_clips)

                # Logging: only log transitions. Use _batch_prev to avoid
                # false re-logs caused by get_active_metaorder() expiry side-effect
                # clearing _active_metaorders between ticks.
                prev_signal = self._active_metaorders.get(coin)
                if best_signal and not prev_signal:
                    logger.info(
                        f"[{coin}] METAORDER DETECTED: {best_signal.direction} "
                        f"wallet={best_signal.wallet[:10]}... "
                        f"clips={best_signal.clip_count} "
                        f"notional=${best_signal.total_notional:.0f} "
                        f"interval={best_signal.avg_interval_s:.1f}s "
                        f"cv={best_signal.cv:.2f} "
                        f"conf={best_signal.confidence:.2f}"
                    )
                elif prev_signal and not best_signal:
                    logger.info(f"[{coin}] METAORDER EXPIRED: was {prev_signal.direction} from {prev_signal.wallet[:10]}...")
                # Always update stored state (even if same signal continues)
                self._active_metaorders[coin] = best_signal
                results[coin] = best_signal

            return results

    def _evaluate_clips(
        self,
        coin: str,
        wallet_clips: dict[tuple[str, str], list["WalletTrade"]],
    ) -> Optional["MetaorderSignal"]:
        """Evaluate wallet clips for a single coin. Returns best signal or None.

        Extracted from detect_metaorders so it can be shared by both
        single-coin and batch paths.
        """
        best_signal: Optional[MetaorderSignal] = None
        best_confidence = 0.0

        for (wallet, direction), clips in wallet_clips.items():
            if len(clips) < 5:  # was 3 — too loose, caught retail. Real TWAPs have 5+ clips
                continue

            clips.sort(key=lambda t: t.timestamp)

            total_notional = sum(t.notional for t in clips)
            if total_notional < 500.0:  # was $200 — raised, $200 is noise on any coin
                continue

            opp_key = (wallet, "sell" if direction == "buy" else "buy")
            opp_clips = wallet_clips.get(opp_key, [])
            opp_notional = sum(t.notional for t in opp_clips)
            if opp_notional > 0:
                same_share = total_notional / (total_notional + opp_notional)
                if same_share < 0.80:
                    continue
            else:
                same_share = 1.0

            intervals = [
                clips[i + 1].timestamp - clips[i].timestamp
                for i in range(len(clips) - 1)
            ]
            if len(intervals) >= 2:
                mean_interval = sum(intervals) / len(intervals)
                # Minimum average interval: real TWAPs have >= 2s between clips.
                # Sub-second bursts are WS subscription replays or market orders,
                # not algorithmic execution.
                if mean_interval < 2.0:
                    continue
                if mean_interval > 0:
                    std_interval = (
                        sum((iv - mean_interval) ** 2 for iv in intervals) / len(intervals)
                    ) ** 0.5
                    cv = std_interval / mean_interval
                else:
                    cv = 999.0
            else:
                cv = 0.0

            clip_conf = min(1.0, len(clips) / 5.0)
            notional_conf = min(1.0, total_notional / 5000.0)
            regularity_conf = max(0.0, 1.0 - cv) if cv < 1.0 else 0.0
            confidence = clip_conf * 0.4 + notional_conf * 0.3 + regularity_conf * 0.3

            max_gap = max(intervals) if intervals else 0
            if max_gap > 45.0:
                continue

            if confidence > best_confidence and cv < 0.6:
                best_confidence = confidence
                avg_interval = sum(intervals) / len(intervals) if intervals else 30.0
                best_signal = MetaorderSignal(
                    wallet=wallet,
                    coin=coin,
                    direction=direction,
                    clip_count=len(clips),
                    total_notional=total_notional,
                    avg_interval_s=avg_interval,
                    cv=cv,
                    confidence=confidence,
                    first_seen=clips[0].timestamp,
                    last_seen=clips[-1].timestamp,
                )

        return best_signal

    def get_coin_trade_stats(self, coin: str) -> tuple[float, float]:
        """Get median trade size and count for a coin.

        Returns (median_notional, n_trades). Used by orchestrator to compute
        clip_size_ratio = metaorder_avg_clip / median_trade for institutional
        detection.
        """
        with self._lock:
            sizes = self._coin_trade_sizes.get(coin)
            if not sizes or len(sizes) < 10:
                return (0.0, 0)
            sorted_sizes = sorted(sizes)
            median = sorted_sizes[len(sorted_sizes) // 2]
            return (median, len(sizes))

    def get_active_metaorder(self, coin: str) -> Optional["MetaorderSignal"]:
        """Get the currently detected metaorder for a coin (or None).

        Note: expiry check is read-only. Does NOT clear the stored signal
        (that's detect_metaorders/detect_metaorders_batch's job). This avoids
        a bug where the expiry side-effect caused batch detection to re-log
        "DETECTED" every tick for the same ongoing metaorder.
        """
        signal = self._active_metaorders.get(coin)
        if signal is None:
            return None
        # Expire if last clip was > 4x expected interval ago
        # (was 2.5x — too aggressive, cuts off moves early.
        # A whale might pause 30s between TWAP legs. 4x gives more room.)
        now = time.time()
        expiry = signal.last_seen + signal.avg_interval_s * 4.0
        if now > expiry:
            return None
        return signal


@dataclass
class MetaorderSignal:
    """Detected metaorder (TWAP/iceberg) from wallet trade stream."""
    wallet: str
    coin: str
    direction: str           # "buy" or "sell"
    clip_count: int          # number of clips observed
    total_notional: float    # cumulative $ aggressed
    avg_interval_s: float    # average seconds between clips
    cv: float                # coefficient of variation of intervals
    confidence: float        # 0..1 composite confidence score
    first_seen: float        # timestamp of first clip
    last_seen: float         # timestamp of most recent clip
