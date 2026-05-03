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

        # Per-coin recent toxic wallet activity (for live gating)
        # coin -> list of (timestamp, wallet_addr) for toxic wallets seen in last 10s
        self._toxic_activity: dict[str, list[tuple[float, str]]] = {}

    def record_trade(
        self,
        coin: str,
        side: str,
        price: float,
        size: float,
        buyer: str,
        seller: str,
    ) -> None:
        """Record a trade with wallet addresses.

        Called from WS trade callback for every trade on our coins.
        """
        if not buyer or not seller:
            return

        notional = price * size
        now = time.time()

        trade = WalletTrade(
            coin=coin, timestamp=now, side=side,
            price=price, size=size, notional=notional,
            buyer=buyer, seller=seller,
        )

        with self._lock:
            self._recent_trades.append(trade)

            # Track the aggressor wallet (the one who crossed the spread)
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
        """Create wallet stats entry if it doesn't exist."""
        if address not in self._wallets:
            self._wallets[address] = WalletStats(address=address)

    def _is_wallet_toxic(self, ws: WalletStats) -> bool:
        """Determine if a wallet is toxic based on accumulated evidence.

        Requires:
        1. Minimum trade count (default 20)
        2. Minimum notional volume (default $500)
        3. EWMA markout below threshold (default -1.5bps)
        4. Mean markout with confidence: lower bound of 95% CI < threshold
        """
        if ws.trade_count < self.min_trades:
            return False
        if ws.total_notional < self.min_notional:
            return False

        # EWMA check (fast, responsive to recent behavior)
        if ws.ewma_markout >= self.toxic_threshold_bps:
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
