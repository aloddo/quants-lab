"""
H2 PriceFeed — Real-time best bid/ask from Bybit perp + Binance spot via WebSocket.

Primary price source for the H2 execution engine. Replaces the 5s REST polling
in the paper trader with event-driven WS feeds (~100ms latency).

Each venue runs its own WS connection with independent reconnect logic.
The FreshnessGuard rejects stale/skewed prices before any trading decision.
"""
import asyncio
import hashlib
import hmac
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Callable

import aiohttp

logger = logging.getLogger(__name__)


# ── Data types ──────────────────────────────────────────────────

@dataclass
class VenuePrice:
    """Best bid/ask from a single venue."""
    symbol: str
    best_bid: float = 0.0
    best_ask: float = 0.0
    bid_qty: float = 0.0
    ask_qty: float = 0.0
    last_update_ts: float = 0.0  # local monotonic time of last WS message
    venue_ts_ms: int = 0         # exchange-reported timestamp (ms)
    update_count: int = 0
    _prev_bid: float = 0.0       # for staleness detection
    _prev_ask: float = 0.0
    _unchanged_count: int = 0


@dataclass
class SpreadSnapshot:
    """Executable spread between two venues at a point in time."""
    symbol: str
    spread_bps: float           # actual executable spread (bn_bid - bb_ask)
    bb_ask: float               # what we'd PAY on Bybit (buy perp)
    bn_bid: float               # what we'd GET on Binance (sell spot)
    bb_bid: float               # for reverse direction
    bn_ask: float               # for reverse direction
    reverse_spread_bps: float   # (bb_bid - bn_ask) for BUY_BN_SELL_BB
    direction: str              # which direction has better spread
    bb_age_ms: float
    bn_age_ms: float
    timestamp: float
    fresh: bool = True          # passed all freshness checks


# ── Freshness Guard ─────────────────────────────────────────────

class FreshnessGuard:
    """Reject stale, skewed, or suspicious price data."""

    MAX_AGE_S = 5.0              # max age for any single venue price (USDC pairs update slower)
    MAX_SKEW_S = 3.0             # max age difference between venues
    MAX_UNCHANGED_TICKS = 10     # unchanged price for 10 WS updates = suspicious
    MAX_SPREAD_MULTIPLE = 5.0    # reject spreads > 5x rolling median

    def __init__(self):
        self._median_spreads: dict[str, float] = {}  # symbol -> rolling median

    def update_median(self, symbol: str, spread_bps: float):
        """Exponential moving average of absolute spread."""
        alpha = 0.05
        prev = self._median_spreads.get(symbol, abs(spread_bps))
        self._median_spreads[symbol] = prev * (1 - alpha) + abs(spread_bps) * alpha

    def check(self, bb: VenuePrice, bn: VenuePrice) -> tuple[bool, str]:
        """
        Returns (is_tradeable, reason).
        All checks must pass for is_tradeable=True.
        """
        now = time.monotonic()
        bb_age = now - bb.last_update_ts
        bn_age = now - bn.last_update_ts

        if bb.last_update_ts == 0 or bn.last_update_ts == 0:
            return False, "no_data"

        if bb_age > self.MAX_AGE_S:
            return False, f"bb_stale_{bb_age:.1f}s"
        if bn_age > self.MAX_AGE_S:
            return False, f"bn_stale_{bn_age:.1f}s"

        if abs(bb_age - bn_age) > self.MAX_SKEW_S:
            return False, f"skew_{abs(bb_age - bn_age):.1f}s"

        if bb._unchanged_count > self.MAX_UNCHANGED_TICKS:
            return False, f"bb_frozen_{bb._unchanged_count}_ticks"
        if bn._unchanged_count > self.MAX_UNCHANGED_TICKS:
            return False, f"bn_frozen_{bn._unchanged_count}_ticks"

        # Spread sanity check
        if bb.best_ask > 0 and bn.best_bid > 0:
            spread = abs(bn.best_bid - bb.best_ask) / bb.best_ask * 10000
            median = self._median_spreads.get(bb.symbol, spread)
            if median > 0 and spread > median * self.MAX_SPREAD_MULTIPLE:
                return False, f"phantom_spike_{spread:.0f}bp_vs_median_{median:.0f}bp"

        return True, "ok"


# ── Bybit WebSocket ─────────────────────────────────────────────

class BybitPriceFeed:
    """Public linear tickers via Bybit WebSocket."""

    WS_URL = "wss://stream.bybit.com/v5/public/linear"
    PING_INTERVAL = 20

    def __init__(self, symbols: list[str]):
        self.symbols = symbols
        self.prices: dict[str, VenuePrice] = {s: VenuePrice(symbol=s) for s in symbols}
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None
        self._running = False
        self._reconnect_count = 0

    async def start(self, session: aiohttp.ClientSession):
        self._session = session
        self._running = True
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                self._reconnect_count += 1
                wait = min(2 ** self._reconnect_count, 30)
                logger.warning(f"Bybit WS error: {e}. Reconnect #{self._reconnect_count} in {wait}s")
                await asyncio.sleep(wait)

    async def _connect_and_listen(self):
        logger.info(f"Bybit WS connecting to {self.WS_URL}...")
        async with self._session.ws_connect(self.WS_URL, heartbeat=self.PING_INTERVAL) as ws:
            self._ws = ws
            self._reconnect_count = 0

            # Subscribe to tickers for all symbols
            args = [f"tickers.{s}" for s in self.symbols]
            await ws.send_json({"op": "subscribe", "args": args})
            logger.info(f"Bybit WS subscribed to {len(args)} tickers")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._handle_message(json.loads(msg.data))
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                    logger.warning(f"Bybit WS closed: {msg.type}")
                    break

    def _handle_message(self, data: dict):
        topic = data.get("topic", "")
        if not topic.startswith("tickers."):
            return

        d = data.get("data", {})
        symbol = d.get("symbol", "")
        if symbol not in self.prices:
            return

        p = self.prices[symbol]

        bid = float(d.get("bid1Price", 0) or 0)
        ask = float(d.get("ask1Price", 0) or 0)

        if bid > 0:
            # Staleness detection
            if bid == p._prev_bid and ask == p._prev_ask:
                p._unchanged_count += 1
            else:
                p._unchanged_count = 0
                p._prev_bid = bid
                p._prev_ask = ask

            p.best_bid = bid
            p.best_ask = ask
            p.bid_qty = float(d.get("bid1Size", 0) or 0)
            p.ask_qty = float(d.get("ask1Size", 0) or 0)
            p.last_update_ts = time.monotonic()
            p.venue_ts_ms = int(d.get("ts", 0))
            p.update_count += 1

    async def stop(self):
        self._running = False
        if self._ws and not self._ws.closed:
            await self._ws.close()


# ── Binance WebSocket ───────────────────────────────────────────

class BinancePriceFeed:
    """Public bookTicker via Binance WebSocket (combined stream)."""

    WS_BASE = "wss://stream.binance.com:9443/stream"
    PING_INTERVAL = 20

    def __init__(self, symbols: list[str]):
        self.symbols = symbols
        self.prices: dict[str, VenuePrice] = {s: VenuePrice(symbol=s) for s in symbols}
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._session: aiohttp.ClientSession | None = None
        self._running = False
        self._reconnect_count = 0

    @property
    def _ws_url(self) -> str:
        streams = "/".join(f"{s.lower()}@bookTicker" for s in self.symbols)
        return f"{self.WS_BASE}?streams={streams}"

    async def start(self, session: aiohttp.ClientSession):
        self._session = session
        self._running = True
        while self._running:
            try:
                await self._connect_and_listen()
            except Exception as e:
                self._reconnect_count += 1
                wait = min(2 ** self._reconnect_count, 30)
                logger.warning(f"Binance WS error: {e}. Reconnect #{self._reconnect_count} in {wait}s")
                await asyncio.sleep(wait)

    async def _connect_and_listen(self):
        url = self._ws_url
        logger.info(f"Binance WS connecting ({len(self.symbols)} streams)...")
        async with self._session.ws_connect(url, heartbeat=self.PING_INTERVAL) as ws:
            self._ws = ws
            self._reconnect_count = 0
            logger.info(f"Binance WS connected")

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    self._handle_message(json.loads(msg.data))
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                    logger.warning(f"Binance WS closed: {msg.type}")
                    break

    def _handle_message(self, data: dict):
        # Combined stream format: {"stream": "btcusdt@bookTicker", "data": {...}}
        d = data.get("data", data)

        symbol = d.get("s", "")
        if symbol not in self.prices:
            return

        p = self.prices[symbol]

        bid = float(d.get("b", 0) or 0)
        ask = float(d.get("a", 0) or 0)

        if bid > 0:
            if bid == p._prev_bid and ask == p._prev_ask:
                p._unchanged_count += 1
            else:
                p._unchanged_count = 0
                p._prev_bid = bid
                p._prev_ask = ask

            p.best_bid = bid
            p.best_ask = ask
            p.bid_qty = float(d.get("B", 0) or 0)
            p.ask_qty = float(d.get("A", 0) or 0)
            p.last_update_ts = time.monotonic()
            p.venue_ts_ms = int(d.get("u", 0))  # updateId, not timestamp
            p.update_count += 1

    async def stop(self):
        self._running = False
        if self._ws and not self._ws.closed:
            await self._ws.close()


# ── Combined PriceFeed ──────────────────────────────────────────

class PriceFeed:
    """
    Combined price feed from both exchanges.

    Manages both WS connections and provides a unified spread interface
    with freshness checking.
    """

    def __init__(self, symbols: list[str], bn_symbol_map: dict[str, str] | None = None):
        """
        Args:
            symbols: internal symbol keys (e.g., ["NOMUSDT", "ACTUSDT"])
            bn_symbol_map: optional mapping from internal key to Binance symbol.
                          e.g., {"NOMUSDT": "NOMUSDC", "ACTUSDT": "ACTUSDC"}
                          If None, assumes Binance uses same symbol as internal key.
        """
        self.symbols = symbols
        self._bn_map = bn_symbol_map or {}  # internal -> Binance WS symbol
        self._bn_reverse = {v: k for k, v in self._bn_map.items()}  # Binance -> internal

        # Bybit always uses USDT symbols (the internal keys)
        self.bybit = BybitPriceFeed(symbols)

        # Binance may use different symbols (e.g., USDC)
        bn_symbols = [self._bn_map.get(s, s) for s in symbols]
        self.binance = BinancePriceFeed(bn_symbols)

        self.guard = FreshnessGuard()
        self._tasks: list[asyncio.Task] = []

    async def start(self, session: aiohttp.ClientSession):
        """Start both WS feeds as background tasks."""
        self._tasks = [
            asyncio.create_task(self.bybit.start(session)),
            asyncio.create_task(self.binance.start(session)),
        ]
        logger.info(f"PriceFeed started for {len(self.symbols)} symbols")

    async def stop(self):
        await self.bybit.stop()
        await self.binance.stop()
        for t in self._tasks:
            t.cancel()

    def get_spread(self, symbol: str) -> SpreadSnapshot | None:
        """
        Returns the executable spread if both venues are fresh and aligned.
        Returns None if freshness checks fail.

        Uses actual bid/ask (not mid) for executable spread calculation:
        - BUY_BB_SELL_BN: buy Bybit ask, sell Binance bid
        - BUY_BN_SELL_BB: buy Binance ask, sell Bybit bid
        """
        bb = self.bybit.prices.get(symbol)
        # Look up Binance price using mapped symbol (e.g., NOMUSDC for NOMUSDT)
        bn_symbol = self._bn_map.get(symbol, symbol)
        bn = self.binance.prices.get(bn_symbol)

        if not bb or not bn:
            return None

        # Freshness check
        fresh, reason = self.guard.check(bb, bn)

        if bb.best_ask <= 0 or bn.best_bid <= 0:
            return None

        # Executable spreads (what you'd actually capture)
        # BUY_BB_SELL_BN: long Bybit perp at ask, sell Binance spot at bid
        spread_buy_bb = (bn.best_bid - bb.best_ask) / bb.best_ask * 10000
        # BUY_BN_SELL_BB: buy Binance spot at ask, short Bybit perp at bid
        spread_buy_bn = (bb.best_bid - bn.best_ask) / bn.best_ask * 10000

        best_spread = max(spread_buy_bb, spread_buy_bn)
        direction = "BUY_BB_SELL_BN" if spread_buy_bb >= spread_buy_bn else "BUY_BN_SELL_BB"

        now = time.monotonic()
        snap = SpreadSnapshot(
            symbol=symbol,
            spread_bps=best_spread,
            bb_ask=bb.best_ask,
            bn_bid=bn.best_bid,
            bb_bid=bb.best_bid,
            bn_ask=bn.best_ask,
            reverse_spread_bps=min(spread_buy_bb, spread_buy_bn),
            direction=direction,
            bb_age_ms=(now - bb.last_update_ts) * 1000,
            bn_age_ms=(now - bn.last_update_ts) * 1000,
            timestamp=time.time(),
            fresh=fresh,
        )

        # Update running median for phantom spike detection
        self.guard.update_median(symbol, best_spread)

        if not fresh:
            logger.debug(f"{symbol} price rejected: {reason}")
            return None  # CRITICAL: do not return stale/skewed prices

        return snap

    def get_spread_for_exit(self, symbol: str, max_age_s: float = 30.0) -> SpreadSnapshot | None:
        """
        Like get_spread but with relaxed freshness for exit decisions.

        Entries need tight freshness (5s) to avoid phantom spreads.
        Exits need to work even with slightly stale data -- it's better
        to exit with a 20-second-old price than be stuck in a position.

        Still rejects: no data, phantom spikes, extreme skew.
        """
        bb = self.bybit.prices.get(symbol)
        bn_symbol = self._bn_map.get(symbol, symbol)
        bn = self.binance.prices.get(bn_symbol)

        if not bb or not bn:
            return None
        if bb.last_update_ts == 0 or bn.last_update_ts == 0:
            return None
        if bb.best_ask <= 0 or bn.best_bid <= 0:
            return None

        now = time.monotonic()
        bb_age = now - bb.last_update_ts
        bn_age = now - bn.last_update_ts

        # Relaxed age check for exits
        if bb_age > max_age_s or bn_age > max_age_s:
            return None

        spread_buy_bb = (bn.best_bid - bb.best_ask) / bb.best_ask * 10000
        spread_buy_bn = (bb.best_bid - bn.best_ask) / bn.best_ask * 10000
        best_spread = max(spread_buy_bb, spread_buy_bn)
        direction = "BUY_BB_SELL_BN" if spread_buy_bb >= spread_buy_bn else "BUY_BN_SELL_BB"

        # Still reject phantom spikes
        median = self.guard._median_spreads.get(symbol, abs(best_spread))
        if median > 0 and abs(best_spread) > median * self.guard.MAX_SPREAD_MULTIPLE:
            return None

        return SpreadSnapshot(
            symbol=symbol,
            spread_bps=best_spread,
            bb_ask=bb.best_ask,
            bn_bid=bn.best_bid,
            bb_bid=bb.best_bid,
            bn_ask=bn.best_ask,
            reverse_spread_bps=min(spread_buy_bb, spread_buy_bn),
            direction=direction,
            bb_age_ms=bb_age * 1000,
            bn_age_ms=bn_age * 1000,
            timestamp=time.time(),
            fresh=(bb_age < self.guard.MAX_AGE_S and bn_age < self.guard.MAX_AGE_S),
        )

    def status(self) -> dict:
        """Return status of all feeds for monitoring."""
        result = {}
        for sym in self.symbols:
            bb = self.bybit.prices[sym]
            bn_sym = self._bn_map.get(sym, sym)
            bn = self.binance.prices.get(bn_sym, VenuePrice(symbol=bn_sym))
            now = time.monotonic()
            result[sym] = {
                "bb_bid": bb.best_bid,
                "bb_ask": bb.best_ask,
                "bb_age_ms": (now - bb.last_update_ts) * 1000 if bb.last_update_ts else -1,
                "bb_updates": bb.update_count,
                "bn_bid": bn.best_bid,
                "bn_ask": bn.best_ask,
                "bn_age_ms": (now - bn.last_update_ts) * 1000 if bn.last_update_ts else -1,
                "bn_updates": bn.update_count,
            }
        return result
