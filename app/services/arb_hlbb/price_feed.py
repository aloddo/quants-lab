"""
Dual-venue WebSocket price feed: Hyperliquid L2 + Bybit orderbook.

Replaces REST polling with sub-100ms price updates.
Each venue runs in its own thread. Spread snapshots are computed on every update.

HL WS: l2Book channel → best bid/ask per coin
Bybit WS: orderbook.1 channel → best bid/ask per symbol
"""
import asyncio
import json
import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Optional

import websocket

logger = logging.getLogger(__name__)


# ── Data Types ────────────────────────────────────────────────────

@dataclass
class BookLevel:
    """Single price level."""
    px: float = 0.0
    sz: float = 0.0


@dataclass
class VenueQuote:
    """Best bid/ask from a venue with optional depth."""
    bid: float = 0.0
    ask: float = 0.0
    bid_sz: float = 0.0  # Size at best bid
    ask_sz: float = 0.0  # Size at best ask
    bids: list = None     # Full bid levels [BookLevel, ...] (deepest available)
    asks: list = None     # Full ask levels [BookLevel, ...]
    ts: float = 0.0

    def __post_init__(self):
        if self.bids is None:
            self.bids = []
        if self.asks is None:
            self.asks = []

    @property
    def mid(self) -> float:
        if self.bid > 0 and self.ask > 0:
            return (self.bid + self.ask) / 2
        return self.bid or self.ask

    @property
    def valid(self) -> bool:
        return self.bid > 0 and self.ask > 0 and self.ask >= self.bid

    def vwap_sell(self, qty: float) -> float:
        """VWAP price for selling `qty` into bids (hitting bids = selling)."""
        if qty <= 0 or not self.bids:
            return self.bid
        remaining = qty
        total_notional = 0.0
        for level in self.bids:
            fill = min(remaining, level.sz)
            total_notional += fill * level.px
            remaining -= fill
            if remaining <= 0:
                break
        filled = qty - remaining
        return total_notional / filled if filled > 0 else self.bid

    def vwap_buy(self, qty: float) -> float:
        """VWAP price for buying `qty` from asks (lifting asks = buying)."""
        if qty <= 0 or not self.asks:
            return self.ask
        remaining = qty
        total_notional = 0.0
        for level in self.asks:
            fill = min(remaining, level.sz)
            total_notional += fill * level.px
            remaining -= fill
            if remaining <= 0:
                break
        filled = qty - remaining
        return total_notional / filled if filled > 0 else self.ask

    def total_bid_depth(self) -> float:
        """Total size across all bid levels."""
        return sum(l.sz for l in self.bids) if self.bids else self.bid_sz

    def total_ask_depth(self) -> float:
        """Total size across all ask levels."""
        return sum(l.sz for l in self.asks) if self.asks else self.ask_sz


@dataclass
class SpreadSnapshot:
    """Cross-venue spread at a point in time."""
    pair: str
    hl_bid: float
    hl_ask: float
    bb_bid: float
    bb_ask: float
    spread_hl_over_bb_bps: float  # (hl_bid - bb_ask) / bb_ask * 10000
    spread_bb_over_hl_bps: float  # (bb_bid - hl_ask) / hl_ask * 10000
    best_spread_bps: float        # max of the two (positive = arb exists)
    direction: str                # "HL_PREMIUM" or "BB_PREMIUM"
    ts: float
    # Depth: size available at best bid/ask (0 if not available)
    hl_bid_sz: float = 0.0
    hl_ask_sz: float = 0.0
    bb_bid_sz: float = 0.0
    bb_ask_sz: float = 0.0
    # VWAP spread at target size (0 if not computed)
    vwap_spread_bps: float = 0.0
    vwap_qty: float = 0.0  # qty used for VWAP calc

    @property
    def entry_spread(self) -> float:
        """The executable spread (crossing the book)."""
        return self.best_spread_bps

    def min_executable_qty(self) -> float:
        """Minimum qty available across both legs for the arb direction.

        For HL_PREMIUM (short HL, long BB): need hl_bid_sz and bb_ask_sz.
        For BB_PREMIUM (short BB, long HL): need bb_bid_sz and hl_ask_sz.
        Returns 0 if NEITHER side has depth info (unknown).
        Returns the minimum of both sides if BOTH have depth.
        If only one side has depth, returns that (conservative — we know at least
        one side is limited, and the other is unknown).
        """
        if self.direction == "HL_PREMIUM":
            a, b = self.hl_bid_sz, self.bb_ask_sz
        else:
            a, b = self.bb_bid_sz, self.hl_ask_sz

        if a > 0 and b > 0:
            return min(a, b)
        elif a > 0:
            return a  # Only one side known — use it as ceiling
        elif b > 0:
            return b
        return 0.0  # Neither side has depth info


# ── Hyperliquid WS Feed ──────────────────────────────────────────

class HLPriceFeed:
    """Subscribe to HL l2Book for real-time best bid/ask."""

    def __init__(self, coins: list[str], ws_url: str = "wss://api.hyperliquid.xyz/ws"):
        self.coins = coins
        self.ws_url = ws_url
        self.quotes: dict[str, VenueQuote] = defaultdict(VenueQuote)
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._connected = threading.Event()
        self._running = False
        self._lock = threading.Lock()
        self._reconnect_count = 0
        self._update_count = 0
        self._on_update: Optional[Callable] = None

    def start(self, on_update: Optional[Callable] = None) -> bool:
        """Start WS connection. on_update(coin) called on each price change."""
        self._on_update = on_update
        self._running = True
        return self._connect()

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        self._connected.clear()

    def get_quote(self, coin: str) -> VenueQuote:
        with self._lock:
            q = self.quotes[coin]
            return VenueQuote(
                bid=q.bid, ask=q.ask,
                bid_sz=q.bid_sz, ask_sz=q.ask_sz,
                bids=list(q.bids), asks=list(q.asks),
                ts=q.ts,
            )

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    def _connect(self) -> bool:
        try:
            self._ws = websocket.WebSocketApp(
                self.ws_url,
                on_message=self._on_message,
                on_open=self._on_open,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._connected.clear()
            self._thread = threading.Thread(
                target=self._ws.run_forever,
                kwargs={"ping_interval": 0},
                daemon=True,
            )
            self._thread.start()

            if not self._connected.wait(timeout=10):
                logger.error("HL WS: connection timeout")
                return False

            logger.info(f"HL WS connected, subscribing to {len(self.coins)} coins")
            return True

        except Exception as e:
            logger.error(f"HL WS connect failed: {e}")
            return False

    def _on_open(self, ws):
        self._connected.set()
        # Subscribe to l2Book for each coin
        for coin in self.coins:
            msg = {
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": coin}
            }
            ws.send(json.dumps(msg))

        # Start keepalive
        threading.Thread(target=self._keepalive, daemon=True).start()

    def _keepalive(self):
        while self._running and self._connected.is_set():
            time.sleep(30)
            if self._ws and self._connected.is_set():
                try:
                    self._ws.send(json.dumps({"method": "ping"}))
                except Exception:
                    pass

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            channel = data.get("channel")

            if channel == "l2Book":
                book_data = data.get("data", {})
                coin = book_data.get("coin", "")
                levels = book_data.get("levels", [])

                if len(levels) >= 2 and levels[0] and levels[1]:
                    best_bid = float(levels[0][0].get("px", 0))
                    best_ask = float(levels[1][0].get("px", 0))
                    bid_sz = float(levels[0][0].get("sz", 0))
                    ask_sz = float(levels[1][0].get("sz", 0))

                    # Parse full depth
                    bid_levels = [
                        BookLevel(px=float(l.get("px", 0)), sz=float(l.get("sz", 0)))
                        for l in levels[0][:20]  # top 20 levels
                    ]
                    ask_levels = [
                        BookLevel(px=float(l.get("px", 0)), sz=float(l.get("sz", 0)))
                        for l in levels[1][:20]
                    ]

                    with self._lock:
                        self.quotes[coin] = VenueQuote(
                            bid=best_bid, ask=best_ask,
                            bid_sz=bid_sz, ask_sz=ask_sz,
                            bids=bid_levels, asks=ask_levels,
                            ts=time.time(),
                        )
                    self._update_count += 1

                    if self._on_update:
                        self._on_update(coin)

        except Exception as e:
            logger.debug(f"HL WS parse error: {e}")

    def _on_error(self, ws, error):
        logger.warning(f"HL WS error: {error}")

    def _on_close(self, ws, code, msg):
        self._connected.clear()
        logger.warning(f"HL WS disconnected: {code}")
        if self._running:
            self._reconnect_count += 1
            backoff = min(30.0, 2.0 ** min(self._reconnect_count, 5))
            logger.info(f"HL WS reconnecting in {backoff:.0f}s...")
            threading.Thread(
                target=self._reconnect, args=(backoff,), daemon=True
            ).start()

    def _reconnect(self, backoff: float):
        time.sleep(backoff)
        if self._running and not self.is_connected:
            if self._connect():
                self._reconnect_count = 0

    def subscribe(self, coin: str):
        """Dynamically subscribe to a new coin."""
        if coin not in self.coins:
            self.coins.append(coin)
        if self._ws and self._connected.is_set():
            msg = {
                "method": "subscribe",
                "subscription": {"type": "l2Book", "coin": coin}
            }
            self._ws.send(json.dumps(msg))

    def unsubscribe(self, coin: str):
        """Dynamically unsubscribe from a coin."""
        if self._ws and self._connected.is_set():
            msg = {
                "method": "unsubscribe",
                "subscription": {"type": "l2Book", "coin": coin}
            }
            self._ws.send(json.dumps(msg))
        if coin in self.coins:
            self.coins.remove(coin)


# ── Bybit WS Feed ────────────────────────────────────────────────

class BybitPriceFeed:
    """Subscribe to Bybit linear orderbook for real-time best bid/ask."""

    def __init__(self, symbols: list[str],
                 ws_url: str = "wss://stream.bybit.com/v5/public/linear"):
        self.symbols = symbols
        self.ws_url = ws_url
        self.quotes: dict[str, VenueQuote] = defaultdict(VenueQuote)
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._connected = threading.Event()
        self._running = False
        self._lock = threading.Lock()
        self._reconnect_count = 0
        self._update_count = 0
        self._on_update: Optional[Callable] = None

    def start(self, on_update: Optional[Callable] = None) -> bool:
        """Start WS connection."""
        self._on_update = on_update
        self._running = True
        return self._connect()

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        self._connected.clear()

    def get_quote(self, symbol: str) -> VenueQuote:
        with self._lock:
            q = self.quotes[symbol]
            return VenueQuote(
                bid=q.bid, ask=q.ask,
                bid_sz=q.bid_sz, ask_sz=q.ask_sz,
                bids=list(q.bids), asks=list(q.asks),
                ts=q.ts,
            )

    @property
    def is_connected(self) -> bool:
        return self._connected.is_set()

    def _connect(self) -> bool:
        try:
            self._ws = websocket.WebSocketApp(
                self.ws_url,
                on_message=self._on_message,
                on_open=self._on_open,
                on_error=self._on_error,
                on_close=self._on_close,
            )
            self._connected.clear()
            self._thread = threading.Thread(
                target=self._ws.run_forever,
                kwargs={"ping_interval": 20, "ping_timeout": 10},
                daemon=True,
            )
            self._thread.start()

            if not self._connected.wait(timeout=10):
                logger.error("Bybit WS: connection timeout")
                return False

            logger.info(f"Bybit WS connected, subscribing to {len(self.symbols)} symbols")
            return True

        except Exception as e:
            logger.error(f"Bybit WS connect failed: {e}")
            return False

    def _on_open(self, ws):
        self._connected.set()
        # Subscribe to orderbook depth 50 for VWAP
        # Bybit valid depths: 1, 50, 200, 500. Using 50 as minimum for multi-level VWAP.
        for i in range(0, len(self.symbols), 10):
            batch = self.symbols[i:i+10]
            args = [f"orderbook.50.{sym}" for sym in batch]
            msg = {"op": "subscribe", "args": args}
            ws.send(json.dumps(msg))

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            topic = data.get("topic", "")

            if "orderbook." in topic:
                # Handle orderbook.1.SYM, orderbook.50.SYM, etc.
                parts = topic.split(".")
                symbol = parts[-1] if len(parts) >= 3 else ""
                msg_type = data.get("type", "snapshot")
                ob_data = data.get("data", {})
                bids = ob_data.get("b", [])
                asks = ob_data.get("a", [])

                # For snapshots, require both sides. For deltas, accept partial.
                if msg_type == "snapshot" and bids and asks or \
                   msg_type == "delta" and (bids or asks):
                    best_bid = float(bids[0][0]) if bids else 0.0
                    best_ask = float(asks[0][0]) if asks else 0.0
                    bid_sz = float(bids[0][1]) if bids and len(bids[0]) > 1 else 0.0
                    ask_sz = float(asks[0][1]) if asks and len(asks[0]) > 1 else 0.0

                    # Parse full depth for VWAP
                    bid_levels = [
                        BookLevel(px=float(b[0]), sz=float(b[1]))
                        for b in bids if len(b) >= 2
                    ]
                    ask_levels = [
                        BookLevel(px=float(a[0]), sz=float(a[1]))
                        for a in asks if len(a) >= 2
                    ]

                    with self._lock:
                        q = self.quotes[symbol]
                        if msg_type == "snapshot":
                            # Full replace
                            q.bid = best_bid
                            q.ask = best_ask
                            q.bid_sz = bid_sz
                            q.ask_sz = ask_sz
                            q.bids = bid_levels
                            q.asks = ask_levels
                        else:
                            # Delta: update sides that have data
                            if best_bid > 0:
                                q.bid = best_bid
                                q.bid_sz = bid_sz
                                q.bids = bid_levels if bid_levels else q.bids
                            if best_ask > 0:
                                q.ask = best_ask
                                q.ask_sz = ask_sz
                                q.asks = ask_levels if ask_levels else q.asks
                        q.ts = time.time()
                        self.quotes[symbol] = q

                    self._update_count += 1

                    if self._on_update:
                        self._on_update(symbol)

        except Exception as e:
            logger.debug(f"Bybit WS parse error: {e}")

    def _on_error(self, ws, error):
        logger.warning(f"Bybit WS error: {error}")

    def _on_close(self, ws, code, msg):
        self._connected.clear()
        logger.warning(f"Bybit WS disconnected: {code}")
        if self._running:
            self._reconnect_count += 1
            backoff = min(30.0, 2.0 ** min(self._reconnect_count, 5))
            threading.Thread(
                target=self._reconnect, args=(backoff,), daemon=True
            ).start()

    def _reconnect(self, backoff: float):
        time.sleep(backoff)
        if self._running and not self.is_connected:
            if self._connect():
                self._reconnect_count = 0


# ── Combined Spread Calculator ────────────────────────────────────

class DualPriceFeed:
    """Combines HL + Bybit feeds and computes cross-venue spreads."""

    def __init__(self, pairs: list[str],
                 hl_ws_url: str = "wss://api.hyperliquid.xyz/ws",
                 bb_ws_url: str = "wss://stream.bybit.com/v5/public/linear"):
        self.pairs = pairs
        self._pair_to_coin = {}   # "APE-USDT" → "APE"
        self._pair_to_sym = {}    # "APE-USDT" → "APEUSDT"
        self._coin_to_pair = {}   # "APE" → "APE-USDT"
        self._sym_to_pair = {}    # "APEUSDT" → "APE-USDT"

        coins = []
        symbols = []
        for p in pairs:
            coin = p.replace("-USDT", "")
            sym = p.replace("-", "")
            self._pair_to_coin[p] = coin
            self._pair_to_sym[p] = sym
            self._coin_to_pair[coin] = p
            self._sym_to_pair[sym] = p
            coins.append(coin)
            symbols.append(sym)

        self.hl_feed = HLPriceFeed(coins, hl_ws_url)
        self.bb_feed = BybitPriceFeed(symbols, bb_ws_url)

        # Callback for spread updates
        self._on_spread: Optional[Callable[[SpreadSnapshot], None]] = None

        # VWAP target qty (in base units) — set by orchestrator based on position_usd / price
        self._vwap_target_qty: float = 0.0

        # Stats
        self._spread_count = 0
        self._last_spreads: dict[str, SpreadSnapshot] = {}

    def start(self, on_spread: Optional[Callable[[SpreadSnapshot], None]] = None) -> bool:
        """Start both feeds. on_spread called on every price update with fresh spread."""
        self._on_spread = on_spread

        hl_ok = self.hl_feed.start(on_update=self._on_hl_update)
        bb_ok = self.bb_feed.start(on_update=self._on_bb_update)

        if not hl_ok:
            logger.error("Failed to start HL price feed")
        if not bb_ok:
            logger.error("Failed to start Bybit price feed")

        return hl_ok and bb_ok

    def stop(self):
        self.hl_feed.stop()
        self.bb_feed.stop()

    def _on_hl_update(self, coin: str):
        pair = self._coin_to_pair.get(coin)
        if pair:
            self._compute_and_emit(pair)

    def _on_bb_update(self, symbol: str):
        pair = self._sym_to_pair.get(symbol)
        if pair:
            self._compute_and_emit(pair)

    def _compute_and_emit(self, pair: str):
        """Compute spread and emit snapshot."""
        coin = self._pair_to_coin[pair]
        sym = self._pair_to_sym[pair]

        hl_q = self.hl_feed.get_quote(coin)
        bb_q = self.bb_feed.get_quote(sym)

        if not hl_q.valid or not bb_q.valid:
            return

        # Staleness check: reject if either quote is >5s old
        now = time.time()
        if now - hl_q.ts > 5.0 or now - bb_q.ts > 5.0:
            return

        # Executable spreads (crossing the book)
        # HL premium: sell HL bid, buy BB ask
        spread_hl = (hl_q.bid - bb_q.ask) / bb_q.ask * 10000
        # BB premium: sell BB bid, buy HL ask
        spread_bb = (bb_q.bid - hl_q.ask) / hl_q.ask * 10000

        best = max(spread_hl, spread_bb)
        direction = "HL_PREMIUM" if spread_hl >= spread_bb else "BB_PREMIUM"

        # Compute VWAP spread at target size if depth available
        vwap_spread = 0.0
        vwap_qty = self._vwap_target_qty
        if vwap_qty > 0:
            if direction == "HL_PREMIUM":
                # Sell HL bids, buy BB asks
                hl_vwap_sell = hl_q.vwap_sell(vwap_qty)
                bb_vwap_buy = bb_q.vwap_buy(vwap_qty)
                if bb_vwap_buy > 0:
                    vwap_spread = (hl_vwap_sell - bb_vwap_buy) / bb_vwap_buy * 10000
            else:
                # Sell BB bids, buy HL asks
                bb_vwap_sell = bb_q.vwap_sell(vwap_qty)
                hl_vwap_buy = hl_q.vwap_buy(vwap_qty)
                if hl_vwap_buy > 0:
                    vwap_spread = (bb_vwap_sell - hl_vwap_buy) / hl_vwap_buy * 10000

        snap = SpreadSnapshot(
            pair=pair,
            hl_bid=hl_q.bid, hl_ask=hl_q.ask,
            bb_bid=bb_q.bid, bb_ask=bb_q.ask,
            spread_hl_over_bb_bps=spread_hl,
            spread_bb_over_hl_bps=spread_bb,
            best_spread_bps=best,
            direction=direction,
            ts=now,
            hl_bid_sz=hl_q.bid_sz, hl_ask_sz=hl_q.ask_sz,
            bb_bid_sz=bb_q.bid_sz, bb_ask_sz=bb_q.ask_sz,
            vwap_spread_bps=vwap_spread,
            vwap_qty=vwap_qty,
        )

        self._spread_count += 1
        self._last_spreads[pair] = snap

        if self._on_spread:
            self._on_spread(snap)

    def get_spread(self, pair: str) -> Optional[SpreadSnapshot]:
        """Get latest spread for a pair."""
        return self._last_spreads.get(pair)

    def get_all_spreads(self) -> dict[str, SpreadSnapshot]:
        """Get all latest spreads."""
        return dict(self._last_spreads)

    @property
    def is_healthy(self) -> bool:
        return self.hl_feed.is_connected and self.bb_feed.is_connected

    def get_metrics(self) -> dict:
        return {
            "hl_connected": self.hl_feed.is_connected,
            "bb_connected": self.bb_feed.is_connected,
            "hl_updates": self.hl_feed._update_count,
            "bb_updates": self.bb_feed._update_count,
            "spread_computations": self._spread_count,
            "pairs_with_data": len(self._last_spreads),
        }
