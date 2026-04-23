"""
UniverseManager — Dynamic WS connection management for H2 V3.

Manages which pairs have active WS connections based on TierEngine output.
Handles Bybit dynamic subscribe/unsubscribe and Binance hot-swap reconnect.

Key design decisions:
- WS universe refreshes every 4h (connection lifecycle is expensive)
- Tier tradeability recomputes every 5min (cheap, in-memory)
- Hysteresis: pair must drop below Tier C for 2 consecutive refresh cycles
  AND not have had an open position in the last 2h before disconnect
- Binance hot-swap: start new connection, wait for 80% of kept pairs to tick,
  then atomically switch. Abort if timeout.
"""
import asyncio
import logging
import time
from dataclasses import dataclass, field

import aiohttp

from app.services.arb.price_feed import (
    PriceFeed,
    BybitPriceFeed,
    BinancePriceFeed,
    VenuePrice,
)
from app.services.arb.tier_engine import TierEngine, TierInfo

logger = logging.getLogger(__name__)

UNIVERSE_REFRESH_INTERVAL = 4 * 3600  # 4 hours
MAX_WS_SYMBOLS = 30
BINANCE_SWAP_TIMEOUT = 30.0  # seconds to wait for new WS to deliver data
BINANCE_SWAP_COVERAGE = 0.80  # require 80% of kept pairs to tick
HYSTERESIS_CYCLES = 2  # must be below threshold for N consecutive cycles
POSITION_COOLDOWN_HOURS = 2  # don't disconnect pair with recent position


@dataclass
class PairState:
    """Track per-pair connection state for hysteresis."""
    symbol_bb: str          # Bybit symbol (internal key)
    symbol_bn: str          # Binance symbol
    connected: bool = False
    below_threshold_count: int = 0  # consecutive refresh cycles below Tier C
    last_position_time: float = 0.0  # timestamp of last open position


class UniverseManager:
    """Manages dynamic WS connections based on TierEngine output."""

    def __init__(
        self,
        tier_engine: TierEngine,
        session: aiohttp.ClientSession,
        max_symbols: int = MAX_WS_SYMBOLS,
    ):
        self.tier_engine = tier_engine
        self._session = session
        self._max_symbols = max_symbols

        # Active WS state
        self._pair_states: dict[str, PairState] = {}  # symbol_bb -> PairState
        self._price_feed: PriceFeed | None = None
        self._last_refresh: float = 0.0
        self._refresh_count: int = 0

    @property
    def price_feed(self) -> PriceFeed | None:
        return self._price_feed

    @property
    def active_symbols_bb(self) -> list[str]:
        """Return list of currently connected Bybit symbols."""
        if self._price_feed:
            return list(self._price_feed.symbols)
        return []

    def mark_position_opened(self, symbol_bb: str):
        """Record that a position was opened on this pair.
        Prevents WS disconnection for POSITION_COOLDOWN_HOURS."""
        state = self._pair_states.get(symbol_bb)
        if state:
            state.last_position_time = time.time()

    async def initialize(self, initial_tiers: dict[str, TierInfo]) -> PriceFeed:
        """Create initial PriceFeed from TierEngine output.

        Called once at startup. Subsequent changes use refresh_universe().
        """
        # Select top pairs up to max
        connected = list(initial_tiers.values())[:self._max_symbols]

        symbols_bb = []
        bn_map = {}  # symbol_bb -> symbol_bn

        for info in connected:
            symbols_bb.append(info.symbol_bb)
            bn_map[info.symbol_bb] = info.symbol_bn
            self._pair_states[info.symbol_bb] = PairState(
                symbol_bb=info.symbol_bb,
                symbol_bn=info.symbol_bn,
                connected=True,
            )

        logger.info(f"UniverseManager: initializing with {len(symbols_bb)} pairs")

        self._price_feed = PriceFeed(symbols_bb, bn_symbol_map=bn_map)
        await self._price_feed.start(self._session)
        self._last_refresh = time.time()

        return self._price_feed

    async def refresh_universe(self) -> list[str]:
        """Refresh WS connections based on current TierEngine tiers.

        Returns list of changes made (for logging).

        Call this every UNIVERSE_REFRESH_INTERVAL (4h).
        """
        if not self._price_feed:
            logger.warning("UniverseManager: no price feed initialized")
            return []

        self._refresh_count += 1
        tiers = self.tier_engine.tiers
        changes = []

        # Target set: all Tier A+B+C pairs, up to max
        target_pairs = list(tiers.values())[:self._max_symbols]
        target_bb = {info.symbol_bb for info in target_pairs}
        current_bb = set(self._price_feed.symbols)

        to_add = target_bb - current_bb
        to_remove = current_bb - target_bb

        # Hysteresis: only remove pairs that have been below threshold for
        # HYSTERESIS_CYCLES consecutive refreshes AND have no recent position
        actual_remove = set()
        now = time.time()
        for sym_bb in to_remove:
            state = self._pair_states.get(sym_bb)
            if not state:
                continue

            # Never disconnect pair with recent position
            if now - state.last_position_time < POSITION_COOLDOWN_HOURS * 3600:
                logger.info(
                    f"UniverseManager: keeping {sym_bb} (position within {POSITION_COOLDOWN_HOURS}h)"
                )
                state.below_threshold_count = 0
                continue

            state.below_threshold_count += 1
            if state.below_threshold_count >= HYSTERESIS_CYCLES:
                actual_remove.add(sym_bb)
            else:
                logger.info(
                    f"UniverseManager: {sym_bb} below threshold "
                    f"({state.below_threshold_count}/{HYSTERESIS_CYCLES}), keeping for now"
                )

        # Reset below_threshold_count for pairs that are back in target
        for sym_bb in current_bb & target_bb:
            state = self._pair_states.get(sym_bb)
            if state:
                state.below_threshold_count = 0

        if not to_add and not actual_remove:
            logger.info("UniverseManager: no changes needed")
            return []

        # Build new symbol set
        new_symbols_bb = sorted((current_bb - actual_remove) | to_add)
        new_bn_map = {}
        for sym_bb in new_symbols_bb:
            # Get Binance symbol from tier info or existing state
            tier_info = self.tier_engine.get_tier_by_bb(sym_bb)
            state = self._pair_states.get(sym_bb)
            if tier_info:
                new_bn_map[sym_bb] = tier_info.symbol_bn
            elif state:
                new_bn_map[sym_bb] = state.symbol_bn
            else:
                # Derive: replace USDT with USDC
                new_bn_map[sym_bb] = sym_bb.replace("USDT", "USDC")

        # Apply changes
        # 1. Bybit: dynamic subscribe/unsubscribe (easy)
        await self._update_bybit(to_add, actual_remove)

        # 2. Binance: stop old, start new (the only safe approach)
        swap_ok = True
        if to_add or actual_remove:
            swap_ok = await self._hot_swap_binance(new_symbols_bb, new_bn_map)

        if not swap_ok:
            logger.error("UniverseManager: Binance swap failed, reverting changes")
            # Revert Bybit changes
            await self._update_bybit(actual_remove, to_add)  # reverse add/remove
            return []

        # 3. Update PriceFeed symbol list (only after confirmed swap)
        self._price_feed.symbols = new_symbols_bb
        self._price_feed._bn_map = new_bn_map
        self._price_feed._bn_reverse = {v: k for k, v in new_bn_map.items()}

        # 4. Update pair states
        for sym_bb in to_add:
            tier_info = self.tier_engine.get_tier_by_bb(sym_bb)
            bn_sym = new_bn_map.get(sym_bb, sym_bb.replace("USDT", "USDC"))
            self._pair_states[sym_bb] = PairState(
                symbol_bb=sym_bb,
                symbol_bn=bn_sym,
                connected=True,
            )
            # Initialize VenuePrice for new pair
            if sym_bb not in self._price_feed.bybit.prices:
                self._price_feed.bybit.prices[sym_bb] = VenuePrice(symbol=sym_bb)
            if bn_sym not in self._price_feed.binance.prices:
                self._price_feed.binance.prices[bn_sym] = VenuePrice(symbol=bn_sym)
            changes.append(f"+{sym_bb}")

        for sym_bb in actual_remove:
            state = self._pair_states.get(sym_bb)
            if state:
                state.connected = False
            # Clean up VenuePrice
            self._price_feed.bybit.prices.pop(sym_bb, None)
            bn_sym = self._price_feed._bn_map.get(sym_bb)
            if bn_sym:
                self._price_feed.binance.prices.pop(bn_sym, None)
            changes.append(f"-{sym_bb}")

        self._last_refresh = time.time()
        logger.info(f"UniverseManager refresh #{self._refresh_count}: {', '.join(changes)}")

        return changes

    async def _update_bybit(self, to_add: set[str], to_remove: set[str]):
        """Dynamic subscribe/unsubscribe on existing Bybit WS connection."""
        ws = self._price_feed.bybit._ws
        if not ws or ws.closed:
            logger.warning("UniverseManager: Bybit WS not available for dynamic update")
            return

        if to_remove:
            args = [f"tickers.{s}" for s in to_remove]
            try:
                await ws.send_json({"op": "unsubscribe", "args": args})
                logger.info(f"Bybit: unsubscribed {len(to_remove)} pairs")
            except Exception as e:
                logger.warning(f"Bybit unsubscribe failed: {e}")

        if to_add:
            args = [f"tickers.{s}" for s in to_add]
            try:
                await ws.send_json({"op": "subscribe", "args": args})
                logger.info(f"Bybit: subscribed {len(to_add)} pairs")
            except Exception as e:
                logger.warning(f"Bybit subscribe failed: {e}")

        # Always update Bybit symbols list (fixes: removal-only refresh left list stale)
        self._price_feed.bybit.symbols = list(
            (set(self._price_feed.bybit.symbols) - to_remove) | to_add
        )

    async def _hot_swap_binance(self, new_symbols_bb: list[str], bn_map: dict[str, str]) -> bool:
        """Replace Binance WS connection with new symbol set.

        The BinancePriceFeed._connect_and_listen() holds a local ws reference,
        so we can't swap _ws underneath it. Instead:
        1. Stop old BinancePriceFeed (closes WS, cancels task)
        2. Create new BinancePriceFeed with updated symbols
        3. Start new feed, transfer VenuePrice state for kept pairs
        4. Replace price_feed.binance reference

        Returns True if swap succeeded, False if failed (old feed stays).
        """
        old_feed = self._price_feed.binance

        # New Binance symbols
        new_bn_symbols = [bn_map[s] for s in new_symbols_bb if s in bn_map]
        if not new_bn_symbols:
            return True  # nothing to do

        logger.info(
            f"Binance swap: {len(old_feed.symbols)} -> {len(new_bn_symbols)} symbols"
        )

        # Preserve VenuePrice state for kept pairs (bid/ask/update_count)
        kept_bn = set(old_feed.symbols) & set(new_bn_symbols)
        preserved_prices = {}
        for sym in kept_bn:
            if sym in old_feed.prices:
                preserved_prices[sym] = old_feed.prices[sym]

        # Stop old feed
        await old_feed.stop()
        # Cancel old Binance task in PriceFeed
        for task in self._price_feed._tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except (asyncio.CancelledError, Exception):
                    pass

        # Create new feed with updated symbols
        new_feed = BinancePriceFeed(new_bn_symbols)

        # Transfer preserved prices (keeps freshness data for exits)
        for sym, price in preserved_prices.items():
            new_feed.prices[sym] = price

        # Start new feed
        self._price_feed.binance = new_feed
        new_task = asyncio.create_task(new_feed.start(self._session))
        self._price_feed._tasks = [
            t for t in self._price_feed._tasks if t.done()
        ]
        self._price_feed._tasks.append(new_task)
        # Also re-add Bybit task if it was cancelled
        if not any(not t.done() for t in self._price_feed._tasks
                   if t != new_task):
            bybit_task = asyncio.create_task(
                self._price_feed.bybit.start(self._session)
            )
            self._price_feed._tasks.append(bybit_task)

        logger.info(
            f"Binance swap complete: {len(new_bn_symbols)} symbols, "
            f"{len(preserved_prices)} prices preserved"
        )
        return True

    def needs_refresh(self) -> bool:
        """Check if it's time for a universe refresh."""
        return time.time() - self._last_refresh >= UNIVERSE_REFRESH_INTERVAL

    def status(self) -> dict:
        """Return status for monitoring."""
        return {
            "connected_pairs": len(self.active_symbols_bb),
            "last_refresh": self._last_refresh,
            "refresh_count": self._refresh_count,
            "pair_states": {
                s: {
                    "connected": st.connected,
                    "below_count": st.below_threshold_count,
                    "last_position": st.last_position_time,
                }
                for s, st in self._pair_states.items()
                if st.connected
            },
        }
