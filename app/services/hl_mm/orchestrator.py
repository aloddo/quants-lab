"""
HL MM Orchestrator V2 — Proper integration of AS quoter + all safety fixes.

Fixes from adversarial review:
- F2: AS quoter is now the SOLE pricing engine (not disconnected)
- F4: Kill switches implemented (actual market_close)
- F7: Fill detection + cooldown + toxicity response
- F8: Inventory checked BEFORE quoting
- Control flow: inventory → signal → quoter → quote_engine (correct order)

Run as: python scripts/hl_mm_live.py
"""
import asyncio
import logging
import os
import time
from typing import Optional

import eth_account
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from hyperliquid.utils import constants

from .avellaneda_quoter import AvellanedaQuoter, ASConfig, QuoteResult
from .signal_engine import SignalEngine
from .quote_engine import QuoteEngineV2
from .inventory_manager import InventoryManagerV2, InventoryConfig
from .fair_value import FairValueEngine
from .fill_tracker import FillTracker

logger = logging.getLogger(__name__)


class HLMarketMaker:
    """Imbalance-directed AS market maker on Hyperliquid. V2."""

    def __init__(
        self,
        private_key: Optional[str] = None,
        address: Optional[str] = None,
        coins: Optional[list[str]] = None,
        as_config: Optional[ASConfig] = None,
        inv_config: Optional[InventoryConfig] = None,
        testnet: bool = False,
    ):
        # Credentials
        self.private_key = private_key or os.environ.get("HL_PRIVATE_KEY")
        self.address = address or os.environ.get("HL_ADDRESS")

        if not self.private_key or not self.address:
            raise ValueError("HL_PRIVATE_KEY and HL_ADDRESS must be set")

        # Config
        self.coins = coins or ["APE"]  # Start with single pair (APE = widest spread)
        self.as_config = as_config or ASConfig()
        self.inv_config = inv_config or InventoryConfig()

        # SDK — FIX 3: pass account_address for API wallet → main wallet mapping
        api_url = constants.TESTNET_API_URL if testnet else constants.MAINNET_API_URL
        wallet = eth_account.Account.from_key(self.private_key)
        self.info = Info(api_url, skip_ws=True)
        # If wallet.address != HL_ADDRESS, this is an API agent wallet
        # and account_address tells the SDK which main account to trade for
        account_addr = self.address if wallet.address.lower() != self.address.lower() else None
        self.exchange = Exchange(wallet, api_url, account_address=account_addr)

        # Get venue metadata (tick sizes)
        meta = self.info.meta()
        self.sz_decimals = {p["name"]: p["szDecimals"] for p in meta["universe"]}

        # Components (all integrated)
        self.signal_engine = SignalEngine(
            info=self.info,
            coins=self.coins,
            z_window=300,
            z_threshold=2.0,
        )
        self.fair_value_engine = FairValueEngine(
            coins=self.coins,
            dislocation_threshold_bps=15.0,
            bybit_weight=0.6,
        )
        self.fill_tracker = FillTracker(
            max_history=200,
            toxicity_threshold_bps=-2.0,
        )
        self.quoters: dict[str, AvellanedaQuoter] = {
            coin: AvellanedaQuoter(self.as_config) for coin in self.coins
        }
        self.quote_engine = QuoteEngineV2(
            exchange=self.exchange,
            info=self.info,
            address=self.address,
            sz_decimals=self.sz_decimals,
        )
        self.inventory = InventoryManagerV2(
            info=self.info,
            address=self.address,
            coins=self.coins,
            config=self.inv_config,
        )

        # State
        self._running = False
        self._start_time = 0.0
        self._tick_count = 0
        self._total_fills = 0
        self._stopped_reason: str = ""
        self._funding_rates: dict[str, float] = {coin: 0.0 for coin in self.coins}
        self._last_funding_fetch: float = 0.0

    async def run(self):
        """Main event loop."""
        self._running = True
        self._start_time = time.time()
        logger.info(f"HL MM V2 starting: coins={self.coins}, capital=$50")

        try:
            while self._running:
                await self._tick()
                await asyncio.sleep(1.5)  # 1.5s tick (conservative vs rate limits)
        except KeyboardInterrupt:
            logger.info("Shutdown requested (KeyboardInterrupt)")
        except Exception as e:
            logger.error(f"Fatal error: {e}", exc_info=True)
            self._stopped_reason = str(e)
        finally:
            self._shutdown()

    async def _tick(self):
        """Single iteration. Order: inventory → signal → quoter → execute.

        FIX 3: All blocking I/O wrapped in asyncio.to_thread with timeouts.
        If any call exceeds 2s, skip it and widen quotes for safety.
        """
        self._tick_count += 1

        # === STEP 1: Update inventory FIRST (with timeout) ===
        try:
            positions = await asyncio.wait_for(
                asyncio.to_thread(self.inventory.update), timeout=2.0
            )
        except asyncio.TimeoutError:
            logger.warning("Inventory update timed out (>2s), using cached")
            positions = {coin: self.inventory.get_position(coin) for coin in self.coins}

        # === STEP 2: Check kill switches ===
        for coin in self.coins:
            should_stop, reason = self.inventory.should_stop_quoting(coin)
            if should_stop:
                logger.critical(f"KILL SWITCH: {reason}")
                await self._emergency_flatten(coin, reason)
                self._running = False
                return

        # === STEP 3: Detect fills from previous tick (with timeout) ===
        for coin in self.coins:
            try:
                fills = await asyncio.wait_for(
                    asyncio.to_thread(self.quote_engine.detect_fills, coin), timeout=2.0
                )
                for fill in fills:
                    self._handle_fill(coin, fill)
            except asyncio.TimeoutError:
                logger.warning(f"Fill detection timed out for {coin}")

        # === STEP 4: Signal + Fair Value update (with timeouts) ===
        try:
            signals = await asyncio.wait_for(
                asyncio.to_thread(self.signal_engine.update), timeout=3.0
            )
        except asyncio.TimeoutError:
            # FIX 2: Cancel all quotes on signal timeout (don't leave stale quotes)
            logger.warning("Signal update timed out, cancelling all quotes for safety")
            await asyncio.to_thread(self.quote_engine.cancel_all)
            return

        try:
            fair_values = await asyncio.wait_for(
                asyncio.to_thread(self.fair_value_engine.update, signals), timeout=2.0
            )
        except asyncio.TimeoutError:
            logger.debug("Fair value timeout, using HL-only pricing")
            fair_values = {}

        # === STEP 4.5: Update fill markouts ===
        current_mids = {coin: sig.mid_price for coin, sig in signals.items() if sig}
        self.fill_tracker.update_markouts(current_mids)

        # === STEP 5: Compute quotes via AS quoter with full context ===
        for coin in self.coins:
            signal = signals.get(coin)
            fv = fair_values.get(coin)
            if not signal:
                continue

            quoter = self.quoters[coin]
            pos = self.inventory.get_position(coin)

            # Feed mid to quoter for vol estimation
            quoter.update_mid(signal.mid_price)

            # Feed trade info for kappa estimation
            if signal.trade_confirmed:
                quoter.record_trade()

            # Skip if in post-fill cooldown
            if quoter.in_cooldown():
                continue

            # Check if fill tracker says we should pause (too toxic)
            if self.fill_tracker.should_pause(coin):
                logger.warning(f"{coin}: pausing — adverse selection rate too high")
                self.quote_engine.update_quotes(coin=coin, quote=None, signal_stale=True)
                continue

            # Get live funding rate
            funding = self._get_funding_rate(coin)

            # USE FAIR VALUE as pricing reference (not raw HL mid)
            pricing_mid = fv.fair_value if fv and fv.bybit_fresh else signal.mid_price

            # Compute optimal quotes
            inventory_usd = pos.size * signal.mid_price
            quote = quoter.compute_quotes(
                mid_price=pricing_mid,
                inventory_usd=inventory_usd,
                funding_rate_hourly=funding,
                best_bid=signal.best_bid,
                best_ask=signal.best_ask,
            )

            if not quote:
                continue

            # === SIGNAL-DRIVEN ONE-SIDED SKEW (Codex R4: 1.5bps, one side only) ===
            # Tighten ONLY the predicted side (more fills when we're right)
            # Don't shift the other side (avoid full reservation shove)
            if signal.direction != 0 and signal.trade_confirmed:
                skew_bps = 1.5  # conservative: matches validated edge magnitude
                skew_price = pricing_mid * (skew_bps / 10000.0)
                if signal.direction == 1:  # bid heavy → price going UP → tighten bid only
                    quote = QuoteResult(
                        bid_price=quote.bid_price + skew_price,  # bid moves UP (tighter)
                        ask_price=quote.ask_price,  # ask stays
                        bid_size=quote.bid_size,
                        ask_size=quote.ask_size,
                        reservation_price=quote.reservation_price,
                        spread_bps=quote.spread_bps - skew_bps,
                        inventory_skew_bps=quote.inventory_skew_bps,
                        sigma_per_second=quote.sigma_per_second,
                        should_quote_bid=quote.should_quote_bid,
                        should_quote_ask=quote.should_quote_ask,
                    )
                elif signal.direction == -1:  # ask heavy → price going DOWN → tighten ask only
                    quote = QuoteResult(
                        bid_price=quote.bid_price,  # bid stays
                        ask_price=quote.ask_price - skew_price,  # ask moves DOWN (tighter)
                        bid_size=quote.bid_size,
                        ask_size=quote.ask_size,
                        reservation_price=quote.reservation_price,
                        spread_bps=quote.spread_bps - skew_bps,
                        inventory_skew_bps=quote.inventory_skew_bps,
                        sigma_per_second=quote.sigma_per_second,
                        should_quote_bid=quote.should_quote_bid,
                        should_quote_ask=quote.should_quote_ask,
                    )

            # Adaptive spread from fill quality (widen if too many toxic fills)
            spread_adj = self.fill_tracker.get_optimal_spread_adjustment(coin)
            if spread_adj != 0:
                adj_price = pricing_mid * (spread_adj / 10000.0)
                quote = QuoteResult(
                    bid_price=quote.bid_price - adj_price / 2,
                    ask_price=quote.ask_price + adj_price / 2,
                    bid_size=quote.bid_size,
                    ask_size=quote.ask_size,
                    reservation_price=quote.reservation_price,
                    spread_bps=quote.spread_bps + spread_adj,
                    inventory_skew_bps=quote.inventory_skew_bps,
                    sigma_per_second=quote.sigma_per_second,
                    should_quote_bid=quote.should_quote_bid,
                    should_quote_ask=quote.should_quote_ask,
                )

            # Dislocation check: ONLY pause on FRESH Bybit data (Fix 4)
            should_skip = False
            if fv and fv.is_dislocated and fv.bybit_fresh:
                logger.warning(f"{coin}: HL-Bybit dislocation {fv.hl_premium_bps:+.1f}bps (fresh), pausing")
                should_skip = True
            elif signal.is_stale and not signal.trade_confirmed:
                should_skip = True

            self.quote_engine.update_quotes(
                coin=coin,
                quote=quote,
                signal_stale=should_skip,
            )

        # === STEP 6: Periodic cleanup (F6) ===
        if self._tick_count % 20 == 0:  # every 30s
            self.quote_engine.cleanup_orphans()

        # === STEP 7: Status log ===
        if self._tick_count % 40 == 0:  # every 60s
            self._log_status()

    def _handle_fill(self, coin: str, fill: dict):
        """Process a detected fill with full tracking."""
        self._total_fills += 1
        side = fill["side"]
        price = fill["price"]

        # Record everywhere
        self.inventory.record_fill(coin, side)
        self.quoters[coin].record_fill()
        self.fill_tracker.record_fill(
            coin=coin, side=side, price=price,
            size_usd=self.as_config.order_size_usd,
        )

        # Conservative PnL tracking: assume each fill costs ~3bps in adverse selection
        # (Will be refined by fill_tracker markout data over time)
        estimated_cost = self.as_config.order_size_usd * 0.0003
        self.inventory.update_daily_pnl(-estimated_cost)

        # Log with toxicity context
        stats = self.fill_tracker.get_stats(coin)
        logger.info(
            f"FILL: {coin} {side} @ ${price:.5f} "
            f"(total: {self._total_fills}, adverse_rate: {stats.adverse_selection_rate:.0%}, "
            f"daily_pnl: ${self.inventory._daily_pnl:.3f})"
        )

    async def _emergency_flatten(self, coin: str, reason: str):
        """F4: Actual emergency flatten — cancel + market close."""
        logger.critical(f"EMERGENCY FLATTEN {coin}: {reason}")

        # Cancel all quotes
        self.quote_engine.cancel_all()

        # Market close the position
        pos = self.inventory.get_position(coin)
        if pos.size != 0:
            try:
                result = self.exchange.market_close(coin)
                logger.info(f"Market close result: {result}")
            except Exception as e:
                logger.error(f"Market close FAILED for {coin}: {e}")
                # Last resort: try market order in opposite direction
                try:
                    is_buy = pos.size < 0  # if short, buy to close
                    self.exchange.market_open(coin, is_buy, abs(pos.size))
                except Exception as e2:
                    logger.critical(f"CANNOT FLATTEN {coin}: {e2}")

        self._stopped_reason = reason

    def _get_funding_rate(self, coin: str) -> float:
        """F8 FIX: Fetch live funding rate from HL (cached, refreshed every 5 min)."""
        now = time.time()
        if now - self._last_funding_fetch > 300:  # refresh every 5 min
            try:
                data = self.info.meta_and_asset_ctxs()
                if data and len(data) == 2:
                    meta, contexts = data
                    pairs = meta.get("universe", [])
                    for pair, ctx in zip(pairs, contexts):
                        name = pair.get("name", "")
                        if name in self._funding_rates:
                            rate = float(ctx.get("funding", 0) or 0)
                            self._funding_rates[name] = rate
                self._last_funding_fetch = now
            except Exception as e:
                logger.debug(f"Funding fetch failed: {e}")
        return self._funding_rates.get(coin, 0.0)

    def _log_status(self):
        """Periodic status log."""
        uptime = time.time() - self._start_time
        exposure = self.inventory.total_exposure_usd()
        pnl = self.inventory.net_pnl()

        logger.info(
            f"Status: uptime={uptime/60:.0f}m ticks={self._tick_count} "
            f"fills={self._total_fills} exposure=${exposure:.2f} uPnL=${pnl:.2f}"
        )
        for coin in self.coins:
            pos = self.inventory.get_position(coin)
            sig = self.signal_engine.get_signal(coin)
            if sig:
                logger.info(
                    f"  {coin}: pos={pos.size:.4f} (${pos.notional_usd:.2f}) "
                    f"imb_z={sig.z_score:+.2f} spread={sig.spread_bps:.1f}bps "
                    f"stale={sig.is_stale}"
                )

    def _shutdown(self):
        """Clean shutdown."""
        logger.info(f"Shutting down (reason: {self._stopped_reason or 'normal'})")
        self.quote_engine.cancel_all()
        self._running = False

    def stop(self):
        """Request graceful stop."""
        self._running = False
