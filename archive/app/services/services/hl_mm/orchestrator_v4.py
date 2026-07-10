"""
HL MM V4 Orchestrator — Clean rewrite, reusing V2/V3 proven modules.

Stripped: wallet_scorer, mm_tracker, Bybit anchor, contrarian quoting, metaorder rider.
Kept: signal_engine, fill_tracker, risk_manager, inventory_manager, state_machine,
      pair_screener, ws_order_client, notifier.

Architecture:
  - ONE WS connection for L2 book (75 monitoring pairs) + orderUpdates
  - Screener scores pairs every 15 min from collected L2 data
  - Top N pairs get ACTIVE status → quote placement
  - Per-pair state machine manages lifecycle
  - Signal engine detects adverse selection → pause toxic side
  - Inventory manager skews quotes to flatten
  - Fill tracker measures markout per fill
  - Risk manager: circuit breakers, portfolio limits
"""
import asyncio
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Optional

import eth_account
import requests as sync_requests
import websockets
from hyperliquid.exchange import Exchange
from hyperliquid.info import Info
from pymongo import MongoClient

from .config import HLMMConfig, load_config, FeeConfig
from .signal_engine import SignalEngine
from .inventory_manager import InventoryManager
from .fill_tracker import FillTracker
from .risk_manager import RiskManager, RiskConfig, RiskAction
from .state_machine import StateMachine, PairContext, PairState
from .pair_screener import PairScreener, ScreenerConfig
from .ws_order_client import WSOrderClient
from .notifier import TelegramNotifier

logger = logging.getLogger(__name__)

HL_API = "https://api.hyperliquid.xyz"
HL_WS = "wss://api.hyperliquid.xyz/ws"


class HLMMv4:
    """V4 Market Maker — one process, many pairs, clean quoting."""

    # Coins used by copy trading bots (V9 + V10). MM must never quote these
    # to avoid position/reconciliation conflicts.
    COPY_TRADER_COINS = {
        # V9 coins (22)
        "BTC", "ETH", "SOL", "SUI", "AAVE", "TAO", "TON", "ZEC",
        "FARTCOIN", "DOGE", "XRP", "PUMP", "MON", "LINK", "CRV",
        "LTC", "INJ", "BNB", "NEAR", "ADA", "AVAX", "TRX",
        # V10 extras (already in V9 list except UNI, DOT)
        "UNI", "DOT",
    }

    def __init__(
        self,
        config: Optional[HLMMConfig] = None,
        max_active_pairs: int = 5,
        order_size_usd: float = 20.0,
        monitor_pairs: int = 75,
    ):
        # Config
        self.config = config or load_config()
        self.max_active = max_active_pairs
        self.order_size_usd = order_size_usd
        self.monitor_count = monitor_pairs

        # HL SDK
        self.private_key = os.environ["HL_PRIVATE_KEY"]
        self.agent_address = os.environ["HL_ADDRESS"]
        self.parent_address = os.environ.get(
            "HL_QUERY_ADDRESS", "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
        )
        self.account = eth_account.Account.from_key(self.private_key)
        # Retry SDK init with backoff (HL rate limits on rapid restarts)
        for attempt in range(5):
            try:
                self.info = Info(HL_API, skip_ws=True)
                self.exchange = Exchange(self.account, HL_API, account_address=self.agent_address)
                break
            except Exception as e:
                wait = (attempt + 1) * 10
                logger.warning(f"SDK init attempt {attempt+1} failed: {e}. Waiting {wait}s...")
                import time as _t
                _t.sleep(wait)

        # MongoDB
        mongo_uri = os.environ.get("MONGO_URI", "mongodb://localhost:27017/quants_lab")
        self.db = MongoClient(mongo_uri).get_default_database()

        # Reused V2 modules
        self.screener = PairScreener(
            self.info,
            config=ScreenerConfig(
                min_daily_volume_usd=500_000,
                max_live_pairs=max_active_pairs,
                maker_fee_bps=1.44,
            ),
        )
        self.signal_engine = SignalEngine()
        self.fill_tracker = FillTracker()
        self.risk_manager = RiskManager(
            RiskConfig(
                max_live_pairs=max_active_pairs,
                max_gross_notional=order_size_usd * max_active_pairs * 3,
                max_net_exposure=order_size_usd * max_active_pairs * 2,
                daily_stop_usd=5.0,
                hard_stop_usd=10.0,
            )
        )
        self.inventory_mgr = InventoryManager(self.info, self.parent_address)
        self.state_machine = StateMachine()
        self.notifier = TelegramNotifier(
            chat_id=os.environ.get("TELEGRAM_CHAT_ID", "-1003576397888"),
        )

        # State
        self.active_coins: set = set()
        self.monitoring_coins: set = set()
        self.sz_decimals: dict = {}
        self._price_decimals: dict = {}  # coin → int (decimals from BBO)
        self.mid_prices: dict = {}
        self.resting_orders: dict = {}  # coin → {"bid_oid": X, "ask_oid": Y}
        self.positions: dict = {}  # coin → qty
        self.running = False
        self._ws = None
        self._last_screener_run = 0
        self._last_stats_log = 0
        self._last_position_sync = 0
        self._signal_cache: dict = {}  # coin → SignalState from last update_book

    # ── Quoting Logic (V4: simple, no contrarian) ──────────────────

    def _compute_quotes(self, coin: str) -> tuple:
        """Compute bid/ask for one pair. Returns (bid_px, ask_px) or (None, None).

        V4 philosophy: quote at native spread with inventory skew.
        Signal engine determines which sides are safe to quote.
        """
        mid = self.mid_prices.get(coin, 0)
        if mid <= 0:
            return None, None

        # Get signal engine assessment (cached from last update_book call)
        signals = self._signal_cache.get(coin)

        # Get book spread
        spread_bps = signals.book.spread_bps if signals and signals.book else 10.0

        # Minimum half-spread: must exceed break-even
        min_half = max(3.0, self.config.fees.hl_maker_fee_bps + 1.0)
        half_spread = max(min_half, spread_bps / 2 * 0.8)

        # Inventory skew (capped so spread never inverts)
        inv_qty = self.positions.get(coin, 0)
        inv_usd = inv_qty * mid
        max_inv = self.order_size_usd * 2
        raw_skew = (inv_usd / max_inv * 3.0) if max_inv > 0 else 0
        skew_bps = max(-half_spread + 1.0, min(raw_skew, half_spread - 1.0))

        # Determine which sides are safe
        bid_ok = True
        ask_ok = True

        if signals:
            # Adverse selection: pause toxic side
            if signals.any_toxic_flag:
                # Which side is toxic depends on direction
                if signals.mid_momentum_5m > 3:
                    ask_ok = False  # trending up, ask is toxic
                elif signals.mid_momentum_5m < -3:
                    bid_ok = False  # trending down, bid is toxic

            # Book imbalance (using z-score)
            if signals.imbalance_z > 1.5:
                ask_ok = False  # heavy bid side, price going up, ask toxic
            elif signals.imbalance_z < -1.5:
                bid_ok = False

        # Inventory limits: non-zero inventory, only quote the exit side
        if inv_usd > 1.0:  # long, only sell to exit
            bid_ok = False
        elif inv_usd < -1.0:  # short, only buy to exit
            ask_ok = False

        # Compute prices
        bid_px = mid * (1 - (half_spread + skew_bps) / 10000) if bid_ok else None
        ask_px = mid * (1 + (half_spread - skew_bps) / 10000) if ask_ok else None

        # Round prices to tick size
        if bid_px:
            bid_px = self._round_price(coin, bid_px)
        if ask_px:
            ask_px = self._round_price(coin, ask_px)

        return bid_px, ask_px

    def _round_price(self, coin: str, px: float) -> float:
        """Round price to match HL tick size (derived from book BBO)."""
        if px <= 0:
            return 0.0
        dec = self._price_decimals.get(coin)
        if dec is not None:
            return round(px, dec)
        # Fallback: 5 significant figures
        mag = math.floor(math.log10(abs(px)))
        decimals = max(0, 4 - mag)
        return round(px, decimals)

    def _round_size(self, coin: str, sz: float) -> float:
        dec = self.sz_decimals.get(coin, 2)
        return round(sz, dec)

    # ── Order Management ──────────────────────────────────────────

    async def _requote_pair(self, coin: str):
        """Cancel and replace orders for one pair."""
        mid = self.mid_prices.get(coin, 0)
        if mid <= 0:
            return

        bid_px, ask_px = self._compute_quotes(coin)

        current = self.resting_orders.get(coin, {})
        sz = self._round_size(coin, self.order_size_usd / mid)
        if sz <= 0:
            return

        # Cancel stale orders
        for side_key, old_oid in list(current.items()):
            if old_oid:
                try:
                    self.exchange.cancel(coin, int(old_oid))
                except Exception:
                    pass

        self.resting_orders[coin] = {}

        # Place new orders (ALO only)
        if bid_px and sz > 0:
            try:
                result = self.exchange.order(coin, True, sz, bid_px, {"limit": {"tif": "Alo"}})
                statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
                if statuses and "resting" in statuses[0]:
                    self.resting_orders.setdefault(coin, {})["bid_oid"] = str(statuses[0]["resting"]["oid"])
                elif statuses:
                    logger.info(f"{coin} bid order not resting: {statuses[0]}")
            except Exception as e:
                logger.warning(f"{coin} bid order error: {e}")

        if ask_px and sz > 0:
            try:
                result = self.exchange.order(coin, False, sz, ask_px, {"limit": {"tif": "Alo"}})
                statuses = result.get("response", {}).get("data", {}).get("statuses", [{}])
                if statuses and "resting" in statuses[0]:
                    self.resting_orders.setdefault(coin, {})["ask_oid"] = str(statuses[0]["resting"]["oid"])
                elif statuses:
                    logger.info(f"{coin} ask order not resting: {statuses[0]}")
            except Exception as e:
                logger.warning(f"{coin} ask order error: {e}")

    # ── WS Handlers ───────────────────────────────────────────────

    def _on_l2_book(self, coin: str, data: dict):
        """Process L2 book update — update mid, feed signal engine, store to MongoDB."""
        levels = data.get("levels", [])
        if len(levels) < 2 or not levels[0] or not levels[1]:
            return

        best_bid = float(levels[0][0]["px"])
        best_ask = float(levels[1][0]["px"])
        mid = (best_bid + best_ask) / 2
        self.mid_prices[coin] = mid
        spread_bps = (best_ask - best_bid) / mid * 10000

        # Derive price decimals from BBO string (first time only)
        if coin not in self._price_decimals:
            px_str = levels[0][0]["px"]
            if "." in px_str:
                self._price_decimals[coin] = len(px_str.split(".")[1])
            else:
                self._price_decimals[coin] = 0

        # Feed to signal engine — returns SignalState
        signal_state = self.signal_engine.update_book(coin, data)
        if signal_state:
            self._signal_cache[coin] = signal_state

        # Store L2 snapshot to MongoDB
        # In data-collection mode: every update (sub-second). In trading mode: every 5s.
        now = time.time()
        if not hasattr(self, '_last_l2_store'):
            self._last_l2_store = {}
        store_interval = 0 if getattr(self, '_data_only', False) else 5
        if now - self._last_l2_store.get(coin, 0) >= store_interval:
            self._last_l2_store[coin] = now
            bid_depth = sum(float(b["px"]) * float(b["sz"]) for b in levels[0][:5])
            ask_depth = sum(float(a["px"]) * float(a["sz"]) for a in levels[1][:5])
            total = bid_depth + ask_depth
            imbalance = (bid_depth - ask_depth) / total if total > 0 else 0
            try:
                self.db["hyperliquid_l2_snapshots_1s"].insert_one({
                    "timestamp_utc": int(now * 1000),
                    "pair": f"{coin}-USDT",
                    "coin": coin,
                    "mid_px": mid,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread_bps": spread_bps,
                    "bid_sz_topn": bid_depth,
                    "ask_sz_topn": ask_depth,
                    "imbalance_topn": imbalance,
                    "recorded_at": datetime.now(timezone.utc),
                })
            except Exception:
                pass  # don't crash on MongoDB write failure

    def _on_order_update(self, updates: list):
        """Process order fill/cancel events. Uses proven V2 format."""
        for update in updates:
            order = update.get("order", {})
            raw_oid = order.get("oid")
            oid = int(raw_oid) if raw_oid is not None else None
            status = update.get("status")
            coin = order.get("coin", "")

            # Ignore fills on copy trader coins (those are V9/V10 fills, not ours)
            if coin in self.COPY_TRADER_COINS:
                continue

            if status == "filled":
                side = order.get("side", "")
                px = float(order.get("limitPx", 0))
                sz = float(order.get("origSz", order.get("sz", 0)))
                is_buy = side == "B"

                # Update position
                if is_buy:
                    self.positions[coin] = self.positions.get(coin, 0) + sz
                else:
                    self.positions[coin] = self.positions.get(coin, 0) - sz

                inv_usd = self.positions.get(coin, 0) * self.mid_prices.get(coin, 0)
                logger.info(
                    f"FILL: {coin} {'BUY' if is_buy else 'SELL'} {sz} @ {px} "
                    f"inv={self.positions.get(coin, 0):.1f} (${inv_usd:.2f})"
                )
                self.notifier.notify_fill(
                    coin=coin, side="BUY" if is_buy else "SELL",
                    size=sz, price=px, size_usd=sz * px,
                    fee=sz * px * 0.000144, edge_bps=0,
                )

                # Record fill for markout tracking
                try:
                    self.fill_tracker.record_fill(
                        coin=coin, side="BUY" if is_buy else "SELL",
                        price=px, size=sz, size_usd=sz * px,
                        fee=sz * px * 0.000144, oid=oid or 0,
                    )
                except Exception as e:
                    logger.debug(f"Fill tracker error: {e}")

                # Clear resting order reference
                orders = self.resting_orders.get(coin, {})
                for key, val in list(orders.items()):
                    if val and int(val) == oid:
                        orders[key] = None

                # IMMEDIATE: cancel same-side order to prevent double-filling
                same_side_key = "bid_oid" if is_buy else "ask_oid"
                same_side_oid = orders.get(same_side_key)
                if same_side_oid:
                    try:
                        self.exchange.cancel(coin, int(same_side_oid))
                        orders[same_side_key] = None
                        logger.info(f"CANCELLED same-side {same_side_key} after fill")
                    except Exception:
                        pass

    def _on_trades(self, coin: str, trades: list):
        """Process trade stream for signal engine."""
        self.signal_engine.update_trades(coin, trades)

    # ── Screener Integration ──────────────────────────────────────

    async def _run_screener(self):
        """Run pair screener and rotate active pairs."""
        now = time.time()
        if now - self._last_screener_run < 900:  # every 15 min
            return
        self._last_screener_run = now

        rankings = await self.screener.scan()
        new_active = set()
        for r in rankings[:self.max_active * 2]:  # scan extra to fill after exclusions
            if r.edge_room_bps > 0 and r.coin not in self.COPY_TRADER_COINS:
                new_active.add(r.coin)
                if len(new_active) >= self.max_active:
                    break

        # Deactivate demoted pairs
        for coin in self.active_coins - new_active:
            logger.info(f"DEMOTING {coin} from active quoting")
            await self._cancel_all_orders(coin)

        # Activate promoted pairs
        for coin in new_active - self.active_coins:
            logger.info(f"PROMOTING {coin} to active quoting")

        self.active_coins = new_active
        logger.info(f"Active pairs: {self.active_coins}")

    async def _cancel_all_orders(self, coin: str):
        """Cancel all orders for a pair."""
        orders = self.resting_orders.get(coin, {})
        for key, oid in list(orders.items()):
            if oid:
                try:
                    self.exchange.cancel(coin, int(oid))
                except Exception:
                    pass
        self.resting_orders[coin] = {}

    # ── Position Sync ─────────────────────────────────────────────

    async def _sync_positions(self):
        """Sync positions from HL API every 30s."""
        now = time.time()
        if now - self._last_position_sync < 30:
            return
        self._last_position_sync = now

        try:
            r = sync_requests.post(f"{HL_API}/info", json={
                "type": "clearinghouseState", "user": self.parent_address
            })
            for p in r.json().get("assetPositions", []):
                pos = p["position"]
                coin = pos["coin"]
                # Skip copy trader coins (those positions belong to V9/V10)
                if coin in self.COPY_TRADER_COINS:
                    continue
                actual = float(pos["szi"])
                tracked = self.positions.get(coin, 0)
                if abs(actual - tracked) > 0.01:
                    logger.warning(f"POSITION SYNC: {coin} tracked={tracked:.1f} actual={actual:.1f}")
                    self.positions[coin] = actual
        except Exception as e:
            logger.error(f"Position sync error: {e}")

    # ── Stats ─────────────────────────────────────────────────────

    def _log_stats(self):
        now = time.time()
        if now - self._last_stats_log < 60:
            return
        self._last_stats_log = now

        logger.info(f"STATS: active={self.active_coins} monitoring={len(self.monitoring_coins)}")
        for coin in self.active_coins:
            mid = self.mid_prices.get(coin, 0)
            inv = self.positions.get(coin, 0)
            inv_usd = inv * mid if mid > 0 else 0
            signals = self._signal_cache.get(coin)
            spread = signals.book.spread_bps if signals and signals.book else 0
            orders = self.resting_orders.get(coin, {})
            logger.info(
                f"  {coin}: mid={mid:.5f} spread={spread:.1f}bp inv={inv:.1f} (${inv_usd:.2f}) "
                f"bid={orders.get('bid_oid', '-')} ask={orders.get('ask_oid', '-')}"
            )

    # ── Main Loop ─────────────────────────────────────────────────

    async def run(self):
        """Main event loop."""
        logger.info(f"HL MM V4 starting: {self.max_active} pairs, ${self.order_size_usd}/side")
        self.notifier.notify_engine_event("START", f"V4: {self.max_active} pairs, ${self.order_size_usd}/side, monitoring {self.monitor_count}")
        self.running = True

        # Get meta for sz_decimals
        meta = self.info.meta_and_asset_ctxs()
        if meta and len(meta) == 2:
            for u in meta[0]["universe"]:
                self.sz_decimals[u["name"]] = u.get("szDecimals", 2)

        # Determine monitoring universe (top by volume, >$500K, spread >3bp)
        self.monitoring_coins = await self._get_monitoring_universe()
        logger.info(f"Monitoring {len(self.monitoring_coins)} pairs")

        # Initial screener run
        await self._run_screener()

        # Bootstrap: if screener found 0 active pairs, seed with known-good MM pairs
        if not self.active_coins:
            bootstrap = {"PNUT", "ORDI", "DASH", "AXS", "NIL", "EIGEN", "DYDX"}
            valid = (bootstrap & self.monitoring_coins) - self.COPY_TRADER_COINS
            self.active_coins = valid
            logger.info(f"BOOTSTRAP: screener empty, seeding with {valid}")

        # WS loop with reconnection
        while self.running:
            try:
                async with websockets.connect(HL_WS, ping_interval=20) as ws:
                    self._ws = ws

                    # Subscribe to orderUpdates FIRST (critical for fill detection)
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "orderUpdates", "user": self.parent_address}
                    }))
                    await asyncio.sleep(0.5)

                    # In data-collection mode, subscribe to ALL monitoring pairs
                    # In trading mode, only active + extras
                    sub_set = self.monitoring_coins if getattr(self, '_data_only', False) else self.active_coins
                    subscribed_coins = set()
                    for coin in sub_set:
                        await ws.send(json.dumps({
                            "method": "subscribe",
                            "subscription": {"type": "l2Book", "coin": coin}
                        }))
                        subscribed_coins.add(coin)
                        await asyncio.sleep(0.1)

                    # Subscribe to extra candidates for L2 data collection
                    max_extra = 65 if getattr(self, '_data_only', False) else 10
                    extra_count = 0
                    for r in self.screener._rankings:
                        if r.coin not in subscribed_coins and extra_count < max_extra:
                            await ws.send(json.dumps({
                                "method": "subscribe",
                                "subscription": {"type": "l2Book", "coin": r.coin}
                            }))
                            subscribed_coins.add(r.coin)
                            extra_count += 1
                            await asyncio.sleep(0.1)

                    logger.info(f"WS subscribed: {len(subscribed_coins)} l2Books + orderUpdates")

                    # Tick-based processing
                    last_requote = 0
                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(msg)
                            channel = data.get("channel")

                            if channel not in ("l2Book", "trades", "orderUpdates", "subscriptionResponse", None):
                                logger.info(f"WS unknown channel: {channel}")

                            if channel == "l2Book":
                                coin_data = data.get("data", {})
                                coin = coin_data.get("coin", "")
                                self._on_l2_book(coin, coin_data)

                            elif channel == "trades":
                                trades = data.get("data", [])
                                if trades:
                                    coin = trades[0].get("coin", "") if isinstance(trades, list) else ""
                                    self._on_trades(coin, trades)

                            elif channel == "orderUpdates":
                                raw = data.get("data", [])
                                if raw:
                                    logger.info(f"WS orderUpdate: {len(raw)} events")
                                self._on_order_update(raw)

                            # Requote active pairs every 5s
                            # DATA COLLECTION MODE: skip quoting
                            if not getattr(self, '_data_only', False):
                                now = time.time()
                                if now - last_requote >= 5:
                                    for coin in list(self.active_coins):
                                        await self._requote_pair(coin)
                                    last_requote = now

                            # Periodic tasks
                            await self._sync_positions()
                            await self._run_screener()
                            self._log_stats()
                            self.fill_tracker.update_markouts(self.mid_prices)

                        except asyncio.TimeoutError:
                            await ws.ping()

            except Exception as e:
                logger.error(f"WS error: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)

        # Shutdown
        logger.info("Shutting down — cancelling all orders")
        for coin in list(self.active_coins):
            await self._cancel_all_orders(coin)

    async def _get_monitoring_universe(self) -> set:
        """Get top N pairs by volume above $500K."""
        meta = await asyncio.to_thread(self.info.meta_and_asset_ctxs)
        if not meta or len(meta) != 2:
            return set()

        pairs = []
        blocked = {"BTC", "ETH", "SOL", "HYPE", "PURR"}
        for u, ctx in zip(meta[0]["universe"], meta[1]):
            coin = u["name"]
            if coin in blocked:
                continue
            vol = float(ctx.get("dayNtlVlm", 0) or 0)
            if vol >= 500_000:
                pairs.append((coin, vol))

        pairs.sort(key=lambda x: x[1], reverse=True)
        return set(c for c, _ in pairs[:self.monitor_count])

    def stop(self):
        self.running = False
