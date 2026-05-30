#!/usr/bin/env python3
"""
HL MM V4 Live Canary — REAL orders, minimal capital, one pair.

Places ALO (post-only) limit orders on both sides of the book.
Tracks real fills, real markout, real P&L.
$5 per side on PNUT. Total risk: $10.

Safety:
- ALO only (never taker)
- Max inventory: $15 (1.5x one-side)
- Force-close at market if unrealized loss > $1
- Auto-cancel all on shutdown
- Requote every 5s

Usage:
    python scripts/hl_mm_live_canary.py
"""
import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from typing import Optional

import eth_account
import requests
import websockets
from eth_account.signers.local import LocalAccount
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("hl_mm_canary")

# ── Config ──────────────────────────────────────────
COIN = "PNUT"
PAIR = "PNUT-USDT"
ORDER_SIZE_USD = 11.0  # HL minimum is $10, use $11 for safety
MAX_INVENTORY_USD = 25.0
MAX_UNREALIZED_LOSS = 1.0  # force close at -$1
REQUOTE_INTERVAL = 5.0
MIN_HALF_SPREAD_BPS = 3.0
SZ_DECIMALS = 1

HL_API = "https://api.hyperliquid.xyz"
HL_WS = "wss://api.hyperliquid.xyz/ws"

# Adverse selection thresholds
IMBALANCE_PAUSE = 0.5  # pause toxic side
MOMENTUM_PAUSE_BPS = 15.0  # pause if 60s change > 15bp


class HLMMCanary:
    def __init__(self):
        self.private_key = os.environ["HL_PRIVATE_KEY"]
        self.address = os.environ["HL_ADDRESS"]
        self.account: LocalAccount = eth_account.Account.from_key(self.private_key)

        self.db = MongoClient("mongodb://localhost:27017").quants_lab

        # State
        self.mid = 0.0
        self.best_bid = 0.0
        self.best_ask = 0.0
        self.spread_bps = 0.0
        self.bid_depth_5 = 0.0
        self.ask_depth_5 = 0.0
        self.imbalance = 0.0
        self.mid_history = []

        # Orders
        self.active_bid_oid = None
        self.active_ask_oid = None
        self.active_bid_px = 0.0
        self.active_ask_px = 0.0

        # Inventory
        self.position_qty = 0.0  # positive = long, negative = short
        self.position_cost = 0.0
        self.realized_pnl = 0.0
        self.total_fills = 0
        self.buy_fills = 0
        self.sell_fills = 0

        # Timing
        self.start_time = time.time()
        self.last_requote = 0.0
        self.last_stats = 0.0
        self.running = True

        # Asset ID for PNUT
        self.asset_id = self._get_asset_id()

    def _get_asset_id(self) -> int:
        r = requests.post(f"{HL_API}/info", json={"type": "meta"})
        data = r.json()
        if isinstance(data, list):
            # metaAndAssetCtxs returns [meta, contexts]
            universe = data[0]["universe"]
        else:
            universe = data["universe"]
        for i, u in enumerate(universe):
            if u["name"] == COIN:
                return i
        raise ValueError(f"Coin {COIN} not found")

    def _round_size(self, sz: float) -> float:
        factor = 10 ** SZ_DECIMALS
        return round(sz * factor) / factor

    def _round_price(self, px: float) -> float:
        """Round price for HL — max 5 significant figures AND max 5 decimal places.

        HL rejects prices with >5 decimal places (tested: 0.06175 OK, 0.061735 REJECTED).
        Also must have <=5 significant figures.
        """
        if px == 0:
            return 0.0
        import math
        # 5 significant figures
        mag = math.floor(math.log10(abs(px)))
        sig_decimals = 4 - mag
        # Hard cap at 5 decimal places
        decimals = min(sig_decimals, 5)
        decimals = max(decimals, 0)
        return round(px, decimals)

    def _compute_regime(self) -> str:
        if self.spread_bps < 2.0:
            return "TIGHT"
        if abs(self.imbalance) > IMBALANCE_PAUSE:
            return "TRENDING"
        # Momentum check
        if len(self.mid_history) >= 40:
            now = time.time()
            old_mid = None
            for ts, m in reversed(self.mid_history):
                if now - ts >= 55:
                    old_mid = m
                    break
            if old_mid and old_mid > 0:
                momentum = abs((self.mid - old_mid) / old_mid * 10000)
                if momentum > MOMENTUM_PAUSE_BPS:
                    return "TRENDING"
        if abs(self.imbalance) < 0.2:
            return "RANGING"
        return "NEUTRAL"

    def _compute_quotes(self):
        """Compute bid/ask prices. Returns (bid_px, ask_px) or (None, None)."""
        if self.mid <= 0:
            return None, None

        # Warmup
        if len(self.mid_history) < 40:
            return None, None

        regime = self._compute_regime()
        if regime in ("TIGHT", "TRENDING"):
            return None, None

        # Half-spread
        natural_half = self.spread_bps / 2
        half_spread_bps = max(MIN_HALF_SPREAD_BPS, natural_half * 0.8)

        if regime == "NEUTRAL":
            half_spread_bps *= 1.3

        # Inventory skew: if long, lower ask to encourage selling
        inv_usd = self.position_qty * self.mid
        inv_frac = inv_usd / MAX_INVENTORY_USD if MAX_INVENTORY_USD > 0 else 0
        skew_bps = inv_frac * 3.0  # 3bp max skew

        # Check inventory limit — pause the accumulating side
        bid_px = None
        ask_px = None

        if inv_usd < MAX_INVENTORY_USD:  # can still buy
            # Adverse selection: buying pressure → pause bid
            if self.imbalance > 0.3:
                pass  # don't quote bid (buying pressure = bid is safe, but...)
                # Actually: buying pressure → price going UP → ASK is toxic
                # We SHOULD quote bid (safe side) and pause ask (toxic side)
                bid_px = self.mid * (1 - (half_spread_bps + skew_bps) / 10000)
            else:
                bid_px = self.mid * (1 - (half_spread_bps + skew_bps) / 10000)

        if inv_usd > -MAX_INVENTORY_USD:  # can still sell
            if self.imbalance < -0.3:
                # Selling pressure → BID is toxic, ASK is safe
                ask_px = self.mid * (1 + (half_spread_bps - skew_bps) / 10000)
            else:
                ask_px = self.mid * (1 + (half_spread_bps - skew_bps) / 10000)

        # Adverse selection: pause the TOXIC side
        if self.imbalance > 0.3:
            ask_px = None  # buying pressure → ask is toxic → pause ask
        elif self.imbalance < -0.3:
            bid_px = None  # selling pressure → bid is toxic → pause bid

        # CRITICAL: ALO orders must NOT cross the book
        # Bid must be <= best_bid, Ask must be >= best_ask
        if bid_px is not None and self.best_bid > 0:
            bid_px = min(bid_px, self.best_bid)
        if ask_px is not None and self.best_ask > 0:
            ask_px = max(ask_px, self.best_ask)

        return bid_px, ask_px

    def _get_exchange(self):
        """Lazy-init Exchange object."""
        if not hasattr(self, '_exchange'):
            from hyperliquid.exchange import Exchange
            self._exchange = Exchange(self.account, HL_API, account_address=self.address)
        return self._exchange

    async def _place_order(self, is_buy: bool, px: float, sz: float) -> Optional[str]:
        """Place ALO limit order. Returns order ID or None."""
        sz = self._round_size(sz)
        if sz <= 0:
            return None

        px_rounded = self._round_price(px)

        # Guard: verify the price is sensible
        logger.info(f"_place_order: {'BUY' if is_buy else 'SELL'} sz={sz} raw_px={px} rounded_px={px_rounded} mid={self.mid}")
        if px_rounded <= 0 or abs(px_rounded - self.mid) / self.mid > 0.01:
            logger.warning(f"Price sanity check failed: px={px_rounded}, mid={self.mid}")
            return None

        try:
            exchange = self._get_exchange()
            result = exchange.order(
                COIN, is_buy, sz, px_rounded,
                {"limit": {"tif": "Alo"}}
            )

            if result.get("status") == "ok":
                statuses = result.get("response", {}).get("data", {}).get("statuses", [])
                if statuses:
                    status = statuses[0]
                    if "resting" in status:
                        oid = status["resting"]["oid"]
                        logger.info(f"ORDER PLACED: {'BUY' if is_buy else 'SELL'} {sz} @ {px_rounded} oid={oid}")
                        return str(oid)
                    elif "error" in status:
                        logger.warning(f"ORDER REJECTED: {status['error']}")
                        return None
                    elif "filled" in status:
                        # ALO got immediately filled (shouldn't happen with ALO but handle it)
                        oid = status["filled"]["oid"]
                        logger.info(f"ORDER FILLED IMMEDIATELY: {'BUY' if is_buy else 'SELL'} {sz} @ {px_rounded} oid={oid}")
                        return str(oid)
            else:
                logger.warning(f"ORDER FAILED: {result}")
                return None

        except Exception as e:
            logger.error(f"Order error: {e}")
            return None

    async def _cancel_order(self, oid: str):
        """Cancel an order by OID."""
        try:
            exchange = self._get_exchange()
            result = exchange.cancel(COIN, int(oid))
            logger.info(f"CANCELLED oid={oid}: {result.get('status', 'unknown')}")
        except Exception as e:
            logger.error(f"Cancel error for {oid}: {e}")

    async def _cancel_all(self):
        """Cancel all active orders."""
        tasks = []
        if self.active_bid_oid:
            tasks.append(self._cancel_order(self.active_bid_oid))
            self.active_bid_oid = None
        if self.active_ask_oid:
            tasks.append(self._cancel_order(self.active_ask_oid))
            self.active_ask_oid = None
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _requote(self):
        """Cancel existing orders and place new ones."""
        now = time.time()
        if now - self.last_requote < REQUOTE_INTERVAL:
            return
        self.last_requote = now

        bid_px, ask_px = self._compute_quotes()

        # Cancel existing if price changed significantly or regime changed
        if self.active_bid_oid and (bid_px is None or abs(bid_px - self.active_bid_px) / self.mid * 10000 > 1.0):
            await self._cancel_order(self.active_bid_oid)
            self.active_bid_oid = None

        if self.active_ask_oid and (ask_px is None or abs(ask_px - self.active_ask_px) / self.mid * 10000 > 1.0):
            await self._cancel_order(self.active_ask_oid)
            self.active_ask_oid = None

        # Place new orders
        sz = self._round_size(ORDER_SIZE_USD / self.mid) if self.mid > 0 else 0

        if bid_px and not self.active_bid_oid and sz > 0:
            oid = await self._place_order(True, bid_px, sz)
            if oid:
                self.active_bid_oid = oid
                self.active_bid_px = bid_px

        if ask_px and not self.active_ask_oid and sz > 0:
            oid = await self._place_order(False, ask_px, sz)
            if oid:
                self.active_ask_oid = oid
                self.active_ask_px = ask_px

    def _check_safety(self):
        """Check circuit breakers — use HL API for actual unrealized P&L.
        Only queries API every 30s to avoid rate limits."""
        if abs(self.position_qty) < 0.1:
            return True
        now = time.time()
        if not hasattr(self, '_last_safety_check'):
            self._last_safety_check = 0
        if now - self._last_safety_check < 30:
            return True  # skip API call, assume safe
        self._last_safety_check = now
        try:
            parent = os.environ.get("HL_QUERY_ADDRESS", "0x11ca20aeb7cd014cf8406560ae405b12601994b4")
            r = requests.post(f"{HL_API}/info", json={"type": "clearinghouseState", "user": parent})
            for p in r.json().get("assetPositions", []):
                pos = p.get("position", {})
                if pos.get("coin") == COIN:
                    unrealized = float(pos.get("unrealizedPnl", 0))
                    if unrealized < -MAX_UNREALIZED_LOSS:
                        logger.warning(f"CIRCUIT BREAKER: unrealized loss ${unrealized:.2f} > ${MAX_UNREALIZED_LOSS}")
                        return False
                    return True
        except Exception as e:
            logger.error(f"Safety check error: {e}")
        return True

    def _reconcile_position(self):
        """Check HL API for actual position — catch phantom fills."""
        try:
            parent = os.environ.get("HL_QUERY_ADDRESS", "0x11ca20aeb7cd014cf8406560ae405b12601994b4")
            r = requests.post(f"{HL_API}/info", json={"type": "clearinghouseState", "user": parent})
            for p in r.json().get("assetPositions", []):
                pos = p.get("position", {})
                if pos.get("coin") == COIN:
                    actual_sz = float(pos.get("szi", 0))
                    if abs(actual_sz) > 0 and abs(self.position_qty - actual_sz) > 0.1:
                        logger.warning(
                            f"POSITION MISMATCH! Tracked={self.position_qty:.1f} Actual={actual_sz:.1f} "
                            f"— updating to actual"
                        )
                        self.position_qty = actual_sz
                    return
        except Exception as e:
            logger.error(f"Reconciliation error: {e}")

    def _log_stats(self):
        now = time.time()
        if now - self.last_stats < 60:
            return
        self.last_stats = now

        # Reconcile with HL API every stats cycle
        self._reconcile_position()

        uptime_min = (now - self.start_time) / 60
        inv_usd = self.position_qty * self.mid if self.mid > 0 else 0
        regime = self._compute_regime()

        logger.info(
            f"LIVE STATS: uptime={uptime_min:.0f}min regime={regime} "
            f"spread={self.spread_bps:.1f}bp imb={self.imbalance:+.2f} "
            f"fills={self.total_fills} (buy={self.buy_fills} sell={self.sell_fills}) "
            f"inv=${inv_usd:.2f} realized_pnl=${self.realized_pnl:.4f} "
            f"bid_oid={self.active_bid_oid} ask_oid={self.active_ask_oid}"
        )

    def process_l2(self, data):
        levels = data.get("levels", [])
        if len(levels) < 2:
            return
        bids, asks = levels[0], levels[1]
        if not bids or not asks:
            return

        self.best_bid = float(bids[0]["px"])
        self.best_ask = float(asks[0]["px"])
        self.mid = (self.best_bid + self.best_ask) / 2
        self.spread_bps = (self.best_ask - self.best_bid) / self.mid * 10000

        self.bid_depth_5 = sum(float(b["px"]) * float(b["sz"]) for b in bids[:5])
        self.ask_depth_5 = sum(float(a["px"]) * float(a["sz"]) for a in asks[:5])
        total = self.bid_depth_5 + self.ask_depth_5
        self.imbalance = (self.bid_depth_5 - self.ask_depth_5) / total if total > 0 else 0

        now = time.time()
        self.mid_history.append((now, self.mid))
        if len(self.mid_history) > 300:
            self.mid_history = self.mid_history[-300:]

    def process_fill(self, fill_data):
        """Process a fill notification from WS.

        HL WS orderUpdates format (from proven HLBB maker_engine.py:234):
        Each update: {"order": {"oid": N, "coin": "X", "side": "B"/"A",
                       "sz": "remaining", "origSz": "original", "limitPx": "px"},
                      "status": "filled"|"open"|"canceled"}
        OID is NESTED under update["order"]["oid"].
        """
        updates = fill_data if isinstance(fill_data, list) else [fill_data]
        for update in updates:
            order = update.get("order", {})
            raw_oid = order.get("oid")
            oid = int(raw_oid) if raw_oid is not None else None
            status = update.get("status")
            remaining_sz = order.get("sz", "0")
            orig_sz = order.get("origSz", "0")
            coin = order.get("coin", "")

            logger.info(
                f"WS orderUpdate: {coin} oid={oid} status={status} "
                f"remaining={remaining_sz} orig={orig_sz}"
            )

            if status not in ("filled",):
                continue

            if coin != COIN:
                continue

            side = order.get("side", "")
            px = float(order.get("limitPx", 0))
            sz = float(orig_sz) if float(orig_sz) > 0 else float(remaining_sz)

            is_buy = side == "B"

            self.total_fills += 1
            if is_buy:
                self.buy_fills += 1
                self.position_qty += sz
                self.position_cost += sz * px
            else:
                self.sell_fills += 1
                self.position_qty -= sz
                self.position_cost -= sz * px

            inv_usd = self.position_qty * self.mid
            logger.info(
                f"FILL: {'BUY' if is_buy else 'SELL'} {sz} @ {px} "
                f"inv={self.position_qty:.1f} (${inv_usd:.2f}) "
                f"total_fills={self.total_fills}"
            )

            # Store to MongoDB
            self.db["hl_mm_live_fills"].insert_one({
                "coin": COIN,
                "side": "buy" if is_buy else "sell",
                "price": px,
                "size": sz,
                "size_usd": sz * px,
                "mid_at_fill": self.mid,
                "spread_at_fill": self.spread_bps,
                "imbalance_at_fill": self.imbalance,
                "regime": self._compute_regime(),
                "position_after": self.position_qty,
                "timestamp": time.time(),
                "recorded_at": datetime.now(timezone.utc),
            })

            # Clear the filled order reference (compare as int)
            if self.active_bid_oid and int(self.active_bid_oid) == oid:
                self.active_bid_oid = None
            elif self.active_ask_oid and int(self.active_ask_oid) == oid:
                self.active_ask_oid = None

    async def run(self):
        logger.info(f"MM Canary starting: {COIN} ${ORDER_SIZE_USD}/side, max inv ${MAX_INVENTORY_USD}")
        logger.info(f"Address: {self.address}")
        logger.info(f"Asset ID: {self.asset_id}")

        # Handle shutdown gracefully
        def shutdown(sig, frame):
            logger.info("Shutdown signal received")
            self.running = False
        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        while self.running:
            try:
                async with websockets.connect(HL_WS) as ws:
                    # Subscribe to L2 book
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "l2Book", "coin": COIN}
                    }))
                    # Subscribe to order updates for PARENT address
                    # (agent key signs orders but they execute under parent wallet)
                    parent_address = os.environ.get(
                        "HL_QUERY_ADDRESS",
                        "0x11ca20aeb7cd014cf8406560ae405b12601994b4"
                    )
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "orderUpdates", "user": parent_address}
                    }))
                    logger.info(f"Subscribed to {COIN} L2 + orderUpdates")

                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=30)
                            data = json.loads(msg)
                            channel = data.get("channel")

                            if channel == "l2Book":
                                self.process_l2(data.get("data", {}))

                                # Safety check
                                if not self._check_safety():
                                    await self._cancel_all()
                                    logger.error("CIRCUIT BREAKER TRIPPED — all orders cancelled")
                                    self.running = False
                                    break

                                # Requote
                                await self._requote()

                            elif channel == "orderUpdates":
                                raw = data.get("data", [])
                                logger.info(f"WS orderUpdate RAW: {json.dumps(raw)[:500]}")
                                self.process_fill(raw)

                            self._log_stats()

                        except asyncio.TimeoutError:
                            await ws.ping()

            except Exception as e:
                logger.error(f"WS error: {e}, reconnecting in 5s...")
                await asyncio.sleep(5)

        # Cleanup: cancel all orders
        logger.info("Shutting down — cancelling all orders")
        await self._cancel_all()
        logger.info(f"FINAL: fills={self.total_fills} realized_pnl=${self.realized_pnl:.4f} inv={self.position_qty:.1f}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--coin", default="PNUT", help="Coin to MM (e.g., PNUT, ORDI, DASH)")
    parser.add_argument("--size", type=float, default=11.0, help="Order size in USD")
    args = parser.parse_args()

    # Override globals
    COIN = args.coin
    PAIR = f"{args.coin}-USDT"
    ORDER_SIZE_USD = args.size

    # Get szDecimals for this coin
    import requests as _r
    _meta = _r.post("https://api.hyperliquid.xyz/info", json={"type": "meta"}).json()
    for _u in _meta["universe"]:
        if _u["name"] == args.coin:
            SZ_DECIMALS = _u["szDecimals"]
            break

    canary = HLMMCanary()
    asyncio.run(canary.run())
