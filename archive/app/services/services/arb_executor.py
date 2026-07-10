"""
Cross-exchange arb executor — Binance SPOT ↔ Bybit PERP.

Executes both legs of an arb trade and tracks positions until unwind.

Lifecycle:
    1. Screener detects spread > threshold
    2. Executor opens BOTH legs simultaneously
    3. Position manager tracks the hedge pair
    4. When spread converges (or hits stop), executor unwinds both legs
    5. Net P&L = (entry_spread - exit_spread) * size - fees

Execution model:
    - Binance SPOT: MARKET orders (taker 10bp) for guaranteed fill
    - Bybit PERP: MARKET orders (taker 5.5bp) for speed
      (upgrade to LIMIT/maker once system is proven)
"""
import asyncio
import hashlib
import hmac
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

import aiohttp
from pymongo import MongoClient

from app.services.binance_client import BinanceClient
from app.services.arb_notifier import notify_open, notify_close
from app.services.arb_safety import OrphanDetector

logger = logging.getLogger(__name__)


class ArbDirection(str, Enum):
    """Which exchange has the cheaper price."""
    BUY_BN_SELL_BB = "BUY_BN_SELL_BB"  # Binance spot cheap, Bybit perp expensive
    BUY_BB_SELL_BN = "BUY_BB_SELL_BN"  # Bybit perp cheap, Binance spot expensive


class PositionStatus(str, Enum):
    PENDING = "PENDING"
    OPEN = "OPEN"
    UNWINDING = "UNWINDING"
    PENDING_UNWIND = "PENDING_UNWIND"  # unwind queued but not yet executed
    CLOSED = "CLOSED"
    FAILED = "FAILED"


@dataclass
class ArbPosition:
    """A hedged position across two exchanges."""
    position_id: str
    symbol: str
    pair: str
    direction: ArbDirection
    status: PositionStatus = PositionStatus.PENDING

    # Entry
    entry_time: float = 0
    entry_spread_bps: float = 0
    target_qty: float = 0       # base asset quantity
    target_usd: float = 0       # notional USD

    # Binance leg
    bn_side: str = ""           # BUY or SELL
    bn_order_id: Optional[int] = None
    bn_fill_price: float = 0
    bn_fill_qty: float = 0
    bn_fee: float = 0
    bn_status: str = ""

    # Bybit leg
    bb_side: str = ""           # Buy or Sell
    bb_order_id: Optional[str] = None
    bb_fill_price: float = 0
    bb_fill_qty: float = 0
    bb_fee: float = 0
    bb_status: str = ""

    # Exit / Unwind
    exit_time: float = 0
    exit_spread_bps: float = 0
    bn_exit_order_id: Optional[int] = None
    bb_exit_order_id: Optional[str] = None
    bn_exit_price: float = 0
    bb_exit_price: float = 0

    # P&L
    gross_pnl: float = 0
    total_fees: float = 0
    net_pnl: float = 0

    # Metadata
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    error: str = ""


class RiskManager:
    """Pre-trade risk checks."""

    def __init__(
        self,
        max_position_usd: float = 300,
        max_total_exposure_usd: float = 2000,
        max_concurrent_positions: int = 10,
        max_position_per_symbol: int = 1,
        min_net_spread_bps: float = 5.0,
    ):
        self.max_position_usd = max_position_usd
        self.max_total_exposure_usd = max_total_exposure_usd
        self.max_concurrent_positions = max_concurrent_positions
        self.max_position_per_symbol = max_position_per_symbol
        self.min_net_spread_bps = min_net_spread_bps

    def check(
        self,
        symbol: str,
        usd_size: float,
        net_spread_bps: float,
        open_positions: list[ArbPosition],
    ) -> tuple[bool, str]:
        """Return (allowed, reason)."""
        if net_spread_bps < self.min_net_spread_bps:
            return False, f"Spread {net_spread_bps:.1f}bp < min {self.min_net_spread_bps}bp"

        if usd_size > self.max_position_usd:
            return False, f"Size ${usd_size:.0f} > max ${self.max_position_usd:.0f}"

        active = [p for p in open_positions if p.status == PositionStatus.OPEN]

        if len(active) >= self.max_concurrent_positions:
            return False, f"Max concurrent positions ({self.max_concurrent_positions}) reached"

        symbol_count = sum(1 for p in active if p.symbol == symbol)
        if symbol_count >= self.max_position_per_symbol:
            return False, f"Already have {symbol_count} position(s) on {symbol}"

        total_exposure = sum(p.target_usd for p in active) + usd_size
        if total_exposure > self.max_total_exposure_usd:
            return False, f"Total exposure ${total_exposure:.0f} > max ${self.max_total_exposure_usd:.0f}"

        return True, "OK"


class ArbExecutor:
    """Executes and manages cross-exchange arb positions."""

    def __init__(
        self,
        binance: BinanceClient,
        risk: RiskManager | None = None,
        mongo_uri: str | None = None,
        mongo_db: str | None = None,
        paper_mode: bool = True,
    ):
        self.binance = binance
        self.risk = risk or RiskManager()
        self.paper_mode = paper_mode
        self.orphan_detector = OrphanDetector()

        # Bybit real credentials
        self.bb_api_key = os.getenv("BYBIT_API_KEY", "")
        self.bb_api_secret = os.getenv("BYBIT_API_SECRET", "")
        self.bb_base_url = "https://api.bybit.com"
        self._bb_session: Optional[aiohttp.ClientSession] = None

        # MongoDB
        uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017/quants_lab")
        db_name = mongo_db or os.getenv("MONGO_DATABASE", "quants_lab")
        self._mongo = MongoClient(uri)
        self._db = self._mongo[db_name]
        self._positions_coll = self._db["arb_positions"]
        self._positions_coll.create_index([("symbol", 1), ("status", 1)])
        self._positions_coll.create_index([("created_at", -1)])
        self._trades_coll = self._db["arb_trades"]

        # In-memory position tracking
        self.positions: list[ArbPosition] = []
        self._load_open_positions()

    def _load_open_positions(self):
        """Load OPEN and PENDING_UNWIND positions from MongoDB on startup."""
        docs = self._positions_coll.find({"status": {"$in": ["OPEN", "PENDING", "PENDING_UNWIND"]}})
        for doc in docs:
            pos = self._doc_to_position(doc)
            self.positions.append(pos)
        if self.positions:
            logger.info(f"Loaded {len(self.positions)} open arb positions from DB")

    def _doc_to_position(self, doc: dict) -> ArbPosition:
        pos = ArbPosition(
            position_id=doc["position_id"],
            symbol=doc["symbol"],
            pair=doc["pair"],
            direction=ArbDirection(doc["direction"]),
        )
        for key in [
            "status", "entry_time", "entry_spread_bps", "target_qty", "target_usd",
            "bn_side", "bn_order_id", "bn_fill_price", "bn_fill_qty", "bn_fee", "bn_status",
            "bb_side", "bb_order_id", "bb_fill_price", "bb_fill_qty", "bb_fee", "bb_status",
            "exit_time", "exit_spread_bps", "bn_exit_order_id", "bb_exit_order_id",
            "bn_exit_price", "bb_exit_price",
            "gross_pnl", "total_fees", "net_pnl", "created_at", "updated_at", "error",
        ]:
            if key in doc:
                val = doc[key]
                if key == "status":
                    val = PositionStatus(val)
                setattr(pos, key, val)
        return pos

    def _save_position(self, pos: ArbPosition):
        """Upsert position to MongoDB."""
        pos.updated_at = time.time()
        doc = asdict(pos)
        doc["status"] = pos.status.value
        doc["direction"] = pos.direction.value
        doc["updated_at_utc"] = datetime.fromtimestamp(pos.updated_at, tz=timezone.utc)
        self._positions_coll.update_one(
            {"position_id": pos.position_id},
            {"$set": doc},
            upsert=True,
        )

    # ── Bybit order execution ────────────────────────────────

    async def _ensure_bb_session(self):
        if self._bb_session is None or self._bb_session.closed:
            self._bb_session = aiohttp.ClientSession()

    async def _bybit_order(
        self, symbol: str, side: str, qty: float, order_type: str = "Market"
    ) -> dict:
        """Place a Bybit linear perp order."""
        await self._ensure_bb_session()

        params = {
            "category": "linear",
            "symbol": symbol,
            "side": side,       # "Buy" or "Sell"
            "orderType": order_type,
            "qty": str(qty),
            "timeInForce": "GTC" if order_type == "Limit" else "IOC",
        }
        body = str(params).replace("'", '"')  # Bybit wants JSON string for signing

        import json
        body_json = json.dumps(params)
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        sign_str = ts + self.bb_api_key + recv_window + body_json
        sig = hmac.new(
            self.bb_api_secret.encode(), sign_str.encode(), hashlib.sha256
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.bb_api_key,
            "X-BAPI-SIGN": sig,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv_window,
            "Content-Type": "application/json",
        }

        async with self._bb_session.post(
            f"{self.bb_base_url}/v5/order/create",
            headers=headers,
            data=body_json,
        ) as resp:
            data = await resp.json()
            if data["retCode"] != 0:
                raise Exception(f"Bybit order error: [{data['retCode']}] {data['retMsg']}")
            return data["result"]

    # ── Trade execution ──────────────────────────────────────

    async def open_position(
        self,
        symbol: str,
        direction: ArbDirection,
        usd_size: float,
        entry_spread_bps: float,
        bn_price: float,
        bb_price: float,
    ) -> ArbPosition:
        """Open a hedged arb position on both exchanges."""
        pair = symbol.replace("USDT", "") + "-USDT"
        pos_id = f"arb_{symbol}_{int(time.time()*1000)}"

        # Compute quantity (base asset)
        ref_price = (bn_price + bb_price) / 2
        qty = usd_size / ref_price

        pos = ArbPosition(
            position_id=pos_id,
            symbol=symbol,
            pair=pair,
            direction=direction,
            entry_time=time.time(),
            entry_spread_bps=entry_spread_bps,
            target_qty=qty,
            target_usd=usd_size,
        )

        # Risk check — rejections are NOT saved to DB (not real failures)
        allowed, reason = self.risk.check(symbol, usd_size, entry_spread_bps, self.positions)
        if not allowed:
            pos.status = PositionStatus.FAILED
            pos.error = f"Risk rejected: {reason}"
            logger.debug(f"ARB REJECTED {symbol}: {reason}")
            return pos  # don't save to DB or positions list

        # Set sides based on direction
        if direction == ArbDirection.BUY_BN_SELL_BB:
            pos.bn_side = "BUY"    # Buy spot on Binance
            pos.bb_side = "Sell"   # Short perp on Bybit
        else:  # BUY_BB_SELL_BN
            pos.bn_side = "SELL"   # Sell spot on Binance
            pos.bb_side = "Buy"    # Long perp on Bybit

        if self.paper_mode:
            # Paper trade: simulate fills at current prices
            # Fee assumptions (verified against actual account tiers 2026-04-18):
            #   Binance spot VIP0: 10bp (maker=taker at this tier, LIMIT saves nothing)
            #   Bybit perp LIMIT:   2bp maker (vs 5.5bp taker — use LIMIT orders)
            #   Round-trip:         24bp total (10+10+2+2)
            # Further reduction possible: buy BNB for 25% discount → 19bp RT
            pos.bn_fill_price = bn_price
            pos.bn_fill_qty = qty
            pos.bn_fee = usd_size * 0.001  # 10bp (VIP0, no BNB discount)
            pos.bn_status = "FILLED_PAPER"

            pos.bb_fill_price = bb_price
            pos.bb_fill_qty = qty
            pos.bb_fee = usd_size * 0.0002  # 2bp (LIMIT/maker on Bybit)
            pos.bb_status = "FILLED_PAPER"

            pos.total_fees = pos.bn_fee + pos.bb_fee
            pos.status = PositionStatus.OPEN

            logger.info(
                f"📝 PAPER OPEN {symbol} | {direction.value} | "
                f"${usd_size:.0f} | spread={entry_spread_bps:.1f}bp | "
                f"fees=${pos.total_fees:.4f}"
            )
            notify_open(symbol, direction.value, entry_spread_bps, usd_size, paper=True)
        else:
            # LIVE execution: sequential legs with orphan detection.
            # Binance first (spot, no funding risk). If it fails, nothing to unwind.
            # If Binance fills but Bybit fails, OrphanDetector triggers rollback.
            try:
                await self._execute_binance_leg(pos, qty)
            except Exception as e:
                pos.status = PositionStatus.FAILED
                pos.error = f"Binance leg failed (no orphan): {e}"
                logger.error(f"❌ BN LEG FAILED {symbol}: {e}")
                self.positions.append(pos)
                self._save_position(pos)
                return pos

            # Binance filled. Now try Bybit.
            try:
                await self._execute_bybit_leg(pos, qty)
            except Exception as e:
                # ORPHAN: Binance filled, Bybit failed. Detect and handle.
                recovery = self.orphan_detector.check_fill_pair(
                    pos.bn_status, pos.bb_status, symbol, direction.value
                )
                if recovery:
                    logger.error(
                        f"🚨 ORPHAN {symbol}: Bybit failed after Binance fill. "
                        f"Recovery action: {recovery['type']} — unwinding Binance leg"
                    )
                    # Attempt immediate rollback of the Binance leg
                    try:
                        reverse_side = "SELL" if pos.bn_side == "BUY" else "BUY"
                        await self.binance.place_market_order(
                            symbol, reverse_side, quantity=pos.bn_fill_qty
                        )
                        logger.info(f"✅ Orphan rollback executed: {reverse_side} {pos.bn_fill_qty} {symbol}")
                    except Exception as rb_err:
                        logger.error(f"🚨🚨 ORPHAN ROLLBACK FAILED {symbol}: {rb_err}")

                pos.status = PositionStatus.FAILED
                pos.error = f"Bybit leg failed (orphan handled): {e}"
                logger.error(f"❌ BB LEG FAILED {symbol}: {e}")
                self.positions.append(pos)
                self._save_position(pos)
                return pos

            # Both legs filled
            pos.total_fees = pos.bn_fee + pos.bb_fee
            pos.status = PositionStatus.OPEN

            logger.info(
                f"🔥 LIVE OPEN {symbol} | {direction.value} | "
                f"BN {pos.bn_side} @ {pos.bn_fill_price:.6f} | "
                f"BB {pos.bb_side} @ {pos.bb_fill_price:.6f} | "
                f"fees=${pos.total_fees:.4f}"
            )

        self.positions.append(pos)
        self._save_position(pos)
        return pos

    async def _execute_binance_leg(self, pos: ArbPosition, qty: float):
        """Execute the Binance spot leg."""
        try:
            if pos.bn_side == "BUY":
                result = await self.binance.place_market_order(
                    pos.symbol, "BUY", quote_quantity=pos.target_usd
                )
            else:
                result = await self.binance.place_market_order(
                    pos.symbol, "SELL", quantity=qty
                )

            # Parse fill info
            fills = result.get("fills", [])
            if fills:
                total_qty = sum(float(f["qty"]) for f in fills)
                total_cost = sum(float(f["qty"]) * float(f["price"]) for f in fills)
                pos.bn_fill_price = total_cost / total_qty if total_qty > 0 else 0
                pos.bn_fill_qty = total_qty
                pos.bn_fee = sum(float(f.get("commission", 0)) for f in fills)
            else:
                pos.bn_fill_price = float(result.get("price", 0))
                pos.bn_fill_qty = float(result.get("executedQty", 0))

            pos.bn_order_id = result.get("orderId")
            pos.bn_status = result.get("status", "UNKNOWN")

        except Exception as e:
            pos.bn_status = f"FAILED: {e}"
            raise

    async def _execute_bybit_leg(self, pos: ArbPosition, qty: float):
        """Execute the Bybit perp leg and fetch actual fill data."""
        try:
            result = await self._bybit_order(pos.symbol, pos.bb_side, qty)
            order_id = result.get("orderId")
            pos.bb_order_id = order_id
            pos.bb_status = "SUBMITTED"

            # Fetch actual fill from Bybit order query (market orders fill instantly)
            await asyncio.sleep(0.5)  # brief wait for fill to settle
            fill_data = await self._bybit_get_order(pos.symbol, order_id)

            if fill_data and float(fill_data.get("cumExecQty", 0)) > 0:
                pos.bb_fill_price = float(fill_data["avgPrice"])
                pos.bb_fill_qty = float(fill_data["cumExecQty"])
                pos.bb_fee = float(fill_data.get("cumExecFee", 0))
                pos.bb_status = fill_data.get("orderStatus", "FILLED")
                logger.info(
                    f"BB fill parsed: {pos.bb_fill_qty} @ {pos.bb_fill_price:.6f}, "
                    f"fee=${pos.bb_fee:.6f}"
                )
            else:
                # Fallback: estimate if query fails (log warning)
                logger.warning(f"BB fill query returned no data for {order_id}, using estimate")
                pos.bb_fill_price = pos.target_usd / qty
                pos.bb_fill_qty = qty
                pos.bb_fee = pos.target_usd * 0.00055  # taker fee estimate

        except Exception as e:
            pos.bb_status = f"FAILED: {e}"
            raise

    async def _bybit_get_order(self, symbol: str, order_id: str) -> dict | None:
        """Query Bybit for actual fill data of an order."""
        await self._ensure_bb_session()

        params = {
            "category": "linear",
            "symbol": symbol,
            "orderId": order_id,
        }

        import json
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        query_str = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
        sign_str = ts + self.bb_api_key + recv_window + query_str
        sig = hmac.new(
            self.bb_api_secret.encode(), sign_str.encode(), hashlib.sha256
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self.bb_api_key,
            "X-BAPI-SIGN": sig,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv_window,
        }

        try:
            async with self._bb_session.get(
                f"{self.bb_base_url}/v5/order/realtime?{query_str}",
                headers=headers,
            ) as resp:
                data = await resp.json()
                if data["retCode"] == 0 and data["result"]["list"]:
                    return data["result"]["list"][0]
                return None
        except Exception as e:
            logger.warning(f"Failed to query Bybit order {order_id}: {e}")
            return None

    # ── Position unwinding ───────────────────────────────────

    async def close_position(
        self, pos: ArbPosition, bn_price: float, bb_price: float,
        bn_ticker: dict = None, bb_ticker: dict = None,
    ) -> ArbPosition:
        """Unwind both legs of an arb position.

        bn_price / bb_price are mid prices (for P&L estimation).
        bn_ticker / bb_ticker (optional) carry bid/ask for accurate exit spread.
        """
        pos.status = PositionStatus.UNWINDING

        if self.paper_mode:
            pos.bn_exit_price = bn_price
            pos.bb_exit_price = bb_price
            pos.exit_time = time.time()

            # Calculate P&L
            if pos.direction == ArbDirection.BUY_BN_SELL_BB:
                # Entry: bought BN, sold BB. Exit: sell BN, buy BB.
                bn_pnl = (bn_price - pos.bn_fill_price) * pos.bn_fill_qty
                bb_pnl = (pos.bb_fill_price - bb_price) * pos.bb_fill_qty
            else:
                # Entry: sold BN, bought BB. Exit: buy BN, sell BB.
                bn_pnl = (pos.bn_fill_price - bn_price) * pos.bn_fill_qty
                bb_pnl = (bb_price - pos.bb_fill_price) * pos.bb_fill_qty

            exit_fees = pos.target_usd * (0.001 + 0.0002)  # exit leg fees (BN 10bp + BB 2bp maker)
            pos.total_fees += exit_fees
            pos.gross_pnl = bn_pnl + bb_pnl
            pos.net_pnl = pos.gross_pnl - pos.total_fees

            # Exit spread: use bid/ask if available (consistent with
            # _compute_current_spread in arb_engine), fall back to mid prices
            if bn_ticker and bb_ticker:
                if pos.direction == ArbDirection.BUY_BN_SELL_BB:
                    exit_spread = (bb_ticker["bid"] - bn_ticker["ask"]) / bn_ticker["ask"] * 10000
                else:
                    exit_spread = (bn_ticker["bid"] - bb_ticker["ask"]) / bb_ticker["ask"] * 10000
            else:
                if pos.direction == ArbDirection.BUY_BN_SELL_BB:
                    exit_spread = (bb_price - bn_price) / bn_price * 10000
                else:
                    exit_spread = (bn_price - bb_price) / bb_price * 10000
            pos.exit_spread_bps = exit_spread

            pos.status = PositionStatus.CLOSED
            logger.info(
                f"📝 PAPER CLOSE {pos.symbol} | "
                f"entry={pos.entry_spread_bps:.1f}bp exit={exit_spread:.1f}bp | "
                f"gross=${pos.gross_pnl:.4f} fees=${pos.total_fees:.4f} "
                f"net=${pos.net_pnl:.4f}"
            )
            hold_h = (pos.exit_time - pos.entry_time) / 3600 if pos.exit_time and pos.entry_time else 0
            notify_close(
                pos.symbol, pos.entry_spread_bps, exit_spread,
                pos.net_pnl, hold_h, "REVERTED" if exit_spread < pos.entry_spread_bps * 0.5 else "CLOSED",
                paper=True,
            )
        else:
            # LIVE: reverse both legs
            try:
                reverse_bn_side = "SELL" if pos.bn_side == "BUY" else "BUY"
                reverse_bb_side = "Sell" if pos.bb_side == "Buy" else "Buy"

                bn_task = self.binance.place_market_order(
                    pos.symbol, reverse_bn_side, quantity=pos.bn_fill_qty
                )
                bb_task = self._bybit_order(pos.symbol, reverse_bb_side, pos.bb_fill_qty)

                bn_result, bb_result = await asyncio.gather(bn_task, bb_task)
                pos.exit_time = time.time()
                pos.status = PositionStatus.CLOSED

                # TODO: parse fill prices from results for accurate P&L
                pos.bn_exit_price = bn_price
                pos.bb_exit_price = bb_price

                logger.info(f"🔥 LIVE CLOSE {pos.symbol} | net=${pos.net_pnl:.4f}")
            except Exception as e:
                pos.error = f"Unwind error: {e}"
                logger.error(f"❌ CLOSE FAILED {pos.symbol}: {e}")

        self._save_position(pos)
        return pos

    # ── Position monitoring ──────────────────────────────────

    def get_open_positions(self) -> list[ArbPosition]:
        return [p for p in self.positions if p.status == PositionStatus.OPEN]

    def get_position_summary(self) -> dict:
        """Summary stats for all positions."""
        open_pos = self.get_open_positions()
        closed = [p for p in self.positions if p.status == PositionStatus.CLOSED]
        failed = [p for p in self.positions if p.status == PositionStatus.FAILED]

        total_pnl = sum(p.net_pnl for p in closed)
        total_fees = sum(p.total_fees for p in closed)
        wins = sum(1 for p in closed if p.net_pnl > 0)

        return {
            "open": len(open_pos),
            "closed": len(closed),
            "failed": len(failed),
            "total_pnl": total_pnl,
            "total_fees": total_fees,
            "win_rate": wins / len(closed) if closed else 0,
            "avg_pnl": total_pnl / len(closed) if closed else 0,
            "total_exposure": sum(p.target_usd for p in open_pos),
        }

    async def close(self):
        """Cleanup sessions."""
        if self._bb_session and not self._bb_session.closed:
            await self._bb_session.close()
        await self.binance.close()
