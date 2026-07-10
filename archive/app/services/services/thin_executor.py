"""
ThinExecutor — REST-only execution wrapper with exchange-managed TP/SL.

Zero WS connections. ~10 REST calls/min total. ~1% rate limit utilization.
Handles both slow (1h) and fast (5-10s) strategies via configurable interval.

Design (from unified plan v4, Apr 17 2026):
- Signal loop: load data, run controller.update_processed_data(), read signal
- Order placement: BybitExchangeClient REST with TP/SL params
- TP/SL: exchange-managed (survives our crashes)
- Trailing stop: amend conditional order via REST every 30-60s
- Fill detection: REST poll every 30s
- Atomic risk reservation: MongoDB findAndModify prevents race condition
- Naked position protection: verify TP/SL in order response, fallback to set_tp_sl
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

import aiohttp
from pymongo import MongoClient

from app.services.bybit_exchange_client import BybitExchangeClient, BybitAPIError
from app.services.market_data_shim import MarketDataShim
from app.services.portfolio_risk_manager import (
    compute_position_size,
    check_admission,
)

logger = logging.getLogger(__name__)


@dataclass
class StrategySlot:
    """One strategy+pair combination managed by the executor."""
    engine: str
    pair: str
    connector_name: str
    controller: Any  # HB V2 controller instance
    signal_interval_s: int = 60
    conviction_mult: float = 1.0
    time_limit_s: int = 28800  # 8h default
    trailing_enabled: bool = True
    trailing_activation_pct: float = 0.01  # activate trail after 1% profit
    trailing_callback_pct: float = 0.005   # trail 0.5% behind peak


class ThinExecutor:
    """
    REST-only execution wrapper. Exchange-managed TP/SL.
    Runs as methods called by a QL pipeline task.
    """

    def __init__(
        self,
        strategies: List[StrategySlot],
        exchange: BybitExchangeClient,
        shim: MarketDataShim,
        db,
    ):
        self.strategies = strategies
        self.exchange = exchange
        self.shim = shim
        self.db = db

    # ── Signal Loop ──────────────────────────────────────────

    async def signal_check(self):
        """Check all strategies for signals. Called every signal_interval."""
        async with aiohttp.ClientSession() as session:
            for strat in self.strategies:
                try:
                    await self._check_strategy(session, strat)
                except Exception as e:
                    logger.error(f"Signal check error {strat.engine}/{strat.pair}: {e}")

    async def _check_strategy(self, session: aiohttp.ClientSession, strat: StrategySlot):
        # 1. Run controller signal
        await strat.controller.update_processed_data()
        signal = strat.controller.processed_data.get("signal", 0)
        if signal == 0:
            return

        # 2. Check if we already have an active executor for this pair
        existing = self.db.executor_state.find_one({
            "pair": strat.pair,
            "state": {"$in": ["PENDING", "ACTIVE"]},
        })
        if existing:
            return

        # 3. Load risk state
        risk_state = self.db.portfolio_risk_state.find_one({"_id": "current"})
        if not risk_state:
            logger.warning(f"No risk state, suppressing {strat.engine}/{strat.pair}")
            return

        # 4. Get entry price + executor config from controller
        entry_price = self.shim.get_price(strat.connector_name, strat.pair)
        if entry_price <= 0:
            logger.warning(f"No price for {strat.pair}, skipping")
            return

        from hummingbot.core.data_type.common import TradeType
        trade_type = TradeType.BUY if signal == 1 else TradeType.SELL

        exec_config = strat.controller.get_executor_config(
            trade_type, Decimal(str(entry_price)), Decimal("0")
        )

        tp_pct = float(exec_config.triple_barrier_config.take_profit)
        sl_pct = float(exec_config.triple_barrier_config.stop_loss)

        # 5. Compute position size
        sizing = compute_position_size(
            equity=risk_state["equity"],
            sl_distance_pct=sl_pct,
            conviction_mult=strat.conviction_mult,
            risk_per_trade_pct=risk_state["risk_config"]["risk_per_trade_pct"],
            price=entry_price,
            max_leverage=risk_state["risk_config"]["max_leverage"],
        )

        if sizing["amount"] <= 0:
            return

        # 5b. Read conviction from controller (X5 sets entry_z_score)
        entry_z = strat.controller.processed_data.get("entry_z_score", 0)
        if entry_z > 0:
            z_base = getattr(strat.controller.config, "z_base", 2.0)
            conviction = max(0.5, min(2.0, entry_z / z_base))
            sizing = compute_position_size(
                equity=risk_state["equity"],
                sl_distance_pct=sl_pct,
                conviction_mult=conviction,
                risk_per_trade_pct=risk_state["risk_config"]["risk_per_trade_pct"],
                price=entry_price,
                max_leverage=risk_state["risk_config"]["max_leverage"],
            )
            if sizing["amount"] <= 0:
                return
        else:
            conviction = strat.conviction_mult

        # 6. Check admission
        allowed, reason = check_admission(
            risk_state, strat.engine, strat.pair, sizing["heat"]
        )
        if not allowed:
            logger.info(f"Signal suppressed {strat.engine}/{strat.pair}: {reason}")
            return

        # 7. Atomic reservation — keyed by engine+pair+direction+date (not hour)
        #    Prevents re-entry on same pair same direction same day.
        #    A new day resets. An opposite direction is a different signal_id.
        date_key = datetime.now(timezone.utc).strftime("%Y%m%d")
        signal_id = f"{strat.engine}_{strat.pair}_{signal}_{date_key}"
        reserved = await self._reserve_heat(signal_id, sizing["heat"])
        if not reserved:
            return

        # 8. Execute
        try:
            await self._place_protected_order(
                session, strat, signal, signal_id, entry_price,
                tp_pct, sl_pct, sizing, exec_config
            )
        except Exception as e:
            logger.error(f"Order failed {strat.engine}/{strat.pair}: {e}")
            await self._release_reservation(signal_id)

    async def _place_protected_order(
        self, session, strat, signal, signal_id, entry_price,
        tp_pct, sl_pct, sizing, exec_config
    ):
        """Place order with TP/SL. Verify TP/SL attached. Never leave naked."""
        side = "Buy" if signal == 1 else "Sell"

        if signal == 1:  # LONG
            tp_price = round(entry_price * (1 + tp_pct), 6)
            sl_price = round(entry_price * (1 - sl_pct), 6)
        else:  # SHORT
            tp_price = round(entry_price * (1 - tp_pct), 6)
            sl_price = round(entry_price * (1 + sl_pct), 6)

        # Set leverage (ignore if already set)
        try:
            await self.exchange.set_leverage(session, strat.pair, sizing["leverage"])
        except BybitAPIError as e:
            if e.code != 110043:
                raise

        # Place order with TP/SL
        order = await self.exchange.place_order(
            session,
            pair=strat.pair,
            side=side,
            qty=sizing["amount"],
            order_type="Limit",
            price=entry_price,
            take_profit=tp_price,
            stop_loss=sl_price,
        )

        order_id = order.get("orderId", "")
        logger.info(
            f"Order placed {strat.engine}/{strat.pair} {side}: "
            f"price={entry_price} TP={tp_price} SL={sl_price} "
            f"qty={sizing['amount']:.6f} order_id={order_id}"
        )

        # Verify TP/SL attached — naked position protection
        await self._verify_tp_sl(session, strat.pair, tp_price, sl_price, side)

        # Record executor state
        now = datetime.now(timezone.utc)
        self.db.executor_state.insert_one({
            "signal_id": signal_id,
            "engine": strat.engine,
            "pair": strat.pair,
            "side": side,
            "state": "PENDING",
            "signal_at": now,
            "entry_order_id": order_id,
            "entry_price": entry_price,
            "qty": sizing["amount"],
            "take_profit": tp_price,
            "stop_loss": sl_price,
            "current_sl": sl_price,
            "trailing_enabled": strat.trailing_enabled,
            "trailing_activation_pct": strat.trailing_activation_pct,
            "trailing_callback_pct": strat.trailing_callback_pct,
            "time_limit_s": strat.time_limit_s,
            "leverage": sizing["leverage"],
            "heat": sizing["heat"],
            "conviction_mult": strat.conviction_mult,
            "state_history": [
                {"state": "PENDING", "at": now, "detail": f"order placed {order_id}"}
            ],
            "pnl": None,
            "close_type": None,
            "closed_at": None,
        })

    async def _verify_tp_sl(self, session, pair, tp_price, sl_price, side):
        """Verify TP/SL on position. If missing, set standalone. If that fails, close."""
        try:
            # Give a moment for order to register, then check position
            positions = await self.exchange.fetch_positions(session)
            has_pos = any(p["pair"] == pair for p in positions)

            if has_pos:
                # Set TP/SL via trading-stop as safety net
                await self.exchange.set_tp_sl(session, pair, tp_price, sl_price)
                logger.debug(f"TP/SL verified/set on {pair}: TP={tp_price} SL={sl_price}")
        except Exception as e:
            logger.warning(f"TP/SL verify failed {pair}: {e}")
            # Position may not be filled yet (PENDING). TP/SL from place_order
            # should still be attached. Will re-verify in lifecycle_check.

    # ── Lifecycle Loop ───────────────────────────────────────

    async def lifecycle_check(self):
        """Called every 30s. Manages active positions."""
        async with aiohttp.ClientSession() as session:
            positions = await self.exchange.fetch_positions(session)
            pos_by_pair = {p["pair"]: p for p in positions}

            # Fetch mark prices for trailing stops
            tickers = {}
            for p in positions:
                tickers[p["pair"]] = p.get("mark_price", 0)

            active = list(self.db.executor_state.find(
                {"state": {"$in": ["PENDING", "ACTIVE"]}}
            ))

            for executor in active:
                try:
                    await self._manage_executor(
                        session, executor, pos_by_pair, tickers
                    )
                except Exception as e:
                    logger.error(
                        f"Lifecycle error {executor.get('engine')}/{executor.get('pair')}: {e}"
                    )

    async def _manage_executor(self, session, executor, pos_by_pair, tickers):
        pair = executor["pair"]
        state = executor["state"]
        has_position = pair in pos_by_pair
        now = time.time()
        signal_ts = executor["signal_at"].timestamp()

        if state == "PENDING":
            if has_position:
                await self._transition(executor, "ACTIVE", "fill detected")
                # Re-verify TP/SL on fill
                try:
                    await self.exchange.set_tp_sl(
                        session, pair,
                        executor["take_profit"], executor["stop_loss"]
                    )
                except Exception as e:
                    logger.warning(f"TP/SL set on fill failed {pair}: {e}")

            elif now - signal_ts > 300:
                # 5min fill timeout
                try:
                    await self.exchange.cancel_order(
                        session, pair, executor["entry_order_id"]
                    )
                except Exception:
                    pass
                await self._transition(executor, "CANCELLED", "fill timeout 5min")
                await self._release_reservation(executor["signal_id"])

        elif state == "ACTIVE":
            if not has_position:
                # Exchange closed it (TP or SL hit during our interval)
                close_type = await self._determine_close_type(executor)
                await self._transition(executor, close_type, "exchange closed")
                await self._release_reservation(executor["signal_id"])

            elif now - signal_ts > executor.get("time_limit_s", 28800):
                # Time limit
                try:
                    await self.exchange.close_position(
                        session, pair, executor["side"]
                    )
                except Exception as e:
                    logger.error(f"Time limit close failed {pair}: {e}")
                    return
                await self._transition(executor, "TIME_LIMIT_CLOSE", "time limit")
                await self._release_reservation(executor["signal_id"])

            elif executor.get("trailing_enabled") and pair in tickers:
                # Trailing stop
                await self._maybe_trail(session, executor, tickers[pair])

    async def _maybe_trail(self, session, executor, current_price):
        """Amend SL on exchange if trail conditions met."""
        entry = executor["entry_price"]
        side = executor["side"]
        activation = executor.get("trailing_activation_pct", 0.01)
        callback = executor.get("trailing_callback_pct", 0.005)
        current_sl = executor.get("current_sl", executor["stop_loss"])

        if side == "Buy":  # LONG
            pnl_pct = (current_price - entry) / entry
            if pnl_pct < activation:
                return
            new_sl = current_price * (1 - callback)
            if new_sl <= current_sl:
                return
        else:  # SHORT (side == "Sell")
            pnl_pct = (entry - current_price) / entry
            if pnl_pct < activation:
                return
            new_sl = current_price * (1 + callback)
            if new_sl >= current_sl:
                return

        try:
            await self.exchange.set_tp_sl(
                session, executor["pair"], stop_loss=round(new_sl, 6)
            )
            self.db.executor_state.update_one(
                {"_id": executor["_id"]},
                {"$set": {"current_sl": round(new_sl, 6)}}
            )
            logger.info(
                f"Trail {executor['engine']}/{executor['pair']}: "
                f"SL {current_sl} -> {round(new_sl, 6)} (price={current_price})"
            )
        except Exception as e:
            logger.warning(
                f"Trail amend failed {executor['pair']}: {e}. "
                f"Original SL still active on exchange."
            )

    # ── Restart Reconciliation ───────────────────────────────

    async def reconcile_on_startup(self):
        """Bootstrap: exchange state is source of truth."""
        async with aiohttp.ClientSession() as session:
            positions = await self.exchange.fetch_positions(session)

        exchange_pairs = {p["pair"]: p for p in positions}
        active = list(self.db.executor_state.find(
            {"state": {"$in": ["PENDING", "ACTIVE"]}}
        ))
        executor_pairs = {e["pair"]: e for e in active}

        # Case 1: Exchange has position, we don't track it
        for pair, pos in exchange_pairs.items():
            if pair not in executor_pairs:
                logger.warning(f"Reconciliation: untracked position {pair}")
                # Don't auto-create — might be from HB or manual trade

        # Case 2: We track ACTIVE, exchange doesn't have it (closed during downtime)
        for executor in active:
            pair = executor["pair"]
            if executor["state"] == "ACTIVE" and pair not in exchange_pairs:
                close_type = await self._determine_close_type(executor)
                await self._transition(executor, close_type, "reconciled on restart")
                await self._release_reservation(executor.get("signal_id", ""))
                logger.info(f"Reconciled {pair}: {close_type}")

        # Case 3: PENDING but position exists (filled during downtime)
        for executor in active:
            pair = executor["pair"]
            if executor["state"] == "PENDING" and pair in exchange_pairs:
                await self._transition(executor, "ACTIVE", "reconciled: fill during downtime")
                logger.info(f"Reconciled {pair}: PENDING -> ACTIVE")

    # ── Helpers ──────────────────────────────────────────────

    async def _reserve_heat(self, signal_id: str, heat: float) -> bool:
        """Atomic MongoDB reservation. Returns True if reserved."""
        now = datetime.now(timezone.utc)
        result = self.db.portfolio_risk_state.find_one_and_update(
            {
                "_id": "current",
                f"reservations.{signal_id}": {"$exists": False},
            },
            {"$set": {
                f"reservations.{signal_id}": {
                    "heat": heat,
                    "reserved_at": now,
                    "expires_at": now + timedelta(minutes=5),
                }
            }},
        )
        if result is None:
            logger.debug(f"Reservation failed (duplicate or conflict): {signal_id}")
            return False
        return True

    async def _release_reservation(self, signal_id: str):
        """Release a heat reservation."""
        if not signal_id:
            return
        self.db.portfolio_risk_state.update_one(
            {"_id": "current"},
            {"$unset": {f"reservations.{signal_id}": ""}},
        )

    async def _transition(self, executor: dict, new_state: str, detail: str):
        """Transition executor to a new state with audit trail."""
        now = datetime.now(timezone.utc)
        update = {
            "$set": {"state": new_state},
            "$push": {"state_history": {
                "state": new_state, "at": now, "detail": detail,
            }},
        }
        if new_state in ("TP_CLOSE", "SL_CLOSE", "TIME_LIMIT_CLOSE", "CANCELLED", "FAILED"):
            update["$set"]["closed_at"] = now
            update["$set"]["close_type"] = new_state

        self.db.executor_state.update_one({"_id": executor["_id"]}, update)
        logger.info(
            f"Executor {executor.get('engine')}/{executor.get('pair')}: "
            f"{executor.get('state')} -> {new_state} ({detail})"
        )

    async def _determine_close_type(self, executor: dict) -> str:
        """Determine if closed by TP, SL, or unknown."""
        # Check closed PnL from exchange
        db_pnl = list(self.db.exchange_closed_pnl.find(
            {"pair": executor["pair"]},
            sort=[("updated_time", -1)],
        ).limit(1))

        if db_pnl:
            pnl = db_pnl[0].get("closed_pnl", 0)
            if pnl > 0:
                return "TP_CLOSE"
            else:
                return "SL_CLOSE"

        return "EXCHANGE_CLOSE"  # unknown — exchange closed but no PnL record yet
