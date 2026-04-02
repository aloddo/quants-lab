"""
Paper trading resolver — places orders on Bybit demo via direct REST API.

On CANDIDATE_READY signal: places market order with TP/SL on api-demo.bybit.com.
On subsequent runs: polls positions, records fills and outcomes.

Uses BybitDemoTrader (direct API) instead of HB executors to avoid connector bugs.
"""
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

from core.tasks import BaseTask, TaskContext
from app.services.bybit_demo_trader import BybitDemoTrader

logger = logging.getLogger(__name__)

# Engine-specific exit parameters
ENGINE_PARAMS = {
    "E1": {
        "tp_pct": 0.03,
        "sl_pct": 0.015,
        "time_limit_hours": 24,
        "max_concurrent": 2,
    },
    "E2": {
        "time_limit_hours": 12,
        "max_concurrent": 1,
    },
}


def _bybit_symbol(pair: str) -> str:
    """Convert BTC-USDT → BTCUSDT."""
    return pair.replace("-", "")


class TestnetResolverTask(BaseTask):
    """Place and track orders on Bybit demo for paper trading."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.position_size_pct = task_config.get("position_size_pct", 0.003)
        self.fallback_capital = task_config.get("fallback_capital", 100000)
        self.engines = task_config.get("engines", ["E1", "E2"])
        self.max_portfolio_positions = task_config.get("max_portfolio_positions", 3)

        api_key = task_config.get("api_key") or os.getenv("BYBIT_DEMO_API_KEY", "")
        api_secret = task_config.get("api_secret") or os.getenv("BYBIT_DEMO_API_SECRET", "")
        self.trader = BybitDemoTrader(api_key, api_secret)

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for TestnetResolverTask")
        # Health check: verify Bybit demo API works
        try:
            balance = await self.trader.get_wallet_balance()
            if balance["equity"] <= 0:
                raise RuntimeError("Bybit demo account has zero equity")
            logger.info(f"Bybit demo balance: ${balance['equity']:.0f}")
        except Exception as e:
            raise RuntimeError(f"Bybit demo API health check failed: {e}")

    async def _get_capital(self) -> float:
        try:
            balance = await self.trader.get_wallet_balance()
            return balance["equity"] if balance["equity"] > 0 else self.fallback_capital
        except Exception:
            return self.fallback_capital

    async def _count_active_positions(self, engine: str) -> int:
        docs = await self.mongodb_client.get_documents(
            "candidates", {"engine": engine, "disposition": "TESTNET_ACTIVE"},
        )
        return len(docs)

    async def _place_order(self, candidate: dict, capital: float) -> None:
        engine = candidate["engine"]
        pair = candidate["pair"]
        direction = candidate["direction"]
        params = ENGINE_PARAMS.get(engine, ENGINE_PARAMS["E1"])

        # Position sizing
        amount_usd = capital * self.position_size_pct
        price = candidate.get("decision_price", 0)
        if price <= 0:
            logger.error(f"Invalid decision_price for {pair}: {price}")
            return

        # Get trading rules for min order size
        symbol = _bybit_symbol(pair)
        side = "Buy" if direction == "LONG" else "Sell"

        # Calculate amount in base asset, rounded to Bybit's step size
        # BTC: 0.001, most alts: 0.1 or 1.0. Use 3 decimals as safe default.
        raw_amount = amount_usd / price
        amount = round(raw_amount, 3)

        # Calculate TP/SL prices
        if engine == "E1":
            if direction == "LONG":
                tp_price = round(price * (1 + params["tp_pct"]), 2)
                sl_price = round(price * (1 - params["sl_pct"]), 2)
            else:
                tp_price = round(price * (1 - params["tp_pct"]), 2)
                sl_price = round(price * (1 + params["sl_pct"]), 2)
        elif engine == "E2":
            tp_price = candidate.get("tp_price")
            sl_price = candidate.get("sl_price")
            if tp_price:
                tp_price = round(float(tp_price), 2)
            if sl_price:
                sl_price = round(float(sl_price), 2)
        else:
            tp_price, sl_price = None, None

        try:
            result = await self.trader.place_market_order(
                symbol=symbol,
                side=side,
                qty=str(amount),
                take_profit=str(tp_price) if tp_price else None,
                stop_loss=str(sl_price) if sl_price else None,
            )
            order_id = result["result"]["orderId"]
            logger.info(f"Placed demo order for {engine}/{pair}: order_id={order_id}, amount={amount}, TP={tp_price}, SL={sl_price}")

            # Wait briefly for fill
            import asyncio
            await asyncio.sleep(2)
            order = await self.trader.get_order(order_id)
            fill_price = float(order.get("avgPrice") or 0)

            # Notify via Telegram
            if self.notification_manager:
                from core.notifiers.base import NotificationMessage
                await self.notification_manager.send_notification(NotificationMessage(
                    title=f"Demo Order — {engine}/{pair}",
                    message=(
                        f"<b>Demo Order Filled</b>\n"
                        f"Engine: {engine} | Pair: {pair}\n"
                        f"Side: {'LONG' if direction == 'LONG' else 'SHORT'} | Amount: ${amount_usd:.0f}\n"
                        f"Entry: {fill_price} | TP: {tp_price} | SL: {sl_price}"
                    ),
                    level="info",
                ))

            # Update candidate in MongoDB
            db = self.mongodb_client.get_database()
            await db["candidates"].update_one(
                {"candidate_id": candidate["candidate_id"]},
                {"$set": {
                    "disposition": "TESTNET_ACTIVE",
                    "bybit_order_id": order_id,
                    "testnet_placed_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                    "testnet_amount": amount,
                    "testnet_amount_usd": amount_usd,
                    "testnet_fill_price": fill_price,
                    "testnet_tp_price": tp_price,
                    "testnet_sl_price": sl_price,
                    "testnet_side": side,
                }},
            )
        except Exception as e:
            logger.error(f"Failed to place demo order for {engine}/{pair}: {e}")
            db = self.mongodb_client.get_database()
            await db["candidates"].update_one(
                {"candidate_id": candidate["candidate_id"]},
                {"$set": {"disposition": "TESTNET_FAILED", "testnet_error": str(e)}},
            )

    async def _poll_active_positions(self) -> Dict[str, int]:
        """Check Bybit demo positions and resolve closed ones."""
        stats = {"polled": 0, "resolved": 0, "errors": 0}

        docs = await self.mongodb_client.get_documents(
            "candidates", {"disposition": "TESTNET_ACTIVE"},
        )
        if not docs:
            return stats

        db = self.mongodb_client.get_database()

        # Get all open positions from Bybit
        try:
            all_positions = await self.trader.get_positions()
            open_symbols = {
                p["symbol"]: p for p in all_positions if float(p.get("size", 0)) > 0
            }
        except Exception as e:
            logger.warning(f"Failed to fetch positions: {e}")
            return stats

        for doc in docs:
            pair = doc.get("pair", "")
            symbol = _bybit_symbol(pair)
            stats["polled"] += 1

            # If position no longer open on Bybit, it was closed (TP/SL/liquidation)
            if symbol not in open_symbols:
                # Position closed — try to get the close details
                fill_price = doc.get("testnet_fill_price", 0)
                side = doc.get("testnet_side", "Buy")

                # Estimate exit: query recent closed PnL
                # For now, mark as resolved with the info we have
                await db["candidates"].update_one(
                    {"candidate_id": doc["candidate_id"]},
                    {"$set": {
                        "disposition": "RESOLVED_TESTNET",
                        "testnet_resolved_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                        "testnet_status": "closed",
                    }},
                )
                stats["resolved"] += 1
                logger.info(f"Resolved {doc.get('engine')}/{pair}: position closed on Bybit")

                if self.notification_manager:
                    from core.notifiers.base import NotificationMessage
                    await self.notification_manager.send_notification(NotificationMessage(
                        title=f"Position Closed — {doc.get('engine')}/{pair}",
                        message=(
                            f"<b>Position Closed</b>\n"
                            f"Engine: {doc.get('engine')} | Pair: {pair}\n"
                            f"Entry: {fill_price}\n"
                            f"Closed by TP/SL or manually"
                        ),
                        level="success",
                    ))
            else:
                # Still open — log unrealized PnL
                pos = open_symbols[symbol]
                upnl = pos.get("unrealisedPnl", 0)
                logger.debug(f"{doc.get('engine')}/{pair}: still active, uPnL={upnl}")

        return stats

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        stats = {"new_orders": 0, "poll_stats": {}, "errors": 0}

        capital = await self._get_capital()

        # Portfolio-level check
        total_active = 0
        for eng in self.engines:
            total_active += await self._count_active_positions(eng)

        if total_active >= self.max_portfolio_positions:
            logger.info(f"Portfolio limit: {total_active}/{self.max_portfolio_positions} — skipping")
        else:
            for engine in self.engines:
                params = ENGINE_PARAMS.get(engine, ENGINE_PARAMS["E1"])
                max_concurrent = params.get("max_concurrent", 2)
                active_count = await self._count_active_positions(engine)

                if active_count >= max_concurrent:
                    continue
                if total_active >= self.max_portfolio_positions:
                    break

                ready_docs = await self.mongodb_client.get_documents(
                    "candidates", {"engine": engine, "disposition": "CANDIDATE_READY"},
                )

                slots = min(max_concurrent - active_count, self.max_portfolio_positions - total_active)
                for doc in ready_docs[:slots]:
                    await self._place_order(doc, capital)
                    stats["new_orders"] += 1
                    total_active += 1

        # Poll active positions
        stats["poll_stats"] = await self._poll_active_positions()

        duration = (datetime.now(timezone.utc) - start).total_seconds()
        return {
            "status": "completed",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "execution_id": context.execution_id,
            "stats": stats,
            "duration_seconds": duration,
        }

    async def cleanup(self, context: TaskContext, result) -> None:
        await self.trader.close()
        await super().cleanup(context, result)

    async def on_success(self, context: TaskContext, result) -> None:
        stats = result.result_data.get("stats", {})
        logger.info(f"TestnetResolverTask: {stats['new_orders']} new, {stats['poll_stats']}")

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"TestnetResolverTask failed: {result.error_message}")
