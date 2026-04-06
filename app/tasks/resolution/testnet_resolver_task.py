"""
Paper trading resolver — places orders on Bybit demo via HB native executors.

Uses bybit_perpetual_testnet connector (patched to point at api-demo.bybit.com).
On CANDIDATE_READY: creates PositionExecutor with TP/SL/time limit.
On subsequent runs: polls executor status, records fills and outcomes.
"""
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict

from core.tasks import BaseTask, TaskContext
from app.services.hb_api_client import HBApiClient

logger = logging.getLogger(__name__)

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


from app.tasks.notifying_task import NotifyingTaskMixin


class TestnetResolverTask(NotifyingTaskMixin, BaseTask):
    """Place and track orders on Bybit demo via HB executors."""

    def __init__(self, config):
        super().__init__(config)
        task_config = self.config.config
        self.connector = task_config.get("connector", "bybit_perpetual_testnet")
        self.account = task_config.get("account", "master_account")
        self.position_size_pct = task_config.get("position_size_pct", 0.003)
        self.fallback_capital = task_config.get("fallback_capital", 100000)
        self.engines = task_config.get("engines", ["E1", "E2"])
        self.max_portfolio_positions = task_config.get("max_portfolio_positions", 3)
        self.hb_client = HBApiClient()

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for TestnetResolverTask")
        if not await self.hb_client.health_check():
            raise RuntimeError(
                f"Hummingbot API at {self.hb_client.base_url} is unreachable."
            )

    async def _get_capital(self) -> float:
        try:
            portfolio = await self.hb_client.get_portfolio_state(self.account)
            if isinstance(portfolio, dict):
                for token_data in portfolio.get("tokens", []):
                    if token_data.get("token") == "USDT":
                        return float(token_data.get("balance", self.fallback_capital))
            return self.fallback_capital
        except Exception as e:
            logger.warning(f"Could not fetch capital: {e}, using fallback {self.fallback_capital}")
            return self.fallback_capital

    async def _count_active_positions(self, engine: str) -> int:
        docs = await self.mongodb_client.get_documents(
            "candidates", {"engine": engine, "disposition": "TESTNET_ACTIVE"},
        )
        return len(docs)

    async def _mark_skipped(self, reason: str) -> None:
        """Mark all CANDIDATE_READY as skipped with reason (for would-have-won analysis)."""
        db = self.mongodb_client.get_database()
        result = await db["candidates"].update_many(
            {"disposition": "CANDIDATE_READY"},
            {"$set": {
                "disposition": "SKIPPED_CONCURRENCY",
                "skipped_reason": reason,
                "skipped_at": int(datetime.now(timezone.utc).timestamp() * 1000),
            }},
        )
        if result.modified_count > 0:
            logger.info(f"Marked {result.modified_count} candidates as SKIPPED ({reason})")

    async def _place_order(self, candidate: dict, capital: float) -> None:
        engine = candidate["engine"]
        pair = candidate["pair"]
        direction = candidate["direction"]
        params = ENGINE_PARAMS.get(engine, ENGINE_PARAMS["E1"])

        amount_usd = capital * self.position_size_pct
        price = candidate.get("decision_price", 0)
        if price <= 0:
            logger.error(f"Invalid decision_price for {pair}: {price}")
            return

        amount = round(amount_usd / price, 3)
        side = 1 if direction == "LONG" else 2

        # TP/SL as percentages for triple barrier
        if engine == "E1":
            tp_pct = params["tp_pct"]
            sl_pct = params["sl_pct"]
        elif engine == "E2":
            tp_abs = candidate.get("tp_price")
            sl_abs = candidate.get("sl_price")
            tp_pct = abs(float(tp_abs) - price) / price if tp_abs else 0.03
            sl_pct = abs(float(sl_abs) - price) / price if sl_abs else 0.015

        time_limit = params.get("time_limit_hours", 24) * 3600

        # Pre-register pair so HB connector builds rate limits (prevents 'weight' crash)
        registered = await self.hb_client.ensure_trading_pair(self.connector, pair, self.account)
        if not registered:
            logger.warning(f"Could not pre-register {pair} — executor may fail")

        request_body = {
            "account_name": self.account,
            "executor_config": {
                "type": "position_executor",
                "connector_name": self.connector,
                "trading_pair": pair,
                "side": side,
                "amount": str(amount),
                "leverage": 1,
                "triple_barrier_config": {
                    "take_profit": str(tp_pct),
                    "stop_loss": str(sl_pct),
                    "time_limit": time_limit,
                },
            },
        }

        try:
            result = await self.hb_client.create_executor(request_body)
            executor_id = result.get("executor_id") or result.get("id")
            logger.info(f"Placed demo order for {engine}/{pair}: executor_id={executor_id}")

            if self.notification_manager:
                from core.notifiers.base import NotificationMessage
                await self.notification_manager.send_notification(NotificationMessage(
                    title=f"Demo Order — {engine}/{pair}",
                    message=(
                        f"<b>Demo Order Placed</b>\n"
                        f"Engine: {engine} | Pair: {pair}\n"
                        f"Side: {'LONG' if direction == 'LONG' else 'SHORT'} | Amount: ${amount_usd:.0f}\n"
                        f"Executor: {executor_id}"
                    ),
                    level="info",
                ))

            db = self.mongodb_client.get_database()
            await db["candidates"].update_one(
                {"candidate_id": candidate["candidate_id"]},
                {"$set": {
                    "disposition": "TESTNET_ACTIVE",
                    "executor_id": executor_id,
                    "testnet_placed_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                    "testnet_amount": amount,
                    "testnet_amount_usd": amount_usd,
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
        stats = {"polled": 0, "resolved": 0, "errors": 0}
        docs = await self.mongodb_client.get_documents(
            "candidates", {"disposition": "TESTNET_ACTIVE"},
        )
        db = self.mongodb_client.get_database()

        for doc in docs:
            executor_id = doc.get("executor_id")
            if not executor_id:
                continue
            try:
                status = await self.hb_client.get_executor_status(executor_id)
                stats["polled"] += 1

                exec_status = status.get("status", "").lower()
                if exec_status in ("completed", "failed", "stopped", "closed", "terminated"):
                    fill_price = status.get("entry_price") or status.get("fill_price")
                    exit_price = status.get("close_price") or status.get("exit_price")
                    pnl = status.get("pnl") or status.get("net_pnl_quote")
                    close_type = status.get("close_type")
                    filled_amount = status.get("filled_amount_quote")

                    # Compute slippage vs decision price
                    decision_price = doc.get("decision_price")
                    slippage_bps = None
                    slippage_bucket = None
                    if decision_price and fill_price:
                        try:
                            dp = float(decision_price)
                            fp = float(fill_price)
                            if dp > 0:
                                direction = doc.get("direction", "LONG")
                                raw_slip = (fp - dp) / dp * 10000
                                slippage_bps = raw_slip if direction == "LONG" else -raw_slip
                                abs_slip = abs(slippage_bps)
                                slippage_bucket = (
                                    "safe" if abs_slip < 10
                                    else "borderline" if abs_slip < 20
                                    else "danger"
                                )
                        except (ValueError, TypeError):
                            pass

                    # Execution latency
                    placed_at = doc.get("testnet_placed_at")
                    signal_ts = doc.get("timestamp_utc")
                    exec_latency_ms = None
                    if placed_at and signal_ts:
                        try:
                            exec_latency_ms = int(placed_at) - int(signal_ts)
                        except (ValueError, TypeError):
                            pass

                    await db["candidates"].update_one(
                        {"candidate_id": doc["candidate_id"]},
                        {"$set": {
                            "disposition": "RESOLVED_TESTNET",
                            "testnet_resolved_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                            "testnet_fill_price": fill_price,
                            "testnet_exit_price": exit_price,
                            "testnet_pnl": pnl,
                            "testnet_close_type": close_type,
                            "testnet_filled_amount_quote": filled_amount,
                            "testnet_status": exec_status,
                            "testnet_raw_result": status,
                            "testnet_slippage_bps": slippage_bps,
                            "testnet_slippage_bucket": slippage_bucket,
                            "testnet_exec_latency_ms": exec_latency_ms,
                        }},
                    )
                    stats["resolved"] += 1
                    logger.info(f"Resolved {doc['engine']}/{doc['pair']}: {exec_status}, pnl={pnl}")

                    if self.notification_manager:
                        from core.notifiers.base import NotificationMessage
                        emoji = "+" if pnl and float(pnl) > 0 else "-"
                        await self.notification_manager.send_notification(NotificationMessage(
                            title=f"Position Closed — {doc['engine']}/{doc['pair']}",
                            message=(
                                f"<b>{emoji} Position Closed</b>\n"
                                f"Engine: {doc['engine']} | Pair: {doc['pair']}\n"
                                f"Close type: {close_type}\n"
                                f"PnL: {pnl}"
                            ),
                            level="success" if pnl and float(pnl) > 0 else "error",
                        ))

            except Exception as e:
                stats["errors"] += 1
                logger.warning(f"Error polling executor {executor_id}: {e}")

        return stats

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        stats = {"new_orders": 0, "poll_stats": {}, "errors": 0}
        capital = await self._get_capital()

        total_active = 0
        for eng in self.engines:
            total_active += await self._count_active_positions(eng)

        if total_active >= self.max_portfolio_positions:
            logger.info(f"Portfolio limit: {total_active}/{self.max_portfolio_positions} — skipping")
            # Mark skipped signals for would-have-won analysis
            await self._mark_skipped("portfolio_full")
        else:
            for engine in self.engines:
                params = ENGINE_PARAMS.get(engine, ENGINE_PARAMS["E1"])
                max_concurrent = params.get("max_concurrent", 2)
                active_count = await self._count_active_positions(engine)

                if active_count >= max_concurrent or total_active >= self.max_portfolio_positions:
                    await self._mark_skipped(f"{engine}_concurrent_limit")
                    continue

                ready_docs = await self.mongodb_client.get_documents(
                    "candidates", {"engine": engine, "disposition": "CANDIDATE_READY"},
                )

                slots = min(max_concurrent - active_count, self.max_portfolio_positions - total_active)
                placed = ready_docs[:slots]
                skipped = ready_docs[slots:]

                for doc in placed:
                    await self._place_order(doc, capital)
                    stats["new_orders"] += 1
                    total_active += 1

                # Mark overflow signals as skipped (for would-have-won tracking)
                db = self.mongodb_client.get_database()
                for doc in skipped:
                    await db["candidates"].update_one(
                        {"candidate_id": doc["candidate_id"]},
                        {"$set": {
                            "disposition": "SKIPPED_CONCURRENCY",
                            "skipped_reason": f"{engine}_slots_full",
                            "skipped_at": int(datetime.now(timezone.utc).timestamp() * 1000),
                        }},
                    )
                    stats.setdefault("skipped", 0)
                    stats["skipped"] += 1

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
        await self.hb_client.close()
        await super().cleanup(context, result)

    async def on_success(self, context: TaskContext, result) -> None:
        stats = result.result_data.get("stats", {})
        logger.info(f"TestnetResolverTask: {stats['new_orders']} new, {stats['poll_stats']}")

    async def on_failure(self, context: TaskContext, result) -> None:
        logger.error(f"TestnetResolverTask failed: {result.error_message}")
