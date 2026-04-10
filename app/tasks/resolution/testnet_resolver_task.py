"""
Paper trading resolver — places orders on Bybit demo via HB native executors.

Uses bybit_perpetual_testnet connector (patched to point at api-demo.bybit.com).
On CANDIDATE_READY: creates PositionExecutor with TP/SL/time limit.
On subsequent runs: polls executor status, records fills and outcomes.
"""
import logging
from datetime import datetime, timezone
from typing import Any, Dict

from core.tasks import BaseTask, TaskContext
from app.engines.fmt import fp, fmt_duration, fmt_pnl
from app.services.hb_api_client import HBApiClient
from app.tasks.notifying_task import NotifyingTaskMixin
from app.tasks.resolution.placement import (
    check_duplicate,
    check_portfolio_limits,
    get_capital,
    mark_skipped,
    place_order,
)

logger = logging.getLogger(__name__)


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
        self._trading_rules: Dict[str, Any] = {}

    async def setup(self, context: TaskContext) -> None:
        await super().setup(context)
        await self._refresh_trading_rules()
        await self._setup_checks()

    async def _refresh_trading_rules(self) -> None:
        """Load trading rules from HB API."""
        try:
            rules = await self.hb_client.get_trading_rules(self.connector)
            self._trading_rules = rules
            logger.info(f"Loaded trading rules for {len(self._trading_rules)} pairs")
        except Exception as e:
            logger.warning(f"Could not load trading rules: {e}")

    async def _setup_checks(self) -> None:
        if not self.mongodb_client:
            raise RuntimeError("MongoDB required for TestnetResolverTask")
        if not await self.hb_client.health_check():
            raise RuntimeError(
                f"Hummingbot API at {self.hb_client.base_url} is unreachable."
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
                            fp_val = float(fill_price)
                            if dp > 0:
                                direction = doc.get("direction", "LONG")
                                raw_slip = (fp_val - dp) / dp * 10000
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

                    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

                    # Position duration
                    duration_s = None
                    if placed_at:
                        duration_s = (now_ms - int(placed_at)) / 1000

                    await db["candidates"].update_one(
                        {"candidate_id": doc["candidate_id"]},
                        {"$set": {
                            "disposition": "RESOLVED_TESTNET",
                            "testnet_resolved_at": now_ms,
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

                    # Rich close notification
                    if self.notification_manager:
                        try:
                            from core.notifiers.base import NotificationMessage

                            pnl_float = float(pnl) if pnl is not None else 0
                            amount_usd = doc.get("testnet_amount_usd", 0)

                            slip_str = ""
                            if slippage_bps is not None:
                                slip_str = f"Slippage: {slippage_bps:.1f} bps ({slippage_bucket})"
                            latency_str = ""
                            if exec_latency_ms is not None:
                                latency_str = f"Latency: {fmt_duration(exec_latency_ms / 1000)}"
                            meta_parts = [s for s in [slip_str, latency_str] if s]
                            meta_line = " | ".join(meta_parts)

                            duration_str = fmt_duration(duration_s) if duration_s else "N/A"

                            await self.notification_manager.send_notification(NotificationMessage(
                                title=f"Position Closed — {close_type}",
                                message=(
                                    f"<b>{'✅' if pnl_float > 0 else '❌'} Position Closed — {close_type}</b>\n"
                                    f"{doc['engine']}/{doc['pair']} {doc.get('direction', '')}\n"
                                    f"Fill: {fp(fill_price)} → Exit: {fp(exit_price)}\n"
                                    f"PnL: {fmt_pnl(pnl_float, amount_usd)}\n"
                                    + (f"{meta_line}\n" if meta_line else "")
                                    + f"Duration: {duration_str}"
                                ),
                                level="success" if pnl_float > 0 else "error",
                            ))
                        except Exception as e:
                            logger.warning(f"Close notification failed: {e}")

            except Exception as e:
                stats["errors"] += 1
                logger.warning(f"Error polling executor {executor_id}: {e}")

        return stats

    async def execute(self, context: TaskContext) -> Dict[str, Any]:
        start = datetime.now(timezone.utc)
        stats = {"new_orders": 0, "skipped": 0, "poll_stats": {}, "errors": 0}
        db = self.mongodb_client.get_database()

        capital = await get_capital(self.hb_client, self.account, self.fallback_capital)

        for engine in self.engines:
            # Check engine/portfolio limits
            limit_reason = await check_portfolio_limits(
                db, engine, self.engines, self.max_portfolio_positions,
            )
            if limit_reason:
                # Mark all CANDIDATE_READY for this engine as skipped
                ready = await self.mongodb_client.get_documents(
                    "candidates", {"engine": engine, "disposition": "CANDIDATE_READY"},
                )
                for doc in ready:
                    await mark_skipped(db, doc["candidate_id"], "SKIPPED_CONCURRENCY", limit_reason)
                    stats["skipped"] += 1
                continue

            ready_docs = await self.mongodb_client.get_documents(
                "candidates", {"engine": engine, "disposition": "CANDIDATE_READY"},
            )

            for doc in ready_docs:
                # Per-position dedup guard
                skip_reason = await check_duplicate(
                    db, engine, doc["pair"], doc.get("direction", ""),
                )
                if skip_reason:
                    disposition = (
                        "SKIPPED_DUPLICATE" if skip_reason == "duplicate_active"
                        else "SKIPPED_DIRECTION_CONFLICT"
                    )
                    await mark_skipped(db, doc["candidate_id"], disposition, skip_reason)
                    stats["skipped"] += 1
                    logger.info(f"Skipped {engine}/{doc['pair']}: {skip_reason}")
                    continue

                # Re-check portfolio limits (may have filled a slot above)
                limit_reason = await check_portfolio_limits(
                    db, engine, self.engines, self.max_portfolio_positions,
                )
                if limit_reason:
                    await mark_skipped(db, doc["candidate_id"], "SKIPPED_CONCURRENCY", limit_reason)
                    stats["skipped"] += 1
                    continue

                executor_id = await place_order(
                    candidate=doc,
                    capital=capital,
                    position_size_pct=self.position_size_pct,
                    connector=self.connector,
                    account=self.account,
                    hb_client=self.hb_client,
                    trading_rules=self._trading_rules,
                    db=db,
                    notification_manager=self.notification_manager,
                )
                if executor_id:
                    stats["new_orders"] += 1

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
